use std::sync::{Arc, Mutex};
use crate::raft::{RaftState, Role};
use crate::client::RaftClient;
use crate::rpc::{RequestVoteArgs, AppendEntriesArgs};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

pub struct Node {
    pub state: Arc<Mutex<RaftState>>,
    pub id: u64,
    pub peers: Vec<u64>, // IDs of other nodes in the cluster
    pub client: Arc<RaftClient>,
    pub heartbeat_rx: mpsc::Receiver<()>,
}

const HEARTBEAT_INTERVAL: u64 = 100; //ms
const MIN_ELECTION_TIMEOUT: u64 = 300; //ms

impl Node {
    pub async fn run(mut self) {
        let mut heartbeat_timer = tokio::time::interval(Duration::from_millis(HEARTBEAT_INTERVAL));
        let mut election_deadline = Instant::now() + Duration::from_millis(Self::rand_election_timeout());

        loop {
            let role = {
                self.state.lock().unwrap().role.clone()
            };

            match role {
                Role::Leader => {
                    tokio::select! {
                        _ = heartbeat_timer.tick() => {
                            self.send_heartbeats().await;
                        }
                        Some(_) = self.heartbeat_rx.recv() => {
                            // Someone else became leader, reset timer
                            election_deadline = Instant::now() + Duration::from_millis(Self::rand_election_timeout());
                        }
                    }
                }

                Role::Follower | Role::Candidate => {
                    tokio::select! {
                        //did we timeout
                        _ = tokio::time::sleep_until(election_deadline) => {
                            //no heartbeat so start election
                            self.start_election().await;

                            election_deadline = Instant::now() + Duration::from_millis(Self::rand_election_timeout());
                        }
                        Some(_) = self.heartbeat_rx.recv() => {
                            // Received heartbeat from leader, reset election timer
                            election_deadline = Instant::now() + Duration::from_millis(Self::rand_election_timeout());
                        }
                    }
                }
            }
        }
    }

    fn rand_election_timeout() -> u64 {
        use rand::Rng;
        rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT..MIN_ELECTION_TIMEOUT * 2)
    }

    async fn send_heartbeats(&self) {
        let (term, commit_index) = {
            let state = self.state.lock().unwrap();
            (state.current_term, state.commit_index as u64)
        };

        println!("Leader {} sending heartbeats for term {}", self.id, term);

        // Send heartbeats
        for peer_id in &self.peers {
            let client = self.client.clone();
            let peer = *peer_id;
            let args = AppendEntriesArgs {
                term,
                leader_id: self.id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![], // Empty for heartbeat
                leader_commit: commit_index,
            };

            // Spawn task to send heartbeat
            tokio::spawn(async move {
                match client.append_entries(peer, args).await {
                    Ok(reply) => {
                        if !reply.success {
                            eprintln!("Heartbeat rejected by peer {}", peer);
                        }
                    }
                    Err(_) => {
                    }
                }
            });
        }
    }


    async fn start_election(&self) {
        // Step 1: Transition to candidate and increment term
        let (current_term, last_log_index, last_log_term) = {
            let mut state = self.state.lock().unwrap();
            state.role = Role::Candidate;
            state.current_term += 1;
            state.voted_for = Some(self.id);

            let last_log_index = state.log.len() - 1;
            let last_log_term = state.log.get(last_log_index).map(|e| e.term).unwrap_or(0);

            println!("Node {} starting election, term {}", self.id, state.current_term);
            (state.current_term, last_log_index as u64, last_log_term)
        };

        // Send RequestVote RPCs
        let mut vote_tasks = Vec::new();

        for peer_id in &self.peers {
            let peer_id = *peer_id;
            let candidate_id = self.id;
            let client = self.client.clone();

            // Spawn a task for each peer
            let task = tokio::spawn(async move {
                Self::send_request_vote(
                    client,
                    peer_id,
                    current_term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                ).await
            });

            vote_tasks.push(task);
        }

        // Wait for all responses in parallel
        let mut votes_received = 1;
        let majority = (self.peers.len() + 1) / 2 + 1;

        for task in vote_tasks {
            if let Ok(vote_granted) = task.await {
                if vote_granted {
                    votes_received += 1;
                }
            }
        }

        // Check if we won the election
        if votes_received >= majority {
            let mut state = self.state.lock().unwrap();
            if state.role == Role::Candidate && state.current_term == current_term {
                state.role = Role::Leader;
                println!("Node {} became leader for term {}", self.id, current_term);

                // Initialize leader state
                let log_len = state.log.len();
                for peer_id in &self.peers {
                    state.next_index.insert(*peer_id, log_len);
                    state.match_index.insert(*peer_id, 0);
                }
            }
        } else {
            println!("Node {} lost election for term {} ({}/{} votes)",
                     self.id, current_term, votes_received, majority);
        }
    }

    async fn send_request_vote(
        client: Arc<RaftClient>,
        peer_id: u64,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> bool {
        let args = RequestVoteArgs {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        };

        println!("Sending RequestVote to peer {} for term {}", peer_id, term);

        match client.request_vote(peer_id, args).await {
            Ok(reply) => {
                println!("Received vote response from peer {}: granted={}", peer_id, reply.vote_granted);
                reply.vote_granted
            }
            Err(_) => {
                false
            }
        }
    }
}