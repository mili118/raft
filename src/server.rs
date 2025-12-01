use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use crate::raft::{RaftState, Role};
use crate::rpc::{
    RaftService, RequestVoteArgs, RequestVoteReply,
    AppendEntriesArgs, AppendEntriesReply,
};

pub struct RaftServiceImpl {
    state: Arc<Mutex<RaftState>>,
    node_id: u64,
    heartbeat_tx: mpsc::Sender<()>,
}

impl RaftServiceImpl {
    pub fn new(state: Arc<Mutex<RaftState>>, node_id: u64, heartbeat_tx: mpsc::Sender<()>) -> Self {
        RaftServiceImpl { state, node_id, heartbeat_tx }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let args = request.into_inner();

        let mut state = self.state.lock().unwrap();

        // If the candidate's term is greater, update our term and become follower
        if args.term > state.current_term {
            state.current_term = args.term;
            state.role = Role::Follower;
            state.voted_for = None;
            state.leader_id = None;
        }

        let mut vote_granted = false;

        // vote yes if
        // 1. candidate's term >= our term
        // 2. We haven't voted for anyone else this term
        // 3. candidate's log is at least same up to date as ours
        if args.term >= state.current_term {
            let already_voted = state.voted_for.is_some() && state.voted_for != Some(args.candidate_id);

            if !already_voted {
                // Check if candidate's log is at least as up-to-date
                let our_last_index = state.log.len() - 1;
                let our_last_term = state.log.get(our_last_index).map(|e| e.term).unwrap_or(0);

                let candidate_log_ok = args.last_log_term > our_last_term ||
                    (args.last_log_term == our_last_term && args.last_log_index >= our_last_index as u64);

                if candidate_log_ok {
                    vote_granted = true;
                    state.voted_for = Some(args.candidate_id);
                    state.current_term = args.term;
                    println!("Node {} granted vote to {} for term {}", self.node_id, args.candidate_id, args.term);
                }
            }
        }

        let reply = RequestVoteReply {
            term: state.current_term,
            vote_granted,
        };

        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let args = request.into_inner();

        let mut state = self.state.lock().unwrap();

        let mut success = false;

        // If leader's term is greater, update our term and become follower
        if args.term > state.current_term {
            state.current_term = args.term;
            state.role = Role::Follower;
            state.voted_for = None;
        }

        // Reply false if term < currentTerm
        if args.term < state.current_term {
            let reply = AppendEntriesReply {
                term: state.current_term,
                success: false,
            };
            return Ok(Response::new(reply));
        }

        // Valid leader for this term
        state.leader_id = Some(args.leader_id);
        state.role = Role::Follower;

        // Notify the node event loop that we received a heartbeat
        let _ = self.heartbeat_tx.try_send(());

        // If this is a heartbeat (empty entries), just accept it
        if args.entries.is_empty() {
            success = true;

            // Update commit index if leader's commit index is higher
            if args.leader_commit > state.commit_index as u64 {
                state.commit_index = std::cmp::min(args.leader_commit as usize, state.log.len() - 1);
            }
        } else {
            // For now, simplified log replication
            // Check if we have the prev_log entry
            if args.prev_log_index == 0 ||
               (args.prev_log_index < state.log.len() as u64 &&
                state.log[args.prev_log_index as usize].term == args.prev_log_term) {

                // Delete conflicting entries and append new ones
                let mut index = args.prev_log_index as usize + 1;
                for entry in args.entries {
                    if index < state.log.len() {
                        if state.log[index].term != entry.term {
                            state.log.truncate(index);
                            state.log.push(entry.into());
                        }
                    } else {
                        state.log.push(entry.into());
                    }
                    index += 1;
                }

                // Update commit index
                if args.leader_commit > state.commit_index as u64 {
                    state.commit_index = std::cmp::min(args.leader_commit as usize, state.log.len() - 1);
                }

                success = true;
            }
        }

        let reply = AppendEntriesReply {
            term: state.current_term,
            success,
        };

        Ok(Response::new(reply))
    }
}
