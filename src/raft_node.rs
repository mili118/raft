use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use log::info;
use rand::Rng;
use tokio::sync::{Mutex, Notify};
use tonic::{transport::Server, Request, Response, Status};
use std::collections::HashMap;

use crate::pb::{
    raft_client::RaftClient,
    raft_server::{Raft, RaftServer},
    AppendEntriesArgs, AppendEntriesReply, Entry as PbEntry, RequestVoteArgs, RequestVoteReply,
};

const TIMEOUT_HEARTBEAT_MS: i64 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerType {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, Debug)]
pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: i32,
}

struct RaftState {
    status: ServerType,
    current_term: i32,
    voted_for: i32,
    log: Vec<PbEntry>,

    commit_index: i32,
    last_applied: i32,

    next_index: Vec<i32>,
    match_index: Vec<i32>,

    timeout_election_ms: i64,
    time_election: Instant,
    time_heartbeat: Instant,

    kv_store: HashMap<String, String>,
}

pub struct RaftNode {
    peers: Vec<String>,
    me: usize,

    state: Mutex<RaftState>,
    notify: Notify,

    killed: AtomicBool,
}

impl RaftNode {
    pub fn new(peers: Vec<String>, me: usize) -> Arc<Self> {
        let now = Instant::now();
        let mut log = Vec::new();

        log.push(PbEntry {
            command: Vec::new(),
            term: 0,
        });

        let n = peers.len();
        let state = RaftState {
            status: ServerType::Follower,
            current_term: 0,
            voted_for: -1,
            log,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; n],
            match_index: vec![0; n],
            timeout_election_ms: random_election_timeout_ms(),
            time_election: now,
            time_heartbeat: now,
            kv_store: HashMap::new(),
        };

        Arc::new(Self {
            peers,
            me,
            state: Mutex::new(state),
            notify: Notify::new(),
            killed: AtomicBool::new(false),
        })
    }

    pub fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn killed(&self) -> bool {
        self.killed.load(Ordering::SeqCst)
    }

    pub async fn get_state(&self) -> (i32, bool) {
        let st = self.state.lock().await;
        (st.current_term, st.status == ServerType::Leader)
    }

    pub async fn start(&self, command: Vec<u8>) -> (i32, i32, bool) {
        if self.killed() {
            return (-1, -1, false);
        }

        let mut st = self.state.lock().await;
        if st.status != ServerType::Leader {
            return (-1, -1, false);
        }

        let index = st.log.len() as i32;
        let term = st.current_term;
        let entry = PbEntry {
            command,
            term: term,
        };
        st.log.push(entry);

        (index, term, true)
    }

    /// Start the ticker + apply_command loops and gRPC server.
    pub async fn run(
        self: Arc<Self>,
        listen_addr: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let raft_service = RaftService { node: self.clone() };

        // Start ticker and applier in background
        let ticker_node = self.clone();
        tokio::spawn(async move {
            ticker_node.ticker().await;
        });

        let applier_node = self.clone();
        tokio::spawn(async move {
            applier_node.apply_command().await;
        });

        let addr = listen_addr.parse()?;
        Server::builder()
            .add_service(RaftServer::new(raft_service))
            .serve(addr)
            .await?;

        Ok(())
    }

    async fn ticker(self: Arc<Self>) {
        while !self.killed() {
            let (status, do_heartbeat, do_broadcast, start_election) = {
                let mut st = self.state.lock().await;
                let mut do_heartbeat = false;
                let mut do_broadcast = false;
                let mut start_election = false;

                match st.status {
                    ServerType::Leader => {
                        let elapsed = st.time_heartbeat.elapsed().as_millis() as i64;
                        if elapsed > TIMEOUT_HEARTBEAT_MS {
                            st.time_heartbeat = Instant::now();
                            do_heartbeat = true;
                        }

                        do_broadcast = true;
                    }
                    _ => {
                        let elapsed = st.time_election.elapsed().as_millis() as i64;
                        if elapsed > st.timeout_election_ms {
                            info!(
                                "In term [{}], server [{}] becomes a candidate",
                                st.current_term, self.me
                            );
                            st.current_term += 1;
                            st.status = ServerType::Candidate;
                            st.time_election = Instant::now();
                            st.timeout_election_ms = random_election_timeout_ms();
                            start_election = true;
                        }
                    }
                }

                (st.status, do_heartbeat, do_broadcast, start_election)
            };

            if status == ServerType::Leader && do_heartbeat {
                self.send_heartbeats().await;
            }
            if status == ServerType::Leader && do_broadcast {
                self.broadcast_new_command().await;
            }
            if start_election {
                self.start_election().await;
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn start_election(self: &Arc<Self>) {
        let (term, last_log_index, last_log_term) = {
            let mut st = self.state.lock().await;
            let last_index = st.log.len() as i32 - 1;
            let last_term = st.log[last_index as usize].term;
            st.voted_for = self.me as i32;
            (st.current_term, last_index, last_term)
        };

        let votes = Arc::new(std::sync::atomic::AtomicI32::new(1));
        let total_peers = self.peers.len();
        let me = self.me;

        for peer in 0..total_peers {
            if peer == me {
                continue;
            }

            let this = self.clone();
            let votes_clone = votes.clone();
            let endpoint = this.peers[peer].clone();

            tokio::spawn(async move {
                let args = RequestVoteArgs {
                    term,
                    candidate_id: me as i32,
                    last_log_index,
                    last_log_term,
                };

                if let Ok(mut client) = RaftClient::connect(endpoint).await {
                    if let Ok(resp) = client.request_vote(Request::new(args)).await {
                        let reply = resp.into_inner();
                        let mut st = this.state.lock().await;
                        if term == st.current_term && st.status == ServerType::Candidate {
                            if reply.vote_granted {
                                let new_votes =
                                    votes_clone.fetch_add(1, Ordering::SeqCst) + 1;
                                if new_votes > (total_peers / 2) as i32 {
                                    info!(
                                        "In term [{}], server [{}] wins election and becomes the leader",
                                        st.current_term, this.me
                                    );
                                    st.status = ServerType::Leader;
                                    for i in 0..total_peers {
                                        if i == this.me {
                                            continue;
                                        }
                                        st.next_index[i] = st.log.len() as i32;
                                        st.match_index[i] = 0;
                                    }
                                    let leader_node = this.clone();
                                    tokio::spawn(async move {
                                        leader_node.check_commit().await;
                                    });
                                }
                            } else if reply.term > st.current_term {
                                st.status = ServerType::Follower;
                                st.voted_for = -1;
                                st.current_term = reply.term;
                            }
                        }
                    }
                }
            });
        }
    }

    async fn send_heartbeats(self: &Arc<Self>) {
        let (term, commit_index, prev_log_index, prev_log_term) = {
            let st = self.state.lock().await;
            let last_index = st.log.len() as i32 - 1;
            let last_term = st.log[last_index as usize].term;
            (st.current_term, st.commit_index, last_index, last_term)
        };

        let total_peers = self.peers.len();
        let me = self.me;

        for peer in 0..total_peers {
            if peer == me {
                continue;
            }

            let this = self.clone();
            let args = AppendEntriesArgs {
                term,
                leader_id: me as i32,
                prev_log_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: commit_index,
            };

            tokio::spawn(async move {
                this.send_append_entries(peer, args, None).await;
            });
        }
    }

    async fn broadcast_new_command(self: &Arc<Self>) {
        let tasks = {
            let st = self.state.lock().await;
            let mut tasks = Vec::new();
            let total_peers = self.peers.len();
            for peer in 0..total_peers {
                if peer == self.me {
                    continue;
                }
                if st.next_index[peer] >= st.log.len() as i32 {
                    continue;
                }
                let send_log_index = st.next_index[peer];
                let prev_log_index = send_log_index - 1;
                let prev_log_term = st.log[prev_log_index as usize].term;
                let entries: Vec<PbEntry> =
                    st.log[send_log_index as usize..].to_vec();
                let args = AppendEntriesArgs {
                    term: st.current_term,
                    leader_id: self.me as i32,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: st.commit_index,
                };
                tasks.push((peer, args, send_log_index));
            }
            tasks
        };

        for (peer, args, send_log_index) in tasks {
            let this = self.clone();
            tokio::spawn(async move {
                this.send_append_entries(peer, args, Some(send_log_index))
                    .await;
            });
        }
    }

    async fn send_append_entries(
        self: Arc<Self>,
        peer: usize,
        args: AppendEntriesArgs,
        send_log_index: Option<i32>,
    ) {
        let term = args.term;
        let endpoint = self.peers[peer].clone();

        if let Ok(mut client) = RaftClient::connect(endpoint).await {
            if let Ok(resp) = client.append_entries(Request::new(args.clone())).await {
                let reply = resp.into_inner();
                let mut st = self.state.lock().await;

                if term != st.current_term {
                    return;
                }
                if reply.term > st.current_term {
                    st.status = ServerType::Follower;
                    st.current_term = reply.term;
                    st.voted_for = -1;
                }

                if let Some(send_index) = send_log_index {
                    if reply.success {
                        let entries_len = args.entries.len() as i32;
                        st.next_index[peer] = send_index + entries_len;
                        st.match_index[peer] = args.prev_log_index + entries_len;
                    } else {
                        if st.next_index[peer] >= 2 {
                            st.next_index[peer] -= 1;
                        }
                    }
                }
            }
        }
    }

    async fn check_commit(self: Arc<Self>) {
        loop {
            if self.killed() {
                return;
            }

            let mut commit_updated = false;

            {
                let mut st = self.state.lock().await;
                if st.status != ServerType::Leader {
                    return;
                }

                let log_len = st.log.len() as i32;
                if st.commit_index < log_len - 1 {
                    let mut commit_index = st.commit_index + 1;
                    while commit_index < log_len - 1 {
                        if st.log[commit_index as usize].term == st.current_term {
                            break;
                        }
                        commit_index += 1;
                    }

                    let mut consensus = 1;
                    for peer in 0..self.peers.len() {
                        if peer == self.me {
                            continue;
                        }
                        if st.match_index[peer] >= commit_index {
                            consensus += 1;
                        }
                    }

                    if consensus > (self.peers.len() / 2) as i32
                        && st.log[commit_index as usize].term == st.current_term
                    {
                        st.commit_index = commit_index;
                        commit_updated = true;
                    }
                }
            }

            if commit_updated {
                self.notify.notify_waiters();
            }

            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    pub async fn apply_command(self: Arc<Self>) {
        loop {
            if self.killed() {
                return;
            }

            let (command_index, command) = {
                loop {
                    let mut st = self.state.lock().await;

                    if st.last_applied < st.commit_index {
                        // There is something to apply
                        st.last_applied += 1;
                        let idx = st.last_applied;
                        let cmd = st.log[idx as usize].command.clone();
                        break (idx, cmd);
                    }

                    drop(st);
                    self.notify.notified().await;

                    if self.killed() {
                        return;
                    }
                }
            };

            info!(
                "Server [{}] applies command: index [{}], content [{:?}]",
                self.me, command_index, command
            );
        }
    }
}

fn random_election_timeout_ms() -> i64 {
    let mut rng = rand::thread_rng();
    400 + rng.gen_range(0..400)
}

/// gRPC service implementation

pub struct RaftService {
    pub node: Arc<RaftNode>,
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let args = request.into_inner();
        let mut st = self.node.state.lock().await;

        if args.term < st.current_term {
            return Ok(Response::new(RequestVoteReply {
                term: st.current_term,
                vote_granted: false,
            }));
        }

        if args.term > st.current_term {
            st.current_term = args.term;
            st.voted_for = -1;
            st.status = ServerType::Follower;
        }

        let mut vote_granted = false;
        let last_log_index = st.log.len() as i32 - 1;
        let last_log_term = st.log[last_log_index as usize].term;

        if st.voted_for == -1 || st.voted_for == args.candidate_id {
            if args.last_log_term > last_log_term
                || (args.last_log_term == last_log_term
                && args.last_log_index >= last_log_index)
            {
                st.voted_for = args.candidate_id;
                st.time_election = Instant::now();
                vote_granted = true;
                info!(
                    "Server [{}] votes for candidate [{}] in term [{}]",
                    self.node.me, args.candidate_id, st.current_term
                );
            }
        }

        Ok(Response::new(RequestVoteReply {
            term: st.current_term,
            vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let args = request.into_inner();
        let mut st = self.node.state.lock().await;

        if args.term < st.current_term {
            return Ok(Response::new(AppendEntriesReply {
                term: st.current_term,
                success: false,
            }));
        }

        if args.term > st.current_term {
            st.voted_for = -1;
            st.current_term = args.term;
        }

        st.status = ServerType::Follower;
        st.time_election = Instant::now();

        if st.log.len() as i32 <= args.prev_log_index
            || st.log[args.prev_log_index as usize].term != args.prev_log_term
        {
            return Ok(Response::new(AppendEntriesReply {
                term: st.current_term,
                success: false,
            }));
        }

        let mut last_index_append = args.prev_log_index;
        if args.prev_log_index + args.entries.len() as i32 > st.log.len() as i32 - 1 {
            let prefix = st.log[..(args.prev_log_index as usize + 1)].to_vec();
            st.log = prefix;
            st.log.extend(args.entries.into_iter());
            last_index_append = st.log.len() as i32 - 1;
        } else {
            // Overwrite/compare existing entries
            let mut idx = args.prev_log_index + 1;
            let mut replaced = false;
            for e in &args.entries {
                let cur_term = st.log[idx as usize].term;
                if e.term == cur_term {
                    idx += 1;
                    last_index_append = idx - 1;
                } else {
                    let prefix = st.log[..(args.prev_log_index as usize + 1)].to_vec();
                    st.log = prefix;
                    st.log.extend(args.entries.clone().into_iter());
                    last_index_append = st.log.len() as i32 - 1;
                    replaced = true;
                    break;
                }
            }
            if !replaced {
                last_index_append = idx - 1;
            }
        }

        if args.leader_commit > st.commit_index {
            let old_commit_index = st.commit_index;
            if last_index_append <= args.leader_commit {
                st.commit_index = last_index_append;
            } else {
                st.commit_index = args.leader_commit;
            }
            if st.commit_index > old_commit_index {
                self.node.notify.notify_waiters();
            }
        }

        Ok(Response::new(AppendEntriesReply {
            term: st.current_term,
            success: true,
        }))
    }
}