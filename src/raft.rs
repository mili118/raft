use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// a single log entry
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

// state from figure 2 of paper
pub struct RaftState {
    //the current election term
    pub current_term:u64,

    // which node id this the current raft voted for
    pub voted_for: Option<u64>,

    //persistent log
    pub log: Vec<LogEntry>,

    //index of highest log entry known to be committed
    pub commit_index: usize,

    //index of highest log entry applied to state machine (this one)
    pub last_applied: usize,

    //role of this node
    pub role: Role,

    //node id of leader node
    pub leader_id: Option<u64>,

    //index of next log entry to send to server (leader last log index + 1)
    pub next_index: HashMap<u64, usize>,

    //index of highest log entry known to be replicated on server (init to 0)
    pub match_index: HashMap<u64, usize>,
}

impl RaftState {
    pub fn new() -> Self {
        RaftState {
            current_term: 0, 
            voted_for: None, 
            log: vec![LogEntry {term: 0, command: String::new()}], // dummy index 0
            commit_index: 0, 
            last_applied: 0, 
            role: Role::Follower, 
            leader_id: None, 
            next_index: HashMap::new(), 
            match_index: HashMap::new(),
        }
    }
}