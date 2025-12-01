// Re-export generated protobuf types
pub mod kvraft {
    tonic::include_proto!("kvraft");
}

// Re-export commonly used types
pub use kvraft::{
    raft_service_server::{RaftService, RaftServiceServer},
    raft_service_client::RaftServiceClient,
    AppendEntriesArgs, AppendEntriesReply,
    RequestVoteArgs, RequestVoteReply,
    Entry,
};

// Conversion from our LogEntry to protobuf Entry
impl From<crate::raft::LogEntry> for Entry {
    fn from(entry: crate::raft::LogEntry) -> Self {
        Entry {
            term: entry.term,
            command: entry.command,
        }
    }
}

// Conversion from protobuf Entry to our LogEntry
impl From<Entry> for crate::raft::LogEntry {
    fn from(entry: Entry) -> Self {
        crate::raft::LogEntry {
            term: entry.term,
            command: entry.command,
        }
    }
}
