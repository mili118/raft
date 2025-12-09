pub mod pb {
    tonic::include_proto!("raftpb");
}

mod raft_node;

pub use crate::raft_node::{ApplyMsg, RaftNode, ServerType};