mod raft;
mod node;
mod client;
mod server;
mod rpc;

use std::sync::{Arc, Mutex};
use clap::Parser;
use tonic::transport::Server;
use crate::raft::RaftState;
use crate::client::RaftClient;
use crate::server::RaftServiceImpl;
use crate::rpc::RaftServiceServer;
use crate::node::Node;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Node ID
    #[arg(short, long)]
    id: u64,

    /// Port to listen on
    #[arg(short, long)]
    port: u16,

    /// Peer addresses in format "id:host:port" (can be specified multiple times)
    #[arg(short = 'P', long = "peer")]
    peers: Vec<String>,
}

fn parse_peer(s: &str) -> Result<(u64, String), String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return Err(format!("Invalid peer format: {}. Expected id:host:port", s));
    }

    let id = parts[0].parse::<u64>()
        .map_err(|_| format!("Invalid peer ID: {}", parts[0]))?;

    let address = format!("{}:{}", parts[1], parts[2]);

    Ok((id, address))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting Raft node {} on port {}", args.id, args.port);

    // Parse peer addresses
    let peers: Vec<(u64, String)> = args.peers
        .iter()
        .map(|p| parse_peer(p).expect("Invalid peer format"))
        .collect();

    println!("Peers: {:?}", peers);

    // Create shared Raft state
    let state = Arc::new(Mutex::new(RaftState::new()));

    // Create gRPC client (connections will be made on-demand)
    let client = Arc::new(RaftClient::new(peers.clone()));

    // Create channel for heartbeat notifications
    let (heartbeat_tx, heartbeat_rx) = tokio::sync::mpsc::channel(100);

    // Create node
    let peer_ids: Vec<u64> = peers.iter().map(|(id, _)| *id).collect();
    let node = Node {
        state: state.clone(),
        id: args.id,
        peers: peer_ids,
        client,
        heartbeat_rx,
    };

    // Create gRPC server
    let service = RaftServiceImpl::new(state, args.id, heartbeat_tx);
    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    println!("gRPC server listening on {}", addr);

    // Spawn server in background
    tokio::spawn(async move {
        Server::builder()
            .add_service(RaftServiceServer::new(service))
            .serve(addr)
            .await
            .expect("Server failed");
    });

    // Give the server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Run node event loop (this blocks)
    node.run().await;

    Ok(())
}
