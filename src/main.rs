// src/main.rs
use std::env;
use std::sync::Arc;

use env_logger::Env;
use raft_rs::RaftNode;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Usage: cargo run -- <me>
    //   me = 0, 1, or 2
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <me>", args[0]);
        eprintln!("  where <me> is 0, 1, or 2");
        std::process::exit(1);
    }

    let me: usize = args[1].parse().expect("me must be 0, 1, or 2");

    // Equivalent to Go: peers := []string{":5001", ":5002", ":5003"}
    // For tonic/HTTP2 we use full URLs for clients.
    let peers = vec![
        "http://127.0.0.1:5001".to_string(),
        "http://127.0.0.1:5002".to_string(),
        "http://127.0.0.1:5003".to_string(),
    ];

    // listening address for this node (like ":5001", ":5002", ":5003" in Go)
    let listen_addr = match me {
        0 => "127.0.0.1:5001",
        1 => "127.0.0.1:5002",
        2 => "127.0.0.1:5003",
        _ => {
            eprintln!("me must be 0, 1, or 2");
            std::process::exit(1);
        }
    }
        .to_string();

    // Create the Raft node
    let node: Arc<RaftNode> = RaftNode::new(peers, me);

    // Start the Raft RPC server + ticker + applier in the background
    let node_for_server = node.clone();
    tokio::spawn(async move {
        if let Err(e) = node_for_server.run(listen_addr).await {
            eprintln!("Raft server error: {:?}", e);
        }
    });

    // Give the cluster time to elect a leader (like time.Sleep(1s) in Go)
    sleep(Duration::from_secs(1)).await;

    // If this node is the leader, start three commands (100, 200, 300)
    let (term, is_leader) = node.get_state().await;
    println!("Node {} in term {}: is_leader = {}", me, term, is_leader);

    if is_leader {
        // Encode integers as 4-byte little-endian; in a real system you'd define a proper command type.
        let cmd100 = 100i32.to_le_bytes().to_vec();
        let cmd200 = 200i32.to_le_bytes().to_vec();
        let cmd300 = 300i32.to_le_bytes().to_vec();

        let res1 = node.start(cmd100).await;
        println!("Node {} Start(100) -> {:?}", me, res1);

        let res2 = node.start(cmd200).await;
        println!("Node {} Start(200) -> {:?}", me, res2);

        let res3 = node.start(cmd300).await;
        println!("Node {} Start(300) -> {:?}", me, res3);
    }

    // Wait a bit so entries can replicate and be applied
    sleep(Duration::from_secs(1)).await;

    // Kill this node (like rf.Kill() in Go)
    node.kill();

    // Let background tasks finish cleanly
    sleep(Duration::from_millis(200)).await;

    Ok(())
}