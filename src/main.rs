use std::env;
use std::sync::Arc;

use env_logger::Env;
use raft_rs::RaftNode;
use tokio::time::{sleep, Duration};
use raft_rs::pb::Command;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Usage: cargo run -- ID
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <me>", args[0]);
        eprintln!("  where <me> is 0, 1, or 2");
        std::process::exit(1);
    }

    let me: usize = args[1].parse().expect("me must be 0, 1, or 2");

    let peers = vec![
        "http://127.0.0.1:5001".to_string(),
        "http://127.0.0.1:5002".to_string(),
        "http://127.0.0.1:5003".to_string(),
    ];

    let listen_addr = match me {
        0 => "127.0.0.1:5001",
        1 => "127.0.0.1:5002",
        2 => "127.0.0.1:5003",
        _ => {
            eprintln!("id must be 0, 1, or 2");
            std::process::exit(1);
        }
    }.to_string();

    let node: Arc<RaftNode> = RaftNode::new(peers, me);

    // Start the Raft RPC server + ticker + applier in the background
    let node_for_server = node.clone();
    tokio::spawn(async move {
        if let Err(e) = node_for_server.run(listen_addr).await {
            eprintln!("Raft server error: {:?}", e);
        }
    });

    sleep(Duration::from_secs(1)).await;

    let (term, is_leader) = node.get_state().await;
    println!("Node {} in term {}: is_leader = {}", me, term, is_leader);

    if is_leader {
        node.start(Command {
            kind: "PUT".into(),
            key: "x".into(),
            value: "100 ".into(),
        }).await;

        node.start(Command {
            kind: "PUT".into(),
            key: "y".into(),
            value: "200 ".into(),
        }).await;

        node.start(Command {
            kind: "APPEND".into(),
            key: "x".into(),
            value: "300 ".into(),
        }).await;
    }

    // Wait so entries can replicate and be applied
    sleep(Duration::from_secs(1)).await;

    node.print_kv().await;
    node.kill();

    // Let background tasks finish
    sleep(Duration::from_millis(200)).await;

    Ok(())
}