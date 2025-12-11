use std::env;
use std::sync::Arc;

use env_logger::Env;
use raft_rs::RaftNode;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <nodeID>", args[0]);
        std::process::exit(1);
    }
    let node_id: usize = args[1].parse().expect("nodeID must be 0, 1, or 2");

    let peers = vec![
        "http://127.0.0.1:5001".to_string(),
        "http://127.0.0.1:5002".to_string(),
        "http://127.0.0.1:5003".to_string(),
    ];

    let listen_addr = match node_id {
        0 => "127.0.0.1:5001",
        1 => "127.0.0.1:5002",
        2 => "127.0.0.1:5003",
        _ => {
            eprintln!("nodeID must be 0, 1, or 2");
            std::process::exit(1);
        }
    }
        .to_string();

    let node: Arc<RaftNode> = RaftNode::new(peers, node_id);

    let node_for_server = node.clone();
    tokio::spawn(async move {
        if let Err(e) = node_for_server.run(listen_addr).await {
            eprintln!("Raft server error: {:?}", e);
        }
    });
    
    sleep(Duration::from_secs(30)).await;
    node.print_kv().await;

    Ok(())
}