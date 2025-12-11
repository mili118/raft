use env_logger::Env;
use raft_rs::pb::{
    raft_client::RaftClient,
    Command, GetStateRequest, SendCommandRequest,
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let server_addrs = vec![
        "http://127.0.0.1:5001".to_string(),
        "http://127.0.0.1:5002".to_string(),
        "http://127.0.0.1:5003".to_string(),
    ];

    sleep(Duration::from_secs(3)).await;

    let mut leader_addr = None;
    for addr in &server_addrs {
        if let Ok(mut client) = RaftClient::connect(addr.clone()).await {
            if let Ok(resp) = client.get_state(GetStateRequest {}).await {
                if resp.into_inner().is_leader {
                    leader_addr = Some(addr.clone());
                    break;
                }
            }
        }
    }

    let leader_addr = leader_addr.expect("No leader found");
    println!("Client: leader is {}", leader_addr);

    for i in 0..3 {
        let key = i.to_string();
        let value = (i * 100).to_string();

        let cmd = Command {
            kind: "PUT".to_string(),
            key,
            value,
        };

        let req = SendCommandRequest {
            command: Some(cmd),
        };

        let mut client = RaftClient::connect(leader_addr.clone()).await?;
        let _ = client.send_command(req).await?;

        sleep(Duration::from_secs(1)).await;
    }

    let get_cmd = Command {
        kind: "GET".to_string(),
        key: "1".to_string(),
        value: "".to_string(),
    };

    let req = SendCommandRequest {
        command: Some(get_cmd),
    };

    let mut client = RaftClient::connect(leader_addr.clone()).await?;
    let resp = client.send_command(req).await?;
    let reply = resp.into_inner();

    if !reply.value.is_empty() {
        println!("Client receives value {}", reply.value);
    } else {
        println!("Client GET returned empty value");
    }

    Ok(())
}