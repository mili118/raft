use tonic::Request;

pub mod raft {
    tonic::include_proto!("raft"); // same package name
}

use raft::greeter_client::GreeterClient;
use raft::HelloRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the server
    let mut client = GreeterClient::connect("http://[::1]:50051").await?;

    // Build the request
    let request = Request::new(HelloRequest {
        name: "World".into(),
    });

    // Send the RPC
    let response = client.say_hello(request).await?;

    println!("SERVER REPLIED: {:?}", response.into_inner().message);

    Ok(())
}