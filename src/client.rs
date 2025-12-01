use std::collections::HashMap;
use crate::rpc::{RaftServiceClient, RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply};

pub struct RaftClient {
    // Map of peer_id -> peer address
    peer_addresses: HashMap<u64, String>,
}

impl RaftClient {
    pub fn new(peers: Vec<(u64, String)>) -> Self {
        let mut peer_addresses = HashMap::new();

        for (peer_id, address) in peers {
            peer_addresses.insert(peer_id, format!("http://{}", address));
        }

        RaftClient { peer_addresses }
    }

    async fn get_client(&self, peer_id: u64) -> Result<RaftServiceClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
        let address = self.peer_addresses.get(&peer_id)
            .ok_or_else(|| format!("Unknown peer {}", peer_id))?;

        let endpoint = tonic::transport::Endpoint::from_shared(address.clone())?
            .timeout(std::time::Duration::from_millis(500))
            .connect_timeout(std::time::Duration::from_millis(200));

        let channel = endpoint.connect().await?;
        Ok(RaftServiceClient::new(channel))
    }

    pub async fn request_vote(
        &self,
        peer_id: u64,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, Box<dyn std::error::Error>> {
        let mut client = self.get_client(peer_id).await?;
        let request = tonic::Request::new(args);
        let response = client.request_vote(request).await?;
        Ok(response.into_inner())
    }

    pub async fn append_entries(
        &self,
        peer_id: u64,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, Box<dyn std::error::Error>> {
        let mut client = self.get_client(peer_id).await?;
        let request = tonic::Request::new(args);
        let response = client.append_entries(request).await?;
        Ok(response.into_inner())
    }
}
