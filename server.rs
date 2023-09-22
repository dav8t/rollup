use futures::{future, prelude::*};
use tarpc::{
    context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};

use tokio::sync::mpsc;

use std::net::{IpAddr, SocketAddr};

#[derive(Clone)]
struct PamphletServer(SocketAddr, mpsc::Sender<SignedTx>);

#[tarpc::server]
impl PamphletRPC for PamphletServer {
    async fn submit_transaction(
        self,
        _: context::Context,
        tx: pamphlet_api::SignedTx,
    ) -> Result<(), String> {
        self.1.send(tx.clone()).await.unwrap();
        Ok(())
    }
}

pub async fn run_server(sx: mpsc::Sender<SignedTx>, addr: String, port: u16) -> anyhow::Result<()> {
    let mut listener = tarpc::serde_transport::tcp::listen(
        &(IpAddr::V4(addr.parse().unwrap()), port),
        Json::default,
    )
    .await?;
    println!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| {
            let server = PamphletServer(channel.transport().peer_addr().unwrap(), sx.clone());
            channel.execute(server.serve())
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
