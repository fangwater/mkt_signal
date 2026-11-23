use super::{
    wire::{position_targets_client::PositionTargetsClient, PositionTarget, TargetReceipt},
    Target,
};
use anyhow::{ensure, Context, Result};
use hyper_util::rt::TokioIo;
use std::{path::PathBuf, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Channel, ClientTlsConfig, Endpoint},
    Streaming,
};

/// One ordered request and receipt sequence on a persistent gRPC stream.
pub struct Client {
    requests: mpsc::Sender<PositionTarget>,
    receipts: Streaming<TargetReceipt>,
    usable: bool,
}
impl Client {
    pub async fn connect_ipc(path: PathBuf) -> Result<Self> {
        ensure!(
            path.is_absolute(),
            "target IPC socket path must be absolute"
        );
        let channel = Endpoint::from_static("http://exec-target.local")
            .connect_timeout(Duration::from_secs(3))
            .connect_with_connector(tower::service_fn(move |_| {
                let path = path.clone();
                async move {
                    tokio::net::UnixStream::connect(path)
                        .await
                        .map(TokioIo::new)
                }
            }))
            .await?;
        Self::open(channel).await
    }

    pub async fn connect_grpc(url: String, tls: ClientTlsConfig) -> Result<Self> {
        let endpoint = Endpoint::from_shared(url)?;
        ensure!(
            endpoint.uri().scheme_str() == Some("https"),
            "remote target gRPC requires HTTPS and mTLS"
        );
        let channel = endpoint
            .tls_config(tls)?
            .connect_timeout(Duration::from_secs(3))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .connect()
            .await?;
        Self::open(channel).await
    }

    async fn open(channel: Channel) -> Result<Self> {
        let (requests, receiver) = mpsc::channel(1);
        let receipts = tokio::time::timeout(
            Duration::from_secs(3),
            PositionTargetsClient::new(channel).push(ReceiverStream::new(receiver)),
        )
        .await
        .context("open target stream timed out")??
        .into_inner();
        Ok(Self {
            requests,
            receipts,
            usable: true,
        })
    }

    /// Send the exact immutable target. A timeout gives no exchange outcome;
    /// reconnect and retry the same batch.
    pub async fn send(&mut self, target: Target, decision_us: u64) -> Result<TargetReceipt> {
        ensure!(
            self.usable,
            "target stream outcome is unknown; reconnect before retry"
        );
        self.usable = false;
        let identity = (
            target.source_id.clone(),
            target.batch_id.clone(),
            target.target_revision,
        );
        self.requests
            .try_send(target.into_wire(decision_us))
            .context("queue target on stream")?;
        let receipt = tokio::time::timeout(Duration::from_secs(2), self.receipts.message())
            .await
            .context("target reply timed out")??
            .context("target stream closed")?;
        ensure!(
            (
                receipt.source_id.as_str(),
                receipt.batch_id.as_str(),
                receipt.target_revision
            ) == (identity.0.as_str(), identity.1.as_str(), identity.2),
            "unexpected target receipt identity"
        );
        self.usable = true;
        Ok(receipt)
    }
}
