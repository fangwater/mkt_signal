use tokio::sync::{broadcast, mpsc};
use tokio::net::TcpStream;
use tokio::{Duration, Instant};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use anyhow::Result;
use crate::args::Args;
use serde_json::Value;

#[async_trait::async_trait]
pub trait WebSocketHandler: Send + Sync {
    async fn start_ws(&self) -> Result<()>;
    async fn stop_ws(&self) -> Result<()>;
}
#[async_trait::async_trait]
pub trait UnixDomainSocketHandler: Send + Sync {
    async fn get_socket_path(&self) -> PathBuf;
    async fn release_socket(&self) -> Result<()>;
}

struct MktProxy {
    args: Args, // 参数
    //每个batch构建一个websocket连接，放在vector中
    ws_handlers: Vec<Box<dyn WebSocketHandler>>, 
    //两个unix domain socket连接，一个用于trade，一个用于inc
    trade_socket_handler: Box<dyn UnixDomainSocketHandler>,
    inc_socket_handler: Box<dyn UnixDomainSocketHandler>,
}




trait ReleaseUnixDomainSocket {
    fn release_socket(socket_path: &Path) -> Result<()> {
        std::fs::remove_file(socket_path).map_err(|e| anyhow::anyhow!("Failed to remove socket file: {}", e));
        Ok(())
    }
}


