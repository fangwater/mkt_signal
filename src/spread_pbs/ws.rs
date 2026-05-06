//! OKex public ws 极简连接 + 重连 + 心跳 + 帧上抛。
//!
//! 不复用 `connection::MktConnection`，避免它的 `Arc<Mutex<WebSocketStream>>`
//! 共享所有权语义；这里 single-thread runtime 下 sink/stream 独占即可。

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};
use url::Url;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

const RECONNECT_BACKOFF_SECS: u64 = 3;
const PING_INTERVAL_SECS: u64 = 20;

/// 启动并维护一条 OKex public ws；ws frame 中的 Text 帧 raw bytes 通过 `frame_tx` 上抛。
///
/// `label` 用作日志前缀（如 `"primary"` / `"secondary"`）。
/// `local_ip` 为空或 `"0.0.0.0"` 时不绑定本地地址。
pub async fn run_okex_public_ws(
    label: &'static str,
    url: String,
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    frame_tx: mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            log::info!("spread_pbs ws[{}] shutdown requested, exiting", label);
            return;
        }

        match connect_and_subscribe(&url, &local_ip, &subscribe_msgs).await {
            Ok((sink, read)) => {
                log::info!("spread_pbs ws[{}] connected to {}", label, url);
                run_session(label, sink, read, &frame_tx, &mut shutdown_rx).await;
                if *shutdown_rx.borrow() {
                    return;
                }
                log::warn!(
                    "spread_pbs ws[{}] disconnected; reconnect in {}s",
                    label, RECONNECT_BACKOFF_SECS
                );
            }
            Err(e) => {
                log::error!(
                    "spread_pbs ws[{}] connect failed: {:#}; retry in {}s",
                    label, e, RECONNECT_BACKOFF_SECS
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

async fn connect_and_subscribe(
    url: &str,
    local_ip: &str,
    subscribe_msgs: &[serde_json::Value],
) -> Result<(WsSink, WsRead)> {
    let stream = open_ws(url, local_ip).await?;
    let (mut sink, read) = stream.split();
    for msg in subscribe_msgs {
        let payload = msg.to_string();
        sink.send(Message::Text(payload))
            .await
            .with_context(|| "send subscribe payload")?;
    }
    Ok((sink, read))
}

async fn open_ws(url: &str, local_ip: &str) -> Result<WsStream> {
    let parsed = Url::parse(url).with_context(|| format!("invalid ws url: {}", url))?;
    let scheme = parsed.scheme().to_string();
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("ws url missing host"))?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow::anyhow!("ws url missing port"))?;

    let local_addr_opt = if local_ip.is_empty() || local_ip == "0.0.0.0" {
        None
    } else {
        Some(local_ip.parse::<IpAddr>().with_context(|| {
            format!("invalid local_ip {}", local_ip)
        })?)
    };

    let tcp = match local_addr_opt {
        Some(local_addr) => connect_tcp_with_local_ip(&host, port, local_addr).await?,
        None => TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("tcp connect to {}:{}", host, port))?,
    };

    let stream = if scheme.eq_ignore_ascii_case("wss") {
        let native = NativeTlsConnector::builder()
            .build()
            .with_context(|| "build native-tls connector")?;
        let connector = TokioTlsConnector::from(native);
        let tls_stream = connector
            .connect(&host, tcp)
            .await
            .with_context(|| "TLS handshake")?;
        let wrapped = MaybeTlsStream::NativeTls(tls_stream);
        let (ws_stream, _resp) = client_async(url, wrapped).await?;
        ws_stream
    } else {
        let plain = MaybeTlsStream::Plain(tcp);
        let (ws_stream, _resp) = client_async(url, plain).await?;
        ws_stream
    };
    Ok(stream)
}

async fn connect_tcp_with_local_ip(host: &str, port: u16, local: IpAddr) -> Result<TcpStream> {
    let mut addrs = lookup_host((host, port))
        .await
        .with_context(|| format!("resolve {}:{}", host, port))?;
    let target = addrs
        .find(|sa| matches!((sa, local), (SocketAddr::V4(_), IpAddr::V4(_)) | (SocketAddr::V6(_), IpAddr::V6(_))))
        .ok_or_else(|| anyhow::anyhow!("no matching address family for {}", host))?;

    let socket = match local {
        IpAddr::V4(ip) => {
            let s = TcpSocket::new_v4()?;
            s.bind((ip, 0).into())
                .with_context(|| format!("bind to {}", ip))?;
            s
        }
        IpAddr::V6(ip) => {
            let s = TcpSocket::new_v6()?;
            s.bind((ip, 0).into())
                .with_context(|| format!("bind to {}", ip))?;
            s
        }
    };
    socket
        .connect(target)
        .await
        .with_context(|| format!("tcp connect to {}", target))
}

async fn run_session(
    label: &'static str,
    mut sink: WsSink,
    mut read: WsRead,
    frame_tx: &mpsc::UnboundedSender<Bytes>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let mut ping_ticker = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    let _ = sink.close().await;
                    return;
                }
            }
            _ = ping_ticker.tick() => {
                if let Err(e) = sink.send(Message::Text("ping".to_string())).await {
                    log::warn!("spread_pbs ws[{}] ping failed: {:#}", label, e);
                    return;
                }
            }
            next = read.next() => {
                match next {
                    Some(Ok(Message::Text(text))) => {
                        // OKex 心跳响应直接丢；其他业务帧 raw bytes 上抛
                        if text == "pong" {
                            continue;
                        }
                        if frame_tx.send(Bytes::from(text)).is_err() {
                            log::info!("spread_pbs ws[{}] frame_tx closed; exit", label);
                            return;
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        if frame_tx.send(Bytes::from(bin)).is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sink.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        log::warn!("spread_pbs ws[{}] close frame: {:?}", label, frame);
                        return;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        log::warn!("spread_pbs ws[{}] read error: {:#}", label, e);
                        return;
                    }
                    None => {
                        log::warn!("spread_pbs ws[{}] stream ended", label);
                        return;
                    }
                }
            }
        }
    }
}
