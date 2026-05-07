//! OKex/Binance/Bybit/Gate/Bitget 通用 ws 连接 + 重连 + 帧上抛。
//!
//! single-thread runtime 下被 `spawn_local` 拉起，sink/stream 独占。
//! per-venue 的心跳格式由 [`crate::spread_pbs::adapter::KeepaliveSpec`] 决定，
//! `keepalive=None` 时仅依赖服务端 ws-Ping/Pong。

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::spread_pbs::adapter::KeepaliveSpec;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

const RECONNECT_BACKOFF_SECS: u64 = 3;

pub struct WsLoopParams {
    pub label: &'static str,
    pub url: String,
    pub local_ip: String,
    pub subscribe_msgs: Vec<serde_json::Value>,
    pub keepalive: Option<KeepaliveSpec>,
}

/// 一条 ws 的连接 + 自动重连主循环。Text/Binary 帧 raw bytes 通过 `frame_tx` 上抛。
pub async fn run_public_ws(
    params: WsLoopParams,
    frame_tx: mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let WsLoopParams {
        label,
        url,
        local_ip,
        subscribe_msgs,
        keepalive,
    } = params;

    loop {
        if *shutdown_rx.borrow() {
            log::info!("spread_pbs ws[{}] shutdown requested, exiting", label);
            return;
        }

        match connect_and_subscribe(&url, &local_ip, &subscribe_msgs).await {
            Ok((sink, read)) => {
                log::info!("spread_pbs ws[{}] connected to {}", label, url);
                run_session(label, sink, read, &frame_tx, &mut shutdown_rx, keepalive.as_ref())
                    .await;
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
    keepalive: Option<&KeepaliveSpec>,
) {
    let interval = keepalive
        .map(|k| k.interval)
        .unwrap_or(Duration::from_secs(60));
    let mut keepalive_ticker = tokio::time::interval(interval);
    keepalive_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // 第一次 tick 立刻就触发，跳过它以免连上来就发心跳干扰订阅。
    keepalive_ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    let _ = sink.close().await;
                    return;
                }
            }
            _ = keepalive_ticker.tick(), if keepalive.is_some() => {
                let payload = (keepalive.unwrap().build)();
                if let Err(e) = sink.send(payload).await {
                    log::warn!("spread_pbs ws[{}] keepalive failed: {:#}", label, e);
                    return;
                }
            }
            next = read.next() => {
                match next {
                    Some(Ok(Message::Text(text))) => {
                        // 应用层 pong（OKex/Bitget 是 "pong"，Bybit/Gate 是 JSON）一律不上抛。
                        if is_keepalive_response(&text) {
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

/// 各家服务端 pong/事件 ack 的轻量识别——避免 parser 对它们报 error。
fn is_keepalive_response(text: &str) -> bool {
    let trimmed = text.trim_start();
    if trimmed == "pong" || trimmed == "\"pong\"" {
        return true;
    }
    // Bybit `{"op":"pong",...}` / `{"success":true,"op":"ping",...}`；
    // Gate `{"channel":"...pong",...}` / `{"event":"subscribe","result":...}`；
    // Binance / Bitget 订阅 ack 也走这里跳过。
    if trimmed.contains("\"pong\"")
        || trimmed.contains("\"op\":\"ping\"")
        || trimmed.contains("\"event\":\"subscribe\"")
        || trimmed.contains("\"event\":\"unsubscribe\"")
    {
        return true;
    }
    false
}
