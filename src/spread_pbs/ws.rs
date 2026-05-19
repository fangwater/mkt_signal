//! OKex/Binance/Bybit/Gate/Bitget 通用 ws 连接 + 重连 + 同步帧处理。
//!
//! single-thread runtime 下被 `spawn_local` 拉起，sink/stream 独占。
//! 帧处理通过 `frame_handler` 闭包**同步**调用——避免 mpsc 转交带来的额外 us 级
//! 延迟（在 colo 场景下这部分占比可观）。Handler 内部读取 `Rc<RefCell<...>>`
//! 共享状态，borrow 区间内不 await，符合单线程模型。

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::watch;
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderName, HeaderValue},
};
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::common::time_util::get_timestamp_us;
use crate::spread_pbs::adapter::KeepaliveSpec;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

const RECONNECT_BACKOFF_SECS: u64 = 3;

/// 帧处理回调：`(recv_us, payload_bytes)`。`recv_us` 是 `read.next()` 命中那一刻
/// 立即抓的本地微秒时间戳，下游可用作"纯网络延迟"统计的端点。
pub type FrameHandler = Rc<dyn Fn(i64, &[u8])>;

pub struct WsLoopParams {
    pub label: &'static str,
    pub url: String,
    pub local_ip: String,
    pub headers: Vec<(String, String)>,
    pub subscribe_msgs: Vec<serde_json::Value>,
    pub keepalive: Option<KeepaliveSpec>,
}

/// 一条 ws 的连接 + 自动重连主循环。每个业务帧同步调 `handler`。
pub async fn run_public_ws(
    params: WsLoopParams,
    handler: FrameHandler,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let WsLoopParams {
        label,
        url,
        local_ip,
        headers,
        subscribe_msgs,
        keepalive,
    } = params;

    loop {
        if *shutdown_rx.borrow() {
            log::info!("spread_pbs ws[{}] shutdown requested, exiting", label);
            return;
        }

        match connect_and_subscribe(&url, &local_ip, &headers, &subscribe_msgs).await {
            Ok((sink, read)) => {
                log::info!("spread_pbs ws[{}] connected to {}", label, url);
                run_session(
                    label,
                    sink,
                    read,
                    &handler,
                    &mut shutdown_rx,
                    keepalive.as_ref(),
                )
                .await;
                if *shutdown_rx.borrow() {
                    return;
                }
                log::warn!(
                    "spread_pbs ws[{}] disconnected; reconnect in {}s",
                    label,
                    RECONNECT_BACKOFF_SECS
                );
            }
            Err(e) => {
                log::error!(
                    "spread_pbs ws[{}] connect failed: {:#}; retry in {}s",
                    label,
                    e,
                    RECONNECT_BACKOFF_SECS
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

async fn connect_and_subscribe(
    url: &str,
    local_ip: &str,
    headers: &[(String, String)],
    subscribe_msgs: &[serde_json::Value],
) -> Result<(WsSink, WsRead)> {
    let stream = open_ws(url, local_ip, headers).await?;
    let (mut sink, read) = stream.split();
    for msg in subscribe_msgs {
        let payload = msg.to_string();
        sink.send(Message::Text(payload))
            .await
            .with_context(|| "send subscribe payload")?;
    }
    Ok((sink, read))
}

async fn open_ws(url: &str, local_ip: &str, headers: &[(String, String)]) -> Result<WsStream> {
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
        Some(
            local_ip
                .parse::<IpAddr>()
                .with_context(|| format!("invalid local_ip {}", local_ip))?,
        )
    };

    let tcp = match local_addr_opt {
        Some(local_addr) => connect_tcp_with_local_ip(&host, port, local_addr).await?,
        None => TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("tcp connect to {}:{}", host, port))?,
    };
    // 关闭 Nagle 减少 ws 数据帧的合并/延迟（colo 场景关键）。
    let _ = tcp.set_nodelay(true);

    let mut request = parsed.clone().into_client_request()?;
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid ws header name: {}", name))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid ws header value for {}", name))?;
        request.headers_mut().insert(name, value);
    }

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
        let (ws_stream, _resp) = client_async(request, wrapped).await?;
        ws_stream
    } else {
        let plain = MaybeTlsStream::Plain(tcp);
        let (ws_stream, _resp) = client_async(request, plain).await?;
        ws_stream
    };
    Ok(stream)
}

async fn connect_tcp_with_local_ip(host: &str, port: u16, local: IpAddr) -> Result<TcpStream> {
    let mut addrs = lookup_host((host, port))
        .await
        .with_context(|| format!("resolve {}:{}", host, port))?;
    let target = addrs
        .find(|sa| {
            matches!(
                (sa, local),
                (SocketAddr::V4(_), IpAddr::V4(_)) | (SocketAddr::V6(_), IpAddr::V6(_))
            )
        })
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
    handler: &FrameHandler,
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
                // recv_us 必须在 await 落地后立刻抓——这是"纯网络延迟"统计的本地端点
                let recv_us = get_timestamp_us();
                match next {
                    Some(Ok(Message::Text(text))) => {
                        if is_keepalive_response(&text) {
                            continue;
                        }
                        let bytes = Bytes::from(text);
                        handler(recv_us, &bytes);
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        let bytes = Bytes::from(bin);
                        handler(recv_us, &bytes);
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
