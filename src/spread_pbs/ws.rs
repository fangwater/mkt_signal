//! OKex/Binance/Bybit/Gate/Bitget 通用 ws 连接 + 重连 + 同步帧处理。
//!
//! single-thread runtime 下被 `spawn_local` 拉起，sink/stream 独占。
//! 帧处理通过 `frame_handler` 闭包**同步**调用——避免 mpsc 转交带来的额外 us 级
//! 延迟（在 colo 场景下这部分占比可观）。Handler 内部读取 `Rc<RefCell<...>>`
//! 共享状态，borrow 区间内不 await，符合单线程模型。

use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_tungstenite_v030::tungstenite::Message;
use tokio_tungstenite_v030::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderName, HeaderValue},
};
use tokio_tungstenite_v030::{client_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::spread_pbs::adapter::{KeepaliveSpec, SubscriptionAckPolicy};
use runtime_common::okex_notice::parse_okex_notice;
use runtime_common::socket_tuning::{tune_tcp_stream, TcpSocketTuning, DEFAULT_WS_BUSY_POLL_US};
use runtime_common::time_util::get_timestamp_us;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

const RECONNECT_BACKOFF_SECS: u64 = 3;
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// 进程内共享的 rustls `ClientConfig`(aws-lc provider + 系统根证书)。
/// 只在首次连接时构建;交易所证书链都是公共 CA,系统根证书与原 native-tls 行为一致。
pub(crate) fn shared_rustls_config() -> Result<Arc<ClientConfig>> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(config.clone());
    }
    let loaded = rustls_native_certs::load_native_certs();
    for err in &loaded.errors {
        log::warn!("spread_pbs rustls native cert load warning: {err}");
    }
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(loaded.certs);
    if added == 0 {
        anyhow::bail!("no usable native root certificates (ignored={ignored})");
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("rustls default protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let config = Arc::new(config);
    let _ = CONFIG.set(config.clone());
    Ok(config)
}

/// 帧处理回调：`(recv_us, payload_bytes)`。`recv_us` 是 `read.next()` 命中那一刻
/// 立即抓的本地微秒时间戳，下游可用作"纯网络延迟"统计的端点。
pub type FrameHandler = Rc<dyn Fn(i64, &[u8]) -> Result<()>>;

#[derive(Clone, Copy, Debug)]
pub struct RollingRestartSpec {
    pub interval: Duration,
    pub first_after: Duration,
}

pub struct WsLoopParams {
    pub label: String,
    pub url: String,
    pub local_ip: String,
    pub remote_ip: Option<String>,
    pub headers: Vec<(String, String)>,
    pub subscribe_msgs: Vec<serde_json::Value>,
    pub keepalive: Option<KeepaliveSpec>,
    pub parse_okex_notices: bool,
    pub business_idle_timeout: Option<Duration>,
    pub rolling_restart: Option<RollingRestartSpec>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionEnd {
    Shutdown,
    Disconnected,
    SubscriptionRejected,
    HandlerRejected,
    BusinessIdle,
    RollingRestart,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SubscriptionKey {
    channel: String,
    coin: String,
}

impl SubscriptionKey {
    fn from_subscription(value: &serde_json::Value) -> Option<Self> {
        Some(Self {
            channel: value.get("type")?.as_str()?.to_string(),
            coin: value.get("coin")?.as_str()?.to_string(),
        })
    }
}

impl std::fmt::Display for SubscriptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.channel, self.coin)
    }
}

#[derive(Clone, Debug)]
struct PendingSubscriptionAcks {
    pending: BTreeMap<SubscriptionKey, usize>,
    count: usize,
}

impl PendingSubscriptionAcks {
    fn from_requests(requests: &[serde_json::Value]) -> std::result::Result<Self, String> {
        let mut pending = BTreeMap::new();
        let mut count = 0usize;
        for (index, request) in requests.iter().enumerate() {
            if request.get("method").and_then(serde_json::Value::as_str) != Some("subscribe") {
                return Err(format!(
                    "request index={index} missing method=subscribe: {request}"
                ));
            }
            let subscription = request
                .get("subscription")
                .ok_or_else(|| format!("request index={index} missing subscription: {request}"))?;
            let key = SubscriptionKey::from_subscription(subscription).ok_or_else(|| {
                format!("request index={index} missing subscription type/coin: {request}")
            })?;
            *pending.entry(key).or_insert(0) += 1;
            count += 1;
        }
        Ok(Self { pending, count })
    }

    fn acknowledge(&mut self, key: &SubscriptionKey) -> bool {
        let Some(remaining) = self.pending.get_mut(key) else {
            return false;
        };
        *remaining -= 1;
        self.count -= 1;
        if *remaining == 0 {
            self.pending.remove(key);
        }
        true
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn summary(&self) -> String {
        self.pending
            .iter()
            .map(|(key, count)| {
                if *count == 1 {
                    key.to_string()
                } else {
                    format!("{key}x{count}")
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Eq, PartialEq)]
enum SubscriptionControlFrame {
    NotControl,
    Acknowledged(SubscriptionKey),
    Rejected(String),
}

/// 一条 ws 的连接 + 自动重连主循环。每个业务帧同步调 `handler`。
pub async fn run_public_ws(
    params: WsLoopParams,
    handler: FrameHandler,
    shutdown_rx: watch::Receiver<bool>,
) {
    run_public_ws_with_ack_policy(params, handler, shutdown_rx, SubscriptionAckPolicy::None).await;
}

pub async fn run_public_ws_with_ack_policy(
    params: WsLoopParams,
    handler: FrameHandler,
    mut shutdown_rx: watch::Receiver<bool>,
    subscription_ack_policy: SubscriptionAckPolicy,
) {
    let WsLoopParams {
        label,
        url,
        local_ip,
        remote_ip,
        headers,
        subscribe_msgs,
        keepalive,
        parse_okex_notices,
        business_idle_timeout,
        rolling_restart,
    } = params;
    let expected_subscription_acks = match subscription_ack_policy {
        SubscriptionAckPolicy::None => None,
        SubscriptionAckPolicy::HyperliquidTypeAndCoin => {
            match PendingSubscriptionAcks::from_requests(&subscribe_msgs) {
                Ok(expected) => Some(expected),
                Err(err) => {
                    log::error!(
                        "spread_pbs ws[{}] invalid acknowledged subscription set: {}",
                        label,
                        err
                    );
                    return;
                }
            }
        }
    };
    let mut rolling_deadline = rolling_restart.map(|spec| Instant::now() + spec.first_after);

    loop {
        if *shutdown_rx.borrow() {
            log::info!("spread_pbs ws[{}] shutdown requested, exiting", label);
            return;
        }

        match connect_and_subscribe(
            &url,
            &local_ip,
            remote_ip.as_deref(),
            &headers,
            &subscribe_msgs,
        )
        .await
        {
            Ok((sink, read)) => {
                log::info!(
                    "spread_pbs ws[{}] connected to {} remote_ip={}",
                    label,
                    url,
                    remote_ip.as_deref().unwrap_or("dns")
                );
                let end = run_session(
                    &label,
                    sink,
                    read,
                    &handler,
                    &mut shutdown_rx,
                    keepalive.as_ref(),
                    parse_okex_notices,
                    business_idle_timeout,
                    rolling_deadline,
                    expected_subscription_acks.clone(),
                )
                .await;
                if end == SessionEnd::Shutdown || *shutdown_rx.borrow() {
                    return;
                }
                if end == SessionEnd::RollingRestart {
                    if let (Some(spec), Some(deadline)) = (rolling_restart, rolling_deadline) {
                        let now = Instant::now();
                        let mut next = deadline + spec.interval;
                        while next <= now {
                            next += spec.interval;
                        }
                        rolling_deadline = Some(next);
                    }
                    log::info!("spread_pbs ws[{}] rolling reconnect", label);
                    continue;
                }
                log::warn!(
                    "spread_pbs ws[{}] session ended reason={:?}; reconnect in {}s",
                    label,
                    end,
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
    remote_ip: Option<&str>,
    headers: &[(String, String)],
    subscribe_msgs: &[serde_json::Value],
) -> Result<(WsSink, WsRead)> {
    let stream = open_ws(url, local_ip, remote_ip, headers).await?;
    let (mut sink, read) = stream.split();
    for msg in subscribe_msgs {
        let payload = msg.to_string();
        sink.send(Message::Text(payload.into()))
            .await
            .with_context(|| "send subscribe payload")?;
    }
    Ok((sink, read))
}

async fn open_ws(
    url: &str,
    local_ip: &str,
    remote_ip: Option<&str>,
    headers: &[(String, String)],
) -> Result<WsStream> {
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
    let remote_addr_opt = remote_ip
        .map(str::trim)
        .filter(|ip| !ip.is_empty())
        .map(|ip| {
            ip.parse::<IpAddr>()
                .with_context(|| format!("invalid remote_ip {}", ip))
        })
        .transpose()?;

    let tcp = if let Some(local_addr) = local_addr_opt {
        connect_tcp_with_local_ip(&host, port, local_addr, remote_addr_opt).await?
    } else if let Some(remote_addr) = remote_addr_opt {
        TcpStream::connect(SocketAddr::new(remote_addr, port))
            .await
            .with_context(|| format!("tcp connect to {}:{}", remote_addr, port))?
    } else {
        TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("tcp connect to {}:{}", host, port))?
    };
    tune_tcp_stream(
        &tcp,
        "spread_pbs ws",
        TcpSocketTuning {
            busy_poll_us: Some(DEFAULT_WS_BUSY_POLL_US),
            ..TcpSocketTuning::default()
        },
    );

    // tungstenite 0.21+ 移除了 url::Url 的 IntoClientRequest 实现,直接用原始字符串。
    let mut request = url.into_client_request()?;
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid ws header name: {}", name))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid ws header value for {}", name))?;
        request.headers_mut().insert(name, value);
    }

    let stream = if scheme.eq_ignore_ascii_case("wss") {
        let connector = TlsConnector::from(shared_rustls_config()?);
        let server_name = ServerName::try_from(host.clone())
            .with_context(|| format!("invalid TLS server name {}", host))?;
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .with_context(|| "TLS handshake")?;
        let wrapped = MaybeTlsStream::Rustls(tls_stream);
        let (ws_stream, _resp) = client_async(request, wrapped).await?;
        ws_stream
    } else {
        let plain = MaybeTlsStream::Plain(tcp);
        let (ws_stream, _resp) = client_async(request, plain).await?;
        ws_stream
    };
    Ok(stream)
}

async fn connect_tcp_with_local_ip(
    host: &str,
    port: u16,
    local: IpAddr,
    remote: Option<IpAddr>,
) -> Result<TcpStream> {
    let target = if let Some(remote) = remote {
        match (remote, local) {
            (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_)) => {
                SocketAddr::new(remote, port)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "remote_ip {} is not compatible with local_ip {}",
                    remote,
                    local
                ));
            }
        }
    } else {
        let mut addrs = lookup_host((host, port))
            .await
            .with_context(|| format!("resolve {}:{}", host, port))?;
        addrs
            .find(|sa| {
                matches!(
                    (sa, local),
                    (SocketAddr::V4(_), IpAddr::V4(_)) | (SocketAddr::V6(_), IpAddr::V6(_))
                )
            })
            .ok_or_else(|| anyhow::anyhow!("no matching address family for {}", host))?
    };

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
    label: &str,
    mut sink: WsSink,
    mut read: WsRead,
    handler: &FrameHandler,
    shutdown_rx: &mut watch::Receiver<bool>,
    keepalive: Option<&KeepaliveSpec>,
    parse_okex_notices: bool,
    business_idle_timeout: Option<Duration>,
    rolling_deadline: Option<Instant>,
    mut pending_subscription_acks: Option<PendingSubscriptionAcks>,
) -> SessionEnd {
    let interval = keepalive
        .map(|k| k.interval)
        .unwrap_or(Duration::from_secs(60));
    let mut keepalive_ticker = tokio::time::interval(interval);
    keepalive_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // 第一次 tick 立刻就触发，跳过它以免连上来就发心跳干扰订阅。
    keepalive_ticker.tick().await;
    let mut business_idle_deadline = business_idle_timeout.map(|timeout| Instant::now() + timeout);
    let mut subscription_ack_deadline = pending_subscription_acks
        .as_ref()
        .filter(|pending| !pending.is_empty())
        .map(|_| Instant::now() + SUBSCRIPTION_ACK_TIMEOUT);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    let _ = sink.close().await;
                    return SessionEnd::Shutdown;
                }
            }
            _ = async {
                match subscription_ack_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let summary = pending_subscription_acks
                    .as_ref()
                    .map(PendingSubscriptionAcks::summary)
                    .unwrap_or_default();
                log::error!(
                    "spread_pbs ws[{}] subscription ack timeout after {}ms missing={}",
                    label,
                    SUBSCRIPTION_ACK_TIMEOUT.as_millis(),
                    summary,
                );
                let _ = sink.close().await;
                return SessionEnd::SubscriptionRejected;
            }
            _ = async {
                match business_idle_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                log::error!(
                    "spread_pbs ws[{}] business frame idle for {}ms; reconnecting",
                    label,
                    business_idle_timeout.unwrap_or_default().as_millis(),
                );
                let _ = sink.close().await;
                return SessionEnd::BusinessIdle;
            }
            _ = async {
                match rolling_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let _ = sink.close().await;
                return SessionEnd::RollingRestart;
            }
            _ = keepalive_ticker.tick(), if keepalive.is_some() => {
                let payload = (keepalive.unwrap().build)();
                if let Err(e) = sink.send(payload).await {
                    log::warn!("spread_pbs ws[{}] keepalive failed: {:#}", label, e);
                    return SessionEnd::Disconnected;
                }
            }
            next = read.next() => {
                // recv_us 必须在 await 落地后立刻抓——这是"纯网络延迟"统计的本地端点
                let recv_us = get_timestamp_us();
                match next {
                    Some(Ok(Message::Text(text))) => {
                        if parse_okex_notices {
                            if let Some(notice) = parse_okex_notice(&text) {
                                log::warn!(
                                    "spread_pbs ws[{}] received OKX notice code={} msg={} conn_id={:?}",
                                    label,
                                    notice.code,
                                    notice.msg,
                                    notice.conn_id
                                );
                                if notice.is_service_upgrade() {
                                    log::warn!(
                                        "spread_pbs ws[{}] OKX service upgrade notice 64008 received; reconnecting before forced close",
                                        label
                                    );
                                    let _ = sink.close().await;
                                    return SessionEnd::Disconnected;
                                }
                                continue;
                            }
                        }
                        match parse_subscription_control_frame(
                            &text,
                            pending_subscription_acks.is_some(),
                        ) {
                            SubscriptionControlFrame::NotControl => {}
                            SubscriptionControlFrame::Acknowledged(key) => {
                                let acknowledged = pending_subscription_acks
                                    .as_mut()
                                    .map(|pending| pending.acknowledge(&key))
                                    .unwrap_or(false);
                                if !acknowledged {
                                    log::warn!(
                                        "spread_pbs ws[{}] unexpected or duplicate subscription ack key={}",
                                        label,
                                        key,
                                    );
                                } else if pending_subscription_acks
                                    .as_ref()
                                    .is_some_and(PendingSubscriptionAcks::is_empty)
                                {
                                    subscription_ack_deadline = None;
                                    log::info!(
                                        "spread_pbs ws[{}] all subscription acks received",
                                        label,
                                    );
                                }
                                continue;
                            }
                            SubscriptionControlFrame::Rejected(reason) => {
                                log::error!(
                                    "spread_pbs ws[{}] subscription rejected: {}",
                                    label,
                                    reason,
                                );
                                let _ = sink.close().await;
                                return SessionEnd::SubscriptionRejected;
                            }
                        }
                        if is_keepalive_response(&text) {
                            continue;
                        }
                        business_idle_deadline =
                            business_idle_timeout.map(|timeout| Instant::now() + timeout);
                        if let Err(err) = handler(recv_us, text.as_bytes()) {
                            log::error!(
                                "spread_pbs ws[{}] frame handler rejected payload: {err:#}; reconnecting",
                                label
                            );
                            let _ = sink.close().await;
                            return SessionEnd::HandlerRejected;
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        business_idle_deadline =
                            business_idle_timeout.map(|timeout| Instant::now() + timeout);
                        if let Err(err) = handler(recv_us, bin.as_ref()) {
                            log::error!(
                                "spread_pbs ws[{}] binary frame handler rejected payload: {err:#}; reconnecting",
                                label
                            );
                            let _ = sink.close().await;
                            return SessionEnd::HandlerRejected;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = sink.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        log::warn!("spread_pbs ws[{}] close frame: {:?}", label, frame);
                        return SessionEnd::Disconnected;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        log::warn!("spread_pbs ws[{}] read error: {:#}", label, e);
                        return SessionEnd::Disconnected;
                    }
                    None => {
                        log::warn!("spread_pbs ws[{}] stream ended", label);
                        return SessionEnd::Disconnected;
                    }
                }
            }
        }
    }
}

fn parse_subscription_control_frame(text: &str, enabled: bool) -> SubscriptionControlFrame {
    if !enabled {
        return SubscriptionControlFrame::NotControl;
    }
    let trimmed = text.trim_start();
    if !trimmed.starts_with('{') {
        return SubscriptionControlFrame::NotControl;
    }
    let head = &trimmed[..trimmed.len().min(256)];
    if !head.contains("subscriptionResponse")
        && !(head.contains("\"channel\"") && head.contains("\"error\""))
    {
        return SubscriptionControlFrame::NotControl;
    }

    let value = match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(value) => value,
        Err(err) => {
            return SubscriptionControlFrame::Rejected(format!(
                "malformed subscription control frame: {err}"
            ));
        }
    };
    match value.get("channel").and_then(serde_json::Value::as_str) {
        Some("error") => {
            let reason = value
                .get("data")
                .map(|data| {
                    data.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| data.to_string())
                })
                .unwrap_or_else(|| "missing error data".to_string());
            SubscriptionControlFrame::Rejected(reason)
        }
        Some("subscriptionResponse") => {
            let data = match value.get("data") {
                Some(data) => data,
                None => {
                    return SubscriptionControlFrame::Rejected(
                        "subscriptionResponse missing data".to_string(),
                    );
                }
            };
            if data.get("method").and_then(serde_json::Value::as_str) != Some("subscribe") {
                return SubscriptionControlFrame::Rejected(
                    "subscriptionResponse missing method=subscribe".to_string(),
                );
            }
            let key = data
                .get("subscription")
                .and_then(SubscriptionKey::from_subscription);
            match key {
                Some(key) => SubscriptionControlFrame::Acknowledged(key),
                None => SubscriptionControlFrame::Rejected(
                    "subscriptionResponse missing subscription type/coin".to_string(),
                ),
            }
        }
        _ => SubscriptionControlFrame::NotControl,
    }
}

/// 各家服务端 pong/事件 ack 的轻量识别——避免 parser 对它们报 error。
fn is_keepalive_response(text: &str) -> bool {
    let trimmed = text.trim_start();
    if trimmed == "pong" || trimmed == "\"pong\"" {
        return true;
    }
    if !trimmed.starts_with('{') {
        return false;
    }
    let head = &trimmed[..trimmed.len().min(128)];
    if !(head.contains("\"event\"")
        || head.contains("\"op\"")
        || head.contains("\"channel\"")
        || head.contains("\"success\"")
        || head.contains("\"result\""))
    {
        return false;
    }
    // Bybit `{"op":"pong",...}` / `{"success":true,"op":"ping",...}`；
    // Gate `{"channel":"...pong",...}` / `{"event":"subscribe","result":...}`；
    // Binance / Bitget 订阅 ack 也走这里跳过。
    if trimmed.contains("\"pong\"")
        || trimmed.contains("\"op\":\"ping\"")
        || trimmed.contains("\"event\":\"subscribe\"")
        || trimmed.contains("\"event\":\"unsubscribe\"")
        || trimmed.starts_with("{\"result\":")
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        is_keepalive_response, parse_subscription_control_frame, PendingSubscriptionAcks,
        SubscriptionControlFrame, SubscriptionKey,
    };

    #[test]
    fn keepalive_filter_does_not_drop_binance_market_data() {
        let trade = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1710000000000,"s":"BTCUSDT","t":1,"p":"68000.1","q":"0.02","T":1710000000000,"m":true}}"#;
        let book_ticker = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":123,"s":"BTCUSDT","b":"68000.1","B":"1.2","a":"68000.2","A":"0.8"}}"#;

        assert!(!is_keepalive_response(trade));
        assert!(!is_keepalive_response(book_ticker));
    }

    #[test]
    fn keepalive_filter_skips_control_frames() {
        assert!(is_keepalive_response("pong"));
        assert!(is_keepalive_response(r#""pong""#));
        assert!(is_keepalive_response(r#" {"result":null,"id":1}"#));
        assert!(is_keepalive_response(r#"{"op":"pong","req_id":"x"}"#));
        assert!(is_keepalive_response(r#"{"success":true,"op":"ping"}"#));
        assert!(is_keepalive_response(
            r#"{"event":"subscribe","result":{"channel":"futures.book_ticker"}}"#
        ));
    }

    #[test]
    fn hyperliquid_acks_match_type_and_coin_not_exact_json() {
        let requests = vec![
            serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": "HYPE"}
            }),
            serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": "HYPE"}
            }),
        ];
        let mut pending = PendingSubscriptionAcks::from_requests(&requests).unwrap();

        let l2_ack = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"HYPE","nSigFigs":null,"mantissa":null,"fast":false}}}"#;
        let SubscriptionControlFrame::Acknowledged(l2_key) =
            parse_subscription_control_frame(l2_ack, true)
        else {
            panic!("expected l2 subscription ack");
        };
        assert!(pending.acknowledge(&l2_key));
        assert_eq!(pending.summary(), "trades:HYPE");

        let trades_ack = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"coin":"HYPE","type":"trades"}}}"#;
        let SubscriptionControlFrame::Acknowledged(trades_key) =
            parse_subscription_control_frame(trades_ack, true)
        else {
            panic!("expected trades subscription ack");
        };
        assert!(pending.acknowledge(&trades_key));
        assert!(pending.is_empty());
    }

    #[test]
    fn hyperliquid_channel_error_rejects_session_even_with_pending_market_data() {
        let error =
            r#"{"channel":"error","data":"Error parsing JSON into valid websocket request"}"#;
        assert_eq!(
            parse_subscription_control_frame(error, true),
            SubscriptionControlFrame::Rejected(
                "Error parsing JSON into valid websocket request".to_string()
            )
        );

        let book = r#"{"channel":"l2Book","data":{"coin":"HYPE","time":1,"levels":[[],[]]}}"#;
        assert_eq!(
            parse_subscription_control_frame(book, true),
            SubscriptionControlFrame::NotControl
        );
    }

    #[test]
    fn hyperliquid_ack_tracker_preserves_duplicate_request_counts() {
        let request = serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": "@107"}
        });
        let mut pending =
            PendingSubscriptionAcks::from_requests(&[request.clone(), request]).unwrap();
        let key = SubscriptionKey {
            channel: "bbo".to_string(),
            coin: "@107".to_string(),
        };

        assert_eq!(pending.summary(), "bbo:@107x2");
        assert!(pending.acknowledge(&key));
        assert_eq!(pending.summary(), "bbo:@107");
        assert!(pending.acknowledge(&key));
        assert!(pending.is_empty());
        assert!(!pending.acknowledge(&key));
    }
}
