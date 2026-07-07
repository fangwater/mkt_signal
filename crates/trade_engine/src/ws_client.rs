use crate::binance_ws::{self, BinanceWsSigner, BINANCE_ED25519_API_KEY_ENV};
use crate::bitget_ws;
use crate::bybit;
use crate::bybit::BybitWsOrderResponse;
use crate::config::LimitConstants;
use crate::engine::{
    internal_open_terminated_outcome, record_internal_open_terminate_summary,
    take_internal_open_terminate, InternalOpenTerminateMap, InternalOpenTerminateSummary,
};
use crate::gate_ws;
use crate::ltp_ws::{self, LtpCredentials};
use crate::okex;
use crate::okex::{OkexCancelOrderParams, OkexNewOrderParams, OkexWsOrderResponse};
use crate::query_parsers::binance_margin_order::parse_binance_margin_order_query_json;
use crate::query_parsers::binance_um_order::parse_binance_um_order_query_json;
use crate::query_parsers::compact_order::ORDER_QUERY_NOT_FOUND_MARKER;
use crate::query_parsers::gate_order_status::{
    parse_gate_futures_order_status_json, parse_gate_spot_order_status_json,
};
use crate::query_request::{QueryRequestMsg, QueryRequestType};
use crate::query_response_handle::QueryExecOutcome;
use crate::response_sink::{QueryResponseSink, TradeResponseSink};
use crate::tcp_loss_health::{TcpHealthMode, TcpLossHealth, TcpLossHealthConfig, Verdict};
use crate::trade_request::{
    BinanceNewOrderParamsRef, BitgetNewOrderParamsRef, GateNewOrderParamsRef, TradeRequestMsg,
    TradeRequestType,
};
use crate::trade_response_handle::TradeExecOutcome;
use account_common::bitget_auth::BitgetCredentials;
use account_common::bybit_auth::BybitCredentials;
use account_common::gate_auth::GateCredentials;
use account_common::okex_auth::OkexCredentials;
use account_common::ApiKey;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use native_tls::TlsConnector;
use rolling_common::latency_kll::LatencyKll;
use rolling_common::latency_snapshot::{
    LatencyBucketStat, LatencySnapshotMsg, ACTION_ID_CANCEL, ACTION_ID_NEW, METRIC_ID_DOWNLINK,
    METRIC_ID_IPC_TO_WS, METRIC_ID_RTT, METRIC_ID_SERVER, METRIC_ID_UPLINK,
};
use runtime_common::exchange::Exchange;
use runtime_common::fast_hash::{fast_hash_map, fast_hash_set, FastHashMap, FastHashSet};
use runtime_common::socket_tuning::{
    read_tcp_retrans_snapshot, tune_tcp_stream, TcpRetransSnapshot, TcpSocketTuning,
    DEFAULT_WS_BUSY_POLL_US,
};
use runtime_common::time_util::get_timestamp_us;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{cell::RefCell, rc::Rc};
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::Notify;
use tokio::time;
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::{
    client_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
        protocol::{frame::coding::CloseCode, CloseFrame},
        Error as WsError, Message,
    },
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use url::Url;

fn extract_okex_login_timestamp(payload: &str) -> Option<String> {
    let v = serde_json::from_str::<Value>(payload).ok()?;
    v.get("args")?
        .get(0)?
        .get("timestamp")?
        .as_str()
        .map(|s| s.to_string())
}

fn is_bybit_pong_response(payload: &str) -> bool {
    let Ok(v) = serde_json::from_str::<Value>(payload) else {
        return false;
    };
    matches!(
        v.get("op").and_then(|x| x.as_str()),
        Some("pong") | Some("ping")
    ) || matches!(v.get("ret_msg").and_then(|x| x.as_str()), Some("pong"))
}

fn parse_bybit_auth_response(payload: &str) -> Option<bool> {
    let Ok(v) = serde_json::from_str::<Value>(payload) else {
        return None;
    };
    if v.get("op").and_then(|x| x.as_str()) != Some("auth") {
        return None;
    }

    if let Some(success) = v.get("success").and_then(|x| x.as_bool()) {
        return Some(success);
    }

    v.get("retCode")
        .and_then(|x| x.as_i64())
        .map(|code| code == 0)
}

fn is_bitget_pong_response(payload: &str) -> bool {
    payload.trim().eq_ignore_ascii_case("pong")
}

fn binance_ed25519_api_key_from_env() -> Result<String> {
    std::env::var(BINANCE_ED25519_API_KEY_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Binance Ed25519 session.logon requires {}",
                BINANCE_ED25519_API_KEY_ENV
            )
        })
}

fn bitget_ws_code_is_success(v: Option<&Value>) -> bool {
    match v {
        Some(Value::String(s)) => s == "0" || s == "00000",
        Some(Value::Number(n)) => n.as_i64() == Some(0),
        _ => false,
    }
}

fn parse_bitget_control_event(payload: &str) -> Option<(String, String, String, bool)> {
    let Ok(v) = serde_json::from_str::<Value>(payload) else {
        return None;
    };
    let event = v.get("event")?.as_str()?.trim();
    if event.eq_ignore_ascii_case("trade") {
        return None;
    }
    let code = match v.get("code") {
        Some(Value::String(s)) => s.trim().to_string(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    };
    let msg = v
        .get("msg")
        .and_then(|x| x.as_str())
        .or_else(|| v.get("message").and_then(|x| x.as_str()))
        .unwrap_or_default()
        .trim()
        .to_string();
    Some((
        event.to_string(),
        code,
        msg,
        bitget_ws_code_is_success(v.get("code")),
    ))
}

fn is_ltp_terminal_order_push(resp: &ltp_ws::LtpWsResponse) -> bool {
    let raw = resp
        .data
        .get("orderState")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(
        raw.as_str(),
        "FILLED" | "CANCELLED" | "CANCELED" | "REJECT" | "FAIL"
    )
}

fn truncate_for_log(text: &str, max_chars: usize) -> String {
    let mut truncated = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            truncated.push_str("...");
            break;
        }
        truncated.push(ch);
    }
    truncated
}

fn format_error_chain(err: &anyhow::Error) -> String {
    let mut parts = Vec::new();
    for cause in err.chain() {
        let part = cause.to_string();
        if parts.last() != Some(&part) {
            parts.push(part);
        }
    }
    parts.join(": ")
}

fn format_ws_error(err: &WsError) -> String {
    match err {
        WsError::Http(resp) => {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-");
            let body = resp
                .body()
                .as_ref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .map(str::trim)
                .filter(|body| !body.is_empty())
                .map(|body| truncate_for_log(&body.replace(['\r', '\n'], " "), 256));
            match body {
                Some(body) => format!(
                    "HTTP {} content-type={} body={}",
                    status, content_type, body
                ),
                None => format!("HTTP {} content-type={}", status, content_type),
            }
        }
        _ => err.to_string(),
    }
}

const DEFAULT_TRADE_ENGINE_TCP_USER_TIMEOUT_MS: u32 = 30_000;

fn trade_engine_tcp_tuning() -> TcpSocketTuning {
    TcpSocketTuning {
        user_timeout_ms: Some(DEFAULT_TRADE_ENGINE_TCP_USER_TIMEOUT_MS),
        busy_poll_us: Some(DEFAULT_WS_BUSY_POLL_US),
        ..TcpSocketTuning::default()
    }
}

fn format_close_frame(frame: Option<&CloseFrame<'_>>) -> String {
    match frame {
        Some(frame) if frame.reason.is_empty() => {
            format!("websocket closed by remote (code={:?})", frame.code)
        }
        Some(frame) => format!(
            "websocket closed by remote (code={:?}, reason={})",
            frame.code,
            truncate_for_log(frame.reason.as_ref(), 256)
        ),
        None => format!("websocket closed by remote (code={:?})", CloseCode::Normal),
    }
}

#[derive(Debug)]
pub enum WsCommand {
    Send(TradeRequestMsg),
    SendQuery(QueryRequestMsg),
    Shutdown,
}

#[derive(Clone, Debug)]
pub(crate) struct WsCommandQueue {
    inner: Rc<RefCell<VecDeque<WsCommand>>>,
    notify: Rc<Notify>,
}

impl WsCommandQueue {
    pub(crate) fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(VecDeque::new())),
            notify: Rc::new(Notify::new()),
        }
    }

    fn push(&self, cmd: WsCommand) {
        self.inner.borrow_mut().push_back(cmd);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<WsCommand> {
        self.inner.borrow_mut().pop_front()
    }

    async fn next(&self) -> WsCommand {
        loop {
            if let Some(cmd) = self.pop() {
                return cmd;
            }
            self.notify.notified().await;
        }
    }
}

/// 跨 WS endpoint 共享的延迟分桶。
///
/// 时间点统一为 5 个：
/// - **T0**：IPC parse 完成（`msg.ipc_recv`）
/// - **T1**：`ws.send` 返回（`track_inflight` 时打点）
/// - **T2**：服务端收到（Gate=`x_in_time`，OKEx=`inTime`，Bitget=`ts`，Binance=`updateTime`）
/// - **T3**：服务端发出（Gate=`x_out_time`，OKEx=`outTime`，Bitget=T2，Binance=T2）
/// - **T4**：本地解析响应（`get_timestamp_us()`）
///
/// `new`/`cancel`：T1−T0（IPC→WS 端到端，所有 venue 通用）。
/// `resp`：仅当 venue 暴露服务端时间戳（Bitget/Gate/OKEx/Binance WS）时为 `Some`。
///
/// trade_engine 跑在 `current_thread` runtime + `LocalSet`，所有 ws task 同线程，
/// 因此 `Rc<RefCell<..>>` 已经够用，无须 `Arc<Mutex<..>>`。
#[derive(Clone)]
pub(crate) struct WsLatencyBuckets {
    pub new: Rc<RefCell<LatencyKll>>,
    pub cancel: Rc<RefCell<LatencyKll>>,
    pub resp: Option<RespLatencyBuckets>,
}

/// `WsLatencyBuckets::take_snapshot` 用：把单桶 KLL 快照写进消息槽位。
fn snap_into(
    buckets: &mut [LatencyBucketStat;
             rolling_common::latency_snapshot::LATENCY_SNAPSHOT_MAX_BUCKETS],
    idx: &mut usize,
    kll: &Rc<RefCell<LatencyKll>>,
    metric_id: u8,
    action_id: u8,
) {
    if let Some(s) = kll.borrow_mut().snapshot_and_reset() {
        if *idx < buckets.len() {
            buckets[*idx] = LatencyBucketStat {
                metric_id,
                action_id,
                _pad: [0; 6],
                n: s.n,
                p50_us: s.p50_us,
                p90_us: s.p90_us,
                p95_us: s.p95_us,
                p99_us: s.p99_us,
            };
            *idx += 1;
        }
    }
}

/// 服务端响应的细粒度延迟分解（4 区间 × {new, cancel}）：
/// - `uplink_*`     = T2 − T1（us → server）
/// - `server_*`     = T3 − T2（服务端处理；Bitget/Binance 退化为 0）
/// - `downlink_*`   = T4 − T3（server → us）
/// - `rtt_*`        = T4 − T1（本地时钟下完整往返）
#[derive(Clone)]
pub(crate) struct RespLatencyBuckets {
    pub uplink_new: Rc<RefCell<LatencyKll>>,
    pub uplink_cancel: Rc<RefCell<LatencyKll>>,
    pub server_new: Rc<RefCell<LatencyKll>>,
    pub server_cancel: Rc<RefCell<LatencyKll>>,
    pub downlink_new: Rc<RefCell<LatencyKll>>,
    pub downlink_cancel: Rc<RefCell<LatencyKll>>,
    pub rtt_new: Rc<RefCell<LatencyKll>>,
    pub rtt_cancel: Rc<RefCell<LatencyKll>>,
}

impl WsLatencyBuckets {
    /// 周期性 publisher 调用：取所有非空桶的 KLL 快照、清空 buffer，拼成定长
    /// `LatencySnapshotMsg`。所有桶都为空时返回 `None`，调用方可以选择不发布。
    pub(crate) fn take_snapshot(&self, venue_id: u32) -> Option<LatencySnapshotMsg> {
        let mut msg = LatencySnapshotMsg::new(venue_id, get_timestamp_us());
        let mut idx = 0usize;

        snap_into(
            &mut msg.buckets,
            &mut idx,
            &self.new,
            METRIC_ID_IPC_TO_WS,
            ACTION_ID_NEW,
        );
        snap_into(
            &mut msg.buckets,
            &mut idx,
            &self.cancel,
            METRIC_ID_IPC_TO_WS,
            ACTION_ID_CANCEL,
        );
        if let Some(r) = self.resp.as_ref() {
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.uplink_new,
                METRIC_ID_UPLINK,
                ACTION_ID_NEW,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.uplink_cancel,
                METRIC_ID_UPLINK,
                ACTION_ID_CANCEL,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.server_new,
                METRIC_ID_SERVER,
                ACTION_ID_NEW,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.server_cancel,
                METRIC_ID_SERVER,
                ACTION_ID_CANCEL,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.downlink_new,
                METRIC_ID_DOWNLINK,
                ACTION_ID_NEW,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.downlink_cancel,
                METRIC_ID_DOWNLINK,
                ACTION_ID_CANCEL,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.rtt_new,
                METRIC_ID_RTT,
                ACTION_ID_NEW,
            );
            snap_into(
                &mut msg.buckets,
                &mut idx,
                &r.rtt_cancel,
                METRIC_ID_RTT,
                ACTION_ID_CANCEL,
            );
        }

        if idx == 0 {
            None
        } else {
            msg.n_buckets = idx as u32;
            Some(msg)
        }
    }

    /// 从 venue 响应里取到 T2/T3 后调用，用 inflight 里记下的 T1 与本地 T4
    /// 推 4 个分位桶。`rtt = T4 − T1` 用 `Instant` 单调时钟（免受墙钟跳动影响）。
    fn record_resp(
        &self,
        action: WsAction,
        sent_at: std::time::Instant,
        sent_at_us: i64,
        t2_us: i64,
        t3_us: i64,
    ) {
        let Some(r) = self.resp.as_ref() else { return };
        let (uplink, server, downlink, rtt) = match action {
            WsAction::New => (&r.uplink_new, &r.server_new, &r.downlink_new, &r.rtt_new),
            WsAction::Cancel => (
                &r.uplink_cancel,
                &r.server_cancel,
                &r.downlink_cancel,
                &r.rtt_cancel,
            ),
            WsAction::Other => return,
        };
        let t4_us = get_timestamp_us();
        let rtt_us = sent_at.elapsed().as_micros() as i64;
        uplink.borrow_mut().push((t2_us - sent_at_us) as f64);
        server.borrow_mut().push((t3_us - t2_us) as f64);
        downlink.borrow_mut().push((t4_us - t3_us) as f64);
        rtt.borrow_mut().push(rtt_us as f64);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WsAction {
    New,
    Cancel,
    Other,
}

fn classify_ws_action(rt: TradeRequestType) -> WsAction {
    use TradeRequestType::*;
    match rt {
        BinanceWsNewUMOrder
        | BinanceWsNewMarginOrder
        | OkexNewMarginOrder
        | OkexNewUMOrder
        | GateUnifiedNewOrder
        | GateFuturesNewOrder
        | BybitNewMarginOrder
        | BybitNewUMOrder
        | BitgetNewMarginOrder
        | BitgetNewUMOrder => WsAction::New,

        BinanceWsCancelUMOrder
        | BinanceWsCancelMarginOrder
        | OkexCancelMarginOrder
        | OkexCancelUMOrder
        | GateUnifiedCancelOrder
        | GateFuturesCancelOrder
        | BybitCancelMarginOrder
        | BybitCancelUMOrder
        | BitgetCancelMarginOrder
        | BitgetCancelUMOrder => WsAction::Cancel,

        _ => WsAction::Other,
    }
}

#[derive(Debug, Default)]
pub(crate) struct WsEndpointState {
    connected: bool,
    cooldown_until: Option<std::time::Instant>,
    // 通用 TCP 丢包健康度（dispatch 路由）软暂停：到期前该端点被路由绕开。
    // 仅在 dispatch+act 模式下由 eval_tcp_health 设置；RR 模式永远为 None。
    tcp_loss_pause_until: Option<std::time::Instant>,
}

impl WsEndpointState {
    fn mark_connected(&mut self) {
        self.connected = true;
    }

    fn mark_disconnected(&mut self) {
        self.connected = false;
    }
}

#[derive(Clone, Debug)]
pub struct WsEndpointHandle {
    cmd_queue: WsCommandQueue,
    state: Rc<RefCell<WsEndpointState>>,
}

impl WsEndpointHandle {
    pub(crate) fn new(cmd_queue: WsCommandQueue, state: Rc<RefCell<WsEndpointState>>) -> Self {
        Self { cmd_queue, state }
    }

    pub(crate) fn send(&self, cmd: WsCommand) -> Result<(), ()> {
        if !self.is_available() {
            return Err(());
        }
        self.enqueue_available(cmd);
        Ok(())
    }

    pub(crate) fn enqueue_available(&self, cmd: WsCommand) {
        self.cmd_queue.push(cmd);
    }

    pub(crate) fn is_available(&self) -> bool {
        self.is_available_at(std::time::Instant::now())
    }

    fn is_available_at(&self, now: std::time::Instant) -> bool {
        let mut state = self.state.borrow_mut();
        if !state.connected {
            return false;
        }
        if let Some(until) = state.cooldown_until {
            if now < until {
                return false;
            }
            state.cooldown_until = None;
        }
        // TCP 丢包软暂停对所有路由都无条件生效（与 cooldown 同级）；RR 模式下从不被设置，
        // 因此不影响 RR。dispatch+act 模式下 eval_tcp_health 置位后，本端点被路由绕开。
        if let Some(until) = state.tcp_loss_pause_until {
            if now < until {
                return false;
            }
            state.tcp_loss_pause_until = None;
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn mark_connected(&self) {
        self.state.borrow_mut().mark_connected();
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct InflightRequestFlags {
    ws_open_update_enabled: bool,
}

#[derive(Clone, Debug)]
struct TradeInflightMeta {
    req_type: TradeRequestType,
    client_order_id: i64,
    ws_open_update_enabled: bool,
    /// 单调时钟，`track_inflight`（即 `ws.send` 之后）时打点；用于本地 RTT。
    sent_at: std::time::Instant,
    /// 墙钟 epoch μs，与 `sent_at` 同时打点；用于跨时钟差值（uplink / downlink）。
    sent_at_us: i64,
}

#[derive(Clone, Copy, Debug)]
struct QueryInflightMeta {
    req_type: QueryRequestType,
    client_query_id: i64,
}

#[derive(Debug, Deserialize)]
struct OkexInstrumentsResponse {
    code: String,
    msg: String,
    data: Vec<OkexInstrumentEntry>,
}

#[derive(Debug, Deserialize)]
struct OkexInstrumentEntry {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instIdCode")]
    inst_id_code: Value,
}

pub struct TradeWsClient {
    id: usize,
    exchange: Exchange,
    logical_exchange: Exchange,
    use_ltp_backend: bool,
    local_ip: IpAddr,
    url: String,
    remote_ip_override: Option<IpAddr>,
    current_remote_addr: Option<SocketAddr>,
    // 当前连接底层 TCP fd + 上一次 TCP_INFO 快照，用于观测 retrans 率（仅 Linux）。
    current_tcp_fd: Option<std::os::fd::RawFd>,
    tcp_health_last: Option<TcpRetransSnapshot>,
    connect_timeout_ms: u64,
    ping_interval_ms: u64,
    max_inflight: usize,
    login_payload: Option<String>,
    binance_creds: Option<ApiKey>,
    binance_ws_signer: Option<BinanceWsSigner>,
    binance_session_logon_transport_id: Option<i64>,
    binance_waiting_session_logon: bool,
    bitget_creds: Option<BitgetCredentials>,
    bybit_creds: Option<BybitCredentials>,
    okex_creds: Option<OkexCredentials>,
    gate_creds: Option<GateCredentials>,
    ltp_creds: Option<LtpCredentials>,
    okex_http_client: Option<reqwest::Client>,
    okex_inst_id_code_cache: FastHashMap<String, i64>,
    okex_loaded_inst_types: FastHashSet<&'static str>,
    gate_ws_kind: Option<gate_ws::GateWsKind>,
    cmd_queue: WsCommandQueue,
    resp_sink: TradeResponseSink,
    query_resp_sink: Option<QueryResponseSink>,
    internal_open_terminates: InternalOpenTerminateMap,
    internal_open_terminate_summary: InternalOpenTerminateSummary,
    pending: VecDeque<TradeRequestMsg>,
    pending_query: VecDeque<QueryRequestMsg>,
    inflight: FastHashMap<i64, TradeInflightMeta>,
    query_inflight: FastHashMap<i64, QueryInflightMeta>,
    next_binance_transport_id: i64,
    engine_shutdown: CancellationToken,
    endpoint_state: Rc<RefCell<WsEndpointState>>,
    shutdown_on_rate_limit: bool,
    rate_limit_cooldown_until: Option<std::time::Instant>,
    shutdown: bool,
    should_reconnect: bool, // 标记是否需要重连（用于 notice 触发的重连）
    planned_reconnect_period_ms: Option<u64>,
    planned_reconnect_offset_ms: u64,
    bitget_waiting_pong: bool,
    bybit_waiting_auth: bool,
    bybit_waiting_pong: bool,
    last_okex_login_ts: Option<String>,
    last_gate_login_req_id: Option<String>,
    lat_buckets: WsLatencyBuckets,
    binance_um_route_group_id: Option<u32>,
    // dispatch 路由：允许 TCP 丢包健康度真正驱动 软暂停(route-away)+硬重连。
    // 仅当 ws_route=dispatch 时为 true；RR 恒为 false（只观测、只打日志）。
    tcp_loss_act: bool,
}

impl TradeWsClient {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: usize,
        exchange: Exchange,
        logical_exchange: Exchange,
        use_ltp_backend: bool,
        local_ip: IpAddr,
        url: String,
        connect_timeout_ms: u64,
        ping_interval_ms: u64,
        max_inflight: usize,
        login_payload: Option<String>,
        binance_creds: Option<ApiKey>,
        gate_ws_kind: Option<gate_ws::GateWsKind>,
        query_resp_sink: Option<QueryResponseSink>,
        cmd_queue: WsCommandQueue,
        resp_sink: TradeResponseSink,
        internal_open_terminates: InternalOpenTerminateMap,
        internal_open_terminate_summary: InternalOpenTerminateSummary,
        engine_shutdown: CancellationToken,
        endpoint_state: Rc<RefCell<WsEndpointState>>,
        shutdown_on_rate_limit: bool,
        lat_buckets: WsLatencyBuckets,
    ) -> Self {
        let (login_payload, bitget_creds, bybit_creds, okex_creds, gate_creds, ltp_creds) =
            if use_ltp_backend {
                let creds = LtpCredentials::from_env().unwrap_or_else(|e| {
                    panic!(
                        "LTP backend requires environment variables LTP_API_KEY and LTP_API_SECRET: {}",
                        e
                    )
                });
                info!(
                    "LTP credentials loaded from environment for ws client id={} logical_exchange={}",
                    id, logical_exchange
                );
                let only_trade = std::env::var("LTP_WS_ONLY_TRADE")
                    .ok()
                    .map(|v| {
                        matches!(
                            v.trim().to_ascii_lowercase().as_str(),
                            "1" | "true" | "yes" | "on"
                        )
                    })
                    .unwrap_or(false);
                let login_payload = creds
                    .build_login_payload(only_trade)
                    .unwrap_or_else(|e| panic!("build LTP login payload failed: {}", e));
                (Some(login_payload), None, None, None, None, Some(creds))
            } else {
                match exchange {
                    Exchange::Bitget => {
                        let creds = BitgetCredentials::from_env().unwrap_or_else(|e| {
                            panic!(
                                "Bitget requires environment variables BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE: {}",
                                e
                            )
                        });
                        info!(
                            "Bitget credentials loaded from environment for ws client id={}",
                            id
                        );
                        let login_payload = bitget_ws::build_login_payload(&creds)
                            .unwrap_or_else(|e| panic!("build Bitget login payload failed: {}", e));
                        (Some(login_payload), Some(creds), None, None, None, None)
                    }
                    Exchange::Bybit => {
                        let creds = BybitCredentials::from_env().unwrap_or_else(|e| {
                            panic!(
                                "Bybit requires environment variables BYBIT_API_KEY, BYBIT_API_SECRET: {}",
                                e
                            )
                        });
                        info!(
                            "Bybit credentials loaded from environment for ws client id={}",
                            id
                        );
                        (None, None, Some(creds), None, None, None)
                    }
                    Exchange::Okex => {
                        // OKX login must use a fresh timestamp (signed) on each connect/reconnect.
                        let creds = OkexCredentials::from_env().unwrap_or_else(|e| {
                            panic!(
                                "OKEx requires environment variables OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE: {}",
                                e
                            )
                        });
                        info!(
                            "OKEx credentials loaded from environment for ws client id={}",
                            id
                        );
                        (None, None, None, Some(creds), None, None)
                    }
                    Exchange::Gate => {
                        let creds = GateCredentials::from_env().unwrap_or_else(|e| {
                            panic!(
                                "Gate requires environment variables GATE_API_KEY, GATE_API_SECRET: {}",
                                e
                            )
                        });
                        info!(
                            "Gate credentials loaded from environment for ws client id={}",
                            id
                        );
                        (None, None, None, None, Some(creds), None)
                    }
                    _ => (login_payload, None, None, None, None, None),
                }
            };

        let binance_ws_signer = if exchange == Exchange::Binance && !use_ltp_backend {
            match binance_creds.as_ref() {
                Some(creds) => {
                    let signer = BinanceWsSigner::from_env_or_secret(creds.secret.clone())
                        .unwrap_or_else(|e| panic!("build Binance WS signer failed: {}", e));
                    if let Some(source) = signer.ed25519_source() {
                        info!(
                            "Binance WS signer loaded for client id={} algorithm={} source={}",
                            id,
                            signer.algorithm(),
                            source
                        );
                    } else {
                        info!(
                            "Binance WS signer loaded for client id={} algorithm={}",
                            id,
                            signer.algorithm()
                        );
                    }
                    Some(signer)
                }
                None => {
                    warn!(
                        "trade ws client id={} missing Binance credentials; ws requests will fail",
                        id
                    );
                    None
                }
            }
        } else {
            None
        };

        Self {
            id,
            exchange,
            logical_exchange,
            use_ltp_backend,
            local_ip,
            url,
            remote_ip_override: None,
            current_remote_addr: None,
            current_tcp_fd: None,
            tcp_health_last: None,
            connect_timeout_ms,
            ping_interval_ms,
            max_inflight,
            login_payload,
            binance_creds,
            binance_ws_signer,
            binance_session_logon_transport_id: None,
            binance_waiting_session_logon: false,
            bitget_creds,
            bybit_creds,
            okex_creds,
            gate_creds,
            ltp_creds,
            okex_http_client: (exchange == Exchange::Okex).then(reqwest::Client::new),
            okex_inst_id_code_cache: fast_hash_map(),
            okex_loaded_inst_types: fast_hash_set(),
            gate_ws_kind,
            cmd_queue,
            resp_sink,
            query_resp_sink,
            internal_open_terminates,
            internal_open_terminate_summary,
            pending: VecDeque::new(),
            pending_query: VecDeque::new(),
            inflight: fast_hash_map(),
            query_inflight: fast_hash_map(),
            next_binance_transport_id: ((id as i64) << 48) + 1,
            engine_shutdown,
            endpoint_state,
            shutdown_on_rate_limit,
            rate_limit_cooldown_until: None,
            shutdown: false,
            should_reconnect: false,
            planned_reconnect_period_ms: None,
            planned_reconnect_offset_ms: 0,
            bitget_waiting_pong: false,
            bybit_waiting_auth: false,
            bybit_waiting_pong: false,
            last_okex_login_ts: None,
            last_gate_login_req_id: None,
            lat_buckets,
            binance_um_route_group_id: None,
            tcp_loss_act: false,
        }
    }

    pub(crate) fn with_remote_ip_override(mut self, remote_ip: IpAddr) -> Self {
        self.remote_ip_override = Some(remote_ip);
        self
    }

    pub(crate) fn with_planned_reconnect(mut self, period_ms: u64, offset_ms: u64) -> Self {
        if period_ms > 0 {
            self.planned_reconnect_period_ms = Some(period_ms);
            self.planned_reconnect_offset_ms = offset_ms % period_ms;
        }
        self
    }

    pub(crate) fn with_binance_um_route_group_id(mut self, route_group_id: u32) -> Self {
        self.binance_um_route_group_id = Some(route_group_id);
        self
    }

    /// dispatch 路由：开启后 TCP 丢包健康度可真正驱动软暂停+硬重连（配合 act 模式）。
    pub(crate) fn with_tcp_loss_act(mut self, enabled: bool) -> Self {
        self.tcp_loss_act = enabled;
        self
    }

    pub fn local_ip(&self) -> IpAddr {
        self.local_ip
    }

    fn waiting_for_binance_session_logon(&self) -> bool {
        self.exchange == Exchange::Binance && self.binance_waiting_session_logon
    }

    fn reset_binance_session_logon(&mut self) {
        self.binance_session_logon_transport_id = None;
        self.binance_waiting_session_logon = false;
    }

    fn next_planned_reconnect_deadline(&self) -> Option<time::Instant> {
        let period_ms = self.planned_reconnect_period_ms?;
        if period_ms == 0 {
            return None;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let period_ms = period_ms as u128;
        let offset_ms = (self.planned_reconnect_offset_ms as u128) % period_ms;
        let current_slot_ms = now_ms % period_ms;
        let delay_ms = if current_slot_ms < offset_ms {
            offset_ms - current_slot_ms
        } else {
            period_ms - (current_slot_ms - offset_ms)
        };
        let delay_ms = delay_ms.max(1).min(u64::MAX as u128) as u64;
        Some(time::Instant::now() + Duration::from_millis(delay_ms))
    }

    fn can_planned_reconnect_now(&self) -> bool {
        self.pending.is_empty()
            && self.pending_query.is_empty()
            && self.inflight.is_empty()
            && self.query_inflight.is_empty()
            && !self.waiting_for_binance_session_logon()
    }

    fn build_binance_session_logon_payload(&mut self) -> Result<Option<String>> {
        if self.exchange != Exchange::Binance || self.use_ltp_backend {
            return Ok(None);
        }
        self.reset_binance_session_logon();
        let Some(true) = self
            .binance_ws_signer
            .as_ref()
            .map(|signer| signer.uses_session_logon())
        else {
            return Ok(None);
        };
        let api_key = binance_ed25519_api_key_from_env()?;
        let transport_id = self.next_transport_id();
        let signer = self
            .binance_ws_signer
            .as_ref()
            .ok_or_else(|| anyhow!("missing binance ws signer"))?;
        let payload = binance_ws::build_session_logon_payload(transport_id, &api_key, signer)?;
        self.binance_session_logon_transport_id = Some(transport_id);
        self.binance_waiting_session_logon = true;
        Ok(Some(payload))
    }

    fn binance_rate_limit_payload(
        status: Option<u16>,
        error_code: Option<i32>,
        text: &str,
    ) -> bool {
        matches!(status, Some(418 | 429))
            || matches!(error_code, Some(-1003))
            || text.contains("HTTP 418")
            || text.contains("HTTP 429")
            || text.contains("\"code\":-1003")
            || text.contains("Too Many Requests")
            || text.contains("Way too many requests")
    }

    fn trigger_engine_shutdown_on_binance_rate_limit(
        &mut self,
        status: Option<u16>,
        error_code: Option<i32>,
        text: &str,
        context: &str,
    ) {
        if self.exchange != Exchange::Binance || self.engine_shutdown.is_cancelled() {
            return;
        }
        if !Self::binance_rate_limit_payload(status, error_code, text) {
            return;
        }
        if self.shutdown_on_rate_limit {
            warn!(
                "trade ws client id={} exchange=binance tripping trade_engine shutdown due to rate limit at {} status={:?} code={:?} detail={}",
                self.id,
                context,
                status,
                error_code,
                text
            );
            self.engine_shutdown.cancel();
            self.shutdown = true;
            return;
        }

        let cooldown_ms = if matches!(status, Some(418)) {
            LimitConstants::BAN_BACKOFF_MS_418
        } else {
            LimitConstants::COOLDOWN_MS_429
        };
        let until = std::time::Instant::now() + Duration::from_millis(cooldown_ms);
        self.rate_limit_cooldown_until = Some(until);
        self.endpoint_state.borrow_mut().cooldown_until = Some(until);
        warn!(
            "trade ws client id={} exchange=binance entering endpoint cooldown due to rate limit at {} status={:?} code={:?} cooldown_ms={} detail={}",
            self.id,
            context,
            status,
            error_code,
            cooldown_ms,
            text
        );
    }

    fn clear_rate_limit_cooldown_if_elapsed(&mut self) {
        let now = std::time::Instant::now();
        if self
            .rate_limit_cooldown_until
            .map(|until| now >= until)
            .unwrap_or(false)
        {
            self.rate_limit_cooldown_until = None;
            let mut state = self.endpoint_state.borrow_mut();
            if state
                .cooldown_until
                .map(|until| now >= until)
                .unwrap_or(false)
            {
                state.cooldown_until = None;
            }
        }
    }

    fn in_rate_limit_cooldown(&mut self) -> bool {
        self.clear_rate_limit_cooldown_if_elapsed();
        self.rate_limit_cooldown_until
            .map(|until| std::time::Instant::now() < until)
            .unwrap_or(false)
    }

    async fn next_command(&self) -> WsCommand {
        self.cmd_queue.next().await
    }

    fn queue_disconnected_command(&mut self, cmd: WsCommand, context: &str) {
        match cmd {
            WsCommand::Send(msg) => {
                debug!(
                    "trade ws client id={} queued order {} client_order_id={}",
                    self.id, context, msg.client_order_id
                );
                self.pending.push_back(msg);
            }
            WsCommand::SendQuery(msg) => {
                debug!(
                    "trade ws client id={} queued query {} client_query_id={}",
                    self.id, context, msg.client_query_id
                );
                self.pending_query.push_back(msg);
            }
            WsCommand::Shutdown => {
                info!(
                    "trade ws client id={} shutdown requested {}",
                    self.id, context
                );
                self.shutdown = true;
            }
        }
    }

    pub async fn run(mut self) {
        let mut backoff_ms = 500u64;
        while !self.shutdown {
            if let Some(until) = self.rate_limit_cooldown_until {
                let now = std::time::Instant::now();
                if now < until {
                    let sleep_until = tokio::time::Instant::from_std(until);
                    tokio::select! {
                        biased;
                        _ = self.engine_shutdown.cancelled() => {
                            info!("trade ws client id={} observed trade_engine shutdown during rate-limit cooldown", self.id);
                            self.shutdown = true;
                        }
                        cmd = self.next_command() => {
                            self.queue_disconnected_command(cmd, "during rate-limit cooldown");
                        }
                        _ = time::sleep_until(sleep_until) => {
                            self.clear_rate_limit_cooldown_if_elapsed();
                        }
                    }
                    if self.shutdown {
                        break;
                    }
                    if self.in_rate_limit_cooldown() {
                        continue;
                    }
                } else {
                    self.clear_rate_limit_cooldown_if_elapsed();
                }
            }

            let local_ip = self.local_ip;
            let url = self.url.clone();
            let remote_ip_override = self.remote_ip_override;
            let connect_timeout_ms = self.connect_timeout_ms;
            let ws_headers = self.ws_handshake_headers();
            tokio::select! {
                biased;
                _ = self.engine_shutdown.cancelled() => {
                    info!("trade ws client id={} observed trade_engine shutdown while disconnected", self.id);
                    self.shutdown = true;
                    break;
                }
                cmd = self.next_command() => {
                    self.queue_disconnected_command(cmd, "while disconnected");
                    if self.shutdown {
                        break;
                    }
                    continue;
                }
                res = Self::establish_connection_with(local_ip, &url, remote_ip_override, connect_timeout_ms, &ws_headers) => {
                    match res {
                        Ok((mut ws, remote_addr, raw_fd)) => {
                            self.endpoint_state.borrow_mut().mark_connected();
                            self.current_remote_addr = Some(remote_addr);
                            self.current_tcp_fd = Some(raw_fd);
                            self.tcp_health_last = None;
                            info!(
                                "trade ws client id={} established connection to {} via {} remote_addr={}",
                                self.id, self.url, self.local_ip, remote_addr
                            );

                            let login_payload = if self.use_ltp_backend {
                                self.ltp_creds.as_ref().and_then(|c| {
                                    let only_trade = std::env::var("LTP_WS_ONLY_TRADE")
                                        .ok()
                                        .map(|v| {
                                            matches!(
                                                v.trim().to_ascii_lowercase().as_str(),
                                                "1" | "true" | "yes" | "on"
                                            )
                                        })
                                        .unwrap_or(false);
                                    c.build_login_payload(only_trade).ok()
                                })
                            } else if self.exchange == Exchange::Bitget {
                                self.bitget_creds
                                    .as_ref()
                                    .and_then(|c| bitget_ws::build_login_payload(c).ok())
                            } else if self.exchange == Exchange::Bybit {
                                self.bybit_creds
                                    .as_ref()
                                    .map(|c| c.build_auth_message().to_string())
                            } else if self.exchange == Exchange::Okex {
                                self.okex_creds
                                    .as_ref()
                                    .map(|c| c.build_login_message().to_string())
                            } else if self.exchange == Exchange::Gate {
                                self.gate_creds.as_ref().map(|c| {
                                    let kind = self
                                        .gate_ws_kind
                                        .unwrap_or(gate_ws::GateWsKind::SpotUnified);
                                    let (payload, req_id) = if kind
                                        == gate_ws::GateWsKind::SpotUnified
                                    {
                                        gate_ws::build_login_message(c)
                                    } else {
                                        gate_ws::build_login_message_with_kind(c, kind)
                                    };
                                    self.last_gate_login_req_id = Some(req_id);
                                    payload
                                })
                            } else if self.exchange == Exchange::Binance {
                                self.build_binance_session_logon_payload()
                                    .unwrap_or_else(|e| panic!("build Binance session.logon payload failed: {}", e))
                            } else {
                                self.login_payload.clone()
                            };

                            if let Some(payload) = login_payload {
                                if self.exchange == Exchange::Bitget {
                                    self.bitget_waiting_pong = false;
                                }
                                self.last_okex_login_ts = extract_okex_login_timestamp(&payload);
                                if self.exchange == Exchange::Bybit {
                                    self.bybit_waiting_auth = true;
                                }
                                let now_s = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let now_ms = chrono::Utc::now().timestamp_millis();
                                match self.exchange {
                                    Exchange::Okex => {
                                        info!(
                                            "trade ws client id={} exchange={} sending login payload ({} bytes) okx_ts={:?} local_unix_s={} local_unix_ms={}",
                                            self.id,
                                            self.exchange,
                                            payload.len(),
                                            self.last_okex_login_ts.as_deref(),
                                            now_s,
                                            now_ms
                                        );
                                    }
                                    Exchange::Gate => {
                                        info!(
                                            "trade ws client id={} exchange={} sending login payload ({} bytes) gate_req_id={:?} local_unix_s={} local_unix_ms={}",
                                            self.id,
                                            self.exchange,
                                            payload.len(),
                                            self.last_gate_login_req_id.as_deref(),
                                            now_s,
                                            now_ms
                                        );
                                    }
                                    Exchange::Bybit => {
                                        info!(
                                            "trade ws client id={} exchange={} sending auth payload ({} bytes) local_unix_s={} local_unix_ms={}",
                                            self.id,
                                            self.exchange,
                                            payload.len(),
                                            now_s,
                                            now_ms
                                        );
                                    }
                                    _ => {
                                        info!(
                                            "trade ws client id={} exchange={} sending login payload ({} bytes) local_unix_s={} local_unix_ms={}",
                                            self.id,
                                            self.exchange,
                                            payload.len(),
                                            now_s,
                                            now_ms
                                        );
                                    }
                                }
                                if let Err(err) = ws.send(Message::Text(payload)).await {
                                    warn!(
                                        "trade ws client id={} send login payload failed: {}",
                                        self.id, err
                                    );
                                    self.reset_binance_session_logon();
                                    let _ = ws.close(None).await;
                                    self.endpoint_state.borrow_mut().mark_disconnected();
                                    self.current_remote_addr = None;
                                    self.current_tcp_fd = None;
                                    continue;
                                }
                            }

                            backoff_ms = 500;
                            if let Err(err) = self.event_loop(&mut ws).await {
                                warn!(
                                    "trade ws client id={} exchange={} url={} connection loop exited: {}",
                                    self.id,
                                    self.exchange,
                                    self.url,
                                    format_error_chain(&err)
                                );
                            }
                            self.reset_binance_session_logon();
                            self.endpoint_state.borrow_mut().mark_disconnected();
                            self.current_remote_addr = None;
                            self.current_tcp_fd = None;
                        }
                        Err(err) => {
                            self.endpoint_state.borrow_mut().mark_disconnected();
                            self.current_remote_addr = None;
                            self.current_tcp_fd = None;
                            let err_text = format_error_chain(&err);
                            self.trigger_engine_shutdown_on_binance_rate_limit(
                                None,
                                None,
                                &err_text,
                                "connect",
                            );
                            warn!(
                                "trade ws client id={} exchange={} url={} failed to connect ({}), retrying in {} ms",
                                self.id,
                                self.exchange,
                                self.url,
                                err_text,
                                backoff_ms
                            );
                        }
                    }
                }
            }
            if self.shutdown {
                break;
            }
            if self.in_rate_limit_cooldown() {
                continue;
            }
            tokio::select! {
                biased;
                _ = self.engine_shutdown.cancelled() => {
                    info!("trade ws client id={} observed trade_engine shutdown during backoff", self.id);
                    self.shutdown = true;
                }
                cmd = self.next_command() => {
                    self.queue_disconnected_command(cmd, "during backoff");
                }
                _ = time::sleep(Duration::from_millis(backoff_ms)) => {}
            }
            backoff_ms = (backoff_ms * 2).min(30_000);
        }
        info!("trade ws client id={} stopped", self.id);
    }

    async fn event_loop(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        let mut ping_interval = time::interval(Duration::from_millis(self.ping_interval_ms));
        ping_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        // TCP 丢包健康度：observe 只算+日志；act 才驱动动作（动作在 Dispatch 路由下生效）。
        // 间隔沿用 TRADE_ENGINE_TCP_HEALTH_LOG_INTERVAL_MS（0=关）；MODE=off 亦关。
        let tcp_health_interval_ms = std::env::var("TRADE_ENGINE_TCP_HEALTH_LOG_INTERVAL_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(10_000);
        // RR 路由永不 act：即便 env=act 也降级为 observe（只观测、只打日志），保证 RR 行为不变。
        // 仅 dispatch 路由(self.tcp_loss_act)才允许 act 真正驱动 pause/reconnect。
        let env_tcp_health_mode = TcpHealthMode::from_env(TcpHealthMode::Observe);
        let tcp_health_mode = match env_tcp_health_mode {
            TcpHealthMode::Off => TcpHealthMode::Off,
            TcpHealthMode::Act if self.tcp_loss_act => TcpHealthMode::Act,
            _ => TcpHealthMode::Observe,
        };
        let tcp_health_enabled =
            tcp_health_interval_ms > 0 && tcp_health_mode != TcpHealthMode::Off;
        let mut tcp_loss_health = TcpLossHealth::new(TcpLossHealthConfig::from_env());
        let mut tcp_health_interval =
            time::interval(Duration::from_millis(tcp_health_interval_ms.max(1)));
        tcp_health_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        tcp_health_interval.tick().await;
        let mut planned_reconnect_deadline = self.next_planned_reconnect_deadline();
        self.flush_pending(ws).await?;
        loop {
            let planned_reconnect_sleep_until = planned_reconnect_deadline
                .unwrap_or_else(|| time::Instant::now() + Duration::from_secs(365 * 24 * 60 * 60));
            tokio::select! {
                biased;
                _ = self.engine_shutdown.cancelled() => {
                    info!("trade ws client id={} observed trade_engine shutdown in event loop", self.id);
                    self.shutdown = true;
                    let _ = ws.close(None).await;
                    return Ok(());
                }
                cmd = self.next_command() => {
                    self.handle_command_connected(cmd, ws).await?;
                    if self.shutdown {
                        return Ok(());
                    }
                }
                message = ws.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            self.handle_incoming(ws, msg).await?;
                            self.flush_pending(ws).await?;
                        }
                        Some(Err(err)) => {
                            return Err(anyhow!("websocket errored: {}", format_ws_error(&err)));
                        }
                        None => {
                            return Err(anyhow!("websocket stream ended without close frame"));
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    if self.exchange == Exchange::Bitget && self.bitget_waiting_pong {
                        warn!(
                            "trade ws client id={} Bitget ping timeout, reconnecting",
                            self.id
                        );
                        let _ = ws.close(None).await;
                        return Err(anyhow!("bitget ping timeout"));
                    }
                    self.send_ping(ws).await?;
                }
                _ = tcp_health_interval.tick(), if tcp_health_enabled => {
                    self.eval_tcp_health(&mut tcp_loss_health, tcp_health_mode);
                }
                _ = time::sleep_until(planned_reconnect_sleep_until), if planned_reconnect_deadline.is_some() => {
                    if self.can_planned_reconnect_now() {
                        info!(
                            "trade ws client id={} exchange={} planned reconnect url={} local_ip={} remote_addr={:?} period_ms={} offset_ms={}",
                            self.id,
                            self.exchange,
                            self.url,
                            self.local_ip,
                            self.current_remote_addr,
                            self.planned_reconnect_period_ms.unwrap_or(0),
                            self.planned_reconnect_offset_ms
                        );
                        let _ = ws.close(None).await;
                        return Ok(());
                    }
                    debug!(
                        "trade ws client id={} exchange={} planned reconnect deferred pending={} pending_query={} inflight={} query_inflight={} waiting_logon={}",
                        self.id,
                        self.exchange,
                        self.pending.len(),
                        self.pending_query.len(),
                        self.inflight.len(),
                        self.query_inflight.len(),
                        self.waiting_for_binance_session_logon()
                    );
                    planned_reconnect_deadline = Some(time::Instant::now() + Duration::from_millis(1_000));
                }
            }

            // 检查是否需要重连（由 notice 触发）
            if self.should_reconnect {
                warn!("trade ws client id={} reconnecting due to notice", self.id);
                self.should_reconnect = false;
                let _ = ws.close(None).await;
                return Err(anyhow!("reconnecting due to notice"));
            }

            if self.shutdown {
                return Ok(());
            }
        }
    }

    async fn handle_command_connected(
        &mut self,
        cmd: WsCommand,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        match cmd {
            WsCommand::Send(msg) => {
                debug!(
                    "trade ws client id={} received order client_order_id={}",
                    self.id, msg.client_order_id
                );
                self.handle_send(msg, ws).await?;
            }
            WsCommand::SendQuery(msg) => {
                debug!(
                    "trade ws client id={} received query client_query_id={}",
                    self.id, msg.client_query_id
                );
                self.handle_send_query(msg, ws).await?;
            }
            WsCommand::Shutdown => {
                info!("trade ws client id={} received shutdown signal", self.id);
                self.shutdown = true;
                let _ = ws.close(None).await;
            }
        }
        Ok(())
    }

    fn ws_handshake_headers(&self) -> Vec<(String, String)> {
        if self.exchange == Exchange::Gate
            && self.gate_ws_kind == Some(gate_ws::GateWsKind::FuturesUsdt)
        {
            vec![("X-Gate-Size-Decimal".to_string(), "1".to_string())]
        } else {
            Vec::new()
        }
    }

    async fn establish_connection_with(
        local_ip: IpAddr,
        url_str: &str,
        remote_ip_override: Option<IpAddr>,
        connect_timeout_ms: u64,
        headers: &[(String, String)],
    ) -> Result<(
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        SocketAddr,
        std::os::fd::RawFd,
    )> {
        let url = Url::parse(url_str).with_context(|| "invalid websocket url")?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow!("websocket url missing host"))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| anyhow!("websocket url missing port"))?;

        let target = if let Some(remote_ip) = remote_ip_override {
            match (remote_ip, local_ip) {
                (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_)) => {
                    SocketAddr::new(remote_ip, port)
                }
                _ => {
                    return Err(anyhow!(
                        "remote ip override {} is not compatible with local ip {}",
                        remote_ip,
                        local_ip
                    ));
                }
            }
        } else {
            let mut candidates = lookup_host((host, port))
                .await
                .with_context(|| format!("resolve {}:{}", host, port))?;
            candidates
                .find(|addr| {
                    matches!(
                        (addr, local_ip),
                        (SocketAddr::V4(_), IpAddr::V4(_)) | (SocketAddr::V6(_), IpAddr::V6(_))
                    )
                })
                .ok_or_else(|| anyhow!("no compatible address family for {}", local_ip))?
        };

        let socket = match local_ip {
            IpAddr::V4(ip) => {
                let s = TcpSocket::new_v4()?;
                s.bind((ip, 0).into())
                    .with_context(|| format!("bind local ipv4 {}", ip))?;
                s
            }
            IpAddr::V6(ip) => {
                let s = TcpSocket::new_v6()?;
                s.bind((ip, 0).into())
                    .with_context(|| format!("bind local ipv6 {}", ip))?;
                s
            }
        };
        let _ = socket.set_send_buffer_size(1 << 20);
        let _ = socket.set_recv_buffer_size(1 << 20);

        let connect_timeout = Duration::from_millis(connect_timeout_ms);
        let stream = tokio::time::timeout(connect_timeout, socket.connect(target))
            .await
            .map_err(|_| anyhow!("connect timeout {}", url_str))?
            .with_context(|| format!("connect {}", target))?;
        tune_tcp_stream(&stream, "trade_engine ws", trade_engine_tcp_tuning());
        // 在 TLS/WS 包装前捕获底层 TCP fd，供 TCP_INFO 健康观测使用。
        // 该 fd 在连接存活期间有效；重连时旧连接关闭、新连接会重新捕获。
        let raw_fd = {
            use std::os::fd::AsRawFd;
            stream.as_raw_fd()
        };

        let mut request = url.clone().into_client_request()?;
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("invalid websocket header name {}", name))?;
            let value = HeaderValue::from_str(value)
                .with_context(|| format!("invalid websocket header value for {}", name))?;
            request.headers_mut().insert(name, value);
        }

        let (ws_stream, _resp) = if url.scheme().eq_ignore_ascii_case("wss") {
            let native = TlsConnector::builder()
                .build()
                .with_context(|| "build tls connector")?;
            let connector = TokioTlsConnector::from(native);
            let tls = connector
                .connect(host, stream)
                .await
                .with_context(|| "tls handshake failed")?;
            client_async(request, MaybeTlsStream::NativeTls(tls))
                .await
                .map_err(|err| anyhow!("websocket handshake (wss): {}", format_ws_error(&err)))?
        } else {
            client_async(request, MaybeTlsStream::Plain(stream))
                .await
                .map_err(|err| anyhow!("websocket handshake (ws): {}", format_ws_error(&err)))?
        };

        Ok((ws_stream, target, raw_fd))
    }

    async fn handle_send(
        &mut self,
        msg: TradeRequestMsg,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        if self.try_internal_terminate_order(&msg, "ws_handle_send") {
            return Ok(());
        }
        if self.pending.len() >= self.max_inflight {
            let reason = format!(
                "ws inflight limit exceeded (limit={}, pending={})",
                self.max_inflight,
                self.pending.len()
            );
            warn!(
                "trade ws client id={} rejecting order {}: {}",
                self.id, msg.client_order_id, reason
            );
            self.notify_rejected(&msg, &reason);
            return Ok(());
        }
        self.pending.push_back(msg);
        self.flush_pending(ws).await
    }

    async fn handle_send_query(
        &mut self,
        msg: QueryRequestMsg,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        if self.query_resp_sink.is_none() {
            return Ok(());
        }
        if self.pending_query.len() >= self.max_inflight {
            let reason = format!(
                "ws query inflight limit exceeded (limit={}, pending={})",
                self.max_inflight,
                self.pending_query.len()
            );
            warn!(
                "trade ws client id={} rejecting query {}: {}",
                self.id, msg.client_query_id, reason
            );
            self.notify_query_rejected(&msg);
            return Ok(());
        }
        self.pending_query.push_back(msg);
        self.flush_pending(ws).await
    }

    async fn flush_pending(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        if self.waiting_for_binance_session_logon() {
            return Ok(());
        }
        while let Some(msg) = self.pending.pop_front() {
            if self.try_internal_terminate_order(&msg, "ws_flush_pending") {
                continue;
            }
            if let Err(err) = self.send_one(ws, &msg).await {
                warn!(
                    "trade ws client id={} send failed for order {}: {}",
                    self.id, msg.client_order_id, err
                );
                self.pending.push_front(msg);
                return Err(err);
            }
        }
        while let Some(msg) = self.pending_query.pop_front() {
            if let Err(err) = self.send_one_query(ws, &msg).await {
                warn!(
                    "trade ws client id={} send failed for query {}: {}",
                    self.id, msg.client_query_id, err
                );
                self.pending_query.push_front(msg);
                return Err(err);
            }
        }
        Ok(())
    }

    fn try_internal_terminate_order(&self, msg: &TradeRequestMsg, stage: &'static str) -> bool {
        if !msg.req_type.is_new_order() {
            return false;
        }
        let Some(state) =
            take_internal_open_terminate(&self.internal_open_terminates, msg.client_order_id)
        else {
            return false;
        };
        info!(
            "trade ws client id={} internal open terminate hit stage={} exchange={} req_type={:?} client_order_id={} trigger_ts={}",
            self.id,
            stage,
            self.exchange,
            msg.req_type,
            msg.client_order_id,
            state.trigger_ts
        );
        record_internal_open_terminate_summary(&self.internal_open_terminate_summary, msg);
        let _ = self.resp_sink.send(internal_open_terminated_outcome(
            msg.req_type,
            msg.client_order_id,
            self.exchange,
            state,
            stage,
        ));
        true
    }

    async fn send_one(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        msg: &TradeRequestMsg,
    ) -> Result<()> {
        let transport_id = self.next_transport_id();
        let payload = self.build_payload(msg, transport_id).await?;
        if self.exchange == Exchange::Gate {
            debug!(
                "trade ws client id={} exchange={} sending order client_order_id={} transport_id={} payload_bytes={}",
                self.id,
                self.exchange,
                msg.client_order_id,
                transport_id,
                payload.len()
            );
        } else if self.exchange == Exchange::Binance {
            debug!(
                "trade ws client id={} exchange={} sending {} req_type={:?} client_order_id={} transport_id={} payload_bytes={}",
                self.id,
                self.exchange,
                Self::binance_trade_action(msg.req_type),
                msg.req_type,
                msg.client_order_id,
                transport_id,
                payload.len()
            );
        }
        let ws_send_start_time_us = get_timestamp_us();
        ws.send(Message::Text(payload)).await?;
        let sent_at = std::time::Instant::now();
        let sent_at_us = get_timestamp_us();
        let ipc_recv_to_ws_send_done_us = msg
            .ipc_recv
            .map(|t0| sent_at.saturating_duration_since(t0).as_micros() as i64);
        if let Some(us) = ipc_recv_to_ws_send_done_us {
            match classify_ws_action(msg.req_type) {
                WsAction::New => self.lat_buckets.new.borrow_mut().push(us as f64),
                WsAction::Cancel => self.lat_buckets.cancel.borrow_mut().push(us as f64),
                WsAction::Other => {}
            }
        }
        self.track_inflight(
            msg,
            transport_id,
            ws_send_start_time_us,
            sent_at,
            sent_at_us,
            ipc_recv_to_ws_send_done_us,
        );
        Ok(())
    }

    async fn send_one_query(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        msg: &QueryRequestMsg,
    ) -> Result<()> {
        let transport_id = self.next_transport_id();
        let payload = self.build_query_payload(msg, transport_id)?;
        if self.exchange == Exchange::Gate {
            info!(
                "trade ws client id={} exchange={} sending query client_query_id={} transport_id={} payload: {}",
                self.id, self.exchange, msg.client_query_id, transport_id, payload
            );
        } else if self.exchange == Exchange::Binance {
            info!(
                "trade ws client id={} exchange={} sending {} req_type={:?} client_query_id={} transport_id={} payload_bytes={}",
                self.id,
                self.exchange,
                Self::binance_query_action(msg.req_type),
                msg.req_type,
                msg.client_query_id,
                transport_id,
                payload.len()
            );
        }
        ws.send(Message::Text(payload)).await?;
        self.track_inflight_query(msg, transport_id);
        Ok(())
    }

    fn binance_trade_action(req_type: TradeRequestType) -> &'static str {
        match req_type {
            TradeRequestType::BinanceWsNewUMOrder | TradeRequestType::BinanceWsNewMarginOrder => {
                "new_order"
            }
            TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => "cancel_order",
            _ => "trade_request",
        }
    }

    fn binance_query_action(req_type: QueryRequestType) -> &'static str {
        match req_type {
            QueryRequestType::BinanceWsUMQuery | QueryRequestType::BinanceWsMarginQuery => {
                "order_query"
            }
            _ => "query_request",
        }
    }

    async fn build_payload(&mut self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        if self.use_ltp_backend {
            return self.build_ltp_payload(msg, transport_id);
        }
        match self.exchange {
            Exchange::Bitget => self.build_bitget_payload(msg, transport_id),
            Exchange::Bybit => self.build_bybit_payload(msg, transport_id),
            Exchange::Okex => self.build_okex_payload(msg, transport_id).await,
            Exchange::Gate => self.build_gate_payload(msg, transport_id),
            Exchange::Binance => self.build_binance_payload(msg, transport_id),
            _ => {
                let params_b64 = BASE64_STANDARD.encode(&msg.params);
                let payload = json!({
                    "transport": "ws",
                    "reqType": msg.req_type as u32,
                    "clientOrderId": msg.client_order_id,
                    "createTime": msg.create_time,
                    "paramsB64": params_b64,
                });
                serde_json::to_string(&payload).with_context(|| "serialize ws payload")
            }
        }
    }

    fn build_bitget_payload(&self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        bitget_ws::build_order_payload(msg, transport_id)
    }

    fn build_ltp_payload(&self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        ltp_ws::build_order_payload(self.logical_exchange, msg, transport_id)
    }

    fn build_query_payload(&self, msg: &QueryRequestMsg, transport_id: i64) -> Result<String> {
        if self.use_ltp_backend {
            return Err(anyhow!(
                "LTP backend query websocket is not implemented for {:?}",
                msg.req_type
            ));
        }
        match self.exchange {
            Exchange::Gate => gate_ws::build_query_payload(msg, transport_id),
            Exchange::Binance => self.build_binance_query_payload(msg, transport_id),
            _ => Err(anyhow!(
                "unsupported query ws payload for exchange {:?}",
                self.exchange
            )),
        }
    }

    fn build_bybit_payload(&self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        let req_id = transport_id.to_string();
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        match msg.req_type {
            TradeRequestType::BybitNewMarginOrder | TradeRequestType::BybitNewUMOrder => {
                bybit::build_new_ws_json_from_parts(
                    msg.req_type,
                    msg.client_order_id,
                    &msg.params,
                    &req_id,
                    timestamp_ms,
                )
                .ok_or_else(|| {
                    anyhow!(
                        "failed to build bybit ws payload (req_type={:?}, client_order_id={})",
                        msg.req_type,
                        msg.client_order_id
                    )
                })
            }
            TradeRequestType::BybitCancelMarginOrder | TradeRequestType::BybitCancelUMOrder => {
                bybit::build_cancel_ws_json_from_parts(
                    msg.req_type,
                    &msg.params,
                    &req_id,
                    timestamp_ms,
                )
                .ok_or_else(|| {
                    anyhow!(
                        "failed to build bybit ws payload (req_type={:?}, client_order_id={})",
                        msg.req_type,
                        msg.client_order_id
                    )
                })
            }
            _ => Err(anyhow!(
                "failed to build bybit ws payload (req_type={:?}, client_order_id={})",
                msg.req_type,
                msg.client_order_id
            )),
        }
    }

    async fn build_okex_payload(
        &mut self,
        msg: &TradeRequestMsg,
        transport_id: i64,
    ) -> Result<String> {
        let inst_id_code = self.resolve_okex_inst_id_code_for_trade_msg(msg).await?;
        let payload = match msg.req_type {
            TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexNewUMOrder => {
                okex::build_new_ws_json_from_parts(
                    msg.req_type,
                    &msg.params,
                    inst_id_code,
                    transport_id,
                )
            }
            TradeRequestType::OkexCancelMarginOrder | TradeRequestType::OkexCancelUMOrder => {
                okex::build_cancel_ws_json_from_parts(
                    msg.req_type,
                    msg.client_order_id,
                    &msg.params,
                    inst_id_code,
                    transport_id,
                )
            }
            _ => None,
        };

        payload.ok_or_else(|| {
            anyhow!(
                "failed to build okex ws payload (req_type={:?}, client_order_id={})",
                msg.req_type,
                msg.client_order_id
            )
        })
    }

    async fn resolve_okex_inst_id_code_for_trade_msg(
        &mut self,
        msg: &TradeRequestMsg,
    ) -> Result<i64> {
        let inst_type = Self::okex_inst_type_for_req(msg.req_type)?;
        let inst_id = match msg.req_type {
            TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexNewUMOrder => {
                OkexNewOrderParams::from_bytes(&msg.params)
                    .map(|params| params.symbol)
                    .ok_or_else(|| anyhow!("decode okex new order params failed"))?
            }
            TradeRequestType::OkexCancelMarginOrder | TradeRequestType::OkexCancelUMOrder => {
                OkexCancelOrderParams::from_bytes(&msg.params)
                    .map(|params| params.inst_id)
                    .ok_or_else(|| anyhow!("decode okex cancel order params failed"))?
            }
            _ => {
                return Err(anyhow!(
                    "unsupported okex trade request for instIdCode resolution: {:?}",
                    msg.req_type
                ));
            }
        };

        self.resolve_okex_inst_id_code(inst_type, &inst_id).await
    }

    fn okex_inst_type_for_req(req_type: TradeRequestType) -> Result<&'static str> {
        match req_type {
            TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexCancelMarginOrder => {
                Ok("MARGIN")
            }
            TradeRequestType::OkexNewUMOrder | TradeRequestType::OkexCancelUMOrder => Ok("SWAP"),
            _ => Err(anyhow!(
                "unsupported okex request type for instIdCode resolution: {:?}",
                req_type
            )),
        }
    }

    fn okex_inst_cache_key(inst_type: &str, inst_id: &str) -> String {
        format!("{inst_type}:{inst_id}")
    }

    async fn resolve_okex_inst_id_code(
        &mut self,
        inst_type: &'static str,
        inst_id: &str,
    ) -> Result<i64> {
        let cache_key = Self::okex_inst_cache_key(inst_type, inst_id);
        if let Some(code) = self.okex_inst_id_code_cache.get(&cache_key).copied() {
            return Ok(code);
        }

        self.refresh_okex_inst_id_code_cache(inst_type).await?;
        self.okex_inst_id_code_cache
            .get(&cache_key)
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "okex instIdCode not found after refresh: instType={} instId={}",
                    inst_type,
                    inst_id
                )
            })
    }

    async fn refresh_okex_inst_id_code_cache(&mut self, inst_type: &'static str) -> Result<()> {
        if self.okex_loaded_inst_types.contains(inst_type) {
            return Ok(());
        }

        let client = self
            .okex_http_client
            .get_or_insert_with(reqwest::Client::new);
        let url = format!(
            "https://www.okx.com/api/v5/public/instruments?instType={}",
            inst_type
        );
        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("fetch okex instruments failed: instType={inst_type}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .with_context(|| format!("read okex instruments body failed: instType={inst_type}"))?;
        if !status.is_success() {
            return Err(anyhow!(
                "fetch okex instruments failed: instType={} status={} body={}",
                inst_type,
                status.as_u16(),
                truncate_for_log(&body, 256)
            ));
        }

        let response: OkexInstrumentsResponse = serde_json::from_str(&body)
            .with_context(|| format!("parse okex instruments failed: instType={inst_type}"))?;
        if response.code != "0" {
            return Err(anyhow!(
                "okex instruments api error: instType={} code={} msg={}",
                inst_type,
                response.code,
                response.msg
            ));
        }

        let mut loaded = 0usize;
        for inst in response.data {
            let Some(inst_id_code) = Self::parse_okex_inst_id_code_value(&inst.inst_id_code) else {
                continue;
            };
            let cache_key = Self::okex_inst_cache_key(inst_type, &inst.inst_id);
            self.okex_inst_id_code_cache.insert(cache_key, inst_id_code);
            loaded += 1;
        }

        if loaded == 0 {
            return Err(anyhow!(
                "okex instruments response did not include any instIdCode values: instType={}",
                inst_type
            ));
        }

        self.okex_loaded_inst_types.insert(inst_type);
        info!(
            "trade ws client id={} loaded okex instIdCode cache: instType={} entries={}",
            self.id, inst_type, loaded
        );
        Ok(())
    }

    fn parse_okex_inst_id_code_value(value: &Value) -> Option<i64> {
        if let Some(n) = value.as_i64() {
            return Some(n);
        }
        if let Some(n) = value.as_u64() {
            return i64::try_from(n).ok();
        }
        value.as_str()?.parse::<i64>().ok()
    }

    fn build_gate_payload(&self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        gate_ws::build_api_payload(msg, transport_id)
    }

    fn build_binance_payload(&self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        let creds = self
            .binance_creds
            .as_ref()
            .ok_or_else(|| anyhow!("missing binance ws credentials"))?;
        let signer = self
            .binance_ws_signer
            .as_ref()
            .ok_or_else(|| anyhow!("missing binance ws signer"))?;
        binance_ws::build_order_payload(msg, transport_id, creds.key.trim(), signer)
    }

    fn build_binance_query_payload(
        &self,
        msg: &QueryRequestMsg,
        transport_id: i64,
    ) -> Result<String> {
        let creds = self
            .binance_creds
            .as_ref()
            .ok_or_else(|| anyhow!("missing binance ws credentials"))?;
        let signer = self
            .binance_ws_signer
            .as_ref()
            .ok_or_else(|| anyhow!("missing binance ws signer"))?;
        binance_ws::build_query_payload(msg, transport_id, creds.key.trim(), signer)
    }

    fn next_transport_id(&mut self) -> i64 {
        let id = self.next_binance_transport_id;
        self.next_binance_transport_id = self.next_binance_transport_id.saturating_add(1);
        id
    }

    fn track_inflight(
        &mut self,
        msg: &TradeRequestMsg,
        transport_id: i64,
        _ws_send_start_time_us: i64,
        sent_at: std::time::Instant,
        sent_at_us: i64,
        _ipc_recv_to_ws_send_done_us: Option<i64>,
    ) {
        let flags = Self::inflight_request_flags(msg);
        self.inflight.insert(
            transport_id,
            TradeInflightMeta {
                req_type: msg.req_type,
                client_order_id: msg.client_order_id,
                ws_open_update_enabled: flags.ws_open_update_enabled,
                sent_at,
                sent_at_us,
            },
        );
    }

    fn inflight_request_flags(msg: &TradeRequestMsg) -> InflightRequestFlags {
        match msg.req_type {
            TradeRequestType::BinanceWsNewUMOrder => {
                let Some(params) = BinanceNewOrderParamsRef::from_bytes(&msg.params) else {
                    return InflightRequestFlags::default();
                };
                InflightRequestFlags {
                    ws_open_update_enabled: params.order_type.is_limit(),
                }
            }
            TradeRequestType::BinanceWsNewMarginOrder => InflightRequestFlags {
                ws_open_update_enabled: BinanceNewOrderParamsRef::from_bytes(&msg.params)
                    .map(|params| params.ws_margin_limit_maker && params.order_type.is_limit())
                    .or_else(|| {
                        std::str::from_utf8(&msg.params)
                            .ok()
                            .map(|raw| raw.contains("price=") && !raw.contains("timeInForce="))
                    })
                    .unwrap_or(false),
            },
            _ => InflightRequestFlags {
                ws_open_update_enabled: Self::ws_open_update_enabled_for_request(msg),
            },
        }
    }

    fn is_supported_ws_open_update_req_type(req_type: TradeRequestType) -> bool {
        matches!(
            req_type,
            TradeRequestType::BinanceWsNewUMOrder
                | TradeRequestType::BinanceWsNewMarginOrder
                | TradeRequestType::BybitNewMarginOrder
                | TradeRequestType::BybitNewUMOrder
                | TradeRequestType::OkexNewMarginOrder
                | TradeRequestType::OkexNewUMOrder
                | TradeRequestType::GateUnifiedNewOrder
                | TradeRequestType::GateFuturesNewOrder
        )
    }

    fn ws_open_update_enabled_for_request(msg: &TradeRequestMsg) -> bool {
        match msg.req_type {
            TradeRequestType::BinanceWsNewUMOrder => {
                BinanceNewOrderParamsRef::from_bytes(&msg.params)
                    .map(|params| params.order_type.is_limit())
                    .or_else(|| {
                        std::str::from_utf8(&msg.params)
                            .ok()
                            .map(|raw| raw.contains("timeInForce=GTX"))
                    })
                    .unwrap_or(false)
            }
            TradeRequestType::BinanceWsNewMarginOrder => {
                BinanceNewOrderParamsRef::from_bytes(&msg.params)
                    .map(|params| params.ws_margin_limit_maker && params.order_type.is_limit())
                    .or_else(|| {
                        std::str::from_utf8(&msg.params)
                            .ok()
                            .map(|raw| raw.contains("price=") && !raw.contains("timeInForce="))
                    })
                    .unwrap_or(false)
            }
            TradeRequestType::BybitNewMarginOrder | TradeRequestType::BybitNewUMOrder => {
                bybit::BybitNewOrderParams::ref_from_bytes(&msg.params)
                    .map(|params| params.order_type.is_limit() && params.price_qv.get_count() > 0)
                    .unwrap_or(false)
            }
            TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexNewUMOrder => {
                OkexNewOrderParams::from_bytes(&msg.params)
                    .map(|params| {
                        matches!(
                            params.order_type,
                            crate::okex::OkexOrderType::PostOnly
                                | crate::okex::OkexOrderType::MmpAndPostOnly
                        )
                    })
                    .unwrap_or(false)
            }
            TradeRequestType::GateUnifiedNewOrder | TradeRequestType::GateFuturesNewOrder => {
                GateNewOrderParamsRef::from_bytes(&msg.params)
                    .map(|params| params.order_type.is_limit())
                    .unwrap_or(false)
            }
            TradeRequestType::BitgetNewMarginOrder | TradeRequestType::BitgetNewUMOrder => {
                BitgetNewOrderParamsRef::from_bytes(&msg.params)
                    .map(|params| params.order_type.is_limit())
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn should_publish_ws_open_update(
        req_type: TradeRequestType,
        status: u16,
        ws_open_update_enabled: bool,
    ) -> bool {
        if !Self::is_supported_ws_open_update_req_type(req_type) {
            return true;
        }
        if !(200..300).contains(&(status as u32)) {
            return true;
        }
        ws_open_update_enabled
    }

    fn track_inflight_query(&mut self, msg: &QueryRequestMsg, transport_id: i64) {
        self.query_inflight.insert(
            transport_id,
            QueryInflightMeta {
                req_type: msg.req_type,
                client_query_id: msg.client_query_id,
            },
        );
    }

    fn resolve_gate_query_identity(
        query_inflight: &mut FastHashMap<i64, QueryInflightMeta>,
        default_req_type: QueryRequestType,
        transport_id: i64,
    ) -> (QueryRequestType, i64) {
        if transport_id > 0 {
            if let Some(meta) = query_inflight.remove(&transport_id) {
                return (meta.req_type, meta.client_query_id);
            }
            return (default_req_type, transport_id);
        }
        (default_req_type, 0)
    }

    fn take_trade_inflight_by_transport_id(
        &mut self,
        transport_id: i64,
    ) -> Option<TradeInflightMeta> {
        if transport_id <= 0 {
            return None;
        }
        self.inflight.remove(&transport_id)
    }

    fn take_trade_inflight_by_client_order_id(
        &mut self,
        client_order_id: i64,
    ) -> Option<TradeInflightMeta> {
        if client_order_id <= 0 {
            return None;
        }
        let transport_id = self.inflight.iter().find_map(|(transport_id, meta)| {
            (meta.client_order_id == client_order_id).then_some(*transport_id)
        })?;
        self.inflight.remove(&transport_id)
    }

    fn take_trade_inflight(
        &mut self,
        transport_id: Option<i64>,
        client_order_id: Option<i64>,
    ) -> Option<TradeInflightMeta> {
        transport_id
            .and_then(|id| self.take_trade_inflight_by_transport_id(id))
            .or_else(|| {
                client_order_id
                    .and_then(|client_id| self.take_trade_inflight_by_client_order_id(client_id))
            })
    }

    fn warn_uncorrelated_trade_payload(&self, payload: &str) {
        warn!(
            "trade ws client id={} exchange={:?} dropping uncorrelated payload: {}",
            self.id,
            self.exchange,
            truncate_for_log(&payload.replace(['\r', '\n'], " "), 512)
        );
    }

    fn warn_uncorrelated_binary_payload(&self, len: usize) {
        warn!(
            "trade ws client id={} exchange={:?} dropping uncorrelated non-utf8 payload: {} bytes",
            self.id, self.exchange, len
        );
    }

    fn notify_rejected(&self, msg: &TradeRequestMsg, reason: &str) {
        let body = json!({
            "transport": "ws",
            "state": "rejected",
            "reason": reason,
            "clientOrderId": msg.client_order_id,
            "endpointId": self.id,
            "localIp": self.local_ip.to_string(),
        })
        .to_string();
        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type: msg.req_type,
            client_order_id: msg.client_order_id,
            status: 429,
            body,
            exchange: self.exchange,
            order_id: 0,
            order_status_u8: 0,
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn notify_query_rejected(&self, msg: &QueryRequestMsg) {
        self.publish_query_error(msg.req_type, msg.client_query_id);
    }

    async fn handle_incoming(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        msg: Message,
    ) -> Result<()> {
        match msg {
            Message::Text(text) => {
                self.process_incoming_payload(&text);
                if self.exchange == Exchange::Binance && self.in_rate_limit_cooldown() {
                    let _ = ws.close(None).await;
                    return Err(anyhow!("binance ws endpoint cooling down after rate limit"));
                }
            }
            Message::Binary(bin) => match std::str::from_utf8(&bin) {
                Ok(text) => {
                    self.process_incoming_payload(text);
                    if self.exchange == Exchange::Binance && self.in_rate_limit_cooldown() {
                        let _ = ws.close(None).await;
                        return Err(anyhow!("binance ws endpoint cooling down after rate limit"));
                    }
                }
                Err(_) => {
                    self.warn_uncorrelated_binary_payload(bin.len());
                }
            },
            Message::Ping(data) => {
                debug!("trade ws client id={} received ping", self.id);
                ws.send(Message::Pong(data)).await?;
            }
            Message::Pong(_) => {
                debug!("trade ws client id={} received pong", self.id);
            }
            Message::Close(frame) => {
                return Err(anyhow!("{}", format_close_frame(frame.as_ref())));
            }
            Message::Frame(_) => {}
        }
        Ok(())
    }

    fn process_incoming_payload(&mut self, payload: &str) {
        if self.use_ltp_backend {
            if ltp_ws::is_text_pong(payload) {
                debug!("trade ws client id={} LTP received pong", self.id);
                return;
            }
            if self.handle_ltp_payload(payload) {
                return;
            }
        }

        if self.exchange == Exchange::Bitget {
            if is_bitget_pong_response(payload) {
                self.bitget_waiting_pong = false;
                debug!("trade ws client id={} Bitget received pong", self.id);
                return;
            }
            if let Some(resp) = bitget_ws::BitgetWsOrderResponse::from_json_str(payload) {
                let Some(meta) = self.take_trade_inflight(Some(resp.id), resp.client_order_id())
                else {
                    self.warn_uncorrelated_trade_payload(payload);
                    return;
                };
                let client_order_id = resp.client_order_id().unwrap_or(meta.client_order_id);
                // Bitget 仅暴露顶层 `ts`（ms），统一模型里 T2=T3=ts。仅成功响应采样。
                if resp.is_success() && resp.ts_ms > 0 {
                    let ts_us = resp.ts_ms.saturating_mul(1000);
                    self.lat_buckets.record_resp(
                        classify_ws_action(meta.req_type),
                        meta.sent_at,
                        meta.sent_at_us,
                        ts_us,
                        ts_us,
                    );
                }
                if resp.is_cancel() {
                    // Bitget cancel-order success lacks sufficient order-state detail; rely on
                    // account stream for terminal state reconciliation and only keep error payloads.
                    if !resp.is_success() {
                        self.publish_bitget_ws_response(
                            client_order_id,
                            meta.req_type,
                            meta.ws_open_update_enabled,
                            &resp,
                        );
                    }
                    return;
                }
                self.publish_bitget_ws_response(
                    client_order_id,
                    meta.req_type,
                    meta.ws_open_update_enabled,
                    &resp,
                );
                return;
            }
            if let Some((event, code, msg, success)) = parse_bitget_control_event(payload) {
                if event.eq_ignore_ascii_case("login") {
                    if success {
                        info!("trade ws client id={} Bitget login successful", self.id);
                    } else {
                        warn!(
                            "trade ws client id={} Bitget login failed: event={} code={} msg={} payload={}",
                            self.id, event, code, msg, payload
                        );
                    }
                } else if !success && (!code.is_empty() || !msg.is_empty()) {
                    warn!(
                        "trade ws client id={} Bitget control event failed: event={} code={} msg={} payload={}",
                        self.id, event, code, msg, payload
                    );
                } else {
                    debug!(
                        "trade ws client id={} Bitget control event: {}",
                        self.id, payload
                    );
                }
                return;
            }
        }

        if self.exchange == Exchange::Bybit {
            if is_bybit_pong_response(payload) {
                self.bybit_waiting_pong = false;
                return;
            }

            if let Some(success) = parse_bybit_auth_response(payload) {
                self.bybit_waiting_auth = false;
                if success {
                    info!(
                        "trade ws client id={} Bybit auth successful: {}",
                        self.id, payload
                    );
                } else {
                    warn!(
                        "trade ws client id={} Bybit auth failed: {}",
                        self.id, payload
                    );
                }
                return;
            }
        }

        // 处理 OKEx 的 notice 消息（服务升级通知等）
        if self.exchange == Exchange::Okex {
            if let Ok(json_val) = serde_json::from_str::<Value>(payload) {
                if let Some(event) = json_val.get("event").and_then(|v| v.as_str()) {
                    // OKX will send control-plane events (login/subscribe/error/notice) with no clOrdId.
                    // Those should not be forwarded into the trade response stream (otherwise they look like
                    // "order responses" with client_order_id=0).
                    if event.eq_ignore_ascii_case("notice") {
                        let code = json_val
                            .get("code")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let msg = json_val
                            .get("msg")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        warn!(
                            "trade ws client id={} received OKEx notice: code={}, msg={}",
                            self.id, code, msg
                        );
                        // code=64008 表示服务升级，需要重连
                        if code == "64008" {
                            warn!(
                                "trade ws client id={} service upgrade notice, will reconnect",
                                self.id
                            );
                            self.should_reconnect = true; // 标记需要重连
                        }
                        return;
                    }
                    // 处理登录响应
                    if event.eq_ignore_ascii_case("login") {
                        let code = json_val
                            .get("code")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        if code == "0" {
                            info!("trade ws client id={} OKEx login successful", self.id);
                        } else {
                            let msg = json_val
                                .get("msg")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default();
                            let now_s = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            warn!(
                                "trade ws client id={} OKEx login failed: code={}, msg={} okx_ts={:?} local_unix_s={} local_unix_ms={}",
                                self.id,
                                code,
                                msg,
                                self.last_okex_login_ts.as_deref(),
                                now_s,
                                now_ms
                            );
                        }
                        return;
                    }

                    if event.eq_ignore_ascii_case("error") {
                        let code = json_val
                            .get("code")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let msg = json_val
                            .get("msg")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let now_s = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        warn!(
                            "trade ws client id={} OKEx error event: code={}, msg={} okx_ts={:?} local_unix_s={} local_unix_ms={}",
                            self.id,
                            code,
                            msg,
                            self.last_okex_login_ts.as_deref(),
                            now_s,
                            now_ms
                        );
                        if code == "60006" {
                            warn!(
                                "trade ws client id={} OKEx reports timestamp expired; check system clock/NTP",
                                self.id
                            );
                        }

                        // 从 error 事件中提取 transport_id，并回查业务 client_order_id
                        let transport_id = json_val
                            .get("id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(0);

                        if transport_id > 0 {
                            // 解析错误码
                            let error_code = code.parse::<i32>().unwrap_or(0);
                            let Some(meta) = self.take_trade_inflight_by_transport_id(transport_id)
                            else {
                                self.warn_uncorrelated_trade_payload(payload);
                                return;
                            };

                            // 发送错误响应
                            let outcome = TradeExecOutcome {
                                req_type: meta.req_type,
                                exchange: self.exchange,
                                client_order_id: meta.client_order_id,
                                status: 400, // Bad Request
                                body: msg.to_string(),
                                order_id: 0,
                                order_status_u8: 0,
                                order_update_time: 0,
                                executed_qty: 0.0,
                                response_price: 0.0,
                            };

                            let _ = self.resp_sink.send(outcome);
                            info!(
                                "trade ws client id={} sent error response to strategy: client_order_id={} error_code={}",
                                self.id, meta.client_order_id, error_code
                            );
                        }

                        return;
                    }

                    debug!(
                        "trade ws client id={} OKEx event ignored: event={}",
                        self.id, event
                    );
                    return;
                }
            }
        }

        if self.exchange == Exchange::Gate && self.handle_gate_payload(payload) {
            return;
        }

        if self.exchange == Exchange::Binance && self.handle_binance_payload(payload) {
            return;
        }

        if self.exchange == Exchange::Bybit {
            if let Some(resp) = BybitWsOrderResponse::from_json_str(payload) {
                let Some(meta) =
                    self.take_trade_inflight(resp.transport_id(), resp.client_order_id())
                else {
                    self.warn_uncorrelated_trade_payload(payload);
                    return;
                };
                let client_order_id = resp.client_order_id().unwrap_or(meta.client_order_id);
                if resp.order_link_id.is_empty() {
                    if resp.ret_code == 0 {
                        warn!(
                            "trade ws client id={} bybit response missing orderLinkId: req_type={:?} req_id={} client_order_id={} ret_code={} ret_msg={}",
                            self.id,
                            meta.req_type,
                            resp.req_id,
                            client_order_id,
                            resp.ret_code,
                            resp.ret_msg
                        );
                    } else {
                        debug!(
                            "trade ws client id={} bybit error response missing orderLinkId: req_type={:?} req_id={} client_order_id={} ret_code={} ret_msg={}",
                            self.id,
                            meta.req_type,
                            resp.req_id,
                            client_order_id,
                            resp.ret_code,
                            resp.ret_msg
                        );
                    }
                }
                // Bybit 仅暴露一个服务端时间戳（顶层 `time` ms 或 header.Timenow ms），
                // 退化为 T2=T3。仅 ret_code==0 时采样。
                if resp.ret_code == 0 && resp.time_now_ms > 0 {
                    let ts_us = resp.time_now_ms.saturating_mul(1000);
                    self.lat_buckets.record_resp(
                        classify_ws_action(meta.req_type),
                        meta.sent_at,
                        meta.sent_at_us,
                        ts_us,
                        ts_us,
                    );
                }
                self.publish_bybit_ws_response(
                    client_order_id,
                    meta.req_type,
                    meta.ws_open_update_enabled,
                    &resp,
                );
                return;
            }
        }

        if self.exchange == Exchange::Okex {
            if let Some(resp) = OkexWsOrderResponse::from_json_str(payload) {
                let Some(meta) = self.take_trade_inflight(Some(resp.id), resp.client_order_id())
                else {
                    self.warn_uncorrelated_trade_payload(payload);
                    return;
                };
                let client_order_id = resp.client_order_id().unwrap_or(meta.client_order_id);
                // OKEx 顶层 `inTime` / `outTime` 直接是 μs；仅 `code==0` 时采样。
                if resp.code == 0 && resp.in_time_us > 0 && resp.out_time_us > 0 {
                    self.lat_buckets.record_resp(
                        classify_ws_action(meta.req_type),
                        meta.sent_at,
                        meta.sent_at_us,
                        resp.in_time_us,
                        resp.out_time_us,
                    );
                }
                self.publish_okex_ws_response(
                    client_order_id,
                    meta.req_type,
                    meta.ws_open_update_enabled,
                    &resp,
                );
                return;
            }
        }
        if let Some((req_type, client_order_id)) = self.extract_correlated(payload) {
            self.publish_generic_response(client_order_id, req_type, payload.to_string(), false);
        } else {
            self.warn_uncorrelated_trade_payload(payload);
        }
    }

    fn handle_ltp_payload(&mut self, payload: &str) -> bool {
        let Some(resp) = ltp_ws::LtpWsResponse::from_json_str(payload) else {
            return false;
        };

        if resp.is_login() {
            if resp.is_success() {
                info!(
                    "trade ws client id={} LTP login successful logical_exchange={}",
                    self.id, self.logical_exchange
                );
            } else {
                warn!(
                    "trade ws client id={} LTP login failed code={} msg={} payload={}",
                    self.id, resp.code, resp.msg, payload
                );
            }
            return true;
        }

        if resp.is_trade_ack() {
            let Some(meta) = self.take_trade_inflight(resp.id, resp.client_order_id_i64()) else {
                self.warn_uncorrelated_trade_payload(payload);
                return true;
            };
            let client_order_id = resp.client_order_id_i64().unwrap_or(meta.client_order_id);
            if resp.is_success() {
                self.publish_ltp_ws_response(client_order_id, &meta, &resp, false);
                self.keep_ltp_inflight_after_success_ack(resp.id, client_order_id, meta);
            } else {
                self.publish_ltp_ws_response(client_order_id, &meta, &resp, true);
            }
            return true;
        }

        if resp.is_order_push() {
            let Some(client_order_id) = resp.client_order_id_i64() else {
                self.warn_uncorrelated_trade_payload(payload);
                return true;
            };
            let Some(meta) = self.take_trade_inflight_by_client_order_id(client_order_id) else {
                debug!(
                    "trade ws client id={} LTP order push without inflight client_order_id={} payload={}",
                    self.id,
                    client_order_id,
                    truncate_for_log(&payload.replace(['\r', '\n'], " "), 512)
                );
                return true;
            };
            self.publish_ltp_ws_response(client_order_id, &meta, &resp, true);
            if !is_ltp_terminal_order_push(&resp) {
                self.keep_ltp_inflight_after_success_ack(None, client_order_id, meta);
            }
            return true;
        }

        false
    }

    fn keep_ltp_inflight_after_success_ack(
        &mut self,
        transport_id: Option<i64>,
        client_order_id: i64,
        meta: TradeInflightMeta,
    ) {
        let mut meta = meta;
        meta.client_order_id = client_order_id;
        if let Some(transport_id) = transport_id.filter(|id| *id > 0) {
            self.inflight.insert(transport_id, meta);
            return;
        }
        let key = self
            .inflight
            .keys()
            .copied()
            .max()
            .unwrap_or(self.next_binance_transport_id)
            .saturating_add(1);
        self.inflight.insert(key, meta);
    }

    fn publish_ltp_ws_response(
        &self,
        client_order_id: i64,
        meta: &TradeInflightMeta,
        resp: &ltp_ws::LtpWsResponse,
        force_publish: bool,
    ) {
        let status = ltp_ws::ltp_status_for_response(resp);
        if !force_publish
            && !Self::should_publish_ws_open_update(
                meta.req_type,
                status,
                meta.ws_open_update_enabled,
            )
        {
            return;
        }
        let body_payload = json!({
            "transport": "ws",
            "backend": "ltp",
            "exchange": self.logical_exchange.as_str(),
            "event": resp.event.as_deref(),
            "channel": resp.channel.as_deref(),
            "code": resp.error_code_for_trade_response(),
            "msg": resp.msg.as_str(),
            "data": resp.data.clone(),
            "endpointId": self.id,
            "localIp": self.local_ip.to_string(),
        })
        .to_string();
        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type: meta.req_type,
            client_order_id,
            status,
            body: body_payload,
            exchange: self.logical_exchange,
            order_id: resp.order_id_i64(),
            order_status_u8: resp.order_status_u8(),
            order_update_time: resp.order_update_time_ms(),
            executed_qty: resp.executed_qty(),
            response_price: resp.response_price(),
        });
    }

    fn publish_bybit_ws_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        ws_open_update_enabled: bool,
        resp: &BybitWsOrderResponse,
    ) {
        let status = if resp.ret_code == 0 { 206 } else { 400 };
        if !Self::should_publish_ws_open_update(req_type, status, ws_open_update_enabled) {
            return;
        }
        let body_payload = json!({
            "transport": "ws",
            "exchange": "bybit",
            "code": resp.ret_code,
            "msg": resp.ret_msg,
            "data": [{
                "orderId": resp.order_id,
                "orderLinkId": resp.order_link_id,
            }],
            "endpointId": self.id,
            "localIp": self.local_ip.to_string(),
        })
        .to_string();

        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status,
            body: body_payload,
            exchange: self.exchange,
            order_id: resp.order_id_i64(),
            order_status_u8: resp.order_status_u8(),
            order_update_time: resp.time_now_ms,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn publish_bitget_ws_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        ws_open_update_enabled: bool,
        resp: &bitget_ws::BitgetWsOrderResponse,
    ) {
        let status = if resp.is_success() { 206 } else { 400 };
        if !Self::should_publish_ws_open_update(req_type, status, ws_open_update_enabled) {
            return;
        }
        let body_payload = json!({
            "transport": "ws",
            "exchange": "bitget",
            "event": resp.event,
            "topic": resp.topic,
            "code": resp.code,
            "msg": resp.msg,
            "args": [{
                "orderId": resp.order_id,
                "clientOid": resp.client_oid,
                "cTime": resp.create_time_ms,
                "category": resp.category,
            }],
            "endpointId": self.id,
            "localIp": self.local_ip.to_string(),
        })
        .to_string();
        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status,
            body: body_payload,
            exchange: self.exchange,
            order_id: resp.order_id_i64(),
            order_status_u8: if resp.is_success() { 1 } else { 0 },
            order_update_time: resp.create_time_ms,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn extract_correlated(&mut self, payload: &str) -> Option<(TradeRequestType, i64)> {
        if let Ok(json_val) = serde_json::from_str::<Value>(payload) {
            if let Some(id) = Self::extract_client_order_id(&json_val) {
                if let Some(meta) = self.take_trade_inflight_by_transport_id(id) {
                    return Some((meta.req_type, meta.client_order_id));
                }
                if let Some(meta) = self.take_trade_inflight_by_client_order_id(id) {
                    return Some((meta.req_type, meta.client_order_id));
                }
            }
        }
        None
    }

    fn extract_client_order_id(val: &Value) -> Option<i64> {
        fn parse_i64_value(v: &Value) -> Option<i64> {
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
            if let Some(n) = v.as_u64() {
                return Some(n as i64);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
                }
                if let Some(rest) = s.strip_prefix("t-") {
                    if let Ok(parsed) = rest.parse::<i64>() {
                        return Some(parsed);
                    }
                }
            }
            None
        }

        for key in ["clientOrderId", "origClientOrderId", "id", "clOrdId"] {
            if let Some(id) = val.get(key) {
                if let Some(parsed) = parse_i64_value(id) {
                    return Some(parsed);
                }
            }
        }

        if let Some(arr) = val.get("data").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(id) = item.get("clOrdId").or_else(|| item.get("id")) {
                    if let Some(parsed) = parse_i64_value(id) {
                        return Some(parsed);
                    }
                }
            }
        }
        None
    }

    fn handle_gate_payload(&mut self, payload: &str) -> bool {
        let Ok(json_val) = serde_json::from_str::<Value>(payload) else {
            return false;
        };

        let channel = json_val
            .get("channel")
            .and_then(|v| v.as_str())
            .or_else(|| {
                json_val
                    .get("header")
                    .and_then(|h| h.get("channel"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("");

        if channel.eq_ignore_ascii_case("spot.login")
            || channel.eq_ignore_ascii_case("futures.login")
        {
            let errs = json_val
                .get("data")
                .and_then(|d| d.get("errs"))
                .and_then(|v| v.as_object());
            if let Some(errs) = errs {
                let label = errs.get("label").and_then(|v| v.as_str()).unwrap_or("");
                let msg = errs.get("message").and_then(|v| v.as_str()).unwrap_or("");
                warn!(
                    "trade ws client id={} Gate login failed: label={} msg={}",
                    self.id, label, msg
                );
            } else {
                info!(
                    "trade ws client id={} Gate login ok req_id={:?}",
                    self.id,
                    self.last_gate_login_req_id.as_deref()
                );
            }
            return true;
        }

        if channel.eq_ignore_ascii_case("spot.order_status")
            || channel.eq_ignore_ascii_case("futures.order_status")
        {
            return self.handle_gate_order_status(&json_val, payload, channel);
        }

        if channel != "spot.order_place"
            && channel != "spot.order_cancel"
            && channel != "futures.order_place"
            && channel != "futures.order_cancel"
        {
            return false;
        }

        if Self::extract_gate_ack(&json_val) == Some(true) {
            return true;
        }

        let Some(meta) = self.take_trade_inflight(
            Self::extract_gate_request_id(&json_val),
            Self::extract_gate_client_order_id(&json_val),
        ) else {
            self.warn_uncorrelated_trade_payload(payload);
            return true;
        };
        let client_order_id =
            Self::extract_gate_client_order_id(&json_val).unwrap_or(meta.client_order_id);
        // Gate header 给出 `x_in_time` / `x_out_time`（μs）；status==200 时采样。
        let header_status_ok = json_val
            .pointer("/header/status")
            .and_then(Self::extract_status_code)
            .map(|s| s == 200)
            .unwrap_or(false);
        if header_status_ok {
            let t2 = json_val
                .pointer("/header/x_in_time")
                .and_then(Self::extract_i64_lossy);
            let t3 = json_val
                .pointer("/header/x_out_time")
                .and_then(Self::extract_i64_lossy);
            if let (Some(t2), Some(t3)) = (t2, t3) {
                if t2 > 0 && t3 > 0 {
                    self.lat_buckets.record_resp(
                        classify_ws_action(meta.req_type),
                        meta.sent_at,
                        meta.sent_at_us,
                        t2,
                        t3,
                    );
                }
            }
        }
        self.publish_gate_ws_response(
            client_order_id,
            meta.req_type,
            meta.ws_open_update_enabled,
            &json_val,
            channel,
        );
        true
    }

    fn extract_status_code(v: &Value) -> Option<u16> {
        if let Some(n) = v.as_u64() {
            return u16::try_from(n).ok();
        }
        if let Some(n) = v.as_i64() {
            return u16::try_from(n).ok();
        }
        if let Some(s) = v.as_str() {
            return s.trim().parse::<u16>().ok();
        }
        None
    }

    fn extract_i64_lossy(v: &Value) -> Option<i64> {
        if let Some(n) = v.as_i64() {
            return Some(n);
        }
        if let Some(n) = v.as_u64() {
            return i64::try_from(n).ok();
        }
        if let Some(s) = v.as_str() {
            return s.trim().parse::<i64>().ok();
        }
        None
    }

    fn handle_binance_payload(&mut self, payload: &str) -> bool {
        let Some(resp) = binance_ws::parse_ws_response(payload) else {
            return false;
        };
        self.trigger_engine_shutdown_on_binance_rate_limit(
            resp.status,
            resp.error_code,
            payload,
            "binance_ws_response",
        );
        let id = resp.id.unwrap_or(0);
        if id <= 0 {
            return false;
        }

        if self.binance_session_logon_transport_id == Some(id) {
            self.binance_session_logon_transport_id = None;
            let status = Self::binance_status(&resp);
            let success =
                (200..300).contains(&(status as u32)) && resp.error_code.unwrap_or(0) == 0;
            if success {
                self.binance_waiting_session_logon = false;
                info!(
                    "trade ws client id={} exchange=binance session.logon successful transport_id={} status={} rate_limits={}",
                    self.id,
                    id,
                    status,
                    resp.rate_limits.len()
                );
            } else {
                warn!(
                    "trade ws client id={} exchange=binance session.logon failed transport_id={} status={} code={} msg={}",
                    self.id,
                    id,
                    status,
                    resp.error_code.unwrap_or(0),
                    resp.error_msg.as_deref().unwrap_or("")
                );
                self.should_reconnect = true;
            }
            return true;
        }

        if let Some(meta) = self.query_inflight.remove(&id) {
            self.publish_binance_query_response(meta.req_type, meta.client_query_id, &resp);
            return true;
        }

        let Some(meta) = self.take_trade_inflight_by_transport_id(id) else {
            self.warn_uncorrelated_trade_payload(payload);
            return true;
        };
        // Binance WS 仅暴露一个服务端时间戳 `result.updateTime`（ms），退化为 T2=T3。
        // 仅 status==200 且非 Binance Margin（现货）时采样——Margin 路径暂不参与响应 RTT 测量。
        let is_binance_margin = matches!(
            meta.req_type,
            TradeRequestType::BinanceWsNewMarginOrder
                | TradeRequestType::BinanceWsCancelMarginOrder
        );
        let (_, _, update_time_ms, _, _) = binance_ws::extract_order_info(&resp);
        if resp.status == Some(200) && !is_binance_margin {
            if update_time_ms > 0 {
                let ts_us = update_time_ms.saturating_mul(1000);
                self.lat_buckets.record_resp(
                    classify_ws_action(meta.req_type),
                    meta.sent_at,
                    meta.sent_at_us,
                    ts_us,
                    ts_us,
                );
            }
        }
        self.publish_binance_ws_response(
            meta.client_order_id,
            meta.req_type,
            meta.ws_open_update_enabled,
            &resp,
        );
        true
    }

    fn binance_status(resp: &binance_ws::BinanceWsResponse) -> u16 {
        match resp.status {
            Some(status) if status > 0 => status,
            _ if resp.error_code.is_some() => 400,
            _ => 206,
        }
    }

    fn is_binance_post_only_reject_response(
        req_type: TradeRequestType,
        resp: &binance_ws::BinanceWsResponse,
    ) -> bool {
        if !req_type.is_new_order() {
            return false;
        }
        if !matches!(resp.error_code, Some(-2010) | Some(-5022)) {
            return false;
        }
        let Some(msg) = resp.error_msg.as_deref() else {
            return resp.error_code == Some(-5022);
        };
        let msg = msg.to_ascii_lowercase();
        msg.contains("would immediately match and take") || msg.contains("post only")
    }

    fn publish_binance_ws_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        ws_open_update_enabled: bool,
        resp: &binance_ws::BinanceWsResponse,
    ) {
        let status = Self::binance_status(resp);
        if !Self::should_publish_ws_open_update(req_type, status, ws_open_update_enabled) {
            return;
        }
        let (order_id, order_status_u8, order_update_time, executed_qty, response_price) =
            binance_ws::extract_order_info(resp);
        let error_code = resp.error_code.unwrap_or(0);
        let suppress_error_log = Self::is_binance_post_only_reject_response(req_type, resp);
        if (200..300).contains(&(status as u32)) && error_code == 0 {
            debug!(
                "trade ws client id={} exchange=binance recv {} response req_type={:?} client_order_id={} status={} code={} order_id={} executed_qty={:.8}",
                self.id,
                Self::binance_trade_action(req_type),
                req_type,
                client_order_id,
                status,
                error_code,
                order_id,
                executed_qty
            );
        } else if !suppress_error_log {
            warn!(
                "trade ws client id={} exchange=binance recv {} response req_type={:?} client_order_id={} status={} code={} order_id={} executed_qty={:.8}",
                self.id,
                Self::binance_trade_action(req_type),
                req_type,
                client_order_id,
                status,
                error_code,
                order_id,
                executed_qty
            );
        }
        #[derive(serde::Serialize)]
        struct BinanceWsBody<'a> {
            transport: &'static str,
            exchange: &'static str,
            status: u16,
            code: i32,
            msg: &'a str,
            result: Option<&'a RawValue>,
            #[serde(rename = "endpointId")]
            endpoint_id: usize,
            #[serde(rename = "localIp")]
            local_ip: String,
        }
        let body = BinanceWsBody {
            transport: "ws",
            exchange: "binance",
            status: resp.status.unwrap_or(0),
            code: resp.error_code.unwrap_or(0),
            msg: resp.error_msg.as_deref().unwrap_or(""),
            result: resp.result.as_deref(),
            endpoint_id: self.id,
            local_ip: self.local_ip.to_string(),
        };
        let body_payload = match serde_json::to_string(&body) {
            Ok(payload) => payload,
            Err(err) => {
                warn!(
                    "trade ws client id={} exchange=binance failed to serialize response body: {}",
                    self.id, err
                );
                "{}".to_string()
            }
        };
        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status,
            body: body_payload,
            exchange: self.exchange,
            order_id,
            order_status_u8,
            order_update_time,
            executed_qty,
            response_price,
        });
    }

    fn publish_binance_query_response(
        &self,
        req_type: QueryRequestType,
        client_query_id: i64,
        resp: &binance_ws::BinanceWsResponse,
    ) {
        let status = Self::binance_status(resp);
        info!(
            "trade ws client id={} exchange=binance recv {} response req_type={:?} client_query_id={} status={} code={}",
            self.id,
            Self::binance_query_action(req_type),
            req_type,
            client_query_id,
            status,
            resp.error_code.unwrap_or(0)
        );
        if resp.error_code == Some(-2013) {
            self.publish_query_response(
                req_type,
                client_query_id,
                status,
                Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER),
            );
            return;
        }
        if !(200..300).contains(&(status as u32)) || resp.error_code.unwrap_or(0) != 0 {
            self.publish_query_error(req_type, client_query_id);
            return;
        }

        let Some(result) = resp.result.as_deref() else {
            self.publish_query_error(req_type, client_query_id);
            return;
        };
        // `result` 已是原始 JSON 分片，直接把切片喂给解析器，省掉 Value→String 往返。
        let result_json = result.get();

        let parsed = match req_type {
            QueryRequestType::BinanceWsMarginQuery => {
                parse_binance_margin_order_query_json(result_json)
            }
            _ => parse_binance_um_order_query_json(result_json),
        };

        if let Some(parsed) = parsed {
            self.publish_query_response(req_type, client_query_id, status, parsed.to_bytes());
        } else {
            self.publish_query_error(req_type, client_query_id);
        }
    }

    fn handle_gate_order_status(&mut self, json_val: &Value, payload: &str, channel: &str) -> bool {
        if Self::extract_gate_ack(json_val) == Some(true) {
            return true;
        }

        let default_req_type = if channel.eq_ignore_ascii_case("futures.order_status") {
            QueryRequestType::GateFuturesOrderQuery
        } else {
            QueryRequestType::GateUnifiedOrderQuery
        };

        let transport_id = Self::extract_gate_request_id(json_val).unwrap_or(0);
        let (req_type, client_query_id) = Self::resolve_gate_query_identity(
            &mut self.query_inflight,
            default_req_type,
            transport_id,
        );

        let status = Self::extract_gate_status(json_val);
        info!(
            "trade ws client id={} exchange=gate recv {} response req_type={:?} client_query_id={} transport_id={} status={}",
            self.id, channel, req_type, client_query_id, transport_id, status
        );
        if !(200..300).contains(&(status as u32)) {
            self.publish_query_error(req_type, client_query_id);
            return true;
        }

        let parsed = if channel.eq_ignore_ascii_case("futures.order_status") {
            parse_gate_futures_order_status_json(payload)
        } else {
            parse_gate_spot_order_status_json(payload)
        };

        if let Some(parsed) = parsed {
            self.publish_query_response(req_type, client_query_id, status, parsed.to_bytes());
        } else {
            self.publish_query_error(req_type, client_query_id);
        }

        true
    }

    fn extract_gate_ack(val: &Value) -> Option<bool> {
        val.get("ack").and_then(|v| v.as_bool()).or_else(|| {
            val.get("payload")
                .and_then(|v| v.get("ack"))
                .and_then(|v| v.as_bool())
        })
    }

    fn extract_gate_request_id(val: &Value) -> Option<i64> {
        fn parse_i64_value(v: &Value) -> Option<i64> {
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
            if let Some(n) = v.as_u64() {
                return Some(n as i64);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
                }
                if let Some(rest) = s.strip_prefix("t-") {
                    if let Ok(parsed) = rest.parse::<i64>() {
                        return Some(parsed);
                    }
                }
            }
            None
        }

        if let Some(id) = val.pointer("/payload/request_id").and_then(parse_i64_value) {
            return Some(id);
        }
        if let Some(id) = val.pointer("/payload/req_id").and_then(parse_i64_value) {
            return Some(id);
        }
        if let Some(id) = val.get("request_id").and_then(parse_i64_value) {
            return Some(id);
        }
        if let Some(id) = val.pointer("/header/request_id").and_then(parse_i64_value) {
            return Some(id);
        }
        if let Some(text) = val.pointer("/data/result/text").and_then(parse_i64_value) {
            return Some(text);
        }
        if let Some(order_id) = val
            .pointer("/data/result/order_id")
            .and_then(parse_i64_value)
        {
            return Some(order_id);
        }
        if let Some(order_id) = val.pointer("/data/result/id").and_then(parse_i64_value) {
            return Some(order_id);
        }
        None
    }

    fn extract_gate_client_order_id(val: &Value) -> Option<i64> {
        fn parse_i64_value(v: &Value) -> Option<i64> {
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
            if let Some(n) = v.as_u64() {
                return Some(n as i64);
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
                }
                if let Some(rest) = s.strip_prefix("t-") {
                    if let Ok(parsed) = rest.parse::<i64>() {
                        return Some(parsed);
                    }
                }
            }
            None
        }

        val.pointer("/data/result/text")
            .and_then(parse_i64_value)
            .or_else(|| {
                val.pointer("/payload/data/result/text")
                    .and_then(parse_i64_value)
            })
    }

    fn extract_gate_status(val: &Value) -> u16 {
        let status = val
            .get("header")
            .and_then(|h| h.get("status"))
            .and_then(|v| {
                if let Some(n) = v.as_u64() {
                    return u16::try_from(n).ok();
                }
                if let Some(n) = v.as_i64() {
                    return u16::try_from(n).ok();
                }
                if let Some(s) = v.as_str() {
                    return s.parse::<u16>().ok();
                }
                None
            })
            .unwrap_or(0);

        let has_errs = val.get("data").and_then(|d| d.get("errs")).is_some();

        if has_errs && (200..300).contains(&(status as u32)) {
            return 400;
        }

        if status == 0 {
            206
        } else {
            status
        }
    }

    fn extract_gate_order_id(val: &Value) -> i64 {
        fn parse_i64_value(v: &Value) -> Option<i64> {
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
            if let Some(n) = v.as_u64() {
                return Some(n as i64);
            }
            if let Some(s) = v.as_str() {
                return s.trim().parse::<i64>().ok();
            }
            None
        }

        val.pointer("/data/result/id")
            .and_then(parse_i64_value)
            .or_else(|| {
                val.pointer("/data/result/order_id")
                    .and_then(parse_i64_value)
            })
            .or_else(|| {
                val.pointer("/payload/data/result/id")
                    .and_then(parse_i64_value)
            })
            .or_else(|| {
                val.pointer("/payload/data/result/order_id")
                    .and_then(parse_i64_value)
            })
            .unwrap_or(0)
    }

    fn extract_gate_order_update_time_ms(val: &Value) -> i64 {
        fn parse_i64_ms(v: &Value) -> Option<i64> {
            if let Some(n) = v.as_i64() {
                if n >= 1_000_000_000_000 {
                    return Some(n);
                }
                return Some(n.saturating_mul(1_000));
            }
            if let Some(n) = v.as_u64() {
                let n = n as i64;
                if n >= 1_000_000_000_000 {
                    return Some(n);
                }
                return Some(n.saturating_mul(1_000));
            }
            if let Some(s) = v.as_str() {
                let s = s.trim();
                if let Ok(parsed_i64) = s.parse::<i64>() {
                    if parsed_i64 >= 1_000_000_000_000 {
                        return Some(parsed_i64);
                    }
                    return Some(parsed_i64.saturating_mul(1_000));
                }
                if let Ok(parsed_f64) = s.parse::<f64>() {
                    if parsed_f64 >= 1_000_000_000_000.0 {
                        return Some(parsed_f64 as i64);
                    }
                    return Some((parsed_f64 * 1_000.0).round() as i64);
                }
            }
            None
        }

        val.pointer("/data/result/update_time")
            .and_then(parse_i64_ms)
            .or_else(|| {
                val.pointer("/data/result/update_time_ms")
                    .and_then(parse_i64_ms)
            })
            .or_else(|| {
                val.pointer("/data/result/create_time")
                    .and_then(parse_i64_ms)
            })
            .or_else(|| val.pointer("/header/response_time").and_then(parse_i64_ms))
            .unwrap_or(0)
    }

    fn publish_gate_ws_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        ws_open_update_enabled: bool,
        resp: &Value,
        channel: &str,
    ) {
        let status = Self::extract_gate_status(resp);
        if !Self::should_publish_ws_open_update(req_type, status, ws_open_update_enabled) {
            return;
        }
        let order_id = Self::extract_gate_order_id(resp);
        let order_update_time = Self::extract_gate_order_update_time_ms(resp);
        let order_status_u8 = if req_type == TradeRequestType::GateUnifiedNewOrder
            || req_type == TradeRequestType::GateFuturesNewOrder
        {
            1
        } else {
            0
        };
        info!(
            "trade ws client id={} exchange=gate recv {} response req_type={:?} client_order_id={} status={} order_id={}",
            self.id, channel, req_type, client_order_id, status, order_id
        );
        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status,
            body: resp.to_string(),
            exchange: self.exchange,
            order_id,
            order_status_u8,
            order_update_time,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn publish_generic_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        body: String,
        is_base64: bool,
    ) {
        let body_payload = if is_base64 {
            json!({
                "transport": "ws",
                "encoding": "base64",
                "payload": body,
                "endpointId": self.id,
                "localIp": self.local_ip.to_string(),
            })
            .to_string()
        } else {
            json!({
                "transport": "ws",
                "encoding": "text",
                "payload": body,
                "endpointId": self.id,
                "localIp": self.local_ip.to_string(),
            })
            .to_string()
        };
        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status: 206,
            body: body_payload,
            exchange: self.exchange,
            order_id: 0,
            order_status_u8: 0,
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn publish_query_response(
        &self,
        req_type: QueryRequestType,
        client_query_id: i64,
        status: u16,
        body: Bytes,
    ) {
        let Some(tx) = &self.query_resp_sink else {
            return;
        };
        let _ = tx.send(QueryExecOutcome {
            req_type,
            client_query_id,
            status,
            body,
            exchange: self.exchange,
            ip_used_weight_1m: None,
            query_count_1m: None,
        });
    }

    fn publish_query_error(&self, req_type: QueryRequestType, client_query_id: i64) {
        self.publish_query_response(req_type, client_query_id, 400, Bytes::from_static(b"E"));
    }

    fn publish_okex_ws_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        ws_open_update_enabled: bool,
        resp: &OkexWsOrderResponse,
    ) {
        let status = if resp.code == 0
            && resp
                .data
                .as_ref()
                .map(|d| d.status_code == 0)
                .unwrap_or(true)
        {
            206
        } else {
            400
        };
        if !Self::should_publish_ws_open_update(req_type, status, ws_open_update_enabled) {
            return;
        }
        // Keep the body compact (no raw JSON), but keep a parseable structure for error extraction.
        let body_payload = json!({
            "transport": "ws",
            "exchange": "okex",
            "code": resp.code,
            "msg": resp.msg,
            "data": [{
                "sCode": resp.data.as_ref().map(|d| d.status_code).unwrap_or(0),
                "sMsg": resp.data.as_ref().map(|d| d.status_msg.as_str()).unwrap_or(""),
            }],
            "endpointId": self.id,
            "localIp": self.local_ip.to_string(),
        })
        .to_string();

        let _ = self.resp_sink.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status,
            body: body_payload,
            exchange: self.exchange,
            order_id: resp.order_id(),
            order_status_u8: resp.order_status_u8(),
            order_update_time: resp.order_update_time_ms(),
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    /// 评估当前连接的 TCP 丢包健康度：与上次 TCP_INFO 快照比较算本窗口 retrans 率，
    /// 喂入滚动窗口给出判定并日志（对齐 `ss`）。observe 只观测；act 的动作（软暂停
    /// 绕开 / 无 inflight 硬重连）在 Dispatch 路由模式下生效——见任务 #10。
    fn eval_tcp_health(&mut self, health: &mut TcpLossHealth, mode: TcpHealthMode) {
        let Some(fd) = self.current_tcp_fd else {
            return;
        };
        let Some(snap) = read_tcp_retrans_snapshot(fd) else {
            return;
        };
        let Some(prev) = self.tcp_health_last else {
            // 重连后第一次没有基线，只记快照。
            self.tcp_health_last = Some(snap);
            return;
        };
        let d_retrans = snap.total_retrans.wrapping_sub(prev.total_retrans) as u64;
        let d_data_segs = snap.data_segs_out.wrapping_sub(prev.data_segs_out) as u64;
        self.tcp_health_last = Some(snap);

        let can_reconnect_now = self.can_planned_reconnect_now();
        let now = std::time::Instant::now();
        let verdict = health.record(d_retrans, d_data_segs, can_reconnect_now, now);
        // act 动作：仅 mode==Act（已被 event_loop 收敛为「dispatch 路由才可能为 Act」）时施加。
        // Pause → 置 tcp_loss_pause_until（is_available 无条件绕开；坏则每 tick 续期）。
        // Reconnect → 同时置 pause 并触发 should_reconnect（event_loop 拆连重建=换源端口=换 ECMP 路径）。
        let mut acted = "none";
        if mode == TcpHealthMode::Act {
            match verdict {
                Verdict::Reconnect => {
                    let until = now + health.pause_duration();
                    self.endpoint_state.borrow_mut().tcp_loss_pause_until = Some(until);
                    self.should_reconnect = true;
                    acted = "reconnect";
                }
                Verdict::Pause => {
                    let until = now + health.pause_duration();
                    self.endpoint_state.borrow_mut().tcp_loss_pause_until = Some(until);
                    acted = "pause";
                }
                Verdict::Healthy => {}
            }
        }
        info!(
            "TcpHealth: endpoint_id={} exchange={} url={} local_ip={} remote_addr={:?} d_data_segs_out={} d_retrans={} window_bp={:?} total_retrans={} rtt_us={} rttvar_us={} mode={:?} verdict={:?} acted={}",
            self.id,
            self.exchange,
            self.url,
            self.local_ip,
            self.current_remote_addr,
            d_data_segs,
            d_retrans,
            health.window_bp(),
            snap.total_retrans,
            snap.rtt_us,
            snap.rttvar_us,
            mode,
            verdict,
            acted,
        );
    }

    async fn send_ping(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        debug!("trade ws client id={} sending ping", self.id);
        if self.use_ltp_backend {
            ws.send(Message::Text("ping".to_string())).await?;
        } else if self.exchange == Exchange::Bitget {
            self.bitget_waiting_pong = true;
            ws.send(Message::Text("ping".to_string())).await?;
        } else if self.exchange == Exchange::Bybit {
            self.bybit_waiting_pong = true;
            ws.send(Message::Text(
                json!({
                    "req_id": format!("ping-{}-{}", self.id, chrono::Utc::now().timestamp_millis()),
                    "op": "ping"
                })
                .to_string(),
            ))
            .await?;
        } else {
            ws.send(Message::Ping(Vec::new())).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        binance_ed25519_api_key_from_env, is_bitget_pong_response, parse_bitget_control_event,
        QueryInflightMeta, TradeWsClient,
    };
    use crate::binance_ws::BINANCE_ED25519_API_KEY_ENV;
    use crate::query_request::QueryRequestType;
    use crate::trade_request::{
        BinanceNewOrderParams, BitgetNewOrderParams, GateNewOrderParams, TradeRequestMsg,
        TradeRequestType,
    };
    use order_common::{OrderType, Side};
    use runtime_common::fast_hash::fast_hash_map;
    use signal_common::tick_math::QuantizedValue;
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn parses_bitget_login_failure_control_event() {
        let payload = r#"{"event":"login","code":"30005","msg":"Login failure"}"#;
        let parsed = parse_bitget_control_event(payload).expect("control event");
        assert_eq!(parsed.0, "login");
        assert_eq!(parsed.1, "30005");
        assert_eq!(parsed.2, "Login failure");
        assert!(!parsed.3);
    }

    #[test]
    fn ignores_bitget_trade_event_in_control_parser() {
        let payload = r#"{"event":"trade","topic":"place-order","code":"0","msg":"success"}"#;
        assert!(parse_bitget_control_event(payload).is_none());
    }

    #[test]
    fn recognizes_bitget_text_pong() {
        assert!(is_bitget_pong_response("pong"));
        assert!(is_bitget_pong_response(" pong "));
        assert!(!is_bitget_pong_response(r#"{"event":"pong"}"#));
    }

    #[test]
    fn binance_ed25519_api_key_has_no_hmac_fallback() {
        let _guard = env_test_lock();
        std::env::remove_var(BINANCE_ED25519_API_KEY_ENV);
        std::env::set_var("BINANCE_API_KEY", "hmac-key");
        let err = binance_ed25519_api_key_from_env().expect_err("ed25519 key must be required");
        assert!(err.to_string().contains(BINANCE_ED25519_API_KEY_ENV));

        std::env::set_var(BINANCE_ED25519_API_KEY_ENV, " ed-key ");
        assert_eq!(binance_ed25519_api_key_from_env().unwrap(), "ed-key");
        std::env::remove_var(BINANCE_ED25519_API_KEY_ENV);
        std::env::remove_var("BINANCE_API_KEY");
    }

    #[test]
    fn gate_order_status_uses_original_client_query_id() {
        let mut query_inflight = fast_hash_map();
        query_inflight.insert(
            5303,
            QueryInflightMeta {
                req_type: QueryRequestType::GateUnifiedOrderQuery,
                client_query_id: 7956522848229523457,
            },
        );

        let (req_type, client_query_id) = TradeWsClient::resolve_gate_query_identity(
            &mut query_inflight,
            QueryRequestType::GateFuturesOrderQuery,
            5303,
        );

        assert_eq!(req_type, QueryRequestType::GateUnifiedOrderQuery);
        assert_eq!(client_query_id, 7956522848229523457);
        assert!(query_inflight.is_empty());
    }

    #[test]
    fn gate_order_status_falls_back_to_transport_id_without_inflight() {
        let mut query_inflight = fast_hash_map();

        let (req_type, client_query_id) = TradeWsClient::resolve_gate_query_identity(
            &mut query_inflight,
            QueryRequestType::GateFuturesOrderQuery,
            281474976710779,
        );

        assert_eq!(req_type, QueryRequestType::GateFuturesOrderQuery);
        assert_eq!(client_query_id, 281474976710779);
    }

    #[test]
    fn ws_open_update_gate_uses_typed_params_only() {
        let typed_limit = GateNewOrderParams::request_bytes_from_parts(
            TradeRequestType::GateFuturesNewOrder,
            1,
            42,
            "BTC_USDT",
            Side::Buy,
            OrderType::Limit,
            QuantizedValue::from_parts(1, -3, 10),
            QuantizedValue::from_parts(1, 0, 100000),
            false,
            false,
        )
        .expect("typed gate request");
        let msg = TradeRequestMsg::parse(&typed_limit).expect("gate typed msg");
        assert!(TradeWsClient::ws_open_update_enabled_for_request(&msg));

        let typed_market = GateNewOrderParams::request_bytes_from_parts(
            TradeRequestType::GateFuturesNewOrder,
            1,
            43,
            "BTC_USDT",
            Side::Buy,
            OrderType::Market,
            QuantizedValue::from_parts(1, -3, 10),
            QuantizedValue::zero(),
            false,
            false,
        )
        .expect("typed gate request");
        let msg = TradeRequestMsg::parse(&typed_market).expect("gate typed msg");
        assert!(!TradeWsClient::ws_open_update_enabled_for_request(&msg));

        let raw_json = TradeRequestMsg::create(
            TradeRequestType::GateFuturesNewOrder,
            1,
            44,
            br#"{"contract":"BTC_USDT","tif":"poc"}"#,
        )
        .expect("raw gate msg");
        assert!(!TradeWsClient::ws_open_update_enabled_for_request(
            &raw_json
        ));
    }

    #[test]
    fn ws_open_update_bitget_uses_typed_params_only() {
        let typed_limit = BitgetNewOrderParams::request_bytes_from_parts(
            TradeRequestType::BitgetNewUMOrder,
            1,
            42,
            "BTCUSDT",
            Side::Buy,
            OrderType::Limit,
            QuantizedValue::from_parts(1, -3, 10),
            QuantizedValue::from_parts(1, 0, 100000),
            false,
        )
        .expect("typed bitget request");
        let msg = TradeRequestMsg::parse(&typed_limit).expect("bitget typed msg");
        assert!(TradeWsClient::ws_open_update_enabled_for_request(&msg));

        let typed_market = BitgetNewOrderParams::request_bytes_from_parts(
            TradeRequestType::BitgetNewUMOrder,
            1,
            43,
            "BTCUSDT",
            Side::Buy,
            OrderType::Market,
            QuantizedValue::from_parts(1, -3, 10),
            QuantizedValue::zero(),
            false,
        )
        .expect("typed bitget request");
        let msg = TradeRequestMsg::parse(&typed_market).expect("bitget typed msg");
        assert!(!TradeWsClient::ws_open_update_enabled_for_request(&msg));

        let raw_json = TradeRequestMsg::create(
            TradeRequestType::BitgetNewUMOrder,
            1,
            44,
            br#"{"symbol":"BTCUSDT","orderType":"limit"}"#,
        )
        .expect("raw bitget msg");
        assert!(!TradeWsClient::ws_open_update_enabled_for_request(
            &raw_json
        ));
    }

    #[test]
    fn binance_inflight_flags_use_single_typed_params_pass() {
        let limit_request = BinanceNewOrderParams::request_bytes_from_parts(
            TradeRequestType::BinanceWsNewUMOrder,
            1_000,
            42,
            "BTCUSDT",
            Side::Buy,
            OrderType::Limit,
            QuantizedValue::from_parts(1, -3, 250),
            QuantizedValue::from_parts(1, 0, 100000),
            false,
            false,
            false,
            true,
            false,
        )
        .expect("typed binance limit request");
        let limit_msg = TradeRequestMsg::parse(&limit_request).expect("binance typed msg");
        let flags = TradeWsClient::inflight_request_flags(&limit_msg);
        assert!(flags.ws_open_update_enabled);

        let market_request = BinanceNewOrderParams::request_bytes_from_parts(
            TradeRequestType::BinanceWsNewUMOrder,
            2_000,
            43,
            "ETHUSDT",
            Side::Sell,
            OrderType::Market,
            QuantizedValue::from_parts(1, -3, 500),
            QuantizedValue::zero(),
            true,
            false,
            false,
            true,
            false,
        )
        .expect("typed binance market request");
        let market_msg = TradeRequestMsg::parse(&market_request).expect("binance typed msg");
        let flags = TradeWsClient::inflight_request_flags(&market_msg);
        assert!(!flags.ws_open_update_enabled);
    }
}
