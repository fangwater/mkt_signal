use crate::binance_ws::{
    BINANCE_ED25519_API_KEY_ENV, BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE_ENV,
    BINANCE_ED25519_PRIVATE_KEY_PATH_ENV,
};
use crate::response_sink::TradeResponseSink;
use crate::trade_request::{
    BinanceCancelOrderParamsRef, BinanceNewOrderParamsRef, TradeRequestMsg, TradeRequestType,
};
use crate::trade_response_handle::TradeExecOutcome;
use crate::ws_client::trade_engine_tcp_tuning;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{NaiveDateTime, Utc};
use log::{debug, info, warn};
use mkt_parsers::msg::basic_account_msg::BinanceBasicOrderMsg;
use native_tls::TlsConnector as NativeTlsConnector;
use openssl::pkey::{Id as PKeyId, PKey, Private};
use openssl::sign::Signer;
use order_common::{OrderExecutionStatus, OrderType, Side};
use runtime_common::exchange::Exchange;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::socket_tuning::tune_tcp_stream;
use signal_common::tick_math::QuantizedValue;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio_native_tls::{TlsConnector, TlsStream};
use tokio_util::sync::CancellationToken;
use url::Url;

pub const BINANCE_SPOT_FIX_ENABLED_ENV: &str = "BINANCE_SPOT_FIX_ENABLED";

const SOH: char = '\x01';
const SOH_BYTE: u8 = 0x01;
const DEFAULT_FIX_OE_URL: &str = "tcp+tls://fix-oe.binance.com:9000";
const DEFAULT_TARGET_COMP_ID: &str = "SPOT";
const DEFAULT_HEARTBTINT: u64 = 30;
const DEFAULT_RECV_WINDOW_MS: &str = "5000";
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
// FIX OE 固定 2 条会话做 RR。官方限制：每账户最多 10 条并发连接、
// 连接尝试 15 次/30s，会话过多在集中重连时可能触发 -1034。
pub(crate) const FIX_OE_SESSIONS: usize = 2;

type FixField = (u32, String);
type BinanceFixStream = TlsStream<TcpStream>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixResponseMode {
    Everything,
    OnlyAcks,
}

impl FixResponseMode {
    fn tag_value(self) -> &'static str {
        match self {
            Self::Everything => "1",
            Self::OnlyAcks => "2",
        }
    }
}

#[derive(Debug)]
pub struct BinanceSpotFixConfig {
    api_key: String,
    private_key: PKey<Private>,
    url: String,
    sender_comp_id: String,
    target_comp_id: String,
    heartbtint: u64,
    recv_window_ms: String,
    source_ip: Option<IpAddr>,
    response_mode: FixResponseMode,
}

#[derive(Debug, Default)]
struct BinanceSpotFixRuntimeState {
    logged_on: bool,
    last_error: Option<String>,
    session_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct BinanceSpotFixHandle {
    tx: mpsc::UnboundedSender<TradeRequestMsg>,
    state: Rc<RefCell<BinanceSpotFixRuntimeState>>,
}

impl BinanceSpotFixHandle {
    pub fn is_available(&self) -> bool {
        self.state.borrow().logged_on
    }

    pub fn last_error(&self) -> Option<String> {
        self.state.borrow().last_error.clone()
    }

    pub fn enqueue(&self, msg: TradeRequestMsg) -> Result<(), TradeRequestMsg> {
        self.tx.send(msg).map_err(|err| err.0)
    }
}

/// cl_ord_id 即 `client_order_id`（本系统的 clOrdId 全部为整数），
/// seq_num 内嵌后按任一键删除都是 O(1)。
#[derive(Debug, Clone, Copy)]
struct InflightFixRequest {
    req_type: TradeRequestType,
    client_order_id: i64,
    orig_client_order_id: Option<i64>,
    seq_num: i64,
}

/// 在途请求登记：cl_ord_id 主索引 + orig->cancel、seq->cl_ord_id 两个辅助索引。
#[derive(Default)]
struct InflightFixTable {
    by_cl_ord_id: FastHashMap<i64, InflightFixRequest>,
    by_orig_cl_ord_id: FastHashMap<i64, i64>,
    by_seq: FastHashMap<i64, i64>,
}

impl InflightFixTable {
    fn new() -> Self {
        Self {
            by_cl_ord_id: fast_hash_map(),
            by_orig_cl_ord_id: fast_hash_map(),
            by_seq: fast_hash_map(),
        }
    }

    fn insert(&mut self, inflight: InflightFixRequest) {
        let key = inflight.client_order_id;
        if let Some(orig) = inflight.orig_client_order_id {
            self.by_orig_cl_ord_id.insert(orig, key);
        }
        self.by_seq.insert(inflight.seq_num, key);
        self.by_cl_ord_id.insert(key, inflight);
    }

    fn remove(&mut self, key: i64) -> Option<InflightFixRequest> {
        let inflight = self.by_cl_ord_id.remove(&key)?;
        if let Some(orig) = inflight.orig_client_order_id {
            self.by_orig_cl_ord_id.remove(&orig);
        }
        self.by_seq.remove(&inflight.seq_num);
        Some(inflight)
    }

    fn key_by_seq(&self, seq: i64) -> Option<i64> {
        self.by_seq.get(&seq).copied()
    }
}

struct BinanceSpotFixClient {
    config: BinanceSpotFixConfig,
    rx: mpsc::UnboundedReceiver<TradeRequestMsg>,
    sink: TradeResponseSink,
    shutdown: CancellationToken,
    state: Rc<RefCell<BinanceSpotFixRuntimeState>>,
    next_seq_num: i64,
    msg_writer: FixMessageWriter,
    time_fmt: FixTimeFormatter,
}

pub(crate) fn spot_fix_enabled_from_env() -> Result<bool> {
    let Ok(value) = std::env::var(BINANCE_SPOT_FIX_ENABLED_ENV) else {
        return Ok(false);
    };
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(false);
    }
    match normalized.as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => Err(anyhow!(
            "invalid {}='{}', expected on/off",
            BINANCE_SPOT_FIX_ENABLED_ENV,
            value
        )),
    }
}

pub(crate) fn is_binance_spot_fix_trade_request(req_type: TradeRequestType) -> bool {
    matches!(
        req_type,
        TradeRequestType::BinanceNewMarginOrder
            | TradeRequestType::BinanceCancelMarginOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder
    )
}

pub(crate) fn spawn_binance_spot_fix_client(
    config: BinanceSpotFixConfig,
    sink: TradeResponseSink,
    shutdown: CancellationToken,
) -> (BinanceSpotFixHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let state = Rc::new(RefCell::new(BinanceSpotFixRuntimeState::default()));
    let handle = BinanceSpotFixHandle {
        tx,
        state: state.clone(),
    };
    let mut client = BinanceSpotFixClient {
        config,
        rx,
        sink,
        shutdown,
        state,
        next_seq_num: 1,
        msg_writer: FixMessageWriter::with_capacity(512),
        time_fmt: FixTimeFormatter::new(),
    };
    let task = tokio::task::spawn_local(async move {
        client.run().await;
    });
    (handle, task)
}

impl BinanceSpotFixConfig {
    pub fn from_env(
        _api_key: String,
        default_source_ip: Option<IpAddr>,
        session_index: usize,
        session_count: usize,
    ) -> Result<Self> {
        let api_key = std::env::var(BINANCE_ED25519_API_KEY_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "{}=on requires {}",
                    BINANCE_SPOT_FIX_ENABLED_ENV,
                    BINANCE_ED25519_API_KEY_ENV
                )
            })?;
        let private_key_path = std::env::var(BINANCE_ED25519_PRIVATE_KEY_PATH_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "{}=on requires {}",
                    BINANCE_SPOT_FIX_ENABLED_ENV,
                    BINANCE_ED25519_PRIVATE_KEY_PATH_ENV
                )
            })?;
        let private_key = load_ed25519_private_key(&private_key_path)?;
        let url = std::env::var("BINANCE_FIX_OE_URL")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_FIX_OE_URL.to_string());
        // SenderCompID 必须在账户活跃会话间唯一；多会话时基于 env 值派生后缀。
        let sender_comp_id = match std::env::var("BINANCE_FIX_SENDER_COMP_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
        {
            Some(base) if session_count <= 1 => base,
            Some(base) => derive_session_comp_id(&base, session_index),
            None => generate_sender_comp_id(),
        };
        validate_comp_id("BINANCE_FIX_SENDER_COMP_ID", &sender_comp_id)?;
        let source_ip = match std::env::var("BINANCE_LOCAL_SOURCE_IP") {
            Ok(value) if !value.trim().is_empty() => Some(
                value
                    .trim()
                    .parse::<IpAddr>()
                    .with_context(|| format!("parse BINANCE_LOCAL_SOURCE_IP='{}'", value.trim()))?,
            ),
            _ => default_source_ip,
        };
        let recv_window_ms = std::env::var("BINANCE_RECV_WINDOW")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_RECV_WINDOW_MS.to_string());

        Ok(Self {
            api_key,
            private_key,
            url,
            sender_comp_id,
            target_comp_id: DEFAULT_TARGET_COMP_ID.to_string(),
            heartbtint: DEFAULT_HEARTBTINT,
            recv_window_ms,
            source_ip,
            response_mode: FixResponseMode::OnlyAcks,
        })
    }

    /// 影子 listener：独立 SenderCompID，Logon 用 EVERYTHING，不发单。
    pub fn from_env_listener(default_source_ip: Option<IpAddr>) -> Result<Self> {
        let mut config = Self::from_env(String::new(), default_source_ip, 0, 1)?;
        config.sender_comp_id = resolve_listener_sender_comp_id()?;
        config.response_mode = FixResponseMode::Everything;
        Ok(config)
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn sender_comp_id(&self) -> &str {
        &self.sender_comp_id
    }

    pub fn source_ip(&self) -> Option<IpAddr> {
        self.source_ip
    }
}

impl BinanceSpotFixClient {
    async fn run(&mut self) {
        while !self.shutdown.is_cancelled() {
            self.set_logged_out(None);
            self.next_seq_num = 1;
            match self.connect_and_logon().await {
                Ok(mut stream) => {
                    self.set_logged_on(None);
                    let session_result = self.process_session(&mut stream).await;
                    self.set_logged_out(None);
                    if let Err(err) = session_result {
                        warn!("Binance Spot FIX session ended: {err:#}");
                        self.set_logged_out(Some(format!("{err:#}")));
                    }
                }
                Err(err) => {
                    warn!("Binance Spot FIX connect/logon failed: {err:#}");
                    self.set_logged_out(Some(format!("{err:#}")));
                }
            }
            if self.shutdown.is_cancelled() {
                break;
            }
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                _ = tokio::time::sleep(RECONNECT_DELAY) => {}
            }
        }
        self.set_logged_out(Some("shutdown".to_string()));
        info!("Binance Spot FIX client stopped");
    }

    async fn connect_and_logon(&mut self) -> Result<BinanceFixStream> {
        let (host, port) = parse_fix_endpoint(&self.config.url)?;
        info!(
            "Binance Spot FIX connecting url={} sender_comp_id={} source_ip={}",
            self.config.url,
            self.config.sender_comp_id,
            self.config
                .source_ip
                .map(|ip| ip.to_string())
                .unwrap_or_else(|| "system-default".to_string())
        );
        let tcp = connect_tcp(&host, port, self.config.source_ip).await?;
        // 与 WS 下单连接相同的 socket 调优：NODELAY + QUICKACK + USER_TIMEOUT + SO_BUSY_POLL。
        // 缺 NODELAY 时 Nagle 会把 burst 中的第二条小包扣在内核等 ACK，直接抬高挂单延迟尾部。
        tune_tcp_stream(
            &tcp,
            "trade_engine binance_spot_fix",
            trade_engine_tcp_tuning(),
        );
        let connector = NativeTlsConnector::builder()
            .build()
            .context("build native TLS connector for Binance Spot FIX")?;
        let connector = TlsConnector::from(connector);
        let mut stream = connector
            .connect(&host, tcp)
            .await
            .with_context(|| format!("TLS connect Binance Spot FIX host={host}"))?;
        let mut buffer = FixReadBuffer::with_capacity(4096);
        let logon = self.build_logon()?;
        send_fix_message(&mut stream, "Logon<A>", &logon).await?;

        loop {
            let raw = read_fix_message(&mut stream, &mut buffer).await?;
            let msg = FixMessage::parse(&raw);
            match msg.msg_type() {
                Some("A") => {
                    let session_id = msg.get(25037).map(ToString::to_string);
                    info!(
                        "Binance Spot FIX logon successful sender_comp_id={} session_id={}",
                        self.config.sender_comp_id,
                        session_id.as_deref().unwrap_or("-")
                    );
                    self.state.borrow_mut().session_id = session_id;
                    return Ok(stream);
                }
                Some("1") => {
                    self.send_heartbeat(&mut stream, msg.get(112)).await?;
                }
                Some("3") | Some("5") => {
                    return Err(anyhow!(
                        "Binance Spot FIX logon rejected msg_type={} code={} text={}",
                        msg.msg_type().unwrap_or("?"),
                        msg.get(25016).or_else(|| msg.get(373)).unwrap_or("0"),
                        msg.get(58).unwrap_or("")
                    ));
                }
                other => {
                    debug!("Binance Spot FIX ignoring pre-logon msg_type={other:?}");
                }
            }
        }
    }

    async fn process_session(&mut self, stream: &mut BinanceFixStream) -> Result<()> {
        let mut buffer = FixReadBuffer::with_capacity(8192);
        let mut inflight = InflightFixTable::new();

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    let logout = self.build_logout("client shutdown");
                    let _ = send_fix_message(stream, "Logout<5>", &logout).await;
                    return Ok(());
                }
                maybe_msg = self.rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        return Ok(());
                    };
                    match self.build_order_request(&msg) {
                        Ok(entry) => {
                            let key = entry.client_order_id;
                            inflight.insert(entry);
                            if let Err(err) = send_fix_message(stream, "Order", self.msg_writer.message()).await {
                                if let Some(entry) = inflight.remove(key) {
                                    self.publish_transport_error(entry, format!("send Binance Spot FIX order failed: {err:#}"));
                                }
                                return Err(err);
                            }
                        }
                        Err(err) => {
                            warn!(
                                "invalid Binance Spot FIX request req_type={:?} client_order_id={} err={err:#}",
                                msg.req_type,
                                msg.client_order_id
                            );
                            self.publish_request_error(&msg, 400, err.to_string());
                        }
                    }
                }
                raw = read_fix_message(stream, &mut buffer) => {
                    let raw = raw?;
                    let msg = FixMessage::parse(&raw);
                    if !self.handle_incoming(stream, &msg, &mut inflight).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn handle_incoming(
        &mut self,
        stream: &mut BinanceFixStream,
        msg: &FixMessage,
        inflight: &mut InflightFixTable,
    ) -> Result<bool> {
        match msg.msg_type() {
            Some("0") => Ok(true),
            Some("1") => {
                self.send_heartbeat(stream, msg.get(112)).await?;
                Ok(true)
            }
            Some("5") => {
                warn!(
                    "Binance Spot FIX logout received text={}",
                    msg.get(58).unwrap_or("")
                );
                Ok(false)
            }
            Some("B") => {
                warn!(
                    "Binance Spot FIX news received text={}",
                    msg.get(58).unwrap_or("")
                );
                Ok(false)
            }
            Some("8") => {
                self.handle_execution_report(msg, inflight);
                Ok(true)
            }
            Some("9") => {
                self.handle_cancel_reject(msg, inflight);
                Ok(true)
            }
            Some("3") => {
                self.handle_reject(msg, inflight);
                Ok(true)
            }
            other => {
                debug!("Binance Spot FIX ignoring msg_type={other:?}");
                Ok(true)
            }
        }
    }

    fn handle_execution_report(&self, msg: &FixMessage, inflight: &mut InflightFixTable) {
        let Some(key) = execution_report_key(msg, inflight) else {
            debug!(
                "Binance Spot FIX ignoring unsolicited ExecutionReport cl_ord_id={:?} orig_cl_ord_id={:?} exec_type={:?}",
                msg.get(11),
                msg.get(41),
                msg.get(150)
            );
            return;
        };
        let Some(entry) = inflight.remove(key) else {
            return;
        };

        let error_code = msg.get(25016).and_then(parse_i32).unwrap_or(0);
        let exec_type = msg.get(150).unwrap_or("");
        let ord_status = msg.get(39).unwrap_or("");
        let rejected = exec_type == "8" || ord_status == "8" || error_code != 0;
        let status = if rejected { 400 } else { 200 };
        let body = serde_json::json!({
            "transport": "fix",
            "msgType": "8",
            "execType": exec_type,
            "ordStatus": ord_status,
            "code": error_code,
            "msg": msg.get(58).unwrap_or(""),
            "clientOrderId": entry.client_order_id,
            "clOrdId": msg.get(11).unwrap_or(""),
            "origClOrdId": msg.get(41).unwrap_or(""),
            "orderId": msg.get(37).unwrap_or("0"),
        })
        .to_string();
        let outcome = TradeExecOutcome {
            req_type: entry.req_type,
            client_order_id: entry.client_order_id,
            status,
            body,
            exchange: Exchange::Binance,
            order_id: msg.get(37).and_then(parse_i64).unwrap_or(0),
            order_status_u8: fix_order_status_u8(ord_status, exec_type),
            order_update_time: msg.get(60).and_then(parse_fix_time_ms).unwrap_or(0),
            executed_qty: msg.get(14).and_then(parse_f64).unwrap_or(0.0),
            response_price: msg
                .get(44)
                .or_else(|| msg.get(31))
                .and_then(parse_f64)
                .unwrap_or(0.0),
        };
        let _ = self.sink.send(outcome);
    }

    fn handle_cancel_reject(&self, msg: &FixMessage, inflight: &mut InflightFixTable) {
        let key = msg
            .get(11)
            .and_then(parse_i64)
            .filter(|cl| inflight.by_cl_ord_id.contains_key(cl))
            .or_else(|| {
                msg.get(41)
                    .and_then(parse_i64)
                    .and_then(|orig| inflight.by_orig_cl_ord_id.get(&orig).copied())
            });
        let Some(key) = key else {
            debug!("Binance Spot FIX ignoring unsolicited OrderCancelReject");
            return;
        };
        let Some(entry) = inflight.remove(key) else {
            return;
        };
        let error_code = msg.get(25016).and_then(parse_i32).unwrap_or(-2011);
        let body = serde_json::json!({
            "transport": "fix",
            "msgType": "9",
            "code": error_code,
            "msg": msg.get(58).unwrap_or(""),
            "clientOrderId": entry.client_order_id,
            "clOrdId": msg.get(11).unwrap_or(""),
            "origClOrdId": msg.get(41).unwrap_or(""),
            "orderId": msg.get(37).unwrap_or("0"),
        })
        .to_string();
        let _ = self.sink.send(TradeExecOutcome {
            req_type: entry.req_type,
            client_order_id: entry.client_order_id,
            status: 400,
            body,
            exchange: Exchange::Binance,
            order_id: msg.get(37).and_then(parse_i64).unwrap_or(0),
            order_status_u8: OrderExecutionStatus::Rejected.to_u8(),
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn handle_reject(&self, msg: &FixMessage, inflight: &mut InflightFixTable) {
        let key = msg
            .get(45)
            .and_then(parse_i64)
            .and_then(|seq| inflight.key_by_seq(seq));
        let Some(key) = key else {
            warn!(
                "Binance Spot FIX session Reject without matching request ref_seq={:?} text={}",
                msg.get(45),
                msg.get(58).unwrap_or("")
            );
            return;
        };
        let Some(entry) = inflight.remove(key) else {
            return;
        };
        let error_code = msg
            .get(25016)
            .or_else(|| msg.get(373))
            .and_then(parse_i32)
            .unwrap_or(-1000);
        let body = serde_json::json!({
            "transport": "fix",
            "msgType": "3",
            "code": error_code,
            "msg": msg.get(58).unwrap_or(""),
            "clientOrderId": entry.client_order_id,
            "refSeqNum": msg.get(45).unwrap_or(""),
            "refMsgType": msg.get(372).unwrap_or(""),
        })
        .to_string();
        let _ = self.sink.send(TradeExecOutcome {
            req_type: entry.req_type,
            client_order_id: entry.client_order_id,
            status: 400,
            body,
            exchange: Exchange::Binance,
            order_id: 0,
            order_status_u8: OrderExecutionStatus::Rejected.to_u8(),
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    /// 构造完成后报文位于 `self.msg_writer.message()`，随后立即发送；
    /// 返回值只携带在途登记所需的元数据，热路径零中间 String 分配。
    fn build_order_request(&mut self, msg: &TradeRequestMsg) -> Result<InflightFixRequest> {
        match msg.req_type {
            TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceWsNewMarginOrder => {
                self.build_new_order_request(msg)
            }
            TradeRequestType::BinanceCancelMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder => self.build_cancel_order_request(msg),
            _ => Err(anyhow!(
                "unsupported Binance Spot FIX request type: {:?}",
                msg.req_type
            )),
        }
    }

    fn build_new_order_request(&mut self, msg: &TradeRequestMsg) -> Result<InflightFixRequest> {
        let params = BinanceNewOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
            anyhow!(
                "Binance Spot FIX new order requires typed params, req_type={:?}",
                msg.req_type
            )
        })?;
        if params.margin_buy {
            return Err(anyhow!(
                "Binance Spot FIX is spot-only and does not support margin_buy/sideEffectType"
            ));
        }
        let ord_type = fix_ord_type(params.order_type)?;
        // 参数校验通过后才分配 seq，避免校验失败烧掉序号造成 gap。
        let seq_num = self.next_seq_num();
        self.write_message_header("D", seq_num);
        self.msg_writer.field_i64(11, msg.client_order_id);
        self.msg_writer.field_decimal(38, &params.quantity_qv);
        self.msg_writer.field_str(40, ord_type);
        if params.order_type.is_limit() {
            self.msg_writer.field_decimal(44, &params.price_qv);
        }
        self.msg_writer.field_str(54, fix_side(params.side));
        self.msg_writer.field_str(55, params.symbol);
        if params.order_type == OrderType::Limit && params.ws_margin_limit_maker {
            self.msg_writer.field_str(18, "6");
        } else if params.order_type.is_limit() {
            self.msg_writer.field_str(59, "1");
        }
        self.msg_writer.finish();
        Ok(InflightFixRequest {
            req_type: msg.req_type,
            client_order_id: msg.client_order_id,
            orig_client_order_id: None,
            seq_num,
        })
    }

    fn build_cancel_order_request(&mut self, msg: &TradeRequestMsg) -> Result<InflightFixRequest> {
        let params = BinanceCancelOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
            anyhow!(
                "Binance Spot FIX cancel order requires typed params, req_type={:?}",
                msg.req_type
            )
        })?;
        let seq_num = self.next_seq_num();
        self.write_message_header("F", seq_num);
        self.msg_writer.field_i64(11, msg.client_order_id);
        self.msg_writer.field_i64(41, params.orig_client_order_id);
        self.msg_writer.field_str(55, params.symbol);
        self.msg_writer.finish();
        Ok(InflightFixRequest {
            req_type: msg.req_type,
            client_order_id: msg.client_order_id,
            orig_client_order_id: Some(params.orig_client_order_id),
            seq_num,
        })
    }

    /// 标准头（35/34/49/52/56），SendingTime 经缓存日期前缀的格式化器直写。
    fn write_message_header(&mut self, msg_type: &str, seq_num: i64) {
        let writer = &mut self.msg_writer;
        writer.begin(msg_type);
        writer.field_i64(34, seq_num);
        writer.field_str(49, &self.config.sender_comp_id);
        writer.begin_field(52);
        self.time_fmt.write_now_into(&mut writer.body);
        writer.end_field();
        writer.field_str(56, &self.config.target_comp_id);
    }

    fn build_logon(&mut self) -> Result<String> {
        let seq_num = self.next_seq_num();
        let sending_time = self.time_fmt.now_string();
        let raw_data = self.sign_logon(seq_num, &sending_time)?;
        let mut fields = self.standard_header_fields("A", seq_num, &sending_time);
        fields.push((25000, self.config.recv_window_ms.clone()));
        fields.push((95, raw_data.len().to_string()));
        fields.push((96, raw_data));
        fields.push((98, "0".to_string()));
        fields.push((108, self.config.heartbtint.to_string()));
        fields.push((141, "Y".to_string()));
        fields.push((553, self.config.api_key.clone()));
        // MessageHandling=UNORDERED：官方文档明确多消息 in-flight 时性能更好；
        // SEQUENTIAL 会在网关侧按 MsgSeqNum 串行化，惩罚撤改单 burst。
        fields.push((25035, "1".to_string()));
        // 发单会话 ONLY_ACKS；影子 listener 用 EVERYTHING 收全账户 ER。
        fields.push((25036, self.config.response_mode.tag_value().to_string()));
        Ok(build_fix_message(&fields))
    }

    fn build_logout(&mut self, text: &str) -> String {
        let seq_num = self.next_seq_num();
        let sending_time = self.time_fmt.now_string();
        let mut fields = self.standard_header_fields("5", seq_num, &sending_time);
        fields.push((58, text.to_string()));
        build_fix_message(&fields)
    }

    async fn send_heartbeat(
        &mut self,
        stream: &mut BinanceFixStream,
        test_req_id: Option<&str>,
    ) -> Result<()> {
        let seq_num = self.next_seq_num();
        let sending_time = self.time_fmt.now_string();
        let mut fields = self.standard_header_fields("0", seq_num, &sending_time);
        if let Some(test_req_id) = test_req_id {
            fields.push((112, test_req_id.to_string()));
        }
        let heartbeat = build_fix_message(&fields);
        send_fix_message(stream, "Heartbeat<0>", &heartbeat).await
    }

    fn standard_header_fields(
        &self,
        msg_type: &str,
        seq_num: i64,
        sending_time: &str,
    ) -> Vec<FixField> {
        vec![
            (35, msg_type.to_string()),
            (34, seq_num.to_string()),
            (49, self.config.sender_comp_id.clone()),
            (52, sending_time.to_string()),
            (56, self.config.target_comp_id.clone()),
        ]
    }

    fn sign_logon(&self, seq_num: i64, sending_time: &str) -> Result<String> {
        let payload = format!(
            "A{SOH}{}{SOH}{}{SOH}{}{SOH}{}",
            self.config.sender_comp_id, self.config.target_comp_id, seq_num, sending_time
        );
        sign_ed25519_base64(&self.config.private_key, payload.as_bytes())
    }

    fn next_seq_num(&mut self) -> i64 {
        let seq = self.next_seq_num;
        self.next_seq_num += 1;
        seq
    }

    fn set_logged_on(&self, last_error: Option<String>) {
        let mut state = self.state.borrow_mut();
        state.logged_on = true;
        state.last_error = last_error;
    }

    fn set_logged_out(&self, last_error: Option<String>) {
        let mut state = self.state.borrow_mut();
        state.logged_on = false;
        state.last_error = last_error;
        state.session_id = None;
    }

    fn publish_request_error(&self, msg: &TradeRequestMsg, status: u16, reason: String) {
        let body = serde_json::json!({
            "transport": "fix",
            "state": "error",
            "code": -1000,
            "msg": reason,
            "clientOrderId": msg.client_order_id,
        })
        .to_string();
        let _ = self.sink.send(TradeExecOutcome {
            req_type: msg.req_type,
            client_order_id: msg.client_order_id,
            status,
            body,
            exchange: Exchange::Binance,
            order_id: 0,
            order_status_u8: OrderExecutionStatus::Rejected.to_u8(),
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }

    fn publish_transport_error(&self, inflight: InflightFixRequest, reason: String) {
        let body = serde_json::json!({
            "transport": "fix",
            "state": "error",
            "code": -1001,
            "msg": reason,
            "clientOrderId": inflight.client_order_id,
        })
        .to_string();
        let _ = self.sink.send(TradeExecOutcome {
            req_type: inflight.req_type,
            client_order_id: inflight.client_order_id,
            status: 0,
            body,
            exchange: Exchange::Binance,
            order_id: 0,
            order_status_u8: OrderExecutionStatus::Rejected.to_u8(),
            order_update_time: 0,
            executed_qty: 0.0,
            response_price: 0.0,
        });
    }
}

/// 影子 listener 解析出的账户推送。本地时间在读到报文后立刻打点。
///
/// `msg_type`：`8` ExecutionReport / `9` OrderCancelReject / `N` ListStatus。
/// 命名字段对齐 account_monitor 的 `BinanceBasicOrderMsg`；`raw_fields` 保留全部 tag。
#[derive(Debug, Clone)]
pub struct FixErEvent {
    pub local_ts_us: i64,
    pub msg_type: String,
    pub sending_time: String,
    pub seq_num: String,
    pub exec_type: String,
    pub exec_type_am: u8,
    pub ord_status: String,
    pub ord_status_am: u8,
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: i64,
    pub orig_client_order_id: i64,
    pub exec_id: String,
    pub trade_id: String,
    pub side: u8,
    pub ord_type: String,
    pub ord_type_am: u8,
    pub time_in_force: String,
    pub time_in_force_am: u8,
    pub price: f64,
    pub order_qty: f64,
    pub last_qty: f64,
    pub last_px: f64,
    pub cum_qty: f64,
    pub leaves_qty: f64,
    pub cum_quote_qty: f64,
    pub avg_px: f64,
    pub transact_time_ms: i64,
    pub order_creation_time: String,
    pub working_indicator: String,
    pub working_time: String,
    pub aggressor_indicator: String,
    pub last_liquidity_ind: String,
    pub is_maker: Option<bool>,
    pub commission: f64,
    pub commission_asset: String,
    pub error_code: String,
    pub text: String,
    pub list_id: String,
    pub stp_mode: String,
    pub match_type: String,
    pub alloc_id: String,
    pub expiry_reason: String,
    pub raw_fields: String,
}

impl FixErEvent {
    /// 转成与 spot `executionReport` 相同的 `BinanceBasicOrderMsg`，供 account_monitor 三源竞速。
    /// 只接受 ExecutionReport `<8>`；NEW 的 trade_id 固定 0，避免 ExecID 污染去重。
    pub fn to_std_spot_order_msg(&self) -> Option<BinanceBasicOrderMsg> {
        if self.msg_type != "8" {
            return None;
        }
        let client_order_id = if self.client_order_id != 0 {
            self.client_order_id
        } else {
            self.orig_client_order_id
        };
        if client_order_id == 0 || self.symbol.is_empty() {
            return None;
        }
        let trade_id = if self.exec_type_am == 5 {
            self.trade_id.parse::<i64>().unwrap_or(0).max(0)
        } else {
            0
        };
        Some(BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_MARGIN,
            self.transact_time_ms,
            self.transact_time_ms,
            self.symbol.clone(),
            self.order_id,
            client_order_id,
            trade_id,
            self.side,
            self.ord_type_am,
            self.time_in_force_am,
            self.exec_type_am,
            self.ord_status_am,
            self.is_maker.unwrap_or(false),
            self.price,
            self.order_qty,
            self.last_qty,
            self.cum_qty,
            self.last_px,
            self.avg_px,
            self.commission,
            0.0,
            self.commission_asset.clone(),
        ))
    }
}

/// 只收不发的 OE 会话：EVERYTHING + 心跳/登出，永不发送 D/F。
pub fn spawn_binance_spot_fix_er_listener(
    config: BinanceSpotFixConfig,
    tx: mpsc::UnboundedSender<FixErEvent>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let mut listener = BinanceSpotFixErListener {
        config,
        tx,
        shutdown,
        next_seq_num: 1,
        time_fmt: FixTimeFormatter::new(),
    };
    tokio::task::spawn_local(async move {
        listener.run().await;
    })
}

struct BinanceSpotFixErListener {
    config: BinanceSpotFixConfig,
    tx: mpsc::UnboundedSender<FixErEvent>,
    shutdown: CancellationToken,
    next_seq_num: i64,
    time_fmt: FixTimeFormatter,
}

impl BinanceSpotFixErListener {
    async fn run(&mut self) {
        while !self.shutdown.is_cancelled() {
            self.next_seq_num = 1;
            match self.connect_and_logon().await {
                Ok(mut stream) => {
                    if let Err(err) = self.process_session(&mut stream).await {
                        warn!("Binance Spot FIX ER listener session ended: {err:#}");
                    }
                }
                Err(err) => {
                    warn!("Binance Spot FIX ER listener connect/logon failed: {err:#}");
                }
            }
            if self.shutdown.is_cancelled() {
                break;
            }
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                _ = tokio::time::sleep(RECONNECT_DELAY) => {}
            }
        }
        info!("Binance Spot FIX ER listener stopped");
    }

    async fn connect_and_logon(&mut self) -> Result<BinanceFixStream> {
        let (host, port) = parse_fix_endpoint(&self.config.url)?;
        info!(
            "Binance Spot FIX ER listener connecting url={} sender_comp_id={} source_ip={} response_mode={}",
            self.config.url,
            self.config.sender_comp_id,
            self.config
                .source_ip
                .map(|ip| ip.to_string())
                .unwrap_or_else(|| "system-default".to_string()),
            self.config.response_mode.tag_value()
        );
        let tcp = connect_tcp(&host, port, self.config.source_ip).await?;
        tune_tcp_stream(
            &tcp,
            "account_monitor binance_spot_fix_er",
            trade_engine_tcp_tuning(),
        );
        let connector = NativeTlsConnector::builder()
            .build()
            .context("build native TLS connector for Binance Spot FIX ER listener")?;
        let connector = TlsConnector::from(connector);
        let mut stream = connector
            .connect(&host, tcp)
            .await
            .with_context(|| format!("TLS connect Binance Spot FIX ER listener host={host}"))?;
        let mut buffer = FixReadBuffer::with_capacity(4096);
        let logon = self.build_logon()?;
        send_fix_message(&mut stream, "Logon<A>", &logon).await?;

        loop {
            let raw = read_fix_message(&mut stream, &mut buffer).await?;
            let msg = FixMessage::parse(&raw);
            match msg.msg_type() {
                Some("A") => {
                    info!(
                        "Binance Spot FIX ER listener logon successful sender_comp_id={} session_id={}",
                        self.config.sender_comp_id,
                        msg.get(25037).unwrap_or("-")
                    );
                    return Ok(stream);
                }
                Some("1") => {
                    self.send_heartbeat(&mut stream, msg.get(112)).await?;
                }
                Some("3") | Some("5") => {
                    return Err(anyhow!(
                        "Binance Spot FIX ER listener logon rejected msg_type={} code={} text={}",
                        msg.msg_type().unwrap_or("?"),
                        msg.get(25016).or_else(|| msg.get(373)).unwrap_or("0"),
                        msg.get(58).unwrap_or("")
                    ));
                }
                other => {
                    debug!("Binance Spot FIX ER listener ignoring pre-logon msg_type={other:?}");
                }
            }
        }
    }

    async fn process_session(&mut self, stream: &mut BinanceFixStream) -> Result<()> {
        let mut buffer = FixReadBuffer::with_capacity(8192);
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    let logout = self.build_logout("listener shutdown");
                    let _ = send_fix_message(stream, "Logout<5>", &logout).await;
                    return Ok(());
                }
                raw = read_fix_message(stream, &mut buffer) => {
                    let raw = raw?;
                    let local_ts_us = runtime_common::time_util::get_timestamp_us();
                    let msg = FixMessage::parse(&raw);
                    if !self.handle_incoming(stream, &msg, &raw, local_ts_us).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn handle_incoming(
        &mut self,
        stream: &mut BinanceFixStream,
        msg: &FixMessage,
        raw: &str,
        local_ts_us: i64,
    ) -> Result<bool> {
        match msg.msg_type() {
            Some("0") => Ok(true),
            Some("1") => {
                self.send_heartbeat(stream, msg.get(112)).await?;
                Ok(true)
            }
            Some("5") => {
                warn!(
                    "Binance Spot FIX ER listener logout received text={}",
                    msg.get(58).unwrap_or("")
                );
                Ok(false)
            }
            Some("B") => {
                warn!(
                    "Binance Spot FIX ER listener news received text={}",
                    msg.get(58).unwrap_or("")
                );
                Ok(false)
            }
            Some("8") | Some("9") | Some("N") => {
                if self
                    .tx
                    .send(parse_fix_er_event(msg, raw, local_ts_us))
                    .is_err()
                {
                    return Ok(false);
                }
                Ok(true)
            }
            other => {
                debug!("Binance Spot FIX ER listener ignoring msg_type={other:?}");
                Ok(true)
            }
        }
    }

    fn build_logon(&mut self) -> Result<String> {
        let seq_num = self.next_seq_num();
        let sending_time = self.time_fmt.now_string();
        let raw_data = self.sign_logon(seq_num, &sending_time)?;
        let mut fields = self.standard_header_fields("A", seq_num, &sending_time);
        fields.push((25000, self.config.recv_window_ms.clone()));
        fields.push((95, raw_data.len().to_string()));
        fields.push((96, raw_data));
        fields.push((98, "0".to_string()));
        fields.push((108, self.config.heartbtint.to_string()));
        fields.push((141, "Y".to_string()));
        fields.push((553, self.config.api_key.clone()));
        fields.push((25035, "1".to_string()));
        fields.push((25036, self.config.response_mode.tag_value().to_string()));
        Ok(build_fix_message(&fields))
    }

    fn build_logout(&mut self, text: &str) -> String {
        let seq_num = self.next_seq_num();
        let sending_time = self.time_fmt.now_string();
        let mut fields = self.standard_header_fields("5", seq_num, &sending_time);
        fields.push((58, text.to_string()));
        build_fix_message(&fields)
    }

    async fn send_heartbeat(
        &mut self,
        stream: &mut BinanceFixStream,
        test_req_id: Option<&str>,
    ) -> Result<()> {
        let seq_num = self.next_seq_num();
        let sending_time = self.time_fmt.now_string();
        let mut fields = self.standard_header_fields("0", seq_num, &sending_time);
        if let Some(test_req_id) = test_req_id {
            fields.push((112, test_req_id.to_string()));
        }
        let heartbeat = build_fix_message(&fields);
        send_fix_message(stream, "Heartbeat<0>", &heartbeat).await
    }

    fn standard_header_fields(
        &self,
        msg_type: &str,
        seq_num: i64,
        sending_time: &str,
    ) -> Vec<FixField> {
        vec![
            (35, msg_type.to_string()),
            (34, seq_num.to_string()),
            (49, self.config.sender_comp_id.clone()),
            (52, sending_time.to_string()),
            (56, self.config.target_comp_id.clone()),
        ]
    }

    fn sign_logon(&self, seq_num: i64, sending_time: &str) -> Result<String> {
        let payload = format!(
            "A{SOH}{}{SOH}{}{SOH}{}{SOH}{}",
            self.config.sender_comp_id, self.config.target_comp_id, seq_num, sending_time
        );
        sign_ed25519_base64(&self.config.private_key, payload.as_bytes())
    }

    fn next_seq_num(&mut self) -> i64 {
        let seq = self.next_seq_num;
        self.next_seq_num += 1;
        seq
    }
}

fn parse_fix_er_event(msg: &FixMessage, raw: &str, local_ts_us: i64) -> FixErEvent {
    let exec_type = msg.get(150).unwrap_or("").to_string();
    let ord_status = msg.get(39).unwrap_or("").to_string();
    let ord_type = msg.get(40).unwrap_or("").to_string();
    let time_in_force = msg.get(59).unwrap_or("").to_string();
    let side = match msg.get(54).unwrap_or("") {
        "1" => 1,
        "2" => 2,
        _ => 0,
    };
    let is_maker = match (msg.get(1057), msg.get(851)) {
        (Some("N"), _) => Some(true),
        (Some("Y"), _) => Some(false),
        (_, Some("1")) => Some(true),
        (_, Some("2")) => Some(false),
        _ => None,
    };
    let (commission, commission_asset) = parse_misc_fees(msg);
    let cum_qty = msg.get(14).and_then(parse_f64).unwrap_or(0.0);
    let cum_quote_qty = msg.get(25017).and_then(parse_f64).unwrap_or(0.0);
    let avg_px = if cum_qty > 0.0 {
        cum_quote_qty / cum_qty
    } else {
        0.0
    };
    let trade_id = msg.get(1003).unwrap_or("").to_string();
    FixErEvent {
        local_ts_us,
        msg_type: msg.msg_type().unwrap_or("").to_string(),
        sending_time: msg.get(52).unwrap_or("").to_string(),
        seq_num: msg.get(34).unwrap_or("").to_string(),
        exec_type_am: fix_exec_type_to_am_u8(&exec_type),
        exec_type,
        ord_status_am: fix_ord_status_to_am_u8(&ord_status),
        ord_status,
        symbol: msg.get(55).unwrap_or("").to_string(),
        order_id: msg.get(37).and_then(parse_i64).unwrap_or(0),
        client_order_id: msg.get(11).and_then(parse_i64).unwrap_or(0),
        orig_client_order_id: msg.get(41).and_then(parse_i64).unwrap_or(0),
        exec_id: msg.get(17).unwrap_or("").to_string(),
        trade_id,
        side,
        ord_type_am: fix_ord_type_to_am_u8(&ord_type),
        ord_type,
        time_in_force_am: fix_tif_to_am_u8(&time_in_force),
        time_in_force,
        price: msg.get(44).and_then(parse_f64).unwrap_or(0.0),
        order_qty: msg.get(38).and_then(parse_f64).unwrap_or(0.0),
        last_qty: msg.get(32).and_then(parse_f64).unwrap_or(0.0),
        last_px: msg.get(31).and_then(parse_f64).unwrap_or(0.0),
        cum_qty,
        leaves_qty: msg.get(151).and_then(parse_f64).unwrap_or(0.0),
        cum_quote_qty,
        avg_px,
        transact_time_ms: msg.get(60).and_then(parse_fix_time_ms).unwrap_or(0),
        order_creation_time: msg.get(25018).unwrap_or("").to_string(),
        working_indicator: msg.get(636).unwrap_or("").to_string(),
        working_time: msg.get(25023).unwrap_or("").to_string(),
        aggressor_indicator: msg.get(1057).unwrap_or("").to_string(),
        last_liquidity_ind: msg.get(851).unwrap_or("").to_string(),
        is_maker,
        commission,
        commission_asset,
        error_code: msg.get(25016).unwrap_or("").to_string(),
        text: msg.get(58).unwrap_or("").to_string(),
        list_id: msg.get(66).unwrap_or("").to_string(),
        stp_mode: msg.get(25001).unwrap_or("").to_string(),
        match_type: msg.get(574).unwrap_or("").to_string(),
        alloc_id: msg.get(70).unwrap_or("").to_string(),
        expiry_reason: msg.get(25056).unwrap_or("").to_string(),
        raw_fields: raw.trim_end_matches(SOH).replace(SOH, "|"),
    }
}

fn parse_misc_fees(msg: &FixMessage) -> (f64, String) {
    let amts = msg.get_all(137);
    let currs = msg.get_all(138);
    let mut total = 0.0;
    let mut asset = String::new();
    for (idx, amt) in amts.iter().enumerate() {
        if let Some(value) = parse_f64(amt) {
            total += value;
        }
        if asset.is_empty() {
            if let Some(curr) = currs.get(idx) {
                asset = curr.clone();
            }
        }
    }
    (total, asset)
}

fn fix_exec_type_to_am_u8(exec_type: &str) -> u8 {
    match exec_type {
        "0" => 1,
        "4" => 2,
        "5" => 3,
        "8" => 4,
        "F" => 5,
        "C" => 6,
        _ => 0,
    }
}

fn fix_ord_status_to_am_u8(status: &str) -> u8 {
    match status {
        "0" | "A" => 1,
        "1" => 2,
        "2" => 3,
        "4" | "6" | "8" => 4,
        "C" => 5,
        _ => 0,
    }
}

fn fix_ord_type_to_am_u8(ord_type: &str) -> u8 {
    match ord_type {
        "2" => 1,
        "1" => 3,
        "3" => 4,
        "4" => 5,
        _ => 0,
    }
}

fn fix_tif_to_am_u8(tif: &str) -> u8 {
    match tif {
        "1" => 0,
        "3" => 1,
        "4" => 2,
        _ => 0,
    }
}

#[derive(Debug)]
struct FixMessage {
    fields: HashMap<u32, Vec<String>>,
}

impl FixMessage {
    fn parse(raw: &str) -> Self {
        let mut fields: HashMap<u32, Vec<String>> = HashMap::new();
        for field in raw.split(SOH) {
            if field.is_empty() {
                continue;
            }
            let Some((tag, value)) = field.split_once('=') else {
                continue;
            };
            let Ok(tag) = tag.parse::<u32>() else {
                continue;
            };
            fields.entry(tag).or_default().push(value.to_string());
        }
        Self { fields }
    }

    fn get(&self, tag: u32) -> Option<&str> {
        self.fields
            .get(&tag)
            .and_then(|values| values.first())
            .map(String::as_str)
    }

    fn get_all(&self, tag: u32) -> &[String] {
        self.fields.get(&tag).map(Vec::as_slice).unwrap_or(&[])
    }

    fn msg_type(&self) -> Option<&str> {
        self.get(35)
    }
}

async fn connect_tcp(host: &str, port: u16, source_ip: Option<IpAddr>) -> Result<TcpStream> {
    let addrs = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolve Binance Spot FIX host={host}"))?;
    let mut last_err = None;
    for addr in addrs {
        if let Some(source_ip) = source_ip {
            if source_ip.is_ipv4() != addr.is_ipv4() {
                continue;
            }
        }
        match connect_tcp_addr(addr, source_ip).await {
            Ok(stream) => return Ok(stream),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("no address for Binance Spot FIX host={host}")))
}

async fn connect_tcp_addr(
    addr: SocketAddr,
    source_ip: Option<IpAddr>,
) -> std::io::Result<TcpStream> {
    if let Some(source_ip) = source_ip {
        let socket = if source_ip.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        socket.bind(SocketAddr::new(source_ip, 0))?;
        socket.connect(addr).await
    } else {
        TcpStream::connect(addr).await
    }
}

fn parse_fix_endpoint(raw: &str) -> Result<(String, u16)> {
    let url = Url::parse(raw).with_context(|| format!("parse BINANCE_FIX_OE_URL={raw}"))?;
    match url.scheme() {
        "tcp+tls" | "tls" | "ssl" => {}
        scheme => return Err(anyhow!("unsupported Binance Spot FIX URL scheme: {scheme}")),
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("missing host in Binance Spot FIX URL: {raw}"))?
        .to_string();
    let port = url.port().unwrap_or(9000);
    Ok((host, port))
}

async fn send_fix_message(stream: &mut BinanceFixStream, label: &str, message: &str) -> Result<()> {
    debug!(
        "Binance Spot FIX send {} msg_type={} seq={} body_len={}",
        label,
        message_field(message, 35).unwrap_or("?"),
        message_field(message, 34).unwrap_or("?"),
        message.len()
    );
    stream.write_all(message.as_bytes()).await?;
    Ok(())
}

/// FIX 读缓冲：读指针替代每条消息的 `Vec::drain` 头部搬移。
/// 取走一条消息只前移 `pos`；仅在需要继续读 socket 且存在跨 read 残留时
/// 做一次小段 compact（残留通常远小于整批消息）。
struct FixReadBuffer {
    buf: Vec<u8>,
    pos: usize,
}

impl FixReadBuffer {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            pos: 0,
        }
    }

    fn compact(&mut self) {
        if self.pos == 0 {
            return;
        }
        if self.pos == self.buf.len() {
            self.buf.clear();
        } else {
            self.buf.copy_within(self.pos.., 0);
            let remain = self.buf.len() - self.pos;
            self.buf.truncate(remain);
        }
        self.pos = 0;
    }

    fn extend(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }
}

async fn read_fix_message(
    stream: &mut BinanceFixStream,
    buffer: &mut FixReadBuffer,
) -> Result<String> {
    loop {
        if let Some(msg) = try_take_fix_message(buffer)? {
            return Ok(msg);
        }
        buffer.compact();
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(anyhow!("Binance Spot FIX socket closed"));
        }
        buffer.extend(&chunk[..n]);
    }
}

fn try_take_fix_message(buffer: &mut FixReadBuffer) -> Result<Option<String>> {
    let unread = &buffer.buf[buffer.pos..];
    let Some(start_rel) = find_bytes(unread, b"8=FIX.4.4\x01") else {
        // 未见消息头：只保留末尾 64 字节等待与后续数据拼接。
        let keep = unread.len().min(64);
        buffer.pos = buffer.buf.len() - keep;
        return Ok(None);
    };
    let msg_start = buffer.pos + start_rel;
    let data = &buffer.buf[msg_start..];
    let Some(first_soh) = data.iter().position(|b| *b == SOH_BYTE) else {
        buffer.pos = msg_start;
        return Ok(None);
    };
    if data.get(first_soh + 1..first_soh + 3) != Some(b"9=") {
        return Err(anyhow!("invalid FIX message: BodyLength tag missing"));
    }
    let Some(second_soh_rel) = data[first_soh + 1..].iter().position(|b| *b == SOH_BYTE) else {
        buffer.pos = msg_start;
        return Ok(None);
    };
    let second_soh = first_soh + 1 + second_soh_rel;
    let body_len_raw = std::str::from_utf8(&data[first_soh + 3..second_soh])?;
    let body_len = body_len_raw
        .parse::<usize>()
        .with_context(|| format!("parse FIX BodyLength={body_len_raw}"))?;
    let body_start = second_soh + 1;
    let total_len = body_start + body_len + b"10=000\x01".len();
    if data.len() < total_len {
        buffer.pos = msg_start;
        return Ok(None);
    }
    let msg = std::str::from_utf8(&data[..total_len])
        .context("FIX message must be ASCII/UTF-8")?
        .to_string();
    buffer.pos = msg_start + total_len;
    Ok(Some(msg))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// 冷路径（logon/logout/heartbeat）使用的一次性构造；热路径见 `FixMessageWriter`。
fn build_fix_message(fields: &[FixField]) -> String {
    debug_assert!(fields.first().is_some_and(|(tag, _)| *tag == 35));
    let mut writer = FixMessageWriter::with_capacity(256);
    let (_, msg_type) = &fields[0];
    writer.begin(msg_type);
    for (tag, value) in &fields[1..] {
        writer.field_str(*tag, value);
    }
    writer.finish().to_string()
}

/// 可复用的 FIX 报文构造器：body/out 双缓冲循环使用，tag 与整数经 itoa 写入，
/// 价格/数量经 `QuantizedValue::write_decimal_to` 直写，热路径零中间 String 分配。
struct FixMessageWriter {
    body: String,
    out: String,
}

impl FixMessageWriter {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            body: String::with_capacity(capacity),
            out: String::with_capacity(capacity + 32),
        }
    }

    fn begin(&mut self, msg_type: &str) {
        self.body.clear();
        push_fix_field(&mut self.body, 35, msg_type);
    }

    fn field_str(&mut self, tag: u32, value: &str) {
        push_fix_field(&mut self.body, tag, value);
    }

    fn field_i64(&mut self, tag: u32, value: i64) {
        let mut digits = itoa::Buffer::new();
        push_fix_field(&mut self.body, tag, digits.format(value));
    }

    fn field_decimal(&mut self, tag: u32, value: &QuantizedValue) {
        self.begin_field(tag);
        value
            .write_decimal_to(&mut self.body)
            .expect("write decimal to String cannot fail");
        self.end_field();
    }

    fn begin_field(&mut self, tag: u32) {
        let mut digits = itoa::Buffer::new();
        self.body.push_str(digits.format(tag));
        self.body.push('=');
    }

    fn end_field(&mut self) {
        self.body.push(SOH);
    }

    /// 补齐 8/9 头与 10 校验和，报文落在 `self.out`，通过 `message()` 读取。
    fn finish(&mut self) -> &str {
        self.out.clear();
        self.out.push_str("8=FIX.4.4\x01");
        self.out.push_str("9=");
        let mut digits = itoa::Buffer::new();
        self.out.push_str(digits.format(self.body.len()));
        self.out.push(SOH);
        self.out.push_str(&self.body);
        let sum = checksum(&self.out);
        self.out.push_str("10=");
        push_three_digits(&mut self.out, sum);
        self.out.push(SOH);
        &self.out
    }

    fn message(&self) -> &str {
        &self.out
    }
}

fn push_fix_field(out: &mut String, tag: u32, value: &str) {
    let mut digits = itoa::Buffer::new();
    out.push_str(digits.format(tag));
    out.push('=');
    out.push_str(value);
    out.push(SOH);
}

fn push_two_digits(out: &mut String, value: u32) {
    debug_assert!(value < 100);
    out.push((b'0' + (value / 10) as u8) as char);
    out.push((b'0' + (value % 10) as u8) as char);
}

fn push_three_digits(out: &mut String, value: u32) {
    debug_assert!(value < 1000);
    out.push((b'0' + (value / 100) as u8) as char);
    out.push((b'0' + ((value / 10) % 10) as u8) as char);
    out.push((b'0' + (value % 10) as u8) as char);
}

fn checksum(raw_without_checksum: &str) -> u32 {
    raw_without_checksum
        .as_bytes()
        .iter()
        .fold(0u32, |sum, byte| sum + *byte as u32)
        % 256
}

fn message_field(message: &str, want_tag: u32) -> Option<&str> {
    for field in message.split(SOH) {
        let (tag, value) = field.split_once('=')?;
        if tag.parse::<u32>().ok()? == want_tag {
            return Some(value);
        }
    }
    None
}

fn load_ed25519_private_key(path: &str) -> Result<PKey<Private>> {
    let passphrase = std::env::var(BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE_ENV)
        .ok()
        .unwrap_or_default();
    let pem = std::fs::read(path)
        .with_context(|| format!("read {}={}", BINANCE_ED25519_PRIVATE_KEY_PATH_ENV, path))?;
    let key = if passphrase.is_empty() {
        PKey::private_key_from_pem(&pem)
            .with_context(|| format!("parse Ed25519 private key PEM from {path}"))?
    } else {
        PKey::private_key_from_pem_passphrase(&pem, passphrase.as_bytes())
            .with_context(|| format!("parse encrypted Ed25519 private key PEM from {path}"))?
    };
    if key.id() != PKeyId::ED25519 {
        return Err(anyhow!(
            "{}={} is not an Ed25519 private key",
            BINANCE_ED25519_PRIVATE_KEY_PATH_ENV,
            path
        ));
    }
    Ok(key)
}

fn sign_ed25519_base64(key: &PKey<Private>, payload: &[u8]) -> Result<String> {
    let mut signer = Signer::new_without_digest(key).context("create Ed25519 signer")?;
    let mut signature = [0u8; 64];
    let len = signer
        .sign_oneshot(&mut signature, payload)
        .context("sign Binance Spot FIX payload with Ed25519")?;
    Ok(BASE64_STANDARD.encode(&signature[..len]))
}

fn validate_comp_id(name: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 8 {
        return Err(anyhow!("{name} must be 1-8 characters"));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(anyhow!("{name} must match ^[a-zA-Z0-9-_]{{1,8}}$"));
    }
    Ok(())
}

fn generate_sender_comp_id() -> String {
    let id = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .to_ascii_uppercase();
    format!("C{}", &id[..7])
}

/// 多会话时从固定 base 派生唯一 SenderCompID：截断到 7 字符 + 会话序号（0-9）。
fn derive_session_comp_id(base: &str, session_index: usize) -> String {
    debug_assert!(session_index < 10, "single-digit session suffix");
    let mut out: String = base.chars().take(7).collect();
    out.push_str(&session_index.to_string());
    out
}

/// 影子 listener 的 SenderCompID：优先独立 env，否则从发单 base 加 `L` 后缀，
/// 避免和 TE 会话 0/1 撞名。
fn resolve_listener_sender_comp_id() -> Result<String> {
    if let Some(explicit) = std::env::var("BINANCE_FIX_ER_PROBE_SENDER_COMP_ID")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        validate_comp_id("BINANCE_FIX_ER_PROBE_SENDER_COMP_ID", &explicit)?;
        return Ok(explicit);
    }
    let sender = match std::env::var("BINANCE_FIX_SENDER_COMP_ID")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        Some(base) => derive_listener_comp_id(&base),
        None => generate_sender_comp_id(),
    };
    validate_comp_id("BINANCE_FIX_ER_PROBE_SENDER_COMP_ID", &sender)?;
    Ok(sender)
}

fn derive_listener_comp_id(base: &str) -> String {
    let mut out: String = base.chars().take(7).collect();
    out.push('L');
    out
}

/// SendingTime(52) 格式化器：缓存 `YYYYMMDD-` 日期前缀（每 UTC 日重算一次），
/// 每条消息只手工格式化 HH:MM:SS.mmm，避免热路径走 chrono 完整 format。
struct FixTimeFormatter {
    cached_day: i64,
    prefix: [u8; 9],
}

impl FixTimeFormatter {
    fn new() -> Self {
        Self {
            cached_day: i64::MIN,
            prefix: [0u8; 9],
        }
    }

    fn write_now_into(&mut self, out: &mut String) {
        self.write_ms_into(Utc::now().timestamp_millis(), out);
    }

    fn write_ms_into(&mut self, epoch_ms: i64, out: &mut String) {
        const MS_PER_DAY: i64 = 86_400_000;
        let day = epoch_ms.div_euclid(MS_PER_DAY);
        if day != self.cached_day {
            self.refresh_prefix(day);
        }
        out.push_str(std::str::from_utf8(&self.prefix).expect("date prefix is ASCII"));
        let ms_of_day = epoch_ms.rem_euclid(MS_PER_DAY);
        push_two_digits(out, (ms_of_day / 3_600_000) as u32);
        out.push(':');
        push_two_digits(out, ((ms_of_day / 60_000) % 60) as u32);
        out.push(':');
        push_two_digits(out, ((ms_of_day / 1_000) % 60) as u32);
        out.push('.');
        push_three_digits(out, (ms_of_day % 1_000) as u32);
    }

    fn refresh_prefix(&mut self, day: i64) {
        let date = chrono::DateTime::<Utc>::from_timestamp(day * 86_400, 0)
            .expect("epoch day within chrono range");
        let formatted = date.format("%Y%m%d-").to_string();
        self.prefix.copy_from_slice(formatted.as_bytes());
        self.cached_day = day;
    }

    fn now_string(&mut self) -> String {
        let mut out = String::with_capacity(21);
        self.write_now_into(&mut out);
        out
    }
}

fn fix_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "1",
        Side::Sell => "2",
    }
}

fn fix_ord_type(order_type: OrderType) -> Result<&'static str> {
    match order_type {
        OrderType::Market => Ok("1"),
        OrderType::Limit => Ok("2"),
        OrderType::StopLoss | OrderType::TakeProfit | OrderType::StopMarket | OrderType::TakeProfitMarket => {
            Err(anyhow!("Binance Spot FIX conditional market orders need trigger fields and are not supported by current TradeRequestMsg"))
        }
        OrderType::StopLossLimit | OrderType::TakeProfitLimit => {
            Err(anyhow!("Binance Spot FIX conditional limit orders need trigger fields and are not supported by current TradeRequestMsg"))
        }
    }
}

fn fix_order_status_u8(ord_status: &str, exec_type: &str) -> u8 {
    match (ord_status, exec_type) {
        ("2", _) => OrderExecutionStatus::Filled.to_u8(),
        ("4", _) => OrderExecutionStatus::Cancelled.to_u8(),
        ("8", _) | (_, "8") => OrderExecutionStatus::Rejected.to_u8(),
        ("C", _) => OrderExecutionStatus::Cancelled.to_u8(),
        ("0", _) | ("1", _) | ("6", _) | ("A", _) => OrderExecutionStatus::Create.to_u8(),
        _ => OrderExecutionStatus::Create.to_u8(),
    }
}

fn parse_i64(raw: &str) -> Option<i64> {
    raw.trim().parse::<i64>().ok()
}

fn parse_i32(raw: &str) -> Option<i32> {
    raw.trim().parse::<i32>().ok()
}

fn parse_f64(raw: &str) -> Option<f64> {
    raw.trim().parse::<f64>().ok()
}

fn parse_fix_time_ms(raw: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(raw.trim(), "%Y%m%d-%H:%M:%S%.f")
        .ok()
        .map(|dt| dt.and_utc().timestamp_millis())
}

/// ExecutionReport 归属：优先 clOrdId(11) 命中在途请求或作为撤单目标，
/// 再看 origClOrdId(41)。非整数 id 一定不是本系统的单，直接判为 unsolicited。
fn execution_report_key(msg: &FixMessage, inflight: &InflightFixTable) -> Option<i64> {
    if let Some(cl_ord_id) = msg.get(11).and_then(parse_i64) {
        if inflight.by_cl_ord_id.contains_key(&cl_ord_id) {
            return Some(cl_ord_id);
        }
        if let Some(cancel_key) = inflight.by_orig_cl_ord_id.get(&cl_ord_id) {
            return Some(*cancel_key);
        }
    }
    if let Some(orig_cl_ord_id) = msg.get(41).and_then(parse_i64) {
        if let Some(cancel_key) = inflight.by_orig_cl_ord_id.get(&orig_cl_ord_id) {
            return Some(*cancel_key);
        }
        if inflight.by_cl_ord_id.contains_key(&orig_cl_ord_id) {
            return Some(orig_cl_ord_id);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::pkey::PKey;

    #[test]
    fn logon_signature_matches_binance_doc_vector() {
        let pem = b"-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u\n-----END PRIVATE KEY-----\n";
        let key = PKey::private_key_from_pem(pem).expect("test key");
        let payload = b"A\x01EXAMPLE\x01SPOT\x011\x0120240627-11:17:25.223";
        let sig = sign_ed25519_base64(&key, payload).expect("signature");
        assert_eq!(
            sig,
            "4MHXelVVcpkdwuLbl6n73HQUXUf1dse2PCgT1DYqW9w8AVZ1RACFGM+5UdlGPrQHrgtS3CvsRURC1oj73j8gCA=="
        );
    }

    #[test]
    fn builds_fix_message_with_body_length_and_checksum() {
        let fields = vec![
            (35, "0".to_string()),
            (34, "2".to_string()),
            (49, "ABC".to_string()),
            (52, "20260630-00:00:00.000".to_string()),
            (56, "SPOT".to_string()),
        ];
        let msg = build_fix_message(&fields);
        assert!(msg.starts_with("8=FIX.4.4\x019="));
        assert!(msg.ends_with("\x01"));
        assert!(message_field(&msg, 10).is_some());
    }

    #[test]
    fn parses_truthy_fix_switch() {
        std::env::set_var(BINANCE_SPOT_FIX_ENABLED_ENV, "on");
        assert!(spot_fix_enabled_from_env().unwrap());
        std::env::set_var(BINANCE_SPOT_FIX_ENABLED_ENV, "off");
        assert!(!spot_fix_enabled_from_env().unwrap());
        std::env::remove_var(BINANCE_SPOT_FIX_ENABLED_ENV);
    }

    #[test]
    fn derives_unique_session_comp_ids() {
        assert_eq!(derive_session_comp_id("MYCOMPID", 0), "MYCOMPI0");
        assert_eq!(derive_session_comp_id("MYCOMPID", 3), "MYCOMPI3");
        assert_eq!(derive_session_comp_id("AB", 1), "AB1");
        for idx in 0..FIX_OE_SESSIONS {
            let comp_id = derive_session_comp_id("LONGBASEID", idx);
            validate_comp_id("test", &comp_id).expect("derived comp id must stay valid");
        }
    }

    #[test]
    fn listener_comp_id_does_not_collide_with_oe_sessions() {
        let base = "MYCOMPID";
        let listener = derive_listener_comp_id(base);
        assert_eq!(listener, "MYCOMPIL");
        validate_comp_id("test", &listener).expect("listener comp id must stay valid");
        for idx in 0..FIX_OE_SESSIONS {
            assert_ne!(listener, derive_session_comp_id(base, idx));
        }
    }

    #[test]
    fn parses_official_new_execution_report() {
        let raw = "8=FIX.4.4\x019=330\x0135=8\x0134=2\x0149=SPOT\x0152=20240611-09:01:46.228950\x0156=qNXO12fH\x0111=1718096506197867067\x0114=0.00000000\x0117=144\x0132=0.00000000\x0137=76\x0138=5.00000000\x0139=0\x0140=2\x0144=10.00000000\x0154=1\x0155=LTCBNB\x0159=4\x0160=20240611-09:01:46.228000\x01150=0\x01151=5.00000000\x01636=Y\x011057=Y\x0125001=1\x0125017=0.00000000\x0125018=20240611-09:01:46.228000\x0125023=20240611-09:01:46.228000\x0110=095\x01";
        let msg = FixMessage::parse(raw);
        let event = parse_fix_er_event(&msg, raw, 42);
        assert_eq!(event.msg_type, "8");
        assert_eq!(event.exec_type, "0");
        assert_eq!(event.exec_type_am, 1);
        assert_eq!(event.ord_status, "0");
        assert_eq!(event.ord_status_am, 1);
        assert_eq!(event.symbol, "LTCBNB");
        assert_eq!(event.order_id, 76);
        assert_eq!(event.client_order_id, 1_718_096_506_197_867_067);
        assert_eq!(event.side, 1);
        assert_eq!(event.ord_type_am, 1);
        assert_eq!(event.time_in_force_am, 2);
        assert_eq!(event.price, 10.0);
        assert_eq!(event.order_qty, 5.0);
        assert_eq!(event.leaves_qty, 5.0);
        assert_eq!(event.working_indicator, "Y");
        assert_eq!(event.is_maker, Some(false));
        assert!(event.trade_id.is_empty());
        assert!(event.raw_fields.contains("150=0"));
        assert!(event.raw_fields.contains("55=LTCBNB"));
        let msg = event.to_std_spot_order_msg().expect("new er maps to order");
        assert_eq!(msg.venue, BinanceBasicOrderMsg::VENUE_MARGIN);
        assert_eq!(msg.execution_type, 1);
        assert_eq!(msg.order_status, 1);
        assert_eq!(msg.trade_id, 0);
        assert_eq!(msg.client_order_id, 1_718_096_506_197_867_067);
    }

    #[test]
    fn trade_id_prefers_tag_1003_over_exec_id() {
        let raw = "8=FIX.4.4\x019=80\x0135=8\x0117=144\x011003=1965177634\x01150=F\x0137=1\x0111=2\x0155=BTCUSDT\x0110=000\x01";
        let msg = FixMessage::parse(raw);
        let event = parse_fix_er_event(&msg, raw, 1);
        assert_eq!(event.exec_id, "144");
        assert_eq!(event.trade_id, "1965177634");
        assert_eq!(event.exec_type_am, 5);
    }

    #[test]
    fn message_writer_produces_consistent_body_length_and_checksum() {
        let mut writer = FixMessageWriter::with_capacity(64);
        writer.begin("D");
        writer.field_i64(34, 7);
        writer.field_str(49, "ABC");
        writer.field_str(52, "20260815-10:00:00.123");
        writer.field_str(56, "SPOT");
        writer.field_i64(11, 123456789);
        let msg = writer.finish().to_string();

        assert!(msg.starts_with("8=FIX.4.4\x019="));
        assert!(msg.ends_with(SOH));

        let body_len: usize = message_field(&msg, 9).unwrap().parse().unwrap();
        let body_start = msg.find("9=").unwrap() + format!("9={body_len}\x01").len();
        let trailer_start = msg.rfind("10=").unwrap();
        assert_eq!(trailer_start - body_start, body_len);

        let expected_sum = checksum(&msg[..trailer_start]);
        assert_eq!(
            message_field(&msg, 10).unwrap(),
            format!("{expected_sum:03}")
        );
        assert_eq!(message_field(&msg, 11).unwrap(), "123456789");
    }

    #[test]
    fn field_decimal_matches_decimal_string() {
        let mut writer = FixMessageWriter::with_capacity(64);
        for qv in [
            QuantizedValue::from_parts(15, -2, 3),
            QuantizedValue::from_parts(1, 0, 250),
            QuantizedValue::zero(),
        ] {
            writer.begin("D");
            writer.field_decimal(38, &qv);
            let msg = writer.finish().to_string();
            assert_eq!(
                message_field(&msg, 38).unwrap(),
                qv.decimal_string(),
                "qv={qv:?}"
            );
        }
    }

    #[test]
    fn fix_time_formatter_matches_chrono_format() {
        let mut formatter = FixTimeFormatter::new();
        for epoch_ms in [
            0i64,
            86_399_999,                // 日末最后一毫秒
            86_400_000,                // 跨日边界，触发前缀重算
            1_755_244_800_123,         // 2026-08-15 附近
            1_755_244_800_000 + 3_601, // 非整秒毫秒位
        ] {
            let mut out = String::new();
            formatter.write_ms_into(epoch_ms, &mut out);
            let expected = chrono::DateTime::<Utc>::from_timestamp_millis(epoch_ms)
                .unwrap()
                .format("%Y%m%d-%H:%M:%S%.3f")
                .to_string();
            assert_eq!(out, expected, "epoch_ms={epoch_ms}");
        }
    }

    #[test]
    fn read_buffer_extracts_messages_and_skips_garbage() {
        let msg1 = build_fix_message(&[(35, "0".to_string()), (34, "2".to_string())]);
        let msg2 = build_fix_message(&[(35, "0".to_string()), (34, "3".to_string())]);

        let mut buffer = FixReadBuffer::with_capacity(64);
        buffer.extend(b"garbage-prefix");
        buffer.extend(msg1.as_bytes());
        let split = msg2.len() / 2;
        buffer.extend(&msg2.as_bytes()[..split]);

        let taken1 = try_take_fix_message(&mut buffer).unwrap();
        assert_eq!(taken1.as_deref(), Some(msg1.as_str()));

        // msg2 只有一半：解析挂起，pos 停在 msg2 起点。
        assert!(try_take_fix_message(&mut buffer).unwrap().is_none());
        buffer.compact();
        assert_eq!(buffer.pos, 0);
        assert_eq!(buffer.buf.len(), split);

        buffer.extend(&msg2.as_bytes()[split..]);
        let taken2 = try_take_fix_message(&mut buffer).unwrap();
        assert_eq!(taken2.as_deref(), Some(msg2.as_str()));

        // 全部消费后 compact 直接清空，无残留搬移。
        buffer.compact();
        assert!(buffer.buf.is_empty());
        assert_eq!(buffer.pos, 0);

        // 纯垃圾且无消息头：只保留末尾 64 字节。
        let mut garbage_buffer = FixReadBuffer::with_capacity(64);
        garbage_buffer.extend(&[b'x'; 200]);
        assert!(try_take_fix_message(&mut garbage_buffer).unwrap().is_none());
        assert_eq!(garbage_buffer.buf.len() - garbage_buffer.pos, 64);
    }

    #[test]
    fn inflight_table_removes_all_indexes() {
        let mut table = InflightFixTable::new();
        table.insert(InflightFixRequest {
            req_type: TradeRequestType::BinanceWsCancelMarginOrder,
            client_order_id: 42,
            orig_client_order_id: Some(41),
            seq_num: 7,
        });
        assert_eq!(table.key_by_seq(7), Some(42));
        assert_eq!(table.by_orig_cl_ord_id.get(&41).copied(), Some(42));

        let removed = table.remove(42).expect("entry exists");
        assert_eq!(removed.seq_num, 7);
        assert!(table.by_cl_ord_id.is_empty());
        assert!(table.by_orig_cl_ord_id.is_empty());
        assert!(table.by_seq.is_empty());
        assert!(table.remove(42).is_none());
    }
}
