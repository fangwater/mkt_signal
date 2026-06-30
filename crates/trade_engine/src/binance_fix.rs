use crate::binance_ws::{
    BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE_ENV, BINANCE_ED25519_PRIVATE_KEY_PATH_ENV,
};
use crate::response_sink::TradeResponseSink;
use crate::trade_request::{
    BinanceCancelOrderParamsRef, BinanceNewOrderParamsRef, TradeRequestMsg, TradeRequestType,
};
use crate::trade_response_handle::TradeExecOutcome;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{NaiveDateTime, Utc};
use log::{debug, info, warn};
use native_tls::TlsConnector as NativeTlsConnector;
use openssl::pkey::{Id as PKeyId, PKey, Private};
use openssl::sign::Signer;
use order_common::{OrderExecutionStatus, OrderType, Side};
use runtime_common::exchange::Exchange;
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

type FixField = (u32, String);
type BinanceFixStream = TlsStream<TcpStream>;

#[derive(Debug)]
pub(crate) struct BinanceSpotFixConfig {
    api_key: String,
    private_key: PKey<Private>,
    url: String,
    sender_comp_id: String,
    target_comp_id: String,
    heartbtint: u64,
    recv_window_ms: String,
    source_ip: Option<IpAddr>,
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

#[derive(Debug, Clone)]
struct InflightFixRequest {
    req_type: TradeRequestType,
    client_order_id: i64,
    cl_ord_id: String,
    orig_cl_ord_id: Option<String>,
}

struct FixOutboundRequest {
    message: String,
    seq_num: i64,
    inflight: InflightFixRequest,
}

struct BinanceSpotFixClient {
    config: BinanceSpotFixConfig,
    rx: mpsc::UnboundedReceiver<TradeRequestMsg>,
    sink: TradeResponseSink,
    shutdown: CancellationToken,
    state: Rc<RefCell<BinanceSpotFixRuntimeState>>,
    next_seq_num: i64,
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
    };
    let task = tokio::task::spawn_local(async move {
        client.run().await;
    });
    (handle, task)
}

impl BinanceSpotFixConfig {
    pub fn from_env(api_key: String, default_source_ip: Option<IpAddr>) -> Result<Self> {
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
        let sender_comp_id = std::env::var("BINANCE_FIX_SENDER_COMP_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(generate_sender_comp_id);
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
        })
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
        let connector = NativeTlsConnector::builder()
            .build()
            .context("build native TLS connector for Binance Spot FIX")?;
        let connector = TlsConnector::from(connector);
        let mut stream = connector
            .connect(&host, tcp)
            .await
            .with_context(|| format!("TLS connect Binance Spot FIX host={host}"))?;
        let mut buffer = Vec::with_capacity(4096);
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
        let mut buffer = Vec::with_capacity(8192);
        let mut inflight_by_cl_ord_id: HashMap<String, InflightFixRequest> = HashMap::new();
        let mut inflight_by_orig_cl_ord_id: HashMap<String, String> = HashMap::new();
        let mut inflight_by_seq: HashMap<i64, String> = HashMap::new();

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
                        Ok(outbound) => {
                            let key = outbound.inflight.cl_ord_id.clone();
                            if let Some(orig) = outbound.inflight.orig_cl_ord_id.clone() {
                                inflight_by_orig_cl_ord_id.insert(orig, key.clone());
                            }
                            inflight_by_seq.insert(outbound.seq_num, key.clone());
                            inflight_by_cl_ord_id.insert(key, outbound.inflight);
                            if let Err(err) = send_fix_message(stream, "Order", &outbound.message).await {
                                let key = inflight_by_seq.remove(&outbound.seq_num);
                                if let Some(key) = key {
                                    if let Some(inflight) = inflight_by_cl_ord_id.remove(&key) {
                                        if let Some(orig) = inflight.orig_cl_ord_id.as_ref() {
                                            inflight_by_orig_cl_ord_id.remove(orig);
                                        }
                                        self.publish_transport_error(inflight, format!("send Binance Spot FIX order failed: {err:#}"));
                                    }
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
                    if !self.handle_incoming(
                        stream,
                        &msg,
                        &mut inflight_by_cl_ord_id,
                        &mut inflight_by_orig_cl_ord_id,
                        &mut inflight_by_seq,
                    ).await? {
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
        inflight_by_cl_ord_id: &mut HashMap<String, InflightFixRequest>,
        inflight_by_orig_cl_ord_id: &mut HashMap<String, String>,
        inflight_by_seq: &mut HashMap<i64, String>,
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
                self.handle_execution_report(
                    msg,
                    inflight_by_cl_ord_id,
                    inflight_by_orig_cl_ord_id,
                    inflight_by_seq,
                );
                Ok(true)
            }
            Some("9") => {
                self.handle_cancel_reject(
                    msg,
                    inflight_by_cl_ord_id,
                    inflight_by_orig_cl_ord_id,
                    inflight_by_seq,
                );
                Ok(true)
            }
            Some("3") => {
                self.handle_reject(
                    msg,
                    inflight_by_cl_ord_id,
                    inflight_by_orig_cl_ord_id,
                    inflight_by_seq,
                );
                Ok(true)
            }
            other => {
                debug!("Binance Spot FIX ignoring msg_type={other:?}");
                Ok(true)
            }
        }
    }

    fn handle_execution_report(
        &self,
        msg: &FixMessage,
        inflight_by_cl_ord_id: &mut HashMap<String, InflightFixRequest>,
        inflight_by_orig_cl_ord_id: &mut HashMap<String, String>,
        inflight_by_seq: &mut HashMap<i64, String>,
    ) {
        let Some(key) =
            self.execution_report_key(msg, inflight_by_cl_ord_id, inflight_by_orig_cl_ord_id)
        else {
            debug!(
                "Binance Spot FIX ignoring unsolicited ExecutionReport cl_ord_id={:?} orig_cl_ord_id={:?} exec_type={:?}",
                msg.get(11),
                msg.get(41),
                msg.get(150)
            );
            return;
        };
        let Some(inflight) = inflight_by_cl_ord_id.remove(&key) else {
            return;
        };
        if let Some(orig) = inflight.orig_cl_ord_id.as_ref() {
            inflight_by_orig_cl_ord_id.remove(orig);
        }
        remove_seq_for_key(inflight_by_seq, &key);

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
            "clientOrderId": inflight.client_order_id,
            "clOrdId": msg.get(11).unwrap_or(""),
            "origClOrdId": msg.get(41).unwrap_or(""),
            "orderId": msg.get(37).unwrap_or("0"),
        })
        .to_string();
        let outcome = TradeExecOutcome {
            req_type: inflight.req_type,
            client_order_id: inflight.client_order_id,
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

    fn execution_report_key(
        &self,
        msg: &FixMessage,
        inflight_by_cl_ord_id: &HashMap<String, InflightFixRequest>,
        inflight_by_orig_cl_ord_id: &HashMap<String, String>,
    ) -> Option<String> {
        if let Some(cl_ord_id) = msg.get(11) {
            if inflight_by_cl_ord_id.contains_key(cl_ord_id) {
                return Some(cl_ord_id.to_string());
            }
            if let Some(cancel_key) = inflight_by_orig_cl_ord_id.get(cl_ord_id) {
                return Some(cancel_key.clone());
            }
        }
        if let Some(orig_cl_ord_id) = msg.get(41) {
            if let Some(cancel_key) = inflight_by_orig_cl_ord_id.get(orig_cl_ord_id) {
                return Some(cancel_key.clone());
            }
            if inflight_by_cl_ord_id.contains_key(orig_cl_ord_id) {
                return Some(orig_cl_ord_id.to_string());
            }
        }
        None
    }

    fn handle_cancel_reject(
        &self,
        msg: &FixMessage,
        inflight_by_cl_ord_id: &mut HashMap<String, InflightFixRequest>,
        inflight_by_orig_cl_ord_id: &mut HashMap<String, String>,
        inflight_by_seq: &mut HashMap<i64, String>,
    ) {
        let key = msg
            .get(11)
            .filter(|cl| inflight_by_cl_ord_id.contains_key(*cl))
            .map(ToString::to_string)
            .or_else(|| {
                msg.get(41)
                    .and_then(|orig| inflight_by_orig_cl_ord_id.get(orig).cloned())
            });
        let Some(key) = key else {
            debug!("Binance Spot FIX ignoring unsolicited OrderCancelReject");
            return;
        };
        let Some(inflight) = inflight_by_cl_ord_id.remove(&key) else {
            return;
        };
        if let Some(orig) = inflight.orig_cl_ord_id.as_ref() {
            inflight_by_orig_cl_ord_id.remove(orig);
        }
        remove_seq_for_key(inflight_by_seq, &key);
        let error_code = msg.get(25016).and_then(parse_i32).unwrap_or(-2011);
        let body = serde_json::json!({
            "transport": "fix",
            "msgType": "9",
            "code": error_code,
            "msg": msg.get(58).unwrap_or(""),
            "clientOrderId": inflight.client_order_id,
            "clOrdId": msg.get(11).unwrap_or(""),
            "origClOrdId": msg.get(41).unwrap_or(""),
            "orderId": msg.get(37).unwrap_or("0"),
        })
        .to_string();
        let _ = self.sink.send(TradeExecOutcome {
            req_type: inflight.req_type,
            client_order_id: inflight.client_order_id,
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

    fn handle_reject(
        &self,
        msg: &FixMessage,
        inflight_by_cl_ord_id: &mut HashMap<String, InflightFixRequest>,
        inflight_by_orig_cl_ord_id: &mut HashMap<String, String>,
        inflight_by_seq: &mut HashMap<i64, String>,
    ) {
        let key = msg
            .get(45)
            .and_then(parse_i64)
            .and_then(|seq| inflight_by_seq.remove(&seq));
        let Some(key) = key else {
            warn!(
                "Binance Spot FIX session Reject without matching request ref_seq={:?} text={}",
                msg.get(45),
                msg.get(58).unwrap_or("")
            );
            return;
        };
        let Some(inflight) = inflight_by_cl_ord_id.remove(&key) else {
            return;
        };
        if let Some(orig) = inflight.orig_cl_ord_id.as_ref() {
            inflight_by_orig_cl_ord_id.remove(orig);
        }
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
            "clientOrderId": inflight.client_order_id,
            "refSeqNum": msg.get(45).unwrap_or(""),
            "refMsgType": msg.get(372).unwrap_or(""),
        })
        .to_string();
        let _ = self.sink.send(TradeExecOutcome {
            req_type: inflight.req_type,
            client_order_id: inflight.client_order_id,
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

    fn build_order_request(&mut self, msg: &TradeRequestMsg) -> Result<FixOutboundRequest> {
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

    fn build_new_order_request(&mut self, msg: &TradeRequestMsg) -> Result<FixOutboundRequest> {
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
        let cl_ord_id = msg.client_order_id.to_string();
        let seq_num = self.next_seq_num();
        let sending_time = current_fix_time();
        let mut fields = self.standard_header_fields("D", seq_num, &sending_time);
        fields.push((11, cl_ord_id.clone()));
        fields.push((38, params.quantity_qv.decimal_string()));
        fields.push((40, ord_type.to_string()));
        if params.order_type.is_limit() {
            fields.push((44, params.price_qv.decimal_string()));
        }
        fields.push((54, fix_side(params.side).to_string()));
        fields.push((55, params.symbol.to_string()));
        if params.order_type == OrderType::Limit && params.ws_margin_limit_maker {
            fields.push((18, "6".to_string()));
        } else if params.order_type.is_limit() {
            fields.push((59, "1".to_string()));
        }
        let message = build_fix_message(&fields);
        Ok(FixOutboundRequest {
            message,
            seq_num,
            inflight: InflightFixRequest {
                req_type: msg.req_type,
                client_order_id: msg.client_order_id,
                cl_ord_id,
                orig_cl_ord_id: None,
            },
        })
    }

    fn build_cancel_order_request(&mut self, msg: &TradeRequestMsg) -> Result<FixOutboundRequest> {
        let params = BinanceCancelOrderParamsRef::from_bytes(&msg.params).ok_or_else(|| {
            anyhow!(
                "Binance Spot FIX cancel order requires typed params, req_type={:?}",
                msg.req_type
            )
        })?;
        let cl_ord_id = msg.client_order_id.to_string();
        let orig_cl_ord_id = params.orig_client_order_id.to_string();
        let seq_num = self.next_seq_num();
        let sending_time = current_fix_time();
        let mut fields = self.standard_header_fields("F", seq_num, &sending_time);
        fields.push((11, cl_ord_id.clone()));
        fields.push((41, orig_cl_ord_id.clone()));
        fields.push((55, params.symbol.to_string()));
        let message = build_fix_message(&fields);
        Ok(FixOutboundRequest {
            message,
            seq_num,
            inflight: InflightFixRequest {
                req_type: msg.req_type,
                client_order_id: msg.client_order_id,
                cl_ord_id,
                orig_cl_ord_id: Some(orig_cl_ord_id),
            },
        })
    }

    fn build_logon(&mut self) -> Result<String> {
        let seq_num = self.next_seq_num();
        let sending_time = current_fix_time();
        let raw_data = self.sign_logon(seq_num, &sending_time)?;
        let mut fields = self.standard_header_fields("A", seq_num, &sending_time);
        fields.push((25000, self.config.recv_window_ms.clone()));
        fields.push((95, raw_data.len().to_string()));
        fields.push((96, raw_data));
        fields.push((98, "0".to_string()));
        fields.push((108, self.config.heartbtint.to_string()));
        fields.push((141, "Y".to_string()));
        fields.push((553, self.config.api_key.clone()));
        fields.push((25035, "2".to_string()));
        Ok(build_fix_message(&fields))
    }

    fn build_logout(&mut self, text: &str) -> String {
        let seq_num = self.next_seq_num();
        let sending_time = current_fix_time();
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
        let sending_time = current_fix_time();
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

async fn read_fix_message(stream: &mut BinanceFixStream, buffer: &mut Vec<u8>) -> Result<String> {
    loop {
        if let Some(msg) = try_take_fix_message(buffer)? {
            return Ok(msg);
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(anyhow!("Binance Spot FIX socket closed"));
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
}

fn try_take_fix_message(buffer: &mut Vec<u8>) -> Result<Option<String>> {
    let Some(start) = find_bytes(buffer, b"8=FIX.4.4\x01") else {
        if buffer.len() > 64 {
            let keep_from = buffer.len() - 64;
            buffer.drain(..keep_from);
        }
        return Ok(None);
    };
    if start > 0 {
        buffer.drain(..start);
    }
    let Some(first_soh) = buffer.iter().position(|b| *b == SOH_BYTE) else {
        return Ok(None);
    };
    if buffer.get(first_soh + 1..first_soh + 3) != Some(b"9=") {
        return Err(anyhow!("invalid FIX message: BodyLength tag missing"));
    }
    let Some(second_soh_rel) = buffer[first_soh + 1..].iter().position(|b| *b == SOH_BYTE) else {
        return Ok(None);
    };
    let second_soh = first_soh + 1 + second_soh_rel;
    let body_len_raw = std::str::from_utf8(&buffer[first_soh + 3..second_soh])?;
    let body_len = body_len_raw
        .parse::<usize>()
        .with_context(|| format!("parse FIX BodyLength={body_len_raw}"))?;
    let body_start = second_soh + 1;
    let total_len = body_start + body_len + b"10=000\x01".len();
    if buffer.len() < total_len {
        return Ok(None);
    }
    let raw = buffer.drain(..total_len).collect::<Vec<u8>>();
    let msg = String::from_utf8(raw).context("FIX message must be ASCII/UTF-8")?;
    Ok(Some(msg))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn build_fix_message(fields: &[FixField]) -> String {
    debug_assert!(fields.first().is_some_and(|(tag, _)| *tag == 35));
    let mut body = String::new();
    for (tag, value) in fields {
        push_fix_field(&mut body, *tag, value);
    }
    let mut out = String::new();
    push_fix_field(&mut out, 8, "FIX.4.4");
    push_fix_field(&mut out, 9, &body.len().to_string());
    out.push_str(&body);
    let checksum = checksum(&out);
    push_fix_field(&mut out, 10, &format!("{checksum:03}"));
    out
}

fn push_fix_field(out: &mut String, tag: u32, value: &str) {
    out.push_str(&tag.to_string());
    out.push('=');
    out.push_str(value);
    out.push(SOH);
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

fn current_fix_time() -> String {
    Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string()
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

fn remove_seq_for_key(map: &mut HashMap<i64, String>, key: &str) {
    if let Some(seq) = map
        .iter()
        .find_map(|(seq, value)| (value == key).then_some(*seq))
    {
        map.remove(&seq);
    }
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
}
