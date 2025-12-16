use crate::common::exchange::Exchange;
use crate::portfolio_margin::okex_auth::OkexCredentials;
use crate::trade_engine::okex::{OkexCancelOrderRequest, OkexNewOrderRequest, OkexWsOrderResponse};
use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use native_tls::TlsConnector;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::time;
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::{
    client_async, tungstenite::protocol::frame::coding::CloseCode, tungstenite::Message,
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

fn extract_okex_login_timestamp(payload: &str) -> Option<String> {
    let v = serde_json::from_str::<Value>(payload).ok()?;
    v.get("args")?
        .get(0)?
        .get("timestamp")?
        .as_str()
        .map(|s| s.to_string())
}

#[derive(Debug)]
pub enum WsCommand {
    Send(TradeRequestMsg),
    Shutdown,
}

pub struct TradeWsClient {
    id: usize,
    exchange: Exchange,
    local_ip: IpAddr,
    url: String,
    connect_timeout_ms: u64,
    ping_interval_ms: u64,
    max_inflight: usize,
    login_payload: Option<String>,
    okex_creds: Option<OkexCredentials>,
    cmd_rx: mpsc::UnboundedReceiver<WsCommand>,
    resp_tx: mpsc::UnboundedSender<TradeExecOutcome>,
    pending: VecDeque<TradeRequestMsg>,
    inflight: HashMap<i64, TradeRequestType>,
    last_dispatched_type: TradeRequestType,
    shutdown: bool,
    should_reconnect: bool, // 标记是否需要重连（用于 notice 触发的重连）
    last_okex_login_ts: Option<String>,
}

impl TradeWsClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        exchange: Exchange,
        local_ip: IpAddr,
        url: String,
        connect_timeout_ms: u64,
        ping_interval_ms: u64,
        max_inflight: usize,
        login_payload: Option<String>,
        cmd_rx: mpsc::UnboundedReceiver<WsCommand>,
        resp_tx: mpsc::UnboundedSender<TradeExecOutcome>,
    ) -> Self {
        let (login_payload, okex_creds) = if exchange == Exchange::Okex {
            // OKX login must use a fresh timestamp (signed) on each connect/reconnect.
            // Keep credentials and rebuild payload in `run()`.
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
            (None, Some(creds))
        } else {
            (login_payload, None)
        };

        Self {
            id,
            exchange,
            local_ip,
            url,
            connect_timeout_ms,
            ping_interval_ms,
            max_inflight,
            login_payload,
            okex_creds,
            cmd_rx,
            resp_tx,
            pending: VecDeque::new(),
            inflight: HashMap::new(),
            last_dispatched_type: match exchange {
                Exchange::Okex => TradeRequestType::OkexNewUMOrder,
                _ => TradeRequestType::BinanceNewUMOrder,
            },
            shutdown: false,
            should_reconnect: false,
            last_okex_login_ts: None,
        }
    }

    pub fn local_ip(&self) -> IpAddr {
        self.local_ip
    }

    pub async fn run(mut self) {
        let mut backoff_ms = 500u64;
        while !self.shutdown {
            let local_ip = self.local_ip;
            let url = self.url.clone();
            let connect_timeout_ms = self.connect_timeout_ms;
            tokio::select! {
                biased;
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WsCommand::Send(msg)) => {
                            debug!(
                                "trade ws client id={} queued order while disconnected client_order_id={}",
                                self.id, msg.client_order_id
                            );
                            self.pending.push_back(msg);
                            continue;
                        }
                        Some(WsCommand::Shutdown) | None => {
                            info!("trade ws client id={} shutdown requested while disconnected", self.id);
                            self.shutdown = true;
                            break;
                        }
                    }
                }
                res = Self::establish_connection_with(local_ip, &url, connect_timeout_ms) => {
                    match res {
                        Ok(mut ws) => {
                            info!(
                                "trade ws client id={} established connection to {} via {}",
                                self.id, self.url, self.local_ip
                            );

                            let login_payload = if self.exchange == Exchange::Okex {
                                self.okex_creds
                                    .as_ref()
                                    .map(|c| c.build_login_message().to_string())
                            } else {
                                self.login_payload.clone()
                            };

                            if let Some(payload) = login_payload {
                                self.last_okex_login_ts = extract_okex_login_timestamp(&payload);
                                let now_s = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let now_ms = chrono::Utc::now().timestamp_millis();
                                info!(
                                    "trade ws client id={} sending login payload ({} bytes) okx_ts={:?} local_unix_s={} local_unix_ms={}",
                                    self.id,
                                    payload.len(),
                                    self.last_okex_login_ts.as_deref(),
                                    now_s,
                                    now_ms
                                );
                                if let Err(err) = ws.send(Message::Text(payload)).await {
                                    warn!(
                                        "trade ws client id={} send login payload failed: {}",
                                        self.id, err
                                    );
                                    let _ = ws.close(None).await;
                                    continue;
                                }
                            }

                            backoff_ms = 500;
                            if let Err(err) = self.event_loop(&mut ws).await {
                                warn!(
                                    "trade ws client id={} connection loop exited: {}",
                                    self.id, err
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                "trade ws client id={} failed to connect ({}), retrying in {} ms",
                                self.id, err, backoff_ms
                            );
                        }
                    }
                }
            }
            if self.shutdown {
                break;
            }
            tokio::select! {
                biased;
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WsCommand::Send(msg)) => {
                            debug!(
                                "trade ws client id={} queued order during backoff client_order_id={}",
                                self.id, msg.client_order_id
                            );
                            self.pending.push_back(msg);
                        }
                        Some(WsCommand::Shutdown) | None => {
                            info!("trade ws client id={} shutdown requested during backoff", self.id);
                            self.shutdown = true;
                        }
                    }
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
        self.flush_pending(ws).await?;
        loop {
            tokio::select! {
                biased;
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WsCommand::Send(msg)) => {
                            debug!("trade ws client id={} received order client_order_id={}", self.id, msg.client_order_id);
                            self.handle_send(msg, ws).await?;
                        }
                        Some(WsCommand::Shutdown) => {
                            info!("trade ws client id={} received shutdown signal", self.id);
                            self.shutdown = true;
                            let _ = ws.close(None).await;
                            return Ok(());
                        }
                        None => {
                            info!("trade ws client id={} command channel closed", self.id);
                            self.shutdown = true;
                            let _ = ws.close(None).await;
                            return Ok(());
                        }
                    }
                }
                message = ws.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            self.handle_incoming(ws, msg).await?;
                        }
                        Some(Err(err)) => {
                            return Err(anyhow!("websocket errored: {}", err));
                        }
                        None => {
                            return Err(anyhow!("websocket closed by remote"));
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    self.send_ping(ws).await?;
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

    async fn establish_connection_with(
        local_ip: IpAddr,
        url_str: &str,
        connect_timeout_ms: u64,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let url = Url::parse(url_str).with_context(|| "invalid websocket url")?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow!("websocket url missing host"))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| anyhow!("websocket url missing port"))?;

        let mut candidates = lookup_host((host, port))
            .await
            .with_context(|| format!("resolve {}:{}", host, port))?;
        let target = candidates
            .find(|addr| match (addr, local_ip) {
                (SocketAddr::V4(_), IpAddr::V4(_)) => true,
                (SocketAddr::V6(_), IpAddr::V6(_)) => true,
                _ => false,
            })
            .ok_or_else(|| anyhow!("no compatible address family for {}", local_ip))?;

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

        let connect_timeout = Duration::from_millis(connect_timeout_ms);
        let stream = tokio::time::timeout(connect_timeout, socket.connect(target))
            .await
            .map_err(|_| anyhow!("connect timeout {}", url_str))?
            .with_context(|| format!("connect {}", target))?;

        let (ws_stream, _resp) = if url.scheme().eq_ignore_ascii_case("wss") {
            let native = TlsConnector::builder()
                .build()
                .with_context(|| "build tls connector")?;
            let connector = TokioTlsConnector::from(native);
            let tls = connector
                .connect(host, stream)
                .await
                .with_context(|| "tls handshake failed")?;
            client_async(url.as_str(), MaybeTlsStream::NativeTls(tls))
                .await
                .with_context(|| "websocket handshake (wss)")?
        } else {
            client_async(url.as_str(), MaybeTlsStream::Plain(stream))
                .await
                .with_context(|| "websocket handshake (ws)")?
        };

        Ok(ws_stream)
    }

    async fn handle_send(
        &mut self,
        msg: TradeRequestMsg,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
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

    async fn flush_pending(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        while let Some(msg) = self.pending.pop_front() {
            if let Err(err) = self.send_one(ws, &msg).await {
                warn!(
                    "trade ws client id={} send failed for order {}: {}",
                    self.id, msg.client_order_id, err
                );
                self.pending.push_front(msg);
                return Err(err);
            }
            self.track_inflight(&msg);
            self.notify_sent(&msg);
        }
        Ok(())
    }

    async fn send_one(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        msg: &TradeRequestMsg,
    ) -> Result<()> {
        let payload = self.build_payload(msg)?;
        ws.send(Message::Text(payload)).await?;
        Ok(())
    }

    fn build_payload(&self, msg: &TradeRequestMsg) -> Result<String> {
        match self.exchange {
            Exchange::Okex => self.build_okex_payload(msg),
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

    fn build_okex_payload(&self, msg: &TradeRequestMsg) -> Result<String> {
        use crate::trade_engine::okex::ToOkexWsJson;
        use crate::trade_engine::trade_request::TradeRequestHeader;

        let header = TradeRequestHeader {
            msg_type: msg.req_type as u32,
            params_length: msg.params.len() as u32,
            create_time: msg.create_time,
            client_order_id: msg.client_order_id,
        };

        let json_val = match msg.req_type {
            TradeRequestType::OkexNewMarginOrder | TradeRequestType::OkexNewUMOrder => {
                OkexNewOrderRequest {
                    header,
                    params: msg.params.clone(),
                }
                .to_ws_json()
            }
            TradeRequestType::OkexCancelMarginOrder | TradeRequestType::OkexCancelUMOrder => {
                OkexCancelOrderRequest {
                    header,
                    params: msg.params.clone(),
                }
                .to_ws_json()
            }
            _ => None,
        };

        let payload = json_val.ok_or_else(|| {
            anyhow!(
                "failed to build okex ws payload (req_type={:?}, client_order_id={})",
                msg.req_type,
                msg.client_order_id
            )
        })?;

        serde_json::to_string(&payload).with_context(|| "serialize okex ws payload")
    }

    fn track_inflight(&mut self, msg: &TradeRequestMsg) {
        self.inflight.insert(msg.client_order_id, msg.req_type);
        self.last_dispatched_type = msg.req_type;
    }

    fn notify_sent(&self, msg: &TradeRequestMsg) {
        let body = json!({
            "transport": "ws",
            "state": "sent",
            "clientOrderId": msg.client_order_id,
            "endpointId": self.id,
            "localIp": self.local_ip.to_string(),
        })
        .to_string();
        let _ = self.resp_tx.send(TradeExecOutcome {
            req_type: msg.req_type,
            client_order_id: msg.client_order_id,
            status: 200,
            body,
            exchange: self.exchange,
            ip_used_weight_1m: None,
            order_count_1m: None,
        });
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
        let _ = self.resp_tx.send(TradeExecOutcome {
            req_type: msg.req_type,
            client_order_id: msg.client_order_id,
            status: 429,
            body,
            exchange: self.exchange,
            ip_used_weight_1m: None,
            order_count_1m: None,
        });
    }

    async fn handle_incoming(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        msg: Message,
    ) -> Result<()> {
        match msg {
            Message::Text(text) => {
                self.process_incoming_payload(&text);
            }
            Message::Binary(bin) => match std::str::from_utf8(&bin) {
                Ok(text) => self.process_incoming_payload(text),
                Err(_) => {
                    let encoded = BASE64_STANDARD.encode(&bin);
                    self.publish_generic_response(0, self.last_dispatched_type, encoded, true);
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
                let code = frame.as_ref().map(|f| f.code).unwrap_or(CloseCode::Normal);
                return Err(anyhow!("websocket closed (code={:?})", code));
            }
            Message::Frame(_) => {}
        }
        Ok(())
    }

    fn process_incoming_payload(&mut self, payload: &str) {
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

                        // 从 error 事件中提取 client_order_id，发送错误响应给策略
                        let client_order_id = json_val
                            .get("id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(0);

                        if client_order_id > 0 {
                            // 解析错误码
                            let error_code = code.parse::<i32>().unwrap_or(0);

                            // 发送错误响应
                            let outcome = TradeExecOutcome {
                                req_type: TradeRequestType::OkexNewUMOrder, // 默认，实际类型无法确定
                                exchange: self.exchange,
                                client_order_id,
                                status: 400, // Bad Request
                                body: msg.to_string(),
                                ip_used_weight_1m: None,
                                order_count_1m: None,
                            };

                            let _ = self.resp_tx.send(outcome);
                            info!(
                                "trade ws client id={} sent error response to strategy: client_order_id={} error_code={}",
                                self.id, client_order_id, error_code
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

        let (req_type, client_order_id) = self.extract_correlated(payload);
        if self.exchange == Exchange::Okex {
            if let Some(resp) = OkexWsOrderResponse::from_json_str(payload) {
                let coid = resp.client_order_id().unwrap_or(client_order_id);
                self.publish_okex_ws_response(coid, req_type, &resp);
                return;
            }
        }
        self.publish_generic_response(client_order_id, req_type, payload.to_string(), false);
    }

    fn extract_correlated(&mut self, payload: &str) -> (TradeRequestType, i64) {
        if let Ok(json_val) = serde_json::from_str::<Value>(payload) {
            if let Some(id) = Self::extract_client_order_id(&json_val) {
                if let Some(req_type) = self.inflight.remove(&id) {
                    return (req_type, id);
                }
                return (self.last_dispatched_type, id);
            }
        }
        (self.last_dispatched_type, 0)
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
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
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
        let _ = self.resp_tx.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status: 206,
            body: body_payload,
            exchange: self.exchange,
            ip_used_weight_1m: None,
            order_count_1m: None,
        });
    }

    fn publish_okex_ws_response(
        &self,
        client_order_id: i64,
        req_type: TradeRequestType,
        resp: &OkexWsOrderResponse,
    ) {
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

        let _ = self.resp_tx.send(TradeExecOutcome {
            req_type,
            client_order_id,
            status: 206,
            body: body_payload,
            exchange: self.exchange,
            ip_used_weight_1m: None,
            order_count_1m: None,
        });
    }

    async fn send_ping(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        debug!("trade ws client id={} sending ping", self.id);
        ws.send(Message::Ping(Vec::new())).await?;
        Ok(())
    }
}
