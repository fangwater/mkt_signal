use crate::common::exchange::Exchange;
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
    cmd_rx: mpsc::UnboundedReceiver<WsCommand>,
    resp_tx: mpsc::UnboundedSender<TradeExecOutcome>,
    pending: VecDeque<TradeRequestMsg>,
    inflight: HashMap<i64, TradeRequestType>,
    last_dispatched_type: TradeRequestType,
    shutdown: bool,
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
        Self {
            id,
            exchange,
            local_ip,
            url,
            connect_timeout_ms,
            ping_interval_ms,
            max_inflight,
            login_payload,
            cmd_rx,
            resp_tx,
            pending: VecDeque::new(),
            inflight: HashMap::new(),
            last_dispatched_type: TradeRequestType::BinanceNewUMOrder,
            shutdown: false,
        }
    }

    pub fn local_ip(&self) -> IpAddr {
        self.local_ip
    }

    pub async fn run(mut self) {
        let mut backoff_ms = 500u64;
        while !self.shutdown {
            match self.establish_connection().await {
                Ok(mut ws) => {
                    info!(
                        "trade ws client id={} established connection to {} via {}",
                        self.id, self.url, self.local_ip
                    );
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
            if self.shutdown {
                break;
            }
            time::sleep(Duration::from_millis(backoff_ms)).await;
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

            if self.shutdown {
                return Ok(());
            }
        }
    }

    async fn establish_connection(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let url = Url::parse(&self.url).with_context(|| "invalid websocket url")?;
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
            .find(|addr| match (addr, self.local_ip) {
                (SocketAddr::V4(_), IpAddr::V4(_)) => true,
                (SocketAddr::V6(_), IpAddr::V6(_)) => true,
                _ => false,
            })
            .ok_or_else(|| anyhow!("no compatible address family for {}", self.local_ip))?;

        let socket = match self.local_ip {
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

        let connect_timeout = Duration::from_millis(self.connect_timeout_ms);
        let stream = tokio::time::timeout(connect_timeout, socket.connect(target))
            .await
            .map_err(|_| anyhow!("connect timeout {}", self.url))?
            .with_context(|| format!("connect {}", target))?;

        let (mut ws_stream, _resp) = if url.scheme().eq_ignore_ascii_case("wss") {
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

        if let Some(payload) = &self.login_payload {
            info!(
                "trade ws client id={} sending login payload ({} bytes)",
                self.id,
                payload.len()
            );
            ws_stream
                .send(Message::Text(payload.clone()))
                .await
                .with_context(|| "send login payload")?;
        }
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
        let (req_type, client_order_id) = self.extract_correlated(payload);
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
        if let Some(id) = val.get("clientOrderId") {
            if let Some(as_i64) = id.as_i64() {
                return Some(as_i64);
            }
            if let Some(s) = id.as_str() {
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
                }
            }
        }
        if let Some(id) = val.get("origClientOrderId") {
            if let Some(as_i64) = id.as_i64() {
                return Some(as_i64);
            }
            if let Some(s) = id.as_str() {
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
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

    async fn send_ping(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<()> {
        debug!("trade ws client id={} sending ping", self.id);
        ws.send(Message::Ping(Vec::new())).await?;
        Ok(())
    }
}
