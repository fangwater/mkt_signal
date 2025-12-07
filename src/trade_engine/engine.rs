use crate::common::exchange::Exchange;
use crate::common::ipc_service_name::build_service_name;
use crate::trade_engine::config::{ApiKey, WsConstants};
use crate::trade_engine::dispatcher::Dispatcher;
use crate::trade_engine::trade_request::TradeRequestMsg;
use crate::trade_engine::trade_response_handle::{spawn_response_handle, TradeExecOutcome};
use crate::trade_engine::trade_type_mapping::TradeTypeMapping;
use crate::trade_engine::ws_client::{TradeWsClient, WsCommand};
use anyhow::{anyhow, Result};
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::net::IpAddr;

pub struct TradeEngine {
    local_ips: Vec<IpAddr>,
    accounts: Vec<ApiKey>,
    req_tx: Option<tokio::sync::mpsc::UnboundedSender<TradeRequestMsg>>,
}

impl TradeEngine {
    pub fn new(local_ips: Vec<IpAddr>, accounts: Vec<ApiKey>) -> Self {
        Self {
            local_ips,
            accounts,
            req_tx: None,
        }
    }

    pub fn sender(&self) -> Option<tokio::sync::mpsc::UnboundedSender<TradeRequestMsg>> {
        self.req_tx.clone()
    }

    pub fn send(&self, req: TradeRequestMsg) -> anyhow::Result<()> {
        if let Some(tx) = &self.req_tx {
            tx.send(req)
                .map_err(|_| anyhow::anyhow!("trade engine not accepting requests"))
        } else {
            Err(anyhow::anyhow!("trade engine not started"))
        }
    }

    pub async fn run(mut self, exchange_name: String) -> Result<()> {
        let canonical_exchange = exchange_name.to_ascii_lowercase();
        if !matches!(
            canonical_exchange.as_str(),
            "binance" | "okex" | "bybit" | "bitget" | "gate"
        ) {
            return Err(anyhow!(
                "unsupported exchange '{}'. Allowed: binance, okex, bybit, bitget, gate",
                canonical_exchange
            ));
        }

        // 构建带命名空间的服务名
        let order_req_service = build_service_name(&format!("order_reqs/{}", canonical_exchange));
        let order_resp_service = build_service_name(&format!("order_resps/{}", canonical_exchange));

        info!(
            "trade_engine starting; exchange={}, req_service='{}', resp_service='{}'",
            canonical_exchange, order_req_service, order_resp_service
        );

        // Iceoryx subscriber for order requests
        let node_name = format!("trade_engine_{}", canonical_exchange);
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&order_req_service)?)
            .publish_subscribe::<[u8; 4096]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let subscriber: Subscriber<ipc::Service, [u8; 4096], ()> =
            service.subscriber_builder().create()?;
        debug!("subscriber created for service: {}", order_req_service);

        // Result publisher
        let resp_service = node
            .service_builder(&ServiceName::new(&order_resp_service)?)
            .publish_subscribe::<[u8; 16384]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let resp_publisher: Publisher<ipc::Service, [u8; 16384], ()> =
            resp_service.publisher_builder().create()?;
        debug!("publisher created for service: {}", order_resp_service);

        // 解析 exchange 枚举
        let exchange = match canonical_exchange.as_str() {
            "binance" => Exchange::Binance,
            "okex" => Exchange::Okex,
            "bybit" => Exchange::Bybit,
            "bitget" => Exchange::BitgetFutures,
            "gate" => Exchange::Gate,
            other => {
                return Err(anyhow!(
                    "unsupported exchange '{}'. Allowed: binance, okex, bybit, bitget, gate",
                    other
                ))
            }
        };

        // Internal mpsc pipeline
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<TradeRequestMsg>();
        self.req_tx = Some(req_tx.clone());
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();

        if exchange == Exchange::Binance && self.accounts.is_empty() {
            return Err(anyhow!("Binance requires API keys in config"));
        }

        // 初始化 REST dispatcher（用于 Binance）
        let rest_dispatcher = if exchange == Exchange::Binance {
            Some(Dispatcher::new(&self.local_ips, &self.accounts)?)
        } else {
            None
        };

        // 初始化 WebSocket 客户端（用于 OKEx）
        let ws_endpoints = if exchange == Exchange::Okex {
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("okex ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
                local_ips.push("0.0.0.0".parse()?);
            } else if local_ips.len() == 1 {
                local_ips.push(local_ips[0]);
                warn!(
                    "okex ws local_ips only 1 provided; duplicating {} for dual connection",
                    local_ips[0]
                );
            } else if local_ips.len() > 2 {
                local_ips.truncate(2);
                warn!(
                    "okex ws local_ips >2; truncating to first two ({}, {})",
                    local_ips[0], local_ips[1]
                );
            }

            let urls = vec![
                WsConstants::OKEX_BUSINESS_WS_URL.to_string(),
                WsConstants::OKEX_BUSINESS_WS_URL.to_string(),
            ];

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut endpoints = Vec::with_capacity(urls.len());
            for (idx, (ip, url)) in local_ips.into_iter().zip(urls.into_iter()).enumerate() {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    url,
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None, // OKEx 认证会自动从环境变量读取
                    rx,
                    resp_tx.clone(),
                );
                info!(
                    "spawning ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                tokio::task::spawn_local(async move {
                    client.run().await;
                });
                endpoints.push(tx);
            }
            Some(endpoints)
        } else {
            None
        };

        // Spawn unified request router
        let _req_worker = tokio::task::spawn_local(async move {
            let mut rest_dispatcher = rest_dispatcher;
            let mut ws_endpoints = ws_endpoints;
            let mut ws_rr_cursor = 0usize; // 轮询计数器

            while let Some(msg) = req_rx.recv().await {
                debug!(
                    "routing request: type={:?}, client_order_id={}",
                    msg.req_type, msg.client_order_id
                );

                // 根据 mapping 判断是否走 WebSocket
                if TradeTypeMapping::is_websocket(msg.req_type) {
                    // 走 WebSocket - 直接轮询分配
                    if let Some(ref mut endpoints) = ws_endpoints {
                        let len = endpoints.len();
                        if len == 0 {
                            warn!("no websocket endpoints available");
                            continue;
                        }

                        let start = ws_rr_cursor;
                        ws_rr_cursor = (ws_rr_cursor + 1) % len;

                        let mut sent = false;
                        for offset in 0..len {
                            let idx = (start + offset) % len;
                            debug!(
                                "routing order client_order_id={} to ws endpoint {}",
                                msg.client_order_id, idx
                            );
                            if endpoints[idx].send(WsCommand::Send(msg.clone())).is_ok() {
                                sent = true;
                                break;
                            } else {
                                warn!("ws endpoint {} not accepting messages, trying next", idx);
                            }
                        }

                        if !sent {
                            warn!(
                                "all ws endpoints unavailable for client_order_id={}",
                                msg.client_order_id
                            );
                            let body = serde_json::json!({
                                "transport": "ws",
                                "state": "error",
                                "reason": "all websocket endpoints unavailable",
                                "clientOrderId": msg.client_order_id,
                            })
                            .to_string();
                            let _ = resp_tx.send(TradeExecOutcome {
                                req_type: msg.req_type,
                                client_order_id: msg.client_order_id,
                                status: 503,
                                body,
                                exchange,
                                ip_used_weight_1m: None,
                                order_count_1m: None,
                            });
                        }
                    } else {
                        warn!(
                            "request type {:?} requires WebSocket but no WS endpoints available",
                            msg.req_type
                        );
                    }
                } else {
                    // 走 REST
                    if let Some(ref mut dispatcher) = rest_dispatcher {
                        let endpoint = TradeTypeMapping::get_endpoint(msg.req_type).to_string();
                        let method = TradeTypeMapping::get_method(msg.req_type).to_string();
                        let weight = TradeTypeMapping::get_weight(msg.req_type);
                        debug!(
                            "dispatch mapping: type={:?} -> {} {} (weight={})",
                            msg.req_type, method, endpoint, weight
                        );

                        let params: std::collections::BTreeMap<String, String> =
                            match std::str::from_utf8(&msg.params) {
                                Ok(s) => url::form_urlencoded::parse(s.as_bytes())
                                    .into_owned()
                                    .collect(),
                                Err(_) => std::collections::BTreeMap::new(),
                            };

                        let evt = crate::trade_engine::order_event::OrderRequestEvent {
                            endpoint,
                            method,
                            params,
                            weight: Some(weight),
                            account: None,
                            req_id: Some(msg.client_order_id.to_string()),
                        };

                        match dispatcher.dispatch(evt).await {
                            Ok(outcome) => {
                                debug!(
                                    "http outcome: status={}, ip={}, body_len={}",
                                    outcome.status,
                                    outcome.ip,
                                    outcome.body.len()
                                );
                                let _ = resp_tx.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: outcome.status,
                                    body: outcome.body,
                                    exchange,
                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                    order_count_1m: outcome.order_count_1m,
                                });
                            }
                            Err(e) => {
                                debug!("http error: {}", e);
                                let _ = resp_tx.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: 0,
                                    body: e.to_string(),
                                    exchange,
                                    ip_used_weight_1m: None,
                                    order_count_1m: None,
                                });
                            }
                        }
                    } else {
                        warn!(
                            "request type {:?} requires REST but no REST dispatcher available",
                            msg.req_type
                        );
                    }
                }
            }

            // Shutdown ws clients
            if let Some(ref endpoints) = ws_endpoints {
                for tx in endpoints {
                    let _ = tx.send(WsCommand::Shutdown);
                }
            }
        });

        let _resp_worker = spawn_response_handle(resp_publisher, resp_rx);

        loop {
            match subscriber.receive()? {
                Some(sample) => {
                    let actual_len = {
                        let payload = sample.payload();
                        payload
                            .iter()
                            .rposition(|&x| x != 0)
                            .map(|pos| pos + 1)
                            .unwrap_or(0)
                    };
                    if actual_len == 0 {
                        drop(sample);
                        continue;
                    }
                    let owned = {
                        let payload = sample.payload();
                        bytes::Bytes::copy_from_slice(&payload[..actual_len])
                    };
                    drop(sample);

                    debug!("received payload bytes: {}", actual_len);

                    match crate::trade_engine::trade_request::TradeRequestMsg::parse(&owned) {
                        Some(msg) => {
                            debug!(
                                "enqueue request: type={:?}, client_order_id={}, params_len={}",
                                msg.req_type,
                                msg.client_order_id,
                                msg.params.len()
                            );
                            let _ = req_tx.send(msg);
                        }
                        None => {
                            warn!("invalid trade request binary payload (len={})", actual_len);
                        }
                    }
                }
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}
