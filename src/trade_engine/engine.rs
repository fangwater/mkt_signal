use crate::common::binance_account_mode::{binance_account_mode, BinanceAccountMode};
use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::{QUERY_REQ_PAYLOAD, QUERY_RESP_PAYLOAD};
use crate::common::ipc_service_name::build_service_name;
use crate::trade_engine::bitget_query_rate_limiter::BitgetQueryRateLimiter;
use crate::trade_engine::config::{ApiKey, WsConstants};
use crate::trade_engine::dispatcher::Dispatcher;
use crate::trade_engine::okex_query_rate_limiter::OkexQueryRateLimiter;
use crate::trade_engine::query_parsers::binance_margin_order::parse_binance_margin_order_query_json;
use crate::trade_engine::query_parsers::binance_pm_balance_snapshot::parse_binance_pm_balance_snapshot;
use crate::trade_engine::query_parsers::binance_spot_account_snapshot_std::parse_binance_spot_account_snapshot_std;
use crate::trade_engine::query_parsers::binance_um_account_snapshot::parse_binance_um_account_snapshot;
use crate::trade_engine::query_parsers::binance_um_balance_snapshot_std::parse_binance_um_balance_snapshot_std;
use crate::trade_engine::query_parsers::binance_um_order::parse_binance_um_order_query_json;
use crate::trade_engine::query_parsers::bitget_account_balance_snapshot::parse_bitget_account_balance_snapshot;
use crate::trade_engine::query_parsers::bitget_order::{
    parse_bitget_order_query_json, BitgetOrderQueryParseErrorKind, BitgetOrderQueryParseResult,
};
use crate::trade_engine::query_parsers::bitget_positions_snapshot::parse_bitget_positions_snapshot;
use crate::trade_engine::query_parsers::bybit_account_balance_snapshot::parse_bybit_account_balance_snapshot;
use crate::trade_engine::query_parsers::bybit_order::{
    parse_bybit_order_query_json, BybitOrderQueryParseErrorKind, BybitOrderQueryParseResult,
};
use crate::trade_engine::query_parsers::bybit_positions_snapshot::parse_bybit_positions_snapshot;
use crate::trade_engine::query_parsers::compact_order::ORDER_QUERY_NOT_FOUND_MARKER;
use crate::trade_engine::query_parsers::gate_positions_snapshot::parse_gate_positions_snapshot_with_meta;
use crate::trade_engine::query_parsers::gate_unified_balance_snapshot::parse_gate_unified_balance_snapshot;
use crate::trade_engine::query_parsers::okex_account_balance_snapshot::parse_okex_account_balance_snapshot;
use crate::trade_engine::query_parsers::okex_order::{
    parse_okex_order_query_json, OkexOrderQueryParseErrorKind, OkexOrderQueryParseResult,
};
use crate::trade_engine::query_parsers::okex_positions_snapshot::parse_okex_positions_snapshot;
use crate::trade_engine::query_request::{QueryRequestMsg, QueryRequestType};
use crate::trade_engine::query_response_handle::{spawn_query_response_handle, QueryExecOutcome};
use crate::trade_engine::query_type_mapping::QueryTypeMapping;
use crate::trade_engine::trade_request::{TradeRequestMsg, TradeRequestType};
use crate::trade_engine::trade_response_handle::{spawn_response_handle, TradeExecOutcome};
use crate::trade_engine::trade_type_mapping::TradeTypeMapping;
use crate::trade_engine::ws_client::{TradeWsClient, WsCommand, WsEndpointHandle};
use anyhow::{anyhow, Result};
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use serde_json::Value;
use std::net::IpAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc as StdRc};
use tokio_util::sync::CancellationToken;

fn request_payload_len(payload: &[u8]) -> Option<usize> {
    // Layout: u32 msg_type, u32 params_length, i64 create_time, i64 client_id, params...
    if payload.len() < 24 {
        return None;
    }
    let params_len = u32::from_le_bytes(payload[4..8].try_into().ok()?) as usize;
    let total = 24usize.saturating_add(params_len);
    if total == 0 || total > payload.len() {
        return None;
    }
    Some(total)
}

fn summarize_bybit_response(body: &str) -> String {
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return format!("non_json body_len={}", body.len());
    };

    let ret_code = v
        .get("retCode")
        .and_then(|x| x.as_i64())
        .map(|x| x.to_string())
        .unwrap_or_else(|| "NA".to_string());
    let ret_msg = v
        .get("retMsg")
        .and_then(|x| x.as_str())
        .unwrap_or("<missing>");

    let list_len = v
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(|x| x.as_array())
        .map(|x| x.len().to_string())
        .unwrap_or_else(|| "NA".to_string());

    format!(
        "retCode={} retMsg={} result.list.len={} body_len={}",
        ret_code,
        ret_msg,
        list_len,
        body.len()
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

async fn join_or_abort(name: &str, mut handle: tokio::task::JoinHandle<()>) {
    match tokio::time::timeout(Duration::from_secs(2), &mut handle).await {
        Ok(Ok(())) => info!("trade_engine worker stopped: {}", name),
        Ok(Err(err)) => warn!("trade_engine worker join error ({}): {}", name, err),
        Err(_) => {
            warn!("trade_engine worker shutdown timeout, aborting: {}", name);
            handle.abort();
            let _ = handle.await;
        }
    }
}

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

    pub async fn run(self, exchange: Exchange) -> Result<()> {
        self.run_with_shutdown(exchange, CancellationToken::new())
            .await
    }

    pub async fn run_with_shutdown(
        mut self,
        exchange: Exchange,
        shutdown: CancellationToken,
    ) -> Result<()> {
        if !matches!(
            exchange,
            Exchange::Binance
                | Exchange::Okex
                | Exchange::Bybit
                | Exchange::Bitget
                | Exchange::Gate
        ) {
            return Err(anyhow!(
                "unsupported exchange '{}'. Allowed: binance, okex, bybit, bitget, gate",
                exchange
            ));
        }

        let canonical_exchange = exchange.as_str();

        // 构建带命名空间的服务名
        let order_req_service = build_service_name(&format!("order_reqs/{}", canonical_exchange));
        let order_resp_service = build_service_name(&format!("order_resps/{}", canonical_exchange));
        let query_req_service = build_service_name(&format!("query_reqs/{}", canonical_exchange));
        let query_resp_service = build_service_name(&format!("query_resps/{}", canonical_exchange));

        info!(
            "trade_engine starting; exchange={}, order_req='{}', order_resp='{}', query_req='{}', query_resp='{}'",
            canonical_exchange, order_req_service, order_resp_service, query_req_service, query_resp_service
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
            .publish_subscribe::<[u8; 64]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let resp_publisher: Publisher<ipc::Service, [u8; 64], ()> =
            resp_service.publisher_builder().create()?;
        debug!("publisher created for service: {}", order_resp_service);

        // Query subscriber/publisher
        let query_service = node
            .service_builder(&ServiceName::new(&query_req_service)?)
            .publish_subscribe::<[u8; QUERY_REQ_PAYLOAD]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let query_subscriber: Subscriber<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()> =
            query_service.subscriber_builder().create()?;
        debug!("subscriber created for service: {}", query_req_service);

        let query_resp_service_obj = node
            .service_builder(&ServiceName::new(&query_resp_service)?)
            .publish_subscribe::<[u8; QUERY_RESP_PAYLOAD]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let query_resp_publisher: Publisher<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()> =
            query_resp_service_obj.publisher_builder().create()?;
        debug!("publisher created for service: {}", query_resp_service);

        // 直接使用传入的 exchange 枚举

        // Internal mpsc pipeline
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<TradeRequestMsg>();
        self.req_tx = Some(req_tx.clone());
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();

        let (query_req_tx, mut query_req_rx) =
            tokio::sync::mpsc::unbounded_channel::<QueryRequestMsg>();
        let (query_resp_tx, query_resp_rx) =
            tokio::sync::mpsc::unbounded_channel::<QueryExecOutcome>();

        if exchange == Exchange::Binance && self.accounts.is_empty() {
            return Err(anyhow!("Binance requires API keys in config"));
        }

        // 初始化 REST dispatcher（用于 Binance）
        let rest_dispatcher = if exchange == Exchange::Binance {
            Some(Rc::new(tokio::sync::Mutex::new(Dispatcher::new(
                &self.local_ips,
                &self.accounts,
                shutdown.clone(),
            )?)))
        } else {
            None
        };

        // 初始化 WebSocket 客户端（用于 OKEx/Gate/Binance）
        let binance_ws_enabled =
            exchange == Exchange::Binance && binance_account_mode() == BinanceAccountMode::Standard;
        if exchange == Exchange::Binance && !binance_ws_enabled {
            info!("binance ws disabled (BINANCE_ACCOUNT_MODE!=STANDARD)");
        }
        let mut worker_handles: Vec<(&'static str, tokio::task::JoinHandle<()>)> = Vec::new();

        let mut gate_futures_ws_endpoints: Option<Vec<WsEndpointHandle>> = None;
        let mut binance_spot_ws_endpoints: Option<Vec<WsEndpointHandle>> = None;

        let ws_endpoints = if exchange == Exchange::Bitget {
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("bitget ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut endpoints = Vec::with_capacity(local_ips.len());
            for (idx, ip) in local_ips.into_iter().enumerate() {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    WsConstants::BITGET_TRADE_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    None,
                    None,
                    rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                );
                info!(
                    "spawning bitget ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("bitget_ws_client", handle));
                endpoints.push(WsEndpointHandle::new(tx, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Bybit {
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("bybit ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut endpoints = Vec::with_capacity(local_ips.len());
            for (idx, ip) in local_ips.into_iter().enumerate() {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    WsConstants::BYBIT_TRADE_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    None,
                    None,
                    rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                );
                info!(
                    "spawning bybit ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("bybit_ws_client", handle));
                endpoints.push(WsEndpointHandle::new(tx, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Okex {
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
                let state = StdRc::new(RefCell::new(Default::default()));
                let client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    url,
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None, // OKEx 认证会自动从环境变量读取
                    None,
                    None,
                    None,
                    rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    state.clone(),
                    false,
                );
                info!(
                    "spawning ws client id={} ip={} max_inflight={}",
                    idx,
                    client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    client.run().await;
                });
                worker_handles.push(("ws_client", handle));
                endpoints.push(WsEndpointHandle::new(tx, state));
            }
            Some(endpoints)
        } else if exchange == Exchange::Gate {
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("gate ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;

            let mut spot_endpoints = Vec::with_capacity(local_ips.len());
            let mut futures_endpoints = Vec::with_capacity(local_ips.len());

            for (idx, ip) in local_ips.into_iter().enumerate() {
                let (spot_tx, spot_rx) = tokio::sync::mpsc::unbounded_channel();
                let spot_state = StdRc::new(RefCell::new(Default::default()));
                let spot_client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    WsConstants::GATE_SPOT_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    Some(crate::trade_engine::gate_ws::GateWsKind::SpotUnified),
                    Some(query_resp_tx.clone()),
                    spot_rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    spot_state.clone(),
                    false,
                );
                info!(
                    "spawning gate spot ws client id={} ip={} max_inflight={}",
                    idx,
                    spot_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    spot_client.run().await;
                });
                worker_handles.push(("gate_spot_ws_client", handle));
                spot_endpoints.push(WsEndpointHandle::new(spot_tx, spot_state));

                let (fut_tx, fut_rx) = tokio::sync::mpsc::unbounded_channel();
                let fut_state = StdRc::new(RefCell::new(Default::default()));
                let fut_client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    WsConstants::GATE_FUTURES_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    None,
                    Some(crate::trade_engine::gate_ws::GateWsKind::FuturesUsdt),
                    Some(query_resp_tx.clone()),
                    fut_rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    fut_state.clone(),
                    false,
                );
                info!(
                    "spawning gate futures ws client id={} ip={} max_inflight={}",
                    idx,
                    fut_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    fut_client.run().await;
                });
                worker_handles.push(("gate_futures_ws_client", handle));
                futures_endpoints.push(WsEndpointHandle::new(fut_tx, fut_state));
            }

            gate_futures_ws_endpoints = Some(futures_endpoints);
            Some(spot_endpoints)
        } else if exchange == Exchange::Binance && binance_ws_enabled {
            let mut local_ips = self.local_ips.clone();
            if local_ips.is_empty() {
                warn!("binance ws local_ips empty; using default binding 0.0.0.0");
                local_ips.push("0.0.0.0".parse()?);
            }

            let connect_timeout_ms = WsConstants::CONNECT_TIMEOUT_MS;
            let ping_interval_ms = WsConstants::PING_INTERVAL_MS;
            let max_inflight = WsConstants::MAX_INFLIGHT;
            let binance_creds = self.accounts.get(0).cloned();

            let shutdown_on_rate_limit = local_ips.len() <= 1;
            let mut um_endpoints = Vec::with_capacity(local_ips.len());
            let mut spot_endpoints = Vec::with_capacity(local_ips.len());
            for (idx, ip) in local_ips.into_iter().enumerate() {
                let (um_tx, um_rx) = tokio::sync::mpsc::unbounded_channel();
                let um_state = StdRc::new(RefCell::new(Default::default()));
                let um_client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    WsConstants::BINANCE_UM_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    binance_creds.clone(),
                    None,
                    Some(query_resp_tx.clone()),
                    um_rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    um_state.clone(),
                    shutdown_on_rate_limit,
                );
                info!(
                    "spawning binance um ws client id={} ip={} max_inflight={}",
                    idx,
                    um_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    um_client.run().await;
                });
                worker_handles.push(("binance_um_ws_client", handle));
                um_endpoints.push(WsEndpointHandle::new(um_tx, um_state));

                let (spot_tx, spot_rx) = tokio::sync::mpsc::unbounded_channel();
                let spot_state = StdRc::new(RefCell::new(Default::default()));
                let spot_client = TradeWsClient::new(
                    idx,
                    exchange,
                    ip,
                    WsConstants::BINANCE_SPOT_WS_URL.to_string(),
                    connect_timeout_ms,
                    ping_interval_ms,
                    max_inflight,
                    None,
                    binance_creds.clone(),
                    None,
                    None,
                    spot_rx,
                    resp_tx.clone(),
                    shutdown.clone(),
                    spot_state.clone(),
                    shutdown_on_rate_limit,
                );
                info!(
                    "spawning binance spot ws client id={} ip={} max_inflight={}",
                    idx,
                    spot_client.local_ip(),
                    max_inflight
                );
                let handle = tokio::task::spawn_local(async move {
                    spot_client.run().await;
                });
                worker_handles.push(("binance_spot_ws_client", handle));
                spot_endpoints.push(WsEndpointHandle::new(spot_tx, spot_state));
            }
            binance_spot_ws_endpoints = Some(spot_endpoints);
            Some(um_endpoints)
        } else {
            None
        };

        // Spawn unified request router
        let ws_endpoints_for_req_worker = ws_endpoints.clone();
        let gate_futures_ws_endpoints_for_req_worker = gate_futures_ws_endpoints.clone();
        let binance_spot_ws_endpoints_for_req_worker = binance_spot_ws_endpoints.clone();
        let rest_dispatcher_for_orders = rest_dispatcher.clone();
        let resp_tx_for_req_worker = resp_tx.clone();
        let exchange_for_req_worker = exchange;
        let shutdown_for_req_worker = shutdown.clone();
        let req_worker = tokio::task::spawn_local(async move {
            let mut ws_endpoints = ws_endpoints_for_req_worker;
            let mut gate_futures_ws_endpoints = gate_futures_ws_endpoints_for_req_worker;
            let mut binance_spot_ws_endpoints = binance_spot_ws_endpoints_for_req_worker;
            let mut ws_rr_cursor = 0usize; // 轮询计数器
            let rest_dispatcher = rest_dispatcher_for_orders;

            loop {
                let Some(msg) = ({
                    tokio::select! {
                        biased;
                        _ = shutdown_for_req_worker.cancelled() => None,
                        msg = req_rx.recv() => msg,
                    }
                }) else {
                    break;
                };
                debug!(
                    "routing request: type={:?}, client_order_id={}",
                    msg.req_type, msg.client_order_id
                );

                // 根据 mapping 判断是否走 WebSocket
                if TradeTypeMapping::is_websocket(msg.req_type) {
                    let mut target_endpoints = if exchange_for_req_worker == Exchange::Gate
                        && matches!(
                            msg.req_type,
                            TradeRequestType::GateFuturesNewOrder
                                | TradeRequestType::GateFuturesCancelOrder
                        ) {
                        gate_futures_ws_endpoints.as_mut()
                    } else if exchange_for_req_worker == Exchange::Binance
                        && matches!(
                            msg.req_type,
                            TradeRequestType::BinanceWsNewMarginOrder
                                | TradeRequestType::BinanceWsCancelMarginOrder
                        )
                    {
                        binance_spot_ws_endpoints.as_mut()
                    } else {
                        ws_endpoints.as_mut()
                    };

                    // 走 WebSocket - 直接轮询分配
                    if let Some(ref mut endpoints) = target_endpoints {
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
                            let _ = resp_tx_for_req_worker.send(TradeExecOutcome {
                                req_type: msg.req_type,
                                client_order_id: msg.client_order_id,
                                status: 503,
                                body,
                                exchange: exchange_for_req_worker,
                                order_id: 0,
                                order_status_u8: 0,
                                order_update_time: 0,
                                executed_qty: 0.0,
                                response_price: 0.0,
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
                    if let Some(dispatcher) = &rest_dispatcher {
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
                            req_type: Some(format!("{:?}", msg.req_type)),
                            endpoint,
                            method,
                            params,
                            weight: Some(weight),
                            account: None,
                            req_id: Some(msg.client_order_id.to_string()),
                            counts_toward_order_limit: TradeTypeMapping::counts_toward_order_limit(
                                msg.req_type,
                            ),
                        };

                        let outcome = {
                            let mut dispatcher = dispatcher.lock().await;
                            dispatcher.dispatch(evt).await
                        };
                        match outcome {
                            Ok(outcome) => {
                                debug!(
                                    "http outcome: status={}, ip={}, body_len={}",
                                    outcome.status,
                                    outcome.ip,
                                    outcome.body.len()
                                );
                                let _ = resp_tx_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: outcome.status,
                                    body: outcome.body,
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
                                });
                            }
                            Err(e) => {
                                debug!("http error: {}", e);
                                let _ = resp_tx_for_req_worker.send(TradeExecOutcome {
                                    req_type: msg.req_type,
                                    client_order_id: msg.client_order_id,
                                    status: 0,
                                    body: e.to_string(),
                                    exchange: exchange_for_req_worker,
                                    order_id: 0,
                                    order_status_u8: 0,
                                    order_update_time: 0,
                                    executed_qty: 0.0,
                                    response_price: 0.0,
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
            if let Some(ref endpoints) = binance_spot_ws_endpoints {
                for tx in endpoints {
                    let _ = tx.send(WsCommand::Shutdown);
                }
            }
        });
        worker_handles.push(("req_worker", req_worker));

        // Query request router
        {
            let rest_dispatcher = rest_dispatcher.clone();
            let exchange_copy = exchange;
            let query_resp_tx = query_resp_tx.clone();
            let binance_ws_endpoints = ws_endpoints.clone();
            let binance_spot_ws_endpoints = binance_spot_ws_endpoints.clone();
            let gate_spot_ws_endpoints = ws_endpoints.clone();
            let gate_futures_ws_endpoints = gate_futures_ws_endpoints.clone();
            let shutdown_for_query_router = shutdown.clone();
            let query_router = tokio::task::spawn_local(async move {
                let okex_http = reqwest::Client::new();
                let okex_creds =
                    crate::portfolio_margin::okex_auth::OkexCredentials::from_env().ok();
                let bybit_http = reqwest::Client::new();
                let bybit_creds =
                    crate::portfolio_margin::bybit_auth::BybitCredentials::from_env().ok();
                let bitget_http = reqwest::Client::new();
                let bitget_creds =
                    crate::portfolio_margin::bitget_auth::BitgetCredentials::from_env().ok();
                let gate_http = reqwest::Client::new();
                let gate_creds =
                    crate::portfolio_margin::gate_auth::GateCredentials::from_env().ok();
                let mut binance_query_rr = 0usize;
                let mut gate_query_rr = 0usize;
                let mut gate_futures_query_rr = 0usize;
                let mut okex_query_rate_limiter = OkexQueryRateLimiter::default();
                let mut bitget_query_rate_limiter = BitgetQueryRateLimiter::default();

                'query_router: loop {
                    let Some(msg) = ({
                        tokio::select! {
                            biased;
                            _ = shutdown_for_query_router.cancelled() => None,
                            msg = query_req_rx.recv() => msg,
                        }
                    }) else {
                        break;
                    };
                    debug!(
                        "routing query: type={:?} client_query_id={}",
                        msg.req_type, msg.client_query_id
                    );

                    match exchange_copy {
                        Exchange::Binance => {
                            if msg.req_type == QueryRequestType::BinanceWsUMQuery
                                || msg.req_type == QueryRequestType::BinanceWsMarginQuery
                            {
                                let target_endpoints =
                                    if msg.req_type == QueryRequestType::BinanceWsMarginQuery {
                                        binance_spot_ws_endpoints.as_ref()
                                    } else {
                                        binance_ws_endpoints.as_ref()
                                    };

                                let Some(endpoints) = target_endpoints else {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no binance ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                };
                                if endpoints.is_empty() {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no binance ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                }

                                let len = endpoints.len();
                                let start = binance_query_rr;
                                binance_query_rr = (binance_query_rr + 1) % len;

                                let mut sent = false;
                                for offset in 0..len {
                                    let idx = (start + offset) % len;
                                    if endpoints[idx]
                                        .send(WsCommand::SendQuery(msg.clone()))
                                        .is_ok()
                                    {
                                        sent = true;
                                        break;
                                    }
                                }

                                if !sent {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"binance ws endpoints unavailable",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                continue;
                            }

                            if !QueryTypeMapping::is_binance_rest(msg.req_type) {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for binance engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(dispatcher) = &rest_dispatcher else {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 503,
                                    body: bytes::Bytes::from_static(
                                        b"no rest dispatcher available",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type).to_string();
                            let method = QueryTypeMapping::get_method(msg.req_type).to_string();
                            let weight = QueryTypeMapping::get_weight(msg.req_type);
                            let params: std::collections::BTreeMap<String, String> =
                                match std::str::from_utf8(&msg.params) {
                                    Ok(s) => url::form_urlencoded::parse(s.as_bytes())
                                        .into_owned()
                                        .collect(),
                                    Err(_) => std::collections::BTreeMap::new(),
                                };

                            let evt = crate::trade_engine::order_event::OrderRequestEvent {
                                req_type: Some(format!("{:?}", msg.req_type)),
                                endpoint,
                                method,
                                params,
                                weight: Some(weight),
                                account: None,
                                req_id: Some(msg.client_query_id.to_string()),
                                counts_toward_order_limit: false,
                            };

                            let outcome = {
                                let mut dispatcher = dispatcher.lock().await;
                                dispatcher.dispatch(evt).await
                            };
                            match outcome {
                                Ok(outcome) => {
                                    match msg.req_type {
                                        crate::trade_engine::query_request::QueryRequestType::BinanceUMQuery
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(v) = parse_binance_um_order_query_json(&outcome.body) {
                                                let _ = query_resp_tx.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: v.to_bytes(),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            } else {
                                                warn!(
                                                    "binance um order query parse failed: client_query_id={} body_len={}",
                                                    msg.client_query_id,
                                                    outcome.body.len()
                                                );
                                                let _ = query_resp_tx.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: bytes::Bytes::from_static(b"E"),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BinanceMarginQuery
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(v) = parse_binance_margin_order_query_json(&outcome.body) {
                                                let _ = query_resp_tx.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: v.to_bytes(),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            } else {
                                                warn!(
                                                    "binance margin order query parse failed: client_query_id={} body_len={}",
                                                    msg.client_query_id,
                                                    outcome.body.len()
                                                );
                                                let _ = query_resp_tx.send(QueryExecOutcome {
                                                    req_type: msg.req_type,
                                                    client_query_id: msg.client_query_id,
                                                    status: outcome.status,
                                                    body: bytes::Bytes::from_static(b"E"),
                                                    exchange: exchange_copy,
                                                    ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                    query_count_1m: outcome.order_count_1m,
                                                });
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BinancePmBalanceSnapshot
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) = parse_binance_pm_balance_snapshot(&outcome.body) {
                                                for payload in msgs {
                                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BinanceUmBalanceSnapshotStd
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_binance_um_balance_snapshot_std(&outcome.body)
                                            {
                                                for payload in msgs {
                                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BinanceUmAccountSnapshot
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) = parse_binance_um_account_snapshot(&outcome.body) {
                                                for payload in msgs {
                                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BinanceUmAccountSnapshotStd
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) = parse_binance_um_account_snapshot(&outcome.body) {
                                                for payload in msgs {
                                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BinanceSpotAccountSnapshotStd
                                            if outcome.status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_binance_spot_account_snapshot_std(&outcome.body)
                                            {
                                                for payload in msgs {
                                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                                        req_type: msg.req_type,
                                                        client_query_id: msg.client_query_id,
                                                        status: outcome.status,
                                                        body: payload,
                                                        exchange: exchange_copy,
                                                        ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                        query_count_1m: outcome.order_count_1m,
                                                    });
                                                }
                                            }
                                        }
                                        _ => {
                                            let _ = query_resp_tx.send(QueryExecOutcome {
                                                req_type: msg.req_type,
                                                client_query_id: msg.client_query_id,
                                                status: outcome.status,
                                                body: bytes::Bytes::from(outcome.body),
                                                exchange: exchange_copy,
                                                ip_used_weight_1m: outcome.ip_used_weight_1m,
                                                query_count_1m: outcome.order_count_1m,
                                            });
                                        }
                                    }
                                }
                                        Err(_e) => {
                                            let _ = query_resp_tx.send(QueryExecOutcome {
                                                req_type: msg.req_type,
                                                client_query_id: msg.client_query_id,
                                                status: 0,
                                        body: bytes::Bytes::from_static(b"E"),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Okex => {
                            if !QueryTypeMapping::is_okex_rest(msg.req_type) {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for okex engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &okex_creds else {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing OKX credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");
                            let path_with_query = if qs.is_empty() {
                                endpoint.to_string()
                            } else {
                                format!("{}?{}", endpoint, qs)
                            };

                            while let Some(block) = okex_query_rate_limiter
                                .should_block(msg.req_type, std::time::Instant::now())
                            {
                                warn!(
                                    "okex query rate limited: req_type={:?} client_query_id={} wait_ms={} queued_in_window={} limit={} window_ms={}",
                                    msg.req_type,
                                    msg.client_query_id,
                                    block.wait_for.as_millis(),
                                    block.queued_in_window,
                                    block.max_requests,
                                    block.window.as_millis()
                                );
                                tokio::select! {
                                    biased;
                                    _ = shutdown_for_query_router.cancelled() => break 'query_router,
                                    _ = tokio::time::sleep(block.wait_for) => {}
                                }
                            }
                            if let Some(snapshot) = okex_query_rate_limiter
                                .record(msg.req_type, std::time::Instant::now())
                            {
                                debug!(
                                    "okex query rate recorded: req_type={:?} client_query_id={} count_in_window={} limit={} window_ms={}",
                                    msg.req_type,
                                    msg.client_query_id,
                                    snapshot.queued_in_window,
                                    snapshot.max_requests,
                                    snapshot.window.as_millis()
                                );
                            }

                            match crate::trade_engine::okex_query::okex_rest_get(
                                &okex_http,
                                creds,
                                &path_with_query,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let body_bytes = match msg.req_type {
                                        crate::trade_engine::query_request::QueryRequestType::OkexMarginQuery
                                            | crate::trade_engine::query_request::QueryRequestType::OkexUMQuery
                                            if status == 200 =>
                                        {
                                            match parse_okex_order_query_json(&body) {
                                                OkexOrderQueryParseResult::Success(v) => {
                                                    v.to_bytes()
                                                }
                                                OkexOrderQueryParseResult::Error {
                                                    kind: OkexOrderQueryParseErrorKind::OrderNotFound,
                                                    ..
                                                } => bytes::Bytes::from_static(
                                                    ORDER_QUERY_NOT_FOUND_MARKER,
                                                ),
                                                OkexOrderQueryParseResult::Error {
                                                    kind: OkexOrderQueryParseErrorKind::Other,
                                                    code: okx_code,
                                                    msg: okx_msg,
                                                } => {
                                                    const QUERY_RESP_HEADER_LEN: usize = 4 + 8;
                                                    let max_body_len = QUERY_RESP_PAYLOAD
                                                        .saturating_sub(QUERY_RESP_HEADER_LEN);
                                                    warn!(
                                                        "okex order query parse failed: client_query_id={} http_status={} okx_code={} okx_msg={} body_len={} max_body_len={}",
                                                        msg.client_query_id,
                                                        status,
                                                        okx_code,
                                                        okx_msg,
                                                        body.len(),
                                                        max_body_len
                                                    );
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::OkexAccountBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_okex_account_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!("okex account balance snapshot parse produced no basic msgs; skipping response body");
                                            bytes::Bytes::new()
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::OkexPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) = parse_okex_positions_snapshot(&body) {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!("okex positions snapshot parse produced no basic msgs; skipping response body");
                                            bytes::Bytes::new()
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: status as u16,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Gate => {
                            if matches!(
                                msg.req_type,
                                crate::trade_engine::query_request::QueryRequestType::GateUnifiedOrderQuery
                                    | crate::trade_engine::query_request::QueryRequestType::GateFuturesOrderQuery
                            ) {
                                let target_endpoints = if matches!(
                                    msg.req_type,
                                    crate::trade_engine::query_request::QueryRequestType::GateFuturesOrderQuery
                                ) {
                                    gate_futures_ws_endpoints.as_ref()
                                } else {
                                    gate_spot_ws_endpoints.as_ref()
                                };

                                let Some(endpoints) = target_endpoints else {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no gate ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                };
                                if endpoints.is_empty() {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"no gate ws endpoints available",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                    continue;
                                }

                                let (cursor, len) = if matches!(
                                    msg.req_type,
                                    crate::trade_engine::query_request::QueryRequestType::GateFuturesOrderQuery
                                ) {
                                    let len = endpoints.len();
                                    let start = gate_futures_query_rr;
                                    gate_futures_query_rr = (gate_futures_query_rr + 1) % len;
                                    (start, len)
                                } else {
                                    let len = endpoints.len();
                                    let start = gate_query_rr;
                                    gate_query_rr = (gate_query_rr + 1) % len;
                                    (start, len)
                                };

                                let mut sent = false;
                                for offset in 0..len {
                                    let idx = (cursor + offset) % len;
                                    if endpoints[idx]
                                        .send(WsCommand::SendQuery(msg.clone()))
                                        .is_ok()
                                    {
                                        sent = true;
                                        break;
                                    }
                                }

                                if !sent {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 503,
                                        body: bytes::Bytes::from_static(
                                            b"gate ws query dispatch failed",
                                        ),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                continue;
                            }

                            if !QueryTypeMapping::is_gate_rest(msg.req_type) {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for gate engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &gate_creds else {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing Gate credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");

                            match crate::trade_engine::gate_query::gate_rest_get(
                                &gate_http, creds, endpoint, qs,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let body_bytes = match msg.req_type {
                                        crate::trade_engine::query_request::QueryRequestType::GateUnifiedBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_gate_unified_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!("gate unified balance snapshot parse produced no basic msgs; skipping response body");
                                            bytes::Bytes::new()
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::GateUnifiedPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(parsed) =
                                                parse_gate_positions_snapshot_with_meta(&body)
                                            {
                                                if !parsed.msgs.is_empty() {
                                                    for payload in parsed.msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                                let no_positions = parsed.rows_total == 0
                                                    || (parsed.rows_with_inst > 0
                                                        && parsed.rows_with_nonzero_size == 0
                                                        && parsed.rows_with_pnl == 0);
                                                if no_positions {
                                                    info!(
                                                        "gate positions snapshot empty; rows_total={}, rows_with_inst={}, rows_nonzero_size={}, rows_with_pnl={}",
                                                        parsed.rows_total,
                                                        parsed.rows_with_inst,
                                                        parsed.rows_with_nonzero_size,
                                                        parsed.rows_with_pnl
                                                    );
                                                } else {
                                                    warn!(
                                                        "gate positions snapshot parse produced no basic msgs; rows_total={}, rows_with_inst={}, rows_nonzero_size={}, rows_with_pnl={}",
                                                        parsed.rows_total,
                                                        parsed.rows_with_inst,
                                                        parsed.rows_with_nonzero_size,
                                                        parsed.rows_with_pnl
                                                    );
                                                }
                                            } else {
                                                warn!(
                                                    "gate positions snapshot parse failed; body_len={}",
                                                    body.len()
                                                );
                                            }
                                            bytes::Bytes::new()
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: status as u16,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Bybit => {
                            if !QueryTypeMapping::is_bybit_rest(msg.req_type) {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for bybit engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &bybit_creds else {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing Bybit credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");
                            let is_bybit_snapshot = matches!(
                                msg.req_type,
                                crate::trade_engine::query_request::QueryRequestType::BybitAccountBalanceSnapshot
                                    | crate::trade_engine::query_request::QueryRequestType::BybitPositionsSnapshot
                            );
                            if !is_bybit_snapshot {
                                debug!(
                                    "trade_engine bybit query start req_type={:?} client_query_id={} endpoint={} qs={}",
                                    msg.req_type, msg.client_query_id, endpoint, qs
                                );
                            }

                            match crate::trade_engine::bybit_query::bybit_rest_get(
                                &bybit_http,
                                creds,
                                endpoint,
                                qs,
                            )
                            .await
                            {
                                Ok((status, body)) => {
                                    let bybit_summary = summarize_bybit_response(&body);
                                    if !is_bybit_snapshot {
                                        debug!(
                                            "trade_engine bybit query response req_type={:?} client_query_id={} status={} {}",
                                            msg.req_type,
                                            msg.client_query_id,
                                            status,
                                            bybit_summary
                                        );
                                    }
                                    let body_bytes = match msg.req_type {
                                        crate::trade_engine::query_request::QueryRequestType::BybitMarginQuery
                                        | crate::trade_engine::query_request::QueryRequestType::BybitUMQuery
                                            if status == 200 =>
                                        {
                                            match parse_bybit_order_query_json(&body) {
                                                BybitOrderQueryParseResult::Success(v) => v.to_bytes(),
                                                BybitOrderQueryParseResult::Error {
                                                    kind: BybitOrderQueryParseErrorKind::OrderNotFound,
                                                    ..
                                                } => bytes::Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER),
                                                BybitOrderQueryParseResult::Error { .. } => {
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BybitAccountBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_bybit_account_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!(
                                                "trade_engine bybit balance snapshot parse produced no basic msgs req_type={:?} client_query_id={} status={} {} body={}",
                                                msg.req_type,
                                                msg.client_query_id,
                                                status,
                                                bybit_summary,
                                                truncate_for_log(&body, 512)
                                            );
                                            bytes::Bytes::new()
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BybitPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) = parse_bybit_positions_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            debug!(
                                                "trade_engine bybit positions snapshot parse produced no basic msgs req_type={:?} client_query_id={} status={} {} body={}",
                                                msg.req_type,
                                                msg.client_query_id,
                                                status,
                                                bybit_summary,
                                                truncate_for_log(&body, 512)
                                            );
                                            bytes::Bytes::new()
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };

                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: status as u16,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    warn!(
                                        "trade_engine bybit query failed req_type={:?} client_query_id={} endpoint={} qs={} err={:#}",
                                        msg.req_type,
                                        msg.client_query_id,
                                        endpoint,
                                        qs,
                                        e
                                    );
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        Exchange::Bitget => {
                            if !QueryTypeMapping::is_bitget_rest(msg.req_type) {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 400,
                                    body: bytes::Bytes::from_static(
                                        b"unsupported query type for bitget engine",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            }
                            let Some(creds) = &bitget_creds else {
                                let _ = query_resp_tx.send(QueryExecOutcome {
                                    req_type: msg.req_type,
                                    client_query_id: msg.client_query_id,
                                    status: 401,
                                    body: bytes::Bytes::from_static(
                                        b"missing Bitget credentials in env",
                                    ),
                                    exchange: exchange_copy,
                                    ip_used_weight_1m: None,
                                    query_count_1m: None,
                                });
                                continue;
                            };

                            let now = std::time::Instant::now();
                            if let Some(block) =
                                bitget_query_rate_limiter.should_block(msg.req_type, now)
                            {
                                warn!(
                                    "bitget query rate-limited: req_type={:?} client_query_id={} queued_in_window={} max_requests={} window_ms={} wait_ms={}",
                                    msg.req_type,
                                    msg.client_query_id,
                                    block.queued_in_window,
                                    block.max_requests,
                                    block.window.as_millis(),
                                    block.wait_for.as_millis()
                                );
                                tokio::time::sleep(block.wait_for).await;
                            }
                            let _ = bitget_query_rate_limiter
                                .record(msg.req_type, std::time::Instant::now());

                            let endpoint = QueryTypeMapping::get_endpoint(msg.req_type);
                            let qs = std::str::from_utf8(&msg.params).unwrap_or("");
                            match crate::trade_engine::bitget_query::bitget_rest_get(
                                &bitget_http,
                                creds,
                                endpoint,
                                qs,
                            )
                                .await
                            {
                                Ok((status, body)) => {
                                    let body_bytes = match msg.req_type {
                                        crate::trade_engine::query_request::QueryRequestType::BitgetMarginQuery
                                        | crate::trade_engine::query_request::QueryRequestType::BitgetUMQuery
                                            if status == 200 =>
                                        {
                                            match parse_bitget_order_query_json(&body) {
                                                BitgetOrderQueryParseResult::Success(v) => v.to_bytes(),
                                                BitgetOrderQueryParseResult::Error {
                                                    kind: BitgetOrderQueryParseErrorKind::OrderNotFound,
                                                    ..
                                                } => bytes::Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER),
                                                BitgetOrderQueryParseResult::Error { .. } => {
                                                    bytes::Bytes::from_static(b"E")
                                                }
                                            }
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BitgetAccountBalanceSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) =
                                                parse_bitget_account_balance_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                            }
                                            warn!(
                                                "bitget account balance snapshot parse produced no basic msgs; body={}",
                                                truncate_for_log(&body, 512)
                                            );
                                            bytes::Bytes::new()
                                        }
                                        crate::trade_engine::query_request::QueryRequestType::BitgetPositionsSnapshot
                                            if status == 200 =>
                                        {
                                            if let Some(msgs) = parse_bitget_positions_snapshot(&body)
                                            {
                                                if !msgs.is_empty() {
                                                    for payload in msgs {
                                                        let _ = query_resp_tx.send(QueryExecOutcome {
                                                            req_type: msg.req_type,
                                                            client_query_id: msg.client_query_id,
                                                            status: status as u16,
                                                            body: payload,
                                                            exchange: exchange_copy,
                                                            ip_used_weight_1m: None,
                                                            query_count_1m: None,
                                                        });
                                                    }
                                                    continue;
                                                }
                                                bytes::Bytes::new()
                                            } else {
                                                warn!(
                                                    "bitget positions snapshot parse failed; body={}",
                                                    truncate_for_log(&body, 512)
                                                );
                                                bytes::Bytes::new()
                                            }
                                        }
                                        _ => bytes::Bytes::from(body),
                                    };

                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: status as u16,
                                        body: body_bytes,
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                                Err(e) => {
                                    let _ = query_resp_tx.send(QueryExecOutcome {
                                        req_type: msg.req_type,
                                        client_query_id: msg.client_query_id,
                                        status: 0,
                                        body: bytes::Bytes::from(e.to_string()),
                                        exchange: exchange_copy,
                                        ip_used_weight_1m: None,
                                        query_count_1m: None,
                                    });
                                }
                            }
                        }
                        _ => {
                            let _ = query_resp_tx.send(QueryExecOutcome {
                                req_type: msg.req_type,
                                client_query_id: msg.client_query_id,
                                status: 400,
                                body: bytes::Bytes::from_static(b"E"),
                                exchange: exchange_copy,
                                ip_used_weight_1m: None,
                                query_count_1m: None,
                            });
                        }
                    }
                }
            });
            worker_handles.push(("query_router", query_router));
        }

        // Query subscriber loop
        let shutdown_for_query_sub = shutdown.clone();
        let query_req_tx_for_sub = query_req_tx.clone();
        let query_subscriber_worker = tokio::task::spawn_local(async move {
            while !shutdown_for_query_sub.is_cancelled() {
                match query_subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        let Some(actual_len) = request_payload_len(payload) else {
                            warn!(
                                "invalid query request binary payload (min_len=24, buf_len={})",
                                payload.len()
                            );
                            drop(sample);
                            continue;
                        };
                        let owned = bytes::Bytes::copy_from_slice(&payload[..actual_len]);
                        drop(sample);

                        match crate::trade_engine::query_request::QueryRequestMsg::parse(&owned) {
                            Some(msg) => {
                                let _ = query_req_tx_for_sub.send(msg);
                            }
                            None => {
                                warn!("invalid query request binary payload (len={})", actual_len);
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("query request receive error: {err}");
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    }
                }
            }
            info!("query subscriber loop exiting (shutdown)");
        });
        worker_handles.push(("query_subscriber", query_subscriber_worker));

        worker_handles.push((
            "resp_publisher",
            spawn_response_handle(resp_publisher, resp_rx),
        ));
        worker_handles.push((
            "query_resp_publisher",
            spawn_query_response_handle(query_resp_publisher, query_resp_rx),
        ));

        while !shutdown.is_cancelled() {
            match subscriber.receive()? {
                Some(sample) => {
                    let payload = sample.payload();
                    let Some(actual_len) = request_payload_len(payload) else {
                        warn!(
                            "invalid trade request binary payload (min_len=24, buf_len={})",
                            payload.len()
                        );
                        drop(sample);
                        continue;
                    };
                    let owned = bytes::Bytes::copy_from_slice(&payload[..actual_len]);
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
                    // Yield to allow Ctrl+C and other tasks to progress on current_thread runtime.
                    tokio::task::yield_now().await;
                }
                None => {
                    tokio::task::yield_now().await;
                }
            }
        }

        info!("trade_engine shutdown requested; stopping workers");
        self.req_tx = None;
        drop(req_tx);
        drop(query_req_tx);

        // Give ws clients a direct shutdown signal to shorten reconnect/backoff delays.
        if let Some(endpoints) = &ws_endpoints {
            for tx in endpoints {
                let _ = tx.send(WsCommand::Shutdown);
            }
        }
        if let Some(endpoints) = &binance_spot_ws_endpoints {
            for tx in endpoints {
                let _ = tx.send(WsCommand::Shutdown);
            }
        }
        drop(ws_endpoints);

        // Close response channels after request paths are down; workers will exit when all senders drop.
        drop(resp_tx);
        drop(query_resp_tx);

        for (name, handle) in worker_handles {
            join_or_abort(name, handle).await;
        }

        info!("trade_engine shutdown complete");
        Ok(())
    }
}
