use anyhow::{anyhow, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::time::Duration;

use crate::common::ipc_service_name::build_service_name;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderType;
use crate::pre_trade::PersistChannel;
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::trade_engine_response::{
    TradeEngineResponse, TradeEngineResponseMessage, TradeRequestKind,
};
use crate::trade_engine::trade_request::TradeRequestType;

thread_local! {
    static TRADE_ENG_HUB: OnceCell<TradeEngHub> = OnceCell::new();
}

const TRADE_REQ_PAYLOAD: usize = 4_096;
const TRADE_RESP_PAYLOAD: usize = 64;
const TRADE_RESP_HEADER_LEN: usize = 22;
const TRADE_RESP_TAIL_LEN: usize = 33;
const TRADE_ENG_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 256;

/// TradeEngHub 负责与多个 trade engine 进程进行双向通信
///
/// * 采用线程本地单例，通过 [`TradeEngHub::with`] 访问
/// * 每个交易所对应独立的 `TradeEngChannel`（Iceoryx publisher + subscriber）
/// * 可以在启动时显式注册多个交易所，也可以按需懒加载
pub struct TradeEngHub {
    channels: RefCell<HashMap<String, TradeEngChannel>>,
}

impl TradeEngHub {
    /// 在当前线程获取 TradeEngHub 单例，并执行闭包
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&TradeEngHub) -> R,
    {
        TRADE_ENG_HUB.with(|cell| {
            let hub = cell.get_or_init(|| {
                info!("Initializing TradeEngHub singleton with default exchange (binance)");
                let hub = TradeEngHub::new();
                hub.ensure_exchange("binance")
                    .expect("Failed to initialize default TradeEngHub");
                hub
            });
            f(hub)
        })
    }

    /// 显式初始化 TradeEngHub（优先在程序启动阶段调用）
    pub fn initialize<S>(exchanges: S) -> Result<()>
    where
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        TRADE_ENG_HUB.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow!("TradeEngHub already initialized"));
            }
            let hub = TradeEngHub::new();
            for exchange in exchanges {
                hub.ensure_exchange(exchange.as_ref())?;
            }
            cell.set(hub)
                .map_err(|_| anyhow!("Failed to set TradeEngHub (race condition)"))
        })
    }

    /// 确保指定交易所已注册（可重复调用）
    pub fn ensure_registered(exchange: &str) -> Result<()> {
        Self::with(|hub| hub.ensure_exchange(exchange))
    }

    /// 发布订单请求到指定交易所
    pub fn publish_order_request(exchange: &str, bytes: &Bytes) -> Result<()> {
        Self::with(|hub| hub.publish_to_exchange(exchange, bytes))
    }

    fn new() -> Self {
        Self {
            channels: RefCell::new(HashMap::new()),
        }
    }

    fn publish_to_exchange(&self, exchange: &str, bytes: &Bytes) -> Result<()> {
        self.ensure_exchange(exchange)?;
        let key = Self::normalize_exchange(exchange);
        let channels = self.channels.borrow();
        let Some(channel) = channels.get(&key) else {
            return Err(anyhow!("TradeEngHub: exchange '{}' not registered", key));
        };
        channel.publish_order_request(bytes)
    }

    fn ensure_exchange(&self, exchange: &str) -> Result<()> {
        let key = Self::normalize_exchange(exchange);
        if self.channels.borrow().contains_key(&key) {
            return Ok(());
        }

        info!(
            "TradeEngHub: registering trade engine channel for exchange '{}'",
            key
        );
        let channel = TradeEngChannel::new(&key)?;
        self.channels.borrow_mut().insert(key, channel);
        Ok(())
    }

    fn normalize_exchange(exchange: &str) -> String {
        exchange.trim().to_ascii_lowercase()
    }
}

struct TradeEngChannel {
    order_req_publisher: Publisher<ipc::Service, [u8; TRADE_REQ_PAYLOAD], ()>,
}

impl TradeEngChannel {
    fn new(exchange: &str) -> Result<Self> {
        let order_req_service = build_service_name(&format!("order_reqs/{}", exchange));
        let order_resp_service = build_service_name(&format!("order_resps/{}", exchange));

        let req_node = NodeBuilder::new()
            .name(&NodeName::new(&format!(
                "pre_trade_order_req_{}",
                sanitize_node_suffix(exchange)
            ))?)
            .create::<ipc::Service>()?;

        let req_service = req_node
            .service_builder(&ServiceName::new(&order_req_service)?)
            .publish_subscribe::<[u8; TRADE_REQ_PAYLOAD]>()
            .subscriber_max_buffer_size(TRADE_ENG_SUBSCRIBER_MAX_BUFFER_SIZE)
            .open_or_create()?;

        let order_req_publisher = req_service.publisher_builder().create()?;
        info!(
            "TradeEngHub: order request publisher created on '{}' (exchange={})",
            order_req_service, exchange
        );

        // 启动该交易所的 trade response 监听任务
        let resp_service_name = order_resp_service.clone();
        let exchange_name = exchange.to_string();
        tokio::task::spawn_local(async move {
            if let Err(err) =
                Self::run_trade_resp_listener(&exchange_name, &resp_service_name).await
            {
                warn!(
                    "Trade response listener exited (exchange={} service={}): {err:?}",
                    exchange_name, resp_service_name
                );
            }
        });

        Ok(Self {
            order_req_publisher,
        })
    }

    fn publish_order_request(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        if bytes.len() > TRADE_REQ_PAYLOAD {
            warn!(
                "Order request truncated: len={} capacity={}",
                bytes.len(),
                TRADE_REQ_PAYLOAD
            );
        }

        let mut buf = [0u8; TRADE_REQ_PAYLOAD];
        let copy_len = bytes.len().min(TRADE_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);

        let sample = self.order_req_publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;

        Ok(())
    }

    async fn run_trade_resp_listener(exchange: &str, service_name: &str) -> Result<()> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(&format!(
                "pre_trade_order_resp_{}",
                sanitize_node_suffix(exchange)
            ))?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
            .subscriber_max_buffer_size(TRADE_ENG_SUBSCRIBER_MAX_BUFFER_SIZE)
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!(
            "TradeEngHub: trade response subscribed on '{}' (exchange={})",
            service_name, exchange
        );

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();

                    if payload.len() < TRADE_RESP_HEADER_LEN {
                        warn!(
                            "Trade response too short: {} bytes (exchange={})",
                            payload.len(),
                            exchange
                        );
                        continue;
                    }

                    let req_type =
                        u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    let client_order_id = i64::from_le_bytes([
                        payload[4],
                        payload[5],
                        payload[6],
                        payload[7],
                        payload[8],
                        payload[9],
                        payload[10],
                        payload[11],
                    ]);
                    let exchange =
                        u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);
                    let status = u16::from_le_bytes([payload[16], payload[17]]);
                    let error_code =
                        i32::from_le_bytes([payload[18], payload[19], payload[20], payload[21]]);
                    let mut order_id = 0i64;
                    let mut order_status_u8 = 0u8;
                    let mut order_update_time = 0i64;
                    let mut executed_qty = 0.0f64;
                    let mut response_price = 0.0f64;
                    if payload.len() >= TRADE_RESP_HEADER_LEN + TRADE_RESP_TAIL_LEN {
                        order_id = i64::from_le_bytes([
                            payload[22],
                            payload[23],
                            payload[24],
                            payload[25],
                            payload[26],
                            payload[27],
                            payload[28],
                            payload[29],
                        ]);
                        order_status_u8 = payload[30];
                        order_update_time = i64::from_le_bytes([
                            payload[31],
                            payload[32],
                            payload[33],
                            payload[34],
                            payload[35],
                            payload[36],
                            payload[37],
                            payload[38],
                        ]);
                        executed_qty = f64::from_le_bytes([
                            payload[39],
                            payload[40],
                            payload[41],
                            payload[42],
                            payload[43],
                            payload[44],
                            payload[45],
                            payload[46],
                        ]);
                        response_price = f64::from_le_bytes([
                            payload[47],
                            payload[48],
                            payload[49],
                            payload[50],
                            payload[51],
                            payload[52],
                            payload[53],
                            payload[54],
                        ]);
                    }
                    let response = TradeEngineResponseMessage::new_with_tail(
                        status,
                        req_type,
                        exchange,
                        client_order_id,
                        error_code,
                        order_id,
                        order_status_u8,
                        order_update_time,
                        executed_qty,
                        response_price,
                    );

                    Self::handle_trade_engine_response(&response);
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("Trade response receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    fn handle_trade_engine_response(response: &TradeEngineResponseMessage) {
        dispatch_trade_engine_response(response);
    }
}

fn sanitize_node_suffix(exchange: &str) -> String {
    let normalized = exchange.trim();
    if normalized.is_empty() {
        return "default".to_string();
    }
    normalized
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn dispatch_trade_engine_response(response: &TradeEngineResponseMessage) {
    let Some(strategy_mgr) = MonitorChannel::try_strategy_mgr() else {
        return;
    };

    let order_id = response.client_order_id();
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    let mut matched = false;

    for strategy_id in strategy_ids {
        let strategy_opt = {
            let mut mgr = strategy_mgr.borrow_mut();
            mgr.take(strategy_id)
        };

        if let Some(mut strategy) = strategy_opt {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                strategy.apply_trade_engine_response(response);
            }
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(strategy);
            }
        }
    }

    if !matched {
        persist_unmatched_trade_engine_response(response);
        let expected_strategy_id = (order_id >> 32) as i32;
        debug!(
            "tradeEngineResponse unmatched: cli_ord_id={} status={} expect_strategy={}",
            order_id,
            response.status(),
            expected_strategy_id
        );
    }
}

fn persist_unmatched_trade_engine_response(response: &TradeEngineResponseMessage) {
    if matches!(response.request_kind(), TradeRequestKind::Other) {
        debug!(
            "skip unmatched persist for non-order tradeEngineResponse: req_type={} cli_ord_id={}",
            response.req_type(),
            response.client_order_id()
        );
        return;
    }

    let Ok(req_type) = TradeRequestType::try_from(response.req_type()) else {
        debug!(
            "skip unmatched persist for unknown req_type: req_type={} cli_ord_id={}",
            response.req_type(),
            response.client_order_id()
        );
        return;
    };

    if !matches!(
        req_type,
        TradeRequestType::BinanceWsNewUMOrder
            | TradeRequestType::BinanceWsCancelUMOrder
            | TradeRequestType::BinanceWsNewMarginOrder
            | TradeRequestType::BinanceWsCancelMarginOrder
    ) {
        debug!(
            "skip unmatched persist for req_type without stable tail fields: req_type={:?} cli_ord_id={}",
            req_type,
            response.client_order_id()
        );
        return;
    }

    let Some(order_mgr) = MonitorChannel::try_order_manager() else {
        return;
    };

    let client_order_id = response.client_order_id();
    let Some(order) = order_mgr.borrow().get(client_order_id) else {
        debug!(
            "tradeEngineResponse unmatched and order missing in order_manager: cli_ord_id={}",
            client_order_id
        );
        return;
    };

    let Some(status) = response.order_status_u8().and_then(OrderStatus::from_u8) else {
        debug!(
            "skip unmatched persist: response lacks order_status_u8, ex={:?} req_type={} cli_ord_id={}",
            response.exchange_enum(),
            response.req_type(),
            client_order_id
        );
        return;
    };

    let Some(update_time_ms) = response.order_update_time() else {
        debug!(
            "skip unmatched persist: response lacks order_update_time, ex={:?} req_type={} cli_ord_id={}",
            response.exchange_enum(),
            response.req_type(),
            client_order_id
        );
        return;
    };

    let event_time_us = response
        .order_update_time()
        .and_then(|ts_ms| ts_ms.checked_mul(1_000))
        .filter(|ts_us| *ts_us > 0)
        .unwrap_or_else(|| update_time_ms.saturating_mul(1_000).max(get_timestamp_us()));
    let order_id = response
        .order_id()
        .or(order.exchange_order_id)
        .unwrap_or(client_order_id);
    let tif = infer_time_in_force(order.venue, order.order_type);
    let cumulative_exec_qty = response
        .executed_qty()
        .unwrap_or(order.cumulative_filled_quantity);

    let execution_type = execution_type_from_status(status);

    PersistChannel::with(|ch| {
        if execution_type == ExecutionType::Trade {
            if cumulative_exec_qty > order.cumulative_filled_quantity + 1e-12 {
                let order_status = if status == OrderStatus::Filled {
                    Some(OrderStatus::Filled)
                } else {
                    Some(OrderStatus::PartiallyFilled)
                };
                let trade = OrderQueryTradeUpdate::new(
                    &order,
                    order_id,
                    event_time_us,
                    cumulative_exec_qty,
                    response.response_price(),
                    order_status,
                    tif,
                );
                ch.publish_trade_update_unmatched(&trade);
            }
            return;
        }

        let update = OrderQueryOrderUpdate::new(
            &order,
            order_id,
            event_time_us,
            status,
            execution_type,
            cumulative_exec_qty,
            tif,
        );
        ch.publish_order_update_unmatched(&update);
    });
}

fn infer_time_in_force(venue: TradingVenue, order_type: OrderType) -> TimeInForce {
    if !order_type.is_limit() {
        return TimeInForce::GTC;
    }
    match venue {
        TradingVenue::BinanceFutures => TimeInForce::GTX,
        _ => TimeInForce::GTC,
    }
}

fn execution_type_from_status(status: OrderStatus) -> ExecutionType {
    match status {
        OrderStatus::New => ExecutionType::New,
        OrderStatus::PartiallyFilled | OrderStatus::Filled => ExecutionType::Trade,
        OrderStatus::Canceled => ExecutionType::Canceled,
        OrderStatus::Expired | OrderStatus::ExpiredInMatch => ExecutionType::Expired,
    }
}
