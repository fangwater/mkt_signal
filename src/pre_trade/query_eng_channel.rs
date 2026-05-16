use anyhow::{anyhow, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use crate::common::basic_account_msg::{
    get_basic_event_type, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::{QUERY_REQ_PAYLOAD, QUERY_RESP_PAYLOAD};
use crate::common::ipc_service_name::build_service_name;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::response_reconcile::apply_query_response_as_updates;
use crate::pre_trade::PersistChannel;
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_query_parser::parse_compact_order_query_resp;
use crate::strategy::query_engine_response::{QueryEngineResponse, QueryEngineResponseMessage};
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::{OrphanStrategyManager, StrategyManager};
use crate::trade_engine::query_request::QueryRequestType;

thread_local! {
    static QUERY_ENG_HUB: OnceCell<QueryEngHub> = const { OnceCell::new() };
}

const QUERY_ENG_SUBSCRIBER_MAX_BUFFER_SIZE: usize = 256;

fn dispatch_query_response_to_strategy_manager(
    strategy_mgr: &Rc<RefCell<StrategyManager>>,
    strategy_id: i32,
    response: &dyn QueryEngineResponse,
) -> bool {
    let client_order_id = response.client_query_id();
    let strategy_opt = {
        let mut mgr = strategy_mgr.borrow_mut();
        if mgr.contains(strategy_id) {
            mgr.take(strategy_id)
        } else {
            None
        }
    };
    if let Some(mut strategy) = strategy_opt {
        let matched = strategy.is_strategy_order(client_order_id);
        if matched {
            let _ = apply_query_response_as_updates(strategy.as_mut(), response);
        }
        if strategy.is_active() {
            strategy_mgr.borrow_mut().insert(strategy);
        }
        matched
    } else {
        false
    }
}

fn dispatch_query_response_to_orphan_manager(
    orphan_strategy_mgr: &Rc<RefCell<OrphanStrategyManager>>,
    strategy_id: i32,
    response: &dyn QueryEngineResponse,
) -> bool {
    let client_order_id = response.client_query_id();
    let strategy_opt = {
        let mut mgr = orphan_strategy_mgr.borrow_mut();
        if let Some(strategy) = mgr.take_by_order_id(client_order_id) {
            Some(strategy)
        } else if mgr.contains(strategy_id) {
            mgr.take(strategy_id)
        } else {
            None
        }
    };
    if let Some(mut strategy) = strategy_opt {
        let matched = strategy.is_strategy_order(client_order_id);
        if matched {
            let _ = apply_query_response_as_updates(strategy.as_mut(), response);
        }
        if strategy.is_active() {
            orphan_strategy_mgr.borrow_mut().insert(strategy);
        }
        matched
    } else {
        false
    }
}

pub struct QueryEngHub {
    channels: RefCell<HashMap<String, QueryEngChannel>>,
}

impl QueryEngHub {
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&QueryEngHub) -> R,
    {
        QUERY_ENG_HUB.with(|cell| {
            let hub = cell.get_or_init(|| {
                info!("Initializing QueryEngHub singleton with default exchange (binance)");
                let hub = QueryEngHub::new();
                hub.ensure_exchange("binance")
                    .expect("Failed to initialize default QueryEngHub");
                hub
            });
            f(hub)
        })
    }

    pub fn initialize<S>(exchanges: S) -> Result<()>
    where
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        QUERY_ENG_HUB.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow!("QueryEngHub already initialized"));
            }
            let hub = QueryEngHub::new();
            for exchange in exchanges {
                hub.ensure_exchange(exchange.as_ref())?;
            }
            cell.set(hub)
                .map_err(|_| anyhow!("Failed to set QueryEngHub (race condition)"))
        })
    }

    pub fn ensure_registered(exchange: &str) -> Result<()> {
        Self::with(|hub| hub.ensure_exchange(exchange))
    }

    pub fn publish_query_request(exchange: &str, bytes: &Bytes) -> Result<()> {
        Self::with(|hub| hub.publish_to_exchange(exchange, bytes))
    }

    /// 发布 query 请求并同步刷新对应订单的最近一次发送时间戳（submit_t）。
    ///
    /// 调用方在 strategy 层有 `client_order_id` 时统一走该 helper：先把 OrderManager
    /// 中对应订单的 `submit_t` 覆写为当前本地时间（µs），随后通过 iceoryx 发布请求。
    pub fn publish_query_request_for(
        client_order_id: i64,
        exchange: &str,
        bytes: &Bytes,
    ) -> Result<()> {
        let now = get_timestamp_us();
        if let Some(om) = MonitorChannel::try_order_manager() {
            om.borrow_mut().update(client_order_id, |order| {
                order.set_submit_time(now);
            });
        }
        Self::publish_query_request(exchange, bytes)
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
            return Err(anyhow!("QueryEngHub: exchange '{}' not registered", key));
        };
        channel.publish_query_request(bytes)
    }

    fn ensure_exchange(&self, exchange: &str) -> Result<()> {
        let key = Self::normalize_exchange(exchange);
        if self.channels.borrow().contains_key(&key) {
            return Ok(());
        }

        info!(
            "QueryEngHub: registering query engine channel for exchange '{}'",
            key
        );
        let channel = QueryEngChannel::new(&key)?;
        self.channels.borrow_mut().insert(key, channel);
        Ok(())
    }

    fn normalize_exchange(exchange: &str) -> String {
        exchange.trim().to_ascii_lowercase()
    }
}

struct QueryEngChannel {
    query_req_publisher: Publisher<ipc::Service, [u8; QUERY_REQ_PAYLOAD], ()>,
}

impl QueryEngChannel {
    fn new(exchange: &str) -> Result<Self> {
        let query_req_service = build_service_name(&format!("query_reqs/{}", exchange));
        let query_resp_service = build_service_name(&format!("query_resps/{}", exchange));

        let req_node = NodeBuilder::new()
            .name(&NodeName::new(&format!(
                "pre_trade_query_req_{}",
                exchange
            ))?)
            .create::<ipc::Service>()?;

        let req_service = req_node
            .service_builder(&ServiceName::new(&query_req_service)?)
            .publish_subscribe::<[u8; QUERY_REQ_PAYLOAD]>()
            .subscriber_max_buffer_size(QUERY_ENG_SUBSCRIBER_MAX_BUFFER_SIZE)
            .open_or_create()?;

        let query_req_publisher = req_service.publisher_builder().create()?;
        info!(
            "QueryEngHub: query request publisher created on '{}' (exchange={})",
            query_req_service, exchange
        );

        let resp_service_name = query_resp_service.clone();
        let exchange_name = exchange.to_string();
        tokio::task::spawn_local(async move {
            if let Err(err) =
                Self::run_query_resp_listener(&exchange_name, &resp_service_name).await
            {
                warn!(
                    "Query response listener exited (exchange={} service={}): {err:?}",
                    exchange_name, resp_service_name
                );
            }
        });

        Ok(Self {
            query_req_publisher,
        })
    }

    fn publish_query_request(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        if bytes.len() > QUERY_REQ_PAYLOAD {
            warn!(
                "Query request truncated: len={} capacity={}",
                bytes.len(),
                QUERY_REQ_PAYLOAD
            );
        }

        let mut buf = [0u8; QUERY_REQ_PAYLOAD];
        let copy_len = bytes.len().min(QUERY_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);

        let sample = self.query_req_publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        Ok(())
    }

    async fn run_query_resp_listener(exchange: &str, service_name: &str) -> Result<()> {
        let exchange_enum = Exchange::from_str(exchange)
            .ok_or_else(|| anyhow!("QueryEngHub: unsupported exchange '{}'", exchange))?;
        let node = NodeBuilder::new()
            .name(&NodeName::new(&format!(
                "pre_trade_query_resp_{}",
                exchange
            ))?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; QUERY_RESP_PAYLOAD]>()
            .subscriber_max_buffer_size(QUERY_ENG_SUBSCRIBER_MAX_BUFFER_SIZE)
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!(
            "QueryEngHub: query response subscribed on '{}' (exchange={})",
            service_name, exchange
        );

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = sample.payload();
                    match QueryEngineResponseMessage::from_payload(payload) {
                        Ok(resp) => {
                            let req_type = QueryRequestType::try_from(resp.req_type()).ok();

                            // Snapshot queries return basic account messages (no huge JSON body).
                            if matches!(
                                resp.req_type(),
                                6101 | 6102
                                    | 6103
                                    | 6104
                                    | 6105
                                    | 7101
                                    | 7102
                                    | 8101
                                    | 8102
                                    | 9101
                                    | 9102
                                    | 9203
                                    | 9204
                            ) {
                                let body = resp.body_bytes().as_ref();
                                let body_is_empty = body.iter().all(|b| *b == 0);
                                let event_type = get_basic_event_type(body);

                                // Apply into MonitorChannel managers (same semantics as account_pubs basic stream).
                                let mc = MonitorChannel::instance();
                                let open_venue = mc.open_venue();
                                let hedge_venue = mc.hedge_venue();
                                let open_exchange =
                                    Exchange::from_str(open_venue.trade_engine_exchange())
                                        .unwrap_or(exchange_enum);
                                let hedge_exchange =
                                    Exchange::from_str(hedge_venue.trade_engine_exchange())
                                        .unwrap_or(exchange_enum);
                                let binance_is_standard =
                                    mc.order_manager().borrow().binance_is_standard();
                                if matches!(
                                    req_type,
                                    Some(
                                        QueryRequestType::BinanceUmAccountSnapshot
                                            | QueryRequestType::BinanceUmAccountSnapshotStd
                                            | QueryRequestType::OkexPositionsSnapshot
                                            | QueryRequestType::GateUnifiedPositionsSnapshot
                                            | QueryRequestType::BybitPositionsSnapshot
                                            | QueryRequestType::BitgetPositionsSnapshot
                                    )
                                ) && body_is_empty
                                {
                                    let mut cleared = false;
                                    if open_venue.is_futures() && exchange_enum == open_exchange {
                                        if let Some((um, _)) = mc.open_um_mgr() {
                                            um.borrow_mut().clear();
                                            cleared = true;
                                            mc.mark_arb_startup_net_seen_for_venue(
                                                open_venue,
                                                "query_positions_empty",
                                            );
                                        }
                                    }
                                    if hedge_venue.is_futures() && exchange_enum == hedge_exchange {
                                        if let Some((um, _)) = mc.hedge_um_mgr() {
                                            um.borrow_mut().clear();
                                            cleared = true;
                                            mc.mark_arb_startup_net_seen_for_venue(
                                                hedge_venue,
                                                "query_positions_empty",
                                            );
                                        }
                                    }
                                    if cleared {
                                        info!(
                                            "positions snapshot returned empty list; cleared pre_trade UM state exchange={} req_type={:?}",
                                            exchange, req_type
                                        );
                                    }
                                    continue;
                                }
                                let account_scope = match req_type {
                                    Some(QueryRequestType::BinanceWsMarginQuery) => {
                                        BasicAccountScope::BinanceStdSpot
                                    }
                                    Some(QueryRequestType::BinanceWsUMQuery) => {
                                        BasicAccountScope::BinanceStdUm
                                    }
                                    Some(QueryRequestType::BinanceUmBalanceSnapshotStd)
                                    | Some(QueryRequestType::BinanceUmAccountSnapshotStd) => {
                                        BasicAccountScope::BinanceStdUm
                                    }
                                    Some(QueryRequestType::BinanceSpotAccountSnapshotStd) => {
                                        BasicAccountScope::BinanceStdSpot
                                    }
                                    Some(QueryRequestType::BinanceMarginQuery)
                                    | Some(QueryRequestType::BinanceUMQuery) => {
                                        BasicAccountScope::BinanceUnified
                                    }
                                    Some(QueryRequestType::OkexMarginQuery)
                                    | Some(QueryRequestType::OkexUMQuery) => {
                                        BasicAccountScope::OkexUnified
                                    }
                                    Some(QueryRequestType::BybitMarginQuery)
                                    | Some(QueryRequestType::BybitUMQuery) => {
                                        BasicAccountScope::BybitUnified
                                    }
                                    Some(QueryRequestType::GateUnifiedOrderQuery)
                                    | Some(QueryRequestType::GateFuturesOrderQuery) => {
                                        BasicAccountScope::GateUnified
                                    }
                                    _ => match exchange_enum {
                                        Exchange::Binance => {
                                            if binance_is_standard {
                                                BasicAccountScope::Unknown
                                            } else {
                                                BasicAccountScope::BinanceUnified
                                            }
                                        }
                                        Exchange::Okex => BasicAccountScope::OkexUnified,
                                        Exchange::Gate => BasicAccountScope::GateUnified,
                                        Exchange::Bitget => BasicAccountScope::BitgetUnified,
                                        Exchange::Bybit => BasicAccountScope::BybitUnified,
                                        _ => BasicAccountScope::Unknown,
                                    },
                                };
                                let scope_matches_venue =
                                    |scope: BasicAccountScope, venue: TradingVenue| match venue {
                                        TradingVenue::BinanceMargin => {
                                            if binance_is_standard {
                                                scope == BasicAccountScope::BinanceStdSpot
                                            } else {
                                                scope == BasicAccountScope::BinanceUnified
                                            }
                                        }
                                        TradingVenue::BinanceFutures => {
                                            if binance_is_standard {
                                                scope == BasicAccountScope::BinanceStdUm
                                            } else {
                                                scope == BasicAccountScope::BinanceUnified
                                            }
                                        }
                                        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                                            scope == BasicAccountScope::OkexUnified
                                        }
                                        TradingVenue::GateMargin | TradingVenue::GateFutures => {
                                            scope == BasicAccountScope::GateUnified
                                        }
                                        TradingVenue::BitgetMargin
                                        | TradingVenue::BitgetFutures => {
                                            scope == BasicAccountScope::BitgetUnified
                                        }
                                        TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                                            scope == BasicAccountScope::BybitUnified
                                        }
                                        _ => false,
                                    };

                                match event_type {
                                    BasicAccountEventType::BalanceUpdate => {
                                        if let Ok(m) = BasicBalanceMsg::from_bytes(body) {
                                            let mut applied = false;
                                            if m.symbol.eq_ignore_ascii_case("USDT") {
                                                if let Some(usdt) = mc.usdt_mgr(account_scope) {
                                                    usdt.borrow_mut().apply_balance(&m);
                                                    applied = true;
                                                }
                                            }
                                            if matches!(
                                                open_venue,
                                                TradingVenue::BinanceMargin
                                                    | TradingVenue::OkexMargin
                                                    | TradingVenue::GateMargin
                                                    | TradingVenue::BitgetMargin
                                                    | TradingVenue::BybitMargin
                                            ) && exchange_enum == open_exchange
                                                && scope_matches_venue(account_scope, open_venue)
                                            {
                                                if let Some(bal) = mc.open_balance_mgr() {
                                                    bal.borrow_mut().apply_balance(&m);
                                                    applied = true;
                                                    mc.mark_arb_startup_net_seen_for_venue(
                                                        open_venue,
                                                        "query_balance",
                                                    );
                                                }
                                            }
                                            if matches!(
                                                hedge_venue,
                                                TradingVenue::BinanceMargin
                                                    | TradingVenue::OkexMargin
                                                    | TradingVenue::GateMargin
                                                    | TradingVenue::BitgetMargin
                                                    | TradingVenue::BybitMargin
                                            ) && exchange_enum == hedge_exchange
                                                && scope_matches_venue(account_scope, hedge_venue)
                                            {
                                                if let Some(bal) = mc.hedge_balance_mgr() {
                                                    bal.borrow_mut().apply_balance(&m);
                                                    applied = true;
                                                    mc.mark_arb_startup_net_seen_for_venue(
                                                        hedge_venue,
                                                        "query_balance",
                                                    );
                                                }
                                            }
                                            let _ = applied;
                                        }
                                    }
                                    BasicAccountEventType::BorrowInterest => {
                                        if let Ok(m) = BasicBorrowInterestMsg::from_bytes(body) {
                                            let mut applied = false;
                                            if m.symbol.eq_ignore_ascii_case("USDT") {
                                                if let Some(usdt) = mc.usdt_mgr(account_scope) {
                                                    usdt.borrow_mut().apply_borrow_interest(&m);
                                                    applied = true;
                                                }
                                            }
                                            if matches!(
                                                open_venue,
                                                TradingVenue::BinanceMargin
                                                    | TradingVenue::OkexMargin
                                                    | TradingVenue::GateMargin
                                                    | TradingVenue::BitgetMargin
                                                    | TradingVenue::BybitMargin
                                            ) && exchange_enum == open_exchange
                                                && scope_matches_venue(account_scope, open_venue)
                                            {
                                                if let Some(bal) = mc.open_balance_mgr() {
                                                    bal.borrow_mut().apply_borrow_interest(&m);
                                                    applied = true;
                                                    mc.mark_arb_startup_net_seen_for_venue(
                                                        open_venue,
                                                        "query_borrow_interest",
                                                    );
                                                }
                                            }
                                            if matches!(
                                                hedge_venue,
                                                TradingVenue::BinanceMargin
                                                    | TradingVenue::OkexMargin
                                                    | TradingVenue::GateMargin
                                                    | TradingVenue::BitgetMargin
                                                    | TradingVenue::BybitMargin
                                            ) && exchange_enum == hedge_exchange
                                                && scope_matches_venue(account_scope, hedge_venue)
                                            {
                                                if let Some(bal) = mc.hedge_balance_mgr() {
                                                    bal.borrow_mut().apply_borrow_interest(&m);
                                                    applied = true;
                                                    mc.mark_arb_startup_net_seen_for_venue(
                                                        hedge_venue,
                                                        "query_borrow_interest",
                                                    );
                                                }
                                            }
                                            let _ = applied;
                                        }
                                    }
                                    BasicAccountEventType::PositionUpdate => {
                                        if let Ok(m) = BasicPositionMsg::from_bytes(body) {
                                            let mut applied = false;
                                            if matches!(
                                                open_venue,
                                                TradingVenue::BinanceFutures
                                                    | TradingVenue::OkexFutures
                                                    | TradingVenue::GateFutures
                                                    | TradingVenue::BitgetFutures
                                                    | TradingVenue::BybitFutures
                                            ) && exchange_enum == open_exchange
                                                && scope_matches_venue(account_scope, open_venue)
                                            {
                                                if let Some((um, _)) = mc.open_um_mgr() {
                                                    um.borrow_mut().apply_position(&m);
                                                    applied = true;
                                                    mc.mark_arb_startup_net_seen_for_venue(
                                                        open_venue,
                                                        "query_position",
                                                    );
                                                }
                                            }
                                            if matches!(
                                                hedge_venue,
                                                TradingVenue::BinanceFutures
                                                    | TradingVenue::OkexFutures
                                                    | TradingVenue::GateFutures
                                                    | TradingVenue::BitgetFutures
                                                    | TradingVenue::BybitFutures
                                            ) && exchange_enum == hedge_exchange
                                                && scope_matches_venue(account_scope, hedge_venue)
                                            {
                                                if let Some((um, _)) = mc.hedge_um_mgr() {
                                                    um.borrow_mut().apply_position(&m);
                                                    applied = true;
                                                    mc.mark_arb_startup_net_seen_for_venue(
                                                        hedge_venue,
                                                        "query_position",
                                                    );
                                                }
                                            }
                                            let _ = applied;
                                        }
                                    }
                                    BasicAccountEventType::UnrealizedPnlUpdate => {
                                        if let Ok(m) = BasicUmUnrealizedMsg::from_bytes(body) {
                                            let mut applied = false;
                                            if matches!(
                                                open_venue,
                                                TradingVenue::BinanceFutures
                                                    | TradingVenue::OkexFutures
                                                    | TradingVenue::GateFutures
                                                    | TradingVenue::BitgetFutures
                                                    | TradingVenue::BybitFutures
                                            ) && exchange_enum == open_exchange
                                                && scope_matches_venue(account_scope, open_venue)
                                            {
                                                if let Some((um, _)) = mc.open_um_mgr() {
                                                    um.borrow_mut().apply_unrealized_pnl(&m);
                                                    applied = true;
                                                }
                                            }
                                            if matches!(
                                                hedge_venue,
                                                TradingVenue::BinanceFutures
                                                    | TradingVenue::OkexFutures
                                                    | TradingVenue::GateFutures
                                                    | TradingVenue::BitgetFutures
                                                    | TradingVenue::BybitFutures
                                            ) && exchange_enum == hedge_exchange
                                                && scope_matches_venue(account_scope, hedge_venue)
                                            {
                                                if let Some((um, _)) = mc.hedge_um_mgr() {
                                                    um.borrow_mut().apply_unrealized_pnl(&m);
                                                    applied = true;
                                                }
                                            }
                                            let _ = applied;
                                        }
                                    }
                                    BasicAccountEventType::AccountRisk => {
                                        match BasicAccountRiskMsg::from_bytes(body) {
                                            Ok(msg) => MonitorChannel::instance()
                                                .apply_account_risk(account_scope, msg),
                                            Err(err) => warn!(
                                                "AccountRisk decode failed via query_eng_channel: scope={} err={err:#}",
                                                account_scope.as_str()
                                            ),
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            if let Some(req_type) = req_type {
                                if matches!(
                                    req_type,
                                    QueryRequestType::BinanceMarginQuery
                                        | QueryRequestType::BinanceUMQuery
                                        | QueryRequestType::BinanceWsUMQuery
                                        | QueryRequestType::BinanceWsMarginQuery
                                        | QueryRequestType::OkexMarginQuery
                                        | QueryRequestType::OkexUMQuery
                                        | QueryRequestType::BybitMarginQuery
                                        | QueryRequestType::BybitUMQuery
                                        | QueryRequestType::BitgetMarginQuery
                                        | QueryRequestType::BitgetUMQuery
                                        | QueryRequestType::GateUnifiedOrderQuery
                                        | QueryRequestType::GateFuturesOrderQuery
                                ) {
                                    let client_order_id = resp.client_query_id();
                                    let strategy_id = (client_order_id >> 32) as i32;
                                    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                                    let orphan_strategy_mgr =
                                        MonitorChannel::instance().orphan_strategy_mgr();
                                    let matched = dispatch_query_response_to_strategy_manager(
                                        &strategy_mgr,
                                        strategy_id,
                                        &resp,
                                    );
                                    let matched = if matched {
                                        true
                                    } else {
                                        dispatch_query_response_to_orphan_manager(
                                            &orphan_strategy_mgr,
                                            strategy_id,
                                            &resp,
                                        )
                                    };
                                    if !matched {
                                        persist_unmatched_query_response(strategy_id, &resp);
                                    }
                                }
                            }
                            debug!(
                                "queryResponse: exchange={} type={} cli_qid={} body_len={}",
                                exchange,
                                resp.req_type(),
                                resp.client_query_id(),
                                resp.body_bytes().len()
                            );
                        }
                        Err(err) => {
                            warn!(
                                "failed to decode query response (exchange={}): {err:#}",
                                exchange
                            )
                        }
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("Query response receive error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        dispatch_query_response_to_orphan_manager, dispatch_query_response_to_strategy_manager,
    };
    use crate::signal::trade_signal::TradeSignal;
    use crate::strategy::query_engine_response::QueryEngineResponseMessage;
    use crate::strategy::{order_update::OrderUpdate, trade_update::TradeUpdate};
    use crate::strategy::{OrphanStrategyManager, Strategy, StrategyManager};
    use bytes::Bytes;
    use std::any::Any;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct ReentrantQueryStrategy {
        id: i32,
        active: bool,
        order_id: i64,
    }

    impl Strategy for ReentrantQueryStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, order_id: i64) -> bool {
            order_id == self.order_id
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {}

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some("BTCUSDT")
        }
    }

    struct ReentrantOrphanQueryStrategy {
        id: i32,
        active: bool,
        order_id: i64,
    }

    impl Strategy for ReentrantOrphanQueryStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, order_id: i64) -> bool {
            order_id == self.order_id
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {}

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some("BTCUSDT")
        }
    }

    #[test]
    fn query_dispatch_releases_strategy_manager_borrow_before_callback() {
        let manager = Rc::new(RefCell::new(StrategyManager::new()));
        manager
            .borrow_mut()
            .insert(Box::new(ReentrantQueryStrategy {
                id: 301,
                active: true,
                order_id: 301_i64 << 32,
            }));
        let response = QueryEngineResponseMessage::new(0, 301_i64 << 32, Bytes::new());

        let matched = dispatch_query_response_to_strategy_manager(&manager, 301, &response);

        assert!(matched);
        assert!(manager.borrow().contains(301));
    }

    #[test]
    fn query_dispatch_releases_orphan_manager_borrow_before_callback() {
        let manager = Rc::new(RefCell::new(OrphanStrategyManager::new()));
        manager
            .borrow_mut()
            .insert(Box::new(ReentrantOrphanQueryStrategy {
                id: 302,
                active: true,
                order_id: 302_i64 << 32,
            }));
        let response = QueryEngineResponseMessage::new(0, 302_i64 << 32, Bytes::new());

        let matched = dispatch_query_response_to_orphan_manager(&manager, 302, &response);

        assert!(matched);
        assert!(manager.borrow().contains(302));
    }

    #[test]
    fn query_dispatch_does_not_steal_unowned_order_from_orphan_manager() {
        let source_manager = Rc::new(RefCell::new(StrategyManager::new()));
        source_manager
            .borrow_mut()
            .insert(Box::new(ReentrantQueryStrategy {
                id: 301,
                active: true,
                order_id: (301_i64 << 32) | 1,
            }));

        let orphan_manager = Rc::new(RefCell::new(OrphanStrategyManager::new()));
        orphan_manager
            .borrow_mut()
            .insert(Box::new(ReentrantOrphanQueryStrategy {
                id: 401,
                active: true,
                order_id: (301_i64 << 32) | 2,
            }));

        let response = QueryEngineResponseMessage::new(0, (301_i64 << 32) | 2, Bytes::new());

        let matched_source =
            dispatch_query_response_to_strategy_manager(&source_manager, 301, &response);
        let matched_orphan =
            dispatch_query_response_to_orphan_manager(&orphan_manager, 301, &response);

        assert!(!matched_source);
        assert!(matched_orphan);
        assert!(source_manager.borrow().contains(301));
        assert!(orphan_manager.borrow().contains(401));
    }
}

fn persist_unmatched_query_response(strategy_id: i32, resp: &QueryEngineResponseMessage) {
    let client_order_id = resp.client_query_id();
    let Some(order_mgr) = MonitorChannel::try_order_manager() else {
        return;
    };

    let Some(order) = order_mgr.borrow().get(client_order_id) else {
        debug!(
            "queryResponse unmatched and order missing in order_manager: strategy_id={} cli_qid={}",
            strategy_id, client_order_id
        );
        return;
    };

    let Some(parsed) = parse_compact_order_query_resp(resp.body_bytes()) else {
        debug!(
            "queryResponse unmatched but body not compact-order payload: strategy_id={} cli_qid={} req_type={}",
            strategy_id,
            client_order_id,
            resp.req_type()
        );
        return;
    };

    let event_time_us = parsed.update_time_ms.saturating_mul(1_000);
    let order_id = if parsed.order_id > 0 {
        parsed.order_id
    } else {
        order.exchange_order_id.unwrap_or(client_order_id)
    };
    let tif = TimeInForce::from_u8(parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);

    PersistChannel::with(|ch| {
        if parsed.executed_qty > order.cumulative_filled_quantity + 1e-12 {
            let order_status = if parsed.status_u8 == OrderExecutionStatus::Filled.to_u8() {
                Some(OrderStatus::Filled)
            } else {
                Some(OrderStatus::PartiallyFilled)
            };
            let trade = OrderQueryTradeUpdate::new(
                &order,
                order_id,
                event_time_us,
                parsed.executed_qty,
                Some(parsed.response_price),
                order_status,
                tif,
            );
            ch.publish_trade_update_unmatched(&trade);
        }

        let status_u8 = parsed.status_u8;
        if status_u8 == OrderExecutionStatus::Create.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                &order,
                order_id,
                event_time_us,
                OrderStatus::New,
                ExecutionType::New,
                parsed.executed_qty,
                tif,
            );
            ch.publish_order_update_unmatched(&update);
        } else if status_u8 == OrderExecutionStatus::Cancelled.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                &order,
                order_id,
                event_time_us,
                OrderStatus::Canceled,
                ExecutionType::Canceled,
                parsed.executed_qty,
                tif,
            );
            ch.publish_order_update_unmatched(&update);
        } else if status_u8 == OrderExecutionStatus::Filled.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                &order,
                order_id,
                event_time_us,
                OrderStatus::Filled,
                ExecutionType::Trade,
                parsed.executed_qty,
                tif,
            );
            ch.publish_order_update_unmatched(&update);
        } else if status_u8 == OrderExecutionStatus::Rejected.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                &order,
                order_id,
                event_time_us,
                OrderStatus::Expired,
                ExecutionType::Rejected,
                parsed.executed_qty,
                tif,
            );
            ch.publish_order_update_unmatched(&update);
        }
    });
}
