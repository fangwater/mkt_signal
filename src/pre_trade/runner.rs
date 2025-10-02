use crate::common::account_msg::{
    get_event_type as get_account_event_type, AccountEventType, AccountPositionMsg,
    AccountUpdateBalanceMsg, AccountUpdatePositionMsg, BalanceUpdateMsg, ExecutionReportMsg,
    OrderTradeUpdateMsg,
};
use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::common::min_qty_table::MinQtyTable;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::binance_pm_spot_manager::{BinancePmSpotAccountManager, BinanceSpotBalance};
use crate::pre_trade::binance_pm_um_manager::{
    BinancePmUmAccountManager, BinanceUmAccountSnapshot, BinanceUmPosition,
};
use crate::pre_trade::config::{
    AccountStreamCfg, PreTradeCfg, SignalSubscriptionsCfg, StrategyParamsCfg, TradeEngineRespCfg,
};
use crate::pre_trade::event::AccountEvent;
use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use crate::signal::binance_forward_arb::{
    BinSingleForwardArbCloseMarginCtx, BinSingleForwardArbCloseUmCtx, BinSingleForwardArbOpenCtx,
    BinSingleForwardArbStrategy, BinSingleForwardArbSnapshot,
};
use crate::signal::strategy::{Strategy, StrategyManager};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, error, info, warn};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use crate::pre_trade::store::{RedisStore, StrategyRecord};

const ACCOUNT_PAYLOAD: usize = 16_384;
const TRADE_RESP_PAYLOAD: usize = 16_384;
const SIGNAL_PAYLOAD: usize = 1_024;
const ORDER_REQ_PAYLOAD: usize = 4_096;

const NODE_PRE_TRADE_ACCOUNT: &str = "pre_trade_account";
const NODE_PRE_TRADE_TRADE_RESP: &str = "pre_trade_trade_resp";
const NODE_PRE_TRADE_SIGNAL_PREFIX: &str = "signals";
const NODE_PRE_TRADE_ORDER_REQ: &str = "pre_trade_order_req";
const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const DERIVATIVES_PAYLOAD: usize = 128;

pub struct PreTrade {
    cfg: PreTradeCfg,
}

impl PreTrade {
    pub fn new(cfg: PreTradeCfg) -> Self {
        Self { cfg }
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade starting");

        let bootstrap = BootstrapResources::load(&self.cfg).await?;

        let order_publisher = OrderPublisher::new(&bootstrap.order_req_service)?;
        let (order_tx, order_rx) = mpsc::unbounded_channel::<Bytes>();
        let (signal_tx, signal_rx) = mpsc::unbounded_channel::<Bytes>();

        // 初始化策略参数从 Redis
        let strategy_params = StrategyParamsCfg::default();

        // 可选打开持久化存储
        let mut store = if let Some(store_cfg) = self.cfg.store.clone() {
            if store_cfg.enable {
                match RedisStore::connect(&store_cfg.redis_url, &store_cfg.prefix).await {
                    Ok(s) => Some(s),
                    Err(err) => {
                        warn!("failed to connect redis store: {err:#}");
                        None
                    }
                }
            } else { None }
        } else { None };

        let mut runtime = RuntimeContext::new(
            bootstrap,
            order_tx.clone(),
            signal_tx.clone(),
            order_publisher,
            strategy_params,
            store.take(),
        );

        // 首次从 Redis 拉取 pre-trade 参数
        if let Err(err) = runtime.reload_params().await {
            warn!("pre_trade initial params load failed: {err:#}");
        }

        // 启动时恢复
        if let Err(err) = runtime.try_recover().await {
            warn!("recovery failed: {err:#}");
        }

        let mut order_rx = order_rx;
        let mut internal_signal_rx = signal_rx;

        let mut account_rx = spawn_account_listener(&self.cfg.account_stream)?;
        let mut trade_resp_rx = spawn_trade_response_listener(&self.cfg.trade_engine)?;
        let mut external_signal_rx = spawn_signal_listeners(&self.cfg.signals)?;

        spawn_derivatives_worker(runtime.price_table.clone())?;

        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    if let Err(err) = runtime.persist_snapshot().await {
                        warn!("persist snapshot failed: {err:#}");
                    } else {
                        info!("snapshot persisted to redis; exiting");
                    }
                    break;
                }
                Some(evt) = account_rx.recv() => {
                    if let Err(err) = handle_account_event(&mut runtime, evt) {
                        warn!("handle account event failed: {err:?}");
                    }
                }
                Some(resp) = trade_resp_rx.recv() => {
                    handle_trade_engine_response(&mut runtime, resp);
                }
                Some(signal) = external_signal_rx.recv() => {
                    handle_trade_signal(&mut runtime, signal);
                }
                Some(signal_bytes) = internal_signal_rx.recv() => {
                    match TradeSignal::from_bytes(&signal_bytes) {
                        Ok(signal) => handle_trade_signal(&mut runtime, signal),
                        Err(err) => warn!("failed to decode internal signal: {err}"),
                    }
                }
                Some(bytes) = order_rx.recv() => {
                    if let Err(err) = runtime.order_publisher.publish(&bytes) {
                        warn!("failed to publish order request: {err}");
                    }
                }
                _ = ticker.tick() => {
                    runtime.tick().await;
                }
                else => break,
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

struct BootstrapResources {
    //币安 合约资产管理器，基于统一账户update更新
    um_manager: BinancePmUmAccountManager,
    //币安 现货资产管理器, 基于统一账户update更新
    spot_manager: BinancePmSpotAccountManager,
    //币安 现货+合约敞口管理器
    exposure_manager: ExposureManager,
    //币安 标记价格表，辅助计算以usdt计价的资产敞口
    price_table: Rc<RefCell<PriceTable>>,
    // 交易对最小下单量/步进信息（spot/futures/margin）
    min_qty_table: Rc<MinQtyTable>,
    //收取订单请求的服务名称
    order_req_service: String,
}

impl BootstrapResources {
    async fn load(cfg: &PreTradeCfg) -> Result<Self> {
        let um_cfg = cfg
            .risk_checks
            .binance_pm_um
            .as_ref()
            .ok_or_else(|| anyhow!("risk_checks.binance_pm_um must be configured"))?;

        let um_api_key = std::env::var(&um_cfg.api_key_env)
            .map_err(|_| anyhow!("environment variable {} not set", um_cfg.api_key_env))?;
        let um_api_secret = std::env::var(&um_cfg.api_secret_env)
            .map_err(|_| anyhow!("environment variable {} not set", um_cfg.api_secret_env))?;

        let um_manager = BinancePmUmAccountManager::new(
            &um_cfg.rest_base,
            um_api_key.clone(),
            um_api_secret.clone(),
            um_cfg.recv_window_ms,
        );
        let um_snapshot = um_manager
            .init()
            .await
            .context("failed to load initial Binance UM snapshot")?;
        log_um_positions(&um_snapshot.positions);

        let spot_cfg = cfg
            .risk_checks
            .binance_spot
            .as_ref()
            .ok_or_else(|| anyhow!("risk_checks.binance_spot must be configured"))?;

        let spot_api_key = if spot_cfg.api_key_env == um_cfg.api_key_env {
            um_api_key.clone()
        } else {
            std::env::var(&spot_cfg.api_key_env)
                .map_err(|_| anyhow!("environment variable {} not set", spot_cfg.api_key_env))?
        };
        let spot_api_secret = if spot_cfg.api_secret_env == um_cfg.api_secret_env {
            um_api_secret.clone()
        } else {
            std::env::var(&spot_cfg.api_secret_env)
                .map_err(|_| anyhow!("environment variable {} not set", spot_cfg.api_secret_env))?
        };

        let asset_filter = spot_cfg
            .asset
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string());

        let spot_manager = BinancePmSpotAccountManager::new(
            &spot_cfg.rest_base,
            spot_api_key,
            spot_api_secret,
            spot_cfg.recv_window_ms,
            asset_filter,
        );
        let spot_snapshot = spot_manager
            .init()
            .await
            .context("failed to load initial Binance spot snapshot")?;
        log_spot_balances(&spot_snapshot.balances);

        let mut price_symbols: BTreeSet<String> = BTreeSet::new();
        collect_price_symbols(&mut price_symbols, &um_snapshot, &spot_snapshot);

        let price_table = Rc::new(RefCell::new(PriceTable::new()));
        {
            let mut table = price_table.borrow_mut();
            table
                .init(&price_symbols)
                .await
                .context("failed to load initial price table")?;
            log_price_table(&table.snapshot());
        }

        let exposure_manager = ExposureManager::new(&um_snapshot, &spot_snapshot);
        {
            let table = price_table.borrow();
            log_exposures(exposure_manager.exposures(), &table.snapshot());
        }

        let order_req_service = resolve_order_req_service(&cfg.trade_engine);

        // 加载交易对 LOT_SIZE/PRICE_FILTER（spot/futures/margin），用于数量/价格对齐
        let mut min_qty_table = MinQtyTable::new();
        if let Err(err) = min_qty_table.refresh_binance().await {
            warn!("failed to refresh Binance exchange filters: {err:#}");
        }
        let min_qty_table = Rc::new(min_qty_table);

        Ok(Self {
            um_manager,
            spot_manager,
            exposure_manager,
            price_table,
            min_qty_table,
            order_req_service,
        })
    }
}

struct RuntimeContext {
    spot_manager: BinancePmSpotAccountManager,
    um_manager: BinancePmUmAccountManager,
    exposure_manager: Rc<RefCell<ExposureManager>>,
    price_table: Rc<RefCell<PriceTable>>,
    order_manager: Rc<RefCell<crate::pre_trade::order_manager::OrderManager>>,
    strategy_mgr: StrategyManager,
    symbol_to_strategy: HashMap<String, i32>,
    strategy_symbols: HashMap<i32, String>,
    order_tx: UnboundedSender<Bytes>,
    signal_tx: UnboundedSender<Bytes>,
    order_publisher: OrderPublisher,
    strategy_params: StrategyParamsCfg,
    min_qty_table: Rc<MinQtyTable>,
    dedup: crate::pre_trade::dedup::DedupCache,
    store: Option<RedisStore>,
    next_params_refresh: std::time::Instant,
    params_refresh_secs: u64,
    last_params_snapshot: Option<PreTradeParamsSnap>,
}

impl RuntimeContext {
    fn new(
        bootstrap: BootstrapResources,
        order_tx: UnboundedSender<Bytes>,
        signal_tx: UnboundedSender<Bytes>,
        order_publisher: OrderPublisher,
        strategy_params: StrategyParamsCfg,
        store: Option<RedisStore>,
    ) -> Self {
        let BootstrapResources {
            um_manager,
            spot_manager,
            exposure_manager,
            price_table,
            min_qty_table,
            order_req_service: _,
        } = bootstrap;

        Self {
            spot_manager,
            um_manager,
            exposure_manager: Rc::new(RefCell::new(exposure_manager)),
            price_table,
            order_manager: Rc::new(RefCell::new(
                crate::pre_trade::order_manager::OrderManager::new(),
            )),
            strategy_mgr: StrategyManager::new(),
            symbol_to_strategy: HashMap::new(),
            strategy_symbols: HashMap::new(),
            order_tx,
            signal_tx,
            order_publisher,
            strategy_params,
            min_qty_table,
            dedup: crate::pre_trade::dedup::DedupCache::new(8192),
            store,
            next_params_refresh: std::time::Instant::now(),
            params_refresh_secs: 30,
            last_params_snapshot: None,
        }
    }

    async fn persist_snapshot(&mut self) -> Result<()> {
        let Some(store) = self.store.as_mut() else {
            return Ok(());
        };
        // 采集订单
        let order_ids = self.order_manager.borrow().get_all_ids();
        let mut orders = Vec::with_capacity(order_ids.len());
        for oid in order_ids {
            if let Some(o) = self.order_manager.borrow().get(oid) { orders.push(o); }
        }
        // 采集策略快照
        let mut strategies: Vec<StrategyRecord> = Vec::new();
        for id in self.strategy_mgr.iter_ids().cloned().collect::<Vec<_>>() {
            if let Some(st) = self.strategy_mgr.get(id) {
                if let Some(snap) = st.snapshot() {
                    strategies.push(StrategyRecord { id, type_name: snap.type_name.to_string(), payload: snap.payload });
                }
            }
        }
        if log::log_enabled!(log::Level::Debug) {
            use log::debug;
            debug!(
                "persist_snapshot: orders={} strategies={}",
                orders.len(),
                strategies.len()
            );
        }
        store.save_snapshot(&orders, &strategies).await?;
        Ok(())
    }

    async fn try_recover(&mut self) -> Result<()> {
        let Some(store) = self.store.as_mut() else { return Ok(()); };

        let orders = store.load_orders().await?;
        if !orders.is_empty() {
            self.order_manager.borrow_mut().restore_orders(orders.clone());
            info!("recovered {} orders from store", orders.len());
            if log::log_enabled!(log::Level::Debug) {
                for o in &orders {
                    debug!(
                        "recovered order: id={} symbol={} type={} side={} qty={:.6} filled={:.6} status={} submit={} create={} filled={} end={}",
                        o.order_id,
                        o.symbol,
                        o.order_type.as_str(),
                        o.side.as_str(),
                        o.quantity,
                        o.cumulative_filled_quantity,
                        o.status.as_str(),
                        o.submit_time,
                        o.create_time,
                        o.filled_time,
                        o.end_time
                    );
                }
            }
        }

        let strategy_records = store.load_strategies().await?;
        if !strategy_records.is_empty() {
            let max_symbol_exposure_ratio = self.strategy_params.max_symbol_exposure_ratio;
            let max_total_exposure_ratio = self.strategy_params.max_total_exposure_ratio;
            for rec in strategy_records {
                match rec.type_name.as_str() {
                    "BinSingleForwardArbStrategy" => {
                        match BinSingleForwardArbSnapshot::from_bytes(&rec.payload) {
                            Ok(snap) => {
                                debug!(
                                    "recovered strategy: type={} id={} symbol={} margin_open={} um_hedge={} margin_close={} um_close={} open_to={:?} close_to={:?}",
                                    rec.type_name,
                                    snap.strategy_id,
                                    snap.symbol,
                                    snap.margin_order_id,
                                    snap.um_hedge_order_id,
                                    snap.close_margin_order_id,
                                    snap.close_um_hedge_order_id,
                                    snap.open_timeout_us,
                                    snap.close_margin_timeout_us
                                );
                                let strategy = BinSingleForwardArbStrategy::from_snapshot(
                                    &snap,
                                    self.order_manager.clone(),
                                    self.exposure_manager.clone(),
                                    self.order_tx.clone(),
                                    max_symbol_exposure_ratio,
                                    max_total_exposure_ratio,
                                    self.min_qty_table.clone(),
                                );
                                let mut strategy = strategy;
                                strategy.set_signal_sender(self.signal_tx.clone());
                                self.insert_strategy(snap.symbol.clone(), Box::new(strategy));
                            }
                            Err(err) => warn!("failed to decode arb snapshot: {err:#}"),
                        }
                    }
                    other => {
                        warn!("unknown strategy snapshot type: {}", other);
                    }
                }
            }
            info!("recovered {} strategies from store", self.strategy_mgr.len());
        }
        Ok(())
    }

    fn cleanup_inactive(&mut self) {
        let active: HashSet<i32> = self.strategy_mgr.iter_ids().cloned().collect();
        self.strategy_symbols.retain(|strategy_id, symbol| {
            if active.contains(strategy_id) {
                true
            } else {
                self.symbol_to_strategy.remove(symbol);
                false
            }
        });
        self.symbol_to_strategy
            .retain(|_symbol, strategy_id| self.strategy_symbols.contains_key(strategy_id));
    }

    fn insert_strategy(&mut self, symbol: String, strategy: Box<dyn Strategy>) {
        let strategy_id = strategy.get_id();
        let upper_symbol = symbol.to_uppercase();
        self.strategy_mgr.insert(strategy);
        self.symbol_to_strategy
            .insert(upper_symbol.clone(), strategy_id);
        self.strategy_symbols.insert(strategy_id, upper_symbol);
        debug!(
            "strategy inserted: strategy_id={} symbol={} active_total={}",
            strategy_id,
            symbol,
            self.strategy_mgr.len()
        );
    }

    fn remove_strategy(&mut self, strategy_id: i32) {
        if let Some(symbol) = self.strategy_symbols.remove(&strategy_id) {
            self.symbol_to_strategy.remove(&symbol);
        }
        self.strategy_mgr.remove(strategy_id);
    }

    fn with_strategy_mut<F>(&mut self, strategy_id: i32, mut f: F)
    where
        F: FnMut(&mut dyn Strategy),
    {
        if let Some(mut strategy) = self.strategy_mgr.take(strategy_id) {
            f(strategy.as_mut());
            if strategy.is_active() {
                self.strategy_mgr.insert(strategy);
            } else {
                drop(strategy);
                self.remove_strategy(strategy_id);
            }
        }
    }

    fn order_sender(&self) -> UnboundedSender<Bytes> {
        self.order_tx.clone()
    }

    fn signal_sender(&self) -> UnboundedSender<Bytes> {
        self.signal_tx.clone()
    }

    fn refresh_exposures(&mut self) {
        let Some(spot_snapshot) = self.spot_manager.snapshot() else {
            return;
        };
        let Some(um_snapshot) = self.um_manager.snapshot() else {
            return;
        };
        self.exposure_manager
            .borrow_mut()
            .recompute(&um_snapshot, &spot_snapshot);

        // 结合最新标记价格，打印三线表（USDT 计价的敞口），便于核对
        if let Some(table) = self.price_table.try_borrow().ok() {
            let price_snap = table.snapshot();
            let exposures = self.exposure_manager.borrow();
            log_exposures(exposures.exposures(), &price_snap);
        }
    }

    async fn tick(&mut self) {
        let now = get_timestamp_us();
        self.strategy_mgr.handle_period_clock(now);
        self.cleanup_inactive();
        if std::time::Instant::now() >= self.next_params_refresh {
            if let Err(err) = self.reload_params().await {
                warn!("pre_trade params refresh failed: {err:#}");
            }
            self.next_params_refresh = std::time::Instant::now()
                + std::time::Duration::from_secs(self.params_refresh_secs.max(5));
        }
    }
}

impl RuntimeContext {
    async fn reload_params(&mut self) -> Result<()> {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        let cli = redis::Client::open(url.clone())?;
        let mut mgr = redis::aio::ConnectionManager::new(cli).await?;
        let params: std::collections::HashMap<String, String> = redis::AsyncCommands::hgetall(&mut mgr, "binance_forward_arb_params").await?;
        let parse_f64 = |k: &str| -> Option<f64> { params.get(k).and_then(|v| v.parse::<f64>().ok()) };
        let parse_u64 = |k: &str| -> Option<u64> { params.get(k).and_then(|v| v.parse::<u64>().ok()) };
        let mut new_refresh = self.params_refresh_secs;
        if let Some(v) = parse_u64("pre_trade_refresh_secs") { new_refresh = v; }
        let mut sp = self.strategy_params.clone();
        if let Some(v) = parse_f64("pre_trade_max_pos_u") { sp.max_pos_u = v; }
        if let Some(v) = parse_f64("pre_trade_max_symbol_exposure_ratio") { sp.max_symbol_exposure_ratio = v; }
        if let Some(v) = parse_f64("pre_trade_max_total_exposure_ratio") { sp.max_total_exposure_ratio = v; }

        let snapshot = PreTradeParamsSnap {
            max_pos_u: sp.max_pos_u,
            max_symbol_exposure_ratio: sp.max_symbol_exposure_ratio,
            max_total_exposure_ratio: sp.max_total_exposure_ratio,
            refresh_secs: new_refresh,
        };
        let changed = self
            .last_params_snapshot
            .as_ref()
            .map(|old| old != &snapshot)
            .unwrap_or(true);

        self.strategy_params = sp;
        self.params_refresh_secs = new_refresh;
        self.last_params_snapshot = Some(snapshot);
        if changed {
            debug!(
                "pre_trade params updated: max_pos_u={:.2} sym_ratio={:.4} total_ratio={:.4} refresh={}s",
                self.strategy_params.max_pos_u,
                self.strategy_params.max_symbol_exposure_ratio,
                self.strategy_params.max_total_exposure_ratio,
                self.params_refresh_secs
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PreTradeParamsSnap {
    max_pos_u: f64,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    refresh_secs: u64,
}


struct OrderPublisher {
    _node: Node<ipc::Service>,
    publisher: Publisher<ipc::Service, [u8; ORDER_REQ_PAYLOAD], ()>,
}

impl OrderPublisher {
    fn new(service: &str) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(NODE_PRE_TRADE_ORDER_REQ)?)
            .create::<ipc::Service>()?;
        let service = node
            .service_builder(&ServiceName::new(service)?)
            .publish_subscribe::<[u8; ORDER_REQ_PAYLOAD]>()
            .open_or_create()?;
        let publisher = service.publisher_builder().create()?;
        info!("order publisher ready: service={}", service.name());
        Ok(Self {
            _node: node,
            publisher,
        })
    }

    fn publish(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > ORDER_REQ_PAYLOAD {
            warn!(
                "order request truncated: len={} capacity={}",
                bytes.len(),
                ORDER_REQ_PAYLOAD
            );
        }
        let mut buf = [0u8; ORDER_REQ_PAYLOAD];
        let copy_len = bytes.len().min(ORDER_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        Ok(())
    }
}

fn collect_price_symbols(
    set: &mut BTreeSet<String>,
    um_snapshot: &BinanceUmAccountSnapshot,
    spot_snapshot: &crate::pre_trade::binance_pm_spot_manager::BinanceSpotBalanceSnapshot,
) {
    for pos in &um_snapshot.positions {
        set.insert(pos.symbol.to_uppercase());
    }
    for bal in &spot_snapshot.balances {
        if bal.asset.eq_ignore_ascii_case("USDT") {
            continue;
        }
        set.insert(format!("{}USDT", bal.asset.to_uppercase()));
    }
}

fn resolve_order_req_service(cfg: &TradeEngineRespCfg) -> String {
    if let Some(req_service) = cfg.req_service.clone() {
        return req_service;
    }
    if cfg.service.contains("order_resps/") {
        return cfg.service.replace("order_resps/", "order_reqs/");
    }
    cfg.service.replace("resps", "reqs")
}

fn spawn_account_listener(cfg: &AccountStreamCfg) -> Result<UnboundedReceiver<AccountEvent>> {
    let (tx, rx) = mpsc::unbounded_channel();
    let service_name = cfg.service.clone();
    let label = cfg.label.clone().unwrap_or_else(|| cfg.service.clone());

    tokio::task::spawn_local(async move {
        let node_name = NODE_PRE_TRADE_ACCOUNT.to_string();
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> =
                service.subscriber_builder().create()?;

            info!(
                "account stream subscribed: service={} label={}",
                service_name, label
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        let received_at = get_timestamp_us();

                        // Account frames format: [type:4][len:4][data:len]
                        let (frame_len, event_type_str) = if payload.len() >= 8 {
                            let msg_type = get_account_event_type(payload);
                            let body_len = u32::from_le_bytes([
                                payload[4], payload[5], payload[6], payload[7],
                            ]) as usize;
                            let total = 8 + body_len;
                            let clamped = total.min(payload.len());
                            (clamped, format!("{:?}", msg_type))
                        } else {
                            (payload.len(), "<too_short>".to_string())
                        };

                        let mut buf = payload[..frame_len].to_vec();
                        debug!(
                            "account evt received: service={}, type={}, frame_bytes={}, cap_bytes={}",
                            service_name,
                            event_type_str,
                            buf.len(),
                            payload.len()
                        );
                        // 降低冗余 info 日志（如 AccountPosition），保留上面的 debug 行

                        let evt = AccountEvent {
                            service: service_name.clone(),
                            received_at,
                            payload_len: buf.len(),
                            payload: std::mem::take(&mut buf),
                            event_type: Some(event_type_str),
                            event_time_ms: None,
                        };
                        if tx.send(evt).is_err() {
                            break;
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("account stream receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = result.await {
            warn!("account listener exited: {err:?}");
        }
    });

    Ok(rx)
}

fn spawn_trade_response_listener(
    cfg: &TradeEngineRespCfg,
) -> Result<UnboundedReceiver<TradeExecOutcome>> {
    let (tx, rx) = mpsc::unbounded_channel();
    let service_name = cfg.service.clone();
    let label = cfg.label.clone().unwrap_or_else(|| cfg.service.clone());

    tokio::task::spawn_local(async move {
        let node_name = NODE_PRE_TRADE_TRADE_RESP.to_string();
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
                service.subscriber_builder().create()?;

            info!(
                "trade response subscribed: service={} label={}",
                service_name, label
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let raw = trim_payload(sample.payload());
                        if raw.is_empty() {
                            continue;
                        }
                        match TradeExecOutcome::parse(raw.as_ref()) {
                            Some(event) => {
                                if tx.send(event).is_err() {
                                    break;
                                }
                            }
                            None => warn!("failed to parse trade response payload"),
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("trade response receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = result.await {
            warn!("trade response listener exited: {err:?}");
        }
    });

    Ok(rx)
}

fn spawn_signal_listeners(cfg: &SignalSubscriptionsCfg) -> Result<UnboundedReceiver<TradeSignal>> {
    let (tx, rx) = mpsc::unbounded_channel();
    if cfg.channels.is_empty() {
        info!("no signal channels configured");
        return Ok(rx);
    }

    for channel in &cfg.channels {
        let channel_name = channel.clone();
        let channel_label = channel.clone();
        // Real service path used by publisher/subscriber
        let service_path = format!("signal_pubs/{}", channel_name);
        let tx_clone = tx.clone();

        tokio::task::spawn_local(async move {
            let node_name = signal_node_name(&channel_name);
            let result = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_path)?)
                    .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "signal subscribed: node={} service={} channel={}",
                    node_name, service.name(), channel_name
                );

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = trim_payload(sample.payload());
                            if log::log_enabled!(log::Level::Debug) {
                                let head = &payload[..payload.len().min(24)];
                                let head_hex: String = head.iter().map(|b| format!("{:02X}", b)).collect();
                                let st = crate::signal::trade_signal::TradeSignal::get_signal_type(&payload)
                                    .map(|t| format!("{:?}", t))
                                    .unwrap_or_else(|| "Unknown".to_string());
                                let gen_ts = crate::signal::trade_signal::TradeSignal::get_generation_time(&payload)
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "-".to_string());
                                let ctx_len = crate::signal::trade_signal::TradeSignal::get_context_length(&payload)
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "-".to_string());
                                debug!(
                                    "signal frame received: channel={} bytes={} head24={} type={} gen_ts={} ctx_len={}",
                                    channel_name,
                                    payload.len(),
                                    head_hex,
                                    st,
                                    gen_ts,
                                    ctx_len
                                );
                            }
                            if payload.is_empty() {
                                continue;
                            }
                            match TradeSignal::from_bytes(&payload) {
                                Ok(signal) => {
                                    if tx_clone.send(signal).is_err() {
                                        break;
                                    }
                                }
                                Err(err) => warn!(
                                    "failed to decode trade signal from channel {}: {}",
                                    channel_name, err
                                ),
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("signal receive error (channel={}): {err}", channel_name);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            };

            if let Err(err) = result.await {
                warn!(
                    "signal listener exited (channel={}): {err:?}",
                    channel_label
                );
            }
        });
    }

    Ok(rx)
}

fn handle_account_event(ctx: &mut RuntimeContext, evt: AccountEvent) -> Result<()> {
    if evt.payload.len() < 8 {
        anyhow::bail!("account payload too short: {} bytes", evt.payload.len());
    }

    let msg_type = get_account_event_type(&evt.payload);
    let hdr_len_bytes = u32::from_le_bytes([
        evt.payload[4],
        evt.payload[5],
        evt.payload[6],
        evt.payload[7],
    ]) as usize;
    debug!(
        "account msg header: service={}, decoded_type={:?}, declared_len={}, evt_meta=({:?},{:?}), first8={:02X?}",
        evt.service,
        msg_type,
        hdr_len_bytes,
        evt.event_type,
        evt.event_time_ms,
        &evt.payload[..8]
    );
    let payload_len = hdr_len_bytes;

    if evt.payload.len() < 8 + payload_len {
        anyhow::bail!(
            "account payload truncated: have {} expect {}",
            evt.payload.len(),
            8 + payload_len
        );
    }

    let data = &evt.payload[8..8 + payload_len];
    match msg_type {
        AccountEventType::AccountPosition => {
            let msg = AccountPositionMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_account_position(&msg);
            if !ctx.dedup.insert_check(key) {
                debug!(
                    "dedup drop AccountPosition: asset={} update_id={} event_time={}",
                    msg.asset, msg.update_id, msg.event_time
                );
                return Ok(());
            }
            debug!(
                "outboundAccountPosition: asset={}, free={}, locked={}, event_time={}",
                msg.asset, msg.free_balance, msg.locked_balance, msg.event_time
            );
            ctx.spot_manager.apply_account_position(
                &msg.asset,
                msg.free_balance,
                msg.locked_balance,
                msg.event_time,
            );
            ctx.refresh_exposures();
        }
        AccountEventType::BalanceUpdate => {
            let msg = BalanceUpdateMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_balance_update(&msg);
            if !ctx.dedup.insert_check(key) {
                debug!(
                    "dedup drop BalanceUpdate: asset={} update_id={} event_time={}",
                    msg.asset, msg.update_id, msg.event_time
                );
                return Ok(());
            }
            ctx.spot_manager
                .apply_balance_delta(&msg.asset, msg.delta, msg.event_time);
            ctx.refresh_exposures();
        }
        AccountEventType::AccountUpdateBalance => {
            let msg = AccountUpdateBalanceMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_account_update_balance(&msg);
            if !ctx.dedup.insert_check(key) {
                debug!(
                    "dedup drop AccountUpdateBalance: asset={} event_time={}",
                    msg.asset, msg.event_time
                );
                return Ok(());
            }
            ctx.spot_manager.apply_balance_snapshot(
                &msg.asset,
                msg.wallet_balance,
                msg.cross_wallet_balance,
                msg.balance_change,
                msg.event_time,
            );
            ctx.refresh_exposures();
        }
        AccountEventType::AccountUpdatePosition => {
            let msg = AccountUpdatePositionMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_account_update_position(&msg);
            if !ctx.dedup.insert_check(key) {
                debug!(
                    "dedup drop AccountUpdatePosition: symbol={} side={} event_time={}",
                    msg.symbol, msg.position_side, msg.event_time
                );
                return Ok(());
            }
            ctx.um_manager.apply_position_update(
                &msg.symbol,
                msg.position_side,
                msg.position_amount,
                msg.entry_price,
                msg.unrealized_pnl,
                msg.breakeven_price,
                msg.event_time,
            );
            ctx.refresh_exposures();
        }
        AccountEventType::ExecutionReport => {
            let report = ExecutionReportMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_execution_report(&report);
            if !ctx.dedup.insert_check(key) {
                debug!(
                    "dedup drop ExecutionReport: symbol={} ord={} trade={} x={} X={}",
                    report.symbol, report.order_id, report.trade_id, report.execution_type, report.order_status
                );
                return Ok(());
            }
            debug!(
                "executionReport: sym={}, cli_id={}, cli_str='{}', ord_id={}, trade_id={}, side={}, maker={}, working={}, otype={}, tif={}, x={}, X={}, px={}, qty={}, last_px={}, last_qty={}, cum_qty={}, fee_amt={}, fee_ccy={}, cum_quote={}, last_quote={}, qoq={}, times(E/T/O/W/I)={}/{}/{}/{}/{}",
                report.symbol,
                report.client_order_id,
                report.client_order_id_str,
                report.order_id,
                report.trade_id,
                report.side,
                report.is_maker,
                report.is_working,
                report.order_type,
                report.time_in_force,
                report.execution_type,
                report.order_status,
                report.price,
                report.quantity,
                report.last_executed_price,
                report.last_executed_quantity,
                report.cumulative_filled_quantity,
                report.commission_amount,
                report.commission_asset,
                report.cumulative_quote,
                report.last_quote,
                report.quote_order_quantity,
                report.event_time,
                report.transaction_time,
                report.order_creation_time,
                report.working_time,
                report.update_id,
            );
            dispatch_execution_report(ctx, &report);
        }
        AccountEventType::OrderTradeUpdate => {
            let update = OrderTradeUpdateMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_order_trade_update(&update);
            if !ctx.dedup.insert_check(key) {
                debug!(
                    "dedup drop OrderTradeUpdate: symbol={} ord={} trade={} x={} X={}",
                    update.symbol, update.order_id, update.trade_id, update.execution_type, update.order_status
                );
                return Ok(());
            }
            debug!(
                "orderTradeUpdate: sym={}, cli_id={}, cli_str='{}', ord_id={}, trade_id={}, side={}, pos_side={}, maker={}, reduce={}, otype={}, tif={}, x={}, X={}, px={}, qty={}, avg_px={}, stop_px={}, last_px={}, last_qty={}, cum_qty={}, fee_amt={}, fee_ccy={}, buy_notional={}, sell_notional={}, realized_pnl={}, times(E/T)={}/{}",
                update.symbol,
                update.client_order_id,
                update.client_order_id_str,
                update.order_id,
                update.trade_id,
                update.side,
                update.position_side,
                update.is_maker,
                update.reduce_only,
                update.order_type,
                update.time_in_force,
                update.execution_type,
                update.order_status,
                update.price,
                update.quantity,
                update.average_price,
                update.stop_price,
                update.last_executed_price,
                update.last_executed_quantity,
                update.cumulative_filled_quantity,
                update.commission_amount,
                update.commission_asset,
                update.buy_notional,
                update.sell_notional,
                update.realized_profit,
                update.event_time,
                update.transaction_time,
            );
            dispatch_order_trade_update(ctx, &update);
        }
        _ => {}
    }

    Ok(())
}

fn handle_trade_engine_response(ctx: &mut RuntimeContext, outcome: TradeExecOutcome) {
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(outcome.client_order_id) {
                strategy.handle_trade_response(&outcome);
            }
        });
    }

    ctx.cleanup_inactive();
}

fn handle_trade_signal(ctx: &mut RuntimeContext, signal: TradeSignal) {
    let raw_signal = signal.to_bytes();
    debug!(
        "trade signal received: type={:?} generation_time={} ctx_len={}",
        signal.signal_type,
        signal.generation_time,
        signal.context.len()
    );
    match signal.signal_type {
        SignalType::BinSingleForwardArbOpen => {
            match BinSingleForwardArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(open_ctx) => {
                    let symbol = open_ctx.spot_symbol.to_uppercase();
                    let strategy_id = StrategyManager::generate_strategy_id(1);
                    let order_tx = ctx.order_sender();
                    let signal_tx = ctx.signal_sender();
                    let now = get_timestamp_us();

                    let mut strategy = BinSingleForwardArbStrategy::new(
                        strategy_id,
                        now,
                        symbol.clone(),
                        ctx.order_manager.clone(),
                        ctx.exposure_manager.clone(),
                        order_tx,
                        ctx.strategy_params.max_symbol_exposure_ratio,
                        ctx.strategy_params.max_total_exposure_ratio,
                        ctx.min_qty_table.clone(),
                    );
                    strategy.set_signal_sender(signal_tx);

                    debug!(
                        "strategy init for open signal: strategy_id={} symbol={} qty={:.6} price={:.8} tick={:.8} type={:?}",
                        strategy_id,
                        open_ctx.spot_symbol,
                        open_ctx.amount,
                        open_ctx.price,
                        open_ctx.price_tick,
                        open_ctx.order_type
                    );

                    strategy.handle_trade_signal(&raw_signal);

                    if strategy.is_active() {
                        ctx.insert_strategy(symbol, Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode open context: {err}"),
            }
        }
        SignalType::BinSingleForwardArbHedge
        | SignalType::BinSingleForwardArbCloseMargin
        | SignalType::BinSingleForwardArbCloseUm => {
            debug!(
                "dispatch signal to existing strategies: type={:?} ctx_len={}",
                signal.signal_type,
                signal.context.len()
            );
            dispatch_signal_to_existing_strategy(ctx, signal, raw_signal);
        }
    }

    ctx.cleanup_inactive();
}

fn dispatch_signal_to_existing_strategy(
    ctx: &mut RuntimeContext,
    signal: TradeSignal,
    raw_signal: Bytes,
) {
    let maybe_symbol = match signal.signal_type {
        SignalType::BinSingleForwardArbHedge => None,
        SignalType::BinSingleForwardArbCloseMargin => {
            match BinSingleForwardArbCloseMarginCtx::from_bytes(signal.context.clone()) {
                Ok(close_ctx) => {
                    debug!(
                        "decoded margin close ctx: spot_symbol={} limit_price={:.8} tick={:.8} exp_time={}",
                        close_ctx.spot_symbol, close_ctx.limit_price, close_ctx.price_tick, close_ctx.exp_time
                    );
                    Some(close_ctx.spot_symbol.to_uppercase())
                }
                Err(err) => {
                    warn!("failed to decode margin close context: {err}");
                    return;
                }
            }
        }
        SignalType::BinSingleForwardArbCloseUm => {
            match BinSingleForwardArbCloseUmCtx::from_bytes(signal.context.clone()) {
                Ok(um_ctx) => {
                    debug!(
                        "decoded UM close ctx: um_symbol={} exp_time={}",
                        um_ctx.um_symbol, um_ctx.exp_time
                    );
                    Some(um_ctx.um_symbol.to_uppercase())
                }
                Err(err) => {
                    warn!("failed to decode UM close context: {err}");
                    return;
                }
            }
        }
        _ => None,
    };

    let strategy_ids: Vec<i32> = if let Some(symbol) = maybe_symbol {
        ctx.symbol_to_strategy
            .get(&symbol)
            .cloned()
            .into_iter()
            .collect()
    } else {
        ctx.strategy_mgr.iter_ids().cloned().collect()
    };

    if strategy_ids.is_empty() {
        debug!("no active strategy matched for this signal");
    }

    for strategy_id in strategy_ids {
        let signal_bytes = raw_signal.clone();
        ctx.with_strategy_mut(strategy_id, |strategy| {
            debug!(
                "forward signal to strategy_id={} raw_len={}",
                strategy.get_id(),
                signal_bytes.len()
            );
            strategy.handle_trade_signal(&signal_bytes);
        });
    }
}

fn dispatch_execution_report(ctx: &mut RuntimeContext, report: &ExecutionReportMsg) {
    let order_id = report.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                strategy.handle_binance_margin_order_update(report);
            }
        });
    }

    if !matched {
        debug!(
            "executionReport not matched to any strategy: client_order_id={} client_order_id_str='{}'",
            order_id,
            report.client_order_id_str
        );
    }

    ctx.cleanup_inactive();
}

fn dispatch_order_trade_update(ctx: &mut RuntimeContext, update: &OrderTradeUpdateMsg) {
    let order_id = update.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                strategy.handle_binance_futures_order_update(update);
            }
        });
    }

    if !matched {
        debug!(
            "orderTradeUpdate not matched to any strategy: client_order_id={} client_order_id_str='{}'",
            order_id,
            update.client_order_id_str
        );
    }

    ctx.cleanup_inactive();
}

fn trim_payload(payload: &[u8]) -> Bytes {
    // 直接拷贝整个 payload，避免将结尾合法的 0 截断
    Bytes::copy_from_slice(payload)
}

// 删除了基于 JSON 的账户元数据提取逻辑，账户事件采用二进制帧头解析

fn signal_node_name(channel: &str) -> String {
    format!(
        "{}{}",
        NODE_PRE_TRADE_SIGNAL_PREFIX,
        sanitize_suffix(channel)
    )
}

fn sanitize_suffix(raw: &str) -> std::borrow::Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    std::borrow::Cow::Owned(sanitized)
}

fn is_valid_node_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn log_um_positions(positions: &[BinanceUmPosition]) {
    if positions.is_empty() {
        info!("UM 持仓为空");
        return;
    }

    let mut rows: Vec<Vec<String>> = positions
        .iter()
        .map(|pos| {
            vec![
                pos.symbol.clone(),
                pos.position_side.to_string(),
                fmt_decimal(pos.position_amt),
                fmt_decimal(pos.entry_price),
                fmt_decimal(pos.leverage),
                fmt_decimal(pos.position_initial_margin),
                fmt_decimal(pos.open_order_initial_margin),
                fmt_decimal(pos.unrealized_profit),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let table = render_three_line_table(
        &[
            "Symbol", "Side", "PosAmt", "EntryPx", "Lev", "PosIM", "OpenIM", "uPnL",
        ],
        &rows,
    );
    info!("UM 持仓概览\n{}", table);
}

fn log_spot_balances(balances: &[BinanceSpotBalance]) {
    if balances.is_empty() {
        warn!("现货资产列表为空");
        return;
    }

    let mut rows: Vec<Vec<String>> = balances
        .iter()
        .map(|bal| {
            vec![
                bal.asset.clone(),
                fmt_decimal(bal.total_wallet_balance),
                fmt_decimal(bal.cross_margin_free),
                fmt_decimal(bal.cross_margin_locked),
                fmt_decimal(bal.cross_margin_borrowed),
                fmt_decimal(bal.um_wallet_balance),
                fmt_decimal(bal.um_unrealized_pnl),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let table = render_three_line_table(
        &[
            "Asset",
            "TotalWallet",
            "CrossFree",
            "CrossLocked",
            "CrossBorrowed",
            "UMWallet",
            "UMUPNL",
        ],
        &rows,
    );
    info!("现货资产概览\n{}", table);
}

fn log_exposures(entries: &[ExposureEntry], price_map: &BTreeMap<String, PriceEntry>) {
    if entries.is_empty() {
        info!("非 USDT 资产敞口为空");
        return;
    }

    let rows: Vec<Vec<String>> = entries
        .iter()
        .map(|entry| {
            let asset = entry.asset.to_uppercase();
            let sym = if asset == "USDT" { "USDT".to_string() } else { format!("{}USDT", asset) };
            let mark = if asset == "USDT" { 1.0 } else { price_map.get(&sym).map(|p| p.mark_price).unwrap_or(0.0) };
            if asset != "USDT" && mark == 0.0 && (entry.spot_total_wallet != 0.0 || entry.um_net_position != 0.0) {
                debug!("missing mark price for {}, exposure valued as 0", sym);
            }
            let spot_usdt = entry.spot_total_wallet * mark;
            let um_usdt = entry.um_net_position * mark;
            let exposure_qty = entry.exposure;
            let exposure_usdt = spot_usdt + um_usdt;
            vec![
                entry.asset.clone(),
                fmt_decimal(entry.spot_total_wallet),
                fmt_decimal(spot_usdt),
                fmt_decimal(entry.um_net_position),
                fmt_decimal(um_usdt),
                fmt_decimal(exposure_qty),
                fmt_decimal(exposure_usdt),
            ]
        })
        .collect();

    let table = render_three_line_table(
        &[
            "Asset",
            "SpotQty",
            "SpotUSDT",
            "UMNetQty",
            "UMNetUSDT",
            "ExposureQty",
            "ExposureUSDT",
        ],
        &rows,
    );
    info!("现货+UM 敞口汇总\n{}", table);
}

fn log_price_table(entries: &BTreeMap<String, PriceEntry>) {
    if entries.is_empty() {
        warn!("未获取到标记价格数据");
        return;
    }

    let rows: Vec<Vec<String>> = entries
        .values()
        .map(|entry| {
            vec![
                entry.symbol.clone(),
                fmt_decimal(entry.mark_price),
                fmt_decimal(entry.index_price),
                entry.update_time.to_string(),
            ]
        })
        .collect();

    let table =
        render_three_line_table(&["Symbol", "MarkPrice", "IndexPrice", "UpdateTime"], &rows);
    info!("标记价格表\n{}", table);
}

fn fmt_decimal(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let mut s = format!("{:.6}", value);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths = compute_widths(headers, rows);
    let mut out = String::new();
    out.push_str(&build_separator(&widths, '-'));
    out.push('\n');
    out.push_str(&build_row(
        headers
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&build_separator(&widths, '='));
    if rows.is_empty() {
        out.push('\n');
        out.push_str(&build_separator(&widths, '-'));
        return out;
    }
    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row.clone(), &widths));
    }
    out.push('\n');
    out.push_str(&build_separator(&widths, '-'));
    out
}

fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                continue;
            }
            widths[idx] = widths[idx].max(cell.len());
        }
    }
    widths
}

fn build_separator(widths: &[usize], fill: char) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&fill.to_string().repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    for (cell, width) in cells.iter().zip(widths.iter()) {
        row.push(' ');
        row.push_str(&format!("{:<width$}", cell, width = *width));
        row.push(' ');
        row.push('|');
    }
    row
}

fn spawn_derivatives_worker(price_table: Rc<RefCell<PriceTable>>) -> Result<()> {
    let service = DERIVATIVES_SERVICE.to_string();
    let node_name = NODE_PRE_TRADE_DERIVATIVES.to_string();
    tokio::task::spawn_local(async move {
        if let Err(err) = derivatives_loop(node_name, service, price_table).await {
            error!("derivatives worker exited: {err:?}");
        }
    });
    Ok(())
}

async fn derivatives_loop(
    node_name: String,
    service: String,
    price_table: Rc<RefCell<PriceTable>>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    info!("derivatives metrics subscribed: service={}", service.name());

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let payload = trim_payload(sample.payload());
                if payload.is_empty() {
                    continue;
                }
                let Some(msg_type) = get_msg_type(&payload) else {
                    continue;
                };
                match msg_type {
                    MktMsgType::MarkPrice => match parse_mark_price(&payload) {
                        Ok(msg) => {
                            let mut table = price_table.borrow_mut();
                            table.update_mark_price(&msg.symbol, msg.mark_price, msg.timestamp);
                        }
                        Err(err) => warn!("parse mark price failed: {err:?}"),
                    },
                    MktMsgType::IndexPrice => match parse_index_price(&payload) {
                        Ok(msg) => {
                            let mut table = price_table.borrow_mut();
                            table.update_index_price(&msg.symbol, msg.index_price, msg.timestamp);
                        }
                        Err(err) => warn!("parse index price failed: {err:?}"),
                    },
                    _ => {}
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("derivatives stream receive error: {err}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}
