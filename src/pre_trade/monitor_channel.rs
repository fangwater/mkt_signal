use anyhow::{anyhow, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn, debug};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::common::account_msg::{
    get_event_type as get_account_event_type, AccountEventType, AccountPositionMsg,
    AccountUpdateBalanceMsg, AccountUpdateFlushMsg, AccountUpdatePositionMsg, BalanceUpdateMsg,
    ExecutionReportMsg, OrderTradeUpdateMsg,
};
use crate::signal::common::ExecutionType;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::binance_pm_spot_manager::{BinancePmSpotAccountManager, BinanceSpotBalance};
use crate::pre_trade::binance_pm_um_manager::{BinancePmUmAccountManager, BinanceUmPosition};
use crate::pre_trade::event::AccountEvent;

const ACCOUNT_PAYLOAD: usize = 16_384;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";

// ==================== Deduplication Cache ====================

/// 简单的去重缓存（固定容量，FIFO 淘汰）
pub struct DedupCache {
    set: HashSet<u64>,
    queue: VecDeque<u64>,
    capacity: usize,
}

impl DedupCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            queue: VecDeque::new(),
            capacity: capacity.max(1024),
        }
    }

    /// 插入并返回是否为新条目；false 表示重复，应丢弃
    pub fn insert_check(&mut self, key: u64) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.queue.len() >= self.capacity {
            if let Some(old) = self.queue.pop_front() {
                self.set.remove(&old);
            }
        }
        self.queue.push_back(key);
        self.set.insert(key);
        true
    }
}

/// 组合多个 u64 片段生成稳定的 64 位哈希
pub fn hash64(parts: &[u64]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut hasher);
    }
    hasher.finish()
}

fn hash_str64(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ---- Key helpers for account event types ----

pub fn key_balance_update(msg: &BalanceUpdateMsg) -> u64 {
    hash64(&[
        AccountEventType::BalanceUpdate as u32 as u64,
        msg.update_id as u64,
        msg.event_time as u64,
    ])
}

pub fn key_account_update_balance(msg: &AccountUpdateBalanceMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdateBalance as u32 as u64,
        msg.event_time as u64,
        msg.transaction_time as u64,
        hash_str64(&msg.asset),
    ])
}

pub fn key_account_position(msg: &AccountPositionMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountPosition as u32 as u64,
        msg.update_id as u64,
        msg.event_time as u64,
        hash_str64(&msg.asset),
    ])
}

pub fn key_account_update_position(msg: &AccountUpdatePositionMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdatePosition as u32 as u64,
        msg.event_time as u64,
        msg.transaction_time as u64,
        msg.symbol_length as u64,
        msg.position_side as u8 as u64,
    ])
}

pub fn key_account_update_flush(msg: &AccountUpdateFlushMsg) -> u64 {
    hash64(&[
        AccountEventType::AccountUpdateFlush as u32 as u64,
        msg.hash,
        hash_str64(&msg.scope),
    ])
}

pub fn key_execution_report(msg: &ExecutionReportMsg) -> u64 {
    hash64(&[
        AccountEventType::ExecutionReport as u32 as u64,
        msg.order_id as u64,
        msg.trade_id as u64,
        msg.update_id as u64,
        msg.event_time as u64,
    ])
}

pub fn key_order_trade_update(msg: &OrderTradeUpdateMsg) -> u64 {
    hash64(&[
        AccountEventType::OrderTradeUpdate as u32 as u64,
        msg.order_id as u64,
        msg.trade_id as u64,
        msg.event_time as u64,
    ])
}

// ==================== Monitor Channel ====================

use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use crate::pre_trade::binance_pm_um_manager::BinanceUmAccountSnapshot;
use crate::pre_trade::binance_pm_spot_manager::BinanceSpotBalanceSnapshot;
use crate::common::min_qty_table::MinQtyTable;
use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::strategy::order_update::OrderUpdate;
use bytes::Bytes;
use std::rc::Rc;
use std::cell::RefCell;

/// MonitorChannel 负责订阅 Binance PM account monitor service
/// 集成账户管理器（UM 合约 + Spot 现货），自动初始化并启动监听
pub struct MonitorChannel {
    dedup: DedupCache,
    tx: UnboundedSender<AccountEvent>,
    rx: Option<UnboundedReceiver<AccountEvent>>,
    /// 币安 合约资产管理器，基于统一账户 update 更新
    pub um_manager: Rc<RefCell<BinancePmUmAccountManager>>,
    /// 币安 现货资产管理器，基于统一账户 update 更新
    pub spot_manager: Rc<RefCell<BinancePmSpotAccountManager>>,
    /// 敞口管理器
    pub exposure_manager: Rc<RefCell<ExposureManager>>,
    /// 价格表
    pub price_table: Rc<RefCell<PriceTable>>,
    /// 交易对最小下单量/步进信息（spot/futures/margin）
    pub min_qty_table: Rc<MinQtyTable>,
    /// 策略管理器
    pub strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    /// 最大杠杆（用于敞口日志）
    max_leverage: f64,
}

impl MonitorChannel {
    /// 创建 Binance PM account monitor 实例并启动监听
    ///
    /// # 功能
    /// - 从环境变量读取 API 凭证（BINANCE_API_KEY, BINANCE_API_SECRET）
    /// - 初始化 UM 合约账户管理器
    /// - 初始化 Spot 现货账户管理器
    /// - 收集所需的价格符号并初始化价格表
    /// - 加载交易对 LOT_SIZE/PRICE_FILTER（spot/futures/margin）
    /// - 创建 ExposureManager 并基于初始快照和价格估值
    /// - 订阅 IceOryx 频道: account_pubs/binance_pm
    /// - 启动后台监听任务（账户状态更新在内部直接处理，订单/成交回报发送到 channel）
    /// - 启动衍生品价格监听任务（mark_price, index_price）
    ///
    /// # 参数
    /// - `strategy_mgr`: 策略管理器
    /// - `max_leverage`: 最大杠杆（用于风险指标日志）
    ///
    /// # 环境变量
    /// - `BINANCE_API_KEY`: Binance API Key（必需）
    /// - `BINANCE_API_SECRET`: Binance API Secret（必需）
    /// - `SPOT_ASSET_FILTER`: 可选，过滤特定现货资产（如 "USDT"）
    ///
    /// # 错误
    /// - API 凭证未设置
    /// - 初始账户快照获取失败
    pub async fn new_binance_pm_monitor(
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
        max_leverage: f64,
    ) -> Result<Self> {
        // Read API credentials from environment variables
        let api_key = std::env::var("BINANCE_API_KEY")
            .map_err(|_| anyhow!("BINANCE_API_KEY environment variable not set"))?;
        let api_secret = std::env::var("BINANCE_API_SECRET")
            .map_err(|_| anyhow!("BINANCE_API_SECRET environment variable not set"))?;

        // Hardcoded REST endpoints
        let rest_base = "https://papi.binance.com";
        let recv_window_ms = 5000;

        // 初始化 UM 合约管理器
        let um_manager = BinancePmUmAccountManager::new(
            rest_base,
            api_key.clone(),
            api_secret.clone(),
            recv_window_ms,
        );
        let um_snapshot = um_manager
            .init()
            .await
            .context("failed to load initial Binance UM snapshot")?;
        log_um_positions(&um_snapshot.positions);

        // 初始化 Spot 现货管理器（使用相同的 PM 账户凭证）
        let asset_filter = std::env::var("SPOT_ASSET_FILTER")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        let spot_manager = BinancePmSpotAccountManager::new(
            rest_base,
            api_key,
            api_secret,
            recv_window_ms,
            asset_filter,
        );
        let spot_snapshot = spot_manager
            .init()
            .await
            .context("failed to load initial Binance spot snapshot")?;
        log_spot_balances(&spot_snapshot.balances);

        // 收集需要的价格符号
        let mut price_symbols: BTreeSet<String> = BTreeSet::new();
        collect_price_symbols(&mut price_symbols, &um_snapshot, &spot_snapshot);

        // 创建并初始化价格表
        let price_table = Rc::new(RefCell::new(PriceTable::new()));
        {
            let mut table = price_table.borrow_mut();
            table
                .init(&price_symbols)
                .await
                .context("failed to load initial price table")?;
            log_price_table(&table.snapshot());
        }

        // 创建 ExposureManager（基于初始快照）
        let mut exposure_manager = ExposureManager::new(&um_snapshot, &spot_snapshot);
        {
            let table = price_table.borrow();
            let snap = table.snapshot();
            // 基于初始价格对总权益/总敞口进行 USDT 计价
            exposure_manager.revalue_with_prices(&snap);
            log_exposures(exposure_manager.exposures(), &snap);
        }
        let exposure_manager = Rc::new(RefCell::new(exposure_manager));

        // 加载交易对 LOT_SIZE/PRICE_FILTER（spot/futures/margin），用于数量/价格对齐
        let mut min_qty_table = MinQtyTable::new();
        if let Err(err) = min_qty_table.refresh_binance().await {
            warn!("failed to refresh Binance exchange filters: {err:#}");
        }
        let min_qty_table = Rc::new(min_qty_table);

        // 包装为 Rc<RefCell<>>
        let um_manager_rc = Rc::new(RefCell::new(um_manager));
        let spot_manager_rc = Rc::new(RefCell::new(spot_manager));

        // 创建事件通道（只用于 ExecutionReport 和 OrderTradeUpdate）
        let (tx, rx) = mpsc::unbounded_channel();

        let service_name = "account_pubs/binance_pm".to_string();
        let node_name = "pre_trade_account_pubs_binance_pm".to_string();

        // 启动账户事件监听任务
        Self::spawn_listener(
            tx.clone(),
            service_name,
            node_name,
            um_manager_rc.clone(),
            spot_manager_rc.clone(),
            exposure_manager.clone(),
            price_table.clone(),
            strategy_mgr.clone(),
            max_leverage,
        );

        // 启动衍生品价格监听任务（mark_price, index_price）
        Self::spawn_derivatives_listener(price_table.clone());

        Ok(Self {
            dedup: DedupCache::new(8192),
            tx,
            rx: Some(rx),
            um_manager: um_manager_rc,
            spot_manager: spot_manager_rc,
            exposure_manager,
            price_table,
            min_qty_table,
            strategy_mgr,
            max_leverage,
        })
    }

    /// 获取 sender，用于发送事件
    pub fn get_sender(&self) -> UnboundedSender<AccountEvent> {
        self.tx.clone()
    }

    /// 获取 receiver，只能调用一次
    pub fn take_receiver(&mut self) -> Option<UnboundedReceiver<AccountEvent>> {
        self.rx.take()
    }

    fn spawn_listener(
        tx: UnboundedSender<AccountEvent>,
        service_name: String,
        node_name: String,
        um_manager: Rc<RefCell<BinancePmUmAccountManager>>,
        spot_manager: Rc<RefCell<BinancePmSpotAccountManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        price_table: Rc<RefCell<PriceTable>>,
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
        max_leverage: f64,
    ) {
        tokio::task::spawn_local(async move {
            let service_name_for_error = service_name.clone();
            let mut dedup = DedupCache::new(8192);

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

                info!("account stream subscribed: service={}", service_name);

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            let received_at = get_timestamp_us();

                            // Account frames format: [type:4][len:4][data:len]
                            if payload.len() < 8 {
                                continue;
                            }

                            let msg_type = get_account_event_type(payload);
                            let body_len = u32::from_le_bytes([
                                payload[4], payload[5], payload[6], payload[7],
                            ]) as usize;
                            let total = 8 + body_len;
                            let frame_len = total.min(payload.len());
                            let data = &payload[8..frame_len];

                            // 根据事件类型处理
                            match msg_type {
                                // 账户状态更新：直接处理，不发送到 channel
                                AccountEventType::AccountPosition => {
                                    if let Ok(msg) = AccountPositionMsg::from_bytes(data) {
                                        let key = key_account_position(&msg);
                                        if !dedup.insert_check(key) {
                                            continue;
                                        }
                                        spot_manager.borrow_mut().apply_account_position(
                                            &msg.asset,
                                            msg.free_balance,
                                            msg.locked_balance,
                                            msg.event_time,
                                        );
                                        refresh_exposures(&um_manager, &spot_manager, &exposure_manager, &price_table, max_leverage);
                                    }
                                }
                                AccountEventType::BalanceUpdate => {
                                    if let Ok(msg) = BalanceUpdateMsg::from_bytes(data) {
                                        let key = key_balance_update(&msg);
                                        if !dedup.insert_check(key) {
                                            continue;
                                        }
                                        spot_manager.borrow_mut().apply_balance_delta(&msg.asset, msg.delta, msg.event_time);
                                        refresh_exposures(&um_manager, &spot_manager, &exposure_manager, &price_table, max_leverage);
                                    }
                                }
                                AccountEventType::AccountUpdateBalance => {
                                    if let Ok(msg) = AccountUpdateBalanceMsg::from_bytes(data) {
                                        if msg.business_unit.eq_ignore_ascii_case("UM") {
                                            spot_manager.borrow_mut().apply_um_wallet_snapshot(
                                                &msg.asset,
                                                msg.wallet_balance,
                                                msg.event_time,
                                            );
                                        } else {
                                            spot_manager.borrow_mut().apply_balance_snapshot(
                                                &msg.asset,
                                                msg.wallet_balance,
                                                msg.cross_wallet_balance,
                                                msg.balance_change,
                                                msg.event_time,
                                            );
                                        }
                                    }
                                }
                                AccountEventType::AccountUpdatePosition => {
                                    if let Ok(msg) = AccountUpdatePositionMsg::from_bytes(data) {
                                        um_manager.borrow_mut().apply_position_update(
                                            &msg.symbol,
                                            msg.position_side,
                                            msg.position_amount,
                                            msg.entry_price,
                                            msg.unrealized_pnl,
                                            msg.breakeven_price,
                                            msg.event_time,
                                        );
                                    }
                                }
                                AccountEventType::AccountUpdateFlush => {
                                    if let Ok(msg) = AccountUpdateFlushMsg::from_bytes(data) {
                                        let key = key_account_update_flush(&msg);
                                        if !dedup.insert_check(key) {
                                            continue;
                                        }
                                        refresh_exposures(&um_manager, &spot_manager, &exposure_manager, &price_table, max_leverage);
                                    }
                                }
                                // ExecutionReport 和 OrderTradeUpdate：直接处理
                                AccountEventType::ExecutionReport => {
                                    if let Ok(report) = ExecutionReportMsg::from_bytes(data) {
                                        let key = key_execution_report(&report);
                                        if !dedup.insert_check(key) {
                                            continue;
                                        }
                                        debug!(
                                            "executionReport: sym={} cli_id={} ord_id={} status={}",
                                            report.symbol, report.client_order_id, report.order_id, report.order_status
                                        );
                                        dispatch_execution_report(&strategy_mgr, &report);
                                    }
                                }
                                AccountEventType::OrderTradeUpdate => {
                                    if let Ok(update) = OrderTradeUpdateMsg::from_bytes(data) {
                                        let key = key_order_trade_update(&update);
                                        if !dedup.insert_check(key) {
                                            continue;
                                        }
                                        debug!(
                                            "orderTradeUpdate: sym={}, cli_id={}, ord_id={}, trade_id={}, x={}, X={}",
                                            update.symbol,
                                            update.client_order_id,
                                            update.order_id,
                                            update.trade_id,
                                            update.execution_type,
                                            update.order_status,
                                        );
                                        dispatch_order_trade_update(&strategy_mgr, &update);
                                    }
                                }
                                _=>{
                                    
                                }
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
                warn!("account listener {} exited: {err:?}", service_name_for_error);
            }
        });
    }
}

// ==================== Helper Functions ====================

/// 刷新敞口：重新计算并打印敞口信息
fn refresh_exposures(
    um_manager: &Rc<RefCell<BinancePmUmAccountManager>>,
    spot_manager: &Rc<RefCell<BinancePmSpotAccountManager>>,
    exposure_manager: &Rc<RefCell<ExposureManager>>,
    price_table: &Rc<RefCell<PriceTable>>,
    max_leverage: f64,
) {
    let spot_snapshot = spot_manager.borrow().snapshot();
    let um_snapshot = um_manager.borrow().snapshot();

    let (Some(spot_snapshot), Some(um_snapshot)) = (spot_snapshot, um_snapshot) else {
        return;
    };

    let positions_changed = exposure_manager
        .borrow_mut()
        .recompute(&um_snapshot, &spot_snapshot);

    // 结合最新标记价格，估值并打印三线表（USDT 计价的敞口），便于核对
    if let Ok(table) = price_table.try_borrow() {
        let price_snap = table.snapshot();
        {
            let mut mgr = exposure_manager.borrow_mut();
            mgr.revalue_with_prices(&price_snap);
        }
        if positions_changed {
            let exposures = exposure_manager.borrow();
            log_exposures(exposures.exposures(), &price_snap);
            log_exposure_summary(
                exposures.total_equity(),
                exposures.total_abs_exposure(),
                exposures.total_position(),
                max_leverage,
            );
        }
    }
}

impl MonitorChannel {
    /// 启动衍生品价格监听任务（mark_price, index_price）
    fn spawn_derivatives_listener(price_table: Rc<RefCell<PriceTable>>) {
        tokio::task::spawn_local(async move {
            let result: Result<()> = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(NODE_PRE_TRADE_DERIVATIVES)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(DERIVATIVES_SERVICE)?)
                    .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!("derivatives price stream subscribed: service={}", DERIVATIVES_SERVICE);

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = Bytes::copy_from_slice(sample.payload());
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
            .await;

            if let Err(err) = result {
                log::error!("derivatives listener exited: {err:?}");
            }
        });
    }
}

fn log_um_positions(positions: &[BinanceUmPosition]) {
    if positions.is_empty() {
        info!("Binance UM account initialized: no open positions");
        return;
    }
    info!("Binance UM account initialized: {} positions", positions.len());
    for pos in positions {
        if pos.position_amt.abs() > 1e-8 {
            info!(
                "  UM position: {} amt={:.8} entry={:.4} upnl={:.4}",
                pos.symbol, pos.position_amt, pos.entry_price, pos.unrealized_profit
            );
        }
    }
}

fn log_spot_balances(balances: &[BinanceSpotBalance]) {
    if balances.is_empty() {
        info!("Binance Spot account initialized: no balances");
        return;
    }
    info!("Binance Spot account initialized: {} assets", balances.len());
    for bal in balances {
        let total_balance = bal.total_wallet_balance;
        let net_asset = bal.net_asset();
        if total_balance.abs() > 1e-8 || net_asset.abs() > 1e-8 {
            info!(
                "  Spot balance: {} total={:.8} net={:.8} (margin_free={:.8} margin_locked={:.8})",
                bal.asset, total_balance, net_asset, bal.cross_margin_free, bal.cross_margin_locked
            );
        }
    }
}

fn log_exposures(entries: &[ExposureEntry], price_map: &BTreeMap<String, PriceEntry>) {
    if entries.is_empty() {
        info!("非 USDT 资产敞口为空");
        return;
    }

    let mut sum_exposure_usdt = 0.0_f64;
    let mut rows: Vec<Vec<String>> = Vec::new();
    for entry in entries {
        let asset = entry.asset.to_uppercase();
        if asset == "USDT" {
            continue;
        }
        let sym = format!("{}USDT", asset);
        let mark = price_map.get(&sym).map(|p| p.mark_price).unwrap_or(0.0);
        let spot_usdt = entry.spot_total_wallet * mark;
        let um_usdt = entry.um_net_position * mark;
        let exposure_usdt = spot_usdt + um_usdt;
        sum_exposure_usdt += exposure_usdt;

        rows.push(vec![
            entry.asset.clone(),
            fmt_decimal(entry.spot_total_wallet),
            fmt_decimal(spot_usdt),
            fmt_decimal(entry.um_net_position),
            fmt_decimal(um_usdt),
            fmt_decimal(entry.exposure),
            fmt_decimal(exposure_usdt),
        ]);
    }

    rows.push(vec![
        "TOTAL".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        fmt_decimal(sum_exposure_usdt),
    ]);

    let table = render_three_line_table(
        &["Asset", "SpotQty", "SpotUSDT", "UMNetQty", "UMNetUSDT", "ExposureQty", "ExposureUSDT"],
        &rows,
    );
    info!("现货+UM 敞口汇总\n{}", table);
}

fn log_exposure_summary(
    total_equity: f64,
    total_exposure: f64,
    total_position: f64,
    max_leverage: f64,
) {
    let leverage = if total_equity.abs() <= f64::EPSILON {
        0.0
    } else {
        total_position / total_equity
    };

    let leverage_cell = format!("{} / {}", fmt_decimal(leverage), fmt_decimal(max_leverage));
    let table = render_three_line_table(
        &["TotalEquity", "TotalExposure", "Leverage"],
        &[vec![fmt_decimal(total_equity), fmt_decimal(total_exposure), leverage_cell]],
    );
    info!("风险指标汇总\n{}", table);
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
    out.push_str(&build_row(headers.iter().map(|h| h.to_string()).collect::<Vec<String>>(), &widths));
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

/// 从账户快照中收集需要的价格符号
fn collect_price_symbols(
    set: &mut BTreeSet<String>,
    um_snapshot: &BinanceUmAccountSnapshot,
    spot_snapshot: &BinanceSpotBalanceSnapshot,
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

/// 打印价格表日志
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

/// 分发 ExecutionReport 到相应的策略
fn dispatch_execution_report(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    report: &ExecutionReportMsg,
) {
    let order_id = report.client_order_id;
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    let mut matched = false;

    for strategy_id in strategy_ids {
        let mut mgr = strategy_mgr.borrow_mut();
        if let Some(mut strategy) = mgr.take(strategy_id) {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match report.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update_with_record(report, None);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update_with_record(report, None);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            report.execution_type(),
                            report.symbol,
                            report.client_order_id,
                            report.order_id
                        );
                        strategy.apply_order_update_with_record(report, None);
                    }
                    _ => {
                        log::error!(
                            "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                            report.execution_type(),
                            report.symbol,
                            report.client_order_id,
                            report.order_id
                        );
                    }
                }
            }
            if strategy.is_active() {
                mgr.insert(strategy);
            }
        }
    }

    if !matched {
        let expected_strategy_id = (order_id >> 32) as i32;
        debug!(
            "executionReport unmatched: sym={} cli_id={} ord_id={} status={} expect_strategy={}",
            report.symbol,
            report.client_order_id,
            report.order_id,
            report.order_status,
            expected_strategy_id
        );
    }
}

/// 分发 OrderTradeUpdate 到相应的策略
fn dispatch_order_trade_update(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    update: &OrderTradeUpdateMsg,
) {
    let order_id: i64 = update.client_order_id;
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    let mut matched = false;

    for strategy_id in strategy_ids {
        let mut mgr = strategy_mgr.borrow_mut();
        if let Some(mut strategy) = mgr.take(strategy_id) {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match update.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update_with_record(update, None);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update_with_record(update, None);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            update.symbol,
                            update.client_order_id,
                            update.order_id
                        );
                        strategy.apply_order_update_with_record(update, None);
                    }
                    _ => {
                        log::error!(
                            "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            update.symbol,
                            update.client_order_id,
                            update.order_id
                        );
                    }
                }
            }
            if strategy.is_active() {
                mgr.insert(strategy);
            }
        }
    }

    if !matched {
        debug!(
            "orderTradeUpdate not matched to any strategy: client_order_id={} client_order_id_str='{}'",
            order_id,
            update.client_order_id_str
        );
    }
}
