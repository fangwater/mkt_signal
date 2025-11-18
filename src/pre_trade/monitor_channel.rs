use anyhow::{anyhow, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::common::account_msg::{
    get_event_type as get_account_event_type, AccountEventType, AccountPositionMsg,
    AccountUpdateBalanceMsg, AccountUpdateFlushMsg, AccountUpdatePositionMsg, BalanceUpdateMsg,
    ExecutionReportMsg, OrderTradeUpdateMsg,
};
use crate::pre_trade::binance_pm_spot_manager::{BinancePmSpotAccountManager, BinanceSpotBalance};
use crate::pre_trade::binance_pm_um_manager::{BinancePmUmAccountManager, BinanceUmPosition};
use crate::signal::common::{ExecutionType, TradingVenue};

const ACCOUNT_PAYLOAD: usize = 16_384;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";

// ==================== Helper Functions ====================

/// 从交易对中提取基础资产（如 BTCUSDT -> BTC）
fn extract_base_asset(symbol_upper: &str) -> Option<String> {
    const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
    for quote in QUOTES {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            return Some(symbol_upper[..symbol_upper.len() - quote.len()].to_string());
        }
    }
    None
}

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

use crate::common::min_qty_table::MinQtyTable;
use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::pre_trade::binance_pm_spot_manager::BinanceSpotBalanceSnapshot;
use crate::pre_trade::binance_pm_um_manager::BinanceUmAccountSnapshot;
use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::order_manager::OrderManager;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use crate::strategy::order_update::OrderUpdate;
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

// Thread-local 单例存储
thread_local! {
    static MONITOR_CHANNEL: RefCell<Option<MonitorChannelInner>> = RefCell::new(None);
}

/// MonitorChannel 单例访问器（零大小类型）
pub struct MonitorChannel;

/// MonitorChannel 内部实现，包含所有状态
struct MonitorChannelInner {
    /// 币安 合约资产管理器，基于统一账户 update 更新
    um_manager: Rc<RefCell<BinancePmUmAccountManager>>,
    /// 币安 现货资产管理器，基于统一账户 update 更新
    spot_manager: Rc<RefCell<BinancePmSpotAccountManager>>,
    /// 敞口管理器
    exposure_manager: Rc<RefCell<ExposureManager>>,
    /// 价格表
    price_table: Rc<RefCell<PriceTable>>,
    /// 交易对最小下单量/步进信息（spot/futures/margin）
    min_qty_table: Rc<MinQtyTable>,
    /// 策略管理器
    strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    /// 订单管理器，所有订单维护在其中，完全成交或者撤单会被移除
    order_manager: Rc<RefCell<OrderManager>>,
    /// 对冲残余量哈希表，key=(symbol, venue)，value=残余量
    hedge_residual_map: Rc<RefCell<HashMap<(String, TradingVenue), f64>>>,
}

impl MonitorChannel {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        MonitorChannel
    }

    /// 访问内部状态的辅助方法（内部使用）
    fn with_inner<F, R>(f: F) -> R
    where
        F: FnOnce(&MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL.with(|mc| {
            let mc_ref = mc.borrow();
            let inner = mc_ref.as_ref().expect("MonitorChannel not initialized");
            f(inner)
        })
    }

    /// 获取 min_qty_table 的引用
    pub fn min_qty_table(&self) -> Rc<MinQtyTable> {
        Self::with_inner(|inner| inner.min_qty_table.clone())
    }

    /// 获取 order_manager 的引用
    pub fn order_manager(&self) -> Rc<RefCell<OrderManager>> {
        Self::with_inner(|inner| inner.order_manager.clone())
    }

    /// 获取 price_table 的引用
    pub fn price_table(&self) -> Rc<RefCell<PriceTable>> {
        Self::with_inner(|inner| inner.price_table.clone())
    }

    /// 获取 strategy_mgr 的引用
    pub fn strategy_mgr(&self) -> Rc<RefCell<crate::strategy::StrategyManager>> {
        Self::with_inner(|inner| inner.strategy_mgr.clone())
    }

    /// 获取 um_manager 的引用
    pub fn um_manager(&self) -> Rc<RefCell<BinancePmUmAccountManager>> {
        Self::with_inner(|inner| inner.um_manager.clone())
    }

    /// 获取 spot_manager 的引用
    pub fn spot_manager(&self) -> Rc<RefCell<BinancePmSpotAccountManager>> {
        Self::with_inner(|inner| inner.spot_manager.clone())
    }

    /// 获取 exposure_manager 的引用
    pub fn exposure_manager(&self) -> Rc<RefCell<ExposureManager>> {
        Self::with_inner(|inner| inner.exposure_manager.clone())
    }

    /// 创建 Binance PM account monitor 实例并初始化 thread-local 单例
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
    /// - 将实例保存到 thread-local 单例
    ///
    /// # 参数
    /// - `strategy_mgr`: 策略管理器
    ///
    /// # 环境变量
    /// - `BINANCE_API_KEY`: Binance API Key（必需）
    /// - `BINANCE_API_SECRET`: Binance API Secret（必需）
    ///
    /// # 错误
    /// - API 凭证未设置
    /// - 初始账户快照获取失败
    pub async fn init_singleton(
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    ) -> Result<()> {
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
        let um_snapshot = match um_manager.init().await {
            Ok(snapshot) => {
                log_um_positions(&snapshot.positions);
                snapshot
            }
            Err(err) => {
                warn!("Failed to load initial Binance UM snapshot: {:#}", err);
                warn!("Starting with empty UM positions");
                BinanceUmAccountSnapshot {
                    positions: vec![],
                    fetched_at: chrono::Utc::now(),
                }
            }
        };

        // 初始化 Spot 现货管理器（使用相同的 PM 账户凭证）
        let spot_manager = BinancePmSpotAccountManager::new(
            rest_base,
            api_key,
            api_secret,
            recv_window_ms,
        );
        let spot_snapshot = match spot_manager.init().await {
            Ok(snapshot) => {
                log_spot_balances(&snapshot.balances);
                snapshot
            }
            Err(err) => {
                warn!("Failed to load initial Binance spot snapshot: {:#}", err);
                warn!("Starting with empty spot balances");
                BinanceSpotBalanceSnapshot {
                    balances: vec![],
                    fetched_at: chrono::Utc::now(),
                }
            }
        };

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
        }

        // 创建 ExposureManager（基于初始快照）
        let mut exposure_manager = ExposureManager::new(&um_snapshot, &spot_snapshot);
        {
            let table = price_table.borrow();
            let snap = table.snapshot();
            // 基于初始价格对总权益/总敞口进行 USDT 计价
            exposure_manager.revalue_with_prices(&snap);
            log_exposures(exposure_manager.exposures(), &snap);
            log_exposure_summary(
                exposure_manager.total_equity(),
                exposure_manager.total_abs_exposure(),
                exposure_manager.total_position(),
            );
            log_leverage_detail(
                exposure_manager.total_spot_value_usd(),
                exposure_manager.total_borrowed_usd(),
                exposure_manager.total_interest_usd(),
                exposure_manager.total_um_unrealized(),
                exposure_manager.total_equity(),
                exposure_manager.total_position(),
            );
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

        let service_name = "account_pubs/binance_pm".to_string();
        let node_name = "pre_trade_account_pubs_binance_pm".to_string();

        // 启动账户事件监听任务
        Self::spawn_listener(
            service_name,
            node_name,
            um_manager_rc.clone(),
            spot_manager_rc.clone(),
            exposure_manager.clone(),
            price_table.clone(),
            strategy_mgr.clone(),
        );

        // 启动衍生品价格监听任务（mark_price, index_price）
        Self::spawn_derivatives_listener(price_table.clone());

        // 创建内部实例并保存到 thread-local
        let inner = MonitorChannelInner {
            um_manager: um_manager_rc,
            spot_manager: spot_manager_rc,
            exposure_manager,
            price_table,
            min_qty_table,
            strategy_mgr,
            order_manager: Rc::new(RefCell::new(OrderManager::new())),
            hedge_residual_map: Rc::new(RefCell::new(HashMap::new())),
        };

        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        Ok(())
    }

    // 检查杠杆率是否超过配置阈值
    pub fn check_leverage(&self) -> Result<(), String> {
        Self::with_inner(|inner| {
            let limit = PreTradeParamsLoader::instance().max_leverage();
            if limit <= 0.0 {
                return Ok(());
            }

            // 获取必要的数据
            let (total_equity, total_position) = {
                let exposure_manager = inner.exposure_manager.borrow();
                let total_equity = exposure_manager.total_equity();
                let total_position = exposure_manager.total_position();
                (total_equity, total_position)
            };

            if total_equity <= f64::EPSILON {
                return Err("账户总权益近似为 0，无法计算杠杆率".to_string());
            }

            let leverage = total_position / total_equity;
            if leverage > limit {
                debug!(
                    "当前杠杆 {:.4} 超过阈值 {:.4} (仓位={:.6}, 权益={:.6})",
                    leverage, limit, total_position, total_equity
                );
                return Err(format!("杠杆率 {:.2} 超过限制 {:.2}", leverage, limit));
            }

            Ok(())
        })
    }

    /// 根据交易场所对齐订单量和价格
    /// 返回 (对齐后的数量, 对齐后的价格)
    pub fn align_order_by_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
    ) -> Result<(f64, f64), String> {
        Self::with_inner(|inner| {
            match venue {
                TradingVenue::BinanceUm | TradingVenue::BinanceMargin => {
                    TradingVenue::align_um_order(symbol, raw_qty, raw_price, &inner.min_qty_table)
                }
                TradingVenue::BinanceSwap => {
                    // TODO: 实现 BinanceSwap 的对齐逻辑
                    Err(format!("尚未实现 BinanceSwap 的订单对齐"))
                }
                TradingVenue::BinanceSpot => {
                    // TODO: 实现 BinanceSpot 的对齐逻辑
                    Err(format!("尚未实现 BinanceSpot 的订单对齐"))
                }
                TradingVenue::OkexSwap | TradingVenue::OkexSpot => {
                    // TODO: 实现 Okex 的对齐逻辑
                    Err(format!("尚未实现 Okex 的订单对齐"))
                }
            }
        })
    }

    /// 检查交易量是否满足最小要求
    /// 包括最小下单量和最小名义金额检查
    pub fn check_min_trading_requirements(
        &self,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
        price_hint: Option<f64>,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            // 1. 检查最小下单量
            let min_qty = match venue {
                TradingVenue::BinanceUm => inner.min_qty_table.futures_um_min_qty_by_symbol(symbol),
                TradingVenue::BinanceMargin => inner.min_qty_table.margin_min_qty_by_symbol(symbol),
                TradingVenue::BinanceSpot | TradingVenue::OkexSpot => {
                    inner.min_qty_table.spot_min_qty_by_symbol(symbol)
                }
                TradingVenue::BinanceSwap | TradingVenue::OkexSwap => {
                    // TODO: 根据实际情况添加 swap 的最小量获取
                    None
                }
            }
            .unwrap_or(0.0);

            if min_qty > 0.0 && qty + 1e-12 < min_qty {
                return Err(format!("交易量 {:.8} 小于最小下单量 {:.8}", qty, min_qty));
            }

            // 2. 检查最小名义金额（仅对 UM 合约）
            if venue == TradingVenue::BinanceUm {
                let min_notional = inner
                    .min_qty_table
                    .futures_um_min_notional_by_symbol(symbol)
                    .unwrap_or(0.0);

                if min_notional > 0.0 {
                    // 如果没有提供价格提示，尝试从价格表获取
                    let price = if let Some(p) = price_hint {
                        p
                    } else {
                        inner.price_table.borrow().mark_price(symbol).unwrap_or(0.0)
                    };

                    if price <= 0.0 {
                        return Err(format!("缺少 {} 的价格信息，无法验证名义金额", symbol));
                    }

                    let notional = price * qty;
                    if notional + 1e-8 < min_notional {
                        return Err(format!(
                            "名义金额 {:.8} 低于最小要求 {:.8} (价格={:.8} 数量={:.8})",
                            notional, min_notional, price, qty
                        ));
                    }
                }
            }

            Ok(())
        })
    }

    fn spawn_listener(
        service_name: String,
        node_name: String,
        um_manager: Rc<RefCell<BinancePmUmAccountManager>>,
        spot_manager: Rc<RefCell<BinancePmSpotAccountManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        price_table: Rc<RefCell<PriceTable>>,
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    ) {
        tokio::task::spawn_local(async move {
            let service_name_for_error = service_name.clone();

            let result: Result<()> = async move {
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
                                        spot_manager.borrow_mut().apply_account_position(
                                            &msg.asset,
                                            msg.free_balance,
                                            msg.locked_balance,
                                            msg.event_time,
                                        );
                                        refresh_exposures(&um_manager, &spot_manager, &exposure_manager, &price_table);
                                    }
                                }
                                AccountEventType::BalanceUpdate => {
                                    if let Ok(msg) = BalanceUpdateMsg::from_bytes(data) {
                                        spot_manager.borrow_mut().apply_balance_delta(&msg.asset, msg.delta, msg.event_time);
                                        refresh_exposures(&um_manager, &spot_manager, &exposure_manager, &price_table);
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
                                    if let Ok(_msg) = AccountUpdateFlushMsg::from_bytes(data) {
                                        refresh_exposures(&um_manager, &spot_manager, &exposure_manager, &price_table);
                                    }
                                }
                                // ExecutionReport 和 OrderTradeUpdate：直接处理
                                AccountEventType::ExecutionReport => {
                                    if let Ok(report) = ExecutionReportMsg::from_bytes(data) {
                                        debug!(
                                            "executionReport: sym={} cli_id={} ord_id={} status={}",
                                            report.symbol, report.client_order_id, report.order_id, report.order_status
                                        );
                                        dispatch_execution_report(&strategy_mgr, &report);
                                    }
                                }
                                AccountEventType::OrderTradeUpdate => {
                                    if let Ok(update) = OrderTradeUpdateMsg::from_bytes(data) {
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
            }
            .await;

            if let Err(err) = result {
                warn!(
                    "account listener {} exited: {err:?}",
                    service_name_for_error
                );
            }
        });
    }
    // ==================== 风控方法（从 RiskChecker 迁移） ====================

    /// 检查当前 symbol 的限价挂单数量
    pub fn check_pending_limit_order(&self, symbol: &str) -> Result<(), String> {
        Self::with_inner(|inner| {
            let max_pending_limit_orders = PreTradeParamsLoader::instance().max_pending_limit_orders();
            if max_pending_limit_orders <= 0 {
                return Ok(());
            }

            let symbol_upper = symbol.to_uppercase();
            let count = inner
                .order_manager
                .borrow()
                .get_symbol_pending_limit_order_count(&symbol_upper);

            if count >= max_pending_limit_orders {
                return Err(format!(
                    "symbol={} 当前限价挂单数={}，达到上限 {}",
                    symbol, count, max_pending_limit_orders
                ));
            }

            Ok(())
        })
    }

    /// 检查当前symbol的敞口是否超过总资产比例限制
    pub fn check_symbol_exposure(&self, symbol: &str) -> Result<(), String> {
        Self::with_inner(|inner| {
            let limit = PreTradeParamsLoader::instance().max_symbol_exposure_ratio();
            if limit <= 0.0 {
                return Ok(());
            }

            let symbol_upper = symbol.to_uppercase();
            let Some(base_asset) = extract_base_asset(&symbol_upper) else {
                return Err(format!(
                    "无法识别 symbol={} 的基础资产，无法校验敞口比例",
                    symbol
                ));
            };

            let (entry, total_equity) = {
                let exposure_manager = inner.exposure_manager.borrow();
                let entry = exposure_manager.exposure_for_asset(&base_asset).cloned();
                let total_equity = exposure_manager.total_equity();
                (entry, total_equity)
            };

            // 如果没有找到敞口信息，认为是首次开仓，当前敞口为0
            let net_exposure = if let Some(ref entry) = entry {
                entry.spot_total_wallet + entry.um_net_position
            } else {
                0.0
            };

            if total_equity <= f64::EPSILON {
                return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
            }

            let mark = if base_asset.eq_ignore_ascii_case("USDT") {
                1.0
            } else {
                let sym = format!("{}USDT", base_asset);
                let snap = inner.price_table.borrow().snapshot();
                snap.get(&sym).map(|e| e.mark_price).unwrap_or(0.0)
            };

            let exposure_usdt = if mark > 0.0 { net_exposure * mark } else { 0.0 };

            if mark == 0.0 && net_exposure != 0.0 {
                let ratio = net_exposure.abs() / total_equity;
                if ratio > limit {
                    debug!(
                        "资产 {} 敞口占比(数量) {:.4}% 超过阈值 {:.2}% (敞口qty={:.6}, 权益={:.6})",
                        base_asset,
                        ratio * 100.0,
                        limit * 100.0,
                        net_exposure,
                        total_equity
                    );
                    return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
                }
                return Ok(());
            }

            let ratio = exposure_usdt.abs() / total_equity;
            if ratio > limit {
                debug!(
                    "资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口USDT={:.6}, 权益={:.6})",
                    base_asset,
                    ratio * 100.0,
                    limit * 100.0,
                    exposure_usdt,
                    total_equity
                );
                return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
            }

            Ok(())
        })
    }

    /// 检查总敞口是否超过配置阈值
    pub fn check_total_exposure(&self) -> Result<(), String> {
        Self::with_inner(|inner| {
            let limit = PreTradeParamsLoader::instance().max_total_exposure_ratio();
            if limit <= 0.0 {
                return Ok(());
            }

            let (total_equity, exposures) = {
                let exposure_manager = inner.exposure_manager.borrow();
                let total_equity = exposure_manager.total_equity();
                let exposures = exposure_manager.exposures().to_vec();
                (total_equity, exposures)
            };

            if total_equity <= f64::EPSILON {
                return Err("账户总权益近似为 0，无法计算总敞口占比".to_string());
            }

            let snap = inner.price_table.borrow().snapshot();
            let mut abs_total_usdt = 0.0_f64;

            for e in exposures.iter() {
                let asset = e.asset.to_uppercase();
                if asset == "USDT" {
                    continue;
                }
                let sym = format!("{}USDT", asset);
                let mark = snap.get(&sym).map(|p| p.mark_price).unwrap_or(0.0);
                let exposure_usdt = (e.spot_total_wallet + e.um_net_position) * mark;
                abs_total_usdt += exposure_usdt.abs();
            }

            let ratio = abs_total_usdt / total_equity;
            if ratio > limit {
                debug!(
                    "总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口USDT={:.6}, 权益={:.6})",
                    ratio * 100.0,
                    limit * 100.0,
                    abs_total_usdt,
                    total_equity
                );
                return Err(format!(
                    "总敞口比例 {:.2}% 超过限制 {:.2}%",
                    ratio * 100.0,
                    limit * 100.0
                ));
            }

            Ok(())
        })
    }

    /// 检查最大持仓限制
    pub fn ensure_max_pos_u(
        &self,
        symbol: &str,
        additional_qty: f64,
        price_hint: f64,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let max_pos_u = PreTradeParamsLoader::instance().max_pos_u();
            if !(max_pos_u > 0.0) {
                panic!("max_pos_u not set!!");
            }

            let symbol_upper = symbol.to_uppercase();
            let base_asset = extract_base_asset(&symbol_upper).ok_or_else(|| {
                format!("无法识别 symbol={} 的基础资产，无法校验 max_pos_u", symbol)
            })?;

            let current_spot_qty = {
                let exposure_manager = inner.exposure_manager.borrow();
                exposure_manager
                    .exposure_for_asset(&base_asset)
                    .map(|entry| entry.spot_total_wallet)
                    .unwrap_or(0.0)
            };

            let base_upper = base_asset.to_uppercase();
            let mark_symbol = format!("{}USDT", base_upper);
            let price_from_table = {
                let table = inner.price_table.borrow();
                table.mark_price(&mark_symbol)
            };
            let price = price_from_table.or_else(|| {
                if price_hint > 0.0 {
                    Some(price_hint)
                } else {
                    None
                }
            });

            let Some(price) = price else {
                warn!("symbol={} 缺少 USDT 标记价格，无法校验 max_pos_u", symbol);
                return Err(format!(
                    "symbol={} 缺少价格信息，无法校验 max_pos_u",
                    symbol
                ));
            };

            let projected_qty = current_spot_qty + additional_qty;
            let current_usdt = current_spot_qty.abs() * price;
            let order_usdt = additional_qty.abs() * price;
            let projected_usdt = projected_qty.abs() * price;
            let limit_eps = 1e-6_f64;

            if projected_usdt > max_pos_u + limit_eps {
                warn!(
                    "symbol={} 当前现货={:.6}({:.4}USDT) 下单数量={:.6}({:.4}USDT) 预计现货={:.4}USDT 超过阈值 {:.4}USDT",
                    symbol,
                    current_spot_qty,
                    current_usdt,
                    additional_qty,
                    order_usdt,
                    projected_usdt,
                    max_pos_u
                );
                return Err(format!(
                    "symbol={} 预计现货持仓 {:.4}USDT 超过阈值 {:.4}USDT",
                    symbol, projected_usdt, max_pos_u
                ));
            }

            Ok(())
        })
    }

    // ==================== 对冲残余量管理方法 ====================

    /// 增加对冲残余量
    pub fn inc_hedge_residual(&self, symbol: String, venue: TradingVenue, delta: f64) {
        Self::with_inner(|inner| {
            if delta <= 1e-12 {
                return;
            }
            let mut map = inner.hedge_residual_map.borrow_mut();
            let key = (symbol.to_uppercase(), venue);
            let current = map.get(&key).copied().unwrap_or(0.0);
            let new_value = current + delta;
            map.insert(key.clone(), new_value);
            debug!(
                "对冲残余量增加: symbol={} venue={:?} delta={:.8} 当前值={:.8} -> {:.8}",
                key.0, venue, delta, current, new_value
            );
        })
    }

    /// 减少对冲残余量
    pub fn dec_hedge_residual(&self, symbol: String, venue: TradingVenue, delta: f64) -> f64 {
        Self::with_inner(|inner| {
            if delta <= 1e-12 {
                return 0.0;
            }
            let mut map = inner.hedge_residual_map.borrow_mut();
            let key = (symbol.to_uppercase(), venue);
            let current = map.get(&key).copied().unwrap_or(0.0);
            let actual_dec = delta.min(current);
            let new_value = (current - actual_dec).max(0.0);

            if new_value <= 1e-12 {
                map.remove(&key);
                debug!(
                    "对冲残余量清零: symbol={} venue={:?} 原值={:.8} 减少={:.8}",
                    key.0, venue, current, actual_dec
                );
            } else {
                map.insert(key.clone(), new_value);
                debug!(
                    "对冲残余量减少: symbol={} venue={:?} delta={:.8} 当前值={:.8} -> {:.8}",
                    key.0, venue, actual_dec, current, new_value
                );
            }

            actual_dec
        })
    }

    /// 查询对冲残余量
    pub fn get_hedge_residual(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| {
            let map = inner.hedge_residual_map.borrow();
            let key = (symbol.to_uppercase(), venue);
            map.get(&key).copied().unwrap_or(0.0)
        })
    }

    /// 清除指定 symbol 和 venue 的对冲残余量
    pub fn clear_hedge_residual(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| {
            let mut map = inner.hedge_residual_map.borrow_mut();
            let key = (symbol.to_uppercase(), venue);
            let removed = map.remove(&key).unwrap_or(0.0);
            if removed > 1e-12 {
                debug!(
                    "对冲残余量清除: symbol={} venue={:?} 清除量={:.8}",
                    key.0, venue, removed
                );
            }
            removed
        })
    }

    /// 获取指定交易对和交易场所的持仓数量（带符号）
    /// 返回持仓数量，正数表示多头，负数表示空头
    pub fn get_position_qty(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| {
            match venue {
                TradingVenue::BinanceUm => {
                    // 查询 UM 合约头寸（带符号）
                    if let Some(snapshot) = inner.um_manager.borrow().snapshot() {
                        snapshot
                            .positions
                            .iter()
                            .find(|p| p.symbol.eq_ignore_ascii_case(symbol))
                            .map(|p| p.position_amt)
                            .unwrap_or(0.0)
                    } else {
                        0.0
                    }
                }
                TradingVenue::BinanceMargin | TradingVenue::BinanceSpot => {
                    // 查询 Margin/Spot 余额（带符号）
                    // 需要从 symbol（如 "ILVUSDT"）中提取 base asset（如 "ILV"）
                    let base_asset = symbol
                        .trim_end_matches("USDT")
                        .trim_end_matches("USDC")
                        .trim_end_matches("BUSD")
                        .trim_end_matches("FDUSD");

                    if let Some(snapshot) = inner.spot_manager.borrow().snapshot() {
                        snapshot
                            .balances
                            .iter()
                            .find(|b| b.asset.eq_ignore_ascii_case(base_asset))
                            .map(|b| b.net_asset())
                            .unwrap_or(0.0)
                    } else {
                        0.0
                    }
                }
                _ => 0.0,
            }
        })
    }

    // ==================== 内部辅助方法 ====================

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

                info!(
                    "derivatives price stream subscribed: service={}",
                    DERIVATIVES_SERVICE
                );

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
                                        table.update_mark_price(
                                            &msg.symbol,
                                            msg.mark_price,
                                            msg.timestamp,
                                        );
                                    }
                                    Err(err) => warn!("parse mark price failed: {err:?}"),
                                },
                                MktMsgType::IndexPrice => match parse_index_price(&payload) {
                                    Ok(msg) => {
                                        let mut table = price_table.borrow_mut();
                                        table.update_index_price(
                                            &msg.symbol,
                                            msg.index_price,
                                            msg.timestamp,
                                        );
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

// ==================== Helper Functions ====================

/// 刷新敞口：重新计算并打印敞口信息
fn refresh_exposures(
    um_manager: &Rc<RefCell<BinancePmUmAccountManager>>,
    spot_manager: &Rc<RefCell<BinancePmSpotAccountManager>>,
    exposure_manager: &Rc<RefCell<ExposureManager>>,
    price_table: &Rc<RefCell<PriceTable>>,
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
            );
            log_leverage_detail(
                exposures.total_spot_value_usd(),
                exposures.total_borrowed_usd(),
                exposures.total_interest_usd(),
                exposures.total_um_unrealized(),
                exposures.total_equity(),
                exposures.total_position(),
            );
        }
    }
}

fn log_um_positions(positions: &[BinanceUmPosition]) {
    if positions.is_empty() {
        debug!("Binance UM account initialized: no open positions");
        return;
    }
    debug!(
        "Binance UM account initialized: {} positions",
        positions.len()
    );
    for pos in positions {
        if pos.position_amt.abs() > 1e-8 {
            debug!(
                "  UM position: {} amt={:.8} entry={:.4} upnl={:.4}",
                pos.symbol, pos.position_amt, pos.entry_price, pos.unrealized_profit
            );
        }
    }
}

fn log_spot_balances(balances: &[BinanceSpotBalance]) {
    if balances.is_empty() {
        debug!("Binance Spot account initialized: no balances");
        return;
    }
    debug!(
        "Binance Spot account initialized: {} assets",
        balances.len()
    );
    for bal in balances {
        let total_balance = bal.total_wallet_balance;
        let net_asset = bal.net_asset();
        if total_balance.abs() > 1e-8 || net_asset.abs() > 1e-8 {
            debug!(
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

fn log_exposure_summary(total_equity: f64, total_exposure: f64, total_position: f64) {
    let leverage = if total_equity.abs() <= f64::EPSILON {
        0.0
    } else {
        total_position / total_equity
    };

    let max_leverage = PreTradeParamsLoader::instance().max_leverage();
    let leverage_cell = format!("{} / {}", fmt_decimal(leverage), fmt_decimal(max_leverage));
    let table = render_three_line_table(
        &["TotalEquity", "TotalExposure", "Leverage"],
        &[vec![
            fmt_decimal(total_equity),
            fmt_decimal(total_exposure),
            leverage_cell,
        ]],
    );
    info!("风险指标汇总\n{}", table);
}

fn log_leverage_detail(
    total_spot_value: f64,
    total_borrowed: f64,
    total_interest: f64,
    total_um_unrealized: f64,
    total_equity: f64,
    total_position: f64,
) {
    let leverage = if total_equity.abs() <= f64::EPSILON {
        0.0
    } else {
        total_position / total_equity
    };

    let max_leverage = PreTradeParamsLoader::instance().max_leverage();
    let leverage_cell = format!("{} / {}", fmt_decimal(leverage), fmt_decimal(max_leverage));

    let table = render_three_line_table(
        &["TotalAsset", "Borrowed", "Interest", "UMUnrealized", "TotalEquity", "Leverage"],
        &[vec![
            fmt_decimal(total_spot_value),
            fmt_decimal(total_borrowed),
            fmt_decimal(total_interest),
            fmt_decimal(total_um_unrealized),
            fmt_decimal(total_equity),
            leverage_cell,
        ]],
    );
    info!("杠杆率详细信息\n{}", table);
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
                        strategy.apply_order_update_with_record(report);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update_with_record(report);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            report.execution_type(),
                            report.symbol,
                            report.client_order_id,
                            report.order_id
                        );
                        strategy.apply_order_update_with_record(report);
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
                        strategy.apply_order_update_with_record(update);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update_with_record(update);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            update.symbol,
                            update.client_order_id,
                            update.order_id
                        );
                        strategy.apply_order_update_with_record(update);
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
