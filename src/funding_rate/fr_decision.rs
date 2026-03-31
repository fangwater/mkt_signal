//! Funding Rate 决策模块（FR 单所版）
//!
//! 纯决策逻辑，不维护状态。接收状态作为参数，返回决策结果并发布信号。

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use serde::Serialize;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use super::common::{compute_spread_rate, Quote, ThresholdKey, VenuePair};
use super::factor_value_hub::FactorValueHub;
use super::funding_rate_factor::FundingRateFactor;
use super::mkt_channel::MktChannel;
use super::rate_fetcher::RateFetcher;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;
use super::tlen_threshold_loader;
use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::depth_pub::query_client::DepthQueryClient;
use crate::funding_rate::FundingRatePeriod;
use crate::market_maker::quote_plan_levels::{
    build_quote_plan_levels, QuotePlanLevel, QuotePlanLevelSpec,
};
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::arb_signal::{
    ArbBackwardQueryMsg, ArbCancelCandidateQueryMsg, ArbCancelTriggerCtx,
};
use crate::signal::cancel_signal::{ArbCancelCtx, ArbCancelReason};
use crate::signal::common::{
    align_price_ceil, align_price_floor, SignalBytes, TradingLeg, TradingVenue,
};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::symbol_match::normalize_symbol_for_whitelist;

// ========== 线程本地单例 ==========

thread_local! {
    static FR_DECISION: OnceCell<RefCell<FrDecision>> = OnceCell::new();
}

fn min_qty_symbol_key(venue: TradingVenue, trade_symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => trade_symbol
            .to_uppercase()
            .replace("-SWAP", "")
            .replace('-', ""),
        TradingVenue::GateMargin | TradingVenue::GateFutures => trade_symbol
            .to_uppercase()
            .replace('_', "")
            .replace('-', ""),
        _ => trade_symbol.to_uppercase(),
    }
}

fn venue_qty_is_contracts(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures | TradingVenue::GateFutures
    )
}

fn contract_qty_multiplier(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol_key: &str,
) -> Option<f64> {
    match venue {
        TradingVenue::BinanceFutures => Some(1.0),
        TradingVenue::OkexFutures | TradingVenue::GateFutures => table
            .contract_multiplier_opt(symbol_key)
            .filter(|v| v.is_finite() && *v > 0.0),
        _ => Some(1.0),
    }
}

fn base_step_size(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    trade_symbol: &str,
) -> (Option<f64>, Option<f64>) {
    let symbol_key = min_qty_symbol_key(venue, trade_symbol);
    let step = table.step_size(&symbol_key);
    let min_qty = table.min_qty(&symbol_key);

    if venue_qty_is_contracts(venue) {
        let mult = contract_qty_multiplier(table, venue, &symbol_key);
        let step_base = step.zip(mult).map(|(s, m)| s * m);
        let min_qty_base = min_qty.zip(mult).map(|(q, m)| q * m);
        (step_base, min_qty_base)
    } else {
        (step, min_qty)
    }
}

// ========== 配置常量 ==========

/// 默认信号发布频道名称（发往 pre_trade）
/// 对应 pre_trade/signal_channel.rs 中的订阅频道
pub const DEFAULT_SIGNAL_CHANNEL: &str = "trade_signal";

/// 默认反向订阅频道名称（来自 pre_trade 的查询反馈）
/// 对应 pre_trade/signal_channel.rs 中的 DEFAULT_BACKWARD_CHANNEL
pub const DEFAULT_BACKWARD_CHANNEL: &str = "trade_query";

const DEFAULT_PNLU_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_PNLU_REDIS_PORT: u16 = 6379;
const DEFAULT_PNLU_REDIS_DB: i64 = 0;
const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
const PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
const TARGET_FACTOR_KEY_PREFIX: &str = TARGET_FACTOR_NAME;

#[derive(Debug, Clone)]
struct FrOpenQuotePlan {
    side: Side,
    inner_price: f64,
    outer_price: f64,
    price_tick: f64,
    qty_tick: f64,
    levels: Vec<QuotePlanLevel>,
}

fn build_fr_level_specs(
    side: Side,
    inner_price: f64,
    volatility: f64,
    level_count: usize,
) -> Vec<QuotePlanLevelSpec> {
    if level_count == 0 || !inner_price.is_finite() || inner_price <= 0.0 {
        return Vec::new();
    }
    if !volatility.is_finite() || volatility < 0.0 {
        return Vec::new();
    }
    if level_count == 1 || volatility <= 0.0 {
        return vec![QuotePlanLevelSpec {
            side,
            side_level_index: 1,
            offset: 0.0,
            base_price: inner_price,
        }];
    }

    let step = volatility / (level_count - 1) as f64;
    (0..level_count)
        .map(|idx| QuotePlanLevelSpec {
            side,
            side_level_index: idx + 1,
            offset: step * idx as f64,
            base_price: inner_price,
        })
        .collect()
}

fn build_fr_open_quote_plan(
    venue: TradingVenue,
    symbol: &str,
    quote: Quote,
    order_amount_u: f64,
    orders_per_round: u32,
    side: Side,
    volatility: f64,
    table: &VenueMinQtyTable,
) -> Result<FrOpenQuotePlan, String> {
    if symbol.trim().is_empty() {
        return Err("symbol is empty".to_string());
    }
    if quote.bid <= 0.0 || quote.ask <= 0.0 || quote.bid >= quote.ask {
        return Err(format!(
            "invalid quote bid={} ask={} symbol={}",
            quote.bid, quote.ask, symbol
        ));
    }
    if !(order_amount_u.is_finite() && order_amount_u > 0.0) {
        return Err(format!(
            "invalid order_amount_u={} (must be finite and >0)",
            order_amount_u
        ));
    }
    if orders_per_round == 0 {
        return Err("orders_per_round must be > 0".to_string());
    }
    if !volatility.is_finite() || volatility < 0.0 {
        return Err(format!(
            "invalid volatility symbol={} volatility={}",
            symbol, volatility
        ));
    }

    let inner_price = match side {
        Side::Buy => quote.bid,
        Side::Sell => quote.ask,
    };
    let outer_price = match side {
        Side::Buy => inner_price * (1.0 - volatility),
        Side::Sell => inner_price * (1.0 + volatility),
    };
    let specs = build_fr_level_specs(side, inner_price, volatility, orders_per_round as usize);
    if specs.is_empty() {
        return Err(format!(
            "empty fr level specs symbol={} side={:?} inner={:.8} volatility={:.8} levels={}",
            symbol, side, inner_price, volatility, orders_per_round
        ));
    }

    let (price_tick, qty_tick, levels) =
        build_quote_plan_levels(venue, symbol, order_amount_u, &specs, table)?;
    if levels.is_empty() {
        return Err(format!(
            "empty levels after alignment symbol={} side={:?} levels={}",
            symbol, side, orders_per_round
        ));
    }

    Ok(FrOpenQuotePlan {
        side,
        inner_price,
        outer_price,
        price_tick,
        qty_tick,
        levels,
    })
}

// ========== 无状态设计 ==========
// FrDecision 不维护任何状态，所有状态由外部（如 Engine）维护

// ========== 资费信号类型 ==========

/// 资费信号类型（内部使用）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FrSignalKind {
    ForwardOpen,   // 正套开仓
    ForwardClose,  // 正套平仓
    BackwardOpen,  // 反套开仓
    BackwardClose, // 反套平仓
}

impl FrSignalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ForwardOpen => "FwdOpen",
            Self::ForwardClose => "FwdClose",
            Self::BackwardOpen => "BwdOpen",
            Self::BackwardClose => "BwdClose",
        }
    }
}

fn resolve_fr_signal_from_flags(
    forward_open: bool,
    forward_close: bool,
    backward_open: bool,
    backward_close: bool,
) -> Option<FrSignalKind> {
    // 优先级规则1: forward_close 和 backward_open 冲突时，选择 backward_open
    if forward_close && backward_open {
        return Some(FrSignalKind::BackwardOpen);
    }

    // 优先级规则2: backward_close 和 forward_open 冲突时，选择 forward_open
    if backward_close && forward_open {
        return Some(FrSignalKind::ForwardOpen);
    }

    // 默认优先级: close > open
    if forward_close {
        return Some(FrSignalKind::ForwardClose);
    }
    if backward_close {
        return Some(FrSignalKind::BackwardClose);
    }
    if forward_open {
        return Some(FrSignalKind::ForwardOpen);
    }
    if backward_open {
        return Some(FrSignalKind::BackwardOpen);
    }

    None
}

// ========== 核心决策类 ==========

/// FrDecision - 套利决策单例（无状态）
///
/// 负责：
/// 1. 接收外部调用，根据传入的 SymbolState 进行决策
/// 2. 订阅来自 pre_trade 的反向查询（backward channel）
/// 3. 发布交易信号到 pre_trade（arb_open/arb_close/arb_cancel）
///
/// # 设计原则
/// - 无状态：不维护任何交易对状态，状态由外部管理
/// - 纯决策：只负责根据输入做决策并发送信号
///
/// # 设计对齐
/// - 与 pre_trade/signal_channel.rs 保持命名和结构一致
/// - signal_pub: 向下游发送信号（对应 pre_trade 的订阅）
/// - backward_sub: 接收下游查询（对应 pre_trade 的 backward_pub）
pub struct FrDecision {
    /// 信号发布器（发往 pre_trade）
    /// 对应 pre_trade/signal_channel.rs 订阅的频道
    signal_pub: SignalPublisher,

    /// 反向订阅器（来自 pre_trade 的查询反馈）
    /// 对应 pre_trade/signal_channel.rs 的 backward_pub
    backward_sub: GenericSignalSubscriber,

    /// 频道名称（用于日志）
    channel_name: String,

    /// IceOryx Node（用于创建服务）
    _node: Node<ipc::Service>,

    /// 因子订阅 / pnlu 回退，用于读取 rl_return_volatility
    factor_value_hub: FactorValueHub,

    /// 开仓波动边界缩放系数（实际边界=vol*open_scale）
    open_scale: f64,

    /// 每轮开/平仓档位数
    open_orders_per_round: u32,

    /// 开仓侧交易对过滤器（固定 venue）
    open_min_qty_table: VenueMinQtyTable,

    /// 开仓侧 depth query client（用于 tlen batch query）
    open_depth_query_client: DepthQueryClient,

    /// 对冲侧交易对过滤器（固定 venue）
    hedge_min_qty_table: VenueMinQtyTable,

    /// 当前运行的交易场所（开仓 / 对冲）
    venues: VenuePair,

    /// 单笔下单名义金额（单位：USDT）
    order_amount: f32,

    /// 开仓挂单最长挂单时间（微秒）
    open_order_ttl_us: i64,

    /// 对冲挂单（MM 模式）最长挂单时间（微秒）
    hedge_timeout_mm_us: i64,

    /// 对冲限价偏移（例如 0.0003 表示万分之 3）
    hedge_price_offset: f64,

    /// 对冲 request_seq 激进阈值（>=该值视为 aggressive，对冲不偏移但仍为 maker）
    hedge_aggressive_seq_threshold: u32,

    /// 信号冷却时间（微秒），默认 5 秒
    /// 每种信号类型（ArbOpen/ArbClose/ArbCancel）有独立的冷却时间
    /// 防止因多个事件同时触发（现货/期货盘口同时更新）导致重复发送相同类型的信号
    signal_cooldown_us: i64,

    /// 最后触发 ArbOpen 的时间戳（微秒）
    /// key: (open_venue, open_symbol, hedge_venue, hedge_symbol)
    last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,

    /// 最后触发 ArbClose 的时间戳（微秒）
    /// key: (open_venue, open_symbol, hedge_venue, hedge_symbol)
    last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,

    /// 最后触发 ArbCancel 的时间戳（微秒）
    /// key: (open_venue, open_symbol, hedge_venue, hedge_symbol)
    last_cancel_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,

    /// tlen 阈值表（按 open_symbol）
    tlen_thresholds: HashMap<String, f64>,

    /// 是否启用基于 tlen 的精确 ArbCancel
    enable_tlen_cancel: bool,

    /// tlen 撤单触发频率（毫秒）
    tlen_cancel_freq_ms: u64,

    /// 上次 tlen 阈值刷新时间
    last_tlen_threshold_reload_ts_us: i64,

    /// 上次 ArbCancelTrigger 发送时间
    last_cancel_trigger_ts_us: i64,
}

struct SignalTableEntry {
    symbol: String,
    pred_fr_pct: f64,
    fr_ma_pct: f64,
    pred_loan_pct: f64,
    cur_loan_pct: f64,
    fr_plus_pred_loan_pct: f64,
    ma_plus_cur_loan_pct: f64,
    fr_sig: &'static str,
    spread_sig: &'static str,
    final_sig: &'static str,
}

impl FrDecision {
    fn normalize_symbol_key(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    pub fn is_initialized() -> bool {
        FR_DECISION.with(|cell| cell.get().is_some())
    }

    /// 访问线程本地单例（只读）
    ///
    /// # 使用示例
    /// ```ignore
    /// FrDecision::with(|decision| {
    ///     decision.get_symbol_state("BTCUSDT");
    /// });
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&FrDecision) -> R,
    {
        FR_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("FrDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    /// 可变访问线程本地单例
    ///
    /// # 使用示例
    /// ```ignore
    /// FrDecision::with_mut(|decision| {
    ///     decision.on_funding_rate_change("BTCUSDT", 0.0001);
    /// });
    /// ```
    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut FrDecision) -> R,
    {
        FR_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("FrDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    pub fn try_with_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut FrDecision) -> R,
    {
        FR_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    /// 初始化单例（必须在 LocalSet 中调用）
    ///
    /// 自动启动：
    /// - spawn_backward_listener: 事件驱动的 backward 监听
    ///
    /// # 事件驱动架构
    /// 决策逻辑采用事件驱动模式，不使用定时轮询。
    /// 调用方应在市场数据更新时主动调用 `make_combined_decision()`。
    pub async fn init_singleton(exchange: Exchange) -> Result<()> {
        let venues = super::common::venue_pair_for_exchange(exchange);
        let result: Result<()> = FR_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = Self::new_sync(venues)?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize FrDecision singleton"))?;
            info!("FrDecision singleton initialized, exchange={}", exchange);
            Ok(())
        });
        result?;

        // 异步加载 min_qty_table
        Self::refresh_min_qty_async(venues).await;

        // 启动 backward 监听任务（处理来自 pre_trade 的查询）
        Self::spawn_backward_listener();
        Self::spawn_tlen_threshold_loader();
        info!("FrDecision backward listener started");

        Ok(())
    }

    /// 创建新实例（私有，同步版本）
    fn new_sync(venues: VenuePair) -> Result<Self> {
        let node_name = NodeName::new("fr_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;

        // 1. 创建信号发布器（发往 pre_trade）
        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;
        info!(
            "FrDecision: signal publisher created on '{}'",
            DEFAULT_SIGNAL_CHANNEL
        );

        // 2. 订阅反向频道（来自 pre_trade 的查询反馈）
        let backward_sub = Self::create_subscriber(&node, DEFAULT_BACKWARD_CHANNEL)?;
        info!(
            "FrDecision: backward subscriber created on '{}'",
            DEFAULT_BACKWARD_CHANNEL
        );

        // min_qty_table 将在 init_singleton 中异步加载
        let open_min_qty_table = VenueMinQtyTable::new(venues.0);
        let hedge_min_qty_table = VenueMinQtyTable::new(venues.1);
        let open_depth_query_client = DepthQueryClient::new(venues.0)?;
        let pnlu_settings = RedisSettings {
            host: DEFAULT_PNLU_REDIS_HOST.to_string(),
            port: DEFAULT_PNLU_REDIS_PORT,
            db: DEFAULT_PNLU_REDIS_DB,
            username: None,
            password: None,
            prefix: None,
        };
        let factor_value_hub = FactorValueHub::new(
            &node,
            venues.0,
            venues.1,
            TARGET_FACTOR_NAME,
            TARGET_FACTOR_KEY_PREFIX,
            pnlu_settings,
            DEFAULT_PNLU_KEY_SUFFIX.to_string(),
            PNLU_MAX_AGE_SECS,
        )?;

        Ok(Self {
            signal_pub,
            backward_sub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            _node: node,
            factor_value_hub,
            open_scale: 1.0,
            open_orders_per_round: 1,
            open_min_qty_table,
            open_depth_query_client,
            hedge_min_qty_table,
            venues,
            order_amount: 100.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            hedge_aggressive_seq_threshold: 6,
            signal_cooldown_us: 5_000_000, // 默认 5 秒
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
            last_cancel_ts: Rc::new(RefCell::new(HashMap::new())),
            tlen_thresholds: HashMap::new(),
            enable_tlen_cancel: false,
            tlen_cancel_freq_ms: 3_000,
            last_tlen_threshold_reload_ts_us: 0,
            last_cancel_trigger_ts_us: 0,
        })
    }

    /// 异步刷新 min_qty_table
    async fn refresh_min_qty_async(venues: VenuePair) {
        let mut open_table = VenueMinQtyTable::new(venues.0);
        let mut hedge_table = VenueMinQtyTable::new(venues.1);

        let open_res = open_table.refresh().await;
        let hedge_res = hedge_table.refresh().await;

        Self::with_mut(|decision| {
            if open_res.is_ok() {
                decision.open_min_qty_table = open_table;
            }
            if hedge_res.is_ok() {
                decision.hedge_min_qty_table = hedge_table;
            }
        });

        match open_res {
            Ok(_) => info!("FrDecision: open venue min_qty_table loaded, venue={:?}", venues.0),
            Err(err) => warn!(
                "FrDecision: failed to refresh open venue filters for {:?}, price_tick may be zero: {err:#}",
                venues.0
            ),
        }
        match hedge_res {
            Ok(_) => info!(
                "FrDecision: hedge venue min_qty_table loaded, venue={:?}",
                venues.1
            ),
            Err(err) => warn!(
                "FrDecision: failed to refresh hedge venue filters for {:?}, price_tick may be zero: {err:#}",
                venues.1
            ),
        }
    }

    /// 创建订阅器（helper）
    fn create_subscriber(
        node: &Node<ipc::Service>,
        channel_name: &str,
    ) -> Result<GenericSignalSubscriber> {
        let service_name = build_service_name(&format!("signal_pubs/{}", channel_name));
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(GenericSignalSubscriber::Size4K(subscriber))
    }

    // ========== 外部主动触发接口 ==========

    /// 联合信号决策：同时检查资费和价差因子
    ///
    /// # 决策流程
    /// 1. 优先检查 cancel 信号（只和价差有关）
    ///    - 如果满足 cancel 条件，检查 cancel 信号冷却
    ///    - 如果在冷却期内，跳过 cancel 信号
    ///    - 否则发送 cancel 信号并更新 cancel 冷却时间戳
    /// 2. 获取资费信号
    /// 3. 如果资费没有信号，返回 None
    /// 4. 如果资费有信号，验证对应的价差 satisfy
    /// 5. 只有资费和价差同时满足时才确定最终信号（Open/Close）
    /// 6. 检查对应信号类型的冷却
    /// 7. 发送信号并更新对应类型的冷却时间戳
    ///
    /// # 冷却机制
    /// - ArbOpen、ArbClose、ArbCancel 三种信号有**独立的**冷却时间
    /// - 每种信号发送后 5 秒内不能再发送同类型信号
    /// - 但不同类型信号之间互不影响
    ///
    /// # 参数
    /// - `open_symbol`: 开仓侧交易对
    /// - `hedge_symbol`: 对冲侧交易对
    /// - `open_venue`: 开仓侧交易所
    /// - `hedge_venue`: 对冲侧交易所
    ///
    /// # 返回
    /// 如果需要发送信号，返回 Ok(Some(signal_type))；否则返回 Ok(None)
    ///
    /// # 事件驱动架构
    /// 该方法应在市场数据更新时被主动调用（事件驱动），而非定时轮询。
    /// 触发时机：MktChannel 收到盘口更新、RateFetcher 更新资费率等
    pub fn make_combined_decision(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        let spread_factor = SpreadFactor::instance();
        let now = get_timestamp_us();
        let symbol_list = SymbolList::instance();
        let open_symbol_key = Self::normalize_symbol_key(open_symbol);
        let hedge_symbol_key = Self::normalize_symbol_key(hedge_symbol);
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "FrDecision decision start open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
        }

        // 步骤1: 优先检查 cancel 信号（只和价差有关，不需要资费）
        let forward_cancel = spread_factor.satisfy_forward_cancel(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        );
        let backward_cancel = spread_factor.satisfy_backward_cancel(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        );
        if forward_cancel || backward_cancel {
            // 检查 cancel 信号冷却
            if self.check_signal_cooldown(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                now,
                &SignalType::ArbCancel,
            ) {
                log::debug!(
                    "FrDecision cancel suppressed by cooldown open={} hedge={} open_venue={:?} hedge_venue={:?} forward_cancel={} backward_cancel={}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue,
                    forward_cancel,
                    backward_cancel
                );
                // 在冷却期内，不返回，继续检查 open/close 信号
            } else {
                log::debug!(
                    "FrDecision cancel triggered open={} hedge={} open_venue={:?} hedge_venue={:?} forward_cancel={} backward_cancel={}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue,
                    forward_cancel,
                    backward_cancel
                );
                // 发送 cancel 信号
                self.emit_signals(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                    SignalType::ArbCancel,
                    None,
                )?;
                // 更新 cancel 冷却时间戳
                self.update_last_signal_ts(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                    now,
                    &SignalType::ArbCancel,
                );
                return Ok(Some(SignalType::ArbCancel));
            }
        }

        let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
        if in_dump {
            log::debug!(
                "FrDecision symbol in dump list open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
        }
        let rate_ready = RateFetcher::is_initial_ready(hedge_venue);
        let rate_fetcher = RateFetcher::instance();
        let loan_required = matches!(
            hedge_venue,
            TradingVenue::BinanceMargin
                | TradingVenue::BinanceFutures
                | TradingVenue::OkexMargin
                | TradingVenue::OkexFutures
                | TradingVenue::GateMargin
                | TradingVenue::GateFutures
        );
        let has_predict_fr = rate_fetcher
            .get_predicted_funding_rate(hedge_symbol_key.as_str(), hedge_venue)
            .is_some();
        let has_predict_loan = if loan_required {
            rate_fetcher
                .get_predict_loan_rate(hedge_symbol_key.as_str(), hedge_venue)
                .is_some()
        } else {
            true
        };
        let open_inputs_ready = has_predict_fr && has_predict_loan;
        let spread_close_signal = || {
            if spread_factor.satisfy_forward_close(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            ) {
                Some(FrSignalKind::ForwardClose)
            } else if spread_factor.satisfy_backward_close(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            ) {
                Some(FrSignalKind::BackwardClose)
            } else {
                None
            }
        };

        // 步骤2: 获取资费信号
        let fr_signal = if in_dump {
            // dump 列表：强制视为 close 信号，只要价差满足 close 条件即可
            let signal = spread_close_signal();
            if signal.is_none() {
                log::debug!(
                    "FrDecision dump close not satisfied open={} hedge={} open_venue={:?} hedge_venue={:?}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue
                );
            }
            signal
        } else if !rate_ready && !open_inputs_ready {
            // 费率未全局就绪且该 symbol 开仓输入不完整：降级为仅允许基于价差触发 close。
            let signal = spread_close_signal();
            if signal.is_none() {
                log::debug!(
                    "FrDecision rate not ready and open inputs incomplete, no spread-close signal open={} hedge={} open_venue={:?} hedge_venue={:?} has_predict_fr={} has_predict_loan={}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue,
                    has_predict_fr,
                    has_predict_loan
                );
            } else {
                log::debug!(
                    "FrDecision rate not ready and open inputs incomplete, allow spread-close open={} hedge={} open_venue={:?} hedge_venue={:?} has_predict_fr={} has_predict_loan={}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue,
                    has_predict_fr,
                    has_predict_loan
                );
            }
            signal
        } else {
            if !rate_ready {
                log::debug!(
                    "FrDecision rate not fully ready, but symbol open inputs are ready; run full FR decision open={} hedge={} open_venue={:?} hedge_venue={:?} has_predict_fr={} has_predict_loan={}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue,
                    has_predict_fr,
                    has_predict_loan
                );
            }
            self.get_funding_rate_signal(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                hedge_venue,
            )?
        };

        // 步骤3: 如果没有可执行信号，返回 None
        let fr_signal = match fr_signal {
            Some(s) => s,
            None => {
                log::debug!(
                    "FrDecision no effective signal open={} hedge={} open_venue={:?} hedge_venue={:?}",
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue
                );
                return Ok(None);
            }
        };

        // 步骤4: 根据资费信号验证对应的价差 satisfy
        let final_signal = match fr_signal {
            FrSignalKind::ForwardOpen => {
                if !open_inputs_ready {
                    log::debug!(
                        "FrDecision forward_open blocked by incomplete open inputs open={} hedge={} open_venue={:?} hedge_venue={:?} has_predict_fr={} has_predict_loan={}",
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue,
                        has_predict_fr,
                        has_predict_loan
                    );
                    return Ok(None);
                }
                if in_dump {
                    log::debug!(
                        "FrDecision forward_open blocked by dump list open={} hedge={} open_venue={:?} hedge_venue={:?}",
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue
                    );
                    return Ok(None);
                }
                let spread_ok = spread_factor.satisfy_forward_open(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                );
                let in_trade_list = symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str());
                if spread_ok && in_trade_list {
                    Some(SignalType::ArbOpen)
                } else {
                    log::debug!(
                        "FrDecision forward_open rejected spread_ok={} in_fwd_list={} open={} hedge={} open_venue={:?} hedge_venue={:?}",
                        spread_ok,
                        in_trade_list,
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue
                    );
                    None
                }
            }
            FrSignalKind::ForwardClose => {
                let spread_ok = spread_factor.satisfy_forward_close(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                );
                if spread_ok {
                    Some(SignalType::ArbClose)
                } else {
                    log::debug!(
                        "FrDecision forward_close rejected spread_ok={} open={} hedge={} open_venue={:?} hedge_venue={:?}",
                        spread_ok,
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue
                    );
                    None
                }
            }
            FrSignalKind::BackwardOpen => {
                if !open_inputs_ready {
                    log::debug!(
                        "FrDecision backward_open blocked by incomplete open inputs open={} hedge={} open_venue={:?} hedge_venue={:?} has_predict_fr={} has_predict_loan={}",
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue,
                        has_predict_fr,
                        has_predict_loan
                    );
                    return Ok(None);
                }
                if in_dump {
                    log::debug!(
                        "FrDecision backward_open blocked by dump list open={} hedge={} open_venue={:?} hedge_venue={:?}",
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue
                    );
                    return Ok(None);
                }
                let spread_ok = spread_factor.satisfy_backward_open(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                );
                let in_trade_list = symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str());
                if spread_ok && in_trade_list {
                    Some(SignalType::ArbOpen)
                } else {
                    log::debug!(
                        "FrDecision backward_open rejected spread_ok={} in_bwd_list={} open={} hedge={} open_venue={:?} hedge_venue={:?}",
                        spread_ok,
                        in_trade_list,
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue
                    );
                    None
                }
            }
            FrSignalKind::BackwardClose => {
                let spread_ok = spread_factor.satisfy_backward_close(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                );
                if spread_ok {
                    Some(SignalType::ArbClose)
                } else {
                    log::debug!(
                        "FrDecision backward_close rejected spread_ok={} open={} hedge={} open_venue={:?} hedge_venue={:?}",
                        spread_ok,
                        open_symbol_key,
                        hedge_symbol_key,
                        open_venue,
                        hedge_venue
                    );
                    None
                }
            }
        };

        // 步骤5: 如果价差不满足，返回 None
        let final_signal = match final_signal {
            Some(s) => s,
            None => return Ok(None),
        };

        let signal_side = Self::side_from_fr_signal(fr_signal);

        // 步骤6: 检查对应信号类型的冷却
        if self.check_signal_cooldown(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            now,
            &final_signal,
        ) {
            log::debug!(
                "FrDecision {:?} suppressed by cooldown open={} hedge={} open_venue={:?} hedge_venue={:?}",
                final_signal,
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
            return Ok(None);
        }

        // 步骤7: 发送信号
        self.emit_signals(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            final_signal.clone(),
            Some(signal_side),
        )?;

        // 步骤8: 更新冷却时间戳
        self.update_last_signal_ts(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            now,
            &final_signal,
        );

        Ok(Some(final_signal))
    }

    /// 获取资费因子信号
    ///
    /// 通过查询 FundingRateFactor 的 satisfy 方法判断资费信号
    ///
    /// # 优先级规则
    /// 1. 如果同时满足 forward_close 和 backward_open，选择 backward_open
    /// 2. 如果同时满足 backward_close 和 forward_open，选择 forward_open
    /// 3. 否则按优先级：close > open
    pub fn evaluate_funding_rate_signal(
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Result<Option<FrSignalKind>> {
        let fr_factor = FundingRateFactor::instance();
        let rate_fetcher = RateFetcher::instance();

        // 从 RateFetcher 获取该 symbol 的周期
        let period = rate_fetcher.get_period(hedge_symbol, hedge_venue);

        // 检查所有条件
        let forward_open = fr_factor.satisfy_forward_open(hedge_symbol, period, hedge_venue);
        let forward_close = fr_factor.satisfy_forward_close(hedge_symbol, period, hedge_venue);
        let backward_open = fr_factor.satisfy_backward_open(hedge_symbol, period, hedge_venue);
        let backward_close = fr_factor.satisfy_backward_close(hedge_symbol, period, hedge_venue);
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "FrDecision funding-rate flags hedge={} venue={:?} period={:?} fwd_open={} fwd_close={} bwd_open={} bwd_close={}",
                hedge_symbol,
                hedge_venue,
                period,
                forward_open,
                forward_close,
                backward_open,
                backward_close
            );
        }

        let signal = resolve_fr_signal_from_flags(
            forward_open,
            forward_close,
            backward_open,
            backward_close,
        );
        if let Some(signal) = signal {
            log::debug!(
                "FrDecision funding-rate signal={} hedge={} venue={:?}",
                signal.as_str(),
                hedge_symbol,
                hedge_venue
            );
        } else {
            log::debug!(
                "FrDecision no funding-rate condition met hedge={} venue={:?}",
                hedge_symbol,
                hedge_venue
            );
        }
        Ok(signal)
    }

    fn get_funding_rate_signal(
        &self,
        _open_symbol: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Result<Option<FrSignalKind>> {
        Self::evaluate_funding_rate_signal(hedge_symbol, hedge_venue)
    }

    /// 处理反向查询（来自 pre_trade 的 backward channel）
    fn handle_backward_query(&mut self, data: Bytes) {
        let query = match ArbBackwardQueryMsg::from_bytes(data) {
            Ok(q) => q,
            Err(err) => {
                warn!("FrDecision: 解析 backward query 失败: {err}");
                return;
            }
        };
        match query {
            ArbBackwardQueryMsg::Hedge(query) => self.handle_hedge_query(query),
            ArbBackwardQueryMsg::CancelCandidates(query) => {
                self.handle_arb_cancel_candidate_query(query)
            }
        }
    }

    fn handle_hedge_query(&mut self, query: ArbHedgeSignalQueryMsg) {
        let Some(side) = query.get_side() else {
            warn!("FrDecision: hedge query side 无效: {}", query.hedge_side);
            return;
        };

        let Some(hedge_venue) = TradingVenue::from_u8(query.hedging_venue) else {
            warn!(
                "FrDecision: hedge query venue 无效: {}",
                query.hedging_venue
            );
            return;
        };

        let hedge_symbol = query.get_hedging_symbol();
        if hedge_symbol.is_empty() {
            warn!("FrDecision: hedge query 未携带对冲 symbol");
            return;
        }

        let open_symbol = query.get_opening_symbol();
        if open_symbol.is_empty() {
            warn!("FrDecision: hedge query 未携带开仓 symbol");
            return;
        }

        let Some(open_venue) = TradingVenue::from_u8(query.opening_venue) else {
            warn!(
                "FrDecision: hedge query opening venue 无效: {}",
                query.opening_venue
            );
            return;
        };

        let hedge_base_qty = query.hedge_base_qty;
        if hedge_base_qty <= 0.0 {
            warn!(
                "FrDecision: hedge query quantity <= 0 strategy_id={} qty={:.8}",
                query.strategy_id, hedge_base_qty
            );
            return;
        }

        let mkt_channel = MktChannel::instance();

        // 获取开仓侧行情
        let Some(open_quote) = mkt_channel.get_quote(&open_symbol, open_venue) else {
            warn!(
                "FrDecision: hedge query 开仓侧无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, open_symbol, open_venue
            );
            return;
        };

        // 获取对冲侧行情
        let Some(hedge_quote) = mkt_channel.get_quote(&hedge_symbol, hedge_venue) else {
            warn!(
                "FrDecision: hedge query 无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, hedge_symbol, hedge_venue
            );
            return;
        };

        let now = get_timestamp_us();
        let seq_threshold = self.hedge_aggressive_seq_threshold;
        let aggressive = query.request_seq >= seq_threshold;
        let target_factor_lookup = self.lookup_target_factor_value(&hedge_symbol, hedge_venue);
        let default_offset = self.hedge_price_offset.abs();
        let mut offset = default_offset;
        let mut offset_source = "config";
        let mut offset_note = String::new();
        let ready = target_factor_lookup.ready.unwrap_or(false);

        if ready {
            if let Some(value) = target_factor_lookup.target_factor_value {
                if value.is_finite() && value > 0.0 {
                    offset = value;
                    offset_source = "rl_return_volatility_factor";
                } else {
                    offset_note = "invalid_factor".to_string();
                }
            } else {
                offset_note = "missing_factor".to_string();
            }
        } else if target_factor_lookup.note == "ok" {
            offset_note = "not_ready".to_string();
        } else {
            offset_note = target_factor_lookup.note.clone();
        }

        if aggressive {
            offset = 0.0;
            offset_source = "aggressive";
            if offset_note.is_empty() {
                offset_note = "aggressive_override".to_string();
            } else {
                offset_note = format!("aggressive_override({})", offset_note);
            }
        }

        info!(
            "FrDecision: hedge query offset source={} key={} symbol={} venue={:?} norm_symbol={} ready={} factor={:?} factor_index={:?} ts_ms={:?} offset={:.6} default_offset={:.6} aggressive={} note={}",
            offset_source,
            target_factor_lookup.key,
            hedge_symbol,
            hedge_venue,
            target_factor_lookup.symbol_key,
            ready,
            target_factor_lookup.target_factor_value,
            target_factor_lookup.factor_index,
            target_factor_lookup.ts_ms,
            offset,
            default_offset,
            aggressive,
            offset_note
        );

        // 统一按 maker 限价对冲：aggressive 仅代表不使用偏移（直接挂在 bid/ask）
        let base_price = match side {
            Side::Buy => hedge_quote.bid,
            Side::Sell => hedge_quote.ask,
        };
        // ask 是卖价，用于 sell 挂单；bid 是买价，用于 buy 挂单
        let limit_price = if base_price > 0.0 {
            match side {
                Side::Buy => base_price * (1.0 - offset),
                Side::Sell => base_price * (1.0 + offset),
            }
        } else {
            0.0
        };

        if limit_price <= 0.0 {
            warn!(
                "FrDecision: hedge query limit_price 无效 strategy_id={} price={:.8}",
                query.strategy_id, limit_price
            );
            return;
        }

        let table = self.table_for(hedge_venue);
        let symbol_key = min_qty_symbol_key(hedge_venue, &hedge_symbol);
        let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let aligned_hedge_qty = self.convert_aligned_base_qty_to_open_venue_qty(
            hedge_venue,
            &hedge_symbol,
            limit_price,
            hedge_base_qty,
        );
        if aligned_hedge_qty <= 0.0 {
            warn!(
                "FrDecision: hedge query aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
                query.strategy_id,
                symbol_key,
                hedge_base_qty
            );
            return;
        }

        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);
        let from_key = self.build_hedge_from_key(now, query.request_seq, spread_rate);
        let mut ctx = ArbHedgeCtx::new_maker(
            query.strategy_id,
            query.client_order_id,
            side.to_u8(),
            aligned_hedge_qty,
            qty_tick,
            limit_price,
            price_tick,
            false,
            now + self.hedge_timeout_mm_us,
        );
        if ctx.hedge_qty_count() <= 0 || ctx.hedge_price_count() <= 0 {
            warn!(
                "FrDecision: hedge query qv invalid strategy_id={} qty={:.8} price={:.8}",
                query.strategy_id,
                ctx.hedge_qty_value(),
                ctx.hedge_price_value()
            );
            return;
        }
        // 设置开仓侧信息（从 query 获取 venue/symbol，盘口从 MktChannel 获取）
        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_symbol);
        // 设置对冲侧信息
        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
        ctx.set_hedging_symbol(&hedge_symbol);
        ctx.market_ts = now;
        ctx.price_offset = offset; // aggressive 时 offset=0.0
        ctx.spread_rate = spread_rate;
        ctx.set_from_key(from_key);

        let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());

        if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "FrDecision: 发送 hedge 信号失败 strategy_id={} err={:?}",
                query.strategy_id, err
            );
            return;
        }

        info!(
            "FrDecision: 回复 hedge query strategy_id={} hedge_symbol={} qty={:.6} side={:?} seq={} aggressive={} limit_price={:.8} offset={:.6} spread_rate={:.6} (maker)",
            query.strategy_id,
            hedge_symbol,
            ctx.hedge_qty_value(),
            side,
            query.request_seq,
            aggressive,
            ctx.hedge_price_value(),
            offset,
            spread_rate
        );
    }

    fn handle_arb_cancel_candidate_query(&mut self, query: ArbCancelCandidateQueryMsg) {
        if query.groups.is_empty() {
            return;
        }
        let now_us = get_timestamp_us();
        let mut cancel_sent = 0usize;
        let mut matched_symbols = 0usize;
        for group in query.groups {
            let open_symbol = group.get_symbol().to_uppercase();
            if open_symbol.is_empty() || group.items.is_empty() {
                continue;
            }
            let Some(threshold) = self.tlen_thresholds.get(&open_symbol).copied() else {
                log::debug!(
                    "FrDecision: missing tlen threshold symbol={}",
                    open_symbol
                );
                continue;
            };
            let tick_indices: Vec<i64> = group
                .items
                .iter()
                .map(|item| item.price_qv.get_count())
                .collect();
            let tlens = match self
                .open_depth_query_client
                .query_batch_tick_indices(&open_symbol, &tick_indices)
            {
                Ok(values) => values,
                Err(err) => {
                    warn!(
                        "FrDecision: ArbCancel tlen batch query failed symbol={} levels={} err={:#}",
                        open_symbol,
                        tick_indices.len(),
                        err
                    );
                    continue;
                }
            };
            let compared_preview = group
                .items
                .iter()
                .zip(tlens.iter().copied())
                .take(12)
                .map(|(item, tlen)| {
                    format!(
                        "{}@{}:{:.4}{}",
                        item.strategy_id,
                        item.price_qv.get_count(),
                        tlen,
                        if tlen < threshold { "<hit" } else { ">=skip" }
                    )
                })
                .collect::<Vec<_>>();
            let (min_tlen, max_tlen) = tlens.iter().copied().fold(
                (f64::INFINITY, f64::NEG_INFINITY),
                |(min_v, max_v), value| (min_v.min(value), max_v.max(value)),
            );
            let hedge_symbol =
                normalize_symbol_for_venue(&open_symbol, self.venues.1).to_ascii_uppercase();
            let mut matched_preview: Vec<String> = Vec::new();
            let mut group_cancel_sent = 0usize;
            for (item, tlen) in group.items.iter().zip(tlens.iter().copied()) {
                if tlen >= threshold {
                    continue;
                }
                if matched_preview.len() < 12 {
                    matched_preview.push(format!(
                        "{}@{}:{:.4}<{:.4}",
                        item.strategy_id,
                        item.price_qv.get_count(),
                        tlen,
                        threshold
                    ));
                }
                if let Err(err) = self.emit_cancel_signal_precise(
                    &open_symbol,
                    &hedge_symbol,
                    self.venues.0,
                    self.venues.1,
                    item.strategy_id,
                    tlen,
                    threshold,
                    now_us,
                ) {
                    warn!(
                        "FrDecision: emit precise ArbCancel failed symbol={} strategy_id={} err={:#}",
                        open_symbol,
                        item.strategy_id,
                        err
                    );
                    continue;
                }
                cancel_sent += 1;
                group_cancel_sent += 1;
            }
            info!(
                "FrDecision: ArbCancel tlen compare symbol={} trigger_ts={} candidates={} threshold={:.4} min_tlen={:.4} max_tlen={:.4} details={}",
                open_symbol,
                query.trigger_ts,
                tick_indices.len(),
                threshold,
                min_tlen,
                max_tlen,
                if compared_preview.is_empty() {
                    "-".to_string()
                } else {
                    compared_preview.join(",")
                }
            );
            if group_cancel_sent > 0 {
                matched_symbols += 1;
                info!(
                    "FrDecision: ArbCancel tlen hits symbol={} trigger_ts={} candidates={} matched={} threshold={:.4} strategies={}",
                    open_symbol,
                    query.trigger_ts,
                    tick_indices.len(),
                    group_cancel_sent,
                    threshold,
                    matched_preview.join(",")
                );
            }
        }
        if cancel_sent > 0 {
            info!(
                "FrDecision: ArbCancel candidate query processed trigger_ts={} matched_symbols={} cancels_sent={}",
                query.trigger_ts, matched_symbols, cancel_sent
            );
        }
    }

    // ========== 信号发布 ==========

    /// 发布交易信号到 pre_trade（支持多信号发送）
    ///
    /// # 参数
    /// - `open_symbol`: 开仓侧交易对
    /// - `hedge_symbol`: 对冲侧交易对
    /// - `open_venue`: 开仓侧交易所
    /// - `hedge_venue`: 对冲侧交易所
    /// - `signal_type`: 信号类型
    ///
    /// # 行为
    /// - ArbOpen / ArbClose: 基于 rl_return_volatility 生成多档 quote plan
    /// - ArbCancel: 发送单个信号
    ///
    /// # 设计对齐
    /// 与 pre_trade/signal_channel.rs 的 publish_backward 对应
    fn emit_signals(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        signal_type: SignalType,
        side: Option<Side>,
    ) -> Result<()> {
        // Use one batch timestamp for all grid offsets in this emit call.
        let batch_ts = get_timestamp_us();
        let mkt_channel = MktChannel::instance();

        // 获取盘口数据
        let spot_quote = mkt_channel.get_quote(spot_symbol, spot_venue);
        let futures_quote = mkt_channel.get_quote(futures_symbol, futures_venue);

        // 如果盘口无效，记录警告并返回
        if spot_quote.is_none() || futures_quote.is_none() {
            warn!(
                "FrDecision: quote unavailable open={} ({:?}) hedge={} ({:?})",
                spot_symbol,
                spot_quote.is_some(),
                futures_symbol,
                futures_quote.is_some()
            );
            return Ok(());
        }

        let spot_quote = spot_quote.unwrap();
        let futures_quote = futures_quote.unwrap();

        // 检查盘口数据的有效性：bid（买价）必须小于 ask（卖价）
        if spot_quote.bid >= spot_quote.ask {
            warn!(
                "FrDecision: invalid open quote bid={:.8} >= ask={:.8} for {}",
                spot_quote.bid, spot_quote.ask, spot_symbol
            );
            return Ok(());
        }
        if futures_quote.bid >= futures_quote.ask {
            warn!(
                "FrDecision: invalid hedge quote bid={:.8} >= ask={:.8} for {}",
                futures_quote.bid, futures_quote.ask, futures_symbol
            );
            return Ok(());
        }

        let spread_rate = compute_spread_rate(&spot_quote, &futures_quote);
        let from_key = self.build_from_key(batch_ts, futures_symbol, futures_venue, spread_rate);

        // 根据信号类型决定发送策略
        match signal_type {
            SignalType::ArbOpen | SignalType::ArbClose => {
                let side = match side {
                    Some(sig) => sig,
                    None => {
                        warn!("FrDecision: {:?} signal missing side info", signal_type);
                        return Ok(());
                    }
                };

                let raw_volatility = self
                    .lookup_target_factor_value(futures_symbol, futures_venue)
                    .target_factor_value
                    .filter(|v| v.is_finite() && *v >= 0.0)
                    .unwrap_or(0.0);
                let plan_volatility = if matches!(signal_type, SignalType::ArbOpen) {
                    (raw_volatility * self.open_scale).max(0.0)
                } else {
                    raw_volatility
                };
                let plan = match build_fr_open_quote_plan(
                    spot_venue,
                    spot_symbol,
                    spot_quote,
                    self.order_amount as f64,
                    self.open_orders_per_round,
                    side,
                    plan_volatility,
                    self.table_for(spot_venue),
                ) {
                    Ok(plan) => plan,
                    Err(err) => {
                        warn!(
                            "FrDecision: build quote plan failed open={} hedge={} side={:?} signal={:?} err={}",
                            spot_symbol, futures_symbol, side, signal_type, err
                        );
                        return Ok(());
                    }
                };

                for level in &plan.levels {
                    let ctx = self.build_open_context_from_level(
                        spot_symbol,
                        futures_symbol,
                        spot_venue,
                        futures_venue,
                        &spot_quote,
                        &futures_quote,
                        level,
                        batch_ts,
                        &from_key,
                    );

                    let signal =
                        TradeSignal::create(signal_type.clone(), batch_ts, 0.0, ctx.to_bytes());
                    self.signal_pub.publish(&signal.to_bytes())?;
                }

                info!(
                    "FrDecision: emitted {} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} open_scale={:.6} plan_vol={:.8} price_tick={:.8} qty_tick={:.8}",
                    plan.levels.len(),
                    signal_type,
                    self.channel_name,
                    spot_symbol,
                    futures_symbol,
                    plan.side,
                    plan.inner_price,
                    plan.outer_price,
                    raw_volatility,
                    self.open_scale,
                    plan_volatility,
                    plan.price_tick,
                    plan.qty_tick
                );
            }

            SignalType::ArbCancel => {
                // Cancel 信号：使用 ArbCancelCtx
                let ctx = self.build_cancel_context(
                    spot_symbol,
                    futures_symbol,
                    spot_venue,
                    futures_venue,
                    &spot_quote,
                    &futures_quote,
                    batch_ts,
                    &String::from_utf8_lossy(&from_key),
                    ArbCancelReason::Spread,
                    0,
                );

                let context = ctx.to_bytes();
                let signal = TradeSignal::create(signal_type.clone(), batch_ts, 0.0, context);
                let signal_bytes = signal.to_bytes();
                self.signal_pub.publish(&signal_bytes)?;
            }

            _ => {
                warn!("FrDecision: unsupported signal type: {:?}", signal_type);
            }
        }

        Ok(())
    }

    fn emit_cancel_signal_precise(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        strategy_id: i32,
        tlen: f64,
        threshold: f64,
        now_us: i64,
    ) -> Result<()> {
        let mkt_channel = MktChannel::instance();
        let spot_quote = match mkt_channel.get_quote(spot_symbol, spot_venue) {
            Some(q) => q,
            None => return Ok(()),
        };
        let futures_quote = match mkt_channel.get_quote(futures_symbol, futures_venue) {
            Some(q) => q,
            None => return Ok(()),
        };
        if spot_quote.bid >= spot_quote.ask || futures_quote.bid >= futures_quote.ask {
            return Ok(());
        }
        let spread_rate = compute_spread_rate(&spot_quote, &futures_quote);
        let volatility = self
            .lookup_target_factor_value(futures_symbol, futures_venue)
            .target_factor_value
            .filter(|v| v.is_finite());
        let from_key = format!(
            "{}:{}:0:0:{spread_rate:.6}:cancel_reason=tlen:tlen={tlen:.8}:tlen_thr={threshold:.8}",
            now_us, 0
        );
        let ctx = self.build_cancel_context(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
            &spot_quote,
            &futures_quote,
            now_us,
            &from_key,
            ArbCancelReason::Tlen,
            strategy_id,
        );
        let signal = TradeSignal::create(SignalType::ArbCancel, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        if let Some(_vol) = volatility {
            // keep parity with other decision paths by forcing factor lookup side effects/log readiness
        }
        Ok(())
    }

    fn build_from_key(
        &self,
        now: i64,
        futures_symbol: &str,
        futures_venue: TradingVenue,
        spread_rate: f64,
    ) -> Vec<u8> {
        let mkt_channel = MktChannel::instance();
        let rate_fetcher = RateFetcher::instance();
        let funding_ma = mkt_channel
            .get_funding_rate_mean(futures_symbol, futures_venue)
            .unwrap_or(0.0);
        let predicted = rate_fetcher
            .get_predicted_funding_rate(futures_symbol, futures_venue)
            .map(|(_, v)| v)
            .unwrap_or(0.0);
        let loan = rate_fetcher
            .get_predict_loan_rate(futures_symbol, futures_venue)
            .map(|(_, v)| v)
            .unwrap_or(0.0);

        format!("{now}:{funding_ma:.6}:{predicted:.6}:{loan:.6}:{spread_rate:.6}").into_bytes()
    }

    fn build_hedge_from_key(&self, now: i64, request_seq: u32, spread_rate: f64) -> Vec<u8> {
        format!("{now}:{request_seq}:{spread_rate:.6}").into_bytes()
    }

    fn lookup_target_factor_value(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> super::factor_value_hub::FactorValueLookupResult {
        self.factor_value_hub
            .lookup_target_factor_value(hedge_symbol, hedge_venue)
    }

    /// 构造 ArbOpen/ArbClose 信号上下文（基于 quote plan level）
    fn build_open_context_from_level(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        spot_quote: &Quote,
        futures_quote: &Quote,
        level: &QuotePlanLevel,
        now: i64,
        from_key: &[u8],
    ) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx::new();
        let spot_trade_symbol = normalize_symbol_for_venue(spot_symbol, spot_venue);
        let futures_trade_symbol = normalize_symbol_for_venue(futures_symbol, futures_venue);

        // opening_leg: 现货（主动腿）
        ctx.opening_leg =
            TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask, spot_quote.ts);
        ctx.set_opening_symbol(&spot_trade_symbol);

        // hedging_leg: 合约（对冲腿）
        ctx.hedging_leg = TradingLeg::new(
            futures_venue,
            futures_quote.bid,
            futures_quote.ask,
            futures_quote.ts,
        );
        ctx.set_hedging_symbol(&futures_trade_symbol);

        ctx.set_side(level.side);
        ctx.set_order_type(OrderType::Limit);
        let aligned_open_price = level.aligned_price;

        let base_qty = self.convert_order_amount_to_aligned_base_qty(
            spot_venue,
            &spot_trade_symbol,
            futures_venue,
            &futures_trade_symbol,
            spot_quote,
            futures_quote,
            aligned_open_price,
            level.side,
        );
        let aligned_open_qty = self.convert_aligned_base_qty_to_open_venue_qty(
            spot_venue,
            &spot_trade_symbol,
            aligned_open_price,
            base_qty,
        );

        let table = self.table_for(spot_venue);
        let symbol_key = min_qty_symbol_key(spot_venue, &spot_trade_symbol);

        let raw_price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let price_fallback = ctx.set_price_with_tick_floor(aligned_open_price, raw_price_tick);
        if raw_price_tick <= 0.0 || price_fallback {
            warn!(
                "FrDecision: missing price_tick for {:?} symbol_key={}, fallback to full-price tick",
                spot_venue, symbol_key
            );
        }
        if ctx.price_count() <= 0 {
            warn!(
                "FrDecision: invalid aligned_open_price for {:?} symbol_key={}, fallback to zero",
                spot_venue, symbol_key
            );
        }

        let raw_amount_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let amount_fallback = ctx.set_amount_with_tick_floor(aligned_open_qty, raw_amount_tick);
        if raw_amount_tick <= 0.0 || amount_fallback {
            warn!(
                "FrDecision: missing step_size for {:?} symbol_key={}, fallback to full-qty tick",
                spot_venue, symbol_key
            );
        }
        if ctx.amount_count() <= 0 {
            warn!(
                "FrDecision: invalid aligned_open_qty for {:?} symbol_key={}, fallback to zero",
                spot_venue, symbol_key
            );
        }

        ctx.exp_time = now + self.open_order_ttl_us;
        ctx.create_ts = now;
        ctx.price_offset = level.offset;
        ctx.spread_rate = compute_spread_rate(spot_quote, futures_quote);

        // hedge_timeout_us 根据 SpreadFactor 的 mode 决定
        let spread_factor = SpreadFactor::instance();
        let mode = spread_factor.get_mode();
        ctx.hedge_timeout_us = match mode {
            super::common::FactorMode::MT => 0,
            super::common::FactorMode::MM => self.hedge_timeout_mm_us,
        };

        ctx.set_from_key(from_key.to_vec());

        ctx
    }

    fn table_for(&self, venue: TradingVenue) -> &VenueMinQtyTable {
        if venue == self.venues.0 {
            &self.open_min_qty_table
        } else {
            &self.hedge_min_qty_table
        }
    }

    fn convert_aligned_base_qty_to_open_venue_qty(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        open_price: f64,
        aligned_base_qty: f64,
    ) -> f64 {
        if aligned_base_qty <= 0.0 || open_price <= 0.0 {
            return 0.0;
        }

        let table = self.table_for(open_venue);
        let symbol_key = min_qty_symbol_key(open_venue, open_symbol);

        let qty_multiplier = if venue_qty_is_contracts(open_venue) {
            let Some(multiplier) = contract_qty_multiplier(table, open_venue, &symbol_key) else {
                warn!(
                    "FrDecision: missing/invalid contract_multiplier for {:?} symbol_key={}, cannot convert aligned base qty to venue qty",
                    open_venue, symbol_key
                );
                return 0.0;
            };
            multiplier
        } else {
            1.0
        };

        let mut venue_qty = aligned_base_qty / qty_multiplier;
        let step = table.step_size(&symbol_key).unwrap_or(0.0);
        if step > 0.0 {
            venue_qty = align_price_floor(venue_qty, step);
            if venue_qty <= 0.0 {
                venue_qty = step;
            }
        }

        if let Some(min_qty) = table.min_qty(&symbol_key) {
            if min_qty > 0.0 && venue_qty < min_qty {
                venue_qty = min_qty;
            }
        }

        if open_venue.is_futures() {
            if let Some(min_notional) = table.min_notional(&symbol_key) {
                if min_notional > 0.0 {
                    let required_base_qty = min_notional / open_price;
                    let required_venue_qty = required_base_qty / qty_multiplier;
                    if venue_qty + 1e-12 < required_venue_qty {
                        venue_qty = if step > 0.0 {
                            align_price_ceil(required_venue_qty, step)
                        } else {
                            required_venue_qty
                        };
                    }
                }
            }
        }

        venue_qty
    }

    /// 将 USDT 名义金额转换为 base qty，并按 open/hedge 两腿共同的 base_step 对齐；
    /// 若任一腿触发 min_qty/min_notional，则“抬到最小”（按 align_step 进位）。
    fn convert_order_amount_to_aligned_base_qty(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        hedge_venue: TradingVenue,
        hedge_symbol: &str,
        open_quote: &Quote,
        hedge_quote: &Quote,
        open_price: f64,
        open_side: Side,
    ) -> f64 {
        if !(self.order_amount > 0.0) {
            warn!(
                "FrDecision: order_amount <= 0 when building signal for {}, skip",
                open_symbol
            );
            return 0.0;
        }
        if open_price <= 0.0 {
            warn!(
                "FrDecision: price for {} <= 0 when converting order amount, fallback to 0",
                open_symbol
            );
            return 0.0;
        }

        let open_table = self.table_for(open_venue);
        let hedge_table = self.table_for(hedge_venue);

        let raw_base_qty = self.order_amount as f64 / open_price;
        let (open_base_step, open_min_base_qty) =
            base_step_size(open_table, open_venue, open_symbol);
        let (hedge_base_step, hedge_min_base_qty) =
            base_step_size(hedge_table, hedge_venue, hedge_symbol);

        let align_step = open_base_step
            .unwrap_or(0.0)
            .max(hedge_base_step.unwrap_or(0.0));
        if align_step <= 0.0 {
            return raw_base_qty;
        }

        let mut base_qty = align_price_floor(raw_base_qty, align_step);
        if base_qty <= 0.0 {
            base_qty = align_step;
        }

        let mut required_min_base = align_step;
        if let Some(v) = open_min_base_qty {
            if v > required_min_base {
                required_min_base = v;
            }
        }
        if let Some(v) = hedge_min_base_qty {
            if v > required_min_base {
                required_min_base = v;
            }
        }

        let open_symbol_key = min_qty_symbol_key(open_venue, open_symbol);
        let hedge_symbol_key = min_qty_symbol_key(hedge_venue, hedge_symbol);

        if let Some(min_notional) = open_table.min_notional(&open_symbol_key) {
            if min_notional > 0.0 && open_price > 0.0 {
                required_min_base = required_min_base.max(min_notional / open_price);
            }
        }

        let hedge_side = if open_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        let hedge_price_for_min_notional = match hedge_side {
            Side::Buy => hedge_quote.ask,
            Side::Sell => hedge_quote.bid,
        };
        if let Some(min_notional) = hedge_table.min_notional(&hedge_symbol_key) {
            if min_notional > 0.0 && hedge_price_for_min_notional > 0.0 {
                required_min_base =
                    required_min_base.max(min_notional / hedge_price_for_min_notional);
            }
        }

        if base_qty + 1e-12 < required_min_base {
            base_qty = align_price_ceil(required_min_base, align_step);
        }

        for (venue, table, symbol_key) in [
            (open_venue, open_table, &open_symbol_key),
            (hedge_venue, hedge_table, &hedge_symbol_key),
        ] {
            if venue_qty_is_contracts(venue)
                && contract_qty_multiplier(table, venue, symbol_key).is_none()
            {
                warn!(
                    "FrDecision: missing contract_multiplier for {:?} symbol_key={} (BinanceFutures fixed=1, OKX/Gate need table), qty alignment may be inaccurate",
                    venue, symbol_key
                );
            }
        }

        if log::log_enabled!(log::Level::Debug) {
            let open_step = open_base_step.unwrap_or(0.0);
            let hedge_step = hedge_base_step.unwrap_or(0.0);
            let open_mid = (open_quote.bid + open_quote.ask) * 0.5;
            let hedge_mid = (hedge_quote.bid + hedge_quote.ask) * 0.5;
            log::debug!(
                "FrDecision qty_align: open={:?} {} step_base={:.10} hedge={:?} {} step_base={:.10} align_step={:.10} raw_base_qty={:.10} base_qty={:.10} required_min_base={:.10} open_price={:.6} hedge_mid={:.6} open_mid={:.6}",
                open_venue,
                open_symbol_key,
                open_step,
                hedge_venue,
                hedge_symbol_key,
                hedge_step,
                align_step,
                raw_base_qty,
                base_qty,
                required_min_base,
                open_price,
                hedge_mid,
                open_mid
            );
        }

        base_qty
    }

    fn side_from_fr_signal(fr_signal: FrSignalKind) -> Side {
        match fr_signal {
            FrSignalKind::ForwardOpen => Side::Buy,
            FrSignalKind::BackwardOpen => Side::Sell,
            FrSignalKind::ForwardClose => Side::Sell,
            FrSignalKind::BackwardClose => Side::Buy,
        }
    }

    pub fn update_open_scale(&mut self, open_scale: f64) {
        if !(open_scale.is_finite() && open_scale > 0.0) {
            warn!("FrDecision: 忽略无效的 open_scale={}", open_scale);
            return;
        }
        self.open_scale = open_scale;
        info!("FrDecision: open_scale 已更新为 {:.6}", self.open_scale);
    }

    pub fn update_open_orders_per_round(&mut self, open_orders_per_round: u32) {
        if open_orders_per_round == 0 {
            warn!("FrDecision: 忽略无效的 open_orders_per_round=0 更新请求");
            return;
        }
        self.open_orders_per_round = open_orders_per_round;
        info!(
            "FrDecision: open_orders_per_round 已更新，总档位 {}",
            self.open_orders_per_round
        );
    }

    /// 更新开仓挂单超时时间（秒）
    pub fn update_open_order_timeout(&mut self, open_secs: u64) {
        if open_secs == 0 {
            warn!("FrDecision: open_secs=0 无效，忽略更新");
            return;
        }
        let ttl = open_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.open_order_ttl_us = ttl as i64;
        info!("FrDecision: open_order_ttl 更新为 {}s", open_secs);
    }

    /// 更新对冲挂单超时时间（秒）
    pub fn update_hedge_timeout(&mut self, hedge_secs: u64) {
        if hedge_secs == 0 {
            warn!("FrDecision: hedge_secs=0 无效，忽略更新");
            return;
        }
        let ttl = hedge_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.hedge_timeout_mm_us = ttl as i64;
        info!("FrDecision: hedge_timeout_mm 更新为 {}s", hedge_secs);
    }

    /// 更新单笔下单数量
    pub fn update_order_amount(&mut self, amount: f32) {
        if amount <= 0.0 {
            warn!("FrDecision: amount <= 0 无效，忽略更新");
            return;
        }
        self.order_amount = amount;
        info!("FrDecision: order_amount 更新为 {:.4}", self.order_amount);
    }

    /// 更新对冲价格偏移（例：0.0003 表示万分之3）
    pub fn update_hedge_price_offset(&mut self, offset: f64) {
        if offset <= 0.0 {
            warn!("FrDecision: hedge offset <= 0 无效，忽略更新");
            return;
        }
        self.hedge_price_offset = offset;
        info!("FrDecision: hedge_price_offset 更新为 {:.6}", offset);
    }

    /// 更新对冲 request_seq 激进阈值（>=该值视为 aggressive，不偏移但仍挂 maker）
    pub fn update_hedge_aggressive_seq_threshold(&mut self, threshold: u32) {
        if threshold == 0 {
            warn!("FrDecision: hedge_aggressive_seq_threshold=0 无效，忽略更新");
            return;
        }
        self.hedge_aggressive_seq_threshold = threshold;
        info!(
            "FrDecision: hedge_aggressive_seq_threshold 更新为 {}",
            threshold
        );
    }

    /// 更新信号冷却时间（秒）
    ///
    /// # 参数
    /// - `cooldown_secs`: 冷却时间（秒），默认 5 秒
    pub fn update_signal_cooldown(&mut self, cooldown_secs: u64) {
        if cooldown_secs == 0 {
            warn!("FrDecision: cooldown_secs=0 无效，忽略更新");
            return;
        }
        let cooldown_us = cooldown_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.signal_cooldown_us = cooldown_us as i64;
        info!(
            "FrDecision: signal_cooldown 更新为 {}s ({}us)",
            cooldown_secs, self.signal_cooldown_us
        );
    }

    pub fn update_enable_tlen_cancel(&mut self, enabled: bool) {
        self.enable_tlen_cancel = enabled;
        info!(
            "FrDecision: enable_tlen_cancel updated enabled={}",
            self.enable_tlen_cancel
        );
    }

    pub fn update_tlen_cancel_freq_ms(&mut self, tlen_cancel_freq_ms: u64) {
        if tlen_cancel_freq_ms == 0 {
            warn!("FrDecision: tlen_cancel_freq_ms=0 无效，忽略更新");
            return;
        }
        self.tlen_cancel_freq_ms = tlen_cancel_freq_ms;
        info!(
            "FrDecision: tlen_cancel_freq_ms updated value={}",
            self.tlen_cancel_freq_ms
        );
    }

    pub fn process_cancel_trigger_interval(&mut self) {
        if !self.enable_tlen_cancel || self.tlen_cancel_freq_ms == 0 {
            return;
        }
        let now_us = get_timestamp_us();
        let interval_us = (self.tlen_cancel_freq_ms as i64).saturating_mul(1_000);
        if self.last_cancel_trigger_ts_us != 0
            && now_us.saturating_sub(self.last_cancel_trigger_ts_us) < interval_us
        {
            return;
        }
        match self.emit_arb_cancel_trigger_signal(now_us) {
            Ok(_) => {
                self.last_cancel_trigger_ts_us = now_us;
                log::debug!(
                    "FrDecision: ArbCancelTrigger emitted freq_ms={}",
                    self.tlen_cancel_freq_ms
                );
            }
            Err(err) => warn!("FrDecision: publish ArbCancelTrigger failed: {err:#}"),
        }
    }

    fn emit_arb_cancel_trigger_signal(&mut self, now_us: i64) -> Result<()> {
        let ctx = ArbCancelTriggerCtx {
            trigger_ts: now_us,
            freq_ms: self.tlen_cancel_freq_ms,
        };
        let signal =
            TradeSignal::create(SignalType::ArbCancelTrigger, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    fn tlen_threshold_reload_due(&self, now_us: i64) -> bool {
        const RELOAD_INTERVAL_US: i64 = 30 * 60 * 1_000_000;
        self.last_tlen_threshold_reload_ts_us == 0
            || now_us.saturating_sub(self.last_tlen_threshold_reload_ts_us)
                >= RELOAD_INTERVAL_US
    }

    /// 检查交易对是否在冷却期内
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `now`: 当前时间戳（微秒）
    /// - `signal_type`: 信号类型（ArbOpen/ArbClose/ArbCancel）
    ///
    /// # 返回
    /// - true: 该类型信号在冷却期内，不应发出
    /// - false: 该类型信号不在冷却期内，可以发出
    fn check_signal_cooldown(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        now: i64,
        signal_type: &SignalType,
    ) -> bool {
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );

        // 根据信号类型选择对应的 last_ts HashMap
        let last_ts_map = match signal_type {
            SignalType::ArbOpen => self.last_open_ts.borrow(),
            SignalType::ArbClose => self.last_close_ts.borrow(),
            SignalType::ArbCancel => self.last_cancel_ts.borrow(),
            _ => {
                warn!("FrDecision: 不支持的信号类型 {:?}", signal_type);
                return false;
            }
        };

        if let Some(&last_ts) = last_ts_map.get(&key) {
            let elapsed = now - last_ts;
            if elapsed < self.signal_cooldown_us {
                return true;
            }
        }
        false
    }

    /// 更新交易对的最后信号时间戳
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `now`: 当前时间戳（微秒）
    /// - `signal_type`: 信号类型（ArbOpen/ArbClose/ArbCancel）
    fn update_last_signal_ts(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        now: i64,
        signal_type: &SignalType,
    ) {
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );

        // 根据信号类型选择对应的 last_ts HashMap
        match signal_type {
            SignalType::ArbOpen => {
                let mut last_ts = self.last_open_ts.borrow_mut();
                last_ts.insert(key, now);
            }
            SignalType::ArbClose => {
                let mut last_ts = self.last_close_ts.borrow_mut();
                last_ts.insert(key, now);
            }
            SignalType::ArbCancel => {
                let mut last_ts = self.last_cancel_ts.borrow_mut();
                last_ts.insert(key, now);
            }
            _ => {
                warn!("FrDecision: 不支持的信号类型 {:?}", signal_type);
            }
        }
    }

    /// 构造 ArbCancel 信号上下文
    fn build_cancel_context(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        spot_quote: &Quote,
        futures_quote: &Quote,
        now: i64,
        from_key: &str,
        reason: ArbCancelReason,
        strategy_id: i32,
    ) -> ArbCancelCtx {
        let mut ctx = ArbCancelCtx::new();
        let spot_trade_symbol = normalize_symbol_for_venue(spot_symbol, spot_venue);
        let futures_trade_symbol = normalize_symbol_for_venue(futures_symbol, futures_venue);

        ctx.opening_leg =
            TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask, spot_quote.ts);
        ctx.set_opening_symbol(&spot_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(
            futures_venue,
            futures_quote.bid,
            futures_quote.ask,
            futures_quote.ts,
        );
        ctx.set_hedging_symbol(&futures_trade_symbol);

        ctx.trigger_ts = now;
        ctx.set_from_key(from_key.as_bytes().to_vec());
        ctx.set_reason(reason);
        ctx.set_target_strategy(strategy_id);

        ctx
    }

    fn spawn_tlen_threshold_loader() {
        tokio::task::spawn_local(async move {
            let redis = RedisSettings::default();
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let (due, open_venue, now_us) = FrDecision::with(|decision| {
                    let now_us = get_timestamp_us();
                    (
                        decision.tlen_threshold_reload_due(now_us),
                        decision.venues.0,
                        now_us,
                    )
                });
                if !due {
                    continue;
                }
                match tlen_threshold_loader::load_from_redis(&redis, open_venue).await {
                    Ok((redis_key, thresholds, bad_fields)) => {
                        let symbols = thresholds.len();
                        FrDecision::with_mut(|decision| {
                            decision.tlen_thresholds = thresholds;
                            decision.last_tlen_threshold_reload_ts_us = now_us;
                        });
                        info!(
                            "FrDecision: tlen thresholds loaded key={} symbols={} bad_fields={}",
                            redis_key, symbols, bad_fields
                        );
                    }
                    Err(err) => warn!(
                        "FrDecision: tlen threshold reload failed venue={:?} err={:#}",
                        open_venue, err
                    ),
                }
            }
        });
    }

    // ========== 信号状态监控 ==========

    /// 打印信号状态表（带阈值对比和最终信号）
    ///
    /// 表格包含：
    /// - FR 数据：预测FR、FR_MA、PredLoan、CurLoan、FR+PLoan、MA+CLoan
    /// - FR 信号：基于资费因子的信号判断
    /// - Spread 信号：基于价差因子的信号判断（只有 open/cancel）
    /// - Final 信号：FR + Spread 联合后的最终信号
    pub fn print_signal_table(symbols: &[String]) {
        use super::common::{ArbDirection, OperationType};
        use super::funding_rate_factor::FundingRateFactor;

        let fr_factor = FundingRateFactor::instance();

        // 先打印 FR 阈值配置
        let default_period = FundingRatePeriod::Hours8;
        let fwd_open_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Forward,
            OperationType::Open,
        );
        let fwd_close_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Forward,
            OperationType::Close,
        );
        let bwd_open_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Backward,
            OperationType::Open,
        );
        let bwd_close_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Backward,
            OperationType::Close,
        );

        info!("FR 阈值配置:");
        if let Some(cfg) = &fwd_open_config {
            info!(
                "  ForwardOpen:   预测FR {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        if let Some(cfg) = &fwd_close_config {
            info!(
                "  ForwardClose:  当前FR_MA {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        if let Some(cfg) = &bwd_open_config {
            info!(
                "  BackwardOpen:  预测FR+Loan {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        if let Some(cfg) = &bwd_close_config {
            info!(
                "  BackwardClose: 当前FR_MA+CurLoan {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        info!("");

        let mut entries = Self::collect_signal_table_entries(symbols);
        entries.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));

        info!("┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐");
        info!("│ Symbol         │ 预测FR% │ FR_MA% │ PredLoan% │ CurLoan% │ FR+PLoan% │ MA+CLoan% │ FR Sig     │ Spread Sig │ Final Sig  │");
        info!("├───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤");

        for e in entries {
            info!(
                "│ {:<14} │ {:>7.3} │ {:>6.3} │ {:>9.3} │ {:>8.3} │ {:>9.3} │ {:>9.3} │ {:<10} │ {:<10} │ {:<10} │",
                e.symbol,
                e.pred_fr_pct,
                e.fr_ma_pct,
                e.pred_loan_pct,
                e.cur_loan_pct,
                e.fr_plus_pred_loan_pct,
                e.ma_plus_cur_loan_pct,
                e.fr_sig,
                e.spread_sig,
                e.final_sig
            );
        }

        info!("└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘");
    }

    fn collect_signal_table_entries(symbols: &[String]) -> Vec<SignalTableEntry> {
        use super::funding_rate_factor::FundingRateFactor;
        use super::mkt_channel::MktChannel;
        use super::rate_fetcher::RateFetcher;
        use super::spread_factor::SpreadFactor;

        let fr_factor = FundingRateFactor::instance();
        let spread_factor = SpreadFactor::instance();
        let rate_fetcher = RateFetcher::instance();
        let mkt_channel = MktChannel::instance();
        let (open_venue, hedge_venue) = FrDecision::with(|d| d.venues);

        symbols
            .iter()
            .map(|symbol| {
                let period = rate_fetcher.get_period(symbol, hedge_venue);

                let fr = rate_fetcher
                    .get_predicted_funding_rate(symbol, hedge_venue)
                    .map(|(_, v)| v)
                    .unwrap_or(0.0);
                let predict_loan = rate_fetcher
                    .get_predict_loan_rate(symbol, hedge_venue)
                    .map(|(_, v)| v)
                    .unwrap_or(0.0);
                let current_loan = rate_fetcher
                    .get_current_loan_rate(symbol, hedge_venue)
                    .map(|(_, v)| v)
                    .unwrap_or(0.0);
                let fr_ma = mkt_channel
                    .get_funding_rate_mean(symbol, hedge_venue)
                    .unwrap_or(0.0);

                let fr_predict_loan = fr + predict_loan * 1.2;
                let fr_ma_cur_loan = fr_ma + current_loan;

                let forward_open = fr_factor.satisfy_forward_open(symbol, period, hedge_venue);
                let forward_close = fr_factor.satisfy_forward_close(symbol, period, hedge_venue);
                let backward_open = fr_factor.satisfy_backward_open(symbol, period, hedge_venue);
                let backward_close = fr_factor.satisfy_backward_close(symbol, period, hedge_venue);

                let fr_signal_label: &'static str = resolve_fr_signal_from_flags(
                    forward_open,
                    forward_close,
                    backward_open,
                    backward_close,
                )
                .map(|signal| signal.as_str())
                .unwrap_or("-");

                let spread_fwd_open =
                    spread_factor.satisfy_forward_open(open_venue, symbol, hedge_venue, symbol);
                let spread_bwd_open =
                    spread_factor.satisfy_backward_open(open_venue, symbol, hedge_venue, symbol);
                let spread_fwd_cancel =
                    spread_factor.satisfy_forward_cancel(open_venue, symbol, hedge_venue, symbol);
                let spread_bwd_cancel =
                    spread_factor.satisfy_backward_cancel(open_venue, symbol, hedge_venue, symbol);

                let spread_signal_label: &'static str = if spread_fwd_cancel {
                    "FwdCancel"
                } else if spread_bwd_cancel {
                    "BwdCancel"
                } else if spread_fwd_open {
                    "FwdOpen"
                } else if spread_bwd_open {
                    "BwdOpen"
                } else {
                    "-"
                };

                let final_signal_label: &'static str = if spread_fwd_cancel {
                    "FwdCancel"
                } else if spread_bwd_cancel {
                    "BwdCancel"
                } else {
                    match fr_signal_label {
                        "FwdOpen" if spread_fwd_open => "FwdOpen",
                        "FwdClose" if spread_bwd_open => "FwdClose",
                        "BwdOpen" if spread_bwd_open => "BwdOpen",
                        "BwdClose" if spread_fwd_open => "BwdClose",
                        _ => "-",
                    }
                };

                SignalTableEntry {
                    symbol: symbol.clone(),
                    pred_fr_pct: fr * 100.0,
                    fr_ma_pct: fr_ma * 100.0,
                    pred_loan_pct: predict_loan * 100.0,
                    cur_loan_pct: current_loan * 100.0,
                    fr_plus_pred_loan_pct: fr_predict_loan * 100.0,
                    ma_plus_cur_loan_pct: fr_ma_cur_loan * 100.0,
                    fr_sig: fr_signal_label,
                    spread_sig: spread_signal_label,
                    final_sig: final_signal_label,
                }
            })
            .collect()
    }

    // ========== 事件循环 ==========

    /// 启动 backward 订阅监听任务（使用 tokio::spawn_local）
    ///
    /// 持续轮询 backward_sub，处理来自 pre_trade 的查询反馈
    /// 策略：有消息时立即处理，无消息时 yield_now() 让出 CPU
    pub fn spawn_backward_listener() {
        tokio::task::spawn_local(async move {
            info!("FrDecision backward 监听任务启动");

            loop {
                let has_message = FR_DECISION.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return false;
                    }
                    let mut decision = decision_ref.unwrap().borrow_mut();

                    // 尝试接收一条消息
                    match decision.backward_sub.receive_msg() {
                        Ok(Some(data)) => {
                            decision.handle_backward_query(data);
                            true // 有消息，继续轮询
                        }
                        Ok(None) => {
                            false // 无消息，让出 CPU
                        }
                        Err(err) => {
                            warn!("FrDecision: backward_sub 接收错误: {}", err);
                            false
                        }
                    }
                });

                // 如果没有消息，让出控制权（但可以快速恢复）
                if !has_message {
                    tokio::task::yield_now().await;
                }
            }
        });
    }
}

#[cfg(test)]
fn convert_usdt_amount_to_qty(
    order_amount_u: f64,
    price: f64,
    min_qty: Option<f64>,
    step: Option<f64>,
) -> f64 {
    if order_amount_u <= 0.0 || price <= 0.0 {
        return 0.0;
    }
    let mut qty = order_amount_u / price;
    if let Some(step) = step {
        qty = align_step_ceil(qty, step);
    }

    if let Some(min_qty) = min_qty {
        if min_qty > 0.0 && qty < min_qty {
            qty = if let Some(step) = step {
                align_step_ceil(min_qty, step)
            } else {
                min_qty
            };
        }
    }

    qty
}

#[cfg(test)]
fn align_step_ceil(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value;
    }
    let units = (value / step).ceil();
    units * step
}

#[cfg(test)]
mod tests {
    use super::{align_step_ceil, convert_usdt_amount_to_qty};

    #[test]
    fn test_convert_usdt_amount_basic() {
        let qty = convert_usdt_amount_to_qty(100.0, 20.0, None, None);
        assert!((qty - 5.0).abs() < 1e-12);
    }

    #[test]
    fn test_convert_usdt_amount_aligns_step() {
        let qty = convert_usdt_amount_to_qty(100.0, 3.0, None, Some(0.05));
        assert!((qty - 33.35).abs() < 1e-12);
    }

    #[test]
    fn test_convert_usdt_amount_hits_min_qty() {
        let qty = convert_usdt_amount_to_qty(10.0, 100.0, Some(0.5), Some(0.1));
        assert!((qty - 0.5).abs() < 1e-12);
    }

    #[test]
    fn test_align_step_handles_invalid() {
        assert_eq!(align_step_ceil(0.0, 0.1), 0.0);
        assert!((align_step_ceil(1.01, 0.1) - 1.1).abs() < 1e-12);
    }
}
