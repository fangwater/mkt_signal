//! MM decision (namespace=mm).
//!
//! 当前实现：
//! - 按 interval 触发（每 symbol）
//! - 读取 open 盘口计算 midprice（mm 下 open=hedge）
//! - 使用 rl_return_volatility 计算价格上下界
//! - 按 orders_per_round 拆 K 档报价
//! - 每档按 order_amount_u 反推数量并做交易所量价对齐，直接发布 MMOpen

use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;

use super::factor_value_hub::FactorValueHub;
use super::fr_decision::DEFAULT_SIGNAL_CHANNEL;
use super::mkt_channel::MktChannel;
use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::redis_client::RedisSettings;
use crate::common::time_util::get_timestamp_us;
use crate::market_maker::open_quote_plan::{
    build_mm_from_key, build_mm_open_quote_plan, resolve_mm_volatility,
};
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{TradingLeg, TradingVenue};
use crate::signal::open_signal::MmOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::symbol_match::normalize_symbol_for_whitelist;

const DEFAULT_PNLU_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_PNLU_REDIS_PORT: u16 = 6379;
const DEFAULT_PNLU_REDIS_DB: i64 = 0;
const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
const PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
const TARGET_FACTOR_KEY_PREFIX: &str = TARGET_FACTOR_NAME;

thread_local! {
    static MM_DECISION: OnceCell<RefCell<MmDecision>> = OnceCell::new();
}

pub struct MmDecision {
    signal_pub: SignalPublisher,
    channel_name: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    order_interval_ms: u64,
    orders_per_round: u32,
    order_amount_u: f64,
    open_order_ttl_us: i64,
    last_report_ts_us: HashMap<String, i64>,
    open_min_qty_table: VenueMinQtyTable,
    factor_value_hub: FactorValueHub,
    // Keep Node alive for iceoryx resources.
    _node: Node<ipc::Service>,
}

impl MmDecision {
    pub fn is_initialized() -> bool {
        MM_DECISION.with(|cell| cell.get().is_some())
    }

    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("MmDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("MmDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    pub fn try_with_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    pub async fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        let result: Result<()> = MM_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }

            let decision = Self::new_sync(open_venue, hedge_venue)?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize MmDecision singleton"))?;

            info!(
                "MmDecision singleton initialized, open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            Ok(())
        });
        result?;

        Self::refresh_min_qty_async(open_venue).await;
        Ok(())
    }

    fn new_sync(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<Self> {
        let node_name = NodeName::new("mm_decision")?;
        let node = NodeBuilder::new().name(&node_name).create::<ipc::Service>()?;
        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;

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
            open_venue,
            hedge_venue,
            TARGET_FACTOR_NAME,
            TARGET_FACTOR_KEY_PREFIX,
            pnlu_settings,
            DEFAULT_PNLU_KEY_SUFFIX.to_string(),
            PNLU_MAX_AGE_SECS,
        )?;

        let open_min_qty_table = VenueMinQtyTable::new(open_venue);

        Ok(Self {
            signal_pub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            open_venue,
            hedge_venue,
            order_interval_ms: 5_000,
            orders_per_round: 8,
            order_amount_u: 100.0,
            open_order_ttl_us: 120_000_000,
            last_report_ts_us: HashMap::new(),
            open_min_qty_table,
            factor_value_hub,
            _node: node,
        })
    }

    async fn refresh_min_qty_async(open_venue: TradingVenue) {
        let mut open_table = VenueMinQtyTable::new(open_venue);
        let open_res = open_table.refresh().await;

        Self::with_mut(|decision| {
            if open_res.is_ok() {
                decision.open_min_qty_table = open_table;
            }
        });

        match open_res {
            Ok(_) => info!(
                "MmDecision: open venue min_qty_table loaded, venue={:?}",
                open_venue
            ),
            Err(err) => warn!(
                "MmDecision: failed to refresh open venue filters for {:?}, price_tick may be zero: {err:#}",
                open_venue
            ),
        }
    }

    pub fn update_order_interval_ms(&mut self, interval_ms: u64) {
        if interval_ms == 0 {
            panic!("MmDecision: order_interval_ms must be > 0");
        }
        self.order_interval_ms = interval_ms;
        info!(
            "MmDecision: order interval updated interval_ms={}",
            self.order_interval_ms
        );
    }

    pub fn update_orders_per_round(&mut self, orders_per_round: u32) {
        if orders_per_round == 0 {
            panic!("MmDecision: orders_per_round must be > 0");
        }
        self.orders_per_round = orders_per_round;
        info!(
            "MmDecision: orders_per_round updated value={}",
            self.orders_per_round
        );
    }

    pub fn update_order_amount(&mut self, order_amount: f32) {
        let val = order_amount as f64;
        if !val.is_finite() || val <= 0.0 {
            panic!(
                "MmDecision: order_amount must be finite and > 0, got {}",
                order_amount
            );
        }
        self.order_amount_u = val;
        info!(
            "MmDecision: order_amount_u updated value={:.6}",
            self.order_amount_u
        );
    }

    pub fn update_open_order_timeout(&mut self, open_order_timeout_secs: u64) {
        self.open_order_ttl_us = if open_order_timeout_secs > 0 {
            (open_order_timeout_secs as i64).saturating_mul(1_000_000)
        } else {
            0
        };
        info!(
            "MmDecision: open_order_timeout updated secs={} ttl_us={}",
            open_order_timeout_secs, self.open_order_ttl_us
        );
    }

    /// MM 报价逻辑：
    /// 1) 读取 midprice
    /// 2) 用 rl_return_volatility 计算价格区间 [mid*(1-vol), mid*(1+vol)]
    /// 3) 按 orders_per_round 拆 K 档
    /// 4) 每档按 order_amount_u 反推 qty 并对齐，直接发布 MMOpen
    pub fn make_mm_decision(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        if open_venue != self.open_venue || hedge_venue != self.hedge_venue {
            return Ok(None);
        }

        if open_venue != hedge_venue {
            warn!(
                "MmDecision: expect open_venue==hedge_venue in namespace=mm, got open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            return Ok(None);
        }

        let now_us = get_timestamp_us();
        let interval_us = (self.order_interval_ms as i64).saturating_mul(1_000);
        let symbol_key = normalize_symbol_for_whitelist(open_symbol, TradingVenue::OkexFutures);
        let last_ts = self
            .last_report_ts_us
            .get(&symbol_key)
            .copied()
            .unwrap_or(0);
        if last_ts > 0 && now_us.saturating_sub(last_ts) < interval_us {
            return Ok(None);
        }

        let mkt_channel = MktChannel::instance();
        let Some(open_quote) = mkt_channel.get_quote(open_symbol, open_venue) else {
            warn!(
                "MmDecision: missing open quote symbol={} venue={:?}",
                open_symbol, open_venue
            );
            return Ok(None);
        };

        let mid_price = (open_quote.bid + open_quote.ask) * 0.5;
        if !mid_price.is_finite() || mid_price <= 0.0 {
            warn!(
                "MmDecision: invalid mid_price symbol={} bid={:.8} ask={:.8}",
                open_symbol, open_quote.bid, open_quote.ask
            );
            return Ok(None);
        }

        let factor_lookup = self
            .factor_value_hub
            .lookup_target_factor_value(hedge_symbol, hedge_venue);
        let factor_ready = factor_lookup.ready.unwrap_or(false);
        let volatility = resolve_mm_volatility(factor_lookup.target_factor_value, factor_ready);
        let plan = match build_mm_open_quote_plan(
            open_venue,
            open_symbol,
            open_quote,
            self.order_amount_u,
            self.orders_per_round,
            self.open_order_ttl_us,
            volatility,
            now_us,
            &self.open_min_qty_table,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                warn!("MmDecision: build open quote plan failed: {}", err);
                return Ok(None);
            }
        };

        // 进入本轮报价，更新时间戳（防止失败时高频重复触发）
        self.last_report_ts_us.insert(symbol_key.clone(), now_us);

        let mut sent = 0usize;
        let mut sent_buy = 0usize;
        let mut sent_sell = 0usize;
        for level in &plan.levels {
            let mut ctx = MmOpenCtx::new();
            ctx.opening_leg = TradingLeg::new(
                open_venue,
                plan.quote.bid,
                plan.quote.ask,
                plan.quote.ts,
            );
            ctx.set_opening_symbol(&plan.symbol);
            ctx.set_side(level.side);
            ctx.set_order_type(OrderType::Limit);
            let _ = ctx.set_amount_with_tick_floor(level.aligned_qty, plan.qty_tick);
            let _ = ctx.set_price_with_tick_floor(level.aligned_price, plan.price_tick);
            if ctx.amount_count() <= 0 || ctx.price_count() <= 0 {
                continue;
            }
            ctx.exp_time = plan.exp_time_us;
            ctx.create_ts = plan.now_us;
            ctx.price_offset = level.offset;
            let from_key = build_mm_from_key(
                plan.now_us,
                self.orders_per_round,
                level.level_index,
                plan.band,
            );
            ctx.set_from_key(from_key.into_bytes());

            let signal = TradeSignal::create(SignalType::MMOpen, now_us, 0.0, ctx.to_bytes());
            if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
                warn!(
                    "MmDecision: publish MMOpen failed symbol={} idx={} side={:?} err={:?}",
                    plan.symbol, level.level_index, level.side, err
                );
                continue;
            }

            sent += 1;
            match level.side {
                Side::Buy => sent_buy += 1,
                Side::Sell => sent_sell += 1,
            }
        }

        info!(
            "MmDecision: quoted symbol={} channel={} now_us={} interval_ms={} orders_per_round={} sent={} buy={} sell={} mid={:.8} vol={:.8} vol_ready={} band=[{:.8}, {:.8}] factor_key={} factor_note={}",
            symbol_key,
            self.channel_name,
            now_us,
            self.order_interval_ms,
            self.orders_per_round,
            sent,
            sent_buy,
            sent_sell,
            plan.band.mid_price,
            plan.band.volatility,
            factor_ready,
            plan.band.lower_price,
            plan.band.upper_price,
            factor_lookup.key,
            factor_lookup.note,
        );

        Ok(None)
    }
}
