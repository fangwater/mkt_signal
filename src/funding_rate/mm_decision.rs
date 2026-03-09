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
use log::{info, trace, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;

use super::common::{build_decision_from_key_base, ReturnScoreThresholdsResolved};
use super::factor_value_hub::FactorValueHub;
use super::fr_decision::DEFAULT_SIGNAL_CHANNEL;
use super::mkt_channel::MktChannel;
use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::market_maker::open_quote_plan::build_mm_open_quote_plan;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::MmCancelCtx;
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
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
const ENV_MODEL_TRUE_THRESHOLD_DEFAULT: f64 = 0.0;

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
    prediction_mode: bool,
    return_model_service: Option<String>,
    environment_model_service: Option<String>,
    environment_model_true_threshold: f64,
    last_report_ts_us: HashMap<String, i64>,
    return_score_thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
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
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;
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
            prediction_mode: false,
            return_model_service: None,
            environment_model_service: None,
            environment_model_true_threshold: ENV_MODEL_TRUE_THRESHOLD_DEFAULT,
            last_report_ts_us: HashMap::new(),
            return_score_thresholds: HashMap::new(),
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

    pub fn update_prediction_mode(&mut self, enabled: bool) {
        self.prediction_mode = enabled;
        info!(
            "MmDecision: prediction_mode updated enabled={}",
            self.prediction_mode
        );
    }

    pub fn update_model_service_roles(
        &mut self,
        return_model_service: String,
        environment_model_service: String,
    ) {
        let return_trimmed = return_model_service.trim();
        if return_trimmed.is_empty() || return_trimmed == "-" {
            panic!(
                "MmDecision: return_model_service must not be '-' or empty (got '{}')",
                return_trimmed
            );
        }
        self.return_model_service = Some(return_trimmed.to_string());
        let env_trimmed = environment_model_service.trim();
        self.environment_model_service = if env_trimmed.is_empty() || env_trimmed == "-" {
            None
        } else {
            Some(env_trimmed.to_string())
        };
        let mut services = vec![return_trimmed.to_string()];
        if let Some(env_service) = self.environment_model_service.as_ref() {
            if !services.iter().any(|service| service == env_service) {
                services.push(env_service.clone());
            }
        }
        self.factor_value_hub
            .update_model_output_services(&self._node, services);
        info!(
            "MmDecision: model roles updated return={:?} environment={:?} env_true_threshold={:.6}",
            self.return_model_service,
            self.environment_model_service,
            self.environment_model_true_threshold
        );
    }

    fn evaluate_environment_signal(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        now_us: i64,
    ) -> super::factor_value_hub::EnvironmentSignalResult {
        self.factor_value_hub.evaluate_environment_signal(
            self.environment_model_service.as_deref(),
            hedge_symbol,
            hedge_venue,
            self.environment_model_true_threshold,
            open_symbol_key,
            now_us,
        )
    }

    pub fn update_return_score_thresholds(
        &mut self,
        thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    ) {
        self.return_score_thresholds = thresholds;
        info!(
            "MmDecision: return score thresholds updated symbols={}",
            self.return_score_thresholds.len(),
        );
    }

    fn select_effective_return_threshold(
        return_score: f64,
        forward_hit: bool,
        backward_hit: bool,
        forward_threshold: f64,
        backward_threshold: f64,
    ) -> Option<f64> {
        match (forward_hit, backward_hit) {
            (true, false) => Some(forward_threshold),
            (false, true) => Some(backward_threshold),
            (true, true) => Some(if return_score >= 0.0 {
                forward_threshold
            } else {
                backward_threshold
            }),
            (false, false) => None,
        }
    }

    fn select_prediction_side(
        &self,
        return_score: Option<f64>,
        thresholds: Option<ReturnScoreThresholdsResolved>,
    ) -> (Option<Side>, Option<f64>, bool, bool, bool) {
        let (Some(score), Some(thresholds)) = (return_score, thresholds) else {
            return (None, None, false, false, false);
        };

        let forward_open_hit = score > thresholds.forward_open;
        let backward_open_hit = score < thresholds.backward_open;
        let prediction_side = if self.prediction_mode {
            if forward_open_hit {
                Some(Side::Buy)
            } else if backward_open_hit {
                Some(Side::Sell)
            } else {
                None
            }
        } else {
            None
        };
        let prediction_threshold = prediction_side.map(|side| match side {
            Side::Buy => thresholds.forward_open,
            Side::Sell => thresholds.backward_open,
        });
        let prediction_ready = forward_open_hit || backward_open_hit;

        (
            prediction_side,
            prediction_threshold,
            forward_open_hit,
            backward_open_hit,
            prediction_ready,
        )
    }

    fn emit_mm_cancel_signal(
        &mut self,
        open_symbol: &str,
        open_venue: TradingVenue,
        side: Side,
        open_quote: crate::funding_rate::common::Quote,
        now_us: i64,
        from_key: &str,
    ) -> Result<()> {
        let mut ctx = MmCancelCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, open_venue);
        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_trade_symbol);
        ctx.set_side(side);
        ctx.trigger_ts = now_us;
        ctx.set_from_key(from_key.as_bytes().to_vec());

        let signal = TradeSignal::create(SignalType::MMCancel, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    /// MM 报价逻辑：
    /// 1) 读取 midprice
    /// 2) 用 rl_return_volatility 计算价格区间 [mid*(1-vol), mid*(1+vol)]
    /// 3) 以 mid 为中心双边拆档：每边 orders_per_round 档（总计 2K）
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
        let symbol_key = normalize_symbol_for_whitelist(open_symbol, TradingVenue::OkexFutures);

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
        let volatility = factor_lookup.target_factor_value.filter(|v| v.is_finite());
        let Some(service_name) = self.return_model_service.as_deref() else {
            warn!(
                "MmDecision: return_model_service not configured, skip decision symbol={}",
                symbol_key
            );
            return Ok(None);
        };

        let score_lookup = self.factor_value_hub.lookup_model_output_score(
            service_name,
            hedge_symbol,
            hedge_venue,
        );
        let return_score = score_lookup.score.filter(|v| v.is_finite());
        let threshold_symbol = score_lookup.symbol_key.to_ascii_uppercase();
        let thresholds = self.return_score_thresholds.get(&threshold_symbol).copied();
        if return_score.is_none() {
            trace!(
                "MmDecision: return score not ready symbol={} service={} model_symbol={} note={} (cancel skipped, open falls back to vol/interval only)",
                symbol_key,
                score_lookup.service_name,
                score_lookup.symbol_key,
                score_lookup.note
            );
        } else if thresholds.is_none() {
            trace!(
                "MmDecision: missing return score thresholds symbol={} threshold_symbol={} venue={:?} (cancel skipped, open falls back to vol/interval only)",
                symbol_key, threshold_symbol, hedge_venue
            );
        }

        let return_score_value = return_score.unwrap_or(f64::NAN);
        let forward_cancel_hit = matches!(
            (return_score, thresholds),
            (Some(score), Some(thresholds)) if score < thresholds.forward_cancel
        );
        let backward_cancel_hit = matches!(
            (return_score, thresholds),
            (Some(score), Some(thresholds)) if score > thresholds.backward_cancel
        );
        let cancel_hit = forward_cancel_hit || backward_cancel_hit;
        let (
            prediction_side,
            open_return_threshold,
            forward_open_hit,
            backward_open_hit,
            prediction_ready,
        ) = self.select_prediction_side(return_score, thresholds);

        let cancel_return_threshold = Self::select_effective_return_threshold(
            return_score_value,
            forward_cancel_hit,
            backward_cancel_hit,
            thresholds
                .map(|value| value.forward_cancel)
                .unwrap_or(f64::NAN),
            thresholds
                .map(|value| value.backward_cancel)
                .unwrap_or(f64::NAN),
        );
        let environment_signal =
            self.evaluate_environment_signal(&symbol_key, hedge_symbol, hedge_venue, now_us);

        if cancel_hit {
            let mut cancel_sent = false;

            if forward_cancel_hit {
                let from_key = build_decision_from_key_base(
                    now_us,
                    return_score,
                    thresholds.map(|value| value.forward_cancel),
                    volatility,
                    environment_signal.score,
                    environment_signal.threshold,
                );
                self.emit_mm_cancel_signal(
                    open_symbol,
                    open_venue,
                    Side::Buy,
                    open_quote,
                    now_us,
                    &from_key,
                )?;
                cancel_sent = true;
            }

            if backward_cancel_hit {
                let from_key = build_decision_from_key_base(
                    now_us,
                    return_score,
                    thresholds.map(|value| value.backward_cancel),
                    volatility,
                    environment_signal.score,
                    environment_signal.threshold,
                );
                self.emit_mm_cancel_signal(
                    open_symbol,
                    open_venue,
                    Side::Sell,
                    open_quote,
                    now_us,
                    &from_key,
                )?;
                cancel_sent = true;
            }

            if cancel_sent {
                info!(
                    "MmDecision: MMCancel symbol={} score={:.8} ret_thr={:?} fwd_cancel_hit={} bwd_cancel_hit={} fwd_cancel_th={:.8} bwd_cancel_th={:.8} service={} model_symbol={} env_src={:?} env_score={:?} env_thr={:?}",
                    symbol_key,
                    return_score_value,
                    cancel_return_threshold,
                    forward_cancel_hit,
                    backward_cancel_hit,
                    thresholds.map(|value| value.forward_cancel).unwrap_or(f64::NAN),
                    thresholds.map(|value| value.backward_cancel).unwrap_or(f64::NAN),
                    score_lookup.service_name,
                    score_lookup.symbol_key,
                    environment_signal.source,
                    environment_signal.score,
                    environment_signal.threshold,
                );
                return Ok(Some(SignalType::MMCancel));
            }
        }

        let Some(volatility) = volatility else {
            trace!(
                "MmDecision: skip MMOpen symbol={} score={:?} ret_thr={:?} prediction_mode={} prediction_side={:?} service={} model_symbol={} factor_key={} factor_note={} (volatility missing)",
                symbol_key,
                return_score,
                open_return_threshold,
                self.prediction_mode,
                prediction_side,
                score_lookup.service_name,
                score_lookup.symbol_key,
                factor_lookup.key,
                factor_lookup.note,
            );
            return Ok(None);
        };

        if self.prediction_mode && !prediction_ready {
            trace!(
                "MmDecision: skip MMOpen by prediction symbol={} score={:?} ret_thr={:?} service={} model_symbol={} fwd_open_hit={} bwd_open_hit={} prediction_mode=true",
                symbol_key,
                return_score,
                open_return_threshold,
                score_lookup.service_name,
                score_lookup.symbol_key,
                forward_open_hit,
                backward_open_hit,
            );
            return Ok(None);
        }

        let interval_us = (self.order_interval_ms as i64).saturating_mul(1_000);
        let last_ts = self
            .last_report_ts_us
            .get(&symbol_key)
            .copied()
            .unwrap_or(0);
        if last_ts > 0 && now_us.saturating_sub(last_ts) < interval_us {
            return Ok(None);
        }
        self.last_report_ts_us.insert(symbol_key.clone(), now_us);
        let from_key = build_decision_from_key_base(
            now_us,
            return_score,
            open_return_threshold,
            Some(volatility),
            environment_signal.score,
            environment_signal.threshold,
        );

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

        let mut sent = 0usize;
        let mut sent_buy = 0usize;
        let mut sent_sell = 0usize;
        for level in &plan.levels {
            if let Some(side) = prediction_side {
                if level.side != side {
                    continue;
                }
            }
            let mut ctx = MmOpenCtx::new();
            ctx.opening_leg =
                TradingLeg::new(open_venue, plan.quote.bid, plan.quote.ask, plan.quote.ts);
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
            ctx.set_from_key(from_key.as_bytes().to_vec());

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
            "MmDecision: MMOpen symbol={} channel={} now_us={} interval_ms={} orders_per_round={} sent={} buy={} sell={} prediction_mode={} prediction_side={:?} score={:?} ret_thr={:?} fwd_open_hit={} bwd_open_hit={} fwd_open_th={:.8} bwd_open_th={:.8} mid={:.8} vol={:.8} factor_ready={} band=[{:.8}, {:.8}] factor_key={} factor_note={}",
            symbol_key,
            self.channel_name,
            now_us,
            self.order_interval_ms,
            self.orders_per_round,
            sent,
            sent_buy,
            sent_sell,
            self.prediction_mode,
            prediction_side,
            return_score,
            open_return_threshold,
            forward_open_hit,
            backward_open_hit,
            thresholds.map(|value| value.forward_open).unwrap_or(f64::NAN),
            thresholds.map(|value| value.backward_open).unwrap_or(f64::NAN),
            plan.band.mid_price,
            plan.band.volatility,
            factor_ready,
            plan.band.lower_price,
            plan.band.upper_price,
            factor_lookup.key,
            factor_lookup.note,
        );

        if sent > 0 {
            Ok(Some(SignalType::MMOpen))
        } else {
            Ok(None)
        }
    }
}
