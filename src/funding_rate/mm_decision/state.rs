use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::debug;
use std::collections::HashMap;

use super::super::arb_decision::DEFAULT_ARBITRAGE_SIGNAL_CHANNEL;
use super::super::common::{
    apply_open_tlen_gate_and_build_from_keys, ReturnScoreThresholdsResolved,
};
use super::super::factor_value_hub::{EnvironmentSignalResult, FactorValueHub};
use super::super::inline_volatility::{snapshot_inline_volatility, InlineVolatilitySnapshot};
use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::depth_pub::query_client::DepthQueryClient;
use crate::market_maker::open_quote_plan::MmOpenQuotePlan;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::{MmCancelCtx, MmCancelReason};
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use crate::signal::mm_signal::MmCancelTriggerCtx;
use crate::signal::open_signal::MmOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

pub(crate) const DEFAULT_PNLU_REDIS_HOST: &str = "127.0.0.1";
pub(crate) const DEFAULT_PNLU_REDIS_PORT: u16 = 6379;
pub(crate) const DEFAULT_PNLU_REDIS_DB: i64 = 0;
pub(crate) const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
pub(crate) const PNLU_MAX_AGE_SECS: i64 = 30 * 60;
pub(crate) const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
pub(crate) const TARGET_FACTOR_KEY_PREFIX: &str = TARGET_FACTOR_NAME;
pub(crate) const TARGET_FACTOR_MAX_AGE_MS: i64 = 30_000;
pub(crate) const ENV_MODEL_TRUE_THRESHOLD_DEFAULT: f64 = 0.0;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MmOpenPublishStats {
    pub(crate) sent: usize,
    pub(crate) sent_buy: usize,
    pub(crate) sent_sell: usize,
    pub(crate) prepared_levels: usize,
    pub(crate) zero_quantized_levels: usize,
    pub(crate) tlen_filtered_levels: usize,
    pub(crate) publish_failures: usize,
}

pub(crate) struct MmDecisionState {
    pub(crate) signal_pub: SignalPublisher,
    pub(crate) depth_query_client: DepthQueryClient,
    pub(crate) open_venue: TradingVenue,
    pub(crate) hedge_venue: TradingVenue,
    pub(crate) order_interval_ms: u64,
    pub(crate) open_orders_per_round: u32,
    pub(crate) open_buy_vol_scale: [f64; 2],
    pub(crate) open_sell_vol_scale: [f64; 2],
    pub(crate) hedge_orders_per_round: u32,
    pub(crate) order_amount_u: f64,
    pub(crate) order_amount_u_overrides: HashMap<String, f64>,
    pub(crate) open_order_ttl_us: i64,
    pub(crate) next_query_delay_ms: u64,
    pub(crate) hedge_vol_multiplier: f64,
    pub(crate) hedge_offset_ratio: f64,
    pub(crate) hedge_price_offset_limit_upper: f64,
    pub(crate) hedge_price_offset_limit_lower: f64,
    pub(crate) hedge_window_scale_low: f64,
    pub(crate) hedge_window_scale_high: f64,
    pub(crate) max_hedge_price_pct_change: f64,
    pub(crate) enable_return_score_adjust_hedge: bool,
    pub(crate) enable_open_cancel: bool,
    pub(crate) enable_tlen_cancel: bool,
    pub(crate) tlen_cancel_freq_ms: u64,
    pub(crate) prediction_mode: bool,
    pub(crate) enable_environment_model: bool,
    pub(crate) enable_volatility_limit: bool,
    pub(crate) open_volatility_limit: f64,
    pub(crate) return_model_service: Option<String>,
    pub(crate) environment_model_service: Option<String>,
    pub(crate) environment_model_true_threshold: f64,
    pub(crate) return_score_thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    pub(crate) tlen_thresholds: HashMap<String, f64>,
    pub(crate) open_min_qty_table: VenueMinQtyTable,
    pub(crate) factor_value_hub: FactorValueHub,
    pub(crate) last_tlen_threshold_reload_ts_us: i64,
    pub(crate) last_cancel_trigger_ts_us: i64,
}

fn resolve_mm_order_amount_u(
    default_order_amount_u: f64,
    overrides: &HashMap<String, f64>,
    open_venue: TradingVenue,
    symbol: &str,
) -> f64 {
    let symbol_key = normalize_symbol_for_venue(symbol, open_venue);
    overrides
        .get(&symbol_key)
        .copied()
        .unwrap_or(default_order_amount_u)
}

impl MmDecisionState {
    pub(crate) fn new(
        node: &Node<ipc::Service>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Self> {
        let signal_pub = SignalPublisher::new(DEFAULT_ARBITRAGE_SIGNAL_CHANNEL)?;
        let depth_query_client = DepthQueryClient::new(open_venue)?;
        let pnlu_settings = RedisSettings {
            host: DEFAULT_PNLU_REDIS_HOST.to_string(),
            port: DEFAULT_PNLU_REDIS_PORT,
            db: DEFAULT_PNLU_REDIS_DB,
            username: None,
            password: None,
            prefix: None,
        };
        let factor_value_hub = FactorValueHub::new(
            node,
            open_venue,
            hedge_venue,
            TARGET_FACTOR_NAME,
            TARGET_FACTOR_KEY_PREFIX,
            pnlu_settings,
            DEFAULT_PNLU_KEY_SUFFIX.to_string(),
            PNLU_MAX_AGE_SECS,
            TARGET_FACTOR_MAX_AGE_MS,
        )?;

        Ok(Self {
            signal_pub,
            depth_query_client,
            open_venue,
            hedge_venue,
            order_interval_ms: 5_000,
            open_orders_per_round: 8,
            open_buy_vol_scale: [0.0, 1.0],
            open_sell_vol_scale: [0.0, 1.0],
            hedge_orders_per_round: 8,
            order_amount_u: 100.0,
            order_amount_u_overrides: HashMap::new(),
            open_order_ttl_us: 120_000_000,
            next_query_delay_ms: 30_000,
            hedge_vol_multiplier: 2.0,
            hedge_offset_ratio: 1.3,
            hedge_price_offset_limit_upper: 0.005,
            hedge_price_offset_limit_lower: 0.0003,
            hedge_window_scale_low: 0.8,
            hedge_window_scale_high: 1.3,
            max_hedge_price_pct_change: 5.0,
            enable_return_score_adjust_hedge: true,
            enable_open_cancel: false,
            enable_tlen_cancel: false,
            tlen_cancel_freq_ms: 3_000,
            prediction_mode: false,
            enable_environment_model: true,
            enable_volatility_limit: true,
            open_volatility_limit: 70.0,
            return_model_service: None,
            environment_model_service: None,
            environment_model_true_threshold: ENV_MODEL_TRUE_THRESHOLD_DEFAULT,
            return_score_thresholds: HashMap::new(),
            tlen_thresholds: HashMap::new(),
            open_min_qty_table: VenueMinQtyTable::new(open_venue),
            factor_value_hub,
            last_tlen_threshold_reload_ts_us: 0,
            last_cancel_trigger_ts_us: 0,
        })
    }

    pub(crate) fn update_order_interval_ms(&mut self, interval_ms: u64) {
        if interval_ms == 0 {
            panic!("MmDecision: order_interval_ms must be > 0");
        }
        self.order_interval_ms = interval_ms;
        debug!(
            "MmDecision: order interval updated interval_ms={}",
            self.order_interval_ms
        );
    }

    pub(crate) fn update_open_orders_per_round(&mut self, open_orders_per_round: u32) {
        if open_orders_per_round == 0 {
            panic!("MmDecision: open_orders_per_round must be > 0");
        }
        self.open_orders_per_round = open_orders_per_round;
        debug!(
            "MmDecision: open_orders_per_round updated value={}",
            self.open_orders_per_round
        );
    }

    pub(crate) fn update_open_vol_scale_ranges(
        &mut self,
        open_buy_vol_scale: [f64; 2],
        open_sell_vol_scale: [f64; 2],
    ) {
        for (name, scale) in [
            ("open_buy_vol_scale", open_buy_vol_scale),
            ("open_sell_vol_scale", open_sell_vol_scale),
        ] {
            if !scale[0].is_finite() || !scale[1].is_finite() {
                panic!("MmDecision: {} must be finite, got {:?}", name, scale);
            }
            if scale[0] < 0.0 || scale[1] < scale[0] {
                panic!(
                    "MmDecision: {} must satisfy 0<=low<=high, got {:?}",
                    name, scale
                );
            }
        }
        self.open_buy_vol_scale = open_buy_vol_scale;
        self.open_sell_vol_scale = open_sell_vol_scale;
        debug!(
            "MmDecision: open vol scale ranges updated buy=[{:.6}, {:.6}] sell=[{:.6}, {:.6}]",
            self.open_buy_vol_scale[0],
            self.open_buy_vol_scale[1],
            self.open_sell_vol_scale[0],
            self.open_sell_vol_scale[1]
        );
    }

    pub(crate) fn update_mm_hedge_params(
        &mut self,
        hedge_orders_per_round: u32,
        hedge_vol_multiplier: f64,
        hedge_offset_ratio: f64,
        hedge_price_offset_limit_lower: f64,
        hedge_price_offset_limit_upper: f64,
        hedge_window_scale_low: f64,
        hedge_window_scale_high: f64,
        max_hedge_price_pct_change: f64,
        next_query_delay_ms: u64,
        enable_return_score_adjust_hedge: bool,
    ) {
        if hedge_orders_per_round == 0 {
            panic!("MmDecision: hedge_orders_per_round must be > 0");
        }
        if !(hedge_vol_multiplier.is_finite() && hedge_vol_multiplier > 0.0) {
            panic!(
                "MmDecision: hedge_vol_multiplier must be finite and > 0, got {}",
                hedge_vol_multiplier
            );
        }
        if !(hedge_offset_ratio.is_finite() && hedge_offset_ratio > 0.0) {
            panic!(
                "MmDecision: hedge_offset_ratio must be finite and > 0, got {}",
                hedge_offset_ratio
            );
        }
        if !hedge_price_offset_limit_lower.is_finite()
            || !hedge_price_offset_limit_upper.is_finite()
        {
            panic!("MmDecision: hedge price offset limits must be finite");
        }
        if !hedge_window_scale_low.is_finite() || !hedge_window_scale_high.is_finite() {
            panic!("MmDecision: hedge_window_scale values must be finite");
        }
        if hedge_window_scale_low <= 0.0 || hedge_window_scale_high < hedge_window_scale_low {
            panic!(
                "MmDecision: hedge_window_scale must satisfy 0<low<=high, got low={} high={}",
                hedge_window_scale_low, hedge_window_scale_high
            );
        }
        if !(max_hedge_price_pct_change.is_finite() && max_hedge_price_pct_change > 0.0) {
            panic!(
                "MmDecision: max_hedge_price_pct_change must be finite and > 0, got {}",
                max_hedge_price_pct_change
            );
        }
        self.hedge_orders_per_round = hedge_orders_per_round;
        self.hedge_vol_multiplier = hedge_vol_multiplier;
        self.hedge_offset_ratio = hedge_offset_ratio;
        self.hedge_price_offset_limit_lower = hedge_price_offset_limit_lower;
        self.hedge_price_offset_limit_upper = hedge_price_offset_limit_upper;
        self.hedge_window_scale_low = hedge_window_scale_low;
        self.hedge_window_scale_high = hedge_window_scale_high;
        self.max_hedge_price_pct_change = max_hedge_price_pct_change;
        self.next_query_delay_ms = next_query_delay_ms;
        self.enable_return_score_adjust_hedge = enable_return_score_adjust_hedge;
        debug!(
            "MmDecision: hedge params updated levels={} vol_multiplier={:.6} offset_ratio={:.6} low={:.6} high={:.6} hedge_window_scale_low={:.6} hedge_window_scale_high={:.6} max_pct_change={:.6} next_query_delay_ms={} enable_return_score_adjust_hedge={}",
            self.hedge_orders_per_round,
            self.hedge_vol_multiplier,
            self.hedge_offset_ratio,
            self.hedge_price_offset_limit_lower,
            self.hedge_price_offset_limit_upper,
            self.hedge_window_scale_low,
            self.hedge_window_scale_high,
            self.max_hedge_price_pct_change,
            self.next_query_delay_ms,
            self.enable_return_score_adjust_hedge
        );
    }

    pub(crate) fn update_order_amount(&mut self, order_amount: f32) {
        let val = order_amount as f64;
        if !val.is_finite() || val <= 0.0 {
            panic!(
                "MmDecision: order_amount must be finite and > 0, got {}",
                order_amount
            );
        }
        self.order_amount_u = val;
        debug!(
            "MmDecision: order_amount_u updated value={:.6}",
            self.order_amount_u
        );
    }

    pub(crate) fn update_order_amount_overrides(&mut self, overrides: HashMap<String, f64>) {
        self.order_amount_u_overrides = overrides;
        debug!(
            "MmDecision: order_amount_u_overrides updated symbols={}",
            self.order_amount_u_overrides.len()
        );
    }

    pub(crate) fn resolve_order_amount_u(&self, symbol: &str) -> f64 {
        resolve_mm_order_amount_u(
            self.order_amount_u,
            &self.order_amount_u_overrides,
            self.open_venue,
            symbol,
        )
    }

    pub(crate) fn update_open_order_timeout(&mut self, open_order_timeout_secs: u64) {
        self.open_order_ttl_us = if open_order_timeout_secs > 0 {
            (open_order_timeout_secs as i64).saturating_mul(1_000_000)
        } else {
            0
        };
        debug!(
            "MmDecision: open_order_timeout updated secs={} ttl_us={}",
            open_order_timeout_secs, self.open_order_ttl_us
        );
    }

    pub(crate) fn update_prediction_mode(&mut self, enabled: bool) {
        self.prediction_mode = enabled;
        debug!(
            "MmDecision: prediction_mode updated enabled={}",
            self.prediction_mode
        );
    }

    pub(crate) fn update_enable_open_cancel(&mut self, enabled: bool) {
        self.enable_open_cancel = enabled;
        debug!(
            "MmDecision: enable_open_cancel updated enabled={}",
            self.enable_open_cancel
        );
    }

    pub(crate) fn update_enable_tlen_cancel(&mut self, enabled: bool) {
        self.enable_tlen_cancel = enabled;
        debug!(
            "MmDecision: enable_tlen_cancel updated enabled={}",
            self.enable_tlen_cancel
        );
    }

    pub(crate) fn update_enable_environment_model(&mut self, enabled: bool) {
        self.enable_environment_model = enabled;
        debug!(
            "MmDecision: enable_environment_model updated enabled={}",
            self.enable_environment_model
        );
    }

    pub(crate) fn update_enable_volatility_limit(&mut self, enabled: bool) {
        self.enable_volatility_limit = enabled;
        debug!(
            "MmDecision: enable_volatility_limit updated enabled={}",
            self.enable_volatility_limit
        );
    }

    pub(crate) fn update_open_volatility_limit(&mut self, percentile: f64) {
        if !(percentile.is_finite() && percentile >= 0.0 && percentile <= 100.0) {
            panic!(
                "MmDecision: open_volatility_limit must be finite and within [0,100], got {}",
                percentile
            );
        }
        self.open_volatility_limit = percentile;
        debug!(
            "MmDecision: open_volatility_limit updated percentile={}",
            self.open_volatility_limit
        );
    }

    pub(crate) fn update_tlen_cancel_freq_ms(&mut self, tlen_cancel_freq_ms: u64) {
        if tlen_cancel_freq_ms == 0 {
            panic!("MmDecision: tlen_cancel_freq_ms must be > 0");
        }
        self.tlen_cancel_freq_ms = tlen_cancel_freq_ms;
        debug!(
            "MmDecision: tlen_cancel_freq_ms updated value={}",
            self.tlen_cancel_freq_ms
        );
    }

    pub(crate) fn update_model_service_roles(
        &mut self,
        node: &Node<ipc::Service>,
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
            .update_model_output_services(node, services);
        debug!(
            "MmDecision: model roles updated return={:?} environment={:?} env_true_threshold={:.6}",
            self.return_model_service,
            self.environment_model_service,
            self.environment_model_true_threshold
        );
    }

    pub(crate) fn update_return_score_thresholds(
        &mut self,
        thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    ) {
        self.return_score_thresholds = thresholds;
        debug!(
            "MmDecision: return score thresholds updated symbols={}",
            self.return_score_thresholds.len(),
        );
    }

    pub(crate) fn snapshot_open_volatility(
        &self,
        symbol_key: &str,
        volatility: f64,
    ) -> InlineVolatilitySnapshot {
        let symbol_key = symbol_key.to_ascii_uppercase();
        snapshot_inline_volatility(&symbol_key, volatility, self.open_volatility_limit)
    }

    pub(crate) fn evaluate_environment_signal(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        now_us: i64,
    ) -> EnvironmentSignalResult {
        self.factor_value_hub.evaluate_environment_signal(
            self.environment_model_service.as_deref(),
            hedge_symbol,
            self.hedge_venue,
            self.environment_model_true_threshold,
            open_symbol_key,
            now_us,
        )
    }

    pub(crate) fn emit_mm_cancel_signal(
        &mut self,
        open_symbol: &str,
        side: Side,
        open_quote: crate::funding_rate::common::Quote,
        now_us: i64,
        from_key: &str,
    ) -> Result<()> {
        let mut ctx = MmCancelCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, self.open_venue);
        ctx.opening_leg = TradingLeg::new(
            self.open_venue,
            open_quote.bid,
            open_quote.ask,
            open_quote.ts,
        );
        ctx.set_opening_symbol(&open_trade_symbol);
        ctx.set_side(side);
        ctx.set_reason(MmCancelReason::ReturnScore);
        ctx.trigger_ts = now_us;
        ctx.set_from_key(from_key.as_bytes().to_vec());

        let signal = TradeSignal::create(SignalType::MMCancel, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    pub(crate) fn emit_mm_cancel_signal_precise(
        &mut self,
        open_symbol: &str,
        open_quote: crate::funding_rate::common::Quote,
        now_us: i64,
        from_key: &str,
        strategy_id: i32,
    ) -> Result<()> {
        let mut ctx = MmCancelCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, self.open_venue);
        ctx.opening_leg = TradingLeg::new(
            self.open_venue,
            open_quote.bid,
            open_quote.ask,
            open_quote.ts,
        );
        ctx.set_opening_symbol(&open_trade_symbol);
        ctx.set_side(Side::Buy);
        ctx.set_reason(MmCancelReason::Tlen);
        ctx.trigger_ts = now_us;
        ctx.set_from_key(from_key.as_bytes().to_vec());
        ctx.set_target_strategy(strategy_id, 0);

        let signal = TradeSignal::create(SignalType::MMCancel, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    pub(crate) fn emit_mm_cancel_trigger_signal(&mut self, now_us: i64) -> Result<()> {
        let ctx = MmCancelTriggerCtx {
            trigger_ts: now_us,
            freq_ms: self.tlen_cancel_freq_ms,
        };
        let signal = TradeSignal::create(SignalType::MMCancelTrigger, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    pub(crate) fn publish_mm_open_plan(
        &mut self,
        now_us: i64,
        plan: &MmOpenQuotePlan,
        from_key: &str,
        prediction_side: Option<Side>,
    ) -> MmOpenPublishStats {
        struct PreparedOpenSignal {
            side: Side,
            level_index: usize,
            side_level_index: usize,
            tick_index: i64,
            aligned_price: f64,
            aligned_qty: f64,
            price_offset: f64,
            price_tick_count: i64,
            qty_tick_count: i64,
            ctx: MmOpenCtx,
        }

        let mut sent = 0usize;
        let mut sent_buy = 0usize;
        let mut sent_sell = 0usize;
        let mut zero_quantized_levels = 0usize;
        let mut tlen_filtered_levels = 0usize;
        let mut publish_failures = 0usize;
        let mut emitted_details = Vec::new();
        let mut prepared = Vec::with_capacity(plan.levels.len());

        for level in &plan.levels {
            if let Some(side) = prediction_side {
                if level.side != side {
                    continue;
                }
            }

            let mut ctx = MmOpenCtx::new();
            ctx.opening_leg = TradingLeg::new(
                self.open_venue,
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
                zero_quantized_levels += 1;
                continue;
            }
            ctx.exp_time = plan.exp_time_us;
            ctx.create_ts = plan.now_us;
            ctx.price_offset = level.offset;
            prepared.push(PreparedOpenSignal {
                side: level.side,
                level_index: level.level_index,
                side_level_index: level.side_level_index,
                tick_index: ctx.price_count(),
                aligned_price: level.aligned_price,
                aligned_qty: level.aligned_qty,
                price_offset: level.offset,
                price_tick_count: level.price_tick_count,
                qty_tick_count: level.qty_tick_count,
                ctx,
            });
        }

        let tlen_gate = if self.enable_tlen_cancel {
            self.tlen_thresholds
                .get(&plan.symbol.to_ascii_uppercase())
                .copied()
        } else {
            None
        };
        if self.enable_tlen_cancel && tlen_gate.is_none() {
            debug!(
                "MmDecision: missing MM tlen threshold for open gating symbol={}",
                plan.symbol
            );
        }

        let gated_prepared = if prepared.is_empty() {
            Vec::new()
        } else {
            let tick_indices: Vec<i64> = prepared.iter().map(|item| item.tick_index).collect();
            let (from_keys, filtered_levels) = apply_open_tlen_gate_and_build_from_keys(
                "MmDecision: MMOpen",
                &self.depth_query_client,
                &plan.symbol,
                &tick_indices,
                from_key,
                tlen_gate,
            );
            tlen_filtered_levels += filtered_levels;
            from_keys
                .into_iter()
                .zip(prepared.into_iter())
                .filter_map(|(from_key_bytes, mut item)| {
                    let from_key_bytes = from_key_bytes?;
                    item.ctx.set_from_key(from_key_bytes);
                    Some(item)
                })
                .collect()
        };

        for item in gated_prepared.iter() {
            let signal = TradeSignal::create(SignalType::MMOpen, now_us, 0.0, item.ctx.to_bytes());
            if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
                publish_failures += 1;
                let from_key_str = String::from_utf8_lossy(&item.ctx.from_key);
                log::warn!(
                    "MmDecision: publish MMOpen failed symbol={} idx={} side_idx={} side={} price={:.8} qty={:.8} tick_index={} price_ticks={} qty_ticks={} offset={:.8} from_key='{}' err={:?}",
                    plan.symbol,
                    item.level_index,
                    item.side_level_index,
                    item.side.as_str(),
                    item.aligned_price,
                    item.aligned_qty,
                    item.tick_index,
                    item.price_tick_count,
                    item.qty_tick_count,
                    item.price_offset,
                    from_key_str,
                    err
                );
                continue;
            }

            let from_key_str = String::from_utf8_lossy(&item.ctx.from_key);
            emitted_details.push(format!(
                "{{idx:{},side_idx:{},side:\"{}\",price:{:.8},qty:{:.8},tick_index:{},price_ticks:{},qty_ticks:{},offset:{:.8},from_key:\"{}\"}}",
                item.level_index,
                item.side_level_index,
                item.side.as_str(),
                item.aligned_price,
                item.aligned_qty,
                item.tick_index,
                item.price_tick_count,
                item.qty_tick_count,
                item.price_offset,
                from_key_str
            ));
            sent += 1;
            match item.side {
                Side::Buy => sent_buy += 1,
                Side::Sell => sent_sell += 1,
            }
        }

        if !emitted_details.is_empty() {
            log::debug!(
                "MmDecision: MMOpenPlan symbol={} bid={:.8} ask={:.8} mid={:.8} volatility={:.8} price_tick={:.8} qty_tick={:.8} sent={} buy={} sell={} details=[{}]",
                plan.symbol,
                plan.quote.bid,
                plan.quote.ask,
                plan.band.mid_price,
                plan.band.volatility,
                plan.price_tick,
                plan.qty_tick,
                sent,
                sent_buy,
                sent_sell,
                emitted_details.join(",")
            );
        }

        MmOpenPublishStats {
            sent,
            sent_buy,
            sent_sell,
            prepared_levels: gated_prepared.len(),
            zero_quantized_levels,
            tlen_filtered_levels,
            publish_failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funding_rate::inline_volatility::observe_inline_volatility;
    use crate::symbol_match::normalize_symbol_for_whitelist;

    #[test]
    fn test_resolve_mm_order_amount_u_uses_symbol_override() {
        let overrides = HashMap::from([(String::from("BTCUSDT"), 150.0)]);
        let amount_u =
            resolve_mm_order_amount_u(100.0, &overrides, TradingVenue::BinanceMargin, "btc-usdt");
        assert_eq!(amount_u, 150.0);
    }

    #[test]
    fn test_resolve_mm_order_amount_u_falls_back_to_default() {
        let overrides = HashMap::from([(String::from("ETHUSDT"), 80.0)]);
        let amount_u =
            resolve_mm_order_amount_u(100.0, &overrides, TradingVenue::BinanceMargin, "btc-usdt");
        assert_eq!(amount_u, 100.0);
    }

    #[test]
    fn snapshot_open_volatility_uses_same_key_as_factor_lookup_sampling() {
        let venue_key = normalize_symbol_for_venue("BTCUSDT", TradingVenue::OkexFutures);
        for i in 0..10 {
            let _ = observe_inline_volatility(&venue_key, i as f64, 70.0, i as i64);
        }

        let snapshot = snapshot_inline_volatility(&venue_key, 10.0, 70.0);
        assert_eq!(snapshot.sample_count, 10);
        assert!(snapshot.threshold.is_some());

        let whitelist_key = normalize_symbol_for_whitelist("BTCUSDT", TradingVenue::OkexFutures);
        let mismatch_snapshot = snapshot_inline_volatility(&whitelist_key, 10.0, 70.0);
        assert_eq!(whitelist_key, "BTCUSDT");
        assert_eq!(mismatch_snapshot.sample_count, 0);
        assert!(mismatch_snapshot.threshold.is_none());
    }
}
