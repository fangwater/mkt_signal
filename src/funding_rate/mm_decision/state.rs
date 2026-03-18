use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::debug;
use std::collections::HashMap;

use super::super::common::ReturnScoreThresholdsResolved;
use super::super::factor_value_hub::{EnvironmentSignalResult, FactorValueHub};
use super::super::fr_decision::DEFAULT_SIGNAL_CHANNEL;
use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::depth_pub::query_client::DepthQueryClient;
use crate::market_maker::open_quote_plan::MmOpenQuotePlan;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::MmCancelCtx;
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
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
pub(crate) const ENV_MODEL_TRUE_THRESHOLD_DEFAULT: f64 = 0.0;

pub(crate) struct MmDecisionState {
    pub(crate) signal_pub: SignalPublisher,
    pub(crate) depth_query_client: DepthQueryClient,
    pub(crate) open_venue: TradingVenue,
    pub(crate) hedge_venue: TradingVenue,
    pub(crate) order_interval_ms: u64,
    pub(crate) open_orders_per_round: u32,
    pub(crate) hedge_orders_per_round: u32,
    pub(crate) order_amount_u: f64,
    pub(crate) open_order_ttl_us: i64,
    pub(crate) next_query_delay_ms: u64,
    pub(crate) hedge_vol_multiplier: f64,
    pub(crate) hedge_offset_ratio: f64,
    pub(crate) hedge_price_offset_limit_upper: f64,
    pub(crate) hedge_price_offset_limit_lower: f64,
    pub(crate) enable_open_cancel: bool,
    pub(crate) prediction_mode: bool,
    pub(crate) return_model_service: Option<String>,
    pub(crate) environment_model_service: Option<String>,
    pub(crate) environment_model_true_threshold: f64,
    pub(crate) return_score_thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    pub(crate) open_min_qty_table: VenueMinQtyTable,
    pub(crate) factor_value_hub: FactorValueHub,
}

impl MmDecisionState {
    pub(crate) fn new(
        node: &Node<ipc::Service>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Self> {
        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;
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
        )?;

        Ok(Self {
            signal_pub,
            depth_query_client,
            open_venue,
            hedge_venue,
            order_interval_ms: 5_000,
            open_orders_per_round: 8,
            hedge_orders_per_round: 8,
            order_amount_u: 100.0,
            open_order_ttl_us: 120_000_000,
            next_query_delay_ms: 30_000,
            hedge_vol_multiplier: 2.0,
            hedge_offset_ratio: 1.3,
            hedge_price_offset_limit_upper: 0.005,
            hedge_price_offset_limit_lower: 0.0003,
            enable_open_cancel: false,
            prediction_mode: false,
            return_model_service: None,
            environment_model_service: None,
            environment_model_true_threshold: ENV_MODEL_TRUE_THRESHOLD_DEFAULT,
            return_score_thresholds: HashMap::new(),
            open_min_qty_table: VenueMinQtyTable::new(open_venue),
            factor_value_hub,
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

    pub(crate) fn update_mm_hedge_params(
        &mut self,
        hedge_orders_per_round: u32,
        hedge_vol_multiplier: f64,
        hedge_offset_ratio: f64,
        hedge_price_offset_limit_lower: f64,
        hedge_price_offset_limit_upper: f64,
        next_query_delay_ms: u64,
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
        self.hedge_orders_per_round = hedge_orders_per_round;
        self.hedge_vol_multiplier = hedge_vol_multiplier;
        self.hedge_offset_ratio = hedge_offset_ratio;
        self.hedge_price_offset_limit_lower = hedge_price_offset_limit_lower;
        self.hedge_price_offset_limit_upper = hedge_price_offset_limit_upper;
        self.next_query_delay_ms = next_query_delay_ms;
        debug!(
            "MmDecision: hedge params updated levels={} vol_multiplier={:.6} offset_ratio={:.6} low={:.6} high={:.6} next_query_delay_ms={}",
            self.hedge_orders_per_round,
            self.hedge_vol_multiplier,
            self.hedge_offset_ratio,
            self.hedge_price_offset_limit_lower,
            self.hedge_price_offset_limit_upper,
            self.next_query_delay_ms
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
        ctx.trigger_ts = now_us;
        ctx.set_from_key(from_key.as_bytes().to_vec());

        let signal = TradeSignal::create(SignalType::MMCancel, now_us, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    pub(crate) fn publish_mm_open_plan(
        &mut self,
        now_us: i64,
        plan: &MmOpenQuotePlan,
        from_key: &str,
        prediction_side: Option<Side>,
    ) -> (usize, usize, usize) {
        struct PreparedOpenSignal {
            side: Side,
            level_index: usize,
            tick_index: i64,
            ctx: MmOpenCtx,
        }

        let mut sent = 0usize;
        let mut sent_buy = 0usize;
        let mut sent_sell = 0usize;
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
                continue;
            }
            ctx.exp_time = plan.exp_time_us;
            ctx.create_ts = plan.now_us;
            ctx.price_offset = level.offset;
            prepared.push(PreparedOpenSignal {
                side: level.side,
                level_index: level.level_index,
                tick_index: ctx.price_count(),
                ctx,
            });
        }

        let batch_from_keys = if prepared.is_empty() {
            Vec::new()
        } else {
            let tick_indices: Vec<i64> = prepared.iter().map(|item| item.tick_index).collect();
            match self
                .depth_query_client
                .query_batch_tick_indices(&plan.symbol, &tick_indices)
            {
                Ok(tlens) => prepared
                    .iter()
                    .zip(tlens.iter().copied())
                    .map(|(_, level_tlen)| {
                        super::from_key::append_mm_open_tlens_to_from_key(from_key, level_tlen)
                    })
                    .collect(),
                Err(err) => {
                    log::warn!(
                        "MmDecision: MMOpen tlen batch query failed symbol={} levels={} err={:#}",
                        plan.symbol,
                        prepared.len(),
                        err
                    );
                    prepared
                        .iter()
                        .map(|_| super::from_key::append_mm_open_tlens_to_from_key(from_key, 0.0))
                        .collect()
                }
            }
        };

        for (item, level_from_key) in prepared.iter_mut().zip(batch_from_keys.into_iter()) {
            item.ctx.set_from_key(level_from_key.into_bytes());

            let signal = TradeSignal::create(SignalType::MMOpen, now_us, 0.0, item.ctx.to_bytes());
            if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
                log::warn!(
                    "MmDecision: publish MMOpen failed symbol={} idx={} side={:?} err={:?}",
                    plan.symbol,
                    item.level_index,
                    item.side,
                    err
                );
                continue;
            }

            sent += 1;
            match item.side {
                Side::Buy => sent_buy += 1,
                Side::Sell => sent_sell += 1,
            }
        }

        (sent, sent_buy, sent_sell)
    }
}
