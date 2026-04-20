use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::time::Duration;

use super::arb_decision::DEFAULT_ARBITRAGE_BACKWARD_CHANNEL;
use super::common::{normalize_tlens_for_compare, ReturnScoreThresholdsResolved};
use super::mkt_channel::MktChannel;
use super::tlen_threshold_loader;
use crate::common::bbo::Bbo;
use crate::common::iceoryx_publisher::SIGNAL_PAYLOAD;
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::RedisSettings;
use crate::common::time_util::get_timestamp_us;
use crate::market_maker::hedge_quote_plan::{
    build_mm_hedge_ctx as build_mm_hedge_ctx_core, resolve_mm_hedge_signal_inputs,
    MmHedgeBuildInput,
};
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::MmHedgeSignalQueryMsg;
use crate::signal::mm_signal::{MmBackwardQueryMsg, MmCancelCandidateQueryMsg};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::symbol_match::normalize_symbol_for_whitelist;

mod cancel;
pub mod from_key;
mod open;
mod state;

use cancel::MmCancelDecision;
use from_key::build_mm_cancel_from_key;
use open::MmOpenDecision;
use state::MmDecisionState;

thread_local! {
    static MM_DECISION: OnceCell<RefCell<MmDecision>> = OnceCell::new();
}

pub struct MmDecision {
    state: MmDecisionState,
    open_decision: MmOpenDecision,
    cancel_decision: MmCancelDecision,
    backward_sub: GenericSignalSubscriber,
    _node: Node<ipc::Service>,
}

impl MmDecision {
    fn should_use_mm_hedge_taker(
        weighted_inventory_price: f64,
        bid: f64,
        ask: f64,
        threshold_pct: f64,
    ) -> Option<(f64, f64)> {
        if !(weighted_inventory_price.is_finite() && weighted_inventory_price > 0.0) {
            return None;
        }
        let hedge_mid = Bbo::new(bid, ask, 0).get_mid_price().unwrap_or(0.0);
        if !(hedge_mid.is_finite() && hedge_mid > 0.0) {
            return None;
        }
        let pct_change = (hedge_mid - weighted_inventory_price).abs() / weighted_inventory_price;
        let threshold_ratio = threshold_pct / 100.0;
        Some((pct_change, threshold_ratio))
    }

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

            debug!(
                "MmDecision singleton initialized, open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            Ok(())
        });
        result?;

        Self::refresh_min_qty_async(open_venue).await;
        Self::spawn_backward_listener();
        Self::spawn_tlen_threshold_loader();
        info!("MmDecision backward listener started");
        Ok(())
    }

    fn new_sync(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<Self> {
        let node_name = NodeName::new("mm_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;
        let state = MmDecisionState::new(&node, open_venue, hedge_venue)?;
        let backward_sub = Self::create_subscriber(&node, DEFAULT_ARBITRAGE_BACKWARD_CHANNEL)?;

        Ok(Self {
            state,
            open_decision: MmOpenDecision::new(),
            cancel_decision: MmCancelDecision::new(),
            backward_sub,
            _node: node,
        })
    }

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

    async fn refresh_min_qty_async(open_venue: TradingVenue) {
        let mut open_table = crate::signal::venue_min_qty_table::VenueMinQtyTable::new(open_venue);
        let open_res = open_table.refresh().await;

        Self::with_mut(|decision| {
            if open_res.is_ok() {
                decision.state.open_min_qty_table = open_table;
            }
        });

        match open_res {
            Ok(_) => debug!(
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
        self.state.update_order_interval_ms(interval_ms);
    }

    pub fn update_open_orders_per_round(&mut self, open_orders_per_round: u32) {
        self.state
            .update_open_orders_per_round(open_orders_per_round);
    }

    pub fn update_open_vol_scale_ranges(
        &mut self,
        open_buy_vol_scale: [f64; 2],
        open_sell_vol_scale: [f64; 2],
    ) {
        self.state
            .update_open_vol_scale_ranges(open_buy_vol_scale, open_sell_vol_scale);
    }

    pub fn update_mm_hedge_params(
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
        self.state.update_mm_hedge_params(
            hedge_orders_per_round,
            hedge_vol_multiplier,
            hedge_offset_ratio,
            hedge_price_offset_limit_lower,
            hedge_price_offset_limit_upper,
            hedge_window_scale_low,
            hedge_window_scale_high,
            max_hedge_price_pct_change,
            next_query_delay_ms,
            enable_return_score_adjust_hedge,
        );
    }

    pub fn update_order_amount(&mut self, order_amount: f32) {
        self.state.update_order_amount(order_amount);
    }

    pub fn update_order_amount_overrides(
        &mut self,
        overrides: std::collections::HashMap<String, f64>,
    ) {
        self.state.update_order_amount_overrides(overrides);
    }

    pub fn update_open_order_timeout(&mut self, open_order_timeout_secs: u64) {
        self.state
            .update_open_order_timeout(open_order_timeout_secs);
    }

    pub fn update_prediction_mode(&mut self, enabled: bool) {
        self.state.update_prediction_mode(enabled);
    }

    pub fn update_enable_open_cancel(&mut self, enabled: bool) {
        self.state.update_enable_open_cancel(enabled);
    }

    pub fn update_enable_tlen_cancel(&mut self, enabled: bool) {
        self.state.update_enable_tlen_cancel(enabled);
    }

    pub fn update_enable_environment_model(&mut self, enabled: bool) {
        self.state.update_enable_environment_model(enabled);
    }

    pub fn update_enable_volatility_limit(&mut self, enabled: bool) {
        self.state.update_enable_volatility_limit(enabled);
    }

    pub fn update_open_volatility_limit(&mut self, percentile: f64) {
        self.state.update_open_volatility_limit(percentile);
    }

    pub fn update_tlen_cancel_freq_ms(&mut self, tlen_cancel_freq_ms: u64) {
        self.state.update_tlen_cancel_freq_ms(tlen_cancel_freq_ms);
    }

    pub fn process_cancel_trigger_interval(&mut self) {
        if !self.state.enable_tlen_cancel || self.state.tlen_cancel_freq_ms == 0 {
            return;
        }
        let now_us = get_timestamp_us();
        let interval_us = (self.state.tlen_cancel_freq_ms as i64).saturating_mul(1_000);
        if self.state.last_cancel_trigger_ts_us != 0
            && now_us.saturating_sub(self.state.last_cancel_trigger_ts_us) < interval_us
        {
            return;
        }
        match self.state.emit_mm_cancel_trigger_signal(now_us) {
            Ok(_) => {
                self.state.last_cancel_trigger_ts_us = now_us;
                debug!(
                    "MmDecision: MMCancelTrigger emitted freq_ms={}",
                    self.state.tlen_cancel_freq_ms
                );
            }
            Err(err) => warn!("MmDecision: publish MMCancelTrigger failed: {err:#}"),
        }
    }

    pub fn update_model_service_roles(
        &mut self,
        return_model_service: String,
        environment_model_service: String,
    ) {
        self.state.update_model_service_roles(
            &self._node,
            return_model_service,
            environment_model_service,
        );
    }

    pub fn update_return_score_thresholds(
        &mut self,
        thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    ) {
        self.state.update_return_score_thresholds(thresholds);
    }

    pub fn process_open_interval(&mut self) {
        self.open_decision.process_interval(&mut self.state);
    }

    pub fn process_return_score_updates(&mut self) {
        for (symbol_key, snapshot) in self
            .state
            .factor_value_hub
            .poll_factor_value_updates_with_inline_sampling(Some(self.state.open_volatility_limit))
        {
            if snapshot.recomputed {
                debug!(
                    "MmDecision: inline open volatility threshold recomputed symbol={} threshold={:.8} samples={} percentile={:.2} last_recompute_tp_ms={}",
                    symbol_key,
                    snapshot.threshold.unwrap_or(f64::NAN),
                    snapshot.sample_count,
                    snapshot.percentile,
                    snapshot.last_recompute_tp_ms.unwrap_or_default()
                );
            }
        }
        self.cancel_decision
            .process_return_score_updates(&mut self.state);
    }

    fn handle_mm_hedge_query(&mut self, query: MmHedgeSignalQueryMsg) {
        let symbol = query.get_symbol().to_uppercase();
        if symbol.is_empty() {
            warn!("MmDecision: MMHedge query missing symbol");
            return;
        }
        let quote = match MktChannel::instance().get_quote(&symbol, self.state.hedge_venue) {
            Some(quote) => quote,
            None => {
                warn!(
                    "MmDecision: MMHedge quote unavailable symbol={} venue={:?}",
                    symbol, self.state.hedge_venue
                );
                return;
            }
        };
        let Some(model_service) = self.state.return_model_service.clone() else {
            warn!("MmDecision: MMHedge return_model_service unavailable");
            return;
        };
        let (signal, signal_qtl, volatility) = match resolve_mm_hedge_signal_inputs(
            &mut self.state.factor_value_hub,
            &model_service,
            &symbol,
            self.state.hedge_venue,
            self.state.enable_return_score_adjust_hedge,
        ) {
            Ok(values) => values,
            Err(err) => {
                warn!(
                    "MmDecision: MMHedge factor lookup failed symbol={} err={}",
                    symbol, err
                );
                return;
            }
        };
        let input = MmHedgeBuildInput {
            venue: self.state.hedge_venue,
            symbol: &symbol,
            quote,
            volatility,
            signal,
            signal_qtl,
            hedge_vol_multiplier: self.state.hedge_vol_multiplier,
            hedge_offset_ratio: self.state.hedge_offset_ratio,
            order_amount_u: self.state.resolve_order_amount_u(&symbol),
            hedge_orders_per_round: self.state.hedge_orders_per_round,
            offset_low: self.state.hedge_price_offset_limit_lower,
            offset_high_limit: self.state.hedge_price_offset_limit_upper,
            hedge_window_scale_low: self.state.hedge_window_scale_low,
            hedge_window_scale_high: self.state.hedge_window_scale_high,
            next_query_delay_ms: self.state.next_query_delay_ms,
            enable_return_score_adjust_hedge: self.state.enable_return_score_adjust_hedge,
        };
        let ctx = match build_mm_hedge_ctx_core(input, &self.state.open_min_qty_table, &query) {
            Ok(ctx) => ctx,
            Err(err) => {
                warn!(
                    "MmDecision: build MMHedge ctx failed symbol={} err={}",
                    symbol, err
                );
                return;
            }
        };
        let mut ctx = ctx;
        ctx.request_seq = query.request_seq;
        if let Some((pct_change, threshold_ratio)) = Self::should_use_mm_hedge_taker(
            query.weighted_inventory_price,
            quote.bid,
            quote.ask,
            self.state.max_hedge_price_pct_change,
        ) {
            ctx.use_taker = pct_change > threshold_ratio;
            let hedge_mid = Bbo::new(quote.bid, quote.ask, quote.ts)
                .get_mid_price()
                .unwrap_or(0.0);
            let pct_change_pct = pct_change * 100.0;
            let threshold_pct = threshold_ratio * 100.0;
            info!(
                "MmDecision: MMHedge mode decision symbol={} request_seq={} hedge_mid={:.8} weighted_inventory_price={:.8} bid={:.8} ask={:.8} compare={:.6}% {} {:.6}% -> {}",
                symbol,
                query.request_seq,
                hedge_mid,
                query.weighted_inventory_price,
                quote.bid,
                quote.ask,
                pct_change_pct,
                if ctx.use_taker { ">" } else { "<=" },
                threshold_pct,
                if ctx.use_taker { "taker" } else { "maker" }
            );
        } else {
            info!(
                "MmDecision: MMHedge mode decision symbol={} request_seq={} hedge_mid=0 weighted_inventory_price={:.8} bid={:.8} ask={:.8} compare=0.000000% <= {:.6}% -> maker note=weighted_inventory_price_or_mid_invalid",
                symbol,
                query.request_seq,
                query.weighted_inventory_price,
                quote.bid,
                quote.ask,
                self.state.max_hedge_price_pct_change
            );
        }
        let hedge_symbol = ctx.get_opening_symbol();
        let tick_indices: Vec<i64> = ctx.price_qv_list.iter().map(|qv| qv.get_count()).collect();
        if tick_indices.is_empty() {
            warn!(
                "MmDecision: MMHedge has empty tick_indices symbol={}",
                symbol
            );
            return;
        }
        match self
            .state
            .depth_query_client
            .query_batch_tick_indices(&hedge_symbol, &tick_indices)
        {
            Ok(tlens) => ctx.tlen_values = tlens,
            Err(err) => {
                warn!(
                    "MmDecision: MMHedge tlen batch query failed symbol={} levels={} err={:#}",
                    symbol,
                    tick_indices.len(),
                    err
                );
                ctx.tlen_values = vec![0.0; tick_indices.len()];
            }
        }
        let signal =
            TradeSignal::create(SignalType::MMHedge, get_timestamp_us(), 0.0, ctx.to_bytes());
        if let Err(err) = self.state.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "MmDecision: publish MMHedge failed symbol={} err={:#}",
                symbol, err
            );
            return;
        }
        info!(
            "MmDecision: MMHedge query reply symbol={} levels={} request_seq={}",
            symbol,
            ctx.price_qv_list.len(),
            ctx.request_seq
        );
    }

    fn handle_mm_cancel_candidate_query(&mut self, query: MmCancelCandidateQueryMsg) {
        if query.groups.is_empty() {
            debug!("MmDecision: MM cancel candidate query empty");
            return;
        }

        let now_us = get_timestamp_us();
        let mut cancel_sent = 0usize;
        let mut matched_symbols = 0usize;
        for group in query.groups {
            let symbol = group.get_symbol().to_uppercase();
            if symbol.is_empty() || group.items.is_empty() {
                continue;
            }
            let threshold_symbol = symbol.to_ascii_uppercase();
            let Some(threshold) = self.state.tlen_thresholds.get(&threshold_symbol).copied() else {
                debug!(
                    "MmDecision: missing MM tlen threshold symbol={}",
                    threshold_symbol
                );
                continue;
            };
            let tick_indices: Vec<i64> = group
                .items
                .iter()
                .map(|item| item.price_qv.get_count())
                .collect();
            let tlens = match self
                .state
                .depth_query_client
                .query_batch_tick_indices(&symbol, &tick_indices)
            {
                Ok(values) => normalize_tlens_for_compare(
                    "MmDecision: MMCancel",
                    &self.state.open_min_qty_table,
                    self.state.open_venue,
                    &symbol,
                    values,
                ),
                Err(err) => {
                    warn!(
                        "MmDecision: MMCancel tlen batch query failed symbol={} levels={} err={:#}",
                        symbol,
                        tick_indices.len(),
                        err
                    );
                    continue;
                }
            };
            let open_quote = match MktChannel::instance().get_quote(&symbol, self.state.open_venue)
            {
                Some(quote) => quote,
                None => {
                    warn!(
                        "MmDecision: MMCancel quote unavailable symbol={} venue={:?}",
                        symbol, self.state.open_venue
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
                let return_lookup = self.state.return_model_service.clone().map(|service_name| {
                    self.state.factor_value_hub.lookup_model_output_score(
                        &service_name,
                        &symbol,
                        self.state.hedge_venue,
                    )
                });
                let return_qtl = return_lookup
                    .as_ref()
                    .and_then(|lookup| lookup.score_quantile)
                    .filter(|value| value.is_finite());
                let volatility = self
                    .state
                    .factor_value_hub
                    .lookup_factor_value(&symbol, self.state.hedge_venue)
                    .target_factor_value
                    .filter(|value| value.is_finite());
                let symbol_key = normalize_symbol_for_whitelist(&symbol, TradingVenue::OkexFutures);
                let environment_signal =
                    self.state
                        .evaluate_environment_signal(&symbol_key, &symbol, now_us);
                let from_key = build_mm_cancel_from_key(
                    now_us,
                    return_qtl,
                    None,
                    volatility,
                    &environment_signal,
                    Some(tlen),
                    Some(threshold),
                );
                if let Err(err) = self.state.emit_mm_cancel_signal_precise(
                    &symbol,
                    open_quote,
                    now_us,
                    &from_key,
                    item.strategy_id,
                ) {
                    warn!(
                        "MmDecision: emit precise MMCancel failed symbol={} strategy_id={} err={:#}",
                        symbol,
                        item.strategy_id,
                        err
                    );
                    continue;
                }
                cancel_sent += 1;
                group_cancel_sent += 1;
            }
            debug!(
                "MmDecision: MMCancel tlen compare symbol={} trigger_ts={} candidates={} threshold={:.4} min_tlen={:.4} max_tlen={:.4} details={}",
                symbol,
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
                debug!(
                    "MmDecision: MMCancel tlen hits symbol={} trigger_ts={} candidates={} matched={} threshold={:.4} strategies={}",
                    symbol,
                    query.trigger_ts,
                    tick_indices.len(),
                    group_cancel_sent,
                    threshold,
                    matched_preview.join(",")
                );
            }
        }
        debug!(
            "MmDecision: MMCancel candidate query processed trigger_ts={} matched_symbols={} cancels_sent={}",
            query.trigger_ts, matched_symbols, cancel_sent
        );
    }

    fn handle_backward_query(&mut self, data: Bytes) {
        let query = match MmBackwardQueryMsg::from_bytes(data) {
            Ok(query) => query,
            Err(err) => {
                warn!("MmDecision: decode MM backward query failed: {err}");
                return;
            }
        };
        match query {
            MmBackwardQueryMsg::Hedge(query) => self.handle_mm_hedge_query(query),
            MmBackwardQueryMsg::CancelCandidates(query) => {
                self.handle_mm_cancel_candidate_query(query)
            }
        }
    }

    pub fn spawn_backward_listener() {
        tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                MmDecision::with_mut(|decision| loop {
                    match decision.backward_sub.receive_msg() {
                        Ok(Some(data)) => decision.handle_backward_query(data),
                        Ok(None) => break,
                        Err(err) => {
                            warn!("MmDecision: backward_sub receive error: {}", err);
                            break;
                        }
                    }
                });
            }
        });
    }

    fn tlen_threshold_reload_due(&self, now_us: i64) -> bool {
        const RELOAD_INTERVAL_US: i64 = 30 * 1_000_000;
        self.state.last_tlen_threshold_reload_ts_us == 0
            || now_us.saturating_sub(self.state.last_tlen_threshold_reload_ts_us)
                >= RELOAD_INTERVAL_US
    }

    fn spawn_tlen_threshold_loader() {
        tokio::task::spawn_local(async move {
            let redis = RedisSettings::default();
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let (due, open_venue, now_us) = MmDecision::with(|decision| {
                    let now_us = get_timestamp_us();
                    (
                        decision.tlen_threshold_reload_due(now_us),
                        decision.state.open_venue,
                        now_us,
                    )
                });
                if !due {
                    continue;
                }
                match tlen_threshold_loader::load_from_redis(&redis, open_venue).await {
                    Ok((redis_key, thresholds, bad_fields)) => {
                        let symbols = thresholds.len();
                        MmDecision::with_mut(|decision| {
                            decision.state.tlen_thresholds = thresholds;
                            decision.state.last_tlen_threshold_reload_ts_us = now_us;
                        });
                        info!(
                            "MmDecision: tlen thresholds loaded key={} symbols={} bad_fields={}",
                            redis_key, symbols, bad_fields
                        );
                    }
                    Err(err) => {
                        warn!(
                            "MmDecision: tlen threshold reload failed venue={:?} err={:#}",
                            open_venue, err
                        );
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::MmDecision;

    #[test]
    fn mm_hedge_taker_switch_uses_weighted_inventory_price() {
        let decision = MmDecision::should_use_mm_hedge_taker(100.0, 109.0, 111.0, 5.0)
            .expect("valid decision inputs");
        let (pct_change, threshold_ratio) = decision;
        assert!((pct_change - 0.10).abs() < 1e-12);
        assert!((threshold_ratio - 0.05).abs() < 1e-12);
        assert!(pct_change > threshold_ratio);
    }

    #[test]
    fn mm_hedge_taker_switch_returns_none_for_invalid_inventory_price() {
        assert!(MmDecision::should_use_mm_hedge_taker(0.0, 99.0, 101.0, 5.0).is_none());
    }
}
