//! Account-target taker execution using native aggressive limit/FAK orders.
//! No network, disk, timers, SDK ownership or predicted position lives here.
use super::{market_data::TickRecord, strategy::*};
use crate::model::*;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub source_id: String,
    pub max_target_lots: i32,
    pub max_order_lots: i32,
    pub quote_max_age_ms: u64,
    pub target_max_age_ms: u64,
    pub retry_interval_ms: u64,
}
impl Settings {
    pub fn validate(&self, risk: &RiskLimits) -> Result<()> {
        identifier(&self.source_id)?;
        ensure!(
            self.max_target_lots > 0
                && self.max_target_lots <= risk.max_position_lots_per_side
                && self.max_order_lots > 0
                && self.max_order_lots <= risk.max_order_lots
                && (1..=5_000).contains(&self.quote_max_age_ms)
                && (1..=60_000).contains(&self.target_max_age_ms)
                && (100..=60_000).contains(&self.retry_interval_ms)
                && risk.enable_fak_fok,
            "ctaker requires explicit bounded lots/ages and tested limit/FAK support"
        );
        Ok(())
    }
}

/// A complete account target frozen under one Manager batch. It carries no
/// inferred strategy name. Native child order keys are audited against this ID.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub source_id: String,
    pub batch_id: String,
    pub target_revision: i64,
    pub trading_day: String,
    pub expires_at_us: u64,
    pub net_targets: BTreeMap<String, i32>,
}
fn identifier(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':')),
        "invalid ctaker source/batch identifier"
    );
    Ok(())
}

pub struct Ctaker {
    settings: Settings,
    scope: BTreeMap<String, (Venue, String)>,
    target: Option<Arc<Target>>,
    last: Option<Arc<Target>>,
    generation: Option<u64>,
    next_attempt_us: BTreeMap<String, u64>,
    next_tag: u64,
    halted: bool,
}
impl Ctaker {
    pub fn new(config: &crate::config::Config) -> Result<Self> {
        let settings = config.ctaker.clone().context("ctaker not configured")?;
        settings.validate(&config.risk)?;
        ensure!(
            !config.symbols.is_empty() && config.symbols.len() <= 128,
            "ctaker requires 1..128 explicit contracts"
        );
        let mut scope = BTreeMap::new();
        for symbol in &config.symbols {
            let venue = serde_json::to_value(symbol.venue)?;
            let market = format!(
                "{}:{}",
                venue.as_str().context("native venue")?,
                symbol.symbol
            );
            ensure!(
                scope
                    .insert(market, (symbol.venue, symbol.symbol.clone()))
                    .is_none(),
                "duplicate ctaker contract"
            );
        }
        Ok(Self {
            settings,
            scope,
            target: None,
            last: None,
            generation: None,
            next_attempt_us: BTreeMap::new(),
            next_tag: 0,
            halted: false,
        })
    }
    pub fn accept(
        &mut self,
        target: Target,
        context: StrategyContext<'_>,
        now_us: u64,
    ) -> Result<()> {
        identifier(&target.source_id)?;
        identifier(&target.batch_id)?;
        ensure!(
            context.can_trade && context.core.is_ready() && self.generation.is_some(),
            "ctaker target admission requires recovered account and ready MD"
        );
        ensure!(
            target.source_id == self.settings.source_id
                && target.trading_day == context.core.trading_day(),
            "ctaker target belongs to another source/trading day"
        );
        ensure!(
            target.target_revision > 0
                && target.expires_at_us > now_us
                && target.expires_at_us - now_us <= self.settings.target_max_age_ms * 1000,
            "ctaker target revision/lease invalid"
        );
        ensure!(
            target.net_targets.keys().eq(self.scope.keys())
                && target
                    .net_targets
                    .values()
                    .all(|v| i64::from(*v).abs() <= i64::from(self.settings.max_target_lots)),
            "ctaker requires the complete bounded native contract vector including zeros"
        );
        if let Some(last) = &self.last {
            if target == **last {
                ensure!(
                    self.target.is_some() && !self.halted,
                    "ctaker requires a renewed target after disconnect/rejection"
                );
                return Ok(());
            }
            ensure!(
                target.target_revision > last.target_revision && target.batch_id != last.batch_id,
                "ctaker rejects old or conflicting target batches"
            );
        }
        ensure!(
            self.scope
                .values()
                .all(|(venue, symbol)| !context.core.has_working_orders(*venue, symbol)),
            "ctaker cannot replace a target while order/fill outcomes are unresolved"
        );
        let target = Arc::new(target);
        self.target = Some(target.clone());
        self.last = Some(target);
        self.next_attempt_us.clear();
        self.halted = false;
        Ok(())
    }
    fn decide(
        &mut self,
        tick: &TickRecord,
        context: StrategyContext<'_>,
        actions: &mut Actions,
        now_us: u64,
    ) -> Result<()> {
        let Some(target) = &self.target else {
            return Ok(());
        };
        if self.halted
            || !context.can_trade
            || !context.core.is_ready()
            || self.generation != Some(tick.connection_generation)
            || now_us >= target.expires_at_us
            || tick.received_us > now_us
            || now_us - tick.received_us > self.settings.quote_max_age_ms * 1000
            || super::text_ref(&tick.trading_day)? != target.trading_day
            || target.trading_day != context.core.trading_day()
        {
            return Ok(());
        }
        let symbol = super::text_ref(&tick.symbol)?;
        let Some((market, _)) = self
            .scope
            .iter()
            .find(|(_, (venue, name))| *venue == tick.venue && name == symbol)
        else {
            return Ok(());
        };
        if context.core.has_working_orders(tick.venue, symbol)
            || self
                .next_attempt_us
                .get(market)
                .is_some_and(|t| *t > now_us)
        {
            return Ok(());
        }
        let long = context.core.position(tick.venue, symbol, Side::Buy);
        let short = context.core.position(tick.venue, symbol, Side::Sell);
        let delta = i64::from(target.net_targets[market]) - (long.total() - short.total());
        if delta == 0 {
            return Ok(());
        }
        let side = if delta > 0 { Side::Buy } else { Side::Sell };
        let opposite = if side == Side::Buy { &short } else { &long };
        let rule = context
            .core
            .instrument(tick.venue, symbol)
            .context("ctaker contract rule unavailable")?;
        if !rule.active {
            return Ok(());
        }
        let (bid, ask) = (tick.bid[0], tick.ask[0]);
        if ![bid, ask, tick.lower_limit, tick.upper_limit]
            .iter()
            .all(|p| p.is_finite() && *p > 0.0)
            || ask < bid
            || tick.lower_limit > bid
            || ask > tick.upper_limit
            || tick.bid_volume[0] <= 0
            || tick.ask_volume[0] <= 0
        {
            return Ok(());
        }
        let price = if side == Side::Buy { ask } else { bid };
        let ticks = (price / rule.price_tick).round();
        if !ticks.is_finite()
            || ticks <= 0.0
            || ticks > 9_007_199_254_740_991.0
            || (ticks * rule.price_tick - price).abs() > rule.price_tick * 1e-6
        {
            return Ok(());
        }
        let (offset, available) = if opposite.total() > 0 {
            (Offset::Close, opposite.total() - opposite.frozen())
        } else {
            (Offset::Open, delta.abs())
        };
        let lots = delta
            .abs()
            .min(available)
            .min(i64::from(self.settings.max_order_lots))
            .min(i64::from(rule.max_lots));
        if lots < i64::from(rule.min_lots) {
            return Ok(());
        }
        let tag = self
            .next_tag
            .checked_add(1)
            .context("ctaker action tag exhausted")?;
        actions.push(Action::SubmitCtaker {
            tag,
            target: target.clone(),
            intent: Intent {
                venue: tick.venue,
                symbol: symbol.into(),
                side,
                offset,
                lots: i32::try_from(lots)?,
                price_type: PriceType::Limit,
                price_ticks: ticks as i64,
                time_in_force: TimeInForce::Fak,
                execution_route: None,
            },
        })?;
        self.next_tag = tag;
        self.next_attempt_us.insert(
            market.clone(),
            now_us.saturating_add(self.settings.retry_interval_ms * 1000),
        );
        Ok(())
    }
}
impl Strategy for Ctaker {
    fn on_tick(
        &mut self,
        tick: &TickRecord,
        context: StrategyContext<'_>,
        actions: &mut Actions,
    ) -> Result<()> {
        self.decide(tick, context, actions, crate::persistence::now_us())
    }
    fn on_target(
        &mut self,
        target: Target,
        context: StrategyContext<'_>,
        now_us: u64,
    ) -> Result<()> {
        self.accept(target, context, now_us)
    }
    fn on_market_state(&mut self, ready: bool, generation: u64) -> Result<()> {
        if !ready || self.generation != Some(generation) {
            self.target = None;
        }
        self.generation = ready.then_some(generation);
        Ok(())
    }
    fn on_result(&mut self, outcome: &ActionOutcome, _: StrategyContext<'_>) -> Result<()> {
        if matches!(outcome, ActionOutcome::Rejected { .. })
            || matches!(outcome,ActionOutcome::Submitted{receipt,..} if !receipt.all_submitted_locally)
        {
            self.halted = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
