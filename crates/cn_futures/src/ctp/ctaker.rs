//! Account-target taker execution using native aggressive limit/FAK orders.
//! No network, disk, timers, SDK ownership or predicted position lives here.
use super::{market_data::TickRecord, strategy::*};
use crate::model::*;
use anyhow::{ensure, Context, Result};
pub use cn_futures_target::Target;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

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

fn identifier(value: &str) -> Result<()> {
    ensure!(
        cn_futures_target::valid_id(value),
        "invalid ctaker source/batch identifier"
    );
    Ok(())
}

pub struct Ctaker {
    settings: Settings,
    scope: BTreeMap<String, (Venue, String)>,
    markets: HashMap<Venue, HashMap<String, String>>,
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
        Self::with_scope(
            settings,
            &config.risk,
            config.symbols.iter().map(|s| (s.venue, s.symbol.as_str())),
        )
    }

    pub(crate) fn with_scope<'a>(
        settings: Settings,
        risk: &RiskLimits,
        contracts: impl IntoIterator<Item = (Venue, &'a str)>,
    ) -> Result<Self> {
        settings.validate(risk)?;
        let mut scope = BTreeMap::new();
        let mut markets: HashMap<Venue, HashMap<String, String>> = HashMap::new();
        for (venue, symbol) in contracts {
            let market = format!("{}:{symbol}", venue.as_str());
            ensure!(
                scope
                    .insert(market.clone(), (venue, symbol.to_owned()))
                    .is_none(),
                "duplicate ctaker contract"
            );
            markets
                .entry(venue)
                .or_default()
                .insert(symbol.to_owned(), market);
        }
        ensure!(
            (1..=cn_futures_target::MAX_CONTRACTS).contains(&scope.len()),
            "ctaker requires 1..128 explicit contracts"
        );
        Ok(Self {
            settings,
            scope,
            markets,
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
    ) -> Result<Arc<Target>> {
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
                return Ok(last.clone());
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
        self.last = Some(target.clone());
        self.next_attempt_us.clear();
        self.halted = false;
        Ok(target)
    }
    fn decide(
        &mut self,
        tick: &TickRecord,
        context: StrategyContext<'_>,
        actions: &mut Actions,
        now_us: u64,
    ) -> Result<()> {
        self.on_quote(
            Quote {
                venue: tick.venue,
                symbol: super::text_ref(&tick.symbol)?,
                trading_day: super::text_ref(&tick.trading_day)?,
                generation: tick.connection_generation,
                received_us: tick.received_us,
                bid: tick.bid[0],
                ask: tick.ask[0],
                bid_lots: tick.bid_volume[0],
                ask_lots: tick.ask_volume[0],
                lower_limit: tick.lower_limit,
                upper_limit: tick.upper_limit,
            },
            context,
            actions,
            now_us,
        )
    }

    pub(crate) fn on_quote(
        &mut self,
        quote: Quote<'_>,
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
            || self.generation != Some(quote.generation)
            || now_us >= target.expires_at_us
            || quote.received_us > now_us
            || now_us - quote.received_us > self.settings.quote_max_age_ms * 1000
            || quote.trading_day != target.trading_day
            || target.trading_day != context.core.trading_day()
        {
            return Ok(());
        }
        let symbol = quote.symbol;
        let Some(market) = self
            .markets
            .get(&quote.venue)
            .and_then(|markets| markets.get(symbol))
        else {
            return Ok(());
        };
        if context.core.has_working_orders(quote.venue, symbol)
            || self
                .next_attempt_us
                .get(market)
                .is_some_and(|t| *t > now_us)
        {
            return Ok(());
        }
        let long = context.core.position(quote.venue, symbol, Side::Buy);
        let short = context.core.position(quote.venue, symbol, Side::Sell);
        let delta = i64::from(target.net_targets[market]) - (long.total() - short.total());
        if delta == 0 {
            return Ok(());
        }
        let side = if delta > 0 { Side::Buy } else { Side::Sell };
        let opposite = if side == Side::Buy { &short } else { &long };
        let rule = context
            .core
            .instrument(quote.venue, symbol)
            .context("ctaker contract rule unavailable")?;
        if !rule.active {
            return Ok(());
        }
        let (bid, ask) = (quote.bid, quote.ask);
        if ![bid, ask, quote.lower_limit, quote.upper_limit]
            .iter()
            .all(|p| p.is_finite() && *p > 0.0)
            || ask < bid
            || quote.lower_limit > bid
            || ask > quote.upper_limit
            || quote.bid_lots <= 0
            || quote.ask_lots <= 0
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
                venue: quote.venue,
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

pub(crate) struct Quote<'a> {
    pub venue: Venue,
    pub symbol: &'a str,
    pub trading_day: &'a str,
    pub generation: u64,
    pub received_us: u64,
    pub bid: f64,
    pub ask: f64,
    pub bid_lots: i32,
    pub ask_lots: i32,
    pub lower_limit: f64,
    pub upper_limit: f64,
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
    ) -> Result<Arc<Target>> {
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
