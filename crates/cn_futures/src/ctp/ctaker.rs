//! Account-target taker execution using native aggressive limit/FAK orders.
//! No network, disk, timers, SDK ownership or predicted position lives here.
use super::{market_data::TickRecord, strategy::*};
use crate::model::*;
use anyhow::{ensure, Context, Result};
pub use cn_futures_target::Target;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub source_id: String,
    pub contracts: Option<BTreeSet<String>>,
    pub quote_max_age_ms: u64,
    pub target_max_age_ms: u64,
    pub retry_interval_ms: u64,
}
impl Settings {
    pub fn validate(&self, risk: &RiskLimits) -> Result<()> {
        identifier(&self.source_id)?;
        if let Some(contracts) = &self.contracts {
            ensure!(
                (1..=cn_futures_target::MAX_CONTRACTS).contains(&contracts.len()),
                "invalid CTP ctaker contract scope"
            );
            for market in contracts {
                let (exchange, symbol) = market.split_once(':').context("missing target venue")?;
                ensure!(
                    is_future_symbol(Venue::parse(exchange)?, symbol),
                    "invalid CTP ctaker contract: {market}"
                );
            }
        }
        ensure!(
            (1..=5_000).contains(&self.quote_max_age_ms)
                && (1..=60_000).contains(&self.target_max_age_ms)
                && (100..=60_000).contains(&self.retry_interval_ms)
                && risk.enable_fak_fok,
            "ctaker requires bounded quote/target ages and tested limit/FAK support"
        );
        Ok(())
    }
}

pub(crate) fn validate_target<'a>(
    settings: &Settings,
    target: &'a Target,
    core: &crate::core::TradingCore,
    now_us: u64,
    allow_options: bool,
) -> Result<Vec<(Venue, &'a str, i32)>> {
    identifier(&target.source_id)?;
    identifier(&target.batch_id)?;
    ensure!(
        target.source_id == settings.source_id && target.trading_day == core.trading_day(),
        "ctaker target belongs to another source/trading day"
    );
    ensure!(
        target.target_revision > 0
            && target.expires_at_us > now_us
            && target.expires_at_us - now_us <= settings.target_max_age_ms * 1000,
        "ctaker target revision/lease invalid"
    );
    ensure!(
        (1..=cn_futures_target::MAX_CONTRACTS).contains(&target.net_targets.len()),
        "ctaker requires a bounded native contract vector including zeros"
    );
    let mut contracts = Vec::with_capacity(target.net_targets.len());
    for (market, &lots) in &target.net_targets {
        let (exchange, symbol) = market.split_once(':').context("missing target venue")?;
        let venue = Venue::parse(exchange)?;
        ensure!(
            is_future_symbol(venue, symbol)
                || (allow_options && option_symbol_parts(venue, symbol).is_some()),
            "invalid ctaker native contract: {market}"
        );
        if settings
            .contracts
            .as_ref()
            .is_some_and(|scope| !scope.contains(market))
        {
            ensure!(
                !needs_contract(core, venue, symbol, lots),
                "CTP target requires execution outside the configured contracts: {market}"
            );
            continue;
        }
        contracts.push((venue, symbol, lots));
    }
    Ok(contracts)
}

pub(crate) fn needs_contract(
    core: &crate::core::TradingCore,
    venue: Venue,
    symbol: &str,
    lots: i32,
) -> bool {
    lots != 0
        || core.position(venue, symbol, Side::Buy).total()
            != core.position(venue, symbol, Side::Sell).total()
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
    fixed_scope: Option<FixedScope>,
    markets: HashMap<Venue, HashMap<String, String>>,
    target: Option<Arc<Target>>,
    last: Option<Arc<Target>>,
    generation: Option<u64>,
    next_attempt_us: BTreeMap<String, u64>,
    next_tag: u64,
    halted: bool,
}

struct FixedScope {
    contracts: BTreeSet<String>,
    max_target_lots: i32,
    max_order_lots: i32,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YidaSettings {
    pub source_id: String,
    max_target_lots: i32,
    max_order_lots: i32,
    quote_max_age_ms: u64,
    target_max_age_ms: u64,
    retry_interval_ms: u64,
}

impl YidaSettings {
    pub fn validate(&self, risk: &RiskLimits) -> Result<()> {
        ensure!(
            self.max_target_lots > 0
                && risk
                    .max_position_lots_per_side
                    .is_some_and(|limit| self.max_target_lots <= limit)
                && self.max_order_lots > 0
                && risk
                    .max_order_lots
                    .is_some_and(|limit| self.max_order_lots <= limit),
            "Yida ctaker lot limits must be within account risk limits"
        );
        self.timing().validate(risk)
    }

    fn timing(&self) -> Settings {
        Settings {
            source_id: self.source_id.clone(),
            contracts: None,
            quote_max_age_ms: self.quote_max_age_ms,
            target_max_age_ms: self.target_max_age_ms,
            retry_interval_ms: self.retry_interval_ms,
        }
    }
}
impl Ctaker {
    pub fn new(config: &crate::config::Config) -> Result<Self> {
        let settings = config.ctaker.clone().context("ctaker not configured")?;
        settings.validate(&config.risk)?;
        Ok(Self::from_scope(settings, None))
    }

    #[cfg(feature = "yida-sdk")]
    pub(crate) fn with_scope<'a>(
        settings: YidaSettings,
        risk: &RiskLimits,
        contracts: impl IntoIterator<Item = (Venue, &'a str)>,
    ) -> Result<Self> {
        settings.validate(risk)?;
        let mut scope = BTreeSet::new();
        for (venue, symbol) in contracts {
            let market = format!("{}:{symbol}", venue.as_str());
            ensure!(scope.insert(market), "duplicate ctaker contract");
        }
        ensure!(
            (1..=cn_futures_target::MAX_CONTRACTS).contains(&scope.len()),
            "ctaker requires 1..128 explicit contracts"
        );
        Ok(Self::from_scope(
            settings.timing(),
            Some(FixedScope {
                contracts: scope,
                max_target_lots: settings.max_target_lots,
                max_order_lots: settings.max_order_lots,
            }),
        ))
    }

    fn from_scope(settings: Settings, fixed_scope: Option<FixedScope>) -> Self {
        Self {
            settings,
            fixed_scope,
            markets: HashMap::new(),
            target: None,
            last: None,
            generation: None,
            next_attempt_us: BTreeMap::new(),
            next_tag: 0,
            halted: false,
        }
    }
    pub fn accept(
        &mut self,
        target: Target,
        context: StrategyContext<'_>,
        now_us: u64,
    ) -> Result<Arc<Target>> {
        ensure!(
            context.can_trade && context.core.is_ready() && self.generation.is_some(),
            "ctaker target admission requires recovered account and ready MD"
        );
        let contracts = validate_target(
            &self.settings,
            &target,
            context.core,
            now_us,
            self.fixed_scope.is_some(),
        )?;
        if let Some(scope) = &self.fixed_scope {
            ensure!(
                target.net_targets.keys().eq(scope.contracts.iter()),
                "ctaker requires the complete fixed native contract vector including zeros"
            );
            ensure!(
                contracts
                    .iter()
                    .all(|&(_, _, lots)| i64::from(lots).abs() <= i64::from(scope.max_target_lots)),
                "Yida ctaker target exceeds account lot limit"
            );
        } else {
            for &(venue, symbol, lots) in &contracts {
                if needs_contract(context.core, venue, symbol, lots) {
                    ensure!(
                        context
                            .core
                            .instrument(venue, symbol)
                            .is_some_and(|rule| rule.active),
                        "ctaker contract rule unavailable: {}:{symbol}",
                        venue.as_str()
                    );
                }
            }
        }
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
        if self.fixed_scope.is_none() {
            for (&venue, markets) in &self.markets {
                for (symbol, market) in markets {
                    ensure!(
                        !context.core.has_working_orders(venue, symbol),
                        "ctaker cannot replace a target while order/fill outcomes are unresolved"
                    );
                    ensure!(
                        target.net_targets.contains_key(market)
                            || !needs_contract(context.core, venue, symbol, 0),
                        "ctaker target omits held contract {market}; send an explicit zero"
                    );
                }
            }
        }
        let mut markets: HashMap<Venue, HashMap<String, String>> = HashMap::new();
        for (venue, symbol, _) in contracts {
            ensure!(
                !context.core.has_working_orders(venue, symbol),
                "ctaker cannot replace a target while order/fill outcomes are unresolved"
            );
            markets
                .entry(venue)
                .or_default()
                .insert(symbol.to_owned(), format!("{}:{symbol}", venue.as_str()));
        }
        self.markets = markets;
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
        let Some(rule) = context.core.instrument(quote.venue, symbol) else {
            return Ok(());
        };
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
        let mut lots = delta.abs().min(available).min(i64::from(rule.max_lots));
        if let Some(scope) = &self.fixed_scope {
            lots = lots.min(i64::from(scope.max_order_lots));
        }
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
        #[cfg(any(feature = "ctp-sdk", test))]
        if let ActionOutcome::CtpDispatchBlocked { checks, .. } = outcome {
            // The action was stopped before reservation or an SDK call. Only
            // transient callback backlog can retry on a later fresh tick.
            self.halted |= !checks.can_retry();
        }
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
