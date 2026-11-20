//! Strategy-target execution, the domestic counterpart of crypto pre_trade.
//! Exec owns the per-strategy position ledger: it crosses opposite strategy
//! gaps internally at the quote mid, trades only the account net, and
//! attributes each of its own fills when the venue reports it. How the net gap
//! is worked is the per-contract execution algorithm ([`super::algo`]).
//! No network, disk, timers, SDK ownership or predicted position lives here.
use super::algo::{
    Algo, AlgoConfig, ChaseParameters, Command, Commands, ExecutionAlgorithm, LiveOrder,
    LiveOrders, Step,
};
use super::{market_data::TickRecord, strategy::*};
use crate::model::*;
use anyhow::{ensure, Context, Result};
use cn_futures_target::ledger::{Ledger, Row, Transfer, UNALLOCATED};
pub use cn_futures_target::Target;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
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
    #[serde(default)]
    pub algorithm: ExecutionAlgorithm,
    #[serde(default)]
    pub chase: ChaseParameters,
}
impl Settings {
    pub fn algo(&self) -> AlgoConfig {
        AlgoConfig {
            algorithm: self.algorithm,
            chase: self.chase,
        }
    }
    pub fn validate(&self, risk: &RiskLimits) -> Result<()> {
        identifier(&self.source_id)?;
        self.algo().validate()?;
        if let Some(contracts) = &self.contracts {
            ensure!(
                (1..=cn_futures_target::MAX_CONTRACTS).contains(&contracts.len()),
                "invalid CTP exec contract scope"
            );
            for market in contracts {
                let (exchange, symbol) = market.split_once(':').context("missing target venue")?;
                ensure!(
                    is_future_symbol(Venue::parse(exchange)?, symbol),
                    "invalid CTP exec contract: {market}"
                );
            }
        }
        ensure!(
            (1..=5_000).contains(&self.quote_max_age_ms)
                && (1..=60_000).contains(&self.target_max_age_ms)
                && (100..=60_000).contains(&self.retry_interval_ms)
                && risk.enable_fak_fok,
            "exec requires bounded quote/target ages and tested limit/FAK support"
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
) -> Result<Vec<(Venue, &'a str, bool)>> {
    target.validate_shape()?;
    ensure!(
        target.source_id == settings.source_id && target.trading_day == core.trading_day(),
        "exec target belongs to another source/trading day"
    );
    ensure!(
        target.target_revision > 0
            && target.expires_at_us > now_us
            && target.expires_at_us - now_us <= settings.target_max_age_ms * 1000,
        "exec target revision/lease invalid"
    );
    let mut contracts = Vec::new();
    for market in target.markets() {
        // Crossing opposite strategy targets needs the contract's quote even at
        // a flat net; held ledger rows are added by the caller.
        let required = needs_contract(
            core,
            venue_of(market)?,
            symbol_of(market)?,
            target.net(market),
        ) || target
            .strategies
            .values()
            .any(|vector| vector.get(market).is_some_and(|lots| *lots != 0));
        let (exchange, symbol) = market.split_once(':').context("missing target venue")?;
        let venue = Venue::parse(exchange)?;
        ensure!(
            is_future_symbol(venue, symbol)
                || (allow_options && option_symbol_parts(venue, symbol).is_some()),
            "invalid exec native contract: {market}"
        );
        if settings
            .contracts
            .as_ref()
            .is_some_and(|scope| !scope.contains(market))
        {
            ensure!(
                !required,
                "CTP target requires execution outside the configured contracts: {market}"
            );
            continue;
        }
        contracts.push((venue, symbol, required));
    }
    Ok(contracts)
}

fn venue_of(market: &str) -> Result<Venue> {
    Venue::parse(market.split_once(':').context("missing target venue")?.0)
}
fn symbol_of(market: &str) -> Result<&str> {
    Ok(market.split_once(':').context("missing target venue")?.1)
}

pub(crate) fn needs_contract(
    core: &crate::core::TradingCore,
    venue: Venue,
    symbol: &str,
    lots: i64,
) -> bool {
    lots != 0
        || core.position(venue, symbol, Side::Buy).total()
            != core.position(venue, symbol, Side::Sell).total()
}

fn identifier(value: &str) -> Result<()> {
    ensure!(
        cn_futures_target::valid_id(value),
        "invalid exec source/batch identifier"
    );
    Ok(())
}

/// `EXCHANGE:symbol`, the contract key of targets and the strategy ledger.
pub type Market = Text<96>;

pub struct Exec {
    settings: Settings,
    fixed_scope: Option<FixedScope>,
    target: Option<Arc<Target>>,
    last: Option<Arc<Target>>,
    generation: Option<u64>,
    next_tag: u64,
    halted: bool,
    wait_note: Option<WaitNote>,
    ledger: Ledger,
    ledger_events: Vec<LedgerEvent>,
    /// Contracts of the last accepted target, built at admission so the quote
    /// path only looks them up.
    contracts: Vec<Contract>,
    index: HashMap<Venue, HashMap<Symbol, usize>>,
    commands: Commands,
}

struct Contract {
    venue: Venue,
    symbol: Symbol,
    market: Market,
    /// Name-sorted strategy targets.
    targets: Vec<(String, i64)>,
    net: i64,
    algo: Algo,
    /// Last reason noted in the trader log.
    wait: &'static str,
}

/// One strategy-ledger change with its replacement contract row. Encoded
/// compactly on the business loop; persist_manager renders it as the
/// `strategy_cross` / `strategy_fill_allocation` evidence Manager replays.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LedgerEvent {
    StrategyCross(StrategyCross),
    StrategyFillAllocation(FillAllocation),
}
/// Opposite strategy gaps (or the unallocated remainder) swapped at the mid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyCross {
    pub source_id: String,
    pub batch_id: String,
    pub target_revision: i64,
    pub market: String,
    pub price: f64,
    pub quote_received_us: u64,
    pub transfers: Vec<Transfer>,
    pub ledger: Row,
}
/// One live factual fill split between strategies. Fills of other sessions'
/// orders, or without a target for the contract, go wholly to the remainder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillAllocation {
    pub source_id: Option<String>,
    pub batch_id: Option<String>,
    pub target_revision: Option<i64>,
    pub market: String,
    pub trade: TradeIdentity,
    pub lots: i64,
    pub price: f64,
    pub allocations: Row,
    pub ledger: Row,
}
impl LedgerEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::StrategyCross(_) => "strategy_cross",
            Self::StrategyFillAllocation(_) => "strategy_fill_allocation",
        }
    }
    pub fn market(&self) -> &str {
        match self {
            Self::StrategyCross(e) => &e.market,
            Self::StrategyFillAllocation(e) => &e.market,
        }
    }
    pub fn ledger(&self) -> &Row {
        match self {
            Self::StrategyCross(e) => &e.ledger,
            Self::StrategyFillAllocation(e) => &e.ledger,
        }
    }
    /// The evidence JSON object, `type` first like every native record.
    pub fn evidence(&self) -> serde_json::Result<serde_json::Value> {
        let body = match self {
            Self::StrategyCross(e) => serde_json::to_value(e)?,
            Self::StrategyFillAllocation(e) => serde_json::to_value(e)?,
        };
        let mut value = serde_json::Map::with_capacity(10);
        value.insert("type".into(), self.kind().into());
        if let serde_json::Value::Object(fields) = body {
            value.extend(fields);
        }
        Ok(serde_json::Value::Object(value))
    }
}

/// Latest per-contract reason a target quote did or did not produce an order action.
pub struct WaitNote {
    pub target: Arc<Target>,
    pub market: Market,
    pub reason: &'static str,
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
    #[serde(default)]
    algorithm: ExecutionAlgorithm,
    #[serde(default)]
    chase: ChaseParameters,
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
            "Yida exec lot limits must be within account risk limits"
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
            algorithm: self.algorithm,
            chase: self.chase,
        }
    }
}
impl Exec {
    pub fn new(config: &crate::config::Config) -> Result<Self> {
        let settings = config.exec.clone().context("exec not configured")?;
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
            ensure!(scope.insert(market), "duplicate exec contract");
        }
        ensure!(
            (1..=cn_futures_target::MAX_CONTRACTS).contains(&scope.len()),
            "exec requires 1..128 explicit contracts"
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
            target: None,
            last: None,
            generation: None,
            next_tag: 0,
            halted: false,
            wait_note: None,
            ledger: Ledger::default(),
            ledger_events: Vec::new(),
            contracts: Vec::new(),
            index: HashMap::new(),
            commands: Commands::new(),
        }
    }
    /// Durable attribution from persist_manager; missing rows are unallocated.
    pub fn restore_ledger(
        &mut self,
        rows: Vec<(String, cn_futures_target::ledger::Row)>,
    ) -> Result<()> {
        self.ledger = Ledger::from_rows(rows)?;
        Ok(())
    }
    /// Operator cutover: credits part of the unallocated account position to
    /// `strategy` at `price` (its cost), recorded like an internal cross.
    pub fn assign_position(
        &mut self,
        core: &crate::core::TradingCore,
        strategy: &str,
        market: &str,
        lots: i64,
        price: f64,
        now_us: u64,
    ) -> Result<()> {
        ensure!(
            core.is_ready(),
            "position assignment requires a recovered account"
        );
        ensure!(
            price.is_finite() && price > 0.0,
            "position assignment needs a positive price"
        );
        let venue = venue_of(market)?;
        let symbol = symbol_of(market)?;
        let account_net = core.position(venue, symbol, Side::Buy).total()
            - core.position(venue, symbol, Side::Sell).total();
        let transfer = self.ledger.assign(market, strategy, lots, account_net)?;
        self.ledger_events
            .push(LedgerEvent::StrategyCross(StrategyCross {
                source_id: self.settings.source_id.clone(),
                batch_id: "assign_position".into(),
                target_revision: 0,
                market: market.to_owned(),
                price,
                quote_received_us: now_us,
                transfers: vec![transfer],
                ledger: self.ledger.row(market),
            }));
        Ok(())
    }
    /// Whether any strategy still holds attribution on this contract.
    pub fn attributes(&self, market: &str) -> bool {
        !self.ledger.row(market).is_empty()
    }
    pub fn take_ledger_events(&mut self) -> Vec<LedgerEvent> {
        std::mem::take(&mut self.ledger_events)
    }
    /// Attributes a newly applied live factual fill. Fills of other sessions'
    /// orders, or without an accepted target, stay in the unallocated remainder.
    pub fn on_fill(&mut self, fill: &Fill, own_order: bool) {
        let lots = i64::from(fill.lots)
            * if fill.identity.side == Side::Buy {
                1
            } else {
                -1
            };
        let contract = self.contract(fill.identity.venue, &fill.identity.symbol);
        let attributed = self
            .last
            .as_ref()
            .filter(|_| own_order)
            .zip(contract.map(|i| &self.contracts[i]));
        let (market, allocations, target) = match attributed {
            Some((target, contract)) => (
                contract.market.to_string(),
                self.ledger
                    .allocate_fill(&contract.market, &contract.targets, lots),
                Some(target),
            ),
            None => (
                format!("{}:{}", fill.identity.venue.as_str(), fill.identity.symbol),
                Row::from([(UNALLOCATED.to_owned(), lots)]),
                None,
            ),
        };
        self.ledger_events
            .push(LedgerEvent::StrategyFillAllocation(FillAllocation {
                source_id: target.map(|t| t.source_id.clone()),
                batch_id: target.map(|t| t.batch_id.clone()),
                target_revision: target.map(|t| t.target_revision),
                ledger: self.ledger.row(&market),
                market,
                trade: fill.identity,
                lots,
                price: fill.price,
                allocations,
            }));
    }
    fn contract(&self, venue: Venue, symbol: &str) -> Option<usize> {
        self.index.get(&venue)?.get(symbol).copied()
    }
    pub fn accept(
        &mut self,
        target: Target,
        context: StrategyContext<'_>,
        now_us: u64,
    ) -> Result<Arc<Target>> {
        ensure!(
            context.can_trade && context.core.is_ready() && self.generation.is_some(),
            "exec target admission requires recovered account and ready MD"
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
                target.markets().eq(scope.contracts.iter()),
                "exec requires the complete fixed native contract vector including zeros"
            );
            ensure!(
                target
                    .markets()
                    .all(|market| target.net(market).abs() <= i64::from(scope.max_target_lots)),
                "Yida exec target exceeds account lot limit"
            );
        } else {
            for &(venue, symbol, required) in &contracts {
                let market = format!("{}:{symbol}", venue.as_str());
                if required || self.attributes(&market) {
                    ensure!(
                        context
                            .core
                            .instrument(venue, symbol)
                            .is_some_and(|rule| rule.active),
                        "exec contract rule unavailable: {}:{symbol}",
                        venue.as_str()
                    );
                }
            }
        }
        if let Some(last) = &self.last {
            if target == **last {
                ensure!(
                    self.target.is_some() && !self.halted,
                    "exec requires a renewed target after disconnect/rejection"
                );
                return Ok(last.clone());
            }
            ensure!(
                target.target_revision > last.target_revision && target.batch_id != last.batch_id,
                "exec rejects old or conflicting target batches"
            );
        }
        // Like crypto targets, a new target replaces the old one at any time; an
        // in-flight FAK's late fills are attributed against the new strategy gaps.
        if self.fixed_scope.is_none() {
            for contract in &self.contracts {
                ensure!(
                    target.contains(&contract.market)
                        || !needs_contract(context.core, contract.venue, &contract.symbol, 0),
                    "exec target omits held contract {}; send an explicit zero",
                    contract.market
                );
            }
        }
        let mut previous: HashMap<Market, Algo> = self
            .contracts
            .drain(..)
            .map(|contract| (contract.market, contract.algo))
            .collect();
        let mut index: HashMap<Venue, HashMap<Symbol, usize>> = HashMap::new();
        for (venue, symbol, _) in contracts {
            let symbol = Symbol::new(symbol)?;
            let market = Market::format(format_args!("{}:{symbol}", venue.as_str()))?;
            let mut algo = previous.remove(&market).unwrap_or_else(|| {
                Algo::new(
                    &self.settings.algo(),
                    self.settings.retry_interval_ms * 1000,
                )
            });
            algo.on_target();
            index
                .entry(venue)
                .or_default()
                .insert(symbol, self.contracts.len());
            self.contracts.push(Contract {
                venue,
                symbol,
                targets: target.strategy_targets(&market),
                net: target.net(&market),
                market,
                algo,
                wait: "",
            });
        }
        self.index = index;
        let target = Arc::new(target);
        self.target = Some(target.clone());
        self.last = Some(target.clone());
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
        let contract = self
            .target
            .as_ref()
            .and_then(|_| self.contract(quote.venue, quote.symbol));
        let reason = self.decide_quote(&quote, contract, context, actions, now_us)?;
        // The callback gate flips with every queued venue/MD event; blocked
        // actions are logged at dispatch, so only stable reasons are noted here.
        if let (Some(index), Some(target)) = (contract, &self.target) {
            let contract = &mut self.contracts[index];
            if reason != WAIT_GATE_CLOSED && contract.wait != reason {
                contract.wait = reason;
                self.wait_note = Some(WaitNote {
                    target: target.clone(),
                    market: contract.market,
                    reason,
                });
            }
        }
        Ok(())
    }

    pub fn take_wait_note(&mut self) -> Option<WaitNote> {
        self.wait_note.take()
    }

    fn decide_quote(
        &mut self,
        quote: &Quote<'_>,
        contract: Option<usize>,
        context: StrategyContext<'_>,
        actions: &mut Actions,
        now_us: u64,
    ) -> Result<&'static str> {
        let Some(target) = &self.target else {
            return Ok("no_target");
        };
        let blocked = if self.halted {
            Some("halted_after_rejection")
        } else if !context.can_trade {
            Some(WAIT_GATE_CLOSED)
        } else if !context.core.is_ready() {
            Some("account_not_ready")
        } else if self.generation != Some(quote.generation) {
            Some("quote_generation_mismatch")
        } else if now_us >= target.expires_at_us {
            Some("target_lease_expired")
        } else if quote.received_us > now_us {
            Some("quote_from_future")
        } else if now_us - quote.received_us > self.settings.quote_max_age_ms * 1000 {
            Some("quote_stale")
        } else if quote.trading_day != target.trading_day {
            Some("quote_trading_day_mismatch")
        } else if target.trading_day != context.core.trading_day() {
            Some("account_trading_day_mismatch")
        } else {
            None
        };
        if let Some(reason) = blocked {
            return Ok(reason);
        }
        let Some(index) = contract else {
            return Ok("contract_not_in_target");
        };
        let contract = &mut self.contracts[index];
        let (venue, symbol) = (contract.venue, quote.symbol);
        let long = context.core.position(venue, symbol, Side::Buy);
        let short = context.core.position(venue, symbol, Side::Sell);
        let account_net = long.total() - short.total();
        if !book_valid(quote) {
            return Ok("quote_book_invalid");
        }
        if self
            .ledger
            .needs_cross(&contract.market, &contract.targets, account_net)
        {
            let transfers = self
                .ledger
                .cross(&contract.market, &contract.targets, account_net);
            self.ledger_events
                .push(LedgerEvent::StrategyCross(StrategyCross {
                    source_id: target.source_id.clone(),
                    batch_id: target.batch_id.clone(),
                    target_revision: target.target_revision,
                    market: contract.market.to_string(),
                    price: quote.bid / 2.0 + quote.ask / 2.0,
                    quote_received_us: quote.received_us,
                    transfers,
                    ledger: self.ledger.row(&contract.market),
                }));
        }
        let Some(rule) = context.core.instrument(venue, symbol) else {
            return Ok("contract_rule_missing");
        };
        if !rule.active {
            return Ok("contract_inactive");
        }
        let (Some(bid_ticks), Some(ask_ticks)) = (
            ticks(quote.bid, rule.price_tick),
            ticks(quote.ask, rule.price_tick),
        ) else {
            return Ok("price_tick_invalid");
        };
        let delta = contract.net - account_net;
        let side = if delta > 0 { Side::Buy } else { Side::Sell };
        let opposite = if side == Side::Buy { &short } else { &long };
        let (offset, available) = if opposite.total() > 0 {
            (Offset::Close, opposite.total() - opposite.frozen())
        } else {
            (Offset::Open, delta.abs())
        };
        let mut tradable = delta.abs().min(available).min(i64::from(rule.max_lots));
        if let Some(scope) = &self.fixed_scope {
            tradable = tradable.min(i64::from(scope.max_order_lots));
        }
        if tradable < i64::from(rule.min_lots) {
            tradable = 0;
        }
        let live: LiveOrders = context
            .core
            .working_orders(venue, symbol)
            .map(|order| LiveOrder {
                key: *order.key,
                side: order.request.side,
                price_ticks: order.request.price_ticks,
                remaining: order.remaining(),
                time_in_force: order.request.time_in_force,
                terminal: order.status.terminal(),
                cancel_pending: order.cancel_pending,
                owned: order.owned,
            })
            .collect();
        self.commands.clear();
        let reason = contract.algo.step(
            &Step {
                delta,
                tradable,
                bid_ticks,
                ask_ticks,
                live: &live,
                now_us,
            },
            &mut self.commands,
        );
        for command in self.commands.iter().take(actions.remaining()) {
            let tag = self
                .next_tag
                .checked_add(1)
                .context("exec action tag exhausted")?;
            actions.push(match *command {
                Command::Place {
                    side,
                    lots,
                    price_ticks,
                    time_in_force,
                } => Action::SubmitExec {
                    tag,
                    target: target.clone(),
                    intent: Intent {
                        venue,
                        symbol: contract.symbol,
                        side,
                        offset,
                        lots: i32::try_from(lots)?,
                        price_type: PriceType::Limit,
                        price_ticks,
                        time_in_force,
                        execution_route: None,
                    },
                },
                Command::Cancel(key) => Action::Cancel { tag, key },
            })?;
            self.next_tag = tag;
        }
        Ok(reason)
    }
}

/// Exact tick count of a validated book price.
fn ticks(price: f64, tick: f64) -> Option<i64> {
    let ticks = (price / tick).round();
    (ticks.is_finite()
        && ticks > 0.0
        && ticks <= 9_007_199_254_740_991.0
        && (ticks * tick - price).abs() <= tick * 1e-6)
        .then_some(ticks as i64)
}

const WAIT_GATE_CLOSED: &str = "execution_gate_closed";

fn book_valid(quote: &Quote<'_>) -> bool {
    let (bid, ask) = (quote.bid, quote.ask);
    [bid, ask, quote.lower_limit, quote.upper_limit]
        .iter()
        .all(|p| p.is_finite() && *p > 0.0)
        && ask >= bid
        && quote.lower_limit <= bid
        && ask <= quote.upper_limit
        && quote.bid_lots > 0
        && quote.ask_lots > 0
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
impl Strategy for Exec {
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
    fn take_wait_note(&mut self) -> Option<WaitNote> {
        Exec::take_wait_note(self)
    }
    fn restore_ledger(
        &mut self,
        rows: Vec<(String, cn_futures_target::ledger::Row)>,
    ) -> Result<()> {
        Exec::restore_ledger(self, rows)
    }
    fn on_fill(&mut self, fill: &Fill, own_order: bool) {
        Exec::on_fill(self, fill, own_order)
    }
    fn take_ledger_events(&mut self) -> Vec<LedgerEvent> {
        Exec::take_ledger_events(self)
    }
    fn attributes(&self, market: &str) -> bool {
        Exec::attributes(self, market)
    }
    fn assign_position(
        &mut self,
        core: &crate::core::TradingCore,
        strategy: &str,
        market: &str,
        lots: i64,
        price: f64,
        now_us: u64,
    ) -> Result<()> {
        Exec::assign_position(self, core, strategy, market, lots, price, now_us)
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
