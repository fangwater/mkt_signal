use anyhow::{bail, ensure, Result};
pub use persist_common::cn_futures::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Account/market capabilities supplied by the native adapter, not inferred from symbols.
#[derive(Debug, Clone)]
pub struct ExecutionScope {
    pub venues: HashSet<Venue>,
    pub reference_limit: u64,
    pub require_market_status: bool,
    pub cancel_requires_system_id: bool,
    pub allow_implicit_fill_reference: bool,
    pub allow_options: bool,
}
impl ExecutionScope {
    pub fn supports_symbol(&self, venue: Venue, symbol: &str) -> bool {
        self.venues.contains(&venue)
            && (is_future_symbol(venue, symbol)
                || (self.allow_options && option_symbol_parts(venue, symbol).is_some()))
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.venues.is_empty() && self.reference_limit > 0,
            "empty execution scope or reference range"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskLimits {
    pub max_order_lots: i32,
    pub max_position_lots_per_side: i32,
    pub max_working_lots: i32,
    pub max_actions_per_second: usize,
    pub minimum_cash_remaining: f64,
    pub enable_fak_fok: bool,
    /// CFFEX Close does not encode a today/yesterday allocation. Confirm this with the broker.
    pub close_priority: ClosePriority,
}
impl RiskLimits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_order_lots > 0
                && self.max_position_lots_per_side > 0
                && self.max_working_lots > 0
                && self.max_actions_per_second > 0,
            "risk limits must be positive"
        );
        ensure!(
            self.minimum_cash_remaining.is_finite() && self.minimum_cash_remaining >= 0.0,
            "invalid minimum cash remaining"
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PreparedOrder {
    pub key: OrderKey,
    pub intent: Intent,
    pub limit_price: f64,
    pub option_market_buy_ceiling: f64,
}
#[derive(Debug, Clone)]
pub struct PreparedCancel {
    pub key: OrderKey,
    pub symbol: String,
    pub venue: Venue,
    pub system_id: String,
    pub execution_route: Option<ExecutionRoute>,
}

pub enum BatchSend {
    SubmittedLocally,
    RejectedBeforeSend { index: usize, code: i32 },
}

pub trait Trader {
    /// Some native SDKs own the reference allocator. Called before local reservation/send.
    fn allocate_reference(&mut self, minimum: u64) -> Result<u64> {
        Ok(minimum)
    }
    /// Nonzero is a documented synchronous SDK rejection, not an asynchronous business reject.
    fn insert(&mut self, order: &PreparedOrder) -> Result<i32>;
    fn cancel(&mut self, order: &PreparedCancel) -> Result<i32>;
    fn insert_batch(&mut self, _: &[PreparedOrder]) -> Result<BatchSend> {
        bail!("native batch submission unsupported")
    }
    fn cancel_batch(&mut self, _: &[PreparedCancel]) -> Result<BatchSend> {
        bail!("native batch cancellation unsupported")
    }
}

pub(crate) fn validate_snapshot(s: &Snapshot, scope: &ExecutionScope) -> Result<()> {
    ensure!(
        s.trading_day.len() == 8 && s.trading_day.bytes().all(|b| b.is_ascii_digit()),
        "invalid trading day"
    );
    ensure!(
        !s.session.is_empty() && s.session.is_ascii() && s.session.len() <= 128,
        "invalid native session scope"
    );
    ensure!(s.available_cash.is_finite(), "nonfinite account available");
    let mut symbols = HashSet::new();
    for rule in &s.instruments {
        rule.validate()?;
        ensure!(
            scope.supports_symbol(rule.venue, &rule.symbol),
            "instrument outside deployment scope"
        );
        ensure!(
            symbols.insert((rule.venue, &rule.symbol)),
            "duplicate instrument {}",
            rule.symbol
        );
    }
    ensure!(!symbols.is_empty(), "empty instrument snapshot");
    let mut positions = HashSet::new();
    for p in &s.positions {
        ensure!(
            scope.supports_symbol(p.venue, &p.symbol) && p.today >= 0 && p.yesterday >= 0,
            "invalid position {}",
            p.symbol
        );
        ensure!(
            positions.insert((p.venue, &p.symbol, p.side)),
            "duplicate position {}",
            p.symbol
        );
    }
    let mut trades = HashMap::new();
    for f in &s.trades {
        validate_fill(f, &s.trading_day, scope)?;
        if let Some(previous) = trades.insert(&f.identity, f) {
            ensure!(previous == f, "conflicting duplicate trade in snapshot");
        }
    }
    for o in &s.orders {
        ensure!(
            scope.supports_symbol(o.venue, &o.symbol),
            "order outside deployment contract scope"
        );
        ensure!(
            o.trading_day == s.trading_day,
            "order trading day differs from snapshot"
        );
        ensure!(
            o.lots > 0 && o.traded >= 0 && o.traded <= o.lots && o.limit_price.is_finite(),
            "invalid order snapshot"
        );
        ensure!(
            o.limit_price >= 0.0
                && (o.status.terminal()
                    || o.price_type == PriceType::Market
                    || o.limit_price > 0.0),
            "working order without a usable limit price requires reconciliation"
        );
        ensure!(
            o.status != OrderStatus::Filled || o.traded == o.lots,
            "filled order has incomplete cumulative quantity"
        );
        ensure!(
            o.status != OrderStatus::Rejected || o.traded == 0,
            "rejected order has reported fills"
        );
        let factual_lots: i64 = trades
            .values()
            .filter(|f| {
                f.identity.venue == o.venue && !o.system_id.is_empty() && f.system_id == o.system_id
            })
            .map(|f| i64::from(f.lots))
            .sum();
        ensure!(
            factual_lots == i64::from(o.traded),
            "order/trade query quantities have not reconciled"
        );
    }
    Ok(())
}

pub(crate) fn validate_fill(f: &Fill, trading_day: &str, scope: &ExecutionScope) -> Result<()> {
    if !scope.supports_symbol(f.identity.venue, &f.identity.symbol) {
        bail!("fill outside configured futures scope");
    }
    ensure!(
        f.identity.trading_day == trading_day && !f.identity.trade_id.trim().is_empty(),
        "invalid fill day or trade identity"
    );
    ensure!(
        f.lots > 0 && f.price.is_finite() && f.price > 0.0,
        "invalid factual fill"
    );
    Ok(())
}

/// One parsed factual update reused by bootstrap, OMS and persistence.
#[derive(Debug)]
pub enum DecodedUpdate {
    None,
    Order(OrderUpdate),
    Trade(Fill),
}
impl DecodedUpdate {
    pub fn order(&self) -> Option<&OrderUpdate> {
        if let Self::Order(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn fill(&self) -> Option<&Fill> {
        if let Self::Trade(v) = self {
            Some(v)
        } else {
            None
        }
    }
}
