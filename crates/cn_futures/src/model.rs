use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    #[serde(rename = "CFFEX")]
    Cffex,
    #[serde(rename = "SHFE")]
    Shfe,
    #[serde(rename = "INE")]
    Ine,
}
impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cffex => "CFFEX",
            Self::Shfe => "SHFE",
            Self::Ine => "INE",
        }
    }
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "CFFEX" => Ok(Self::Cffex),
            "SHFE" => Ok(Self::Shfe),
            "INE" => Ok(Self::Ine),
            _ => bail!("unsupported domestic-futures venue {value}"),
        }
    }
    pub fn separate_close_dates(self) -> bool {
        matches!(self, Self::Shfe | Self::Ine)
    }
}

/// Account/market capabilities supplied by the native adapter, not inferred from symbols.
#[derive(Debug, Clone)]
pub struct ExecutionScope {
    pub venues: HashSet<Venue>,
    pub reference_limit: u64,
    pub require_market_status: bool,
    pub cancel_requires_system_id: bool,
    pub allow_implicit_fill_reference: bool,
}
impl ExecutionScope {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.venues.is_empty() && self.reference_limit > 0,
            "empty execution scope or reference range"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}
impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Offset {
    Open,
    Close,
    CloseToday,
    CloseYesterday,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosePriority {
    TodayFirst,
    YesterdayFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
    Fak,
    Fok,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceType {
    #[default]
    Limit,
    Market,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionSelection {
    #[default]
    Any,
    Fixed,
    Preferred,
    Packed,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRoute {
    pub mode: ConnectionSelection,
    pub connection_id: Option<u8>,
}
impl ExecutionRoute {
    pub fn validate(self) -> Result<()> {
        ensure!(
            match self.mode {
                ConnectionSelection::Fixed | ConnectionSelection::Preferred =>
                    self.connection_id.is_some_and(|id| id < 64),
                _ => self.connection_id.is_none(),
            },
            "fixed/preferred requires connection_id 0..63; any/packed must omit it"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderKey {
    /// Opaque native session scope. Only the adapter interprets its contents.
    pub session: String,
    pub order_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub offset: Offset,
    pub lots: i32,
    #[serde(default)]
    pub price_type: PriceType,
    /// Integer multiples of the venue's actual price tick; must be zero for market orders.
    pub price_ticks: i64,
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub execution_route: Option<ExecutionRoute>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instrument {
    pub venue: Venue,
    pub symbol: String,
    pub price_tick: f64,
    pub min_lots: i32,
    pub max_lots: i32,
    /// None means unavailable/unsupported, never fall back to limit-order quantity rules.
    pub market_lots: Option<LotLimits>,
    pub active: bool,
    /// Local cash-budget requirement, explicitly configured; not an exchange margin quote.
    pub cash_budget_per_open_lot: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LotLimits {
    pub minimum: i32,
    pub maximum: i32,
}
impl Instrument {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            is_future_symbol(self.venue, &self.symbol),
            "invalid domestic futures symbol {}",
            self.symbol
        );
        ensure!(
            self.price_tick.is_finite() && self.price_tick > 0.0,
            "invalid price tick for {}",
            self.symbol
        );
        ensure!(
            self.min_lots > 0 && self.max_lots >= self.min_lots,
            "invalid lot limits for {}",
            self.symbol
        );
        if let Some(limits) = self.market_lots {
            ensure!(
                limits.minimum > 0 && limits.maximum >= limits.minimum,
                "invalid market lot limits for {}",
                self.symbol
            );
        }
        ensure!(
            self.cash_budget_per_open_lot.is_finite() && self.cash_budget_per_open_lot > 0.0,
            "missing positive cash budget for {}",
            self.symbol
        );
        Ok(())
    }
}

pub fn is_cffex_future(symbol: &str) -> bool {
    let n = symbol.len();
    if n < 5 || !symbol.is_ascii() {
        return false;
    }
    let (product, month) = symbol.split_at(n - 4);
    matches!(
        product,
        "IF" | "IH" | "IC" | "IM" | "TS" | "TF" | "T" | "TL"
    ) && month.bytes().all(|b| b.is_ascii_digit())
        && month[2..]
            .parse::<u8>()
            .is_ok_and(|m| (1..=12).contains(&m))
}

pub fn is_future_symbol(venue: Venue, symbol: &str) -> bool {
    if venue == Venue::Cffex {
        return is_cffex_future(symbol);
    }
    if !symbol.is_ascii() || !(5..=12).contains(&symbol.len()) {
        return false;
    }
    let (product, month) = symbol.split_at(symbol.len() - 4);
    product.bytes().all(|b| b.is_ascii_lowercase())
        && !product.is_empty()
        && month.bytes().all(|b| b.is_ascii_digit())
        && month[2..]
            .parse::<u8>()
            .is_ok_and(|m| (1..=12).contains(&m))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub today: i32,
    pub yesterday: i32,
    pub frozen_today: i32,
    pub frozen_yesterday: i32,
}
impl Position {
    pub fn total(&self) -> i64 {
        i64::from(self.today) + i64::from(self.yesterday)
    }
    pub fn frozen(&self) -> i64 {
        i64::from(self.frozen_today) + i64::from(self.frozen_yesterday)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.today >= 0
                && self.yesterday >= 0
                && self.frozen_today >= 0
                && self.frozen_yesterday >= 0,
            "negative position"
        );
        ensure!(
            self.frozen_today <= self.today && self.frozen_yesterday <= self.yesterday,
            "frozen position exceeds inventory"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    Working,
    Filled,
    Cancelled,
    Rejected,
}
impl OrderStatus {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub key: OrderKey,
    pub symbol: String,
    pub venue: Venue,
    pub system_id: String,
    pub side: Side,
    pub offset: Offset,
    pub lots: i32,
    pub traded: i32,
    pub price_type: PriceType,
    pub limit_price: f64,
    pub status: OrderStatus,
    pub cancel_rejected: bool,
    pub trading_day: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TradeIdentity {
    pub venue: Venue,
    pub symbol: String,
    pub trading_day: String,
    pub trade_id: String,
    pub side: Side,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub identity: TradeIdentity,
    pub order_ref: String,
    /// Native logical identity when the adapter can determine it without a system ID.
    pub order_key: Option<OrderKey>,
    pub system_id: String,
    pub offset: Offset,
    pub lots: i32,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub today: i32,
    pub yesterday: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub trading_day: String,
    pub session: String,
    pub max_order_ref: u64,
    pub available_cash: f64,
    pub instruments: Vec<Instrument>,
    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderUpdate>,
    pub trades: Vec<Fill>,
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

#[derive(Debug, Clone, Serialize)]
pub struct Order {
    pub request: Intent,
    pub key: OrderKey,
    pub system_id: String,
    pub limit_price: f64,
    pub status: OrderStatus,
    pub filled: i32,
    pub reported_filled: i32,
    pub cancelled: i32,
    pub reserved_today: i32,
    pub reserved_yesterday: i32,
    pub local_cash_held: f64,
    pub cancel_pending: bool,
    /// Locally submitted in this process; recovered/external orders have no strategy attribution.
    pub owned: bool,
}
impl Order {
    pub fn remaining(&self) -> i32 {
        self.request.lots - self.filled - self.cancelled
    }
}

#[derive(Debug, Clone)]
pub struct PreparedOrder {
    pub key: OrderKey,
    pub intent: Intent,
    pub limit_price: f64,
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
            scope.venues.contains(&rule.venue),
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
            scope.venues.contains(&p.venue)
                && is_future_symbol(p.venue, &p.symbol)
                && p.today >= 0
                && p.yesterday >= 0,
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
            scope.venues.contains(&o.venue) && is_future_symbol(o.venue, &o.symbol),
            "order outside deployment futures scope"
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
    if !scope.venues.contains(&f.identity.venue)
        || !is_future_symbol(f.identity.venue, &f.identity.symbol)
    {
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
