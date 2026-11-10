//! Shared domestic trading records. Native identities and lot/offset semantics stay intact.
use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};

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
    pub option: Option<OptionContract>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionRight {
    Call,
    Put,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionContract {
    pub right: OptionRight,
    pub underlying: String,
    pub strike_price: f64,
    pub contract_multiple: i32,
    pub expiry_date: i32,
    pub upper_limit_price: f64,
    pub cash_buffer_per_buy_lot: f64,
}
impl OptionContract {
    pub fn validate(&self, venue: Venue, symbol: &str) -> Result<()> {
        let (underlying, right, strike) = option_symbol_parts(venue, symbol)
            .ok_or_else(|| anyhow::anyhow!("invalid SHFE/INE option symbol {symbol}"))?;
        ensure!(
            underlying == self.underlying
                && right == self.right
                && self.strike_price.is_finite()
                && self.strike_price > 0.0
                && (strike - self.strike_price).abs() <= self.strike_price * 1e-12
                && self.contract_multiple > 0
                && self.expiry_date.to_string().len() == 8
                && self.upper_limit_price.is_finite()
                && self.upper_limit_price >= 0.0
                && self.cash_buffer_per_buy_lot.is_finite()
                && self.cash_buffer_per_buy_lot >= 0.0,
            "inconsistent native option contract metadata for {symbol}"
        );
        Ok(())
    }
}

pub fn option_symbol_parts(venue: Venue, symbol: &str) -> Option<(&str, OptionRight, f64)> {
    if !matches!(venue, Venue::Shfe | Venue::Ine) || !symbol.is_ascii() || symbol.len() >= 32 {
        return None;
    }
    let separator = symbol.find(['C', 'P'])?;
    let (underlying, suffix) = symbol.split_at(separator);
    if !is_future_symbol(venue, underlying) {
        return None;
    }
    let strike = &suffix[1..];
    if strike.is_empty() || strike.bytes().any(|b| !b.is_ascii_digit() && b != b'.') {
        return None;
    }
    let price = strike.parse::<f64>().ok()?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    Some((
        underlying,
        if suffix.starts_with('C') {
            OptionRight::Call
        } else {
            OptionRight::Put
        },
        price,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LotLimits {
    pub minimum: i32,
    pub maximum: i32,
}
impl Instrument {
    pub fn validate(&self) -> Result<()> {
        if let Some(option) = &self.option {
            option.validate(self.venue, &self.symbol)?;
            ensure!(
                option.cash_buffer_per_buy_lot > 0.0,
                "positive option buyer cash buffer required"
            );
        } else {
            ensure!(
                is_future_symbol(self.venue, &self.symbol),
                "invalid domestic futures symbol {}",
                self.symbol
            );
        }
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
    pub time_in_force: TimeInForce,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub cash_reserved_per_lot: f64,
    pub cancel_pending: bool,
    /// Locally submitted in this process; recovered/external orders have no strategy attribution.
    pub owned: bool,
}
impl Order {
    pub fn remaining(&self) -> i32 {
        self.request.lots - self.filled - self.cancelled
    }
}

/// Account identity is bound once to a database. Hash inputs include adapter and endpoint/broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreIdentity {
    pub adapter: String,
    pub account_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistedOrderKey {
    pub trading_day: String,
    pub venue: Venue,
    pub native: OrderKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendState {
    Prepared,
    SubmittedLocally,
    RejectedLocally,
    Uncertain,
    Observed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeOrderRecord {
    pub identity: PersistedOrderKey,
    pub order: Order,
    /// Exact local request, retained when SDK recovery reconstructs a less complete request.
    pub submitted_intent: Option<Intent>,
    /// Local origin is historical provenance, never strategy ownership in a new process.
    pub locally_submitted: bool,
    pub send_state: SendState,
    pub instrument: Option<Instrument>,
    pub first_received_us: u64,
    pub updated_us: u64,
    pub evidence_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeTradeRecord {
    pub fill: Fill,
    /// None means the SDK did not report a fee. Currency is CNY for Yida, unknown for CTP.
    pub commission: Option<f64>,
    pub commission_currency: Option<String>,
    /// SDK native time is retained verbatim; it is not a Unix timestamp.
    pub native_time: Option<i32>,
    pub received_us: u64,
    pub evidence_seq: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NativeMutation {
    pub orders: Vec<NativeOrderRecord>,
    pub trades: Vec<NativeTradeRecord>,
    /// Set only for a complete, validated SDK recovery. Missing unresolved requests block trading.
    pub reconciliation_day: Option<String>,
    /// Keys in `orders` backed by an actual venue order report.
    pub observed: Vec<PersistedOrderKey>,
}
