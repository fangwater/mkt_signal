//! Shared domestic trading records. Native identities and lot/offset semantics stay intact.
//! Identities are fixed-capacity `Copy` text, so the trading path never allocates
//! for them; serde keeps the original string representation.
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};

pub use crate::text::Text;
/// Native contract code (CTP InstrumentID capacity).
pub type Symbol = Text<81>;
/// Native trading day, e.g. `20261120`.
pub type Day = Text<16>;
/// Opaque adapter session scope, e.g. `ctp:<front>:<session>`.
pub type Session = Text<48>;
/// Native order reference.
pub type Reference = Text<16>;
/// Venue-assigned system or trade identifier.
pub type NativeId = Text<64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    #[serde(rename = "CFFEX")]
    Cffex,
    #[serde(rename = "SHFE")]
    Shfe,
    #[serde(rename = "INE")]
    Ine,
    #[serde(rename = "DCE")]
    Dce,
    #[serde(rename = "CZCE")]
    Czce,
    #[serde(rename = "GFEX")]
    Gfex,
}
impl Venue {
    pub const ALL: [Self; 6] = [
        Self::Cffex,
        Self::Shfe,
        Self::Ine,
        Self::Dce,
        Self::Czce,
        Self::Gfex,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cffex => "CFFEX",
            Self::Shfe => "SHFE",
            Self::Ine => "INE",
            Self::Dce => "DCE",
            Self::Czce => "CZCE",
            Self::Gfex => "GFEX",
        }
    }
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "CFFEX" => Ok(Self::Cffex),
            "SHFE" => Ok(Self::Shfe),
            "INE" => Ok(Self::Ine),
            "DCE" => Ok(Self::Dce),
            "CZCE" => Ok(Self::Czce),
            "GFEX" => Ok(Self::Gfex),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderKey {
    /// Opaque native session scope. Only the adapter interprets its contents.
    pub session: Session,
    pub order_ref: Reference,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub venue: Venue,
    pub symbol: Symbol,
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
    pub symbol: Symbol,
    pub price_tick: f64,
    pub contract_multiple: i32,
    pub expiry_date: i32,
    pub commission: Option<CommissionRate>,
    pub margin: Option<MarginRate>,
    pub min_lots: i32,
    pub max_lots: i32,
    /// None means unavailable/unsupported, never fall back to limit-order quantity rules.
    pub market_lots: Option<LotLimits>,
    pub active: bool,
    /// Optional configured floor when complete venue margin and commission rates are available.
    pub cash_budget_per_open_lot: f64,
    pub option: Option<OptionContract>,
}

/// Account-specific rates returned by the venue, never an inferred fill fee.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CommissionRate {
    pub open_by_money: f64,
    pub open_by_volume: f64,
    pub close_by_money: f64,
    pub close_by_volume: f64,
    pub close_today_by_money: f64,
    pub close_today_by_volume: f64,
}
impl CommissionRate {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            [
                self.open_by_money,
                self.open_by_volume,
                self.close_by_money,
                self.close_by_volume,
                self.close_today_by_money,
                self.close_today_by_volume
            ]
            .iter()
            .all(|x| x.is_finite() && *x >= 0.0),
            "invalid commission rates"
        );
        Ok(())
    }
    pub fn estimate(&self, offset: Offset, price: f64, multiple: i32, lots: i32) -> Result<f64> {
        self.validate()?;
        ensure!(
            price.is_finite() && price > 0.0 && multiple > 0 && lots > 0,
            "invalid fee inputs"
        );
        let (money, volume) = match offset {
            Offset::Open => (self.open_by_money, self.open_by_volume),
            Offset::CloseToday => (self.close_today_by_money, self.close_today_by_volume),
            Offset::Close | Offset::CloseYesterday => (self.close_by_money, self.close_by_volume),
        };
        let mut per_lot = price * f64::from(multiple) * money + volume;
        // Generic close does not identify which date the exchange will consume.
        if offset == Offset::Close {
            per_lot = per_lot.max(
                price * f64::from(multiple) * self.close_today_by_money
                    + self.close_today_by_volume,
            );
        }
        let result = per_lot * f64::from(lots);
        ensure!(result.is_finite(), "commission overflow");
        Ok(result)
    }
}
/// Effective absolute investor rates. Relative broker add-ons are combined with
/// independently queried exchange rates before constructing this value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MarginRate {
    pub long_by_money: f64,
    pub long_by_volume: f64,
    pub short_by_money: f64,
    pub short_by_volume: f64,
}
impl MarginRate {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            [
                self.long_by_money,
                self.long_by_volume,
                self.short_by_money,
                self.short_by_volume
            ]
            .iter()
            .all(|x| x.is_finite() && *x >= 0.0),
            "invalid margin rates"
        );
        Ok(())
    }
    pub fn estimate(&self, side: Side, price: f64, multiple: i32, lots: i32) -> Result<f64> {
        self.validate()?;
        ensure!(
            price.is_finite() && price > 0.0 && multiple > 0 && lots > 0,
            "invalid margin inputs"
        );
        let (money, volume) = match side {
            Side::Buy => (self.long_by_money, self.long_by_volume),
            Side::Sell => (self.short_by_money, self.short_by_volume),
        };
        let result = (price * f64::from(multiple) * money + volume) * f64::from(lots);
        ensure!(result.is_finite(), "margin overflow");
        Ok(result)
    }
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
    pub underlying: Symbol,
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
                option.contract_multiple == self.contract_multiple
                    && option.expiry_date == self.expiry_date,
                "option/common contract metadata differs for {}",
                self.symbol
            );
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
            self.contract_multiple > 0,
            "missing positive contract multiplier for {}",
            self.symbol
        );
        let year = self.expiry_date / 10000;
        let month = self.expiry_date / 100 % 100;
        let day = self.expiry_date % 100;
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days = match month {
            2 => {
                if leap {
                    29
                } else {
                    28
                }
            }
            4 | 6 | 9 | 11 => 30,
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            _ => 0,
        };
        ensure!(
            (1900..=9999).contains(&year) && (1..=days).contains(&day),
            "invalid contract expiry for {}",
            self.symbol
        );
        if let Some(rate) = self.commission {
            rate.validate()?;
        }
        if let Some(rate) = self.margin {
            rate.validate()?;
        }
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
            self.cash_budget_per_open_lot.is_finite()
                && (self.cash_budget_per_open_lot > 0.0
                    || (self.cash_budget_per_open_lot == 0.0
                        && self.option.is_none()
                        && self.margin.is_some()
                        && self.commission.is_some())),
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
    let digits = if venue == Venue::Czce { 3 } else { 4 };
    if !symbol.is_ascii() || !(digits + 1..=digits + 3).contains(&symbol.len()) {
        return false;
    }
    let (product, month) = symbol.split_at(symbol.len() - digits);
    product.bytes().all(|b| {
        if venue == Venue::Czce {
            b.is_ascii_uppercase()
        } else {
            b.is_ascii_lowercase()
        }
    }) && !product.is_empty()
        && month.bytes().all(|b| b.is_ascii_digit())
        && month[digits - 2..]
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub key: OrderKey,
    pub symbol: Symbol,
    pub venue: Venue,
    pub system_id: NativeId,
    pub side: Side,
    pub offset: Offset,
    pub lots: i32,
    pub traded: i32,
    pub price_type: PriceType,
    pub time_in_force: TimeInForce,
    pub limit_price: f64,
    pub status: OrderStatus,
    pub cancel_rejected: bool,
    pub trading_day: Day,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TradeIdentity {
    pub venue: Venue,
    pub symbol: Symbol,
    pub trading_day: Day,
    pub trade_id: NativeId,
    pub side: Side,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub identity: TradeIdentity,
    pub order_ref: Reference,
    /// Native logical identity when the adapter can determine it without a system ID.
    pub order_key: Option<OrderKey>,
    pub system_id: NativeId,
    pub offset: Offset,
    pub lots: i32,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub venue: Venue,
    pub symbol: Symbol,
    pub side: Side,
    pub today: i32,
    pub yesterday: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub trading_day: Day,
    pub session: Session,
    pub max_order_ref: u64,
    pub available_cash: f64,
    pub instruments: Vec<Instrument>,
    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderUpdate>,
    pub trades: Vec<Fill>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub request: Intent,
    pub key: OrderKey,
    pub system_id: NativeId,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistedOrderKey {
    pub trading_day: Day,
    pub venue: Venue,
    pub native: OrderKey,
}

/// `session/order_ref`, the native order identity carried in the record
/// extension (`client_order_text`) of domestic uniform orders and trade updates.
pub type ClientOrderText = Text<80>;

impl PersistedOrderKey {
    /// Stable 63-bit `client_order_id` of a native order, shared by its uniform
    /// orders and trade updates; the exact identity travels as `client_order_text`.
    pub fn client_order_id(&self) -> i64 {
        // FNV-1a over length-delimited parts.
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for part in [
            self.trading_day.as_bytes(),
            &[self.venue as u8],
            self.native.session.as_bytes(),
            self.native.order_ref.as_bytes(),
        ] {
            for byte in (part.len() as u16).to_le_bytes().iter().chain(part) {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
        }
        (hash & i64::MAX as u64) as i64
    }
    pub fn client_order_text(&self) -> ClientOrderText {
        Text::format(format_args!(
            "{}/{}",
            self.native.session, self.native.order_ref
        ))
        .expect("session and reference capacities fit the client order text")
    }
    pub fn from_client_order_text(trading_day: Day, venue: Venue, text: &str) -> Result<Self> {
        let (session, order_ref) = text
            .rsplit_once('/')
            .with_context(|| format!("invalid native client order text {text}"))?;
        Ok(Self {
            trading_day,
            venue,
            native: OrderKey {
                session: Text::new(session)?,
                order_ref: Text::new(order_ref)?,
            },
        })
    }
}

impl Venue {
    pub fn trading_venue(self) -> order_common::TradingVenue {
        use order_common::TradingVenue as T;
        match self {
            Self::Cffex => T::CffexFutures,
            Self::Shfe => T::ShfeFutures,
            Self::Ine => T::IneFutures,
            Self::Dce => T::DceFutures,
            Self::Czce => T::CzceFutures,
            Self::Gfex => T::GfexFutures,
        }
    }
    pub fn from_trading_venue(venue: order_common::TradingVenue) -> Option<Self> {
        use order_common::TradingVenue as T;
        Some(match venue {
            T::CffexFutures => Self::Cffex,
            T::ShfeFutures => Self::Shfe,
            T::IneFutures => Self::Ine,
            T::DceFutures => Self::Dce,
            T::CzceFutures => Self::Czce,
            T::GfexFutures => Self::Gfex,
            _ => return None,
        })
    }
    pub fn from_code(code: u8) -> Option<Self> {
        order_common::TradingVenue::from_u8(code).and_then(Self::from_trading_venue)
    }
    /// `EXCHANGE:symbol`, the contract key of targets and strategy attribution.
    pub fn market(self, symbol: &str) -> String {
        format!("{}:{symbol}", self.as_str())
    }
}

impl OrderStatus {
    /// Cross-market order status. A rejection is terminal without fills, which
    /// the crypto vocabulary calls expired; `filled` distinguishes partial fills.
    pub fn uniform(self, filled: i32) -> order_common::OrderStatus {
        use order_common::OrderStatus as U;
        match self {
            Self::Pending => U::New,
            Self::Working if filled > 0 => U::PartiallyFilled,
            Self::Working => U::New,
            Self::Filled => U::Filled,
            Self::Cancelled => U::Canceled,
            Self::Rejected => U::Expired,
        }
    }
}

impl Offset {
    pub fn position_effect(self) -> order_common::PositionEffect {
        use order_common::PositionEffect as P;
        match self {
            Self::Open => P::Open,
            Self::Close => P::Close,
            Self::CloseToday => P::CloseToday,
            Self::CloseYesterday => P::CloseYesterday,
        }
    }
}

/// The domestic store: crypto-format records only, keyed by a 20-digit
/// microsecond timestamp like crypto `uniform_orders` / `trade_updates`.
/// persist_manager is the only writer; Exec and Manager open it read-only.
pub mod store_cf {
    pub const ORDERS: &str = "cn_uniform_orders";
    pub const TRADES: &str = "cn_trade_updates";
    pub const ALL: &[&str] = &[ORDERS, TRADES];
}

#[cfg(test)]
mod contract_economics_tests {
    use super::*;
    #[test]
    fn native_symbols_cover_all_six_exchanges_without_case_or_month_inference() {
        for (venue, symbol) in [
            (Venue::Cffex, "IF2612"),
            (Venue::Shfe, "rb2701"),
            (Venue::Ine, "sc2701"),
            (Venue::Dce, "m2701"),
            (Venue::Czce, "SR701"),
            (Venue::Gfex, "lc2701"),
        ] {
            assert!(is_future_symbol(venue, symbol));
            assert_eq!(Venue::parse(venue.as_str()).unwrap(), venue);
        }
        for (venue, symbol) in [
            (Venue::Czce, "sr701"),
            (Venue::Dce, "M2701"),
            (Venue::Gfex, "lc2713"),
            (Venue::Czce, "SR700"),
            (Venue::Shfe, "SP rb2701&rb2705"),
        ] {
            assert!(!is_future_symbol(venue, symbol));
        }
        assert!(Venue::Shfe.separate_close_dates() && Venue::Ine.separate_close_dates());
        assert!(!Venue::Dce.separate_close_dates() && !Venue::Czce.separate_close_dates());
    }
    #[test]
    fn commission_uses_both_notional_and_per_lot_rates_and_distinguishes_close_today() {
        let rate = CommissionRate {
            open_by_money: 0.000023,
            open_by_volume: 2.0,
            close_by_money: 0.000023,
            close_by_volume: 1.0,
            close_today_by_money: 0.00023,
            close_today_by_volume: 3.0,
        };
        assert!((rate.estimate(Offset::Open, 4000.0, 300, 2).unwrap() - 59.2).abs() < 1e-8);
        assert!((rate.estimate(Offset::CloseToday, 4000.0, 300, 2).unwrap() - 558.0).abs() < 1e-8);
        assert!((rate.estimate(Offset::Close, 4000.0, 300, 2).unwrap() - 558.0).abs() < 1e-8);
        assert!(
            (rate
                .estimate(Offset::CloseYesterday, 4000.0, 300, 2)
                .unwrap()
                - 57.2)
                .abs()
                < 1e-8
        );
        assert!(rate.estimate(Offset::Open, 4000.0, 0, 2).is_err());
        let margin = MarginRate {
            long_by_money: 0.12,
            long_by_volume: 2.0,
            short_by_money: 0.15,
            short_by_volume: 5.0,
        };
        assert_eq!(
            margin.estimate(Side::Buy, 4000.0, 300, 1).unwrap(),
            144002.0
        );
        assert_eq!(
            margin.estimate(Side::Sell, 4000.0, 300, 1).unwrap(),
            180005.0
        );
    }
}
