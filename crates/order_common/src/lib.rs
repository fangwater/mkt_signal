use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Trading venue across exchange and market type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ValueEnum)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    BinanceMargin = 0,
    BinanceFutures = 1,
    OkexMargin = 2,
    OkexFutures = 3,
    BybitMargin = 4,
    BybitFutures = 5,
    BitgetMargin = 6,
    BitgetFutures = 7,
    GateMargin = 8,
    GateFutures = 9,
    AsterMargin = 10,
    AsterFutures = 11,
    HyperliquidMargin = 12,
    HyperliquidFutures = 13,
}

impl TradingVenue {
    pub fn trade_engine_exchange(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => "binance",
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => "okex",
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => "bybit",
            TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => "bitget",
            TradingVenue::GateMargin | TradingVenue::GateFutures => "gate",
            TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures => "hyperliquid",
            TradingVenue::AsterMargin | TradingVenue::AsterFutures => "aster",
        }
    }

    pub fn data_pub_slug(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin => "binance-margin",
            TradingVenue::BinanceFutures => "binance-futures",
            TradingVenue::OkexMargin => "okex-margin",
            TradingVenue::OkexFutures => "okex-futures",
            TradingVenue::BybitMargin => "bybit-margin",
            TradingVenue::BybitFutures => "bybit-futures",
            TradingVenue::BitgetMargin => "bitget-margin",
            TradingVenue::BitgetFutures => "bitget-futures",
            TradingVenue::GateMargin => "gate-margin",
            TradingVenue::GateFutures => "gate-futures",
            TradingVenue::AsterMargin => "aster-margin",
            TradingVenue::AsterFutures => "aster-futures",
            TradingVenue::HyperliquidMargin => "hyperliquid-margin",
            TradingVenue::HyperliquidFutures => "hyperliquid-futures",
        }
    }

    pub fn describe_u8(value: u8) -> String {
        Self::from_u8(value)
            .map(|venue| format!("{:?}", venue))
            .unwrap_or_else(|| format!("Unknown({})", value))
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(TradingVenue::BinanceMargin),
            1 => Some(TradingVenue::BinanceFutures),
            2 => Some(TradingVenue::OkexMargin),
            3 => Some(TradingVenue::OkexFutures),
            4 => Some(TradingVenue::BybitMargin),
            5 => Some(TradingVenue::BybitFutures),
            6 => Some(TradingVenue::BitgetMargin),
            7 => Some(TradingVenue::BitgetFutures),
            8 => Some(TradingVenue::GateMargin),
            9 => Some(TradingVenue::GateFutures),
            10 => Some(TradingVenue::AsterMargin),
            11 => Some(TradingVenue::AsterFutures),
            12 => Some(TradingVenue::HyperliquidMargin),
            13 => Some(TradingVenue::HyperliquidFutures),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin => "BinanceMargin",
            TradingVenue::BinanceFutures => "BinanceFutures",
            TradingVenue::OkexMargin => "OkexMargin",
            TradingVenue::OkexFutures => "OkexFutures",
            TradingVenue::BybitMargin => "BybitMargin",
            TradingVenue::BybitFutures => "BybitFutures",
            TradingVenue::BitgetMargin => "BitgetMargin",
            TradingVenue::BitgetFutures => "BitgetFutures",
            TradingVenue::GateMargin => "GateMargin",
            TradingVenue::GateFutures => "GateFutures",
            TradingVenue::AsterMargin => "AsterMargin",
            TradingVenue::AsterFutures => "AsterFutures",
            TradingVenue::HyperliquidMargin => "HyperliquidMargin",
            TradingVenue::HyperliquidFutures => "HyperliquidFutures",
        }
    }

    pub fn exchange_name(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => "binance",
            TradingVenue::OkexFutures => "okex_futures",
            TradingVenue::OkexMargin => "okex_margin",
            TradingVenue::BybitMargin => "bybit_margin",
            TradingVenue::BybitFutures => "bybit_futures",
            TradingVenue::BitgetMargin => "bitget_margin",
            TradingVenue::BitgetFutures => "bitget_futures",
            TradingVenue::GateMargin => "gate_margin",
            TradingVenue::GateFutures => "gate_futures",
            TradingVenue::AsterMargin => "aster_margin",
            TradingVenue::AsterFutures => "aster_futures",
            TradingVenue::HyperliquidMargin => "hyperliquid_margin",
            TradingVenue::HyperliquidFutures => "hyperliquid_futures",
        }
    }

    pub fn venue_type(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin => "margin",
            TradingVenue::BinanceFutures => "futures",
            TradingVenue::OkexFutures => "futures",
            TradingVenue::OkexMargin => "margin",
            TradingVenue::BybitMargin => "margin",
            TradingVenue::BitgetMargin => "margin",
            TradingVenue::BybitFutures => "futures",
            TradingVenue::BitgetFutures => "futures",
            TradingVenue::GateMargin => "margin",
            TradingVenue::GateFutures => "futures",
            TradingVenue::AsterMargin => "margin",
            TradingVenue::AsterFutures => "futures",
            TradingVenue::HyperliquidMargin => "margin",
            TradingVenue::HyperliquidFutures => "futures",
        }
    }

    pub fn is_futures(&self) -> bool {
        matches!(
            self,
            TradingVenue::BinanceFutures
                | TradingVenue::OkexFutures
                | TradingVenue::BitgetFutures
                | TradingVenue::BybitFutures
                | TradingVenue::GateFutures
                | TradingVenue::AsterFutures
                | TradingVenue::HyperliquidFutures
        )
    }

    pub fn is_spot(&self) -> bool {
        matches!(
            self,
            TradingVenue::BinanceMargin
                | TradingVenue::OkexMargin
                | TradingVenue::BitgetMargin
                | TradingVenue::BybitMargin
                | TradingVenue::GateMargin
                | TradingVenue::AsterMargin
                | TradingVenue::HyperliquidMargin
        )
    }

    pub fn supports_pre_trade_stack(&self) -> bool {
        matches!(
            self,
            TradingVenue::BinanceMargin
                | TradingVenue::BinanceFutures
                | TradingVenue::OkexMargin
                | TradingVenue::OkexFutures
                | TradingVenue::BybitMargin
                | TradingVenue::BybitFutures
                | TradingVenue::BitgetMargin
                | TradingVenue::BitgetFutures
                | TradingVenue::GateMargin
                | TradingVenue::GateFutures
        )
    }
}

impl TryFrom<u8> for TradingVenue {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::from_u8(value).ok_or_else(|| format!("Invalid TradingVenue value: {}", value))
    }
}

impl From<TradingVenue> for u8 {
    fn from(venue: TradingVenue) -> Self {
        venue.to_u8()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
}

impl TimeInForce {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GTC" => Some(TimeInForce::GTC),
            "IOC" => Some(TimeInForce::IOC),
            "FOK" => Some(TimeInForce::FOK),
            "GTX" => Some(TimeInForce::GTX),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => "GTX",
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(TimeInForce::GTC),
            1 => Some(TimeInForce::IOC),
            2 => Some(TimeInForce::FOK),
            3 => Some(TimeInForce::GTX),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            TimeInForce::GTC => 0,
            TimeInForce::IOC => 1,
            TimeInForce::FOK => 2,
            TimeInForce::GTX => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionType {
    New,
    Canceled,
    Replaced,
    Rejected,
    Trade,
    Expired,
    TradePrevention,
}

impl ExecutionType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NEW" => Some(ExecutionType::New),
            "CANCELED" | "CANCELLED" => Some(ExecutionType::Canceled),
            "REPLACED" => Some(ExecutionType::Replaced),
            "REJECTED" => Some(ExecutionType::Rejected),
            "TRADE" => Some(ExecutionType::Trade),
            "EXPIRED" => Some(ExecutionType::Expired),
            "TRADE_PREVENTION" => Some(ExecutionType::TradePrevention),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionType::New => "NEW",
            ExecutionType::Canceled => "CANCELED",
            ExecutionType::Replaced => "REPLACED",
            ExecutionType::Rejected => "REJECTED",
            ExecutionType::Trade => "TRADE",
            ExecutionType::Expired => "EXPIRED",
            ExecutionType::TradePrevention => "TRADE_PREVENTION",
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            ExecutionType::New => 1,
            ExecutionType::Canceled => 2,
            ExecutionType::Replaced => 3,
            ExecutionType::Rejected => 4,
            ExecutionType::Trade => 5,
            ExecutionType::Expired => 6,
            ExecutionType::TradePrevention => 7,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(ExecutionType::New),
            2 => Some(ExecutionType::Canceled),
            3 => Some(ExecutionType::Replaced),
            4 => Some(ExecutionType::Rejected),
            5 => Some(ExecutionType::Trade),
            6 => Some(ExecutionType::Expired),
            7 => Some(ExecutionType::TradePrevention),
            _ => None,
        }
    }

    pub fn is_trade(&self) -> bool {
        matches!(self, ExecutionType::Trade)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExecutionType::Canceled
                | ExecutionType::Rejected
                | ExecutionType::Expired
                | ExecutionType::TradePrevention
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Expired,
    ExpiredInMatch,
}

impl OrderStatus {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NEW" => Some(OrderStatus::New),
            "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
            "FILLED" => Some(OrderStatus::Filled),
            "CANCELED" | "CANCELLED" => Some(OrderStatus::Canceled),
            "EXPIRED" => Some(OrderStatus::Expired),
            "EXPIRED_IN_MATCH" => Some(OrderStatus::ExpiredInMatch),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::New => "NEW",
            OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::Expired => "EXPIRED",
            OrderStatus::ExpiredInMatch => "EXPIRED_IN_MATCH",
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            OrderStatus::New => 1,
            OrderStatus::PartiallyFilled => 2,
            OrderStatus::Filled => 3,
            OrderStatus::Canceled => 4,
            OrderStatus::Expired => 5,
            OrderStatus::ExpiredInMatch => 6,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(OrderStatus::New),
            2 => Some(OrderStatus::PartiallyFilled),
            3 => Some(OrderStatus::Filled),
            4 => Some(OrderStatus::Canceled),
            5 => Some(OrderStatus::Expired),
            6 => Some(OrderStatus::ExpiredInMatch),
            _ => None,
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        )
    }

    pub fn is_partially_filled(&self) -> bool {
        matches!(self, OrderStatus::PartiallyFilled)
    }

    pub fn has_filled(&self) -> bool {
        matches!(self, OrderStatus::PartiallyFilled | OrderStatus::Filled)
    }

    pub fn is_active(&self) -> bool {
        matches!(self, OrderStatus::New | OrderStatus::PartiallyFilled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Side {
    Buy = 1,
    Sell = 2,
}

impl Side {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Buy),
            2 => Some(Self::Sell),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "buy" | "BUY" | "Buy" => Some(Self::Buy),
            "sell" | "SELL" | "Sell" => Some(Self::Sell),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub fn as_str_lower(&self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }

    pub fn is_buy(&self) -> bool {
        matches!(self, Self::Buy)
    }

    pub fn is_sell(&self) -> bool {
        matches!(self, Self::Sell)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderExecutionStatus {
    Commit = 1,
    Create = 2,
    Filled = 3,
    Cancelled = 4,
    Rejected = 5,
}

impl OrderExecutionStatus {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Commit),
            2 => Some(Self::Create),
            3 => Some(Self::Filled),
            4 => Some(Self::Cancelled),
            5 => Some(Self::Rejected),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "CREATE" => Some(Self::Create),
            "COMMIT" => Some(Self::Commit),
            "FILLED" => Some(Self::Filled),
            "CANCELLED" => Some(Self::Cancelled),
            "REJECTED" => Some(Self::Rejected),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Create => "CREATE",
            Self::Commit => "COMMIT",
            Self::Filled => "FILLED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderType {
    Limit = 1,
    Market = 3,
    StopLoss = 4,
    StopLossLimit = 5,
    TakeProfit = 6,
    TakeProfitLimit = 7,
    StopMarket = 8,
    TakeProfitMarket = 9,
}

impl OrderType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Limit),
            3 => Some(Self::Market),
            4 => Some(Self::StopLoss),
            5 => Some(Self::StopLossLimit),
            6 => Some(Self::TakeProfit),
            7 => Some(Self::TakeProfitLimit),
            8 => Some(Self::StopMarket),
            9 => Some(Self::TakeProfitMarket),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::Limit => 1,
            Self::Market => 3,
            Self::StopLoss => 4,
            Self::StopLossLimit => 5,
            Self::TakeProfit => 6,
            Self::TakeProfitLimit => 7,
            Self::StopMarket => 8,
            Self::TakeProfitMarket => 9,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "LIMIT" => Some(Self::Limit),
            "MARKET" => Some(Self::Market),
            "STOP_LOSS" => Some(Self::StopLoss),
            "STOP_LOSS_LIMIT" => Some(Self::StopLossLimit),
            "TAKE_PROFIT" => Some(Self::TakeProfit),
            "TAKE_PROFIT_LIMIT" => Some(Self::TakeProfitLimit),
            "STOP_MARKET" => Some(Self::StopMarket),
            "TAKE_PROFIT_MARKET" => Some(Self::TakeProfitMarket),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Limit => "LIMIT",
            Self::Market => "MARKET",
            Self::StopLoss => "STOP_LOSS",
            Self::StopLossLimit => "STOP_LOSS_LIMIT",
            Self::TakeProfit => "TAKE_PROFIT",
            Self::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
            Self::StopMarket => "STOP_MARKET",
            Self::TakeProfitMarket => "TAKE_PROFIT_MARKET",
        }
    }

    pub fn is_limit(&self) -> bool {
        matches!(
            self,
            Self::Limit | Self::StopLossLimit | Self::TakeProfitLimit
        )
    }

    pub fn is_market(&self) -> bool {
        matches!(
            self,
            Self::Market | Self::StopMarket | Self::TakeProfitMarket
        )
    }

    pub fn is_conditional(&self) -> bool {
        !matches!(self, Self::Limit | Self::Market)
    }
}
