use serde::{Deserialize, Serialize};

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
