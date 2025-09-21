use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum Exchange {
    Binance = 1,
    #[value(name = "binance-futures")]
    #[serde(rename = "binance-futures")]
    BinanceFutures = 2,
    Okex = 3,
    #[value(name = "okex-swap")]
    #[serde(rename = "okex-swap")]
    OkexSwap = 4,
    #[value(name = "bybit-spot")]
    #[serde(rename = "bybit-spot")]
    BybitSpot = 5,
    Bybit = 6,
}

impl Exchange {
    pub fn as_str(&self) -> &str {
        match self {
            Exchange::Binance => "binance",
            Exchange::BinanceFutures => "binance-futures",
            Exchange::Okex => "okex",
            Exchange::OkexSwap => "okex-swap",
            Exchange::BybitSpot => "bybit-spot",
            Exchange::Bybit => "bybit",
        }
    }

    /// 从 u8 转换为 Exchange
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Exchange::Binance),
            2 => Some(Exchange::BinanceFutures),
            3 => Some(Exchange::Okex),
            4 => Some(Exchange::OkexSwap),
            5 => Some(Exchange::BybitSpot),
            6 => Some(Exchange::Bybit),
            _ => None,
        }
    }

    /// 转换为 u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

impl Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
