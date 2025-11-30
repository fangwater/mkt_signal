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
    #[value(name = "bitget-margin")]
    #[serde(rename = "bitget-margin")]
    BitgetMargin = 7,
    #[value(name = "bitget-futures")]
    #[serde(rename = "bitget-futures")]
    BitgetFutures = 8,
    Gate = 9,
    #[value(name = "gate-futures")]
    #[serde(rename = "gate-futures")]
    GateFutures = 10,
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
            Exchange::BitgetMargin => "bitget-margin",
            Exchange::BitgetFutures => "bitget-futures",
            Exchange::Gate => "gate",
            Exchange::GateFutures => "gate-futures",
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
            7 => Some(Exchange::BitgetMargin),
            8 => Some(Exchange::BitgetFutures),
            9 => Some(Exchange::Gate),
            10 => Some(Exchange::GateFutures),
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
