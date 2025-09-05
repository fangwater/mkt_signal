use std::fmt::{self, Display};
use serde::{Deserialize, Serialize};
use clap::ValueEnum;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum Exchange {
    Binance,
    #[value(name = "binance-futures")]
    #[serde(rename = "binance-futures")]
    BinanceFutures,
    Okex,
    #[value(name = "okex-swap")]
    #[serde(rename = "okex-swap")]
    OkexSwap,
    Bybit,
    #[value(name = "bybit-spot")]
    #[serde(rename = "bybit-spot")]
    BybitSpot,
}

impl Exchange {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "binance" => Some(Exchange::Binance),
            "binance-futures" => Some(Exchange::BinanceFutures),
            "okex" => Some(Exchange::Okex),
            "okex-swap" => Some(Exchange::OkexSwap),
            "bybit" => Some(Exchange::Bybit),
            "bybit-spot" => Some(Exchange::BybitSpot),
            _ => None,
        }
    }
    
    pub fn as_str(&self) -> &str {
        match self {
            Exchange::Binance => "binance",
            Exchange::BinanceFutures => "binance-futures",
            Exchange::Okex => "okex",
            Exchange::OkexSwap => "okex-swap",
            Exchange::Bybit => "bybit",
            Exchange::BybitSpot => "bybit-spot",
        }
    }
}

impl Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}