use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum Exchange {
    Binance = 1,
    Okex = 2,
    Bybit = 3,
    Bitget = 4,
    Gate = 5,
}

impl Exchange {
    pub fn as_str(&self) -> &str {
        match self {
            Exchange::Binance => "binance",
            Exchange::Okex => "okex",
            Exchange::Bybit => "bybit",
            Exchange::Bitget => "bitget",
            Exchange::Gate => "gate",
        }
    }

    /// 从 u8 转换为 Exchange
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Exchange::Binance),
            2 => Some(Exchange::Okex),
            3 => Some(Exchange::Bybit),
            4 => Some(Exchange::Bitget),
            5 => Some(Exchange::Gate),
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
