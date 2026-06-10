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

/// 从交易对符号中提取 base asset 和 quote asset
pub fn extract_assets_from_symbol(symbol: &str) -> (String, String) {
    let symbol_upper = normalize_symbol_for_internal(symbol);
    let (base, quote) = extract_assets_from_internal_symbol(&symbol_upper);
    (base.to_string(), quote.to_string())
}

/// 规范化为 pre_trade 内部统一使用的 symbol key。
pub fn normalize_symbol_for_internal(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for ch in symbol.trim().chars() {
        if matches!(ch, '-' | '_' | '/') {
            continue;
        }
        for upper in ch.to_uppercase() {
            out.push(upper);
        }
    }
    if out.ends_with("SWAP") {
        out.truncate(out.len().saturating_sub("SWAP".len()));
    }
    out
}

/// 根据 venue 修正符号格式。
pub fn normalize_symbol_for_venue(symbol: &str, venue: TradingVenue) -> String {
    let symbol_upper = normalize_symbol_for_internal(symbol);

    match venue {
        TradingVenue::OkexMargin => {
            let (base, quote) = extract_assets_from_internal_symbol(&symbol_upper);
            format!("{}-{}", base, quote)
        }
        TradingVenue::OkexFutures => {
            let (base, quote) = extract_assets_from_internal_symbol(&symbol_upper);
            format!("{}-{}-SWAP", base, quote)
        }
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => symbol_upper,
        _ => symbol_upper,
    }
}

pub fn okex_inst_id_from_symbol(symbol: &str, venue: TradingVenue) -> Result<String, String> {
    let symbol_upper = symbol.to_uppercase();

    if symbol_upper.contains('-') {
        return match venue {
            TradingVenue::OkexMargin => Ok(symbol_upper.replace("-SWAP", "")),
            TradingVenue::OkexFutures => {
                if symbol_upper.ends_with("-SWAP") {
                    Ok(symbol_upper)
                } else {
                    Ok(format!("{symbol_upper}-SWAP"))
                }
            }
            _ => Err(format!("venue {:?} not okex", venue)),
        };
    }

    let (base, quote) = extract_assets_from_symbol(&symbol_upper);
    match venue {
        TradingVenue::OkexMargin => Ok(format!("{base}-{quote}")),
        TradingVenue::OkexFutures => Ok(format!("{base}-{quote}-SWAP")),
        _ => Err(format!("venue {:?} not okex", venue)),
    }
}

pub fn gate_currency_pair_from_symbol(symbol: &str) -> String {
    let mut upper = symbol.to_ascii_uppercase();
    if upper.contains("-SWAP") {
        upper = upper.replace("-SWAP", "");
    }
    if upper.contains('_') {
        return upper;
    }
    if upper.contains('-') {
        return upper.replace('-', "_");
    }
    let (base, quote) = extract_assets_from_symbol(&upper);
    format!("{base}_{quote}")
}

/// 生成 min-qty/filter 表使用的 symbol key。
pub fn min_qty_symbol_key(venue: TradingVenue, symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            symbol.to_uppercase().replace(['_', '-'], "")
        }
        _ => symbol.to_uppercase(),
    }
}

fn extract_assets_from_internal_symbol(symbol_upper: &str) -> (&str, &str) {
    const QUOTE_ASSETS: [&str; 7] = ["USDT", "USDC", "BUSD", "FDUSD", "BIDR", "TRY", "USD"];

    for quote in QUOTE_ASSETS {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            let base = &symbol_upper[..symbol_upper.len() - quote.len()];
            return (base, quote);
        }
    }

    (symbol_upper, "USDT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_assets_from_symbol() {
        assert_eq!(
            extract_assets_from_symbol("BTCUSDT"),
            ("BTC".to_string(), "USDT".to_string())
        );
        assert_eq!(
            extract_assets_from_symbol("ETHUSDC"),
            ("ETH".to_string(), "USDC".to_string())
        );
        assert_eq!(
            extract_assets_from_symbol("aptusdt"),
            ("APT".to_string(), "USDT".to_string())
        );
        assert_eq!(
            extract_assets_from_symbol("apt-usdt-swap"),
            ("APT".to_string(), "USDT".to_string())
        );
    }

    #[test]
    fn test_normalize_symbol_for_internal() {
        assert_eq!(normalize_symbol_for_internal("APTUSDT"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("APT-USDT"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("APT_USDT"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("APT-USDT-SWAP"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("apt/usdt/swap"), "APTUSDT");
    }

    #[test]
    fn test_min_qty_symbol_key() {
        assert_eq!(
            min_qty_symbol_key(TradingVenue::OkexFutures, "APT-USDT-SWAP"),
            "APTUSDT"
        );
        assert_eq!(
            min_qty_symbol_key(TradingVenue::GateFutures, "APT_USDT"),
            "APTUSDT"
        );
        assert_eq!(
            min_qty_symbol_key(TradingVenue::BinanceFutures, "APTUSDT"),
            "APTUSDT"
        );
    }

    #[test]
    fn test_normalize_symbol_for_okex_margin() {
        assert_eq!(
            normalize_symbol_for_venue("APT-USDT-SWAP", TradingVenue::OkexMargin),
            "APT-USDT"
        );
        assert_eq!(
            normalize_symbol_for_venue("APT-USDT", TradingVenue::OkexMargin),
            "APT-USDT"
        );
        assert_eq!(
            normalize_symbol_for_venue("APTUSDT", TradingVenue::OkexMargin),
            "APT-USDT"
        );
    }

    #[test]
    fn test_normalize_symbol_for_okex_futures() {
        assert_eq!(
            normalize_symbol_for_venue("APT-USDT-SWAP", TradingVenue::OkexFutures),
            "APT-USDT-SWAP"
        );
        assert_eq!(
            normalize_symbol_for_venue("APT-USDT", TradingVenue::OkexFutures),
            "APT-USDT-SWAP"
        );
        assert_eq!(
            normalize_symbol_for_venue("APTUSDT", TradingVenue::OkexFutures),
            "APT-USDT-SWAP"
        );
    }

    #[test]
    fn test_normalize_symbol_for_binance() {
        assert_eq!(
            normalize_symbol_for_venue("APT-USDT-SWAP", TradingVenue::BinanceMargin),
            "APTUSDT"
        );
        assert_eq!(
            normalize_symbol_for_venue("APT-USDT", TradingVenue::BinanceMargin),
            "APTUSDT"
        );
        assert_eq!(
            normalize_symbol_for_venue("APTUSDT", TradingVenue::BinanceMargin),
            "APTUSDT"
        );
    }

    #[test]
    fn test_okex_inst_id_from_symbol() {
        assert_eq!(
            okex_inst_id_from_symbol("APTUSDT", TradingVenue::OkexMargin).unwrap(),
            "APT-USDT"
        );
        assert_eq!(
            okex_inst_id_from_symbol("APT-USDT", TradingVenue::OkexFutures).unwrap(),
            "APT-USDT-SWAP"
        );
        assert_eq!(
            okex_inst_id_from_symbol("APT-USDT-SWAP", TradingVenue::OkexFutures).unwrap(),
            "APT-USDT-SWAP"
        );
    }

    #[test]
    fn test_gate_currency_pair_from_symbol() {
        assert_eq!(gate_currency_pair_from_symbol("CCUSDT"), "CC_USDT");
        assert_eq!(gate_currency_pair_from_symbol("CC-USDT"), "CC_USDT");
        assert_eq!(gate_currency_pair_from_symbol("CC_USDT"), "CC_USDT");
        assert_eq!(gate_currency_pair_from_symbol("CC-USDT-SWAP"), "CC_USDT");
    }
}
