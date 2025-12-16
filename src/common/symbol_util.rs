//! 符号格式转换工具函数
//!
//! 提供不同交易所间的符号格式转换功能

use crate::signal::common::TradingVenue;

/// 从交易对符号中提取 base asset 和 quote asset
///
/// # 例子
/// - "BTCUSDT" -> ("BTC", "USDT")
/// - "ETHUSDC" -> ("ETH", "USDC")
pub fn extract_assets_from_symbol(symbol: &str) -> (String, String) {
    let symbol_upper = symbol.to_uppercase();
    const QUOTE_ASSETS: [&str; 7] = ["USDT", "USDC", "BUSD", "FDUSD", "BIDR", "TRY", "USD"];

    for quote in QUOTE_ASSETS {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            let base = &symbol_upper[..symbol_upper.len() - quote.len()];
            return (base.to_string(), quote.to_string());
        }
    }

    // 如果没有匹配到已知的 quote asset，默认返回整个符号作为 base，USDT 作为 quote
    (symbol_upper, "USDT".to_string())
}

/// 根据 venue 修正符号格式
///
/// 不同交易所的交易对格式不同，此函数根据目标 venue 自动转换符号格式
///
/// # 规则
/// - **OkexMargin** (现货杠杆): 使用现货格式 (APT-USDT)，去掉 -SWAP 后缀
/// - **OkexFutures** (永续合约): 使用合约格式 (APT-USDT-SWAP)，添加 -SWAP 后缀
/// - **BinanceMargin/BinanceFutures**: 使用归一化格式 (APTUSDT)，去掉分隔符
/// - 其他交易所: 保持原样
///
/// # 例子
/// ```ignore
/// // OKEx 现货杠杆：去掉 -SWAP 后缀
/// assert_eq!(normalize_symbol_for_venue("APT-USDT-SWAP", TradingVenue::OkexMargin), "APT-USDT");
/// assert_eq!(normalize_symbol_for_venue("APTUSDT", TradingVenue::OkexMargin), "APT-USDT");
///
/// // OKEx 永续合约：添加 -SWAP 后缀
/// assert_eq!(normalize_symbol_for_venue("APT-USDT", TradingVenue::OkexFutures), "APT-USDT-SWAP");
/// assert_eq!(normalize_symbol_for_venue("APTUSDT", TradingVenue::OkexFutures), "APT-USDT-SWAP");
///
/// // Binance: 去掉分隔符
/// assert_eq!(normalize_symbol_for_venue("APT-USDT-SWAP", TradingVenue::BinanceMargin), "APTUSDT");
/// assert_eq!(normalize_symbol_for_venue("APT-USDT", TradingVenue::BinanceMargin), "APTUSDT");
/// ```
///
/// # 重要性
/// 此函数解决了不同交易所账户类型的产品代码限制：
/// - OKEx 现货杠杆账户只能交易现货交易对 (如 APT-USDT)
/// - OKEx 永续合约账户只能交易 SWAP 合约 (如 APT-USDT-SWAP)
/// - 信号生成时符号格式错误会导致下单失败 (OKEx 错误码 60012: Illegal request)
pub fn normalize_symbol_for_venue(symbol: &str, venue: TradingVenue) -> String {
    let symbol_upper = symbol.to_uppercase();

    match venue {
        TradingVenue::OkexMargin => {
            // 现货杠杆：去掉 -SWAP，确保格式为 BASE-QUOTE
            if symbol_upper.ends_with("-SWAP") {
                symbol_upper.replace("-SWAP", "")
            } else if symbol_upper.contains('-') {
                symbol_upper
            } else {
                // 如果是 APTUSDT 格式，转换为 APT-USDT
                let (base, quote) = extract_assets_from_symbol(&symbol_upper);
                format!("{}-{}", base, quote)
            }
        }
        TradingVenue::OkexFutures => {
            // 永续合约：确保有 -SWAP 后缀
            if symbol_upper.ends_with("-SWAP") {
                symbol_upper
            } else if symbol_upper.contains('-') {
                format!("{}-SWAP", symbol_upper)
            } else {
                // 如果是 APTUSDT 格式，转换为 APT-USDT-SWAP
                let (base, quote) = extract_assets_from_symbol(&symbol_upper);
                format!("{}-{}-SWAP", base, quote)
            }
        }
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => {
            // Binance: 使用 APTUSDT 格式（不带分隔符）
            if symbol_upper.contains('-') {
                symbol_upper.replace("-", "").replace("SWAP", "")
            } else {
                symbol_upper
            }
        }
        _ => {
            // 其他交易所：保持原样
            symbol_upper
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_assets_from_symbol() {
        assert_eq!(extract_assets_from_symbol("BTCUSDT"), ("BTC".to_string(), "USDT".to_string()));
        assert_eq!(extract_assets_from_symbol("ETHUSDC"), ("ETH".to_string(), "USDC".to_string()));
        assert_eq!(extract_assets_from_symbol("aptusdt"), ("APT".to_string(), "USDT".to_string()));
    }

    #[test]
    fn test_normalize_symbol_for_okex_margin() {
        // OkexMargin: 应该去掉 -SWAP
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
        // OkexFutures: 应该添加 -SWAP
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
        // Binance: 应该去掉分隔符
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
}
