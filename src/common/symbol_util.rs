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
    let symbol_upper = normalize_symbol_for_internal(symbol);
    let (base, quote) = extract_assets_from_internal_symbol(&symbol_upper);
    (base.to_string(), quote.to_string())
}

/// 规范化为 pre_trade 内部统一使用的 symbol key。
///
/// 规则：
/// - 全部转大写
/// - 去掉常见分隔符 `-` / `_` / `/`
/// - 若尾部带有 `SWAP` 后缀则去掉，统一为现货风格的 `BASEQUOTE`
///
/// # 例子
/// - `BTCUSDT` -> `BTCUSDT`
/// - `BTC-USDT` -> `BTCUSDT`
/// - `BTC-USDT-SWAP` -> `BTCUSDT`
/// - `BTC_USDT` -> `BTCUSDT`
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
        _ => {
            // 其他交易所：保持原样
            symbol_upper
        }
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
    fn test_normalize_symbol_for_internal() {
        assert_eq!(normalize_symbol_for_internal("APTUSDT"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("APT-USDT"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("APT_USDT"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("APT-USDT-SWAP"), "APTUSDT");
        assert_eq!(normalize_symbol_for_internal("apt/usdt/swap"), "APTUSDT");
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
