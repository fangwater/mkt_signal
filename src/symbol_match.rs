use crate::signal::common::TradingVenue;

/// 符号标准化工具，用于跨 venue 对齐
///
/// - 统一大写
/// - 去除分隔符 '-' 和 '_'（便于 okex/gate/bybit 等对齐）
/// - 去除常见永续后缀（-SWAP/SWAP）
pub fn normalize_symbol_for_pairing(symbol: &str, exchange_hint: &str) -> String {
    let mut cleaned = normalize_symbol_binance_style(symbol);
    if exchange_hint.starts_with("okex") && cleaned.ends_with("SWAP") {
        cleaned.truncate(cleaned.len().saturating_sub(4));
    }
    cleaned
}

/// 将任意符号规整为“币安风格”字符串（全大写、无分隔符/后缀）
pub fn normalize_symbol_binance_style(symbol: &str) -> String {
    let mut upper = symbol.to_uppercase();
    upper = upper.replace(['-', '_'], "");
    if upper.ends_with("SWAP") {
        upper.truncate(upper.len().saturating_sub(4));
    }
    upper
}

/// 按 venue 生成交易所原生格式的符号（用于请求/订阅）
///
/// 传入的 `normalized` 应为 normalize_symbol_binance_style 处理后的字符串。
pub fn format_symbol_for_venue(normalized: &str, venue: TradingVenue) -> String {
    let norm = normalized.to_uppercase();
    match venue {
        TradingVenue::OkexMargin => split_base_quote(&norm)
            .map(|(base, quote)| format!("{}-{}", base, quote))
            .unwrap_or(norm),
        TradingVenue::OkexFutures => split_base_quote(&norm)
            .map(|(base, quote)| format!("{}-{}-SWAP", base, quote))
            .unwrap_or(norm),
        TradingVenue::GateMargin | TradingVenue::GateFutures => split_base_quote(&norm)
            .map(|(base, quote)| format!("{}_{}", base, quote))
            .unwrap_or(norm),
        // 其他交易所直接使用规整后的符号（Binance/Bybit/Bitget）
        _ => norm,
    }
}

fn split_base_quote(norm: &str) -> Option<(String, String)> {
    // 按常见 quote 后缀切分（优先匹配较长的后缀）
    const QUOTES: [&str; 3] = ["USDT", "USDC", "USD"];
    for quote in QUOTES {
        if norm.ends_with(quote) && norm.len() > quote.len() {
            let base = &norm[..norm.len() - quote.len()];
            return Some((base.to_string(), quote.to_string()));
        }
    }
    None
}
