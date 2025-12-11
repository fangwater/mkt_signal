/// 符号标准化工具，用于跨 venue 对齐
///
/// - 统一大写
/// - 去除分隔符 '-' 和 '_'（便于 okex/gate/bybit 等对齐）
/// - 去除 OKEx 等可能带的 "-SWAP"/"SWAP" 后缀
pub fn normalize_symbol_for_pairing(symbol: &str, exchange_hint: &str) -> String {
    let upper = symbol.to_uppercase();
    let mut cleaned = upper.replace(['-', '_'], "");
    if exchange_hint.starts_with("okex") && cleaned.ends_with("SWAP") {
        cleaned.truncate(cleaned.len().saturating_sub(4));
    }
    cleaned
}
