use crate::common::mkt_msg::MktMsgType;
use crate::signal::common::TradingVenue;

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

/// 在 open/hedge 两侧间转换 symbol（按 venue + 消息类型）
///
/// 规则：
/// - Binance：两侧完全等价，直接返回大写 symbol
/// - OKEx：
///   - ask_bid_spread/bid_ask_spread 等盘口类：两侧等价，直接返回大写 symbol
///   - FundingRate/MarkPrice：futures 侧可能带 "-SWAP" 后缀；margin 侧不带
///     - margin -> futures：若无 "-SWAP" 则追加
///     - futures -> margin：去掉末尾 "-SWAP"（若存在）
/// - 其他交易所目前按等价处理（返回大写）
pub fn map_symbol_between_venues(
    symbol: &str,
    from: TradingVenue,
    to: TradingVenue,
    msg_type: MktMsgType,
) -> String {
    let mut upper = symbol.to_uppercase();

    // 同一 venue 或非 OKEx，直接返回大写
    let is_okex_pair = matches!(
        (from, to),
        (TradingVenue::OkexMargin, TradingVenue::OkexFutures)
            | (TradingVenue::OkexFutures, TradingVenue::OkexMargin)
    );
    if from == to || !is_okex_pair {
        return upper;
    }

    match msg_type {
        MktMsgType::FundingRate | MktMsgType::MarkPrice => {
            // 资金费率 / MarkPrice 需要处理 "-SWAP" 后缀
            if matches!(from, TradingVenue::OkexMargin) && matches!(to, TradingVenue::OkexFutures) {
                if !upper.ends_with("-SWAP") {
                    upper.push_str("-SWAP");
                }
                upper
            } else if matches!(from, TradingVenue::OkexFutures)
                && matches!(to, TradingVenue::OkexMargin)
            {
                if let Some(stripped) = upper.strip_suffix("-SWAP") {
                    stripped.to_string()
                } else {
                    upper
                }
            } else {
                upper
            }
        }
        _ => upper, // 盘口类等价，不做处理
    }
}

/// 用于白名单/比对的符号规范化：大写，移除 '-'/'_'，并去掉 OKEx 的 "-SWAP" 后缀
pub fn normalize_symbol_for_whitelist(symbol: &str, venue: TradingVenue) -> String {
    let mut cleaned = symbol.to_uppercase().replace(['-', '_'], "");
    if matches!(venue, TradingVenue::OkexMargin | TradingVenue::OkexFutures)
        && cleaned.ends_with("SWAP")
    {
        cleaned.truncate(cleaned.len().saturating_sub(4));
    }
    cleaned
}
