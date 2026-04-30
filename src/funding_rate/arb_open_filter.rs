use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;

use super::mkt_channel::MktChannel;
use super::funding_threshold_loader::FactorDirectionalThresholds;

/// Per-side(forward/backward)阈值选择,用于因子链上单个因子的阈值比较。
pub fn select_factor_threshold(side: Side, t: FactorDirectionalThresholds) -> f64 {
    match side {
        Side::Buy => t.forward_open,
        Side::Sell => t.backward_open,
    }
}

/// 因子注册表(Rust 侧):任何新加的因子在这里登记实时值的取数路径。
/// 与 Python 侧 `INTRA_FACTOR_CHAIN`/`CROSS_FACTOR_CHAIN`(在
/// `intra_scripts/sync_intra_funding_thresholds.py` 等)必须 lockstep——
/// 新增因子时两边同步。未登记的因子返回 None,arb_decision 会报 `miss_<factor>_value`。
pub fn lookup_factor_realtime_value(
    factor: &str,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<f64> {
    let mkt_channel = MktChannel::instance();
    match factor {
        "hedge_premium_rate" => mkt_channel.get_premium_rate(hedge_symbol, hedge_venue),
        "spread_fr" => {
            let open_fr = mkt_channel.get_latest_funding_rate(open_symbol, open_venue)?;
            let hedge_fr = mkt_channel.get_latest_funding_rate(hedge_symbol, hedge_venue)?;
            Some(hedge_fr - open_fr)
        }
        // premium_rate alias:历史日志/旧调用方仍用 "premium_rate" 标签
        "premium_rate" => mkt_channel.get_premium_rate(hedge_symbol, hedge_venue),
        _ => None,
    }
}

/// 兼容旧调用方:保留按 venue 类型推断 factor 的便捷查值入口。
/// 新的因子链评估请直接用 `lookup_factor_realtime_value(factor, ...)`。
pub fn lookup_realtime_open_filter_value(
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<(f64, &'static str)> {
    if open_venue.is_futures() && hedge_venue.is_futures() {
        let value = lookup_factor_realtime_value(
            "spread_fr",
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
        )?;
        return Some((value, "spread_fr"));
    }
    if hedge_venue.is_futures() {
        let value = lookup_factor_realtime_value(
            "premium_rate",
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
        )?;
        return Some((value, "premium_rate"));
    }
    None
}
