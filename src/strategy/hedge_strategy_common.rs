use crate::common::exchange::Exchange;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::symbol_util::extract_base_asset;

pub(crate) const HEDGE_QUERY_INTERVAL_US: i64 = 30_000_000;
pub(crate) const HEDGE_QUERY_WATCHDOG_US: i64 = 30_000;
pub(crate) const CANCEL_RESEND_THROTTLE_US: i64 = 500_000;
// 净敞口按 mark price 折算后超过该 USDT 阈值，MM hedge 才会发 query。
pub(crate) const NET_EXPOSURE_EPS_USDT: f64 = 5.0;

pub(crate) fn mark_price_lookup_symbol(symbol: &str, exchange: Exchange) -> String {
    extract_base_asset(symbol)
        .map(|base_asset| create_symbol_mapper(exchange).asset_to_price_symbol(&base_asset))
        .unwrap_or_else(|| symbol.to_uppercase())
}
