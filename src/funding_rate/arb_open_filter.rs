use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;

use super::mkt_channel::MktChannel;
use super::xarb_funding_threshold_loader::XarbFundingThresholdsResolved;

pub fn select_open_filter_threshold(side: Side, thresholds: XarbFundingThresholdsResolved) -> f64 {
    match side {
        Side::Buy => thresholds.forward_open,
        Side::Sell => thresholds.backward_open,
    }
}

pub fn lookup_realtime_open_filter_value(
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Option<(f64, &'static str)> {
    let mkt_channel = MktChannel::instance();

    if open_venue.is_futures() && hedge_venue.is_futures() {
        let open_fr = mkt_channel.get_latest_funding_rate(open_symbol, open_venue)?;
        let hedge_fr = mkt_channel.get_latest_funding_rate(hedge_symbol, hedge_venue)?;
        return Some((hedge_fr - open_fr, "spread_fr"));
    }

    if hedge_venue.is_futures() {
        return mkt_channel
            .get_premium_rate(hedge_symbol, hedge_venue)
            .map(|value| (value, "premium_rate"));
    }

    None
}
