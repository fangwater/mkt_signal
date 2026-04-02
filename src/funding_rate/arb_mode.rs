use crate::signal::common::TradingVenue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbMode {
    FundingArb,
    SpotFuturesXarb,
    FuturesPairXarb,
}

impl ArbMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FundingArb => "funding_arb",
            Self::SpotFuturesXarb => "spot_futures_xarb",
            Self::FuturesPairXarb => "futures_pair_xarb",
        }
    }

    pub fn from_venues(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Self {
        let open_is_margin = matches!(
            open_venue,
            TradingVenue::BinanceMargin
                | TradingVenue::OkexMargin
                | TradingVenue::BybitMargin
                | TradingVenue::BitgetMargin
                | TradingVenue::GateMargin
                | TradingVenue::HyperliquidMargin
        );
        let hedge_is_futures = matches!(
            hedge_venue,
            TradingVenue::BinanceFutures
                | TradingVenue::OkexFutures
                | TradingVenue::BybitFutures
                | TradingVenue::BitgetFutures
                | TradingVenue::GateFutures
                | TradingVenue::HyperliquidFutures
        );
        if open_is_margin && hedge_is_futures {
            if open_venue.trade_engine_exchange() == hedge_venue.trade_engine_exchange() {
                Self::SpotFuturesXarb
            } else {
                Self::FundingArb
            }
        } else {
            Self::FuturesPairXarb
        }
    }

    pub fn uses_funding_rules(self) -> bool {
        matches!(self, Self::FundingArb)
    }
}
