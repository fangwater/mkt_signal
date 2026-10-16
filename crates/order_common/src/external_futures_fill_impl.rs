use crate::{OrderStatus, Side, TradeUpdate, TradingVenue};
use mkt_parsers::msg::{
    bybit_account_msg::BybitBasicOrderMsg, external_futures_fill::ExternalFuturesFill,
};
impl TradeUpdate for ExternalFuturesFill {
    fn event_time(&self) -> i64 {
        self.timestamp_us
    }
    fn trade_time(&self) -> i64 {
        self.timestamp_us
    }
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn order_id(&self) -> i64 {
        BybitBasicOrderMsg::stable_i64_from_str(&self.order_id)
    }
    fn client_order_id(&self) -> i64 {
        0
    }
    fn side(&self) -> Side {
        if self.side == 1 {
            Side::Buy
        } else {
            Side::Sell
        }
    }
    fn price(&self) -> f64 {
        self.price
    }
    fn is_maker(&self) -> bool {
        self.is_maker.unwrap_or(false)
    }
    fn factual_is_maker(&self) -> Option<bool> {
        self.is_maker
    }
    fn trading_venue(&self) -> TradingVenue {
        if self.venue == 5 {
            TradingVenue::BybitFutures
        } else {
            TradingVenue::BitgetFutures
        }
    }
    fn cumulative_filled_quantity(&self) -> f64 {
        self.quantity
    }
    fn order_status(&self) -> Option<OrderStatus> {
        None
    }
}
