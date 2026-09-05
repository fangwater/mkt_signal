use crate::order_update::OrderUpdate;
use crate::trade_update::TradeUpdate;
use crate::trade_update_lite::TradeUpdateLite;
use crate::{ExecutionType, OrderStatus, OrderType, Side, TimeInForce, TradingVenue};
use mkt_parsers::msg::hyperliquid_account_msg::{
    HyperliquidBasicFillMsg, HyperliquidBasicOrderMsg,
};

fn trading_venue(value: u8) -> TradingVenue {
    match TradingVenue::from_u8(value) {
        Some(TradingVenue::HyperliquidFutures) => TradingVenue::HyperliquidFutures,
        _ => TradingVenue::HyperliquidMargin,
    }
}

impl OrderUpdate for HyperliquidBasicOrderMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        self.order_id
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn side(&self) -> Side {
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn order_type(&self) -> OrderType {
        OrderType::from_u8(self.order_type).unwrap_or(OrderType::Limit)
    }

    fn time_in_force(&self) -> TimeInForce {
        TimeInForce::from_u8(self.time_in_force).unwrap_or(TimeInForce::GTC)
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn quantity(&self) -> f64 {
        self.quantity
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn status(&self) -> OrderStatus {
        OrderStatus::from_u8(self.order_status).unwrap_or(OrderStatus::New)
    }

    fn raw_status(&self) -> &str {
        &self.raw_status
    }

    fn execution_type(&self) -> ExecutionType {
        ExecutionType::from_u8(self.execution_type).unwrap_or(ExecutionType::New)
    }

    fn raw_execution_type(&self) -> &str {
        "order_update"
    }

    fn trading_venue(&self) -> TradingVenue {
        trading_venue(self.venue)
    }

    fn client_order_id_str(&self) -> Option<&str> {
        Some(&self.cloid)
    }
}

// The account monitor emits fills as `HyperliquidBasicFillMsg`. This implementation
// exists because the generic lifecycle dispatcher requires both traits; its
// trade methods are never selected for Hyperliquid orderUpdates.
impl TradeUpdate for HyperliquidBasicOrderMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn trade_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        self.order_id
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn side(&self) -> Side {
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn is_maker(&self) -> bool {
        false
    }

    fn trading_venue(&self) -> TradingVenue {
        trading_venue(self.venue)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn order_status(&self) -> Option<OrderStatus> {
        Some(OrderStatus::from_u8(self.order_status).unwrap_or(OrderStatus::New))
    }
}

impl TradeUpdateLite for HyperliquidBasicFillMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn trade_time(&self) -> i64 {
        self.trade_time.saturating_mul(1_000)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn trade_id(&self) -> &[u8; mkt_parsers::msg::basic_account_msg::TRADE_ID_LEN] {
        &self.trade_id
    }

    fn side(&self) -> Side {
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn last_filled_quantity(&self) -> f64 {
        self.last_filled_quantity
    }

    fn is_maker(&self) -> bool {
        self.is_maker != 0
    }

    fn trading_venue(&self) -> TradingVenue {
        trading_venue(self.venue)
    }
}

/// The account processor derives cumulative quantity only from deduplicated
/// factual `userFills`. Status remains unknown until an order `origSz` mapping
/// is available.
impl TradeUpdate for HyperliquidBasicFillMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn trade_time(&self) -> i64 {
        self.trade_time.saturating_mul(1_000)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        self.order_id
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn side(&self) -> Side {
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn is_maker(&self) -> bool {
        self.is_maker != 0
    }

    fn trading_venue(&self) -> TradingVenue {
        trading_venue(self.venue)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn order_status(&self) -> Option<OrderStatus> {
        OrderStatus::from_u8(self.order_status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_hyperliquid_venue_and_microsecond_time() {
        let msg = HyperliquidBasicOrderMsg::create(
            TradingVenue::HyperliquidFutures.to_u8(),
            1_725_000_000_123,
            "BTCUSDC".to_string(),
            9,
            42,
            "0x0000000000000000000000000000002a".to_string(),
            Side::Buy.to_u8(),
            OrderType::Limit.to_u8(),
            TimeInForce::GTC.to_u8(),
            ExecutionType::New.to_u8(),
            OrderStatus::PartiallyFilled.to_u8(),
            60_000.0,
            1.0,
            0.25,
            "open".to_string(),
        );
        assert_eq!(OrderUpdate::event_time(&msg), 1_725_000_000_123_000);
        assert_eq!(
            OrderUpdate::trading_venue(&msg),
            TradingVenue::HyperliquidFutures
        );
        assert_eq!(OrderUpdate::status(&msg), OrderStatus::PartiallyFilled);
        assert_eq!(
            OrderUpdate::client_order_id_str(&msg),
            Some(msg.cloid.as_str())
        );
    }

    #[test]
    fn factual_fill_supports_lite_routing_and_unmatched_persistence() {
        let msg = HyperliquidBasicFillMsg::create(
            TradingVenue::HyperliquidMargin.to_u8(),
            100,
            101,
            "PURRUSDC".to_string(),
            9,
            0,
            String::new(),
            "hl:0123456789abcdef0123456789abcdef",
            88,
            "0xabc".to_string(),
            String::new(),
            Side::Sell.to_u8(),
            true,
            0.25,
            12.0,
            12.0,
            None,
        );
        assert_eq!(TradeUpdateLite::trade_time(&msg), 101_000);
        assert_eq!(TradeUpdateLite::last_filled_quantity(&msg), 12.0);
        assert_eq!(TradeUpdate::order_id(&msg), 9);
        assert_eq!(TradeUpdate::client_order_id(&msg), 0);
        assert_eq!(TradeUpdate::order_status(&msg), None);
        assert_eq!(TradeUpdate::cumulative_filled_quantity(&msg), 12.0);
        assert_eq!(
            TradeUpdate::trading_venue(&msg),
            TradingVenue::HyperliquidMargin
        );

        let known = HyperliquidBasicFillMsg::create(
            TradingVenue::HyperliquidFutures.to_u8(),
            100,
            101,
            "BTCUSDC".to_string(),
            10,
            42,
            "0x0000000000000000000000000000002a".to_string(),
            "hl:fedcba9876543210fedcba9876543210",
            89,
            "0xdef".to_string(),
            String::new(),
            Side::Buy.to_u8(),
            false,
            60_000.0,
            0.4,
            1.0,
            Some(OrderStatus::Filled.to_u8()),
        );
        assert_eq!(TradeUpdate::cumulative_filled_quantity(&known), 1.0);
        assert_eq!(TradeUpdate::order_status(&known), Some(OrderStatus::Filled));
    }
}
