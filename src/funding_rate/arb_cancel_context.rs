use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::pre_trade::order_manager::Side;
use crate::signal::cancel_signal::{ArbCancelCtx, ArbCancelReason};
use crate::signal::common::{TradingLeg, TradingVenue};

use super::common::Quote;

pub struct ArbCancelContextInput<'a> {
    pub open_symbol: &'a str,
    pub hedge_symbol: &'a str,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub open_quote: &'a Quote,
    pub hedge_quote: &'a Quote,
    pub now: i64,
    pub from_key: &'a str,
    pub reason: ArbCancelReason,
    pub side: Side,
    pub strategy_id: i32,
}

pub fn build_arb_cancel_context(input: ArbCancelContextInput<'_>) -> ArbCancelCtx {
    let mut ctx = ArbCancelCtx::new();
    let open_trade_symbol = normalize_symbol_for_venue(input.open_symbol, input.open_venue);
    let hedge_trade_symbol = normalize_symbol_for_venue(input.hedge_symbol, input.hedge_venue);

    ctx.opening_leg = TradingLeg::new(
        input.open_venue,
        input.open_quote.bid,
        input.open_quote.ask,
        input.open_quote.ts,
    );
    ctx.set_opening_symbol(&open_trade_symbol);

    ctx.hedging_leg = TradingLeg::new(
        input.hedge_venue,
        input.hedge_quote.bid,
        input.hedge_quote.ask,
        input.hedge_quote.ts,
    );
    ctx.set_hedging_symbol(&hedge_trade_symbol);

    ctx.trigger_ts = input.now;
    ctx.set_from_key(input.from_key.as_bytes().to_vec());
    ctx.set_reason(input.reason);
    ctx.set_side(input.side);
    ctx.set_target_strategy(input.strategy_id);
    ctx
}
