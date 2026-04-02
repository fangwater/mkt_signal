use anyhow::Result;

use crate::common::iceoryx_publisher::SignalPublisher;
use crate::signal::cancel_signal::{ArbCancelCtx, ArbCancelReason};
use crate::signal::common::SignalBytes;
use crate::signal::trade_signal::{SignalType, TradeSignal};

use super::arb_cancel_context::{build_arb_cancel_context, ArbCancelContextInput};
use super::common::Quote;
use crate::signal::common::TradingVenue;

pub struct ArbCancelEmitInput<'a> {
    pub signal_pub: &'a SignalPublisher,
    pub open_symbol: &'a str,
    pub hedge_symbol: &'a str,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub open_quote: &'a Quote,
    pub hedge_quote: &'a Quote,
    pub now: i64,
    pub from_key: &'a str,
    pub reason: ArbCancelReason,
    pub strategy_id: i32,
}

pub fn build_precise_arb_cancel_ctx(input: ArbCancelEmitInput<'_>) -> ArbCancelCtx {
    build_arb_cancel_context(ArbCancelContextInput {
        open_symbol: input.open_symbol,
        hedge_symbol: input.hedge_symbol,
        open_venue: input.open_venue,
        hedge_venue: input.hedge_venue,
        open_quote: input.open_quote,
        hedge_quote: input.hedge_quote,
        now: input.now,
        from_key: input.from_key,
        reason: input.reason,
        strategy_id: input.strategy_id,
    })
}

pub fn emit_precise_arb_cancel(input: ArbCancelEmitInput<'_>) -> Result<()> {
    let signal_pub = input.signal_pub;
    let ctx = build_precise_arb_cancel_ctx(input);
    let signal = TradeSignal::create(SignalType::ArbCancel, ctx.trigger_ts, 0.0, ctx.to_bytes());
    signal_pub.publish(&signal.to_bytes())?;
    Ok(())
}
