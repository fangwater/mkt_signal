use anyhow::Result;

use crate::common::iceoryx_publisher::SignalPublisher;
use crate::signal::trade_signal::{SignalType, TradeSignal};

pub fn emit_levels_as_signals<TCtx>(
    signal_pub: &SignalPublisher,
    signal_type: SignalType,
    generation_time: i64,
    contexts: impl IntoIterator<Item = TCtx>,
    to_bytes: impl Fn(TCtx) -> bytes::Bytes,
) -> Result<usize> {
    let mut sent = 0usize;
    for ctx in contexts {
        let signal = TradeSignal::create(signal_type.clone(), generation_time, 0.0, to_bytes(ctx));
        signal_pub.publish(&signal.to_bytes())?;
        sent += 1;
    }
    Ok(sent)
}
