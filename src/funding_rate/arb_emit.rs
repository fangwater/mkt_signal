use anyhow::Result;

use crate::common::iceoryx_publisher::TradeSignalPublisher;
use crate::common::time_util::get_timestamp_us;
use crate::rolling_metrics::arb_open_latency::record_arb_open_latency;
use signal_common::trade_signal::{SignalType, TradeSignal};

pub fn emit_levels_as_signals<TCtx>(
    signal_pub: &TradeSignalPublisher,
    signal_type: SignalType,
    generation_time: i64,
    contexts: impl IntoIterator<Item = TCtx>,
    to_bytes: impl Fn(TCtx) -> bytes::Bytes,
) -> Result<usize> {
    let mut sent = 0usize;
    for ctx in contexts {
        let publish_start_us = get_timestamp_us();
        if matches!(signal_type, SignalType::ArbOpen) && generation_time > 0 {
            record_arb_open_latency(
                "ts_publish_minus_generation",
                publish_start_us.saturating_sub(generation_time),
            );
        }
        let signal = TradeSignal::create(signal_type.clone(), generation_time, 0.0, to_bytes(ctx));
        signal_pub.publish(&signal.to_bytes())?;
        if matches!(signal_type, SignalType::ArbOpen) {
            record_arb_open_latency(
                "ts_signal_publish_cost",
                get_timestamp_us().saturating_sub(publish_start_us),
            );
        }
        sent += 1;
    }
    Ok(sent)
}
