use anyhow::Result;

use ipc_common::iceoryx_publisher::TradeSignalPublisher;
use rolling_common::arb_open_latency::record_arb_open_latency;
use runtime_common::time_util::get_timestamp_us;
use signal_common::trade_signal::SignalType;

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
        let context = to_bytes(ctx);
        signal_pub.publish_trade_signal_parts(
            signal_type,
            generation_time,
            0.0,
            context.as_ref(),
        )?;
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
