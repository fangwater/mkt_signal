use anyhow::Result;
use bytes::BytesMut;

use ipc_common::iceoryx_publisher::{TradeSignalPublisher, TRADE_SIGNAL_PAYLOAD};
use rolling_common::arb_open_latency::record_arb_open_latency;
use runtime_common::time_util::get_timestamp_us;
use signal_common::common::SignalBytes;
use signal_common::trade_signal::SignalType;

pub fn emit_levels_as_signals<TCtx>(
    signal_pub: &TradeSignalPublisher,
    signal_type: SignalType,
    generation_time: i64,
    contexts: impl IntoIterator<Item = TCtx>,
) -> Result<usize>
where
    TCtx: SignalBytes,
{
    let mut sent = 0usize;
    let mut context = BytesMut::with_capacity(
        TRADE_SIGNAL_PAYLOAD - signal_common::trade_signal::TRADE_SIGNAL_HEADER_LEN,
    );
    for ctx in contexts {
        let publish_start_us = get_timestamp_us();
        if matches!(signal_type, SignalType::ArbOpen) && generation_time > 0 {
            record_arb_open_latency(
                "ts_publish_minus_generation",
                publish_start_us.saturating_sub(generation_time),
            );
        }
        context.clear();
        ctx.write_to(&mut context);
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
