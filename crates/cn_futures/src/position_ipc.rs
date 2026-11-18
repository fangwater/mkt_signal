//! Shared by both runtimes; delivery failures invalidate observation and never
//! block a trading iteration or retry a trading request.
use crate::core::TradingCore;
use cn_futures_state::{publisher::Producer, RuntimeStatus};

#[derive(Default)]
pub struct Observation<'a> {
    pub account: Option<&'a cn_futures_state::AccountFunds>,
    pub marks: &'a [cn_futures_state::InstrumentMark],
    pub connected: bool,
    pub dropped: u64,
}

pub(crate) fn publish(
    publisher: &mut Option<Producer>,
    core: &TradingCore,
    transport_ready: bool,
    can_trade: bool,
    status: RuntimeStatus,
    force: bool,
    observation: Observation<'_>,
) {
    let failed = publisher.as_mut().is_some_and(|publisher| {
        publisher
            .capture(force, |snapshot| {
                core.capture_positions(snapshot)?;
                snapshot.account = observation.account.copied();
                snapshot.marks.extend_from_slice(observation.marks);
                snapshot.l1_connected = observation.connected;
                snapshot.l1_dropped = observation.dropped;
                snapshot.transport_ready = transport_ready;
                snapshot.can_trade = can_trade && status == RuntimeStatus::Running;
                snapshot.runtime_status = status;
                Ok(())
            })
            .is_err()
    });
    if failed {
        *publisher = None;
    } // Receiver will expire the last capture.
}
