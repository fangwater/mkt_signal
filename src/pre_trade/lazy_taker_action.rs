use ipc_common::iceoryx_publisher::GenericPublisher;
use log::warn;
use signal_common::lazy_taker_action::{
    LazyTakerAction, LazyTakerActionMsg, LAZY_TAKER_ACTION_CHANNEL, LAZY_TAKER_ACTION_PAYLOAD,
};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

thread_local! {
    static ACTION_PUBLISHER: RefCell<Option<GenericPublisher<LAZY_TAKER_ACTION_PAYLOAD>>> =
        const { RefCell::new(None) };
}

static PUBLISH_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn publish_lazy_taker_action(
    local_tp_us: i64,
    symbol: &str,
    model_name: &str,
    venue: u8,
    action: LazyTakerAction,
    direction: i8,
) -> bool {
    let Some(msg) =
        LazyTakerActionMsg::new(local_tp_us, symbol, model_name, venue, action, direction)
    else {
        record_publish_error("invalid action message");
        return false;
    };
    let raw = msg.encode();
    ACTION_PUBLISHER.with(|cell| {
        let mut publisher = cell.borrow_mut();
        if publisher.is_none() {
            match GenericPublisher::new(LAZY_TAKER_ACTION_CHANNEL) {
                Ok(created) => *publisher = Some(created),
                Err(err) => {
                    record_publish_error(&format!("create publisher failed: {err:#}"));
                    return false;
                }
            }
        }
        match publisher
            .as_ref()
            .expect("publisher initialized")
            .publish(&raw)
        {
            Ok(()) => true,
            Err(err) => {
                record_publish_error(&format!("publish failed: {err:#}"));
                false
            }
        }
    })
}

fn record_publish_error(reason: &str) {
    let count = PUBLISH_ERROR_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if count == 1 || count.is_multiple_of(100) {
        warn!(
            "lazy taker action publish error count={} reason={}",
            count, reason
        );
    }
}
