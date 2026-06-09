use crate::latency_kll::LatencyKll;
use std::cell::RefCell;
use std::collections::HashMap;

const ARB_OPEN_LATENCY_CAPACITY: usize = 10_000;
const ARB_OPEN_LATENCY_MAX_US: i64 = 2_000_000;

thread_local! {
    static ARB_OPEN_LAT: RefCell<HashMap<&'static str, LatencyKll>> = RefCell::new(HashMap::new());
}

pub fn record_arb_open_latency(stage: &'static str, delta_us: i64) {
    if !(0..=ARB_OPEN_LATENCY_MAX_US).contains(&delta_us) {
        return;
    }
    ARB_OPEN_LAT.with(|map| {
        map.borrow_mut()
            .entry(stage)
            .or_insert_with(|| {
                LatencyKll::with_capacity(
                    format!("arb_open_path {stage}"),
                    ARB_OPEN_LATENCY_CAPACITY,
                )
            })
            .push(delta_us as f64);
    });
}
