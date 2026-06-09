use std::time::{SystemTime, UNIX_EPOCH};

#[inline(always)]
pub fn get_timestamp_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before UNIX_EPOCH")
        .as_micros() as i64
}
