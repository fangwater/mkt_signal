use std::time::{SystemTime, UNIX_EPOCH};

/// 获取当前时间的微秒时间戳。
#[inline(always)]
pub fn get_timestamp_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before UNIX_EPOCH")
        .as_micros() as i64
}
