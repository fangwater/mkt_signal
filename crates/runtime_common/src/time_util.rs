use std::time::{SystemTime, UNIX_EPOCH};

/// 获取当前时间的微秒时间戳
///
/// 这是一个高性能的实现，直接使用 SystemTime 来获取时间戳
/// 返回值是从 Unix epoch (1970-01-01 00:00:00 UTC) 开始的微秒数
#[inline(always)]
pub fn get_timestamp_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间早于 UNIX_EPOCH")
        .as_micros() as i64
}
