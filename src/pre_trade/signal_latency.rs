//! 通用 signal→submit 延迟测度。
//!
//! 在 egress 单点（`TradeEngHub::publish_order_request_for`）采集每张单从信号生成
//! (`signal_t`) 到报单/撤单请求发出 (`submit_t`) 的延迟，按 signal 类型 lazy 分桶。
//! 做法对标 trade_engine 的 `WsLatencyBuckets`（`ws_client.rs`）。
//!
//! 桶按需创建：同一进程只跑一个 mode（arb 或 mm），跑 arb 时只会出现 `Arb*` 桶，
//! mm 桶永不创建，零冗余。空桶不存在、不打日志。

use crate::rolling_metrics::latency_kll::LatencyKll;
use std::cell::RefCell;
use std::collections::HashMap;

/// 每桶容量，对齐 trade_engine 的 `LatencyKll::DEFAULT_CAPACITY`（10_000）；
/// 30s 时间窗口由 `LatencyKll` 默认继承，满任一即 flush。
const SIGNAL_LATENCY_CAPACITY: usize = 10_000;

/// 合理性上限（µs）。signal→submit 即时路径是 ms 级；超过此值的样本几乎只可能来自
/// 复用陈旧 order 的非即时 egress（orphan 接管、expired 撤单沿用很旧的 signal_t），
/// 计入会污染分位尾部，直接丢弃。2s 远高于任何正常即时链路。
const SIGNAL_LATENCY_MAX_US: i64 = 2_000_000;

thread_local! {
    /// signal 类型名 → 该类型的 signal→submit 延迟 KLL。lazy 建桶。
    static SIG_LAT: RefCell<HashMap<&'static str, LatencyKll>> = RefCell::new(HashMap::new());
}

/// 记录一次 signal→submit 延迟样本（µs）。
///
/// - `label`：signal 类型名（`SignalType::as_str()`，`&'static str`），作为桶 key。
/// - `submit_us`：报单/撤单请求发出的本地时间。
/// - `signal_us`：触发该动作的信号生成时间。
///
/// `signal_us <= 0`（无信号上下文，如 orphan 兜底单）、时钟回拨导致的负延迟、或超过
/// `SIGNAL_LATENCY_MAX_US`（复用陈旧 order 的非即时 egress）的样本直接丢弃。
pub fn record_signal_submit_latency(label: &'static str, submit_us: i64, signal_us: i64) {
    if signal_us <= 0 {
        return;
    }
    let delta = submit_us - signal_us;
    if !(0..=SIGNAL_LATENCY_MAX_US).contains(&delta) {
        return;
    }
    let delta_us = delta as f64;
    SIG_LAT.with(|map| {
        map.borrow_mut()
            .entry(label)
            .or_insert_with(|| {
                LatencyKll::with_capacity(format!("pre_trade {label}"), SIGNAL_LATENCY_CAPACITY)
            })
            .push(delta_us);
    });
}
