use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::common::sliding_quantile::SlidingQuantileWindow;

/// 单生产者、多读者友好的定长环形缓冲，使用连续内存存储 f32 值。
/// 内部通过自增计数 + 取模定位，写入覆盖最旧元素。
pub struct RingBuffer {
    capacity: usize,
    data: Vec<UnsafeCell<f32>>,
    write_cursor: AtomicU64,
    published: AtomicU64,
    quantile_window: RwLock<SlidingQuantileWindow>,
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring capacity must be positive");
        let mut data = Vec::with_capacity(capacity);
        data.resize_with(capacity, || UnsafeCell::new(0.0));
        Self {
            capacity,
            data,
            write_cursor: AtomicU64::new(0),
            published: AtomicU64::new(0),
            quantile_window: RwLock::new(SlidingQuantileWindow::new(capacity, capacity)),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 写入一个新值，若超出容量则覆盖最早的数据。
    #[inline]
    pub fn push(&self, value: f32) {
        let pos = self.write_cursor.fetch_add(1, Ordering::Relaxed);
        let idx = (pos % self.capacity as u64) as usize;
        unsafe {
            *self.data[idx].get() = value;
        }
        let _ = self.quantile_window.write().push_f64(value as f64);
        std::sync::atomic::fence(Ordering::Release);
        self.published.store(pos + 1, Ordering::Release);
    }

    /// 当前有效数据量（至多等于容量）。
    #[inline]
    pub fn len(&self) -> usize {
        let total = self.published.load(Ordering::Acquire) as usize;
        total.min(self.capacity)
    }

    /// 获取最新值；若缓冲为空则返回 None。
    #[inline]
    pub fn last(&self) -> Option<f32> {
        let total = self.published.load(Ordering::Acquire);
        if total == 0 {
            return None;
        }
        let idx = ((total - 1) % self.capacity as u64) as usize;
        std::sync::atomic::fence(Ordering::Acquire);
        let value = unsafe { *self.data[idx].get() };
        Some(value)
    }

    pub fn quantiles_linear(&self, active_window: usize, qs: &[f32]) -> (usize, Vec<Option<f64>>) {
        let mut window = self.quantile_window.write();
        let active_window = active_window.min(self.capacity).max(1);
        if window.history_capacity() != self.capacity || window.active_window() != active_window {
            window.reconfigure(self.capacity, active_window);
        }
        let sample_size = window.sample_size();
        let values = window.quantiles_linear(qs);
        (sample_size, values)
    }
}

#[cfg(test)]
mod tests {
    use super::RingBuffer;

    #[test]
    fn quantiles_follow_latest_active_window() {
        let ring = RingBuffer::new(5);
        for value in [1.0_f32, 2.0, 3.0, 4.0] {
            ring.push(value);
        }

        let (count, values) = ring.quantiles_linear(3, &[0.0, 0.5, 1.0]);
        assert_eq!(count, 3);
        assert_eq!(values, vec![Some(2.0), Some(3.0), Some(4.0)]);
    }

    #[test]
    fn quantiles_reconfigure_from_history() {
        let ring = RingBuffer::new(6);
        for value in [1.0_f32, 2.0, 3.0, 4.0, 5.0] {
            ring.push(value);
        }

        let (count, values) = ring.quantiles_linear(4, &[0.0, 0.5, 1.0]);
        assert_eq!(count, 4);
        assert_eq!(values, vec![Some(2.0), Some(3.5), Some(5.0)]);
    }
}
