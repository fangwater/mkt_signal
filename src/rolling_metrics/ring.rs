use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::rolling_metrics::kll_quantile::segmented_quantiles_linear;

/// 单生产者、多读者友好的定长环形缓冲，使用连续内存存储 f32 值。
/// 内部通过自增计数 + 取模定位，写入覆盖最旧元素。
pub struct RingBuffer {
    capacity: usize,
    data: Vec<AtomicU32>,
    write_cursor: AtomicU64,
    published: AtomicU64,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring capacity must be positive");
        let mut data = Vec::with_capacity(capacity);
        data.resize_with(capacity, || AtomicU32::new(0.0_f32.to_bits()));
        Self {
            capacity,
            data,
            write_cursor: AtomicU64::new(0),
            published: AtomicU64::new(0),
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
        self.data[idx].store(value.to_bits(), Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::Release);
        self.published.store(pos + 1, Ordering::Release);
    }

    /// 当前有效数据量（至多等于容量）。
    #[inline]
    pub fn len(&self) -> usize {
        let total = self.published.load(Ordering::Acquire) as usize;
        total.min(self.capacity)
    }

    /// 是否为空。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        let value = f32::from_bits(self.data[idx].load(Ordering::Relaxed));
        Some(value)
    }

    pub fn quantiles_linear(&self, active_window: usize, qs: &[f32]) -> (usize, Vec<Option<f64>>) {
        let active_window = active_window.min(self.capacity).max(1);
        let (window_len, values) = self.latest_values(active_window);
        let (sample_size, values) = segmented_quantiles_linear(values, window_len, qs);
        (sample_size, values)
    }

    fn latest_values(&self, active_window: usize) -> (usize, impl Iterator<Item = f64> + '_) {
        let total = self.published.load(Ordering::Acquire) as usize;
        let sample_size = total.min(self.capacity).min(active_window);
        let start = total.saturating_sub(sample_size);
        let values = (start..total).map(|pos| {
            let idx = pos % self.capacity;
            f32::from_bits(self.data[idx].load(Ordering::Relaxed)) as f64
        });
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
