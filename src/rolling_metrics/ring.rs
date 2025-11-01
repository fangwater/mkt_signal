use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

/// 单生产者、多读者友好的定长环形缓冲，使用连续内存存储 f32 值。
/// 内部通过自增计数 + 取模定位，写入覆盖最旧元素。
pub struct RingBuffer {
    capacity: usize,
    data: Vec<UnsafeCell<f32>>,
    write_cursor: AtomicU64,
    published: AtomicU64,
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

    /// 拷贝最近 `count` 条数据到 `dst`，并保持时间顺序（旧→新）。
    /// 返回实际拷贝条数。
    pub fn copy_latest(&self, count: usize, dst: &mut Vec<f32>) -> usize {
        dst.clear();
        if count == 0 {
            return 0;
        }
        let total = self.published.load(Ordering::Acquire);
        if total == 0 {
            return 0;
        }

        let available = total.min(self.capacity as u64) as usize;
        let to_copy = count.min(available);
        dst.reserve(to_copy);

        std::sync::atomic::fence(Ordering::Acquire);

        let capacity = self.capacity as u64;
        let mut pos = total.saturating_sub(to_copy as u64);
        for _ in 0..to_copy {
            let idx = (pos % capacity) as usize;
            let value = unsafe { *self.data[idx].get() };
            dst.push(value);
            pos += 1;
        }
        to_copy
    }
}
