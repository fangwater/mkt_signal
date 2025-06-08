//检查是否需要重启
use tokio::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct RestartChecker {
    pub is_primary: bool,
    pub restart_duration_secs: u64,
}

impl RestartChecker {
    pub fn new(is_primary: bool, restart_duration: u64) -> Self {
        Self {
            is_primary,
            restart_duration_secs: restart_duration * 60,
        }
    }
    /// - 下一个对齐点的tokio Instant, 以及这个对齐的instant，对于初始纪元时间，偏移了多少个interval
    pub fn next_alignment(interval: Duration) -> (Instant, u64) {
        // 获取当前系统时间
        let now = SystemTime::now();
        
        // 计算自UNIX纪元以来的持续时间
        let since_epoch = now.duration_since(UNIX_EPOCH)
            .expect("System time is before UNIX epoch!");
        
        // 计算自纪元以来的总纳秒数 (使用u128防止溢出)
        let total_nanos: u128 = since_epoch.as_nanos();
        let interval_nanos: u128 = interval.as_nanos();
        
        // 处理间隔为0的特殊情况
        if interval_nanos == 0 {
            //error
            panic!("Interval is 0!");
        }
        
        // 计算当前偏移量和下一个对齐点的时间
        let current_interval_count = total_nanos / interval_nanos;
        let next_interval_count = current_interval_count + 1;
        let wait_nanos = next_interval_count * interval_nanos - total_nanos;
        
        // 计算下一个对齐点的tokio Instant
        let next_instant = Instant::now() + Duration::from_nanos(wait_nanos as u64);
        
        // 返回下一个对齐点的Instant和从纪元开始计算的间隔数
        (next_instant, next_interval_count as u64)
    }

    pub fn get_next_restart_instant(&self) -> Instant {
        //获取重启间隔
        let duration = Duration::from_secs(self.restart_duration_secs);
        //获取下一个对齐点
        let (next_instant, interval_count) = Self::next_alignment(duration);
        //primary在奇数个偏移时重启
        if self.is_primary {
            if interval_count % 2 != 0 {
                //对齐的重启时间点，对应interval_count是奇数，primary直接使用
                return next_instant;
            }
            else {
                //对齐的重启时间点，对应interval_count是偶数，primary需要等待一个interval后重启
                return next_instant + duration;
            }
        }
        //secondary在偶数个偏移时重启   
        else {
            if interval_count % 2 == 0 {
                //对齐的重启时间点，对应interval_count是偶数，secondary直接使用
                return next_instant;
            }
            else {
                //对齐的重启时间点，对应interval_count是奇数，secondary需要等待一个interval后重启
                return next_instant + duration;
            }
        }
    }   
}