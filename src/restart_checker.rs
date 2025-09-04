//检查是否需要重启
use tokio::time::{Duration, Instant};

pub struct RestartChecker {
    pub restart_duration_secs: u64,
}

impl RestartChecker {
    pub fn new(restart_duration: u64) -> Self {
        Self {
            restart_duration_secs: restart_duration,
        }
    }
    pub fn get_next_restart_instant(&self) -> Instant {
        // 简单地返回从现在开始的下一个重启时间
        let duration = Duration::from_secs(self.restart_duration_secs);
        let next_instant = Instant::now() + duration;
        
        log::info!("Next restart in {} seconds, at instant: {:?}", 
                 self.restart_duration_secs, next_instant);
        
        next_instant
    }   
}