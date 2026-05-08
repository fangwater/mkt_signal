//! 多交易所自动还款调度器。
//!
//! 启动策略：
//! 1. 启动后立即对所有已注册 Repayer 各跑一次。
//! 2. 之后每小时 :55 (UTC) 唤醒一次，依次跑所有 Repayer。
//!
//! Repayer 自己负责状态查询、还款执行、错误处理；调度器只负责定时和分发。

use async_trait::async_trait;
use chrono::{Timelike, Utc};
use log::info;
use std::time::Duration;

#[async_trait]
pub trait Repayer: Send + Sync {
    /// 用于日志的标识（e.g. "binance" / "gate" / "bybit"）。
    fn name(&self) -> &str;
    /// 一次完整的"查负债 → 决策 → 调还款"循环。自己处理所有错误，绝不 panic。
    async fn check_and_repay(&self);
}

#[derive(Default)]
pub struct AutoRepayService {
    repayers: Vec<Box<dyn Repayer>>,
}

impl AutoRepayService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, repayer: Box<dyn Repayer>) {
        info!("auto-repay registered: {}", repayer.name());
        self.repayers.push(repayer);
    }

    pub fn is_empty(&self) -> bool {
        self.repayers.is_empty()
    }

    /// 启动后台任务：先立即跑一轮，然后每小时 :55 唤醒一次。
    pub fn start(self) {
        if self.repayers.is_empty() {
            info!("auto-repay service has no repayers registered, not starting");
            return;
        }
        let names: Vec<String> = self.repayers.iter().map(|r| r.name().to_string()).collect();
        tokio::spawn(async move {
            info!(
                "auto-repay service started ({} repayer(s): {})",
                self.repayers.len(),
                names.join(", ")
            );

            // 启动即跑一轮
            self.run_all_once("startup").await;

            loop {
                let wait = Self::time_until_next_55min();
                info!(
                    "next auto-repay tick in {} seconds (UTC :55)",
                    wait.as_secs()
                );
                tokio::time::sleep(wait).await;
                self.run_all_once("hourly_55").await;
            }
        });
    }

    async fn run_all_once(&self, reason: &str) {
        for repayer in &self.repayers {
            info!("auto-repay tick: name={} reason={}", repayer.name(), reason);
            repayer.check_and_repay().await;
        }
    }

    /// 计算到下一个 UTC :55 的等待时间。
    fn time_until_next_55min() -> Duration {
        Self::time_until_next_55min_from(Utc::now())
    }

    fn time_until_next_55min_from(now: chrono::DateTime<Utc>) -> Duration {
        let current_min = now.minute();
        let current_sec = now.second();
        let minutes_to_wait = if current_min < 55 {
            55 - current_min
        } else {
            60 - current_min + 55
        };
        let total_seconds = minutes_to_wait as i64 * 60 - current_sec as i64;
        let total_seconds = total_seconds.max(1) as u64;
        Duration::from_secs(total_seconds)
    }
}

/// 已知 retCode/HTTP 等"无负债可还"的 hint，统一打印降噪。
pub fn looks_like_no_liability(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("no liability") || lower.contains("nothing to repay")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(h: u32, m: u32, s: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 8, h, m, s).unwrap()
    }

    #[test]
    fn time_until_next_55min_from_zero() {
        // 00:00:00 → 等 55 分钟
        let d = AutoRepayService::time_until_next_55min_from(at(0, 0, 0));
        assert_eq!(d.as_secs(), 55 * 60);
    }

    #[test]
    fn time_until_next_55min_one_second_before() {
        // 00:54:59 → 1 秒
        let d = AutoRepayService::time_until_next_55min_from(at(0, 54, 59));
        assert_eq!(d.as_secs(), 1);
    }

    #[test]
    fn time_until_next_55min_at_target() {
        // 00:55:00 → 等到下一小时的 :55，即 60 分钟
        let d = AutoRepayService::time_until_next_55min_from(at(0, 55, 0));
        assert_eq!(d.as_secs(), 60 * 60);
    }

    #[test]
    fn time_until_next_55min_after_target() {
        // 00:55:01 → 到 01:55:00 = 59 分 59 秒
        let d = AutoRepayService::time_until_next_55min_from(at(0, 55, 1));
        assert_eq!(d.as_secs(), 59 * 60 + 59);
    }

    #[test]
    fn time_until_next_55min_late_hour() {
        // 23:54:00 → 60 秒
        let d = AutoRepayService::time_until_next_55min_from(at(23, 54, 0));
        assert_eq!(d.as_secs(), 60);
    }

    #[test]
    fn time_until_next_55min_late_after_target() {
        // 23:55:30 → 到 00:55:00 = 59 分 30 秒
        let d = AutoRepayService::time_until_next_55min_from(at(23, 55, 30));
        assert_eq!(d.as_secs(), 59 * 60 + 30);
    }

    #[test]
    fn no_liability_hint_detection() {
        assert!(looks_like_no_liability(
            "{\"retCode\":182102,\"retMsg\":\"no liability\"}"
        ));
        assert!(looks_like_no_liability("Nothing to repay"));
        assert!(!looks_like_no_liability("ok"));
    }
}
