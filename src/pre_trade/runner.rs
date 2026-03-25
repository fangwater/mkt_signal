use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::resample_channel::ResampleChannel;
use crate::pre_trade::signal_throttle::log_active_signal_throttles;
use anyhow::Result;
use log::{info, warn};
use std::time::Duration;

pub struct PreTrade {}

impl PreTrade {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade main loop starting");

        // 定时器状态
        let resample_interval = std::time::Duration::from_secs(3);
        let mut next_resample = std::time::Instant::now() + resample_interval;
        let throttle_log_interval_secs =
            std::env::var("PRE_TRADE_SIGNAL_THROTTLE_LOG_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(60);
        let throttle_log_interval = std::time::Duration::from_secs(throttle_log_interval_secs);
        let mut next_throttle_log = std::time::Instant::now() + throttle_log_interval;
        info!(
            "pre_trade signal throttle log started (interval={}s)",
            throttle_log_interval_secs
        );

        // 周期检查频率设为 20ms，提高 MM trigger 响应及时性，同时保持较低调度开销
        let mut ticker = tokio::time::interval(Duration::from_millis(20));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    break;
                }
                _ = ticker.tick() => {
                    let now = get_timestamp_us();
                    MonitorChannel::instance().strategy_mgr().borrow_mut().handle_period_clock(now);
                    let instant_now = std::time::Instant::now();

                    // 发布重采样数据
                    while instant_now >= next_resample {
                        let result = ResampleChannel::with(|ch| ch.publish_resample_entries());
                        if let Err(err) = result {
                            warn!("pre_trade resample publish failed: {err:#}");
                            next_resample = std::time::Instant::now() + resample_interval;
                            break;
                        }
                        next_resample += resample_interval;
                    }

                    while instant_now >= next_throttle_log {
                        log_active_signal_throttles(50);
                        next_throttle_log += throttle_log_interval;
                    }
                }
                else => break,
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}
