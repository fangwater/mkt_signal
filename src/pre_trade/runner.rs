use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParams;
use crate::pre_trade::resample_channel::ResampleChannel;
use anyhow::Result;
use log::{info, warn};
use std::time::Duration;


pub struct PreTrade {}

impl PreTrade {
    pub fn new() -> Self {
        Self {}
    } 

    pub async fn run(self) -> Result<()> {
        info!("pre_trade starting");

        // 定时器状态
        let resample_interval = std::time::Duration::from_secs(3);
        let mut next_resample = std::time::Instant::now() + resample_interval;
        let mut next_params_refresh = std::time::Instant::now();

        // 首次从 Redis 拉取 pre-trade 参数 
        let redis_url = std::env::var("REDIS_URL").ok();
        if let Err(err) = PreTradeParams::instance().load_from_redis(redis_url.as_deref()).await {
            warn!("pre_trade initial params load failed: {err:#}");
        } 

        // 提升周期检查频率到 100ms，使策略状态响应更及时
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
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
                    
                    // 从 Redis 刷新参数 
                    if instant_now >= next_params_refresh {
                        let redis_url = std::env::var("REDIS_URL").ok();
                        match PreTradeParams::instance().load_from_redis(redis_url.as_deref()).await {
                            Ok(()) => {}
                            Err(err) => {
                                warn!("pre_trade params refresh failed: {err:#}");
                            } 
                        } 
                        let refresh_secs = PreTradeParams::instance().refresh_secs();
                        next_params_refresh = instant_now + std::time::Duration::from_secs(refresh_secs.max(5));
                    } 

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
                } 
                else => break,
            }
        } 

        info!("pre_trade exiting");
        Ok(()) 
    }
}
