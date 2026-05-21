use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateLimiter;
use crate::pre_trade::query_eng_channel::QueryEngHub;
use crate::pre_trade::resample_channel::ResampleChannel;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::signal_throttle::log_active_signal_throttles;
use crate::pre_trade::trade_eng_channel::TradeEngHub;
use crate::strategy::{OrphanStrategyManager, StrategyManager};
use anyhow::Result;
use log::{info, warn};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

pub struct PreTrade {}

fn drive_strategy_manager_period_clock_rc(
    strategy_mgr: &Rc<RefCell<StrategyManager>>,
    now: i64,
) -> usize {
    let iterations = strategy_mgr.borrow().len();
    let mut inspected = 0usize;
    for _ in 0..iterations {
        let strategy_opt = { strategy_mgr.borrow_mut().take_next_queued() };
        let Some(mut strategy) = strategy_opt else {
            break;
        };
        inspected += 1;
        strategy.handle_period_clock(now);
        if strategy.is_active() {
            strategy_mgr.borrow_mut().insert(strategy);
        }
    }
    inspected
}

fn drive_orphan_manager_period_clock_rc(
    orphan_strategy_mgr: &Rc<RefCell<OrphanStrategyManager>>,
    now: i64,
) -> usize {
    let iterations = orphan_strategy_mgr.borrow().len();
    let mut inspected = 0usize;
    for _ in 0..iterations {
        let strategy_opt = { orphan_strategy_mgr.borrow_mut().take_next_queued() };
        let Some(mut strategy) = strategy_opt else {
            break;
        };
        inspected += 1;
        strategy.handle_period_clock(now);
        if strategy.is_active() {
            orphan_strategy_mgr.borrow_mut().insert(strategy);
        }
    }
    inspected
}

fn drive_strategy_manager_period_clock(now: i64) {
    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
    let _ = drive_strategy_manager_period_clock_rc(&strategy_mgr, now);
}

fn drive_orphan_manager_period_clock(now: i64) {
    let orphan_strategy_mgr = MonitorChannel::instance().orphan_strategy_mgr();
    let _ = drive_orphan_manager_period_clock_rc(&orphan_strategy_mgr, now);
}

impl Default for PreTrade {
    fn default() -> Self {
        Self::new()
    }
}

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
        let order_rate_cleanup_interval = std::time::Duration::from_secs(10);
        let mut next_order_rate_cleanup = std::time::Instant::now() + order_rate_cleanup_interval;
        let arb_startup_net_log_interval = std::time::Duration::from_secs(30);
        let mut next_arb_startup_net_log = std::time::Instant::now() + arb_startup_net_log_interval;
        info!(
            "pre_trade signal throttle log started (interval={}s)",
            throttle_log_interval_secs
        );
        info!("pre_trade MM open order rate cleanup started (interval=10s window=60s)");

        // 周期检查频率设为 20ms，提高 MM trigger 响应及时性，同时保持较低调度开销。
        // IPC hot path 不等待这个 tick；空闲时先做 bounded busy-poll，超过预算才 yield。
        let period_clock_interval = Duration::from_millis(20);
        let mut next_period_clock = Instant::now();
        let idle_spin_iters = std::env::var("PRE_TRADE_REACTOR_IDLE_SPIN_ITERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(64);
        let mut idle_spin_count = 0usize;
        info!(
            "pre_trade reactor idle spin configured (iters={})",
            idle_spin_iters
        );

        loop {
            let mut has_work = false;
            has_work |= TradeEngHub::drain_pending_responses();
            has_work |= MonitorChannel::drain_pending_state_updates();
            has_work |= QueryEngHub::drain_pending_responses();
            has_work |= SignalChannel::drain_pending();

            let instant_now = Instant::now();
            let mut ran_periodic = false;
            if instant_now >= next_period_clock {
                ran_periodic = true;
                let now = get_timestamp_us();
                drive_strategy_manager_period_clock(now);
                drive_orphan_manager_period_clock(now);
                while instant_now >= next_period_clock {
                    next_period_clock += period_clock_interval;
                }

                // 发布重采样数据
                while instant_now >= next_resample {
                    let result = ResampleChannel::with(|ch| ch.publish_resample_entries());
                    if let Err(err) = result {
                        warn!("pre_trade resample publish failed: {err:#}");
                        next_resample = Instant::now() + resample_interval;
                        break;
                    }
                    next_resample += resample_interval;
                }

                while instant_now >= next_throttle_log {
                    log_active_signal_throttles(50);
                    next_throttle_log += throttle_log_interval;
                }

                while instant_now >= next_order_rate_cleanup {
                    OrderRateLimiter::cleanup_expired(now);
                    next_order_rate_cleanup += order_rate_cleanup_interval;
                }

                while instant_now >= next_arb_startup_net_log {
                    let status = MonitorChannel::instance().arb_startup_net_gate_status();
                    if status.enabled && !status.ready {
                        warn!(
                            "双边net还没有初始化: open_ready={} hedge_ready={} open_ts_us={} hedge_ts_us={} dropped_arb_signals={}",
                            status.open_ready,
                            status.hedge_ready,
                            status.open_ts_us,
                            status.hedge_ts_us,
                            status.dropped_signals
                        );
                    }
                    next_arb_startup_net_log += arb_startup_net_log_interval;
                }
            }

            if has_work || ran_periodic {
                idle_spin_count = 0;
                continue;
            }

            if idle_spin_count < idle_spin_iters {
                idle_spin_count += 1;
                std::hint::spin_loop();
                continue;
            }
            idle_spin_count = 0;

            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tokio::task::yield_now() => {}
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{drive_orphan_manager_period_clock_rc, drive_strategy_manager_period_clock_rc};
    use crate::signal::trade_signal::TradeSignal;
    use crate::strategy::orphan_order_strategy::OrphanOrderStrategy;
    use crate::strategy::{order_update::OrderUpdate, trade_update::TradeUpdate};
    use crate::strategy::{OrphanStrategyManager, Strategy, StrategyManager};
    use std::any::Any;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    struct ReentrantTickStrategy {
        id: i32,
        manager: Rc<RefCell<StrategyManager>>,
        tick_hits: Rc<Cell<u32>>,
        active: bool,
    }

    impl Strategy for ReentrantTickStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, _order_id: i64) -> bool {
            false
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {
            self.tick_hits.set(self.tick_hits.get() + 1);
            let _ = self.manager.borrow_mut().contains(self.id);
            self.active = false;
        }

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some("BTCUSDT")
        }
    }

    struct ReentrantOrphanTickStrategy {
        id: i32,
        manager: Rc<RefCell<OrphanStrategyManager>>,
        tick_hits: Rc<Cell<u32>>,
        active: bool,
    }

    impl Strategy for ReentrantOrphanTickStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, _order_id: i64) -> bool {
            false
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {
            self.tick_hits.set(self.tick_hits.get() + 1);
            let _ = self.manager.borrow_mut().contains(self.id);
            self.active = false;
        }

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some("BTCUSDT")
        }
    }

    #[test]
    fn period_clock_driver_releases_strategy_manager_borrow_before_callback() {
        let manager = Rc::new(RefCell::new(StrategyManager::new()));
        let tick_hits = Rc::new(Cell::new(0));
        manager.borrow_mut().insert(Box::new(ReentrantTickStrategy {
            id: 101,
            manager: manager.clone(),
            tick_hits: tick_hits.clone(),
            active: true,
        }));

        let inspected = drive_strategy_manager_period_clock_rc(&manager, 0);

        assert_eq!(inspected, 1);
        assert_eq!(tick_hits.get(), 1);
        assert!(!manager.borrow().contains(101));
    }

    #[test]
    fn period_clock_driver_releases_orphan_manager_borrow_before_callback() {
        let manager = Rc::new(RefCell::new(OrphanStrategyManager::new()));
        let tick_hits = Rc::new(Cell::new(0));
        manager
            .borrow_mut()
            .insert(Box::new(ReentrantOrphanTickStrategy {
                id: 202,
                manager: manager.clone(),
                tick_hits: tick_hits.clone(),
                active: true,
            }));

        let inspected = drive_orphan_manager_period_clock_rc(&manager, 0);

        assert_eq!(inspected, 1);
        assert_eq!(tick_hits.get(), 1);
        assert!(!manager.borrow().contains(202));
    }

    #[test]
    fn period_clock_driver_removes_empty_orphan_strategy() {
        let manager = Rc::new(RefCell::new(OrphanStrategyManager::new()));
        manager
            .borrow_mut()
            .insert(Box::new(OrphanOrderStrategy::new(303, "BTCUSDT")));

        let inspected = drive_orphan_manager_period_clock_rc(&manager, 0);

        assert_eq!(inspected, 1);
        assert!(!manager.borrow().contains(303));
        assert!(manager.borrow().is_empty());
    }
}
