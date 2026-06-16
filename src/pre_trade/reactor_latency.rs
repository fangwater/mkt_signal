use rolling_common::kll_quantile::StreamingKllSketch;
use std::cell::RefCell;
use std::time::{Duration, Instant};

const REACTOR_LATENCY_WINDOW: Duration = Duration::from_secs(30);

struct StageLatency {
    stage: ReactorStage,
    samples: StreamingKllSketch,
    window_start: Instant,
}

impl StageLatency {
    fn new(stage: ReactorStage) -> Self {
        Self {
            stage,
            samples: StreamingKllSketch::new(),
            window_start: Instant::now(),
        }
    }

    fn push(&mut self, delta_us: i64) {
        if self.window_start.elapsed() >= REACTOR_LATENCY_WINDOW {
            self.flush();
        }
        if delta_us < 0 {
            return;
        }
        self.samples.insert(delta_us as f64);
    }

    fn flush(&mut self) {
        if self.samples.is_empty() {
            self.window_start = Instant::now();
            return;
        }
        let qs = [0.50_f32, 0.90, 0.95, 0.99];
        let n = self.samples.len();
        let results = self.samples.quantiles(&qs);
        let p50 = results.first().and_then(|v| *v).unwrap_or(f64::NAN);
        let p90 = results.get(1).and_then(|v| *v).unwrap_or(f64::NAN);
        let p95 = results.get(2).and_then(|v| *v).unwrap_or(f64::NAN);
        let p99 = results.get(3).and_then(|v| *v).unwrap_or(f64::NAN);
        log::info!(
            "pre_trade_reactor[{}] latency_us n={} p50={:.0} p90={:.0} p95={:.0} p99={:.0}",
            self.stage.label(),
            n,
            p50,
            p90,
            p95,
            p99
        );
        self.samples.reset();
        self.window_start = Instant::now();
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ReactorStage {
    Periodic,
    ReactorGap,
    BeforeSignal,
    PreviousLoop,
    MonitorRefreshBasicState,
    MonitorPendingRisk,
}

impl ReactorStage {
    fn label(self) -> &'static str {
        match self {
            Self::Periodic => "periodic",
            Self::ReactorGap => "reactor_gap",
            Self::BeforeSignal => "before_signal",
            Self::PreviousLoop => "previous_loop",
            Self::MonitorRefreshBasicState => "monitor_refresh_basic_state",
            Self::MonitorPendingRisk => "monitor_pending_risk",
        }
    }
}

struct ReactorLatencies {
    periodic: StageLatency,
    reactor_gap: StageLatency,
    before_signal: StageLatency,
    previous_loop: StageLatency,
    monitor_refresh_basic_state: StageLatency,
    monitor_pending_risk: StageLatency,
}

impl ReactorLatencies {
    fn new() -> Self {
        Self {
            periodic: StageLatency::new(ReactorStage::Periodic),
            reactor_gap: StageLatency::new(ReactorStage::ReactorGap),
            before_signal: StageLatency::new(ReactorStage::BeforeSignal),
            previous_loop: StageLatency::new(ReactorStage::PreviousLoop),
            monitor_refresh_basic_state: StageLatency::new(ReactorStage::MonitorRefreshBasicState),
            monitor_pending_risk: StageLatency::new(ReactorStage::MonitorPendingRisk),
        }
    }

    fn get_mut(&mut self, stage: ReactorStage) -> &mut StageLatency {
        match stage {
            ReactorStage::Periodic => &mut self.periodic,
            ReactorStage::ReactorGap => &mut self.reactor_gap,
            ReactorStage::BeforeSignal => &mut self.before_signal,
            ReactorStage::PreviousLoop => &mut self.previous_loop,
            ReactorStage::MonitorRefreshBasicState => &mut self.monitor_refresh_basic_state,
            ReactorStage::MonitorPendingRisk => &mut self.monitor_pending_risk,
        }
    }
}

thread_local! {
    static REACTOR_LATENCIES: RefCell<ReactorLatencies> =
        RefCell::new(ReactorLatencies::new());
}

pub fn record_stage_latency(stage: ReactorStage, start_us: i64, end_us: i64) {
    let delta_us = end_us.saturating_sub(start_us);
    REACTOR_LATENCIES.with(|latencies| {
        latencies.borrow_mut().get_mut(stage).push(delta_us);
    });
}
