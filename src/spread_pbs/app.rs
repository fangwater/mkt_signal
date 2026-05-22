use anyhow::{bail, Context, Result};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::common::mkt_msg::AskBidSpreadMsg;
use crate::common::time_util::get_timestamp_us;
use crate::mkt_pub::cfg::Config;
use crate::rolling_metrics::latency_kll::LatencyStats;
use crate::rolling_metrics::latency_snapshot::{
    LatencyBucketStat, LatencySnapshotMsg, ACTION_ID_MARKET_DATA, METRIC_ID_SPREAD_E2E,
    METRIC_ID_SPREAD_NET,
};

use crate::spread_pbs::adapter::{create_adapter, BboFrame, VenueAdapter};
use crate::spread_pbs::latency::LatencyKll;
use crate::spread_pbs::publisher::{SpreadLatencyPublisher, SpreadPublisher};
use crate::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};

const DEDUP_RESET_INTERVAL_US: i64 = 5 * 60 * 1_000_000;

pub struct SpreadPbsApp {
    config: Config,
}

/// 一条 ws 连接的运行态：shutdown 通道 + JoinHandle，外加重启用得上的 local_ip。
struct WsLeg {
    label: &'static str,
    local_ip: String,
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// 跨 leg 共享的上下文：adapter / publisher / state / ws url 在 spread_pbs 整个生命周期不变。
struct LegCtx {
    adapter: Rc<dyn VenueAdapter>,
    publisher: Rc<SpreadPublisher>,
    state: Rc<RefCell<SharedState>>,
    url: String,
}

impl SpreadPbsApp {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// 主入口：拉 symbol → 起双路 ws → 帧在 ws task 内同步处理（无 mpsc）。
    /// 与 dat_pbs 对齐：两条 ws 按错开半周期定时重启，重启前重新拉 symbol。
    ///
    /// 必须在 `LocalSet` 上下文里 await（`main` 用 `LocalSet::run_until`）。
    pub async fn run(self) -> Result<()> {
        let venue = self.config.venue;
        let venue_slug: &'static str = venue.data_pub_slug();

        let adapter = match create_adapter(venue).await? {
            Some(a) => Rc::<dyn VenueAdapter>::from(a),
            None => bail!(
                "spread_pbs 当前不支持 venue {:?}（仅 OKex/Binance/Bybit/Gate/Bitget × spot+futures）",
                venue
            ),
        };
        log::info!(
            "spread_pbs starting venue={} adapter={}",
            venue_slug,
            adapter.name()
        );

        // ---- 首次拉 symbol（含 BinanceFutures，无硬编码） ----
        // spread_pbs 不归 pm2 管，启动期 REST 抖动不能直接退出；用退避循环等到拿到非空列表
        let initial_symbols = self.config.wait_for_symbols().await;
        let mut current_symbols: HashSet<String> = initial_symbols.iter().cloned().collect();
        let initial_subs = adapter.build_subscribe(&initial_symbols);
        if initial_subs.is_empty() {
            bail!(
                "adapter.build_subscribe 返回空（{} symbols 数={}）",
                venue_slug,
                initial_symbols.len()
            );
        }
        log::info!(
            "spread_pbs[{}] initial symbols={} subscribe_batches={}",
            venue_slug,
            initial_symbols.len(),
            initial_subs.len()
        );

        // ---- IceOryx publisher + 共享态（Rc<RefCell> 单线程零锁，跨重启复用）----
        let publisher = Rc::new(
            SpreadPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx publisher for {}", venue_slug))?,
        );
        let latency_publisher = Rc::new(
            SpreadLatencyPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx latency publisher for {}", venue_slug))?,
        );
        let net_label = format!("{}-net", venue_slug);
        let ipc_net_label = format!("{}-ipc-net", venue_slug);
        let ipc_e2e_label = format!("{}-ipc", venue_slug);
        let state: Rc<RefCell<SharedState>> = Rc::new(RefCell::new(SharedState {
            dedup: HashMap::with_capacity(2048),
            latency_e2e: LatencyKll::new(venue_slug),
            latency_net: LatencyKll::new(net_label),
            latency_ipc_e2e: LatencyKll::new(ipc_e2e_label),
            latency_ipc_net: LatencyKll::new(ipc_net_label),
            published: 0,
            dropped_by_seq: 0,
            last_dedup_reset_us: get_timestamp_us(),
        }));

        let ctx = LegCtx {
            adapter: adapter.clone(),
            publisher: publisher.clone(),
            state: state.clone(),
            url: adapter.ws_url(),
        };

        // ---- 起两条 leg：primary / secondary，独立 shutdown 通道 ----
        let mut primary = spawn_leg(
            "primary",
            self.config.primary_local_ip.clone(),
            initial_subs.clone(),
            &ctx,
        );
        let mut secondary = spawn_leg(
            "secondary",
            self.config.secondary_local_ip.clone(),
            initial_subs,
            &ctx,
        );

        // ---- 错开半周期：primary t+T，secondary t+T/2，与 src/mkt_pub/app.rs 一致 ----
        let restart_duration = Duration::from_secs(self.config.restart_duration_secs);
        let mut next_primary_restart = Instant::now() + restart_duration;
        let mut next_secondary_restart = Instant::now() + restart_duration / 2;
        log::info!(
            "spread_pbs[{}] restart period={}s; first primary at +{}s, secondary at +{}s",
            venue_slug,
            self.config.restart_duration_secs,
            self.config.restart_duration_secs,
            self.config.restart_duration_secs / 2,
        );

        let mut stats_ticker = tokio::time::interval(Duration::from_secs(30));
        stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        stats_ticker.tick().await;

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    log::info!("spread_pbs[{}] SIGINT received, shutting down", venue_slug);
                    let _ = primary.shutdown_tx.send(true);
                    let _ = secondary.shutdown_tx.send(true);
                    let _ = (&mut primary.handle).await;
                    let _ = (&mut secondary.handle).await;
                    break;
                }
                _ = stats_ticker.tick() => {
                    let mut s = state.borrow_mut();
                    if let Some(msg) = take_latency_snapshot(&mut s, venue.to_u8() as u32) {
                        if let Err(e) = latency_publisher.publish(msg.into_bytes()) {
                            log::warn!("spread_pbs[{}] latency snapshot publish failed: {:#}", venue_slug, e);
                        }
                    }
                    log::info!(
                        "spread_pbs[{}] stats published={} dropped_by_seq={} symbols_seen={}",
                        venue_slug, s.published, s.dropped_by_seq, s.dedup.len()
                    );
                }
                _ = tokio::time::sleep_until(next_primary_restart) => {
                    restart_leg(
                        venue_slug,
                        &mut primary,
                        &self.config,
                        &ctx,
                        &mut current_symbols,
                    ).await;
                    next_primary_restart = Instant::now() + restart_duration;
                }
                _ = tokio::time::sleep_until(next_secondary_restart) => {
                    restart_leg(
                        venue_slug,
                        &mut secondary,
                        &self.config,
                        &ctx,
                        &mut current_symbols,
                    ).await;
                    next_secondary_restart = Instant::now() + restart_duration;
                }
            }
        }

        Ok(())
    }
}

fn spawn_leg(
    label: &'static str,
    local_ip: String,
    subs: Vec<serde_json::Value>,
    ctx: &LegCtx,
) -> WsLeg {
    let (tx, rx) = watch::channel(false);
    let handler = make_handler(
        label,
        ctx.adapter.clone(),
        ctx.publisher.clone(),
        ctx.state.clone(),
    );
    let handle = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label,
            url: ctx.url.clone(),
            local_ip: local_ip.clone(),
            headers: ctx.adapter.ws_headers(),
            subscribe_msgs: subs,
            keepalive: ctx.adapter.keepalive(),
        },
        handler,
        rx,
    ));
    WsLeg {
        label,
        local_ip,
        shutdown_tx: tx,
        handle,
    }
}

async fn restart_leg(
    venue_slug: &'static str,
    leg: &mut WsLeg,
    config: &Config,
    ctx: &LegCtx,
    current_symbols: &mut HashSet<String>,
) {
    log::info!("spread_pbs[{}] leg={} restart begin", venue_slug, leg.label);

    // 先拉新 symbol；失败/空一律保留旧 leg，不重启。
    let new_symbols = match config.get_symbols().await {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => {
            log::error!(
                "spread_pbs[{}] leg={} restart skipped: get_symbols() returned empty",
                venue_slug,
                leg.label
            );
            return;
        }
        Err(e) => {
            log::error!(
                "spread_pbs[{}] leg={} restart skipped: get_symbols() failed: {:#}",
                venue_slug,
                leg.label,
                e
            );
            return;
        }
    };
    let new_subs = ctx.adapter.build_subscribe(&new_symbols);
    if new_subs.is_empty() {
        log::error!(
            "spread_pbs[{}] leg={} restart skipped: adapter.build_subscribe empty (symbols={})",
            venue_slug,
            leg.label,
            new_symbols.len()
        );
        return;
    }
    let new_subs_len = new_subs.len();

    let new_set: HashSet<String> = new_symbols.iter().cloned().collect();
    let added = new_set.difference(current_symbols).count();
    let removed = current_symbols.difference(&new_set).count();
    *current_symbols = new_set;

    // 关旧、等真正退出，再起新。错开半周期保证此刻另一条 leg 仍在工作。
    let _ = leg.shutdown_tx.send(true);
    let _ = (&mut leg.handle).await;

    let new_leg = spawn_leg(leg.label, leg.local_ip.clone(), new_subs, ctx);
    leg.shutdown_tx = new_leg.shutdown_tx;
    leg.handle = new_leg.handle;

    log::info!(
        "spread_pbs[{}] leg={} restart done symbols={} added={} removed={} subscribe_batches={}",
        venue_slug,
        leg.label,
        new_symbols.len(),
        added,
        removed,
        new_subs_len,
    );
}

struct SharedState {
    dedup: HashMap<String, i64>,
    /// 被采纳消息：`accepted_us - ts_ms*1000`（u 最新判断通过后立刻采样）。
    latency_e2e: LatencyKll,
    /// 同上，保留 `-net` 标签便于与旧日志兼容。
    latency_net: LatencyKll,
    /// IPC latency snapshot buckets. Kept separate so periodic IPC snapshots do not reset log KLLs.
    latency_ipc_e2e: LatencyKll,
    latency_ipc_net: LatencyKll,
    published: u64,
    dropped_by_seq: u64,
    last_dedup_reset_us: i64,
}

fn make_handler(
    label: &'static str,
    adapter: Rc<dyn VenueAdapter>,
    publisher: Rc<SpreadPublisher>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |recv_us: i64, raw: &[u8]| {
        // 上层只 from_slice 一次，把同一份 &Value 给 adapter；非 JSON 文本帧
        // （如 Bitget "pong"）静默丢弃。dedup 统一在 process_frame 用 BboFrame.seq_id 完成。
        let frames = match serde_json::from_slice::<serde_json::Value>(raw) {
            Ok(value) => match adapter.parse_frame(&value) {
                Ok(v) => v,
                Err(e) => {
                    log::error!(
                        "spread_pbs[{}] adapter.parse_frame failed: {:#} payload={}",
                        label,
                        e,
                        value
                    );
                    return;
                }
            },
            Err(_) => match adapter.parse_binary_frame(raw) {
                Ok(v) => v,
                Err(e) => {
                    log::error!(
                        "spread_pbs[{}] adapter.parse_binary_frame failed: {:#}",
                        label,
                        e
                    );
                    return;
                }
            },
        };
        if frames.is_empty() {
            return;
        }
        let accepted_us = get_timestamp_us();
        let mut s = state.borrow_mut();
        for f in frames {
            process_frame(&mut s, &publisher, recv_us, accepted_us, f);
        }
    })
}

fn process_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadPublisher>,
    recv_us: i64,
    accepted_us: i64,
    f: BboFrame,
) {
    reset_dedup_high_water_if_needed(state, accepted_us, &f);

    let prev = state.dedup.get(&f.symbol).copied().unwrap_or(i64::MIN);
    if f.seq_id <= prev {
        state.dropped_by_seq += 1;
        return;
    }
    state.dedup.insert(f.symbol.clone(), f.seq_id);

    if f.ts_us > 0 {
        let net_us = (recv_us - f.ts_us) as f64;
        let e2e_us = (accepted_us - f.ts_us) as f64;
        state.latency_net.push(net_us);
        state.latency_e2e.push(e2e_us);
        state.latency_ipc_net.push(net_us);
        state.latency_ipc_e2e.push(e2e_us);
    }

    let msg = AskBidSpreadMsg::create(
        f.symbol.clone(),
        f.ts_us,
        f.bid_price,
        f.bid_amount,
        f.ask_price,
        f.ask_amount,
    );
    let bytes = msg.to_bytes();
    if let Err(e) = publisher.publish(&bytes) {
        log::warn!("spread_pbs publish failed: {:#}", e);
        return;
    }
    state.published += 1;
}

fn take_latency_snapshot(state: &mut SharedState, venue_id: u32) -> Option<LatencySnapshotMsg> {
    let mut msg = LatencySnapshotMsg::new(venue_id, get_timestamp_us());
    let mut idx = 0usize;

    snap_latency_bucket(
        &mut msg,
        &mut idx,
        METRIC_ID_SPREAD_NET,
        state.latency_ipc_net.snapshot_and_reset(),
    );
    snap_latency_bucket(
        &mut msg,
        &mut idx,
        METRIC_ID_SPREAD_E2E,
        state.latency_ipc_e2e.snapshot_and_reset(),
    );

    if idx == 0 {
        None
    } else {
        msg.n_buckets = idx as u32;
        Some(msg)
    }
}

fn snap_latency_bucket(
    msg: &mut LatencySnapshotMsg,
    idx: &mut usize,
    metric_id: u8,
    stats: Option<LatencyStats>,
) {
    let Some(stats) = stats else {
        return;
    };
    if *idx >= msg.buckets.len() {
        return;
    }
    msg.buckets[*idx] = LatencyBucketStat {
        metric_id,
        action_id: ACTION_ID_MARKET_DATA,
        _pad: [0; 6],
        n: stats.n,
        p50_us: stats.p50_us,
        p90_us: stats.p90_us,
        p95_us: stats.p95_us,
        p99_us: stats.p99_us,
    };
    *idx += 1;
}

fn reset_dedup_high_water_if_needed(state: &mut SharedState, accepted_us: i64, f: &BboFrame) {
    if accepted_us.saturating_sub(state.last_dedup_reset_us) >= DEDUP_RESET_INTERVAL_US {
        let cleared = state.dedup.len();
        state.dedup.clear();
        state.last_dedup_reset_us = accepted_us;
        log::warn!(
            "spread_pbs dedup high-water reset by interval cleared_symbols={} interval_us={}",
            cleared,
            DEDUP_RESET_INTERVAL_US
        );
    }

    let prev = state.dedup.get(&f.symbol).copied().unwrap_or(i64::MIN);
    if f.reset_seq && f.seq_id < prev {
        let cleared = state.dedup.len();
        state.dedup.clear();
        state.last_dedup_reset_us = accepted_us;
        log::warn!(
            "spread_pbs dedup high-water reset by snapshot symbol={} snapshot_u={} prev_u={} cleared_symbols={}",
            f.symbol,
            f.seq_id,
            prev,
            cleared
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state(now_us: i64) -> SharedState {
        SharedState {
            dedup: HashMap::new(),
            latency_e2e: LatencyKll::new("test-e2e"),
            latency_net: LatencyKll::new("test-net"),
            latency_ipc_e2e: LatencyKll::new("test-ipc-e2e"),
            latency_ipc_net: LatencyKll::new("test-ipc-net"),
            published: 0,
            dropped_by_seq: 0,
            last_dedup_reset_us: now_us,
        }
    }

    fn frame(symbol: &str, seq_id: i64, reset_seq: bool) -> BboFrame {
        BboFrame {
            symbol: symbol.to_string(),
            ts_us: 0,
            seq_id,
            reset_seq,
            bid_price: 1.0,
            bid_amount: 1.0,
            ask_price: 2.0,
            ask_amount: 1.0,
        }
    }

    #[test]
    fn bybit_snapshot_lower_u_resets_all_high_water_marks() {
        let now_us = 1_000_000;
        let mut state = test_state(now_us);
        state.dedup.insert("BTCUSDT".to_string(), 100);
        state.dedup.insert("ETHUSDT".to_string(), 200);

        reset_dedup_high_water_if_needed(&mut state, now_us + 1, &frame("BTCUSDT", 1, true));

        assert!(state.dedup.is_empty());
        assert_eq!(state.last_dedup_reset_us, now_us + 1);
    }

    #[test]
    fn bybit_forced_snapshot_same_u_does_not_reset_high_water_marks() {
        let now_us = 1_000_000;
        let mut state = test_state(now_us);
        state.dedup.insert("BTCUSDT".to_string(), 100);
        state.dedup.insert("ETHUSDT".to_string(), 200);

        reset_dedup_high_water_if_needed(&mut state, now_us + 1, &frame("BTCUSDT", 100, true));

        assert_eq!(state.dedup.get("BTCUSDT"), Some(&100));
        assert_eq!(state.dedup.get("ETHUSDT"), Some(&200));
        assert_eq!(state.last_dedup_reset_us, now_us);
    }

    #[test]
    fn dedup_high_water_marks_reset_periodically() {
        let now_us = 1_000_000;
        let mut state = test_state(now_us);
        state.dedup.insert("BTCUSDT".to_string(), 100);
        state.dedup.insert("ETHUSDT".to_string(), 200);

        reset_dedup_high_water_if_needed(
            &mut state,
            now_us + DEDUP_RESET_INTERVAL_US,
            &frame("BTCUSDT", 100, false),
        );

        assert!(state.dedup.is_empty());
        assert_eq!(state.last_dedup_reset_us, now_us + DEDUP_RESET_INTERVAL_US);
    }

    #[test]
    fn latency_snapshot_contains_spread_net_and_e2e_buckets() {
        let mut state = test_state(1_000_000);
        state.latency_ipc_net.push(10.0);
        state.latency_ipc_net.push(20.0);
        state.latency_ipc_e2e.push(12.0);
        state.latency_ipc_e2e.push(22.0);

        let msg = take_latency_snapshot(&mut state, 7).expect("snapshot");
        assert_eq!(msg.venue_id, 7);
        assert_eq!(msg.n_buckets, 2);
        assert_eq!(msg.buckets[0].metric_id, METRIC_ID_SPREAD_NET);
        assert_eq!(msg.buckets[0].action_id, ACTION_ID_MARKET_DATA);
        assert_eq!(msg.buckets[0].n, 2);
        assert_eq!(msg.buckets[1].metric_id, METRIC_ID_SPREAD_E2E);
        assert_eq!(msg.buckets[1].action_id, ACTION_ID_MARKET_DATA);
        assert_eq!(msg.buckets[1].n, 2);
        assert!(take_latency_snapshot(&mut state, 7).is_none());
    }
}
