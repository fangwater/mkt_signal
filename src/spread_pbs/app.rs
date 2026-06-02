use anyhow::{bail, Context, Result};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::common::mkt_msg::{AskBidSpreadMsg, IncMsg, TradeMsg};
use crate::common::time_util::get_timestamp_us;
use crate::mkt_pub::cfg::Config;
use crate::rolling_metrics::latency_kll::LatencyStats;
use crate::rolling_metrics::latency_snapshot::{
    LatencyBucketStat, LatencySnapshotMsg, ACTION_ID_MARKET_DATA, METRIC_ID_SPREAD_E2E,
    METRIC_ID_SPREAD_NET,
};

use crate::spread_pbs::adapter::{
    create_adapter, BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};
use crate::spread_pbs::latency::LatencyKll;
use crate::spread_pbs::okex::fetch_books_sbe_snapshot_bytes;
use crate::spread_pbs::okex_derivatives::{
    build_okex_derivatives_subscribe_msgs, parse_okex_derivatives_frame, OKEX_PUBLIC_WS_URL,
};
use crate::spread_pbs::publisher::{
    SpreadDerivativesPublisher, SpreadIncrementalPublisher, SpreadLatencyPublisher,
    SpreadPublisher, SpreadTradePublisher,
};
use crate::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};

const DEDUP_RESET_INTERVAL_US: i64 = 5 * 60 * 1_000_000;

pub struct SpreadPbsApp {
    config: Config,
}

fn is_okex_venue(venue: crate::signal::common::TradingVenue) -> bool {
    matches!(
        venue,
        crate::signal::common::TradingVenue::OkexMargin
            | crate::signal::common::TradingVenue::OkexFutures
    )
}

fn is_okex_derivatives_venue(venue: crate::signal::common::TradingVenue) -> bool {
    matches!(venue, crate::signal::common::TradingVenue::OkexFutures)
}

fn build_market_subscribe(
    adapter: &Rc<dyn VenueAdapter>,
    symbols: &[String],
    include_incremental: bool,
) -> Vec<serde_json::Value> {
    let mut out = adapter.build_subscribe(symbols);
    if include_incremental {
        out.extend(adapter.build_incremental_subscribe(symbols));
    }
    out
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
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    incremental_max_levels: Option<usize>,
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
        let okex_incremental_enabled =
            is_okex_venue(venue) && self.config.data_types.enable_incremental;
        let initial_subs =
            build_market_subscribe(&adapter, &initial_symbols, okex_incremental_enabled);
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
        let trade_publisher = if is_okex_venue(venue) {
            Some(Rc::new(
                SpreadTradePublisher::new_create_only(venue_slug).unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to create replacement trade ipc channel dat_pbs/{}/trade: {:#}",
                        venue_slug, venue_slug, e
                    )
                }),
            ))
        } else {
            None
        };
        let incremental_publisher = if okex_incremental_enabled {
            Some(Rc::new(
                SpreadIncrementalPublisher::new_create_only(venue_slug).unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to create replacement incremental ipc channel dat_pbs/{}/incremental: {:#}",
                        venue_slug, venue_slug, e
                    )
                }),
            ))
        } else {
            None
        };
        let derivatives_publisher = if is_okex_derivatives_venue(venue)
            && self.config.data_types.enable_derivatives
        {
            Some(Rc::new(
                SpreadDerivativesPublisher::new_create_only(venue_slug).unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to create replacement derivatives ipc channel dat_pbs/{}/derivatives: {:#}",
                        venue_slug, venue_slug, e
                    )
                }),
            ))
        } else {
            None
        };
        let latency_publisher = Rc::new(
            SpreadLatencyPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx latency publisher for {}", venue_slug))?,
        );
        let net_label = format!("{}-net", venue_slug);
        let ipc_net_label = format!("{}-ipc-net", venue_slug);
        let ipc_e2e_label = format!("{}-ipc", venue_slug);
        let state: Rc<RefCell<SharedState>> = Rc::new(RefCell::new(SharedState {
            dedup: HashMap::with_capacity(2048),
            trade_dedup: HashMap::with_capacity(2048),
            incremental_seq: HashMap::with_capacity(2048),
            latency_e2e: LatencyKll::new(venue_slug),
            latency_net: LatencyKll::new(net_label),
            latency_ipc_e2e: LatencyKll::new(ipc_e2e_label),
            latency_ipc_net: LatencyKll::new(ipc_net_label),
            published: 0,
            trades_published: 0,
            incremental_published: 0,
            incremental_dropped_by_seq: 0,
            incremental_gap_warnings: 0,
            derivatives_published: 0,
            dropped_by_seq: 0,
            trades_dropped_by_seq: 0,
            last_dedup_reset_us: get_timestamp_us(),
        }));

        let ctx = LegCtx {
            adapter: adapter.clone(),
            publisher: publisher.clone(),
            trade_publisher: trade_publisher.clone(),
            incremental_publisher: incremental_publisher.clone(),
            incremental_max_levels: self.config.data_types.max_levels_per_msg,
            state: state.clone(),
            url: adapter.ws_url(),
        };

        if let Some(incremental_publisher) = incremental_publisher.as_ref() {
            bootstrap_okex_incremental_symbols(
                venue_slug,
                &adapter,
                incremental_publisher,
                &state,
                &initial_symbols,
                self.config.data_types.max_levels_per_msg,
                "initial",
            )
            .await;
        }

        let derivatives_symbols: Rc<RefCell<HashSet<String>>> =
            Rc::new(RefCell::new(initial_symbols.iter().cloned().collect()));
        let mut derivatives_leg = derivatives_publisher.as_ref().map(|publisher| {
            spawn_derivatives_leg(
                self.config.primary_local_ip.clone(),
                build_okex_derivatives_subscribe_msgs(
                    &initial_symbols,
                    self.config.get_batch_size(),
                ),
                publisher.clone(),
                derivatives_symbols.clone(),
                state.clone(),
            )
        });

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
                    if let Some(leg) = derivatives_leg.as_mut() {
                        let _ = leg.shutdown_tx.send(true);
                    }
                    let _ = (&mut primary.handle).await;
                    let _ = (&mut secondary.handle).await;
                    if let Some(leg) = derivatives_leg.as_mut() {
                        let _ = (&mut leg.handle).await;
                    }
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
                        "spread_pbs[{}] stats published={} trades_published={} incremental_published={} derivatives_published={} dropped_by_seq={} trades_dropped_by_seq={} incremental_dropped_by_seq={} incremental_gap_warnings={} symbols_seen={} trade_symbols_seen={} incremental_symbols_seen={}",
                        venue_slug,
                        s.published,
                        s.trades_published,
                        s.incremental_published,
                        s.derivatives_published,
                        s.dropped_by_seq,
                        s.trades_dropped_by_seq,
                        s.incremental_dropped_by_seq,
                        s.incremental_gap_warnings,
                        s.dedup.len(),
                        s.trade_dedup.len(),
                        s.incremental_seq.len()
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
        ctx.trade_publisher.clone(),
        ctx.incremental_publisher.clone(),
        ctx.incremental_max_levels,
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

async fn bootstrap_okex_incremental_symbols(
    venue_slug: &'static str,
    adapter: &Rc<dyn VenueAdapter>,
    publisher: &Rc<SpreadIncrementalPublisher>,
    state: &Rc<RefCell<SharedState>>,
    symbols: &[String],
    max_levels: Option<usize>,
    reason: &str,
) {
    let mut bootstrapped = 0usize;
    for symbol in symbols {
        let Some(inst_id_code) = adapter.inst_id_code(symbol) else {
            log::warn!(
                "spread_pbs[{}] OKX books bootstrap skipped: symbol={} missing instIdCode",
                venue_slug,
                symbol
            );
            continue;
        };
        let raw = match fetch_books_sbe_snapshot_bytes(inst_id_code).await {
            Ok(raw) => raw,
            Err(e) => {
                log::warn!(
                    "spread_pbs[{}] OKX books bootstrap snapshot failed symbol={} instIdCode={} reason={} err={:#}",
                    venue_slug,
                    symbol,
                    inst_id_code,
                    reason,
                    e
                );
                continue;
            }
        };
        let frames = match adapter.parse_incremental_binary_frame(&raw) {
            Ok(frames) => frames,
            Err(e) => {
                log::warn!(
                    "spread_pbs[{}] OKX books bootstrap decode failed symbol={} instIdCode={} reason={} err={:#}",
                    venue_slug,
                    symbol,
                    inst_id_code,
                    reason,
                    e
                );
                continue;
            }
        };
        let mut s = state.borrow_mut();
        for frame in frames {
            process_incremental_frame(&mut s, publisher, frame, max_levels);
            bootstrapped += 1;
        }
    }
    log::info!(
        "spread_pbs[{}] OKX books bootstrap done symbols={} frames={} reason={}",
        venue_slug,
        symbols.len(),
        bootstrapped,
        reason
    );
}

fn spawn_derivatives_leg(
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    publisher: Rc<SpreadDerivativesPublisher>,
    active_symbols: Rc<RefCell<HashSet<String>>>,
    state: Rc<RefCell<SharedState>>,
) -> WsLeg {
    let (tx, rx) = watch::channel(false);
    let handler = make_derivatives_handler(publisher, active_symbols, state);
    let handle = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label: "okex-derivatives",
            url: OKEX_PUBLIC_WS_URL.to_string(),
            local_ip: local_ip.clone(),
            headers: Vec::new(),
            subscribe_msgs,
            keepalive: Some(KeepaliveSpec::text(Duration::from_secs(25), "ping")),
        },
        handler,
        rx,
    ));
    WsLeg {
        label: "okex-derivatives",
        local_ip,
        shutdown_tx: tx,
        handle,
    }
}

fn make_derivatives_handler(
    publisher: Rc<SpreadDerivativesPublisher>,
    active_symbols: Rc<RefCell<HashSet<String>>>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |_recv_us: i64, raw: &[u8]| {
        let value = match serde_json::from_slice::<serde_json::Value>(raw) {
            Ok(value) => value,
            Err(_) => return,
        };
        let bytes = {
            let symbols = active_symbols.borrow();
            parse_okex_derivatives_frame(&value, &symbols)
        };
        if bytes.is_empty() {
            return;
        }
        let mut s = state.borrow_mut();
        for msg in bytes {
            if let Err(e) = publisher.publish(&msg) {
                log::warn!("spread_pbs derivatives publish failed: {:#}", e);
                continue;
            }
            s.derivatives_published += 1;
        }
    })
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
    let new_subs = build_market_subscribe(
        &ctx.adapter,
        &new_symbols,
        ctx.incremental_publisher.is_some(),
    );
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
    trade_dedup: HashMap<String, i64>,
    incremental_seq: HashMap<String, i64>,
    /// 被采纳消息：`accepted_us - ts_ms*1000`（u 最新判断通过后立刻采样）。
    latency_e2e: LatencyKll,
    /// 同上，保留 `-net` 标签便于与旧日志兼容。
    latency_net: LatencyKll,
    /// IPC latency snapshot buckets. Kept separate so periodic IPC snapshots do not reset log KLLs.
    latency_ipc_e2e: LatencyKll,
    latency_ipc_net: LatencyKll,
    published: u64,
    trades_published: u64,
    incremental_published: u64,
    incremental_dropped_by_seq: u64,
    incremental_gap_warnings: u64,
    derivatives_published: u64,
    dropped_by_seq: u64,
    trades_dropped_by_seq: u64,
    last_dedup_reset_us: i64,
}

fn make_handler(
    label: &'static str,
    adapter: Rc<dyn VenueAdapter>,
    publisher: Rc<SpreadPublisher>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |recv_us: i64, raw: &[u8]| {
        // 上层只 from_slice 一次，把同一份 &Value 给 adapter；非 JSON 文本帧
        // （如 Bitget "pong"）静默丢弃。dedup 统一在 process_frame 用 BboFrame.seq_id 完成。
        let (frames, trades, incrementals) = match serde_json::from_slice::<serde_json::Value>(raw)
        {
            Ok(value) => {
                let frames = match adapter.parse_frame(&value) {
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
                };
                (frames, Vec::new(), Vec::new())
            }
            Err(_) => {
                let frames = match adapter.parse_binary_frame(raw) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "spread_pbs[{}] adapter.parse_binary_frame failed: {:#}",
                            label,
                            e
                        );
                        Vec::new()
                    }
                };
                let trades = if trade_publisher.is_some() {
                    match adapter.parse_trade_binary_frame(raw) {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!(
                                "spread_pbs[{}] adapter.parse_trade_binary_frame failed: {:#}",
                                label,
                                e
                            );
                            Vec::new()
                        }
                    }
                } else {
                    Vec::new()
                };
                let incrementals = if incremental_publisher.is_some() {
                    match adapter.parse_incremental_binary_frame(raw) {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!(
                                "spread_pbs[{}] adapter.parse_incremental_binary_frame failed: {:#}",
                                label,
                                e
                            );
                            Vec::new()
                        }
                    }
                } else {
                    Vec::new()
                };
                (frames, trades, incrementals)
            }
        };
        if frames.is_empty() && trades.is_empty() && incrementals.is_empty() {
            return;
        }
        let accepted_us = get_timestamp_us();
        let mut s = state.borrow_mut();
        for f in frames {
            process_frame(&mut s, &publisher, recv_us, accepted_us, f);
        }
        if let Some(trade_publisher) = trade_publisher.as_ref() {
            for trade in trades {
                process_trade_frame(&mut s, trade_publisher, trade);
            }
        }
        if let Some(incremental_publisher) = incremental_publisher.as_ref() {
            for incremental in incrementals {
                process_incremental_frame(
                    &mut s,
                    incremental_publisher,
                    incremental,
                    incremental_max_levels,
                );
            }
        }
    })
}

fn process_trade_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadTradePublisher>,
    f: TradeFrame,
) {
    let prev = state
        .trade_dedup
        .get(&f.symbol)
        .copied()
        .unwrap_or(i64::MIN);
    if f.seq_id <= prev {
        state.trades_dropped_by_seq += 1;
        return;
    }
    state.trade_dedup.insert(f.symbol.clone(), f.seq_id);

    let msg = TradeMsg::create(
        f.symbol,
        f.trade_id,
        f.timestamp_us,
        f.side,
        f.price,
        f.amount,
    );
    let bytes = msg.to_bytes();
    if let Err(e) = publisher.publish(&bytes) {
        log::warn!("spread_pbs trade publish failed: {:#}", e);
        return;
    }
    state.trades_published += 1;
}

fn process_incremental_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    frame: IncrementalFrame,
    max_levels: Option<usize>,
) {
    let (symbol, timestamp, seq_id, prev_seq_id, is_snapshot, bids, asks) = match frame {
        IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id,
            prev_seq_id,
            is_snapshot,
            bids,
            asks,
        } => (
            symbol,
            timestamp,
            seq_id,
            prev_seq_id,
            is_snapshot,
            bids,
            asks,
        ),
        IncrementalFrame::SequenceOnly {
            symbol,
            timestamp,
            seq_id,
            prev_seq_id,
        } => {
            warn_incremental_gap_if_needed(state, &symbol, seq_id, prev_seq_id, false);
            state.incremental_seq.insert(symbol, seq_id);
            let _ = timestamp;
            return;
        }
    };

    warn_incremental_gap_if_needed(state, &symbol, seq_id, prev_seq_id, is_snapshot);

    let prev = state
        .incremental_seq
        .get(&symbol)
        .copied()
        .unwrap_or(i64::MIN);
    if !is_snapshot && seq_id <= prev && seq_id != prev_seq_id {
        state.incremental_dropped_by_seq += 1;
        return;
    }

    let chunks = split_levels(bids.len(), asks.len(), max_levels);
    let total_chunks = chunks.len();
    for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
        chunks.into_iter().enumerate()
    {
        let mut inc_msg = IncMsg::create(
            symbol.clone(),
            seq_id,
            prev_seq_id,
            timestamp,
            is_snapshot,
            bids_count as u32,
            asks_count as u32,
        );
        inc_msg.set_chunk_index(chunk_idx as u8);
        inc_msg.set_is_last(chunk_idx == total_chunks - 1);
        for (idx, level) in bids[bids_start..bids_start + bids_count].iter().enumerate() {
            inc_msg.set_bid_level(idx, *level);
        }
        for (idx, level) in asks[asks_start..asks_start + asks_count].iter().enumerate() {
            inc_msg.set_ask_level(idx, *level);
        }
        let bytes = inc_msg.to_bytes();
        if let Err(e) = publisher.publish(&bytes) {
            log::warn!("spread_pbs incremental publish failed: {:#}", e);
            return;
        }
        state.incremental_published += 1;
    }
    state.incremental_seq.insert(symbol, seq_id);
}

fn warn_incremental_gap_if_needed(
    state: &mut SharedState,
    symbol: &str,
    seq_id: i64,
    prev_seq_id: i64,
    is_snapshot: bool,
) {
    if is_snapshot {
        return;
    }
    let prev = state.incremental_seq.get(symbol).copied();
    if seq_id < prev_seq_id {
        state.incremental_gap_warnings += 1;
        log::warn!(
            "spread_pbs OKX books sequence reset observed symbol={} prevSeqId={} seqId={}; not resyncing",
            symbol,
            prev_seq_id,
            seq_id
        );
        return;
    }
    if seq_id == prev_seq_id {
        return;
    }
    if let Some(last_seq) = prev {
        if prev_seq_id != last_seq {
            state.incremental_gap_warnings += 1;
            log::warn!(
                "spread_pbs OKX books gap observed symbol={} local_seq={} prevSeqId={} seqId={}; not resyncing",
                symbol,
                last_seq,
                prev_seq_id,
                seq_id
            );
        }
    }
}

fn split_levels(
    total_bids: usize,
    total_asks: usize,
    max_levels: Option<usize>,
) -> Vec<(usize, usize, usize, usize)> {
    let total = total_bids + total_asks;
    match max_levels {
        Some(max) if total > max && max > 0 => {
            let mut chunks = Vec::new();
            let mut bids_sent = 0;
            let mut asks_sent = 0;
            while bids_sent < total_bids || asks_sent < total_asks {
                let bids_remaining = total_bids - bids_sent;
                let asks_remaining = total_asks - asks_sent;
                let remaining = bids_remaining + asks_remaining;
                let chunk_bids = if remaining <= max {
                    bids_remaining
                } else {
                    let ratio = bids_remaining as f64 / remaining as f64;
                    ((max as f64 * ratio).round() as usize)
                        .max(1)
                        .min(bids_remaining)
                };
                let chunk_asks = (max - chunk_bids).min(asks_remaining);
                chunks.push((bids_sent, chunk_bids, asks_sent, chunk_asks));
                bids_sent += chunk_bids;
                asks_sent += chunk_asks;
            }
            chunks
        }
        _ => vec![(0, total_bids, 0, total_asks)],
    }
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
            trade_dedup: HashMap::new(),
            incremental_seq: HashMap::new(),
            latency_e2e: LatencyKll::new("test-e2e"),
            latency_net: LatencyKll::new("test-net"),
            latency_ipc_e2e: LatencyKll::new("test-ipc-e2e"),
            latency_ipc_net: LatencyKll::new("test-ipc-net"),
            published: 0,
            trades_published: 0,
            incremental_published: 0,
            incremental_dropped_by_seq: 0,
            incremental_gap_warnings: 0,
            derivatives_published: 0,
            dropped_by_seq: 0,
            trades_dropped_by_seq: 0,
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
