//! Full local production adapter path against an explicitly marked synthetic SDK.
//! This executable refuses vendor libraries and never connects to a network front.
use anyhow::{ensure, Context, Result};
use cn_futures::{
    core::{SingleLoop, TradingCore},
    model::*,
    persistence,
    runtime::{lock_flow, Journal},
    yida::{self, config::Config, sdk::YidaTrader, session::Session},
};
use serde_json::json;
use std::{
    ffi::CString,
    hint::black_box,
    path::PathBuf,
    time::{Duration, Instant},
};

fn percentiles(mut samples: Vec<u64>) -> serde_json::Value {
    samples.sort_unstable();
    let n = samples.len();
    if n == 0 {
        return json!({"count":0});
    }
    json!({"count":n,"p50_ns":samples[n/2],"p95_ns":samples[(n-1)*95/100],"p99_ns":samples[(n-1)*99/100],"p999_ns":samples[(n-1)*999/1000]})
}
fn marked_fixture(path: &std::path::Path) -> Result<()> {
    let path = CString::new(path.as_os_str().as_encoded_bytes())?;
    unsafe {
        let handle = libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL);
        ensure!(!handle.is_null(), "cannot open offline benchmark fixture");
        let symbol = libc::dlsym(handle, c"cn_offline_benchmark_fixture".as_ptr());
        let valid = if symbol.is_null() {
            false
        } else {
            let marker: unsafe extern "C" fn() -> u32 = std::mem::transmute(symbol);
            marker() == 0x434e424d
        };
        libc::dlclose(handle);
        ensure!(
            valid,
            "refusing an unmarked SDK: this benchmark accepts only its synthetic offline fixture"
        );
    }
    Ok(())
}
fn flush(j: Option<&Journal>) -> Result<()> {
    if let Some(j) = j {
        j.record_with(
            json!({"type":"benchmark_barrier"}),
            false,
            Default::default(),
            true,
        )?;
    }
    Ok(())
}
struct Timings {
    submit: Vec<u64>,
    ffi: Vec<u64>,
    callback_cpu: Vec<u64>,
    callback_to_oms: Vec<u64>,
    terminal: Vec<u64>,
    burst_flush: Vec<u64>,
    backlog_peak: u64,
    first_trade: Option<yida::RawEvent>,
    first_order: Option<yida::RawEvent>,
}
impl Timings {
    fn new(n: usize) -> Self {
        Self {
            submit: Vec::with_capacity(n * 2),
            ffi: Vec::with_capacity(n * 2),
            callback_cpu: Vec::with_capacity(n * 12),
            callback_to_oms: Vec::with_capacity(n * 12),
            terminal: Vec::with_capacity(n * 2),
            burst_flush: Vec::new(),
            backlog_peak: 0,
            first_trade: None,
            first_order: None,
        }
    }
    fn backlog(&mut self, j: Option<&Journal>) {
        if let Some(j) = j {
            self.backlog_peak = self.backlog_peak.max(j.stats().backlog);
        }
    }
}
fn drain(
    runtime: &mut SingleLoop<YidaTrader>,
    j: Option<&Journal>,
    times: &mut Timings,
) -> Result<()> {
    while let Ok(received) = runtime.trader.events.try_recv() {
        let before = Instant::now();
        let e = received.event;
        ensure!(
            !matches!(e.kind, yida::FATAL | yida::DISCONNECTED | yida::INVALIDATED),
            "offline native callback invalidated the benchmark session"
        );
        if let Some(j) = j {
            j.yida_event(&e, received.received_us, false)?;
        }
        let parsed = yida::decode_event(&e, false)?;
        yida::runtime::apply_live_update(runtime, &e, &parsed)?;
        let complete = Instant::now();
        if matches!(e.kind, yida::ORDER | yida::TRADE) {
            times
                .callback_cpu
                .push(complete.duration_since(before).as_nanos() as u64);
            times
                .callback_to_oms
                .push(complete.duration_since(received.received_at).as_nanos() as u64);
        }
        if times.first_trade.is_none() && e.kind == yida::TRADE {
            times.first_trade = Some(e);
        }
        if times.first_order.is_none() && e.kind == yida::ORDER && e.status == 3 {
            times.first_order = Some(e);
        }
        if let Some(j) = j {
            yida::runtime::journal_update(
                j,
                &runtime.core,
                &e,
                &parsed,
                received.received_us,
                true,
            )?;
        }
        times.backlog(j);
    }
    Ok(())
}
fn submit(
    runtime: &mut SingleLoop<YidaTrader>,
    j: Option<&Journal>,
    times: &mut Timings,
    side: Side,
    offset: Offset,
    tif: TimeInForce,
) -> Result<OrderKey> {
    let intent = Intent {
        venue: Venue::Shfe,
        symbol: "cu2612".into(),
        side,
        offset,
        lots: 1,
        price_type: PriceType::Limit,
        price_ticks: 20000,
        time_in_force: tif,
        execution_route: None,
    };
    let day = runtime.core.trading_day().to_owned();
    let before = Instant::now();
    let receipt = runtime.submit_planned_with(black_box(intent), Instant::now(), |p| {
        if let Some(j) = j {
            j.prepared_submit(&day, p)
        } else {
            Ok(())
        }
    })?;
    times.submit.push(before.elapsed().as_nanos() as u64);
    ensure!(
        receipt.all_submitted_locally && receipt.children.len() == 1,
        "offline submit failed"
    );
    if let Some(ns) = runtime.trader.take_insert_ns() {
        times.ffi.push(ns);
    }
    let key = receipt
        .children
        .into_iter()
        .next()
        .context("empty native receipt")?
        .key;
    if let Some(j) = j {
        j.orders(&runtime.core, std::slice::from_ref(&key), &[])?;
    }
    drain(runtime, j, times)?;
    if tif != TimeInForce::Day {
        ensure!(
            runtime
                .core
                .order(&key)
                .is_some_and(|o| o.filled == 1 && o.status == OrderStatus::Filled),
            "factual fill missing from offline SDK"
        );
        times.terminal.push(before.elapsed().as_nanos() as u64);
    }
    Ok(key)
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let arg = |flag: &str| args.windows(2).find(|a| a[0] == flag).map(|a| a[1].clone());
    let config = Config::load(&PathBuf::from(
        arg("--config").context("--config required")?,
    ))?;
    marked_fixture(&config.sdk_library)?;
    ensure!(
        config.sdk_config.is_absolute(),
        "absolute offline configuration required"
    );
    let cycles = arg("--cycles")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(2000);
    ensure!(cycles > 0, "positive cycle count required");
    let burst = arg("--burst")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(32);
    ensure!(burst > 0, "positive burst size required");
    let passive = args.iter().any(|a| a == "--passive-cancel");
    let without_journal = args.iter().any(|a| a == "--without-journal");
    let _lock = lock_flow(&config.flow_directory)?;
    let journal = if without_journal {
        None
    } else {
        Some(Journal::start(
            &config.journal,
            config.event_capacity,
            vec![
                "fixture-only".into(),
                "fixture-app".into(),
                "fixture-auth".into(),
            ],
            &config.flow_directory.join("persist_manager"),
            persistence::store_identity("offline_pipeline", &[b"synthetic"]),
            false,
        )?)
    };
    let trader = YidaTrader::new(&config, config.credentials()?)?;
    let mut core = TradingCore::new(config.risk.clone(), yida::scope())?;
    core.reserve_orders(cycles.saturating_mul(2).saturating_add(4096))?;
    let mut runtime = SingleLoop {
        core,
        trader,
        execute: true,
    };
    let mut session = Session::default();
    runtime.trader.start()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !session.ready {
        ensure!(Instant::now() < deadline, "offline SDK bootstrap timed out");
        let received = runtime
            .trader
            .events
            .recv_timeout(Duration::from_millis(100))?;
        let e = received.event;
        if let Some(j) = &journal {
            j.yida_event(&e, received.received_us, false)?;
        }
        if e.kind == yida::READY_LOGIN {
            runtime.trader.login()?;
        }
        let parsed = yida::decode_event(&e, session.is_pre_snapshot_rejection(&e));
        if let Some(snapshot) = session.on_event(&e, &config, &parsed)? {
            let trades = snapshot.trades.clone();
            runtime.core.restore(snapshot)?;
            if let Some(j) = &journal {
                j.reconciled(&mut runtime.core, trades)?;
            }
            runtime.trader.recovered();
        }
    }
    if let Some(cpu) = arg("--cpu").map(|s| s.parse::<usize>()).transpose()? {
        ensure!(cpu < libc::CPU_SETSIZE as usize, "invalid benchmark CPU");
        unsafe {
            let mut mask = std::mem::zeroed();
            libc::CPU_ZERO(&mut mask);
            libc::CPU_SET(cpu, &mut mask);
            ensure!(
                libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) == 0,
                "benchmark CPU affinity failed"
            );
        }
    }
    let initial_buy = runtime.core.position(Venue::Shfe, "cu2612", Side::Buy);
    let initial_sell = runtime.core.position(Venue::Shfe, "cu2612", Side::Sell);
    let mut times = Timings::new(cycles);
    let begin = Instant::now();
    let mut burst_begin = Instant::now();
    for n in 0..cycles {
        if passive {
            let key = submit(
                &mut runtime,
                journal.as_ref(),
                &mut times,
                Side::Sell,
                Offset::Open,
                TimeInForce::Day,
            )?;
            let before = Instant::now();
            runtime.cancel_with(&key, Instant::now(), |p| {
                if let Some(j) = &journal {
                    j.prepared_cancel(p)
                } else {
                    Ok(())
                }
            })?;
            drain(&mut runtime, journal.as_ref(), &mut times)?;
            ensure!(
                runtime
                    .core
                    .order(&key)
                    .is_some_and(|o| o.status == OrderStatus::Cancelled && o.filled == 0),
                "passive offline cancellation failed"
            );
            times.terminal.push(before.elapsed().as_nanos() as u64);
        } else {
            submit(
                &mut runtime,
                journal.as_ref(),
                &mut times,
                Side::Sell,
                Offset::Open,
                TimeInForce::Fak,
            )?;
            submit(
                &mut runtime,
                journal.as_ref(),
                &mut times,
                Side::Buy,
                Offset::CloseToday,
                TimeInForce::Fak,
            )?;
        }
        ensure!(
            !runtime
                .trader
                .fault
                .load(std::sync::atomic::Ordering::Acquire),
            "offline callback queue overflow"
        );
        if (n + 1) % burst == 0 {
            times.backlog(journal.as_ref());
            flush(journal.as_ref())?;
            times
                .burst_flush
                .push(burst_begin.elapsed().as_nanos() as u64);
            burst_begin = Instant::now();
        }
    }
    flush(journal.as_ref())?;
    let elapsed = begin.elapsed();
    ensure!(
        runtime.core.position(Venue::Shfe, "cu2612", Side::Buy) == initial_buy
            && runtime.core.position(Venue::Shfe, "cu2612", Side::Sell) == initial_sell,
        "offline position ledger changed"
    );
    ensure!(
        runtime.trader.pending_response_count() == 0,
        "completed native requests leaked deadline associations"
    );
    // Replay an old completed request after the whole run, proving late updates still correlate.
    if let Some(e) = times.first_order {
        let parsed = yida::decode_event(&e, false)?;
        yida::runtime::apply_live_update(&mut runtime, &e, &parsed)?;
    }
    if let Some(e) = times.first_trade {
        let parsed = yida::decode_event(&e, false)?;
        ensure!(
            !runtime
                .core
                .on_fill(parsed.fill().context("missing replayed fill")?)?,
            "late duplicate fill was counted twice"
        );
    }
    let stats = journal.as_ref().map(Journal::stats);
    println!(
        "{}",
        json!({"benchmark":"native_sdk_pipeline","synthetic_sdk":true,"network":false,"profiling_feature":true,"protocol":config.trading_protocol,"journal":!without_journal,"mode":if passive{"passive_cancel"}else{"fak_open_close"},"cycles":cycles,"native_requests":cycles*2,"burst_cycles":burst,"wall_ns":elapsed.as_nanos() as u64,"native_requests_per_second":cycles as f64*2.0/elapsed.as_secs_f64(),"submit_path":percentiles(times.submit),"native_bridge_call":percentiles(times.ffi),"callback_processing":percentiles(times.callback_cpu),"callback_to_oms":percentiles(times.callback_to_oms),"terminal_path":percentiles(times.terminal),"burst_through_durability":percentiles(times.burst_flush),"backlog_peak_observed":times.backlog_peak,"durable_records":stats.map(|s|s.durable),"synced_wal_batches":stats.map(|s|s.synced_batches),"largest_wal_batch":stats.map(|s|s.max_batch),"late_duplicate_verified":true,"pending_requests_after_run":runtime.trader.pending_response_count()})
    );
    let tail = runtime.trader.events.clone();
    drop(runtime.trader);
    for received in tail.try_iter() {
        if let Some(j) = &journal {
            j.yida_event(&received.event, received.received_us, true)?;
        }
    }
    if let Some(j) = journal {
        j.finish()?;
    }
    Ok(())
}
