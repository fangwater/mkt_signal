//! Offline, release-mode microbenchmark. No vendor SDK, network, or real orders.
use anyhow::Result;
use cn_futures::{core::TradingCore, ctp, model::*, persistence, runtime::Journal, yida};
use serde_json::json;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    time::{Duration, Instant},
};

thread_local! { static ALLOCS: Cell<Option<(u64,u64)>> = const { Cell::new(None) }; }
struct CountingAllocator;
fn allocated(size: usize) {
    let _ = ALLOCS.try_with(|c| {
        if let Some((n, b)) = c.get() {
            c.set(Some((n + 1, b + size as u64)));
        }
    });
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        allocated(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        allocated(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        allocated(n);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn snapshot(history: usize, working: usize) -> Snapshot {
    Snapshot {
        trading_day: "20261110".into(),
        session: ctp::session_key(7, 11),
        max_order_ref: (history + working) as u64,
        available_cash: 1_000_000_000.0,
        instruments: vec![Instrument {
            venue: Venue::Cffex,
            symbol: "IF2612".into(),
            price_tick: 0.2,
            contract_multiple: 300,
            expiry_date: 20261218,
            commission: None,
            margin: None,
            min_lots: 1,
            max_lots: 10,
            market_lots: None,
            active: true,
            cash_budget_per_open_lot: 100.0,
            option: None,
        }],
        positions: vec![],
        trades: vec![],
        orders: (0..history + working)
            .map(|i| OrderUpdate {
                key: OrderKey {
                    session: ctp::session_key(7, 11),
                    order_ref: (i + 1).to_string(),
                },
                symbol: "IF2612".into(),
                venue: Venue::Cffex,
                system_id: (i + 1).to_string(),
                side: Side::Sell,
                offset: Offset::Open,
                lots: 1,
                traded: 0,
                price_type: PriceType::Limit,
                time_in_force: TimeInForce::Day,
                limit_price: 8000.0,
                status: if i < history {
                    OrderStatus::Rejected
                } else {
                    OrderStatus::Working
                },
                cancel_rejected: false,
                trading_day: "20261110".into(),
            })
            .collect(),
    }
}
fn intent() -> Intent {
    Intent {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Buy,
        offset: Offset::Open,
        lots: 1,
        price_type: PriceType::Limit,
        price_ticks: 20000,
        time_in_force: TimeInForce::Day,
        execution_route: None,
    }
}
fn limits() -> RiskLimits {
    RiskLimits {
        max_order_lots: 10,
        max_position_lots_per_side: 10000,
        max_working_lots: 10000,
        max_actions_per_second: 100000,
        minimum_cash_remaining: 0.0,
        enable_fak_fok: true,
        close_priority: ClosePriority::TodayFirst,
    }
}
fn report(name: &str, mut times: Vec<u64>, allocs: u64, bytes: u64, extra: serde_json::Value) {
    times.sort_unstable();
    let n = times.len();
    println!(
        "{}",
        json!({"benchmark":name,"operations":n,"p50_ns":times[n/2],"p95_ns":times[(n-1)*95/100],"p99_ns":times[(n-1)*99/100],"p999_ns":times[(n-1)*999/1000],"allocations_per_op":allocs as f64/n as f64,"allocated_bytes_per_op":bytes as f64/n as f64,"context":extra})
    );
}
fn core_bench(samples: usize, ops: usize) -> Result<()> {
    for (history, working) in [(0, 0), (10000, 0), (10000, 8)] {
        let base = snapshot(history, working);
        let mut times = Vec::with_capacity(samples * ops);
        let mut allocs = 0;
        let mut bytes = 0;
        for _ in 0..samples {
            let mut core = TradingCore::new(limits(), ctp::scope())?;
            core.set_trading_status(Venue::Cffex, "IF", true);
            core.restore(base.clone())?;
            let start = Instant::now();
            ALLOCS.with(|c| c.set(Some((0, 0))));
            for i in 0..ops {
                let t = Instant::now();
                let p =
                    core.prepare(black_box(intent()), start + Duration::from_micros(i as u64))?;
                core.reject(black_box(&p.key))?;
                times.push(t.elapsed().as_nanos() as u64);
                black_box(p);
            }
            let (n, b) = ALLOCS.with(|c| c.replace(None).unwrap());
            allocs += n;
            bytes += b;
        }
        report(
            "prepare_and_local_reject",
            times,
            allocs,
            bytes,
            json!({"history_at_start_of_each_sample":history,"working_orders":working,"ops_per_sample":ops}),
        );
    }
    Ok(())
}
fn barrier(j: &Journal) -> Result<()> {
    j.record_with(
        json!({"type":"benchmark_barrier"}),
        false,
        Default::default(),
        true,
    )
}
fn journal_bench(j: &Journal, samples: usize, ops: usize, legacy: bool) -> Result<()> {
    let mut e = yida::RawEvent {
        kind: yida::ORDER,
        day: 20261110,
        reference: 17,
        system_id: 9_007_199_254_740_993,
        order_group: 1,
        lots: 1,
        price: 100000.0,
        commission: 52.660000000000004,
        ..Default::default()
    };
    e.symbol[..6].copy_from_slice(b"cu2612");
    e.exchange[..4].copy_from_slice(b"SHFE");
    let p = PreparedOrder {
        key: OrderKey {
            session: "yida:20261110:group:1".into(),
            order_ref: "101".into(),
        },
        intent: Intent {
            venue: Venue::Shfe,
            symbol: "cu2612".into(),
            ..intent()
        },
        limit_price: 100000.0,
        option_market_buy_ceiling: 0.0,
    };
    for prepared in [false, true] {
        let mut times = Vec::with_capacity(samples * ops);
        let mut allocs = 0;
        let mut bytes = 0;
        for _ in 0..samples {
            barrier(j)?;
            ALLOCS.with(|c| c.set(Some((0, 0))));
            for _ in 0..ops {
                let t = Instant::now();
                if prepared {
                    if legacy {
                        j.record_with(json!({"type":"prepared_submit","key":p.key,"intent":p.intent,"limit_price":p.limit_price}),false,persistence::prepared("20261110",black_box(&p)),false)?;
                    } else {
                        j.prepared_submit("20261110", black_box(&p))?;
                    }
                } else if legacy {
                    j.record(
                        json!({"type":"yida_event","received_us":1,"event":black_box(e)}),
                        false,
                    )?;
                } else {
                    j.yida_event(black_box(&e), 1, false)?;
                }
                times.push(t.elapsed().as_nanos() as u64);
            }
            let (n, b) = ALLOCS.with(|c| c.replace(None).unwrap());
            allocs += n;
            bytes += b;
        }
        barrier(j)?;
        report(
            if prepared {
                "prepared_submit_enqueue"
            } else {
                "raw_yida_audit_enqueue"
            },
            times,
            allocs,
            bytes,
            json!({"fsync_wait_in_timing":false,"audit_mode":if legacy { "business_thread_json" } else { "writer_thread_json" },"writer_thread_affinity":"original process mask"}),
        );
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let option = |flag: &str, default: usize| {
        args.windows(2)
            .find(|a| a[0] == flag)
            .and_then(|a| a[1].parse().ok())
            .unwrap_or(default)
    };
    let samples = option("--samples", 15);
    let ops = option("--ops", 128);
    anyhow::ensure!(samples > 0 && ops > 0 && ops <= 4096, "invalid sample size");
    let path = std::env::temp_dir().join(format!(
        "cn-futures-hot-path-{}-{}",
        std::process::id(),
        persistence::now_us()
    ));
    let j = Journal::start(
        &path.join("audit.jsonl"),
        8192,
        vec![],
        &path.join("db"),
        persistence::store_identity("offline_benchmark", &[b"synthetic"]),
        false,
    )?;
    let cpu = option("--cpu", usize::MAX);
    #[cfg(target_os = "linux")]
    if cpu != usize::MAX {
        anyhow::ensure!(cpu < libc::CPU_SETSIZE as usize, "CPU outside CPU_SETSIZE");
        unsafe {
            let mut mask: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut mask);
            libc::CPU_SET(cpu, &mut mask);
            anyhow::ensure!(
                libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) == 0,
                "set benchmark business-thread affinity: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    println!(
        "{}",
        json!({"environment":{"cpu":cpu,"sdk_loaded":false,"network":false,"includes_allocation_counter_overhead":true,"cpu_isolated":false}})
    );
    core_bench(samples, ops)?;
    journal_bench(&j, samples, ops, args.iter().any(|s| s == "--legacy-audit"))?;
    j.finish()?;
    std::fs::remove_dir_all(path)?;
    Ok(())
}
