//! Build compact, native hourly warm-up inputs from read-only Tardis archives.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Datelike, NaiveDate};
use clap::{Parser, Subcommand};
use mkt_signal::{
    common::amount_threshold::AmountThreshold,
    cta_factor_calc::{
        config::{Combination, FactorRule, Interval, Market, Plan},
        engine::Engine,
        hourly_history::{self, HourlyHistory, HourlyInput, Manifest, SymbolFile, HOUR_MS},
        runtime::atomic_json,
    },
    depth_pub::orderbook::OrderBook,
    factor_pub::{
        tardis_input::{discover_files, require_discovered_coverage, ReplayEvent, ReplayInput},
        trade_flow_feature_pub::local_baseline::{BaselineBar, HourlyNotionalKll},
        trade_notional_kll::{order_size_thresholds, previous_month},
    },
};
use rayon::prelude::*;
use rolling_common::kll_quantile::FrozenKllSketch;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    action: Action,
}
#[derive(Subcommand)]
enum Action {
    /// Read-only validation: seed native history, replay retained Kafka, stop at its captured head.
    Replay {
        #[arg(long)]
        history: PathBuf,
        #[arg(long, required = true)]
        rules: Vec<PathBuf>,
        #[arg(long, default_value = "config/cta_prep_binance_futures.toml")]
        config: PathBuf,
        #[arg(long)]
        control_snapshot: PathBuf,
        #[arg(long)]
        brokers: Option<String>,
    },
    Build {
        #[arg(long, required = true)]
        rules: Vec<PathBuf>,
        #[arg(long)]
        source_root: PathBuf,
        #[arg(long)]
        from: NaiveDate,
        /// Exclusive UTC day boundary.
        #[arg(long)]
        until: NaiveDate,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        control_snapshot: PathBuf,
        #[arg(long, default_value_t = 6)]
        workers: usize,
        #[arg(long)]
        symbol: Vec<String>,
    },
    Check {
        #[arg(long)]
        history: PathBuf,
        #[arg(long, required = true)]
        rules: Vec<PathBuf>,
        /// Timestamp of the first completed Kafka period (milliseconds).
        #[arg(long)]
        first_period_ms: Option<i64>,
    },
}
#[derive(Serialize, Deserialize)]
struct Completed {
    start_ms: i64,
    end_ms: i64,
    depth: bool,
    size: bool,
    control_sha256: String,
    elapsed_secs: f64,
    data: SymbolFile,
}
struct Job {
    symbol: String,
    depth: bool,
    size: bool,
}

fn needs_depth(name: &str) -> bool {
    name.starts_with("depth_") || name.starts_with("factor_") && !name.starts_with("factor_trades_")
}
fn needs_size(name: &str) -> bool {
    matches!(
        name,
        "TD_TI_031"
            | "baseline_035"
            | "baseline_046"
            | "factor_trades_018"
            | "large_sell"
            | "net_buy_medium"
            | "small_order"
            | "small_sell"
            | "medium_buy"
    )
}
fn rule_jobs(paths: &[PathBuf]) -> Result<(Vec<Job>, Plan)> {
    let mut jobs = BTreeMap::<String, Job>::new();
    let mut combinations = Vec::new();
    for (i, path) in paths.iter().enumerate() {
        let rules: Vec<FactorRule> = serde_json::from_slice(&fs::read(path)?)
            .with_context(|| format!("read four-column rules {}", path.display()))?;
        let combination = Combination {
            name: format!("history_{i}"),
            strategy_group: "history".into(),
            symbol_group: None,
            venue: Market::BinanceFutures,
            interval: Interval::Hour,
            reference_venue: None,
            factor_rules: rules,
            signal: None,
            parameters: BTreeMap::new(),
        };
        combination.validate()?;
        for rule in &combination.factor_rules {
            let id = combination.rule_id(rule)?;
            let job = jobs.entry(id.symbol.clone()).or_insert(Job {
                symbol: id.symbol,
                depth: false,
                size: false,
            });
            job.depth |= needs_depth(&id.factor);
            job.size |= needs_size(&id.factor);
        }
        combinations.push(combination);
    }
    Ok((jobs.into_values().collect(), Plan::merge(combinations)?))
}
fn midnight(day: NaiveDate) -> Result<i64> {
    Ok(day
        .and_hms_opt(0, 0, 0)
        .context("invalid UTC day")?
        .and_utc()
        .timestamp_millis())
}
fn month(ms: i64) -> Result<String> {
    Ok(DateTime::from_timestamp_millis(ms)
        .context("invalid trade time")?
        .format("%Y-%m")
        .to_string())
}
fn capture_bar(
    rows: &mut Vec<HourlyInput>,
    bar: BaselineBar,
    start: i64,
    end: i64,
    classified: bool,
    quotes: &mut BTreeMap<i64, Option<f64>>,
    depth: bool,
) {
    let nq_price = if depth {
        quotes.remove(&bar.start_ms).flatten()
    } else {
        (bar.has_trade && bar.close.is_finite() && bar.close > 0.0).then_some(bar.close)
    };
    if bar.start_ms >= start && bar.start_ms < end {
        rows.push(HourlyInput {
            bar,
            nq_price,
            classified,
        });
    }
}
fn build_one(
    job: &Job,
    source_root: &Path,
    from: NaiveDate,
    until: NaiveDate,
    output: &Path,
    current: &BTreeMap<String, [f64; 2]>,
    current_month: &str,
    control_sha: &str,
) -> Result<Completed> {
    let start = midnight(from)?;
    let end = midnight(until)?;
    let meta_path = output.join(format!("{}.json", job.symbol));
    if meta_path.is_file() {
        let previous: Completed = serde_json::from_slice(&fs::read(&meta_path)?)?;
        ensure!(
            previous.start_ms == start
                && previous.end_ms == end
                && previous.depth == job.depth
                && previous.size == job.size
                && previous.control_sha256 == control_sha,
            "existing output parameters differ for {}",
            job.symbol
        );
        ensure!(
            hourly_history::sha256(&output.join(&previous.data.file))? == previous.data.sha256,
            "existing output checksum differs for {}",
            job.symbol
        );
        eprintln!("REUSE {} {} rows", job.symbol, previous.data.rows);
        return Ok(previous);
    }
    let started = Instant::now();
    let last = until.pred_opt().context("invalid end day")?;
    let root = source_root.join(format!("binance_usd_{}", job.symbol.to_ascii_lowercase()));
    let trade_from = if job.size {
        let first = from.with_day(1).context("month start")?;
        first
            .pred_opt()
            .context("previous month")?
            .with_day(1)
            .context("previous month start")?
    } else {
        from
    };
    let trades = discover_files(
        &root,
        "trades",
        "binance-futures",
        &job.symbol,
        Some(&trade_from.to_string()),
        Some(&last.to_string()),
    )?;
    ensure!(
        require_discovered_coverage(&trades, trade_from, last, &job.symbol)?.is_some(),
        "no trades for {}",
        job.symbol
    );
    let books = if job.depth {
        let paths = discover_files(
            &root,
            "incremental_book_L2",
            "binance-futures",
            &job.symbol,
            Some(&from.to_string()),
            Some(&last.to_string()),
        )?;
        ensure!(
            require_discovered_coverage(&paths, from, last, &job.symbol)?.is_some(),
            "no L2 for {}",
            job.symbol
        );
        paths
    } else {
        Vec::new()
    };
    let mut input = ReplayInput::from_files(trades, books, &job.symbol)?;
    let mut agg=mkt_signal::factor_pub::trade_flow_feature_pub::local_baseline::LocalBaselineAggregator::for_completed_periods(HOUR_MS);
    let mut hourly = HourlyNotionalKll::new();
    let mut sketches = BTreeMap::<String, Vec<FrozenKllSketch>>::new();
    let mut thresholds = BTreeMap::<String, [f64; 2]>::new();
    let mut quote_book = OrderBook::new();
    let mut quote_update = 0i64;
    let mut quotes = BTreeMap::new();
    let mut rows = Vec::new();
    let mut nbook = 0u64;
    let mut ntrade = 0u64;
    let mut last_hour = None;
    while let Some(event) = input.next_event()? {
        let ts = event.timestamp_us();
        if ts > end * 1000 {
            break;
        }
        let bucket_us = ts - 1; // Match the live worker's right-closed hour.
        let ready = job.size;
        let mut book_update = None;
        let completed = match event {
            ReplayEvent::Book(book) => {
                ensure!(book.symbol == job.symbol, "book symbol mismatch");
                nbook += 1;
                let completed =
                    agg.on_retained_book(bucket_us, book.is_snapshot, &book.bids, &book.asks);
                book_update = Some(book);
                completed
            }
            ReplayEvent::Trade(trade) => {
                ensure!(trade.symbol == job.symbol, "trade symbol mismatch");
                if job.size {
                    if let Some(snapshot) =
                        hourly.on_trade(trade.timestamp_us, trade.price, trade.amount)
                    {
                        sketches
                            .entry(month(snapshot.hour_start_ms)?)
                            .or_default()
                            .push(snapshot.sketch);
                    }
                }
                if ts <= start * 1000 {
                    continue;
                }
                ntrade += 1;
                if job.size {
                    let apply = month(bucket_us / 1000)?;
                    if !thresholds.contains_key(&apply) {
                        let limits = if apply == current_month {
                            *current
                                .get(&format!("binance-futures:{}", job.symbol))
                                .with_context(|| {
                                    format!("missing current shared thresholds for {}", job.symbol)
                                })?
                        } else {
                            let previous = previous_month(&apply)?;
                            let all = sketches.get(&previous).with_context(|| {
                                format!("missing factual KLL {} {previous}", job.symbol)
                            })?;
                            let merged = FrozenKllSketch::merge_all(all.iter())?
                                .context("empty monthly KLL")?;
                            let (medium, large) = order_size_thresholds(&merged, 0.5, 0.9)?;
                            [medium, large]
                        };
                        ensure!(
                            limits[0] > 0.0 && limits[0] <= limits[1],
                            "invalid monthly thresholds"
                        );
                        thresholds.insert(apply.clone(), limits);
                    }
                    let limits = thresholds[&apply];
                    agg.on_trade_with_threshold(
                        bucket_us,
                        trade.side == 'B',
                        trade.price,
                        trade.amount,
                        AmountThreshold {
                            medium_notional_threshold: limits[0],
                            large_notional_threshold: limits[1],
                        },
                    )
                } else {
                    agg.on_trade(bucket_us, trade.side == 'B', trade.price, trade.amount)
                }
            }
        };
        for bar in completed {
            capture_bar(&mut rows, bar, start, end, ready, &mut quotes, job.depth);
        }
        if let Some(book) = book_update {
            if book.is_snapshot {
                quote_book = OrderBook::new();
            }
            quote_update = quote_update.saturating_add(1);
            let bids = book
                .bids
                .iter()
                .map(|p| (p.price, p.amount))
                .collect::<Vec<_>>();
            let asks = book
                .asks
                .iter()
                .map(|p| (p.price, p.amount))
                .collect::<Vec<_>>();
            quote_book.apply_update(&bids, &asks, quote_update, ts);
            if !quote_book.is_valid() {
                quote_book.prune_crossed_by_best_update_id();
            }
            let mid = quote_book
                .best_bid_price()
                .zip(quote_book.best_ask_price())
                .filter(|(b, a)| b.is_finite() && a.is_finite() && *b > 0.0 && a >= b)
                .map(|(b, a)| (b + a) * 0.5);
            quotes.insert(bucket_us.div_euclid(HOUR_MS * 1000) * HOUR_MS, mid);
        }
        let h = bucket_us.div_euclid(HOUR_MS * 1000);
        if last_hour != Some(h) {
            last_hour = Some(h);
            if rows.len() % 24 == 0 {
                eprintln!(
                    "PROGRESS {} {} hours books={} trades={} elapsed={:.1}s",
                    job.symbol,
                    rows.len(),
                    nbook,
                    ntrade,
                    started.elapsed().as_secs_f64()
                );
            }
        }
    }
    for bar in agg.close_completed_period(end) {
        capture_bar(&mut rows, bar, start, end, job.size, &mut quotes, job.depth);
    }
    let mut data = hourly_history::write_symbol(output, &job.symbol, &rows)?;
    data.source_book_events = nbook;
    data.source_trades = ntrade;
    data.thresholds = thresholds;
    let completed = Completed {
        start_ms: start,
        end_ms: end,
        depth: job.depth,
        size: job.size,
        control_sha256: control_sha.into(),
        elapsed_secs: started.elapsed().as_secs_f64(),
        data,
    };
    atomic_json(&meta_path, &completed)?;
    eprintln!(
        "DONE {} {} hours {:.1}s",
        job.symbol,
        rows.len(),
        completed.elapsed_secs
    );
    Ok(completed)
}
fn main() -> Result<()> {
    let args = Args::parse();
    match args.action {
        Action::Replay {
            history,
            rules,
            config,
            control_snapshot,
            brokers,
        } => {
            use period_pbs::kafka::{decode_period_payload, RawKafkaConsumer};
            let history = HourlyHistory::load(&history)?;
            let (_, plan) = rule_jobs(&rules)?;
            let mut config = mkt_signal::cta_factor_calc::config::Config::load(&config)?;
            let control: serde_json::Value = serde_json::from_slice(&fs::read(control_snapshot)?)?;
            config.amount_thresholds =
                serde_json::from_value(control["amount_thresholds"].clone())?;
            let mut kafka = config.kafka.clone();
            if let Some(brokers) = brokers {
                kafka.brokers = brokers;
                kafka.broker_addr_rewrites.clear();
            }
            kafka.topics = vec!["binance-futures".into()];
            kafka.group_id = format!("cta-hourly-history-check-{}", uuid::Uuid::new_v4());
            kafka.client_id = kafka.group_id.clone();
            kafka.enable_auto_commit = false;
            kafka.offset_reset = "earliest".into();
            let consumer = RawKafkaConsumer::new(&kafka)?;
            let marks = consumer.query_topic_watermarks(&kafka.topics, 10000, 10000)?;
            ensure!(
                marks.iter().all(|m| m.high > m.low),
                "empty retained Kafka partition"
            );
            let offsets = marks
                .iter()
                .map(|m| (m.topic.clone(), m.partition, m.low))
                .collect::<Vec<_>>();
            consumer.assign_offsets(&offsets)?;
            let mut engine = Engine::new(plan, &config)?;
            let mut consumed = BTreeMap::new();
            let mut first = None;
            let mut last = 0;
            let mut seed = None;
            let mut records = 0u64;
            let mut last_points = BTreeMap::new();
            let mut points = 0usize;
            let started = Instant::now();
            let mut progress = Instant::now();
            while marks.iter().any(|m| {
                consumed
                    .get(&(m.topic.clone(), m.partition))
                    .is_none_or(|v| *v < m.high - 1)
            }) {
                ensure!(
                    started.elapsed().as_secs() < 5400,
                    "Kafka replay exceeded 90 minutes"
                );
                let Some(record) = consumer.poll(1000) else {
                    continue;
                };
                let record = record?;
                let (_, _, period) =
                    decode_period_payload(&record.payload, kafka.payload_compression)?;
                if first.is_none() {
                    ensure!(
                        hourly_history::cutoff_ms(period.ts)? <= history.manifest.end_ms,
                        "history ends before retained Kafka; refresh package (first={}, end={})",
                        period.ts,
                        history.manifest.end_ms
                    );
                    first = Some(period.ts);
                    seed = Some(engine.seed_hourly_history(&history, history.manifest.end_ms)?);
                    eprintln!(
                        "KAFKA first_period_ms={} seed_end_ms={}",
                        period.ts, history.manifest.end_ms
                    );
                }
                for point in engine.on_period(Market::BinanceFutures, &period)? {
                    ensure!(
                        point.ts_ms > history.manifest.end_ms,
                        "replayed a seeded hourly observation"
                    );
                    let previous = last_points.insert(point.factor_id.clone(), point.ts_ms);
                    ensure!(
                        previous.is_none_or(|ts| point.ts_ms > ts),
                        "duplicate or regressing hourly factor point"
                    );
                    points += 1;
                }
                consumed.insert((record.topic, record.partition), record.offset);
                records += 1;
                last = last.max(period.ts);
                if progress.elapsed().as_secs() >= 20 {
                    eprintln!("KAFKA records={records} last_period_ms={last} points={points} elapsed={:.1}s",started.elapsed().as_secs_f64());
                    progress = Instant::now();
                }
            }
            let not_ready = engine
                .latest
                .iter()
                .filter(|(_, point)| !point.ready())
                .map(|(id, _)| id)
                .collect::<Vec<_>>();
            let missing = engine
                .plan
                .calculations
                .keys()
                .filter(|id| !engine.latest.contains_key(*id))
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "first_period_ms":first,"last_period_ms":last,"records":records,"hourly_points":points,
                    "elapsed_secs":started.elapsed().as_secs_f64(),"seed":seed,"latest":engine.latest,
                    "not_ready":not_ready,"missing":missing,"consumed":consumed.iter().map(|((t,p),o)|(format!("{t}:{p}"),o)).collect::<BTreeMap<_,_>>()
                }))?
            );
        }
        Action::Build {
            rules,
            source_root,
            from,
            until,
            output,
            control_snapshot,
            workers,
            symbol,
        } => {
            ensure!(
                from < until && workers > 0 && workers <= 32,
                "invalid bounded history job"
            );
            let (mut jobs, _) = rule_jobs(&rules)?;
            if !symbol.is_empty() {
                let selected = symbol.into_iter().collect::<BTreeSet<_>>();
                jobs.retain(|j| selected.contains(&j.symbol));
            }
            ensure!(!jobs.is_empty(), "no selected symbols");
            let control: serde_json::Value = serde_json::from_slice(&fs::read(&control_snapshot)?)?;
            let current: BTreeMap<String, [f64; 2]> =
                serde_json::from_value(control["amount_thresholds"].clone())?;
            let apply = control["threshold_metadata"]["apply_month"]
                .as_str()
                .context("shared threshold month")?;
            let control_sha = hourly_history::sha256(&control_snapshot)?;
            fs::create_dir_all(&output)?;
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()?;
            let results = pool.install(|| {
                jobs.par_iter()
                    .map(|j| {
                        build_one(
                            j,
                            &source_root,
                            from,
                            until,
                            &output,
                            &current,
                            apply,
                            &control_sha,
                        )
                        .map(|done| (j.symbol.clone(), done.data))
                    })
                    .collect::<Vec<_>>()
            });
            let mut symbols = BTreeMap::new();
            for result in results {
                let (symbol, data) = result?;
                symbols.insert(symbol, data);
            }
            let manifest=Manifest{market:Market::BinanceFutures,interval:Interval::Hour,start_ms:midnight(from)?,
                end_ms:midnight(until)?,source:"Tardis trades + incremental_book_L2; current Redis thresholds and historical previous-month hourly KLL".into(),symbols};
            atomic_json(&output.join("manifest.json"), &manifest)?;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        Action::Check {
            history,
            rules,
            first_period_ms,
        } => {
            let history = HourlyHistory::load(&history)?;
            let (_, plan) = rule_jobs(&rules)?;
            let config = mkt_signal::cta_factor_calc::config::Config::load(Path::new(
                "config/cta_prep_binance_futures.toml",
            ))?;
            let mut engine = Engine::new(plan, &config)?;
            let cutoff = first_period_ms
                .map(hourly_history::cutoff_ms)
                .transpose()?
                .unwrap_or(history.manifest.end_ms);
            let summary = engine.seed_hourly_history(&history, cutoff)?;
            for (id, ready) in &summary.ready_formulas {
                if *ready {
                    continue;
                }
                let calculation = &engine.plan.calculations[id];
                if let Some(depth) = mkt_signal::factor_pub::depth_book::DepthFactorId::from_name(
                    &calculation.id.factor,
                ) {
                    let rows = &history.rows[&calculation.id.symbol];
                    let raw = rows
                        .iter()
                        .filter(|r| r.bar.start_ms + HOUR_MS <= cutoff)
                        .map(|r| {
                            r.bar
                                .full_depth
                                .as_ref()
                                .map(|b| b.factor(depth))
                                .filter(|v| v.is_finite())
                        })
                        .collect::<Vec<_>>();
                    let recent = raw.iter().rev().take(48).collect::<Vec<_>>();
                    let unique = recent
                        .iter()
                        .filter_map(|v| v.map(f64::to_bits))
                        .collect::<BTreeSet<_>>();
                    eprintln!(
                        "{}",
                        serde_json::json!({"unready_factor":id,
                            "factual_raw_count":raw.iter().flatten().count(),"last_48_valid_raw":recent.iter().filter(|v|v.is_some()).count(),
                            "last_48_distinct_raw":unique.len(),"latest_raw":raw.last().copied().flatten(),
                            "last_24_bbo_spread_bps":rows.iter().filter(|r|r.bar.start_ms+HOUR_MS<=cutoff).rev().take(24).map(|r|{
                                let bid=r.bar.depth20.bids[0].0;let ask=r.bar.depth20.asks[0].0;
                                (bid>0.0 && ask>=bid).then_some((ask-bid)/((ask+bid)*0.5)*10000.0)
                            }).collect::<Vec<_>>()
                        })
                    );
                }
            }
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    fn gzip(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut out = GzEncoder::new(fs::File::create(path).unwrap(), Compression::fast());
        out.write_all(text.as_bytes()).unwrap();
        out.finish().unwrap();
    }

    #[test]
    fn sparse_tardis_hours_include_right_edge_and_use_causal_quote_or_trade_close() {
        let root = std::env::temp_dir().join(format!("cta-hourly-build-{}", uuid::Uuid::new_v4()));
        let source = root.join("binance_usd_btcusdt");
        let from = NaiveDate::from_ymd_opt(2026, 11, 7).unwrap();
        let until = from.succ_opt().unwrap();
        let start = midnight(from).unwrap();
        let mut trades =
            String::from("exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n");
        let mut books = String::from(
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n",
        );
        for (i, hour) in [0, 2].into_iter().enumerate() {
            let mid = 100.0 + i as f64 * 10.0;
            let book = (start + hour * HOUR_MS + 1000) * 1000;
            for (side, price) in [("bid", mid - 1.0), ("ask", mid + 1.0)] {
                books += &format!("binance-futures,BTCUSDT,{book},{book},true,{side},{price},1\n");
            }
            for (n, side, price) in [(0, "buy", mid), (1, "sell", mid + 0.5)] {
                let ts = (start + (hour + 1) * HOUR_MS) * 1000 - n;
                trades += &format!(
                    "binance-futures,BTCUSDT,{ts},{ts},{}, {side},{price},1\n",
                    i * 2 + n as usize + 1
                )
                .replace(", ", ",");
            }
        }
        // Keep exchange timestamps sorted while testing the exact closing boundary.
        let mut lines = trades.lines().skip(1).collect::<Vec<_>>();
        lines.sort_by_key(|s| s.split(',').nth(2).unwrap().parse::<i64>().unwrap());
        trades = format!("{}\n{}\n", trades.lines().next().unwrap(), lines.join("\n"));
        gzip(
            &source.join("trades/binance-futures_trades_2026-11-07_BTCUSDT.csv.gz"),
            &trades,
        );
        gzip(
            &source.join(
                "incremental_book_L2/binance-futures_incremental_book_L2_2026-11-07_BTCUSDT.csv.gz",
            ),
            &books,
        );
        for depth in [false, true] {
            let output = root.join(if depth { "depth" } else { "trades" });
            fs::create_dir_all(&output).unwrap();
            let job = Job {
                symbol: "BTCUSDT".into(),
                depth,
                size: false,
            };
            let done = build_one(
                &job,
                &root,
                from,
                until,
                &output,
                &BTreeMap::new(),
                "2026-11",
                "fixture",
            )
            .unwrap();
            let manifest = Manifest {
                market: Market::BinanceFutures,
                interval: Interval::Hour,
                start_ms: start,
                end_ms: midnight(until).unwrap(),
                source: "fixture".into(),
                symbols: BTreeMap::from([("BTCUSDT".into(), done.data)]),
            };
            atomic_json(&output.join("manifest.json"), &manifest).unwrap();
            let loaded = HourlyHistory::load(&output).unwrap();
            let rows = &loaded.rows["BTCUSDT"];
            assert_eq!(rows.len(), 2);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.bar.start_ms, start + i as i64 * 2 * HOUR_MS);
                assert_eq!(row.bar.count, 2);
                assert_eq!(row.bar.close, 100.0 + i as f64 * 10.0);
                assert_eq!(row.nq_price, Some(100.0 + i as f64 * 10.0));
            }
        }
        fs::remove_dir_all(root).unwrap();
    }
}
