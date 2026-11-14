//! Build compact, native hourly warm-up inputs from read-only Tardis archives.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Datelike, NaiveDate};
use clap::{Parser, Subcommand};
use mkt_signal::{
    common::amount_threshold::AmountThreshold,
    cta_factor_calc::{
        config::{Config, Interval, Market, Plan},
        engine::{Engine, FactorPoint, MinuteFormulaState},
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
    /// Migrate this worker's factual minute journal into its bounded state checkpoint.
    MinuteState {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        generation: PathBuf,
        #[arg(long)]
        control_snapshot: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Inspect the first retained source period in every required Kafka partition.
    KafkaStart {
        #[arg(long)]
        config: PathBuf,
    },
    /// Extend a native archive with a contiguous archive from the same market.
    Extend {
        #[arg(long)]
        history: PathBuf,
        #[arg(long)]
        append: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Read-only validation: seed native history, replay retained Kafka, stop at its captured head.
    Replay {
        #[arg(long)]
        history: PathBuf,
        #[arg(long, default_value = "config/cta_prep_binance_futures.toml")]
        config: PathBuf,
        #[arg(long)]
        control_snapshot: PathBuf,
        #[arg(long)]
        brokers: Option<String>,
        #[arg(long)]
        minute_state: Option<PathBuf>,
    },
    Build {
        #[arg(long)]
        config: PathBuf,
        /// Source market matching the archived Tardis trades/book files.
        #[arg(long)]
        market: String,
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
        #[arg(long)]
        config: PathBuf,
    },
}
#[derive(Serialize, Deserialize)]
struct Completed {
    market: Market,
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

fn needs_size(name: &str) -> bool {
    mkt_signal::cta_factor_calc::requires_amount_thresholds(name)
}
fn source_plan(path: &Path) -> Result<(Config, Plan)> {
    let config = Config::load(path)?;
    let plan = Plan::load(&config.combinations_dir)?;
    Ok((config, plan))
}
fn market_jobs(plan: &Plan, market: Market) -> Vec<Job> {
    let mut jobs = BTreeMap::<String, Job>::new();
    for calculation in plan
        .calculations
        .values()
        .filter(|c| c.id.interval == Interval::Hour)
    {
        if !calculation.id.markets().any(|source| source == market) {
            continue;
        }
        let job = jobs.entry(calculation.id.symbol.clone()).or_insert(Job {
            symbol: calculation.id.symbol.clone(),
            depth: false,
            size: false,
        });
        job.depth |= hourly_history::requires_book(&calculation.id);
        job.size |= calculation.id.is_formula()
            && calculation.id.market == market
            && needs_size(&calculation.id.factor);
    }
    jobs.into_values().collect()
}
fn source_layout(market: Market) -> (&'static str, &'static str) {
    match market {
        Market::BinanceFutures => ("binance_usd", "binance-futures"),
        Market::OkexFutures => ("okex_usd", "okex-swap"),
        Market::BinanceMargin => ("binance_spot", "binance"),
    }
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
    market: Market,
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
            previous.market == market
                && previous.start_ms == start
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
    let (prefix, exchange) = source_layout(market);
    let root = source_root.join(format!("{prefix}_{}", job.symbol.to_ascii_lowercase()));
    let trade_from = if job.size && month(start)? != current_month {
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
        exchange,
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
            exchange,
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
                ensure!(
                    runtime_common::symbol_util::normalize_symbol_for_internal(&trade.symbol)
                        == job.symbol,
                    "{} trade symbol mismatch for {}",
                    market.name(),
                    job.symbol
                );
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
                                .get(&format!("{}:{}", market.name(), job.symbol))
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
        market,
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
fn extend_archives(history: &Path, append: &Path, output: &Path) -> Result<Manifest> {
    let mut base = HourlyHistory::load(history)?;
    let next = HourlyHistory::load(append)?;
    ensure!(
        base.manifest.market == next.manifest.market
            && base.manifest.end_ms == next.manifest.start_ms
            && base.rows.keys().eq(next.rows.keys()),
        "hourly extension requires the same market, symbols and contiguous bounds"
    );
    for (symbol, rows) in &next.rows {
        ensure!(
            base.manifest.symbols[symbol].last_end_ms == base.manifest.end_ms
                && rows
                    .first()
                    .is_some_and(|r| r.bar.start_ms == next.manifest.start_ms)
                && rows
                    .last()
                    .is_some_and(|r| r.bar.start_ms + HOUR_MS == next.manifest.end_ms)
                && rows
                    .windows(2)
                    .all(|r| r[1].bar.start_ms - r[0].bar.start_ms == HOUR_MS),
            "incomplete hourly extension for {symbol}"
        );
        for (month, values) in &next.manifest.symbols[symbol].thresholds {
            ensure!(
                base.manifest.symbols[symbol]
                    .thresholds
                    .get(month)
                    .is_none_or(|old| old == values),
                "hourly extension changes classified thresholds for {symbol} {month}"
            );
        }
    }
    ensure!(!output.exists(), "hourly extension output already exists");
    fs::create_dir_all(output)?;
    let mut symbols = BTreeMap::new();
    for (symbol, rows) in &next.rows {
        let combined = base.rows.get_mut(symbol).unwrap();
        combined.extend_from_slice(rows);
        let mut meta = hourly_history::write_symbol(output, symbol, combined)?;
        let old = &base.manifest.symbols[symbol];
        let added = &next.manifest.symbols[symbol];
        meta.source_book_events = old.source_book_events + added.source_book_events;
        meta.source_trades = old.source_trades + added.source_trades;
        meta.thresholds = old.thresholds.clone();
        meta.thresholds.extend(added.thresholds.clone());
        symbols.insert(symbol.clone(), meta);
    }
    let manifest = Manifest {
        market: base.manifest.market,
        interval: Interval::Hour,
        start_ms: base.manifest.start_ms,
        end_ms: next.manifest.end_ms,
        source: format!(
            "{}; contiguous extension: {}",
            base.manifest.source, next.manifest.source
        ),
        symbols,
    };
    atomic_json(&output.join("manifest.json"), &manifest)?;
    Ok(manifest)
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.action {
        Action::MinuteState {
            config,
            generation,
            control_snapshot,
            output,
        } => {
            use std::io::BufRead;
            let (mut config, plan) = source_plan(&config)?;
            let old: serde_json::Value =
                serde_json::from_slice(&fs::read(generation.join("plan.json"))?)?;
            let matching = plan
                .calculations
                .iter()
                .filter(|(id, c)| {
                    c.id.is_formula()
                        && c.id.interval != Interval::Hour
                        && serde_json::to_value(&c.parameters).ok().as_ref()
                            == old["calculations"][*id].get("parameters")
                })
                .map(|(id, _)| id.clone())
                .collect::<BTreeSet<_>>();
            let control: serde_json::Value = serde_json::from_slice(&fs::read(control_snapshot)?)?;
            config.amount_thresholds =
                serde_json::from_value(control["amount_thresholds"].clone())?;
            let mut engine = Engine::new(plan, &config)?;
            let mut files = fs::read_dir(&generation)?
                .map(|e| e.map(|e| e.path()))
                .collect::<std::io::Result<Vec<_>>>()?;
            files.retain(|p| {
                p.file_name().and_then(|v| v.to_str()).is_some_and(|v| {
                    v.len() == 14
                        && v.ends_with(".jsonl")
                        && v[..8].bytes().all(|b| b.is_ascii_digit())
                })
            });
            files.sort();
            let mut rows = 0;
            for file in files {
                let mut reader = std::io::BufReader::new(fs::File::open(file)?);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line)? == 0 || !line.ends_with('\n') {
                        break;
                    }
                    let point: FactorPoint = serde_json::from_str(&line)?;
                    if matching.contains(&point.factor_id)
                        && engine.replay_minute_journal_point(point)?
                    {
                        rows += 1;
                    }
                }
            }
            ensure!(rows > 0, "no matching factual minute journal rows");
            let state = engine.minute_formula_state(&config.amount_thresholds);
            let restored = Engine::new(engine.plan.clone(), &config)?
                .restore_minute_formula_state(&state, &config)?;
            atomic_json(&output, &state)?;
            println!(
                "{}",
                serde_json::json!({"journal_rows":rows,"minute_formulas":restored,"output":output})
            );
        }
        Action::KafkaStart { config } => {
            let (config, plan) = source_plan(&config)?;
            let starts = hourly_history::retained_kafka_start(&config.kafka, plan.topics())?;
            println!("{}", serde_json::to_string_pretty(&starts)?);
        }
        Action::Extend {
            history,
            append,
            output,
        } => {
            let manifest = extend_archives(&history, &append, &output)?;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        Action::Replay {
            history,
            config,
            control_snapshot,
            brokers,
            minute_state,
        } => {
            use period_pbs::kafka::{decode_period_payload, RawKafkaConsumer};
            let (mut config, plan) = source_plan(&config)?;
            let histories = hourly_history::load_for_plan(Some(&history), &plan)?;
            let control: serde_json::Value = serde_json::from_slice(&fs::read(control_snapshot)?)?;
            config.amount_thresholds =
                serde_json::from_value(control["amount_thresholds"].clone())?;
            let mut kafka = config.kafka.clone();
            if let Some(brokers) = brokers {
                kafka.brokers = brokers;
                kafka.broker_addr_rewrites.clear();
            }
            kafka.topics = plan.topics();
            kafka.group_id = format!("cta-hourly-history-check-{}", uuid::Uuid::new_v4());
            kafka.client_id = kafka.group_id.clone();
            kafka.enable_auto_commit = false;
            kafka.offset_reset = "earliest".into();
            let consumer = RawKafkaConsumer::new_unassigned(&kafka)?;
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
            let mut first = BTreeMap::new();
            let mut last = 0;
            let seed = engine.seed_hourly_histories(&histories)?;
            let restored = if let Some(path) = minute_state {
                let state: MinuteFormulaState = serde_json::from_slice(&fs::read(path)?)?;
                engine.restore_minute_formula_state(&state, &config)?
            } else {
                0
            };
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
                let market = Market::ALL
                    .into_iter()
                    .find(|m| m.topic() == record.topic)
                    .context("unknown replay topic")?;
                if !first.contains_key(&market) {
                    engine.validate_minute_state_start(market, period.ts)?;
                    if let Some(history) = histories.get(&market) {
                        ensure!(
                            hourly_history::cutoff_ms(period.ts)? <= history.manifest.end_ms,
                            "{} history ends before retained Kafka; refresh package",
                            market.name()
                        );
                    }
                    first.insert(market, period.ts);
                }
                for point in engine.on_period(market, &period)? {
                    if engine.plan.calculations[&point.factor_id].id.interval == Interval::Hour {
                        let source = engine.plan.calculations[&point.factor_id].id.market;
                        ensure!(
                            point.ts_ms > histories[&source].manifest.end_ms,
                            "replayed a seeded hourly observation"
                        );
                    }
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
                    eprintln!(
                        "KAFKA records={records} last_period_ms={last} points={points} elapsed={:.1}s",
                        started.elapsed().as_secs_f64()
                    );
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
                    "not_ready":not_ready,"missing":missing,"minute_formulas_restored":restored,"consumed":consumed.iter().map(|((t,p),o)|(format!("{t}:{p}"),o)).collect::<BTreeMap<_,_>>()
                }))?
            );
        }
        Action::Build {
            config,
            market,
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
            let market: Market = serde_json::from_value(serde_json::json!(market))?;
            let (_, plan) = source_plan(&config)?;
            let mut jobs = market_jobs(&plan, market);
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
                            market,
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
            let manifest=Manifest{market,interval:Interval::Hour,start_ms:midnight(from)?,
                end_ms:midnight(until)?,source:"Tardis trades + incremental_book_L2; current Redis thresholds and historical previous-month hourly KLL".into(),symbols};
            atomic_json(&output.join("manifest.json"), &manifest)?;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        Action::Check { history, config } => {
            let (config, plan) = source_plan(&config)?;
            let histories = hourly_history::load_for_plan(Some(&history), &plan)?;
            let summary = Engine::new(plan, &config)?.seed_hourly_histories(&histories)?;
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

    #[test]
    fn hourly_extension_keeps_factual_rows_and_rejects_gaps_or_overwrite() {
        let root = std::env::temp_dir().join(format!("cta-extend-{}", uuid::Uuid::new_v4()));
        let archive = |name: &str, start: i64| {
            let path = root.join(name);
            fs::create_dir_all(&path).unwrap();
            let rows = (0..2)
                .map(|i| {
                    let ts = start + i * HOUR_MS;
                    let mut aggregator = mkt_signal::factor_pub::trade_flow_feature_pub::local_baseline::LocalBaselineAggregator::for_completed_periods(HOUR_MS);
                    aggregator.on_trade(ts * 1000, true, 100.0, 1.0);
                    HourlyInput {
                        bar: aggregator.close_completed_period(ts + HOUR_MS).remove(0),
                        nq_price: Some(100.0 + i as f64),
                        classified: false,
                    }
                })
                .collect::<Vec<_>>();
            let meta = hourly_history::write_symbol(&path, "ETHUSDT", &rows).unwrap();
            let manifest = Manifest {
                market: Market::BinanceFutures,
                interval: Interval::Hour,
                start_ms: start,
                end_ms: start + 2 * HOUR_MS,
                source: "fixture".into(),
                symbols: BTreeMap::from([("ETHUSDT".into(), meta)]),
            };
            atomic_json(&path.join("manifest.json"), &manifest).unwrap();
            path
        };
        let base = archive("base", HOUR_MS);
        let next = archive("next", 3 * HOUR_MS);
        let output = root.join("combined");
        extend_archives(&base, &next, &output).unwrap();
        let result = HourlyHistory::load(&output).unwrap();
        assert_eq!(result.rows["ETHUSDT"].len(), 4);
        assert_eq!(result.manifest.start_ms, HOUR_MS);
        assert_eq!(result.manifest.end_ms, 5 * HOUR_MS);
        assert_eq!(result.rows["ETHUSDT"][2].nq_price, Some(100.0));
        assert!(extend_archives(&base, &next, &output).is_err());
        let gap = archive("gap", 4 * HOUR_MS);
        assert!(extend_archives(&base, &gap, &root.join("invalid")).is_err());
        assert!(!root.join("invalid").exists());
        fs::remove_dir_all(root).unwrap();
    }

    fn gzip(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut out = GzEncoder::new(fs::File::create(path).unwrap(), Compression::fast());
        out.write_all(text.as_bytes()).unwrap();
        out.finish().unwrap();
    }

    #[test]
    fn history_classification_covers_all_factors_requiring_live_thresholds() {
        for name in [
            "baseline_196",
            "factor_trades_019",
            "factor_trades_020",
            "TD_TI_031",
        ] {
            assert!(
                needs_size(name),
                "missing factual classification for {name}"
            );
        }
        assert!(!needs_size("TD_TI_008"));
        assert!(!needs_size("depth_touch_net"));
    }

    #[test]
    fn jobs_include_both_spread_legs_and_scope_formula_thresholds_to_owner() {
        use mkt_signal::cta_factor_calc::config::Combination;
        let combination: Combination = serde_json::from_value(serde_json::json!({
            "name":"test", "strategy_group":"test", "venue":"okex-futures", "interval":"1h",
            "factor_rules":[
                {"标的":"BTC","因子":"baseline_196","符号":1,"nq":"关闭"},
                {"标的":"BTC","因子":"spread","符号":1,"nq":"关闭","reference_venue":"binance-futures"},
                {"标的":"ETH","因子":"avg_price","符号":1,"nq":"关闭"}
            ]
        })).unwrap();
        let plan = Plan::merge([combination]).unwrap();
        let okx = market_jobs(&plan, Market::OkexFutures);
        let bn = market_jobs(&plan, Market::BinanceFutures);
        assert_eq!(okx.len(), 2);
        assert!(okx[0].depth && okx[0].size);
        assert!(!okx[1].depth && !okx[1].size);
        assert_eq!(bn.len(), 1);
        assert_eq!(bn[0].symbol, "BTCUSDT");
        assert!(bn[0].depth && !bn[0].size);
        assert!(market_jobs(&plan, Market::BinanceMargin).is_empty());
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
        // Tardis filenames use the internal symbol, while OKX CSV rows use
        // the native contract name and quantities stay in native contracts.
        let okx = root.join("okex_usd_btcusdt");
        gzip(
            &okx.join("trades/okex-swap_trades_2026-11-07_BTCUSDT.csv.gz"),
            &trades.replace("binance-futures,BTCUSDT", "okex-swap,BTC-USDT-SWAP"),
        );
        gzip(
            &okx.join(
                "incremental_book_L2/okex-swap_incremental_book_L2_2026-11-07_BTCUSDT.csv.gz",
            ),
            &books.replace("binance-futures,BTCUSDT", "okex-swap,BTC-USDT-SWAP"),
        );
        // Current-month classifications use the factual shared snapshot and
        // must not scan an unused previous-month KLL source.
        for (directory, exchange) in [(&source, "binance-futures"), (&okx, "okex-swap")] {
            let old = directory.join(format!(
                "trades/{exchange}_trades_2026-10-01_BTCUSDT.csv.gz"
            ));
            fs::write(old, b"unused previous month must not be opened").unwrap();
        }
        for (market, depth, size) in [
            (Market::BinanceFutures, false, false),
            (Market::BinanceFutures, true, false),
            (Market::OkexFutures, false, false),
            (Market::OkexFutures, true, false),
            (Market::BinanceFutures, true, true),
            (Market::OkexFutures, true, true),
        ] {
            let output = root.join(format!("{}-{depth}-{size}", market.name()));
            fs::create_dir_all(&output).unwrap();
            let job = Job {
                symbol: "BTCUSDT".into(),
                depth,
                size,
            };
            let done = build_one(
                &job,
                market,
                &root,
                from,
                until,
                &output,
                &BTreeMap::from([(format!("{}:BTCUSDT", market.name()), [100.0, 200.0])]),
                "2026-11",
                "fixture",
            )
            .unwrap();
            let manifest = Manifest {
                market,
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
                assert_eq!(row.bar.volume, 2.0);
                assert_eq!(row.classified, size);
                assert_eq!(row.bar.close, 100.0 + i as f64 * 10.0);
                assert_eq!(row.nq_price, Some(100.0 + i as f64 * 10.0));
            }
        }
        fs::remove_dir_all(root).unwrap();
    }
}
