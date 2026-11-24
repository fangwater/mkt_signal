//! SDK-free native-futures Prep: historical warm-up, live inputs and absolute lot targets.
mod config;
mod engine;
mod extras;
mod hfq;
mod history;
mod live;
mod panel;
mod publication;
mod recovery;
#[cfg(test)]
mod tests;

use anyhow::{ensure, Context, Result};
use axum::{routing::get, Json, Router};
use clap::Parser;
use config::Config;
use engine::Ledger;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    check: bool,
    #[arg(long)]
    preflight: bool,
}
fn atomic(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.new");
    let file = std::fs::File::create(&temporary)?;
    serde_json::to_writer_pretty(&file, value)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
fn ledger(cfg: &Config) -> Result<Ledger> {
    let filename = if cfg.interval == "1h" {
        "positions.json".to_string()
    } else {
        format!("positions_{}.json", cfg.interval)
    };
    let p = cfg.state_directory().join(filename);
    if p.exists() {
        Ok(serde_json::from_slice(&std::fs::read(p)?)?)
    } else {
        Ok(Ledger::default())
    }
}
fn status(
    cfg: &Config,
    main: &history::Dominant,
    engine: &engine::Engine,
    error: Option<&str>,
    live_ready: bool,
) -> Value {
    json!({"name":cfg.name,"phase":if error.is_some() {"paused"} else if live_ready {"live"} else {"waiting_live"},"exchange":cfg.exchange,"symbol":cfg.symbol,"interval":"1h","position_unit":"lot","dominant":main,"factor_ready":engine.ready(),"live_ready":live_ready,"error":error,"last_bar_end_ms":engine.last_end_ms,"rules":engine.latest,"positions":engine.targets(),"factor_signals":engine.factor_signals(),"updated_us":chrono::Utc::now().timestamp_micros()})
}

#[derive(Default)]
struct Health {
    ingress: Option<String>,
    dominant: Option<String>,
}
impl Health {
    fn error(&self) -> Option<&str> {
        self.ingress.as_deref().or(self.dominant.as_deref())
    }
    fn pause(&mut self, error: String) {
        if self.ingress.as_deref() != Some(&error) {
            eprintln!("native input waiting; factual targets retained: {error}");
        }
        self.ingress = Some(error);
    }
}
fn close_hours(
    input: &live::Input,
    hours: &mut engine::Bars,
    rows: &[cn_market_proto::MinuteBar],
) -> Result<Vec<engine::Bar>> {
    let mut closed = Vec::new();
    for row in rows {
        closed.extend(hours.push(row.clone(), input.multiple)?);
        if hours
            .last_minute_ms()
            .is_some_and(|last| input.context.ends_after(last))
        {
            if let Some(hour) = hours.finish(input.multiple)? {
                closed.push(hour);
            }
        }
    }
    Ok(closed)
}
fn history_begin(cfg: &Config) -> i64 {
    (chrono::Utc::now() - chrono::Duration::days(cfg.history_days)).timestamp_millis()
}
async fn preflight(cfg: &Config, main: &history::Dominant, ledger: Ledger) -> Result<Value> {
    let context = history::parameters(cfg).await?;
    let until = chrono::Utc::now().timestamp_millis();
    let archive = hfq::fetch(cfg, main, history_begin(cfg), until).await?;
    let multiple = archive.multiple;
    let mut engine = engine::Engine::new(cfg, ledger.clone())?;
    let mut hours = engine::Bars::default();
    let mut extras = extras::Extras::new(cfg)?;
    let extra_ledgers = extras.ledgers();
    recovery::warm_hfq(
        &archive,
        main,
        &context,
        &mut engine,
        &mut hours,
        &mut extras,
    )?;
    if cfg.enabled && !engine.ready() {
        let unready: Vec<_> = engine
            .latest
            .iter()
            .filter(|r| !r.ready)
            .map(|r| json!({"factor":r.factor,"raw":r.raw,"value":r.value,"quantile":r.quantile}))
            .collect();
        anyhow::bail!(
            "gRPC warm-up incomplete after {} hourly bars: {}",
            engine.hours,
            json!(unready)
        );
    }
    ensure!(
        engine.ledger == ledger,
        "preflight changed factual positions"
    );
    ensure!(
        extras.ledgers() == extra_ledgers,
        "preflight changed minute-frequency ledgers"
    );
    extras.check_enabled_ready()?;
    Ok(
        json!({"exchange":cfg.exchange,"symbol":cfg.symbol,"contract":main.contract,"multiplier":multiple,"source_minute_rows":archive.rows.len(),"hours":engine.hours,"ready_rules":engine.latest.iter().filter(|r|r.ready).count(),"total_rules":cfg.rules.len(),"last_end_ms":engine.last_end_ms,"hfq_basis_trading_day":archive.basis_day,"hfq_generation":archive.generation,"session_snapshot":context.snapshot_as_of,"source":"grpc_hfq","positions_preserved":true,"frequencies":extras.views(false)}),
    )
}
struct ClosedHour {
    bar: engine::Bar,
    live_eligible: bool,
    last_received_us: u64,
}

/// Redis minutes are accumulated independently of delayed historical requests.
#[derive(Default)]
struct Stream {
    pending: recovery::Buffer,
    input: Option<live::Input>,
    minutes: Vec<cn_market_proto::MinuteBar>,
    aligned: bool,
    needs_restart: bool,
    extras: Option<extras::Extras>,
    resume: Option<(live::Context, f64)>,
    last_closed: Option<i64>,
    // The final minute can seal before the hour is eligible to advance.
    closed_hours: VecDeque<ClosedHour>,
    recent: VecDeque<cn_market_proto::MinuteBar>,
    gap_start: Option<i64>,
    gaps: Vec<(i64, i64)>,
    /// Set by each live alignment; cleared once the first fresh live input
    /// lets the newest undecided bar decide (see `Engine::catch_up`).
    catch_up_due: bool,
}
impl Stream {
    fn phase<'a>(
        &self,
        history_loaded: bool,
        market_context: Option<&live::Context>,
        now_ms: i64,
        active_phase: &'a str,
    ) -> &'a str {
        if self.needs_restart {
            return "paused";
        }
        if market_context.is_some_and(|context| !context.allows(now_ms)) {
            return "market_closed";
        }
        if self.aligned {
            return active_phase;
        }
        if !history_loaded {
            return "warming";
        }
        if self.minutes.is_empty() {
            "waiting_live"
        } else {
            "synchronizing"
        }
    }

    fn reset(&mut self) {
        let needs_restart = self.aligned || self.needs_restart;
        let extras = self.extras.take();
        *self = Self {
            needs_restart,
            extras,
            ..Self::default()
        };
    }
    fn begin_recovery(&mut self) {
        let extras = self.extras.take();
        *self = Self {
            extras,
            ..Self::default()
        };
    }
    fn interrupt(&mut self, main: &history::Dominant, from_ms: Option<i64>, reason: &str) {
        if !self.aligned {
            self.reset();
            return;
        }
        if let Some(input) = self.input.take() {
            self.resume = Some((input.context, input.multiple));
        }
        let start = from_ms
            .or_else(|| self.last_closed.map(|last| last + 60_000))
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis().div_euclid(60_000) * 60_000);
        // A queued bar has not advanced the engine. Repair its unconfirmed tail.
        let start = self
            .closed_hours
            .front()
            .map_or(start, |pending| start.min(pending.bar.end_ms - 60_000));
        self.closed_hours.clear();
        if self.gap_start.is_none_or(|old| start < old) {
            self.gap_start = Some(start);
            eprintln!(
                "{}",
                json!({"type":"redis_minute_gap","contract":main.contract,
                "trading_day":main.trading_day,"start_ms":start,"reason":reason})
            );
        }
    }

    fn remember(
        &mut self,
        row: cn_market_proto::MinuteBar,
        market: &live::Context,
        main: &history::Dominant,
    ) {
        if let Some(last) = self.last_closed {
            let start = (last + 60_000..row.open_time_ms)
                .step_by(60_000)
                .find(|&minute| market.allows(minute));
            if let Some(start) = start.filter(|&start| self.gap_start.is_none_or(|old| start < old))
            {
                self.gap_start = Some(start);
                eprintln!(
                    "{}",
                    json!({"type":"redis_minute_gap","contract":main.contract,
                    "trading_day":row.trading_day,"start_ms":start,"reason":"missing completed minute"})
                );
            }
        }
        if let Some(start) = self.gap_start.take() {
            if start < row.open_time_ms {
                self.gaps.push((start, row.open_time_ms - 60_000));
            }
        }
        self.last_closed = Some(row.open_time_ms);
        if self.recent.len() == 4096 {
            self.recent.pop_front();
        }
        self.recent.push_back(row);
    }
    fn activate(&mut self) {
        self.aligned = true;
        self.catch_up_due = true;
        self.minutes = Vec::new();
        self.pending = recovery::Buffer::default();
        if let Some(input) = self.input.as_mut() {
            input.live_after_ms = chrono::Utc::now().timestamp_millis();
        }
    }
    fn next_day(
        &mut self,
        previous: &history::Dominant,
        next: &history::Dominant,
        first_ms: i64,
        hours: &mut engine::Bars,
    ) -> Result<()> {
        ensure!(
            !self.needs_restart,
            "source continuity lost; restart for initialization"
        );
        ensure!(
            previous.contract == next.contract,
            "dominant contract changed; restart for new-contract initialization"
        );
        if self.resume.is_some() && self.input.is_none() {
            return Ok(());
        }
        let input = self
            .input
            .as_mut()
            .context("missing initialized Redis input")?;
        // An ordered quote from the next day also closes the previous day's
        // final source minute. Preserve formula windows across ordinary days.
        let rows = input.closed(first_ms, previous)?;
        self.accept(rows, hours, previous, false)?;
        let input = self
            .input
            .take()
            .context("missing initialized Redis input")?;
        let cutoff = input.live_after_ms;
        self.input = Some(live::Input::new(
            input.context,
            input.multiple,
            first_ms - 60_000,
        ));
        if let Some(input) = self.input.as_mut() {
            input.live_after_ms = cutoff;
        }
        Ok(())
    }
    fn accept(
        &mut self,
        mut rows: Vec<cn_market_proto::MinuteBar>,
        hours: &mut engine::Bars,
        main: &history::Dominant,
        dominant_ready: bool,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        if self.aligned {
            rows.retain(|row| self.last_closed.is_none_or(|last| row.open_time_ms > last));
            let input = self.input.as_ref().context("missing live input")?;
            if self.gap_start.is_some() && !rows.is_empty() {
                *hours = engine::Bars::default();
                self.extras
                    .as_mut()
                    .context("missing minute frequency states")?
                    .discard_partial_bars();
            }
            let fresh = (chrono::Utc::now().timestamp_micros() as u64)
                .saturating_sub(input.last_received_us)
                <= 5_000_000;
            for row in &rows {
                self.extras
                    .as_mut()
                    .context("missing minute frequency states")?
                    .push(
                        row.clone(),
                        input.multiple,
                        main,
                        &input.context,
                        dominant_ready.then_some(input.live_after_ms),
                        fresh,
                    )?;
            }
            for bar in close_hours(input, hours, &rows)? {
                // An hour formed after an unrepaired gap is misaligned (it starts
                // at the first minute after the hole): it must not decide live.
                let continuous = self.gaps.is_empty() && self.gap_start.is_none();
                self.closed_hours.push_back(ClosedHour {
                    live_eligible: dominant_ready && continuous && bar.end_ms > input.live_after_ms,
                    last_received_us: input.last_received_us,
                    bar,
                });
            }
            let market = input.context.clone();
            for row in rows {
                self.remember(row, &market, main);
            }
        } else {
            ensure!(
                self.minutes.len() + rows.len() <= 4096,
                "Redis minute cache exhausted"
            );
            self.minutes.extend(rows);
        }
        Ok(())
    }
    fn advance_hours(
        &mut self,
        engine: &mut engine::Engine,
        main: &history::Dominant,
        dominant_ready: bool,
        now_us: i64,
    ) -> Result<()> {
        let Some(input) = self.input.as_ref() else {
            return Ok(());
        };
        let now_ms = now_us.div_euclid(1_000);
        while let Some(closed) = self.closed_hours.front().filter(|h| h.bar.end_ms <= now_ms) {
            let fresh = (now_us as u64).saturating_sub(closed.last_received_us) <= 5_000_000;
            let live = closed.live_eligible
                && dominant_ready
                && fresh
                && now_ms - closed.bar.end_ms <= 120_000;
            engine.observe(&closed.bar, &main.contract, input.multiple, live)?;
            self.closed_hours.pop_front();
        }
        Ok(())
    }
    fn observe(
        &mut self,
        event: live::Event,
        hours: &mut engine::Bars,
        main: &history::Dominant,
        dominant_ready: bool,
    ) -> Result<()> {
        ensure!(
            !self.needs_restart,
            "source continuity lost; restart for initialization"
        );
        if matches!(event, live::Event::Reset) {
            self.interrupt(main, None, "Redis continuity reset");
            return Ok(());
        }
        if let Some(first) = self
            .input
            .as_ref()
            .and_then(|input| input.missing_before(&event, main))
        {
            self.interrupt(
                main,
                Some(first),
                "missing trading minute before an ordered Redis quote",
            );
        }
        if let (Some((context, multiple)), live::Event::Quote(quote)) =
            (self.resume.as_ref(), &event)
        {
            if quote.symbol() == main.contract.as_bytes()
                && quote.trading_day == main.trading_day
                && quote.exchange_ts_us > 0
            {
                self.input = Some(live::Input::new(
                    context.clone(),
                    *multiple,
                    quote.exchange_ts_us / 1_000,
                ));
                self.resume = None;
            } else {
                return Ok(());
            }
        }
        if let Some(input) = self.input.as_mut() {
            let rows = input.observe(event, main)?;
            self.accept(rows, hours, main, dominant_ready)
        } else {
            self.pending.push(&event, main)
        }
    }
    fn prepare(
        &mut self,
        context: live::Context,
        multiple: f64,
        main: &history::Dominant,
    ) -> Result<()> {
        ensure!(
            !self.needs_restart && !self.aligned,
            "gRPC preparation is startup-only"
        );
        let (input, rows) = live::Input::from_quotes(context, multiple, self.pending.take(), main)?;
        self.input = Some(input);
        self.minutes = rows;
        Ok(())
    }
    fn ready(&self) -> bool {
        self.aligned
            && self.gap_start.is_none()
            && self.input.as_ref().is_some_and(|i| {
                (chrono::Utc::now().timestamp_micros() as u64).saturating_sub(i.last_received_us)
                    <= 5_000_000
            })
    }

    fn repair_point(&self) -> RepairPoint {
        RepairPoint {
            last_closed: self.last_closed,
            gap_start: self.gap_start,
            gaps: self.gaps.clone(),
        }
    }

    fn apply_repair(
        &mut self,
        repair: PreparedRepair,
        main: &history::Dominant,
        engine: &mut engine::Engine,
        hours: &mut engine::Bars,
        cutoff: i64,
        dominant_ready: bool,
    ) -> Result<()> {
        let replacement = repair.rebuilt;
        ensure!(
            replacement.engine.ledger == engine.ledger
                && replacement.extras.ledgers()
                    == self
                        .extras
                        .as_ref()
                        .context("missing frequency states")?
                        .ledgers(),
            "historical repair would replace the current target ledger"
        );
        // Intraday, live minutes keep closing while the rebuild runs: replay
        // every one after the rebuilt history so the hour stays aligned.
        let newer: Vec<_> = self
            .recent
            .iter()
            .filter(|row| {
                repair
                    .last_closed
                    .is_none_or(|last| row.open_time_ms > last)
            })
            .cloned()
            .collect();
        ensure!(
            newer.is_empty() || !self.aligned || self.input.is_some(),
            "live input is re-initialising; retry the repair"
        );
        *engine = replacement.engine;
        *hours = replacement.hours;
        self.closed_hours.clear();
        self.extras = Some(replacement.extras);
        self.last_closed = repair.last_closed.or(self.last_closed);
        self.gaps.clear();
        if self.gap_start.is_some_and(|start| start <= cutoff) {
            self.gap_start = None;
        }
        self.recent.clear();
        let replayed = newer.len();
        if self.aligned && !newer.is_empty() {
            self.accept(newer, hours, main, dominant_ready)?;
        }
        // A bar closed inside the repaired span never decided: decide the
        // newest one once the live feed is fresh (see `Engine::catch_up`).
        self.catch_up_due = true;
        println!(
            "{}",
            json!({"type":"redis_gap_repaired","contract":main.contract,
            "trading_day":main.trading_day,"through_ms":cutoff,"replayed_live_minutes":replayed,
            "positions_preserved":true})
        );
        Ok(())
    }
    /// Repairs stay valid while no new gap opened since the job started;
    /// newly closed live minutes are replayed on adoption.
    fn same_gaps(&self, point: &RepairPoint) -> bool {
        self.gap_start == point.gap_start
            && self.gaps == point.gaps
            && self.last_closed >= point.last_closed
    }
}

#[derive(PartialEq, Eq)]
struct RepairPoint {
    last_closed: Option<i64>,
    gap_start: Option<i64>,
    gaps: Vec<(i64, i64)>,
}

struct PreparedRepair {
    rebuilt: recovery::Recovered,
    last_closed: Option<i64>,
}

struct RepairJob {
    cutoff: i64,
    contract: String,
    config: Vec<u8>,
    point: RepairPoint,
    task: tokio::task::JoinHandle<Result<PreparedRepair>>,
}

impl RepairPoint {
    fn rebuild(
        &self,
        mut history: history::History,
        recent: &VecDeque<cn_market_proto::MinuteBar>,
        cfg: &Config,
        main: &history::Dominant,
        market: &live::Context,
        ledger: Ledger,
        cutoff: i64,
    ) -> Result<PreparedRepair> {
        let archive_cutoff = history
            .hfq
            .as_ref()
            .context("missing HFQ repair archive")?
            .cutoff;
        let gaps = self
            .gaps
            .iter()
            .copied()
            .chain(self.gap_start.map(|start| (start, cutoff)));
        for (first, last) in gaps {
            for minute in (first.max(archive_cutoff + 60_000)..=last.min(cutoff))
                .step_by(60_000)
                .filter(|&minute| market.allows(minute))
            {
                ensure!(
                    history
                        .minutes
                        .get(&minute)
                        .is_some_and(|row| recovery::closed(&history, row)),
                    "Market Replay has not completed Redis gap minute {minute}"
                );
            }
        }
        // Reuse complete local minutes outside known gaps. The service is
        // authoritative for missing or explicitly invalidated minutes only.
        for row in recent.iter().filter(|row| row.open_time_ms <= cutoff) {
            let ms = row.open_time_ms;
            if !self
                .gaps
                .iter()
                .any(|&(first, last)| (first..=last).contains(&ms))
                && self.gap_start.is_none_or(|start| ms < start)
            {
                history.minutes.insert(ms, row.clone());
            }
        }
        let last_closed = recovery::last_closed(&history, i64::MAX);
        let rebuilt = recovery::recover(cfg, main, &history, ledger, market, &[])?;
        Ok(PreparedRepair {
            rebuilt,
            last_closed,
        })
    }
}

static REPAIR_CONCURRENCY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

type Report = Arc<RwLock<Value>>;

fn read_report(state: &Report) -> Value {
    state.read().unwrap_or_else(|p| p.into_inner()).clone()
}

fn targets_view(v: Value) -> Value {
    json!({"live_ready":v["live_ready"],"factor_ready":v["factor_ready"],
        "positions":v["positions"],"frequencies":v["frequencies"],"strategy_group":v["strategy_group"],
        "strategies":v["strategies"],"manager_publication":v["manager_publication"],"error":v["error"],
        "products":v["products"]})
}

fn group_report(primary: &Report, products: &BTreeMap<String, Report>) -> Value {
    let mut report = read_report(primary);
    if !products.is_empty() {
        let reports: BTreeMap<_, _> = products
            .iter()
            .map(|(symbol, state)| (symbol, read_report(state)))
            .collect();
        report["products"] = json!(reports);
    }
    report
}

fn configs(path: &Path) -> Result<Vec<(PathBuf, Config)>> {
    let primary = Config::load(path)?;
    let mut configs = vec![(path.to_path_buf(), primary.clone())];
    let mut products = BTreeSet::from([primary.symbol.clone()]);
    for entry in &primary.additional_products {
        let file = path
            .parent()
            .context("missing CTA config parent")?
            .join(entry);
        let mut product = Config::load(&file)
            .with_context(|| format!("load CTA product configuration {}", file.display()))?;
        ensure!(
            product.name == primary.name
                && product.additional_products.is_empty()
                && product.publication.is_none(),
            "additional product must belong to the same CTA group"
        );
        ensure!(
            products.insert(product.symbol.clone()),
            "duplicate CTA product {}",
            product.symbol
        );
        ensure!(
            product.redis_endpoint == primary.redis_endpoint,
            "additional products must use the group's Redis source"
        );
        product.attach_to(&primary);
        product.validate()?;
        configs.push((file, product));
    }
    if let Some(publication) = &primary.publication {
        ensure!(
            publication.products.is_subset(&products),
            "Manager publication references an unconfigured product"
        );
    }
    Ok(configs)
}

pub async fn run() -> Result<()> {
    let args = Args::parse();
    let configs = configs(&args.config)?;
    if args.check {
        for (_, cfg) in &configs {
            println!(
                "{} {}/{}: {} hourly rules",
                cfg.name,
                cfg.exchange,
                cfg.symbol,
                cfg.rules.len()
            );
        }
        return Ok(());
    }
    let root = configs[0].1.clone();
    std::fs::create_dir_all(&root.data_directory)?;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.data_directory.join(".lock"))?;
    lock.try_lock()
        .context("CN CTA environment already running")?;
    if args.preflight {
        let mut receipts = BTreeMap::new();
        for (_, cfg) in &configs {
            let receipt = async {
                let main = history::dominant(cfg).await?;
                preflight(cfg, &main, ledger(cfg)?).await
            }
            .await
            .with_context(|| format!("preflight {}/{}", cfg.exchange, cfg.symbol))?;
            receipts.insert(cfg.symbol.clone(), receipt);
        }
        let mut report = receipts
            .remove(&root.symbol)
            .context("missing primary preflight")?;
        if !receipts.is_empty() {
            report["products"] = json!(receipts);
        }
        println!("{report}");
        return Ok(());
    }
    let mut tasks = tokio::task::JoinSet::new();
    let mut reports = BTreeMap::new();
    let mut app = Router::new();
    let (inputs, venue_clocks) =
        live::capture(root.redis_endpoint, configs.iter().map(|(_, cfg)| cfg))?;
    let mut workers = Vec::new();
    for ((path, cfg), rx) in configs.into_iter().zip(inputs) {
        std::fs::create_dir_all(cfg.state_directory())?;
        let engine = engine::Engine::new(&cfg, ledger(&cfg)?)?;
        let extras = extras::Extras::new(&cfg)?;
        let positions = publication::positions(&engine, &extras)?;
        let publisher = publication::Publisher::start(&cfg, positions.clone())
            .map_err(|err| format!("{err:#}"));
        if let Err(err) = &publisher {
            eprintln!(
                "Manager publication unavailable for {}/{}: {err}",
                cfg.exchange, cfg.symbol
            );
        }
        let state = Arc::new(RwLock::new(json!({"name":cfg.name,"phase":"warming",
            "symbol":cfg.symbol,"exchange":cfg.exchange,"strategy_group":cfg.strategy_group()?,
            "strategies":positions,"live_ready":false,"factor_ready":false,"positions":engine.targets(),"factor_signals":engine.factor_signals(),
            "manager_publication": publication_status(&cfg, &publisher)})));
        reports.insert(cfg.symbol.clone(), state.clone());
        let catalog = Arc::new(RwLock::new(panel::catalog(&cfg)?));
        let (config_tx, config_rx) = tokio::sync::mpsc::channel(8);
        let router = panel::router(&cfg, catalog.clone(), config_tx)?;
        if cfg.is_additional_product() {
            let read = state.clone();
            let positions = state.clone();
            app = app.nest(
                &format!("/products/{}", cfg.symbol.to_ascii_lowercase()),
                router
                    .route(
                        "/state",
                        get(move || async move { Json(read_report(&read)) }),
                    )
                    .route(
                        "/targets",
                        get(move || async move { Json(targets_view(read_report(&positions))) }),
                    ),
            );
        } else {
            app = app.merge(router);
        }
        let worker = Worker {
            cfg,
            path,
            rx,
            engine,
            extras,
            state,
            catalog,
            config_rx,
            publisher,
            venue_clocks: venue_clocks.clone(),
        };
        workers.push(worker);
    }
    for worker in workers {
        tasks.spawn(run_worker(worker));
    }
    let primary = reports
        .remove(&root.symbol)
        .context("missing primary CTA state")?;
    let read = primary.clone();
    let read_products = reports.clone();
    let app = app
        .route(
            "/state",
            get(move || async move { Json(group_report(&read, &read_products)) }),
        )
        .route(
            "/targets",
            get(move || async move { Json(targets_view(group_report(&primary, &reports))) }),
        );
    let listener = tokio::net::TcpListener::bind(&root.bind).await?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        result = axum::serve(listener, app) => result?,
        result = tasks.join_next() => result.context("no CTA product workers")???,
    }
    Ok(())
}

struct Worker {
    cfg: Config,
    path: PathBuf,
    rx: crossbeam_channel::Receiver<live::Event>,
    engine: engine::Engine,
    extras: extras::Extras,
    state: Report,
    catalog: Report,
    config_rx: tokio::sync::mpsc::Receiver<panel::Mutation>,
    publisher: std::result::Result<Option<publication::Publisher>, String>,
    venue_clocks: Arc<live::VenueClocks>,
}

fn publication_status(
    cfg: &Config,
    publisher: &std::result::Result<Option<publication::Publisher>, String>,
) -> Value {
    match publisher {
        Ok(Some(publisher)) => publisher.status(),
        Ok(None) => Value::Null,
        Err(error) => json!({
            "strategy_group": cfg.strategy_group().ok(),
            "source_id": cfg.publication_settings().map(|p| p.source_id.as_str()),
            "error": error,
        }),
    }
}

async fn run_worker(worker: Worker) -> Result<()> {
    let Worker {
        mut cfg,
        path,
        rx,
        mut engine,
        extras,
        state,
        catalog,
        mut config_rx,
        mut publisher,
        venue_clocks,
    } = worker;
    let mut current = history::dominant(&cfg)
        .await
        .with_context(|| format!("load dominant for {}/{}", cfg.exchange, cfg.symbol))?;
    // Unshifted REST row (with its secondary) for reuse on periodic refreshes.
    let mut source_dominant = current.clone();
    let mut stream = Stream {
        extras: Some(extras),
        ..Stream::default()
    };
    let mut history = Some(history::History::default());
    let mut market_context: Option<live::Context> = None;
    let mut hours = engine::Bars::default();
    let mut load_start = history_begin(&cfg);
    let mut loaded = false;
    let mut health = Health::default();
    let mut next_fetch = tokio::time::Instant::now();
    let mut next_repair = next_fetch;
    let mut repair_job: Option<RepairJob> = None;
    let mut repair_error: Option<String> = None;
    let mut next_dominant = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        // A saved configuration (lots included) is a decision of its own:
        // changed targets are pushed at once instead of at the next bar.
        let mut saved_decision = None;
        while let Ok(command) = config_rx.try_recv() {
            let saved = panel::process(
                command,
                &path,
                &mut cfg,
                &mut engine,
                stream
                    .extras
                    .as_mut()
                    .context("missing minute frequency states")?,
                &catalog,
            );
            if saved {
                saved_decision = Some((chrono::Utc::now().timestamp_millis(), current.trading_day));
            }
        }
        let decision_before = engine.ledger.last_decision_ms;
        let extra_before = stream
            .extras
            .as_ref()
            .and_then(extras::Extras::last_live_decision);
        // Read the venue clock before draining: every own snapshot stamped
        // before it is then already in this queue (see `live::VenueClocks`).
        let venue_ms = venue_clocks.get(&cfg.exchange);
        // Drain all events accumulated during the preceding network request,
        // including reset markers, before closing bars or attempting a handoff.
        loop {
            let event = match rx.try_recv() {
                Ok(event) => event,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    anyhow::bail!("Redis capture thread stopped")
                }
            };
            if let live::Event::Quote(q) = &event {
                if q.trading_day > current.trading_day {
                    match current.for_day(q.trading_day) {
                        Ok(next) => {
                            if history.is_some() {
                                stream.reset();
                                history = Some(history::History::default());
                                load_start = history_begin(&cfg);
                                loaded = false;
                                next_fetch = tokio::time::Instant::now();
                            } else if let Err(e) = stream.next_day(
                                &current,
                                &next,
                                q.exchange_ts_us / 1000,
                                &mut hours,
                            ) {
                                stream.reset();
                                health.pause(format!("native day transition: {e:#}"));
                            }
                            current = next;
                        }
                        Err(e) => {
                            stream.reset();
                            health.pause(format!("dominant trading day: {e:#}"));
                            continue;
                        }
                    }
                }
            }
            let fault_minute = match &event {
                live::Event::Quote(quote)
                    if quote.symbol() == current.contract.as_bytes()
                        && quote.exchange_ts_us > 0 =>
                {
                    Some(quote.exchange_ts_us.div_euclid(60_000_000) * 60_000)
                }
                _ => None,
            };
            if let Err(e) = stream.observe(event, &mut hours, &current, health.dominant.is_none()) {
                let error = format!("Redis input: {e:#}");
                if stream.aligned && !stream.needs_restart {
                    let suspected = fault_minute.map(|minute| {
                        stream
                            .last_closed
                            .map_or(minute, |last| (last + 60_000).min(minute))
                    });
                    stream.interrupt(&current, suspected, &error);
                } else {
                    stream.reset();
                    health.pause(error);
                }
            }
            if stream.needs_restart {
                stream.begin_recovery();
                history = Some(history::History::default());
                market_context = None;
                hours = engine::Bars::default();
                load_start = history_begin(&cfg);
                loaded = false;
                next_fetch = tokio::time::Instant::now();
            }
        }
        if let Some(input) = stream.input.as_mut() {
            let rows = input.clock(chrono::Utc::now().timestamp_millis(), venue_ms, &current);
            let dominant_ready = health.dominant.is_none();
            if let Err(e) = rows.and_then(|rows| {
                stream.accept(rows, &mut hours, &current, dominant_ready)?;
                stream.advance_hours(
                    &mut engine,
                    &current,
                    dominant_ready,
                    chrono::Utc::now().timestamp_micros(),
                )
            }) {
                let error = format!("Redis minute: {e:#}");
                if stream.aligned {
                    stream.interrupt(&current, None, &error);
                } else {
                    stream.reset();
                    health.pause(error);
                }
            }
        }
        if stream.aligned {
            if let Some(missing) = stream
                .input
                .as_ref()
                .and_then(|input| input.overdue_minute(chrono::Utc::now().timestamp_millis()))
            {
                stream.interrupt(
                    &current,
                    Some(missing),
                    "trading minute not received by the close grace period",
                );
            }
        }
        if stream.needs_restart {
            stream.begin_recovery();
            history = Some(history::History::default());
            market_context = None;
            hours = engine::Bars::default();
            load_start = history_begin(&cfg);
            loaded = false;
            next_fetch = tokio::time::Instant::now();
        }
        // The newest warm-up bar decides once the live feed is fresh, with the
        // active trading day, exactly as an always-running process would have.
        if stream.catch_up_due && stream.ready() && health.error().is_none() {
            stream.catch_up_due = false;
            let hourly = engine.catch_up(current.trading_day)?;
            let minutes = stream
                .extras
                .as_mut()
                .context("missing minute frequency states")?
                .catch_up(current.trading_day)?;
            if hourly.is_some() || !minutes.is_empty() {
                // Publish now: the decision is fresh even though its bar is not.
                saved_decision = Some((chrono::Utc::now().timestamp_millis(), current.trading_day));
                println!(
                    "{}",
                    json!({"type":"catch_up_decision","contract":current.contract,
                    "trading_day":current.trading_day,"hour_bar_end_ms":hourly,
                    "frequencies":minutes,"targets":engine.targets()})
                );
            }
        }
        // Persist every new factual decision before any await or factor rebuild.
        if engine.ledger.last_decision_ms != decision_before {
            atomic(
                &cfg.state_directory().join("positions.json"),
                &engine.ledger,
            )?;
            atomic(
                &cfg.state_directory().join("targets.json"),
                &engine.targets(),
            )?;
            println!("{}", engine.targets());
        }
        stream
            .extras
            .as_mut()
            .context("missing minute frequency states")?
            .persist()?;
        if loaded
            && stream.minutes.last().is_none_or(|row| {
                row.open_time_ms + 60_000 <= chrono::Utc::now().timestamp_millis()
            })
        {
            if let (Some(history_input), Some(market)) = (history.as_ref(), market_context.as_ref())
            {
                match recovery::recover(
                    &cfg,
                    &current,
                    history_input,
                    engine.ledger.clone(),
                    market,
                    &stream.minutes,
                ) {
                    Ok(recovered) => {
                        println!(
                            "{}",
                            json!({"type":"grpc_redis_aligned","contract":current.contract,
                            "anchor_ms":recovered.anchor_ms,"history_rows":recovered.history_rows,
                            "cached_redis_minutes":stream.minutes.len(),"hours":recovered.engine.hours,
                            "positions_preserved":true})
                        );
                        engine = recovered.engine;
                        hours = recovered.hours;
                        stream.extras = Some(recovered.extras);
                        stream.last_closed =
                            recovery::last_closed(history_input, recovered.anchor_ms);
                        for row in std::mem::take(&mut stream.minutes) {
                            stream.remember(row, market, &current);
                        }
                        if stream.input.is_none() {
                            stream.resume = Some((market.clone(), history_input.multiple()?));
                        }
                        stream.activate();
                        // The live path can use the warm state immediately.
                        // Missing minutes are handled by the off-session job.
                        history = None;
                        health.ingress = None;
                    }
                    Err(e) => health.pause(format!("gRPC catch-up: {e:#}")),
                }
            }
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        let off_session = market_context
            .as_ref()
            .is_some_and(|market| !market.allows(now_ms));
        // Market Replay (MDL plus the ZMQ minute cache) usually completes a
        // live gap within a minute, so repair inside sessions as well.
        let _ = off_session;
        if !stream.aligned {
            if let Some(job) = repair_job.take() {
                job.task.abort();
            }
        } else if let Some(market) = market_context.as_ref() {
            if repair_job
                .as_ref()
                .is_some_and(|job| job.task.is_finished())
            {
                let RepairJob {
                    cutoff,
                    contract,
                    config,
                    point,
                    task,
                } = repair_job.take().expect("ready repair job");
                if contract == current.contract
                    && stream.same_gaps(&point)
                    && config == serde_json::to_vec(&cfg)?
                {
                    let dominant_ready = health.dominant.is_none();
                    let result = match task.await {
                        Ok(Ok(repair)) => stream.apply_repair(
                            repair,
                            &current,
                            &mut engine,
                            &mut hours,
                            cutoff,
                            dominant_ready,
                        ),
                        Ok(Err(error)) => Err(error),
                        Err(error) => Err(error.into()),
                    };
                    match result {
                        Ok(()) => repair_error = None,
                        Err(error) => {
                            let error = format!("{error:#}");
                            if repair_error.as_deref() != Some(error.as_str()) {
                                eprintln!(
                                    "{} Market Replay repair pending: {error}",
                                    current.contract
                                );
                            }
                            repair_error = Some(error);
                        }
                    }
                }
                next_repair = tokio::time::Instant::now() + Duration::from_secs(30);
            } else if repair_job.is_none()
                && (!stream.gaps.is_empty() || stream.gap_start.is_some())
                && tokio::time::Instant::now() >= next_repair
            {
                let cutoff = (now_ms.div_euclid(60_000) - 1) * 60_000;
                let repair_cfg = cfg.clone();
                let repair_main = current.clone();
                let repair_market = market.clone();
                let ledger = engine.ledger.clone();
                let recent = stream.recent.clone();
                let point = stream.repair_point();
                let reference = stream.repair_point();
                let task = tokio::spawn(async move {
                    let permit = REPAIR_CONCURRENCY.acquire().await?;
                    let history = recovery::load_repair(&repair_cfg, &repair_main, cutoff).await?;
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        point.rebuild(
                            history,
                            &recent,
                            &repair_cfg,
                            &repair_main,
                            &repair_market,
                            ledger,
                            cutoff,
                        )
                    })
                    .await?
                });
                repair_job = Some(RepairJob {
                    cutoff,
                    contract: current.contract.clone(),
                    config: serde_json::to_vec(&cfg)?,
                    point: reference,
                    task,
                });
            }
        }
        let ready = stream.ready() && health.error().is_none();
        let extras = stream.extras.as_ref().context("missing frequency states")?;
        let positions = publication::positions(&engine, extras)?;
        let hour_decision = engine
            .last_decision_day
            .filter(|_| engine.ledger.last_decision_ms > decision_before)
            .map(|day| (engine.ledger.last_decision_ms, day));
        let extra_decision = extras
            .last_live_decision()
            .filter(|decision| Some(*decision) != extra_before);
        if let Ok(Some(publisher)) = &mut publisher {
            publisher.observe(
                positions.clone(),
                hour_decision.max(extra_decision).max(saved_decision),
                current.trading_day,
                ready,
            )?;
        }
        let mut report = status(&cfg, &current, &engine, health.error(), ready);
        report["strategy_group"] = json!(cfg.strategy_group()?);
        report["strategies"] = json!(positions);
        report["manager_publication"] = publication_status(&cfg, &publisher);
        let mut frequencies = stream
            .extras
            .as_ref()
            .context("missing minute frequency states")?
            .views(ready);
        frequencies["1h"] = json!({"enabled":cfg.enabled,"factor_ready":engine.ready(),"live_ready":ready && engine.ready(),
            "last_bar_end_ms":engine.last_end_ms,"rules":engine.latest,"positions":engine.targets()});
        report["frequencies"] = frequencies;
        report["phase"] = json!(stream.phase(
            loaded,
            market_context.as_ref(),
            chrono::Utc::now().timestamp_millis(),
            report["phase"].as_str().unwrap_or("waiting_live"),
        ));
        report["initialization_complete"] = json!(history.is_none());
        report["restart_required"] = json!(stream.needs_restart);
        report["grpc_through_ms"] = json!(history.as_ref().map(|h| h.through_ms));
        report["history_rows"] = json!(history.as_ref().map_or(0, |h| h.minutes.len()
            + h.hfq.as_ref().map_or(0, |hfq| hfq.rows.len())));
        report["redis_first_minute_ms"] = json!(stream.minutes.first().map(|r| r.open_time_ms));
        report["redis_cached_minutes"] = json!(stream.minutes.len());
        report["redis_gap_since_ms"] = json!(stream
            .gap_start
            .or_else(|| stream.gaps.first().map(|&(start, _)| start)));
        report["history_repair_error"] = json!(repair_error);
        *state.write().unwrap_or_else(|p| p.into_inner()) = report;

        // Slow gRPC is expected. It never replaces or advances the local anchor.
        if let Some(history) = history.as_mut() {
            if history.hfq.is_none() && tokio::time::Instant::now() >= next_fetch {
                match hfq::fetch(
                    &cfg,
                    &current,
                    history_begin(&cfg),
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
                {
                    Ok(archive) => {
                        load_start = archive.cutoff;
                        history.hfq = Some(archive);
                    }
                    Err(error) => {
                        health.pause(format!("gRPC HFQ history: {error:#}"));
                        next_fetch = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                }
            }
            if history.hfq.is_some() && tokio::time::Instant::now() >= next_fetch {
                let until = chrono::Utc::now().timestamp_millis();
                let start = if loaded {
                    history
                        .minutes
                        .values()
                        .find(|r| r.provisional)
                        .map(|r| r.open_time_ms)
                        .or_else(|| history.minutes.keys().next_back().copied())
                        .unwrap_or(load_start)
                } else {
                    load_start
                };
                let end = if loaded {
                    until
                } else {
                    (start + 30 * 86_400_000 - 1).min(until)
                };
                let result = history.fetch(&cfg, &current, start, end).await;
                let succeeded = result.is_ok();
                match result {
                    Ok(()) => {
                        if !loaded {
                            load_start = end + 1;
                            loaded = load_start > until;
                        }
                        if history.contract.is_some()
                            && market_context.as_ref().is_none_or(|context| {
                                context.snapshot_as_of != history.session_snapshot
                            })
                        {
                            match history::parameters(&cfg).await {
                                Ok(context)
                                    if context.snapshot_as_of == history.session_snapshot =>
                                {
                                    market_context = Some(context);
                                }
                                Ok(_) => {
                                    health.pause("gRPC minute/session snapshot differs".into())
                                }
                                Err(e) => health.pause(format!("gRPC parameters: {e:#}")),
                            }
                        }
                        if stream.input.is_none() && stream.pending.has() {
                            if let Some(context) = market_context.as_ref().filter(|context| {
                                context.snapshot_as_of == history.session_snapshot
                            }) {
                                if let Err(e) =
                                    stream.prepare(context.clone(), history.multiple()?, &current)
                                {
                                    stream.reset();
                                    health.pause(format!("Redis baseline preparation: {e:#}"));
                                }
                            }
                        }
                    }
                    Err(e) => health.pause(format!("gRPC history: {e:#}")),
                }
                next_fetch = tokio::time::Instant::now()
                    + if succeeded && !loaded {
                        Duration::from_millis(100)
                    } else {
                        Duration::from_secs(5)
                    };
            }
        }
        if tokio::time::Instant::now() >= next_dominant {
            match history::dominant_with(&cfg, Some(&source_dominant)).await {
                Ok(row) => {
                    source_dominant = row.clone();
                    let row = if row.trading_day < current.trading_day {
                        row.for_day(current.trading_day)
                    } else {
                        Ok(row)
                    };
                    match row {
                        Ok(next) => {
                            if let Some(history) = history.as_mut() {
                                if next.contract != current.contract
                                    || next.source_trading_day > current.source_trading_day
                                {
                                    stream.reset();
                                    *history = history::History::default();
                                    load_start = history_begin(&cfg);
                                    loaded = false;
                                    next_fetch = tokio::time::Instant::now();
                                }
                            } else if next.contract != current.contract {
                                stream.reset();
                                health.pause("dominant contract changed; restart for new-contract initialization".into());
                            }
                            current = next;
                            health.dominant = None;
                        }
                        Err(e) => health.dominant = Some(format!("dominant selection: {e:#}")),
                    }
                }
                Err(e) => health.dominant = Some(format!("dominant REST: {e:#}")),
            }
            next_dominant = tokio::time::Instant::now() + Duration::from_secs(60);
        }
    }
}
