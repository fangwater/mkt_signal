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
    collections::{BTreeMap, BTreeSet},
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
    json!({"name":cfg.name,"phase":if error.is_some() {"paused"} else if live_ready {"live"} else {"waiting_live"},"exchange":cfg.exchange,"symbol":cfg.symbol,"interval":"1h","position_unit":"lot","dominant":main,"factor_ready":engine.ready(),"live_ready":live_ready,"error":error,"last_bar_end_ms":engine.last_end_ms,"rules":engine.latest,"positions":engine.targets(),"updated_us":chrono::Utc::now().timestamp_micros()})
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
fn consume(
    engine: &mut engine::Engine,
    input: &live::Input,
    hours: &mut engine::Bars,
    main: &history::Dominant,
    rows: Vec<cn_market_proto::MinuteBar>,
    dominant_ready: bool,
) -> Result<()> {
    let mut closed = Vec::new();
    for row in rows {
        closed.extend(hours.push(row, input.multiple)?);
        if hours
            .last_minute_ms()
            .is_some_and(|last| input.context.ends_after(last))
        {
            if let Some(hour) = hours.finish(input.multiple)? {
                closed.push(hour);
            }
        }
    }
    let now = chrono::Utc::now().timestamp_millis();
    let fresh = (chrono::Utc::now().timestamp_micros() as u64)
        .saturating_sub(input.last_received_us)
        <= 5_000_000;
    for hour in closed {
        let live = dominant_ready
            && fresh
            && hour.end_ms > input.live_after_ms
            && hour.end_ms <= now + live::SEAL_LEAD_MS
            && now - hour.end_ms <= 120_000;
        engine.observe(&hour, &main.contract, input.multiple, live)?;
    }
    Ok(())
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
/// ZMQ minutes are accumulated independently of delayed historical requests.
#[derive(Default)]
struct Stream {
    pending: recovery::Buffer,
    input: Option<live::Input>,
    minutes: Vec<cn_market_proto::MinuteBar>,
    aligned: bool,
    needs_restart: bool,
    extras: Option<extras::Extras>,
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
    fn activate(&mut self) {
        self.aligned = true;
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
        engine: &mut engine::Engine,
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
        let input = self
            .input
            .as_mut()
            .context("missing initialized ZMQ input")?;
        // An ordered quote from the next day also closes the previous day's
        // final source minute. Preserve formula windows across ordinary days.
        let rows = input.closed(first_ms, previous)?;
        self.accept(rows, engine, hours, previous, false)?;
        let input = self.input.take().context("missing initialized ZMQ input")?;
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
        rows: Vec<cn_market_proto::MinuteBar>,
        engine: &mut engine::Engine,
        hours: &mut engine::Bars,
        main: &history::Dominant,
        dominant_ready: bool,
    ) -> Result<()> {
        if self.aligned {
            let input = self.input.as_ref().context("missing live input")?;
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
            consume(
                engine,
                self.input.as_ref().context("missing live input")?,
                hours,
                main,
                rows,
                dominant_ready,
            )?;
        } else {
            ensure!(
                self.minutes.len() + rows.len() <= 4096,
                "ZMQ minute cache exhausted"
            );
            self.minutes.extend(rows);
        }
        Ok(())
    }
    fn observe(
        &mut self,
        event: live::Event,
        engine: &mut engine::Engine,
        hours: &mut engine::Bars,
        main: &history::Dominant,
        dominant_ready: bool,
    ) -> Result<()> {
        ensure!(
            !self.needs_restart,
            "source continuity lost; restart for initialization"
        );
        if matches!(event, live::Event::Reset) {
            self.reset();
            ensure!(!self.needs_restart, "ZMQ reset; restart for initialization");
            return Ok(());
        }
        if let Some(input) = self.input.as_mut() {
            let rows = input.observe(event, main)?;
            self.accept(rows, engine, hours, main, dominant_ready)
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
            && self.input.as_ref().is_some_and(|i| {
                (chrono::Utc::now().timestamp_micros() as u64).saturating_sub(i.last_received_us)
                    <= 5_000_000
            })
    }
}

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
            product.zmq_endpoint == primary.zmq_endpoint,
            "additional products must use the group's ZMQ source"
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
    let inputs = live::capture(&root.zmq_endpoint, configs.iter().map(|(_, cfg)| cfg))?;
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
            "strategies":positions,"live_ready":false,"factor_ready":false,"positions":engine.targets(),
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
    let mut next_dominant = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        while let Ok(command) = config_rx.try_recv() {
            panel::process(
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
        }
        let decision_before = engine.ledger.last_decision_ms;
        let extra_before = stream
            .extras
            .as_ref()
            .and_then(extras::Extras::last_live_decision);
        // Drain all events accumulated during the preceding network request,
        // including reset markers, before closing bars or attempting a handoff.
        loop {
            let event = match rx.try_recv() {
                Ok(event) => event,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    anyhow::bail!("ZMQ capture thread stopped")
                }
            };
            if let Ok(Some(publisher)) = &mut publisher {
                match &event {
                    live::Event::Quote(q) => publisher.quote(q),
                    live::Event::Reset => publisher.reset(),
                }
            }
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
                                &mut engine,
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
            if let Err(e) = stream.observe(
                event,
                &mut engine,
                &mut hours,
                &current,
                health.dominant.is_none(),
            ) {
                stream.reset();
                health.pause(format!("ZMQ input: {e:#}"));
            }
        }
        if let Some(input) = stream.input.as_mut() {
            let rows = input.clock(chrono::Utc::now().timestamp_millis(), &current);
            if let Err(e) = rows.and_then(|rows| {
                stream.accept(
                    rows,
                    &mut engine,
                    &mut hours,
                    &current,
                    health.dominant.is_none(),
                )
            }) {
                stream.reset();
                health.pause(format!("ZMQ minute: {e:#}"));
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
        if loaded {
            if let (Some(history_input), Some(input)) = (history.as_ref(), &stream.input) {
                match recovery::recover(
                    &cfg,
                    &current,
                    history_input,
                    engine.ledger.clone(),
                    input,
                    &stream.minutes,
                ) {
                    Ok(recovered) => {
                        println!(
                            "{}",
                            json!({"type":"grpc_zmq_aligned","contract":current.contract,
                            "anchor_ms":recovered.anchor_ms,"history_rows":recovered.history_rows,
                            "cached_zmq_minutes":stream.minutes.len(),"hours":recovered.engine.hours,
                            "positions_preserved":true})
                        );
                        engine = recovered.engine;
                        hours = recovered.hours;
                        stream.extras = Some(recovered.extras);
                        stream.activate();
                        // gRPC is a startup dependency. Drop its data and make
                        // further runtime historical requests unreachable.
                        history = None;
                        health.ingress = None;
                    }
                    Err(e) => health.pause(format!("gRPC catch-up: {e:#}")),
                }
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
                hour_decision.max(extra_decision),
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
        report["zmq_first_minute_ms"] = json!(stream.minutes.first().map(|r| r.open_time_ms));
        report["zmq_cached_minutes"] = json!(stream.minutes.len());
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
                                    health.pause(format!("ZMQ baseline preparation: {e:#}"));
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
