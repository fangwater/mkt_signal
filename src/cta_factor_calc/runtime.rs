use super::{
    config::{Combination, Config, Interval, Market, Plan, Worker},
    engine::{Engine, FactorEvent, FactorPoint},
};
use crate::factor_pub::model_output_publisher::ModelPublisher;
use anyhow::{ensure, Context, Result};
use clap::{ArgGroup, Parser};
use log::{info, warn};
use period_pbs::kafka::{decode_period_payload, RawKafkaConsumer};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(group(ArgGroup::new("action").args(["check", "put_combination", "delete_combination", "import_symbol_factors", "replay_only"])))]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    /// Validate and print the merged plan without Kafka, IPC, or filesystem writes.
    #[arg(long)]
    check: bool,
    /// Install or replace one named combination from JSON.
    #[arg(long)]
    put_combination: Option<PathBuf>,
    /// Convert the existing symbol -> factor -> weight JSON table.
    #[arg(long, requires_all=["name","strategy_group","market","interval"])]
    import_symbol_factors: Option<PathBuf>,
    #[arg(long, requires = "import_symbol_factors")]
    name: Option<String>,
    #[arg(long, requires = "import_symbol_factors")]
    strategy_group: Option<String>,
    #[arg(long, requires = "import_symbol_factors")]
    market: Option<String>,
    #[arg(long, requires = "import_symbol_factors")]
    interval: Option<String>,
    #[arg(long, requires = "import_symbol_factors")]
    include_nq: bool,
    #[arg(long)]
    delete_combination: Option<String>,
    /// Apply the requested configuration edit; otherwise print its merged plan.
    #[arg(long)]
    execute: bool,
    /// Replay a fixed head into history and exit without creating IPC.
    #[arg(long)]
    replay_only: bool,
    #[arg(long)]
    core: Option<usize>,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
pub fn service_path(worker: Worker) -> String {
    format!("factor_output/{}", worker.name())
}

pub(crate) struct FileLock(File);
#[derive(Debug)]
pub struct CatalogConflict(pub &'static str);
impl std::fmt::Display for CatalogConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for CatalogConflict {}

pub fn catalog_revision(plan: &Plan) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(plan)?)))
}
impl FileLock {
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(CatalogConflict("configuration is busy; reload and retry").into());
        }
        Ok(Self(file))
    }
}
impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

pub fn atomic_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let parent = path.parent().context("file has no parent directory")?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Edits affect only the named JSON configuration. Shared calculations survive
/// removal until no remaining combination references them. History is untouched.
pub fn edit_combination(
    config: &Config,
    put: Option<Combination>,
    delete: Option<&str>,
    execute: bool,
    worker: Worker,
) -> Result<Plan> {
    edit_combination_checked(config, put, delete, execute, worker, None)
}

pub fn edit_combination_checked(
    config: &Config,
    put: Option<Combination>,
    delete: Option<&str>,
    execute: bool,
    worker: Worker,
    expected_revision: Option<&str>,
) -> Result<Plan> {
    ensure!(
        put.is_some() != delete.is_some(),
        "choose one configuration edit"
    );
    if execute {
        fs::create_dir_all(&config.combinations_dir)?;
    }
    let _lock = execute
        .then(|| FileLock::acquire(&config.combinations_dir.join(".catalog.lock")))
        .transpose()?;
    let current = Plan::load(&config.combinations_dir)?;
    if let Some(expected) = expected_revision {
        if expected != catalog_revision(&current)? {
            return Err(CatalogConflict("configuration changed; reload before saving").into());
        }
    }
    let mut combinations = current.combinations;
    let name = if let Some(value) = &put {
        value.validate()?;
        if let Some(previous) = combinations.get(&value.name) {
            ensure!(
                (previous.signal.is_none() && value.signal.is_none())
                    || previous.manager_strategy_name() == value.manager_strategy_name(),
                "signal strategy group/market/frequency cannot change in place; create a new combination"
            );
        }
        combinations.insert(value.name.clone(), value.clone());
        value.name.clone()
    } else {
        let name = delete.unwrap();
        ensure!(
            combinations.remove(name).is_some(),
            "unknown combination {name}"
        );
        name.to_owned()
    };
    let plan = Plan::merge(combinations.into_values())?;
    worker.validate_plan(&plan)?;
    if execute {
        let path = config.combinations_dir.join(format!("{name}.json"));
        if let Some(value) = put {
            atomic_json(&path, &value)?;
        } else {
            fs::remove_file(path)?;
            File::open(&config.combinations_dir)?.sync_all()?;
        }
    }
    Ok(plan)
}

struct Journal {
    root: PathBuf,
    writer: Option<(String, BufWriter<File>)>,
}
impl Journal {
    fn new(root: PathBuf, plan: &Plan) -> Result<Self> {
        fs::create_dir_all(&root)?;
        atomic_json(&root.join("plan.json"), plan)?;
        Ok(Self { root, writer: None })
    }
    fn record(&mut self, point: &FactorPoint) -> Result<()> {
        let date = chrono::DateTime::from_timestamp_millis(point.ts_ms)
            .context("invalid factor time")?
            .format("%Y%m%d")
            .to_string();
        if self.writer.as_ref().is_none_or(|(old, _)| old != &date) {
            if let Some((_, writer)) = &mut self.writer {
                writer.flush()?;
            }
            self.writer = Some((
                date.clone(),
                BufWriter::new(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(self.root.join(format!("{date}.jsonl")))?,
                ),
            ));
        }
        let writer = &mut self.writer.as_mut().unwrap().1;
        serde_json::to_writer(&mut *writer, point)?;
        writer.write_all(b"\n")?;
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if let Some((_, writer)) = &mut self.writer {
            writer.flush()?;
        }
        Ok(())
    }
}

struct Generation {
    id: String,
    engine: Engine,
    consumer: Option<RawKafkaConsumer>,
    targets: BTreeMap<(String, i32), i64>,
    consumed: BTreeMap<(String, i32), i64>,
    deadline: Instant,
    journal: Journal,
    publish_from: i64,
    records: u64,
}
impl Generation {
    fn new(plan: Plan, config: &Config, worker: Worker) -> Result<Self> {
        worker.validate_plan(&plan)?;
        let id = uuid::Uuid::new_v4().to_string();
        let topics = plan.topics();
        let mut kafka = config.kafka.clone();
        kafka.topics = topics.clone();
        kafka.offset_reset = "earliest".into();
        kafka.enable_auto_commit = false;
        kafka.group_id = format!("{}-{id}", worker.name());
        kafka.client_id = kafka.group_id.clone();
        let consumer = (!topics.is_empty())
            .then(|| RawKafkaConsumer::new(&kafka))
            .transpose()?;
        let mut targets = BTreeMap::new();
        if let Some(consumer) = &consumer {
            for mark in consumer.query_topic_watermarks(
                &topics,
                kafka.metadata_timeout_ms,
                kafka.watermark_timeout_ms,
            )? {
                ensure!(
                    mark.high > mark.low,
                    "required Kafka topic {} partition {} has no retained records",
                    mark.topic,
                    mark.partition
                );
                targets.insert((mark.topic, mark.partition), mark.high - 1);
            }
        }
        let journal = Journal::new(config.history_dir.join("generations").join(&id), &plan)?;
        Ok(Self {
            id,
            engine: Engine::new(plan, config)?,
            consumer,
            targets,
            consumed: BTreeMap::new(),
            deadline: Instant::now() + Duration::from_secs(config.catchup_timeout_secs),
            journal,
            publish_from: now_ms().saturating_sub(
                (config.lookback_secs.saturating_mul(1000)).min(i64::MAX as u64) as i64,
            ),
            records: 0,
        })
    }
    fn caught_up(&self) -> bool {
        self.targets
            .iter()
            .all(|(key, target)| self.consumed.get(key).is_some_and(|n| n >= target))
    }
    fn update_head(&mut self, config: &Config) -> Result<()> {
        if let Some(consumer) = &self.consumer {
            for mark in consumer.query_topic_watermarks(
                &self.engine.plan.topics(),
                config.kafka.metadata_timeout_ms,
                config.kafka.watermark_timeout_ms,
            )? {
                ensure!(
                    mark.high > mark.low,
                    "required Kafka topic is empty during replay"
                );
                self.targets
                    .insert((mark.topic, mark.partition), mark.high - 1);
            }
        }
        Ok(())
    }
    fn poll(&mut self, config: &Config, timeout: u64) -> Result<Vec<FactorPoint>> {
        let Some(consumer) = &self.consumer else {
            return Ok(Vec::new());
        };
        let Some(record) = consumer.poll(timeout) else {
            return Ok(Vec::new());
        };
        let record = record.context("poll factor Kafka consumer")?;
        let market = Market::ALL
            .into_iter()
            .find(|m| m.topic() == record.topic)
            .context("unexpected factor Kafka topic")?;
        let (_, _, period) =
            decode_period_payload(&record.payload, config.kafka.payload_compression)
                .context("decode factor PeriodMessage")?;
        let points = self.engine.on_period(market, &period)?;
        self.consumed
            .insert((record.topic, record.partition), record.offset);
        self.records += 1;
        let points = points
            .into_iter()
            .filter(|p| p.ts_ms >= self.publish_from)
            .collect::<Vec<_>>();
        for point in &points {
            self.journal.record(point)?;
        }
        Ok(points)
    }
}

fn send_event(publisher: &ModelPublisher, event: &FactorEvent) -> Result<()> {
    let payload = serde_json::to_vec(event)?;
    ensure!(
        payload.len() <= mkt_parsers::msg::model_ipc::MODEL_PAYLOAD_MAX_BYTES,
        "factor output exceeds IPC capacity"
    );
    ensure!(publisher.publish(&payload), "factor output delivery failed");
    Ok(())
}

fn send(publisher: &ModelPublisher, generation: &str, point: &FactorPoint) -> Result<()> {
    send_event(
        publisher,
        &FactorEvent::Point {
            generation: generation.into(),
            point: point.clone(),
        },
    )
}

/// The same engine drives both workers. Perp/Spread select configuration roots,
/// and own their frequency position tables in the same process.
pub async fn main(worker: Worker) -> Result<()> {
    // Position sizing starts the thread-local BBO consumers when the first
    // table is enabled, including after a configuration hot reload.
    tokio::task::LocalSet::new().run_until(run(worker)).await
}

async fn run(worker: Worker) -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    ensure!(
        !args.execute
            || args.put_combination.is_some()
            || args.delete_combination.is_some()
            || args.import_symbol_factors.is_some(),
        "--execute requires a configuration edit"
    );
    let config_path = args
        .config
        .unwrap_or_else(|| PathBuf::from(worker.default_config()));
    let mut config = Config::load(&config_path)?;
    let editing = args.put_combination.is_some()
        || args.delete_combination.is_some()
        || args.import_symbol_factors.is_some();
    let plan = if editing {
        let mut put = args
            .put_combination
            .as_ref()
            .map(|path| -> Result<Combination> {
                serde_json::from_slice(&fs::read(path)?).context("parse requested combination")
            })
            .transpose()?;
        if let Some(path) = &args.import_symbol_factors {
            let market: Market = serde_json::from_value(json!(args.market.as_ref().unwrap()))?;
            let interval: Interval =
                serde_json::from_value(json!(args.interval.as_ref().unwrap()))?;
            put = Some(Combination::from_symbol_factors(
                args.name.as_ref().unwrap().clone(),
                args.strategy_group.as_ref().unwrap().clone(),
                market,
                interval,
                serde_json::from_slice(&fs::read(path)?)?,
                args.include_nq,
            )?);
        }
        edit_combination(
            &config,
            put,
            args.delete_combination.as_deref(),
            args.execute,
            worker,
        )?
    } else {
        Plan::load(&config.combinations_dir)?
    };
    worker.validate_plan(&plan)?;
    if args.check || editing {
        let position_tables=plan.combinations.values().filter_map(|c|c.signal.as_ref().map(|p|
            json!({"strategy_name":c.manager_strategy_name(),"strategy_group":c.strategy_group,"combination_name":c.name,"interval":c.interval,"enabled":p.enabled,"max_pos_u":p.max_pos_u}))).collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string_pretty(
                &json!({"worker":worker,"topics":plan.topics(),"position_tables":position_tables,"plan":plan})
            )?
        );
        return Ok(());
    }
    runtime_common::affinity::maybe_pin_current_thread(
        args.core,
        &format!(
            "{}_CORE",
            worker.name().replace('-', "_").to_ascii_uppercase()
        ),
    )?;
    fs::create_dir_all(&config.history_dir)?;
    let _runtime_lock = FileLock::acquire(&config.history_dir.join(".worker.lock"))?;
    let mut thresholds_error = None;
    match super::thresholds::load(&plan) {
        Ok(values) => {
            info!(
                "shared monthly amount thresholds loaded: {} symbols",
                values.len()
            );
            config.amount_thresholds = values;
        }
        Err(e) => {
            warn!("shared amount thresholds unavailable: {e:#}");
            thresholds_error = Some(format!("amount thresholds: {e:#}"));
        }
    }
    let mut last_threshold_reload = Instant::now();
    let mut active: Option<Generation> = None;
    let mut candidate = Some(Generation::new(plan, &config, worker)?);
    let publisher = if args.replay_only {
        None
    } else {
        Some(ModelPublisher::with_capacity(
            &format!("cta_factor_{}", worker.name()),
            &service_path(worker),
            8192,
            8192,
        )?)
    };
    let mut last_status = Instant::now();
    let mut error: Option<String> = None;
    let mut kafka_error: Option<String> = None;
    let mut observed_plan = candidate.as_ref().unwrap().engine.plan.clone();
    let mut position_tables = if args.replay_only {
        None
    } else {
        Some(super::positions::PositionTables::new(&config, worker)?)
    };
    let mut last_position_poll = Instant::now() - Duration::from_secs(1);
    let mut positions_error: Option<String> = None;
    loop {
        let mut progressed = false;
        if let Some(generation) = &mut active {
            let records = generation.records;
            match generation.poll(&config, 0) {
                Ok(points) => {
                    if generation.records > records {
                        kafka_error = None;
                    }
                    progressed |= !points.is_empty();
                    if let Some(publisher) = &publisher {
                        for point in points {
                            if let Err(e) = send(publisher, &generation.id, &point) {
                                error = Some(e.to_string());
                            }
                        }
                    }
                }
                Err(e) => {
                    kafka_error = Some(format!("active Kafka: {e:#}"));
                }
            }
        }
        if let Some(generation) = &mut candidate {
            let result = (|| -> Result<bool> {
                ensure!(
                    Instant::now() < generation.deadline,
                    "factor replay timed out"
                );
                let records = generation.records;
                generation.poll(&config, 0)?;
                progressed |= records != generation.records;
                if !generation.caught_up() {
                    return Ok(false);
                }
                if !args.replay_only {
                    generation.update_head(&config)?;
                }
                Ok(generation.caught_up())
            })();
            match result {
                Ok(true) => {
                    generation.journal.flush()?;
                    info!(
                        "factor worker={} generation={} replay complete records={} calculations={}",
                        worker.name(),
                        generation.id,
                        generation.records,
                        generation.engine.plan.calculations.len()
                    );
                    if let Some(publisher) = &publisher {
                        for point in generation
                            .engine
                            .latest
                            .values()
                            .filter(|p| p.ts_ms >= generation.publish_from)
                        {
                            send(publisher, &generation.id, point)?;
                        }
                    }
                    if let Some(tables) = &mut position_tables {
                        tables.activate_generation(now_ms() * 1000);
                    }
                    active = candidate.take();
                    error = None;
                    kafka_error = None;
                    if args.replay_only {
                        write_status(
                            &config,
                            worker,
                            active.as_ref(),
                            None,
                            thresholds_error.as_deref(),
                            None,
                            None,
                        )?;
                        return Ok(());
                    }
                }
                Ok(false) => {}
                Err(e) => {
                    if active.is_none() {
                        return Err(e);
                    }
                    warn!("factor replacement replay failed: {e:#}");
                    error = Some(format!("replacement replay: {e:#}"));
                    candidate = None;
                }
            }
        }
        if last_status.elapsed() >= Duration::from_secs(1) {
            if let Some(generation) = &mut active {
                generation.journal.flush()?;
            }
            let next_plan = Plan::load(&config.combinations_dir).and_then(|p| {
                worker.validate_plan(&p)?;
                Ok(p)
            });
            let mut next_thresholds = None;
            if last_threshold_reload.elapsed() >= Duration::from_secs(180)
                || next_plan.as_ref().is_ok_and(|p| p != &observed_plan)
            {
                if let Ok(plan) = &next_plan {
                    match super::thresholds::load(plan) {
                        Ok(values) => {
                            next_thresholds = Some(values);
                            thresholds_error = None;
                        }
                        Err(e) => thresholds_error = Some(format!("amount thresholds: {e:#}")),
                    }
                }
                last_threshold_reload = Instant::now();
            }
            let thresholds_changed = next_thresholds
                .as_ref()
                .is_some_and(|v| v != &config.amount_thresholds);
            match next_plan {
                Ok(next) if next != observed_plan || thresholds_changed => {
                    let changed_definitions = thresholds_changed
                        || next.calculations.iter().any(|(id, new)| {
                            active
                                .as_ref()
                                .and_then(|g| g.engine.plan.calculations.get(id))
                                .is_none_or(|old| old.parameters != new.parameters)
                        });
                    if changed_definitions || active.is_none() {
                        let mut replay_config = config.clone();
                        if let Some(values) = next_thresholds {
                            replay_config.amount_thresholds = values;
                        }
                        match Generation::new(next.clone(), &replay_config, worker) {
                            Ok(g) => {
                                config = replay_config;
                                candidate = Some(g);
                                observed_plan = next;
                                error = None;
                            }
                            Err(e) => {
                                error = Some(format!("configuration replay preparation: {e:#}"))
                            }
                        }
                    } else {
                        candidate = None;
                        error = None;
                        let generation = active.as_mut().unwrap();
                        if let Some(publisher) = &publisher {
                            for id in generation
                                .engine
                                .plan
                                .calculations
                                .keys()
                                .filter(|id| !next.calculations.contains_key(*id))
                            {
                                if let Err(e) = send_event(
                                    publisher,
                                    &FactorEvent::Removed {
                                        generation: generation.id.clone(),
                                        factor_id: id.clone(),
                                    },
                                ) {
                                    error = Some(e.to_string());
                                }
                            }
                        }
                        generation.engine.retain_plan(next.clone())?;
                        observed_plan = next;
                    }
                }
                Ok(_) => {}
                Err(e) => error = Some(format!("configuration: {e:#}")),
            }
            let positions = position_tables.as_ref().map(|p| p.snapshot()).transpose()?;
            write_status(
                &config,
                worker,
                active.as_ref(),
                candidate.as_ref(),
                error
                    .as_deref()
                    .or(kafka_error.as_deref())
                    .or(thresholds_error.as_deref()),
                positions.as_ref(),
                positions_error.as_deref(),
            )?;
            last_status = Instant::now();
        }
        if last_position_poll.elapsed() >= Duration::from_secs(1) {
            if let Some(tables) = &mut position_tables {
                match tables.tick(
                    &observed_plan,
                    active.as_ref().map(|g| &g.engine),
                    now_ms() * 1000,
                ) {
                    Ok(()) => positions_error = None,
                    Err(e) => {
                        tables.pause();
                        let message = format!("position tables: {e:#}");
                        if positions_error.as_ref() != Some(&message) {
                            warn!("{message}");
                        }
                        positions_error = Some(message);
                    }
                }
            }
            last_position_poll = Instant::now();
        }
        // Yield both replay and network tasks, without imposing a per-record sleep.
        if !progressed {
            tokio::time::sleep(Duration::from_millis(2)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}

fn write_status(
    config: &Config,
    worker: Worker,
    active: Option<&Generation>,
    candidate: Option<&Generation>,
    error: Option<&str>,
    position_tables: Option<&serde_json::Value>,
    positions_error: Option<&str>,
) -> Result<()> {
    atomic_json(
        &config.history_dir.join("state.json"),
        &json!({"worker":worker,"service":service_path(worker),"updated_ms":now_ms(),"generation":active.map(|g| &g.id),"warming":candidate.is_some(),"warm_records":candidate.map(|g| g.records),"plan":active.map(|g| &g.engine.plan),"pending_plan":candidate.map(|g| &g.engine.plan),"latest":active.map(|g| &g.engine.latest),"error":error,"position_tables":position_tables,"positions_error":positions_error,"amount_threshold_symbols":config.amount_thresholds.len()}),
    )
}
