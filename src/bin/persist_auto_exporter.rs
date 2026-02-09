use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use log::{error, info, warn};
use mkt_signal::persist_manager::{self, exporter, RocksDbStore};
use serde::Deserialize;
use tar::Builder;

const EXPORT_DELAY_SECS: i64 = 60;

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    let state_path = resolve_state_path(&args.state_file)?;
    let mut state = ExportState::load(&state_path)?;

    info!(
        "persist_auto_exporter started config={} state={}",
        args.config.display(),
        state_path.display()
    );

    loop {
        let now = Utc::now();
        let next_tick = next_hour_tick(now)?;
        let sleep_dur = (next_tick - now)
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::from_secs(0));
        info!("next schedule tick at {}", next_tick);
        std::thread::sleep(sleep_dur);

        let run_now = Utc::now();
        let result = run_once(&args.config, &mut state, &state_path, run_now);
        if let Err(err) = result {
            error!("run_once failed: {err:#}");
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "persist_auto_exporter")]
struct Args {
    #[arg(long, default_value = "config/persist_auto_exporter.toml")]
    config: PathBuf,

    #[arg(long)]
    state_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct AutoExporterConfig {
    targets: Vec<TargetConfig>,
}

#[derive(Debug, Deserialize)]
struct TargetConfig {
    name: String,
    input_dir: String,
    output_dir: String,
    interval: ExportInterval,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum ExportInterval {
    Hour,
    Day,
}

impl ExportInterval {
    fn as_str(self) -> &'static str {
        match self {
            ExportInterval::Hour => "hour",
            ExportInterval::Day => "day",
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct ExportState {
    #[serde(default)]
    targets: HashMap<String, i64>,
}

impl ExportState {
    fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let txt = fs::read_to_string(path)
            .with_context(|| format!("failed to read state file {}", path.display()))?;
        let st: ExportState = toml::from_str(&txt)
            .with_context(|| format!("failed to parse state file {}", path.display()))?;
        Ok(st)
    }

    fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create state dir {}", parent.display()))?;
        }

        let mut out = String::new();
        out.push_str("[targets]\n");
        for (name, ts) in &self.targets {
            out.push_str(&format!("{} = {}\n", quote_toml_key(name), ts));
        }

        fs::write(path, out).with_context(|| format!("failed to write {}", path.display()))
    }

    fn last_export_end(&self, name: &str) -> Option<i64> {
        self.targets.get(name).copied()
    }

    fn set_last_export_end(&mut self, name: &str, end_ts: i64) {
        self.targets.insert(name.to_string(), end_ts);
    }
}

fn quote_toml_key(input: &str) -> String {
    let escaped = input.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{}\"", escaped)
}

fn resolve_state_path(input: &Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = input {
        return Ok(path.clone());
    }

    let mut dir = std::env::current_exe().context("resolve current executable")?;
    dir.pop();
    Ok(dir.join("persist_auto_exporter_state.toml"))
}

fn run_once(
    config_path: &Path,
    state: &mut ExportState,
    state_path: &Path,
    now: DateTime<Utc>,
) -> Result<()> {
    let cfg = load_config(config_path)?;
    if cfg.targets.is_empty() {
        return Err(anyhow!(
            "no targets configured in {}",
            config_path.display()
        ));
    }

    let mut changed = false;
    for target in &cfg.targets {
        match process_target(target, state, now) {
            Ok(did_export) => {
                if did_export {
                    changed = true;
                }
            }
            Err(err) => {
                error!("target={} export failed: {err:#}", target.name);
            }
        }
    }

    if changed {
        state.save(state_path)?;
    }

    Ok(())
}

fn load_config(path: &Path) -> Result<AutoExporterConfig> {
    let txt = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let cfg: AutoExporterConfig = toml::from_str(&txt)
        .with_context(|| format!("failed to parse config {}", path.display()))?;

    for target in &cfg.targets {
        let input_dir = Path::new(&target.input_dir);
        let output_dir = Path::new(&target.output_dir);
        ensure_absolute("input_dir", input_dir)?;
        ensure_absolute("output_dir", output_dir)?;
        if !input_dir.exists() {
            return Err(anyhow!(
                "target={} input_dir does not exist: {}",
                target.name,
                input_dir.display()
            ));
        }
    }

    Ok(cfg)
}

fn process_target(
    target: &TargetConfig,
    state: &mut ExportState,
    now: DateTime<Utc>,
) -> Result<bool> {
    let state_key = format!("{}#{}", target.name, target.interval.as_str());

    let delayed = now - Duration::seconds(EXPORT_DELAY_SECS);
    if matches!(target.interval, ExportInterval::Day) && delayed.hour() != 0 {
        info!(
            "target={} skip interval=day at hour={}",
            target.name,
            delayed.hour()
        );
        return Ok(false);
    }

    let export_boundary = latest_finished_window_end(now, target.interval)?;
    let export_boundary_ts = export_boundary.timestamp();
    if export_boundary_ts <= 0 {
        return Ok(false);
    }

    let last_done = state.last_export_end(&state_key).unwrap_or(i64::MIN);
    if last_done >= export_boundary_ts {
        info!(
            "target={} no new window interval={} last_done={} boundary={}",
            target.name,
            target.interval.as_str(),
            last_done,
            export_boundary_ts
        );
        return Ok(false);
    }

    let (start_us, end_us, label) = match target.interval {
        ExportInterval::Hour => {
            let start = export_boundary - Duration::hours(1) + Duration::microseconds(1);
            (
                start.timestamp_micros(),
                export_boundary.timestamp_micros(),
                export_boundary.format("%Y%m%d_%H").to_string(),
            )
        }
        ExportInterval::Day => {
            let start = export_boundary - Duration::days(1) + Duration::microseconds(1);
            (
                start.timestamp_micros(),
                export_boundary.timestamp_micros(),
                export_boundary.format("%Y%m%d").to_string(),
            )
        }
    };

    let start_us_u64 = i64_to_u64_ts(start_us, "start_us")?;
    let end_us_u64 = i64_to_u64_ts(end_us, "end_us")?;

    info!(
        "target={} exporting interval={} window={}..{} label={}",
        target.name,
        target.interval.as_str(),
        start_us_u64,
        end_us_u64,
        label
    );

    let cf_names = persist_manager::required_column_families();
    let tuning = persist_manager::default_tuning();
    let store = RocksDbStore::open_read_only_with_tuning(&target.input_dir, &cf_names, &tuning)?;

    let output_dir = Path::new(&target.output_dir);
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output_dir {}", output_dir.display()))?;

    let stage_dir = output_dir.join(".stage").join(&target.name).join(&label);
    if stage_dir.exists() {
        fs::remove_dir_all(&stage_dir)
            .with_context(|| format!("failed to cleanup stage {}", stage_dir.display()))?;
    }
    fs::create_dir_all(&stage_dir)
        .with_context(|| format!("failed to create stage {}", stage_dir.display()))?;

    exporter::export_window_to_dir(&store, &stage_dir, start_us_u64, end_us_u64)?;

    let archive_name = format!("{}_{}.tar.gz", target.name, label);
    let archive_path = output_dir.join(archive_name);
    bundle_window_archive(&stage_dir, &archive_path)?;

    if let Err(err) = fs::remove_dir_all(&stage_dir) {
        warn!(
            "target={} failed to remove stage {}: {err}",
            target.name,
            stage_dir.display()
        );
    }

    state.set_last_export_end(&state_key, export_boundary.timestamp());
    info!(
        "target={} export finished archive={} end_ts={}",
        target.name,
        archive_path.display(),
        export_boundary.timestamp()
    );
    Ok(true)
}

fn latest_finished_window_end(
    now: DateTime<Utc>,
    interval: ExportInterval,
) -> Result<DateTime<Utc>> {
    let delayed = now - Duration::seconds(EXPORT_DELAY_SECS);

    match interval {
        ExportInterval::Hour => {
            let hour_end = Utc
                .with_ymd_and_hms(
                    delayed.year(),
                    delayed.month(),
                    delayed.day(),
                    delayed.hour(),
                    0,
                    0,
                )
                .single()
                .ok_or_else(|| anyhow!("invalid hour boundary from {}", delayed))?;
            Ok(hour_end - Duration::microseconds(1))
        }
        ExportInterval::Day => {
            let day_start = Utc
                .with_ymd_and_hms(delayed.year(), delayed.month(), delayed.day(), 0, 0, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid day boundary from {}", delayed))?;
            Ok(day_start - Duration::microseconds(1))
        }
    }
}

fn next_hour_tick(now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let this_hour_tick = Utc
        .with_ymd_and_hms(now.year(), now.month(), now.day(), now.hour(), 1, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid hour tick from {}", now))?;
    if now <= this_hour_tick {
        return Ok(this_hour_tick);
    }

    let next_hour = now + Duration::hours(1);
    Utc.with_ymd_and_hms(
        next_hour.year(),
        next_hour.month(),
        next_hour.day(),
        next_hour.hour(),
        1,
        0,
    )
    .single()
    .ok_or_else(|| anyhow!("invalid next hour tick from {}", now))
}

fn bundle_window_archive(stage_dir: &Path, archive_path: &Path) -> Result<()> {
    let files = [
        "order_updates_unmatched.parquet",
        "trade_updates_unmatched.parquet",
        "uniform_orders.parquet",
    ];

    let tar_gz = fs::File::create(archive_path)
        .with_context(|| format!("failed to create {}", archive_path.display()))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);

    for name in files {
        let path = stage_dir.join(name);
        if !path.exists() {
            return Err(anyhow!("missing export file {}", path.display()));
        }
        tar.append_path_with_name(&path, name)
            .with_context(|| format!("failed to add {}", path.display()))?;
    }

    tar.finish().context("failed to finish tar archive")?;
    let enc = tar.into_inner().context("failed to finalize tar writer")?;
    enc.finish().context("failed to finalize gzip stream")?;
    Ok(())
}

fn i64_to_u64_ts(input: i64, field: &str) -> Result<u64> {
    u64::try_from(input).map_err(|_| anyhow!("{} is negative: {}", field, input))
}

fn ensure_absolute(field: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(anyhow!("{} must be absolute: {}", field, path.display()));
    }
    Ok(())
}
