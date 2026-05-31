use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::persist_manager::sync::{
    run_collector, run_multi_collector, run_verify_config, run_verify_recent, CollectorArgs,
    MultiCollectorConfig, VerifyArgs, VerifyConfigArgs,
};
use std::path::PathBuf;

const DEFAULT_CENTER_DB_PATH: &str = "data/persist_sync_center";
const DEFAULT_BATCH_RECORDS: usize = 1_000;
const DEFAULT_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_BUCKET_US: u64 = 60_000_000;

#[derive(Parser, Debug)]
#[command(name = "persist_sync_collector")]
#[command(about = "Pull persist RocksDB records from manager sync sources")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Continuously pull manager outbox records into local center RocksDB.
    Run {
        /// Stable source id of the manager.
        #[arg(long)]
        source_id: String,

        /// Manager gRPC endpoint, for example http://10.77.0.1:50051.
        #[arg(long)]
        manager: String,

        /// Local center RocksDB path.
        #[arg(long, default_value = DEFAULT_CENTER_DB_PATH)]
        center_db: String,

        /// Max records per streamed batch.
        #[arg(long, default_value_t = DEFAULT_BATCH_RECORDS)]
        batch_records: usize,

        /// Max bytes per streamed batch.
        #[arg(long, default_value_t = DEFAULT_BATCH_BYTES)]
        batch_bytes: usize,

        /// Use RocksDB sync writes for center DB.
        #[arg(long, default_value_t = false)]
        sync_writes: bool,
    },

    /// Continuously pull all configured manager sources into one local center RocksDB.
    RunAll {
        /// TOML config with center_db and manager sources. Each source uses id + url.
        #[arg(long)]
        config: PathBuf,
    },

    /// Compare all configured sources and optionally repair missing/mismatched records.
    VerifyAll {
        /// TOML config with center_db and manager sources. Each source uses id + url.
        #[arg(long)]
        config: PathBuf,

        /// Limit verification to one source id from config.
        #[arg(long)]
        id: Option<String>,

        /// Lookback window in hours ending at now. Ignored if start-us/end-us are set.
        #[arg(long, default_value_t = 24)]
        hours: u64,

        /// Explicit start timestamp in microseconds.
        #[arg(long)]
        start_us: Option<u64>,

        /// Explicit inclusive end timestamp in microseconds.
        #[arg(long)]
        end_us: Option<u64>,

        /// Bucket size in microseconds.
        #[arg(long, default_value_t = DEFAULT_BUCKET_US)]
        bucket_us: u64,

        /// Fetch and repair missing/mismatched records.
        #[arg(long, default_value_t = false)]
        repair: bool,
    },

    /// Compare recent buckets and optionally repair missing/mismatched records.
    Verify {
        /// Stable source id of the manager.
        #[arg(long)]
        source_id: String,

        /// Manager gRPC endpoint, for example http://10.77.0.1:50051.
        #[arg(long)]
        manager: String,

        /// Local center RocksDB path.
        #[arg(long, default_value = DEFAULT_CENTER_DB_PATH)]
        center_db: String,

        /// Lookback window in hours ending at now. Ignored if start-us/end-us are set.
        #[arg(long, default_value_t = 24)]
        hours: u64,

        /// Explicit start timestamp in microseconds.
        #[arg(long)]
        start_us: Option<u64>,

        /// Explicit inclusive end timestamp in microseconds.
        #[arg(long)]
        end_us: Option<u64>,

        /// Bucket size in microseconds.
        #[arg(long, default_value_t = DEFAULT_BUCKET_US)]
        bucket_us: u64,

        /// Limit verification to selected CF names. Defaults to order_export CFs. Can be repeated.
        #[arg(long = "cf")]
        cf_names: Vec<String>,

        /// Fetch and repair missing/mismatched records.
        #[arg(long, default_value_t = false)]
        repair: bool,

        /// Use RocksDB sync writes for center DB.
        #[arg(long, default_value_t = false)]
        sync_writes: bool,
    },
}

fn resolve_window_us(hours: u64, start_us: Option<u64>, end_us: Option<u64>) -> Result<(u64, u64)> {
    let now_us = get_timestamp_us() as u64;
    let end_us = end_us.unwrap_or(now_us);
    let start_us = match start_us {
        Some(value) => value,
        None => {
            let lookback = hours
                .checked_mul(3_600_000_000)
                .context("hours lookback overflow")?;
            end_us.saturating_sub(lookback)
        }
    };
    Ok((start_us, end_us))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    let args = Args::parse();
    let local = tokio::task::LocalSet::new();
    local.run_until(async move {
        match args.command {
            Command::Run {
                source_id,
                manager,
                center_db,
                batch_records,
                batch_bytes,
                sync_writes,
            } => {
                run_collector(CollectorArgs {
                    source_id,
                    manager_endpoint: manager,
                    center_db_path: center_db,
                    batch_records,
                    batch_bytes,
                    sync_writes,
                })
                .await
            }
            Command::RunAll { config } => {
                let raw = std::fs::read_to_string(&config)
                    .with_context(|| format!("failed to read {}", config.display()))?;
                let config: MultiCollectorConfig = toml::from_str(&raw)
                    .with_context(|| format!("failed to parse {}", config.display()))?;
                run_multi_collector(config).await
            }
            Command::VerifyAll {
                config,
                id,
                hours,
                start_us,
                end_us,
                bucket_us,
                repair,
            } => {
                let raw = std::fs::read_to_string(&config)
                    .with_context(|| format!("failed to read {}", config.display()))?;
                let config: MultiCollectorConfig = toml::from_str(&raw)
                    .with_context(|| format!("failed to parse {}", config.display()))?;
                let (start_us, end_us) = resolve_window_us(hours, start_us, end_us)?;
                let reports = run_verify_config(VerifyConfigArgs {
                    config,
                    only_id: id,
                    start_us,
                    end_us,
                    bucket_us,
                    repair,
                })
                .await?;
                for (id, report) in reports {
                    println!(
                        "id={} matched_buckets={} mismatched_buckets={} missing_records={} mismatched_records={} repaired_records={}",
                        id,
                        report.matched_buckets,
                        report.mismatched_buckets,
                        report.missing_records,
                        report.mismatched_records,
                        report.repaired_records
                    );
                }
                Ok(())
            }
            Command::Verify {
                source_id,
                manager,
                center_db,
                hours,
                start_us,
                end_us,
                bucket_us,
                cf_names,
                repair,
                sync_writes,
            } => {
                let (start_us, end_us) = resolve_window_us(hours, start_us, end_us)?;
                let report = run_verify_recent(VerifyArgs {
                    source_id,
                    manager_endpoint: manager,
                    center_db_path: center_db,
                    cf_names,
                    start_us,
                    end_us,
                    bucket_us,
                    repair,
                    sync_writes,
                })
                .await?;
                println!(
                    "matched_buckets={} mismatched_buckets={} missing_records={} mismatched_records={} repaired_records={}",
                    report.matched_buckets,
                    report.mismatched_buckets,
                    report.missing_records,
                    report.mismatched_records,
                    report.repaired_records
                );
                Ok(())
            }
        }
    })
    .await
}
