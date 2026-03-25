use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Utc};
use clap::Parser;
use log::info;
use mkt_signal::persist_manager::{self, exporter, RocksDbStore};

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    let resolved = ResolvedArgs::from_args(args)?;

    info!(
        "export env_name={} date={} input_dir={} output_dir={}",
        resolved.env_name,
        resolved.date.format("%Y-%m-%d"),
        resolved.input_dir.display(),
        resolved.output_dir.display()
    );

    let cf_names = persist_manager::required_column_families();
    let tuning = persist_manager::default_tuning();
    let store = RocksDbStore::open_read_only_with_tuning(
        &resolved.input_dir.to_string_lossy(),
        &cf_names,
        &tuning,
    )?;

    let (start_us, end_us) = utc_day_bounds_us(resolved.date)?;
    exporter::export_window_to_dir(&store, &resolved.output_dir, start_us, end_us)?;

    info!(
        "export finished env_name={} date={} window={}..{}",
        resolved.env_name,
        resolved.date.format("%Y-%m-%d"),
        start_us,
        end_us
    );
    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    name = "order_export",
    about = "Export one UTC day of MM order parquet files from RocksDB"
)]
struct Args {
    /// Base directory that contains env folders like /home/ubuntu/binance_mm_alpha
    #[arg(long)]
    base_dir: Option<PathBuf>,

    /// MM env name, for example binance_mm_alpha. If omitted, try to infer from cwd.
    #[arg(long)]
    env_name: Option<String>,

    /// UTC date to export, in YYYY-MM-DD format.
    #[arg(long, required = true, value_parser = parse_utc_date)]
    date: NaiveDate,

    /// Override RocksDB directory. Relative paths resolve under base_dir.
    #[arg(long)]
    input_dir: Option<PathBuf>,

    /// Override output root directory. Relative paths resolve under base_dir.
    #[arg(long)]
    output_root: Option<PathBuf>,
}

#[derive(Debug)]
struct ResolvedArgs {
    env_name: String,
    date: NaiveDate,
    input_dir: PathBuf,
    output_dir: PathBuf,
}

impl ResolvedArgs {
    fn from_args(args: Args) -> Result<Self> {
        let base_dir = args
            .base_dir
            .or_else(|| env_path_multi(&["ORDER_EXPORT_BASE_DIR", "PERSIST_EXPORT_BASE_DIR"]))
            .ok_or_else(|| {
                anyhow!("base_dir is required: set ORDER_EXPORT_BASE_DIR or pass --base-dir")
            })?;
        ensure_absolute("base_dir", &base_dir)?;

        let env_name =
            resolve_env_name(args.env_name.or_else(|| {
                env_string_multi(&["ORDER_EXPORT_ENV_NAME", "PERSIST_EXPORT_ENV_NAME"])
            }))?;
        validate_binance_mm_env_name(&env_name)?;

        let date = args.date;
        let input_dir = resolve_path(
            &base_dir,
            args.input_dir.or_else(|| {
                env_path_multi(&["ORDER_EXPORT_INPUT_DIR", "PERSIST_EXPORT_INPUT_DIR"])
            }),
            Path::new(&env_name).join("data").join("persist_manager"),
        );
        let output_root = resolve_path(
            &base_dir,
            args.output_root.or_else(|| {
                env_path_multi(&["ORDER_EXPORT_OUTPUT_ROOT", "PERSIST_EXPORT_OUTPUT_ROOT"])
            }),
            Path::new("exporter_data").join(&env_name),
        );
        let output_dir = output_root.join(date.format("%Y%m%d").to_string());

        if !input_dir.exists() {
            return Err(anyhow!("input_dir does not exist: {}", input_dir.display()));
        }

        Ok(Self {
            env_name,
            date,
            input_dir,
            output_dir,
        })
    }
}

fn ensure_absolute(field: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(anyhow!("{} must be absolute: {}", field, path.display()));
    }
    Ok(())
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_string_multi(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| env_string(key))
}

fn env_path_multi(keys: &[&str]) -> Option<PathBuf> {
    env_string_multi(keys).map(PathBuf::from)
}

fn resolve_env_name(value: Option<String>) -> Result<String> {
    if let Some(name) = value {
        return Ok(name.to_ascii_lowercase());
    }

    let cwd = std::env::current_dir().context("failed to resolve current directory")?;
    let name = cwd
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or_else(|| anyhow!("--env-name is required when cwd cannot be inferred"))?;
    Ok(name.to_ascii_lowercase())
}

fn validate_binance_mm_env_name(env_name: &str) -> Result<()> {
    let Some(suffix) = env_name.strip_prefix("binance_mm_") else {
        return Err(anyhow!(
            "env_name must match binance_mm_<suffix> (got: {})",
            env_name
        ));
    };
    if suffix.is_empty()
        || !suffix
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
    {
        return Err(anyhow!(
            "env_name must match binance_mm_<suffix> (got: {})",
            env_name
        ));
    }
    Ok(())
}

fn resolve_path(
    base_dir: &Path,
    override_path: Option<PathBuf>,
    default_relative: PathBuf,
) -> PathBuf {
    match override_path {
        Some(path) if path.is_absolute() => path,
        Some(path) => base_dir.join(path),
        None => base_dir.join(default_relative),
    }
}

fn parse_utc_date(input: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|err| format!("invalid UTC date '{}': {}", input, err))
}

fn utc_day_bounds_us(date: NaiveDate) -> Result<(u64, u64)> {
    let start = Utc
        .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid UTC date {}", date))?;
    let end = start + Duration::days(1) - Duration::microseconds(1);
    let start_us = u64::try_from(start.timestamp_micros())
        .map_err(|_| anyhow!("negative start timestamp for {}", date))?;
    let end_us = u64::try_from(end.timestamp_micros())
        .map_err(|_| anyhow!("negative end timestamp for {}", date))?;
    Ok((start_us, end_us))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_binance_mm_env_name_accepts_expected_values() {
        assert!(validate_binance_mm_env_name("binance_mm_alpha").is_ok());
        assert!(validate_binance_mm_env_name("binance_mm_beta-1").is_ok());
    }

    #[test]
    fn validate_binance_mm_env_name_rejects_other_patterns() {
        assert!(validate_binance_mm_env_name("binance_mm").is_err());
        assert!(validate_binance_mm_env_name("okex_mm_alpha").is_err());
        assert!(validate_binance_mm_env_name("binance_mm_ALPHA").is_err());
    }

    #[test]
    fn utc_day_bounds_cover_full_day() {
        let (start_us, end_us) =
            utc_day_bounds_us(NaiveDate::from_ymd_opt(2026, 3, 25).unwrap()).unwrap();
        assert_eq!(start_us, 1_774_396_800_000_000);
        assert_eq!(end_us, 1_774_483_199_999_999);
    }
}
