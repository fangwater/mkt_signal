use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc,
};
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
        "export env_name={} window={} input_dir={} output_dir={}",
        resolved.env_name,
        resolved.window_label,
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

    exporter::export_window_to_dir(
        &store,
        &resolved.output_dir,
        resolved.start_us,
        resolved.end_us,
    )?;

    info!(
        "export finished env_name={} window={} bounds={}..{}",
        resolved.env_name, resolved.window_label, resolved.start_us, resolved.end_us
    );
    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    name = "order_export",
    about = "Export UTC day or time-range order parquet files from RocksDB"
)]
struct Args {
    /// Base directory that contains env folders like /home/ubuntu/binance_mm_alpha
    /// or /home/ubuntu/binance_fr_trade01
    #[arg(long)]
    base_dir: Option<PathBuf>,

    /// Env name, for example binance_mm_alpha or binance_fr_trade01.
    /// If omitted, try to infer from cwd.
    #[arg(long)]
    env_name: Option<String>,

    /// UTC date to export, in YYYY-MM-DD format.
    #[arg(long, value_parser = parse_utc_date, conflicts_with_all = ["start", "end"])]
    date: Option<NaiveDate>,

    /// UTC start timestamp to export, for example 2026-03-25T01:02:03Z.
    #[arg(long, value_parser = parse_utc_datetime, requires = "end", conflicts_with = "date")]
    start: Option<DateTime<Utc>>,

    /// UTC end timestamp to export, inclusive, for example 2026-03-25T02:03:04Z.
    #[arg(long, value_parser = parse_utc_datetime, requires = "start", conflicts_with = "date")]
    end: Option<DateTime<Utc>>,

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
    window_label: String,
    start_us: u64,
    end_us: u64,
    input_dir: PathBuf,
    output_dir: PathBuf,
}

#[derive(Debug)]
struct ExportWindow {
    label: String,
    start_us: u64,
    end_us: u64,
    output_dir_name: String,
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
        validate_supported_env_name(&env_name)?;

        let window = ExportWindow::from_cli_inputs(args.date, args.start, args.end)?;
        let input_dir = resolve_path(
            &base_dir,
            args.input_dir.or_else(|| {
                env_path_multi(&["ORDER_EXPORT_INPUT_DIR", "PERSIST_EXPORT_INPUT_DIR"])
            }),
            Path::new(&env_name).join("data").join("persist_manager"),
        );
        let output_root = match args
            .output_root
            .or_else(|| env_path_multi(&["ORDER_EXPORT_OUTPUT_ROOT", "PERSIST_EXPORT_OUTPUT_ROOT"]))
        {
            Some(path) => resolve_path(&base_dir, Some(path), PathBuf::new()),
            None => std::env::current_dir().context("failed to resolve current directory")?,
        };
        let output_dir = output_root.join(&window.output_dir_name);

        if !input_dir.exists() {
            return Err(anyhow!("input_dir does not exist: {}", input_dir.display()));
        }

        Ok(Self {
            env_name,
            window_label: window.label,
            start_us: window.start_us,
            end_us: window.end_us,
            input_dir,
            output_dir,
        })
    }
}

impl ExportWindow {
    fn from_cli_inputs(
        date: Option<NaiveDate>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        match (date, start, end) {
            (Some(date), None, None) => Self::for_date(date),
            (None, Some(start), Some(end)) => Self::for_explicit_range(start, end),
            (None, None, None) => Err(anyhow!("either --date or --start/--end is required")),
            _ => Err(anyhow!("use either --date or both --start and --end")),
        }
    }

    fn for_date(date: NaiveDate) -> Result<Self> {
        let (start_us, end_us) = utc_day_bounds_us(date)?;
        Ok(Self {
            label: date.format("%Y-%m-%d").to_string(),
            start_us,
            end_us,
            output_dir_name: date.format("%Y%m%d").to_string(),
        })
    }

    fn for_explicit_range(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Self> {
        let start_us = timestamp_to_micros(start, "start")?;
        let end_us = timestamp_to_micros(end, "end")?;
        if end_us < start_us {
            return Err(anyhow!(
                "--end must be greater than or equal to --start (got {} < {})",
                format_utc_datetime(end),
                format_utc_datetime(start)
            ));
        }

        Ok(Self {
            label: format!(
                "{}..{}",
                format_utc_datetime(start),
                format_utc_datetime(end)
            ),
            start_us,
            end_us,
            output_dir_name: format!(
                "{}__{}",
                format_output_dir_component(start),
                format_output_dir_component(end)
            ),
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

fn is_valid_env_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && suffix
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
}

fn is_valid_exchange_name(exchange: &str) -> bool {
    !exchange.is_empty()
        && exchange
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

fn validate_supported_env_name(env_name: &str) -> Result<()> {
    if let Some((exchange, suffix)) = env_name.split_once("_mm_") {
        if is_valid_exchange_name(exchange) && is_valid_env_suffix(suffix) {
            return Ok(());
        }
    }

    if let Some((exchange, suffix)) = env_name.split_once("_fr_") {
        if is_valid_exchange_name(exchange) && is_valid_env_suffix(suffix) {
            return Ok(());
        }
    }

    Err(anyhow!(
        "env_name must match <exchange>_mm_<suffix> or <exchange>_fr_<suffix> (got: {})",
        env_name
    ))
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

fn parse_utc_datetime(input: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(input) {
        return Ok(parsed.with_timezone(&Utc));
    }

    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(input, fmt) {
            return Ok(Utc.from_utc_datetime(&parsed));
        }
    }

    Err(format!(
        "invalid UTC timestamp '{}': expected RFC3339 like 2026-03-25T01:02:03Z",
        input
    ))
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

fn timestamp_to_micros(ts: DateTime<Utc>, field: &str) -> Result<u64> {
    u64::try_from(ts.timestamp_micros())
        .map_err(|_| anyhow!("negative {} timestamp {}", field, format_utc_datetime(ts)))
}

fn format_utc_datetime(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn format_output_dir_component(ts: DateTime<Utc>) -> String {
    format_utc_datetime(ts).replace('-', "").replace(':', "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_supported_env_name_accepts_mm_values() {
        assert!(validate_supported_env_name("binance_mm_alpha").is_ok());
        assert!(validate_supported_env_name("binance_mm_beta-1").is_ok());
        assert!(validate_supported_env_name("okex_mm_alpha").is_ok());
        assert!(validate_supported_env_name("gate_mm_trade02").is_ok());
        assert!(validate_supported_env_name("bybit_mm_hf01").is_ok());
    }

    #[test]
    fn validate_supported_env_name_accepts_fr_values() {
        assert!(validate_supported_env_name("binance_fr_trade01").is_ok());
        assert!(validate_supported_env_name("okex_fr_hf01").is_ok());
        assert!(validate_supported_env_name("gate_fr_trade02").is_ok());
    }

    #[test]
    fn validate_supported_env_name_rejects_other_patterns() {
        assert!(validate_supported_env_name("binance_mm").is_err());
        assert!(validate_supported_env_name("okex_mm_").is_err());
        assert!(validate_supported_env_name("_mm_alpha").is_err());
        assert!(validate_supported_env_name("binance_mm_ALPHA").is_err());
        assert!(validate_supported_env_name("binance_fr").is_err());
        assert!(validate_supported_env_name("binance_fr_ALPHA").is_err());
    }

    #[test]
    fn utc_day_bounds_cover_full_day() {
        let (start_us, end_us) =
            utc_day_bounds_us(NaiveDate::from_ymd_opt(2026, 3, 25).unwrap()).unwrap();
        assert_eq!(start_us, 1_774_396_800_000_000);
        assert_eq!(end_us, 1_774_483_199_999_999);
    }

    #[test]
    fn export_window_accepts_date_mode() {
        let window = ExportWindow::from_cli_inputs(
            Some(NaiveDate::from_ymd_opt(2026, 3, 25).unwrap()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(window.label, "2026-03-25");
        assert_eq!(window.start_us, 1_774_396_800_000_000);
        assert_eq!(window.end_us, 1_774_483_199_999_999);
        assert_eq!(window.output_dir_name, "20260325");
    }

    #[test]
    fn export_window_accepts_explicit_time_range() {
        let start = parse_utc_datetime("2026-03-25T01:02:03Z").unwrap();
        let end = parse_utc_datetime("2026-03-25T02:03:04.123456Z").unwrap();
        let window = ExportWindow::from_cli_inputs(None, Some(start), Some(end)).unwrap();

        assert_eq!(
            window.label,
            "2026-03-25T01:02:03.000000Z..2026-03-25T02:03:04.123456Z"
        );
        assert_eq!(window.start_us, 1_774_400_523_000_000);
        assert_eq!(window.end_us, 1_774_404_184_123_456);
        assert_eq!(
            window.output_dir_name,
            "20260325T010203.000000Z__20260325T020304.123456Z"
        );
    }

    #[test]
    fn export_window_rejects_missing_mode() {
        let err = ExportWindow::from_cli_inputs(None, None, None).unwrap_err();
        assert!(err
            .to_string()
            .contains("either --date or --start/--end is required"));
    }

    #[test]
    fn export_window_rejects_reverse_range() {
        let start = parse_utc_datetime("2026-03-25T02:03:04Z").unwrap();
        let end = parse_utc_datetime("2026-03-25T01:02:03Z").unwrap();
        let err = ExportWindow::from_cli_inputs(None, Some(start), Some(end)).unwrap_err();
        assert!(err
            .to_string()
            .contains("--end must be greater than or equal to --start"));
    }

    #[test]
    fn parse_utc_datetime_accepts_naive_utc_input() {
        let parsed = parse_utc_datetime("2026-03-25 01:02:03.123456").unwrap();
        assert_eq!(
            parsed.to_rfc3339_opts(SecondsFormat::Micros, true),
            "2026-03-25T01:02:03.123456Z"
        );
    }
}
