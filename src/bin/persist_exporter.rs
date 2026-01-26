use anyhow::{anyhow, Context, Result};
use clap::Parser;
use mkt_signal::persist_manager::{self, exporter, RocksDbStore};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();

    let config_path = resolve_config_path()?;
    let config_text = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: ExporterConfig =
        toml::from_str(&config_text).context("failed to parse config")?;
    let target = config
        .targets
        .iter()
        .find(|t| t.name == args.target)
        .ok_or_else(|| {
            let names = config
                .targets
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            anyhow!("target not found: {} (available: {})", args.target, names)
        })?;

    let input_dir = Path::new(&target.input_dir);
    ensure_absolute("input_dir", input_dir)?;
    if !input_dir.exists() {
        return Err(anyhow!(
            "input_dir does not exist: {}",
            input_dir.display()
        ));
    }

    let output_dir = Path::new(&target.output_dir);
    ensure_absolute("output_dir", output_dir)?;

    let cf_names = persist_manager::required_column_families();
    let tuning = persist_manager::default_tuning();
    let store = RocksDbStore::open_read_only_with_tuning(
        &target.input_dir,
        &cf_names,
        &tuning,
    )?;

    exporter::export_all_to_dir(&store, output_dir)?;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "persist_exporter")]
struct Args {
    /// 目标配置名称
    #[arg(long)]
    target: String,
}

#[derive(Debug, Deserialize)]
struct ExporterConfig {
    targets: Vec<ExportTarget>,
}

#[derive(Debug, Deserialize)]
struct ExportTarget {
    name: String,
    input_dir: String,
    output_dir: String,
}

fn ensure_absolute(field: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(anyhow!("{} must be absolute: {}", field, path.display()));
    }
    Ok(())
}

fn resolve_config_path() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("failed to resolve current executable path")?;
    let exe_dir = exe
        .parent()
        .ok_or_else(|| anyhow!("failed to resolve executable directory"))?;
    let primary = exe_dir.join("persist_exporter.toml");
    if primary.exists() {
        return Ok(primary);
    }

    let fallback = PathBuf::from("config/persist_exporter.toml");
    if fallback.exists() {
        return Ok(fallback);
    }

    Err(anyhow!(
        "persist_exporter.toml not found (searched {}, {})",
        primary.display(),
        fallback.display()
    ))
}
