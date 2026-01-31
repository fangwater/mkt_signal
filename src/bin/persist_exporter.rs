use log::info;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use mkt_signal::persist_manager::{self, exporter, RocksDbStore};
use tar::Builder;

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();

    let exe_dir = resolve_exe_dir()?;
    let config_path = resolve_config_path(&exe_dir)?;
    let config_text = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: ExporterConfig =
        toml::from_str(&config_text).context("failed to parse config")?;
    if config.targets.is_empty() {
        return Err(anyhow!("no targets configured in {}", config_path.display()));
    }

    let selected = if let Some(name) = args.target.as_deref() {
        let target = config
            .targets
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| {
                let names = config
                    .targets
                    .iter()
                    .map(|t| t.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow!("target not found: {} (available: {})", name, names)
            })?;
        vec![target]
    } else {
        config.targets.iter().collect::<Vec<_>>()
    };

    for target in selected {
        info!(
            "export target={} input_dir={} output_dir={}",
            target.name, target.input_dir, target.output_dir
        );
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
        bundle_export_artifacts(target, output_dir, &exe_dir)?;
        info!("export finished target={}", target.name);
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "persist_exporter")]
struct Args {
    /// 目标配置名称（不填则导出所有 target）
    #[arg(long)]
    target: Option<String>,
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

fn resolve_exe_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("failed to resolve current executable path")?;
    let exe_dir = exe
        .parent()
        .ok_or_else(|| anyhow!("failed to resolve executable directory"))?;
    Ok(exe_dir.to_path_buf())
}

fn resolve_config_path(exe_dir: &Path) -> Result<PathBuf> {
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

fn resolve_artifact(exe_dir: &Path, fallback: &Path, label: &str) -> Result<PathBuf> {
    let primary = exe_dir.join(
        fallback
            .file_name()
            .ok_or_else(|| anyhow!("invalid artifact name for {}", label))?,
    );
    if primary.exists() {
        return Ok(primary);
    }
    if fallback.exists() {
        info!(
            "artifact {} not found in {}, fallback to {}",
            label,
            exe_dir.display(),
            fallback.display()
        );
        return Ok(fallback.to_path_buf());
    }
    Err(anyhow!(
        "artifact {} not found (searched {}, {})",
        label,
        primary.display(),
        fallback.display()
    ))
}

fn bundle_export_artifacts(
    target: &ExportTarget,
    output_dir: &Path,
    exe_dir: &Path,
) -> Result<()> {
    let archive_path = output_dir.join(format!("{}.tar.gz", target.name));

    let parquet_files = [
        "signals_arb_open.parquet",
        "signals_arb_hedge.parquet",
        "signals_arb_cancel.parquet",
        "signals_arb_close.parquet",
        "order_updates.parquet",
        "trade_updates.parquet",
    ];

    let okx_cache = resolve_artifact(
        exe_dir,
        Path::new("scripts/okx_swap_multipliers.json"),
        "okx_swap_multipliers.json",
    )?;

    let tar_gz = fs::File::create(&archive_path)
        .with_context(|| format!("failed to create {}", archive_path.display()))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);

    for name in parquet_files {
        let path = output_dir.join(name);
        if !path.exists() {
            return Err(anyhow!("missing export file {}", path.display()));
        }
        tar.append_path_with_name(&path, name)
            .with_context(|| format!("failed to add {}", path.display()))?;
    }

    tar.append_path_with_name(&okx_cache, "okx_swap_multipliers.json")
        .with_context(|| format!("failed to add {}", okx_cache.display()))?;

    tar.finish().context("failed to finish tar archive")?;
    let enc = tar.into_inner().context("failed to finalize tar writer")?;
    enc.finish().context("failed to finalize gzip stream")?;
    info!("bundle created {}", archive_path.display());
    Ok(())
}
