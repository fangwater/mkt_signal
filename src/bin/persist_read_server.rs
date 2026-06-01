use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;

use mkt_signal::persist_manager::read_server::{PersistReadServer, PersistReadServerConfig};

#[derive(Parser, Debug)]
#[command(name = "persist_read_server")]
#[command(about = "Read-only RocksDB Secondary HTTP server for persisted order records")]
struct Args {
    #[arg(long, default_value = "config/persist.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct UnifiedPersistConfig {
    center_db: Option<PathBuf>,
    sources: Option<Vec<UnifiedSourceConfig>>,
    read_server: Option<PersistReadServerConfig>,
}

#[derive(Debug, Deserialize)]
struct UnifiedSourceConfig {
    id: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    let config = load_config(&args.config)?;
    PersistReadServer::new(config)?.run().await
}

fn load_config(path: &Path) -> Result<PersistReadServerConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let envelope: UnifiedPersistConfig =
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;

    let mut config = match envelope.read_server {
        Some(config) => config,
        None => {
            if envelope.center_db.is_some() || envelope.sources.is_some() {
                bail!(
                    "unified persist config {} must contain [read_server] to start persist_read_server",
                    path.display()
                );
            }
            toml::from_str(&raw).with_context(|| {
                format!(
                    "failed to parse {} as persist_read_server config",
                    path.display()
                )
            })?
        }
    };

    if config.enabled == Some(false) {
        bail!("read_server.enabled=false in {}", path.display());
    }

    if config.primary_dir.is_none() {
        config.primary_dir = envelope.center_db;
    }

    if config.source_id.is_none() {
        if let Some(sources) = envelope.sources {
            let mut ids = sources
                .into_iter()
                .map(|source| source.id.trim().to_string())
                .filter(|id| !id.is_empty())
                .collect::<Vec<_>>();
            ids.sort();
            ids.dedup();
            if ids.len() == 1 {
                config.source_id = ids.pop();
            } else if ids.len() > 1 {
                bail!(
                    "unified persist config {} has multiple sources; set read_server.source_id explicitly",
                    path.display()
                );
            }
        }
    }

    Ok(config)
}
