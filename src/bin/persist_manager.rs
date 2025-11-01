use anyhow::Result;
use log::info;
use mkt_signal::persist_manager::{config::PersistManagerCfg, PersistManager};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let cfg_path = std::env::var("PERSIST_MANAGER_CFG")
        .unwrap_or_else(|_| "config/persist_manager.toml".to_string());
    let cfg = PersistManagerCfg::load(&cfg_path).await?;
    info!("persist_manager config loaded from {}", cfg_path);

    let manager = PersistManager::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(manager.run()).await
}
