pub mod config;
mod execution;
mod http;
mod iceoryx;
mod signal;
mod storage;
mod um_order_update;

use std::sync::Arc;

use anyhow::Result;
use log::{info, warn};

use config::PersistManagerCfg;
use execution::ExecutionPersistor;
use signal::SignalPersistor;
use storage::RocksDbStore;
use um_order_update::UmOrderUpdatePersistor;

pub struct PersistManager {
    cfg: PersistManagerCfg,
}

impl PersistManager {
    pub fn new(cfg: PersistManagerCfg) -> Self {
        Self { cfg }
    } 

    pub async fn run(self) -> Result<()> {
        let cfg = self.cfg;
        let signal_enabled = cfg.signal.enabled;
        let http_cfg = cfg.http.clone();
        let mut cf_names: Vec<&'static str> = Vec::new();
        let execution_enabled = cfg.execution.enabled;
        let um_execution_enabled = cfg.um_execution.enabled;
        if signal_enabled {
            cf_names.extend_from_slice(signal::required_column_families());
        }
        if execution_enabled {
            cf_names.extend_from_slice(execution::required_column_families());
        }
        if um_execution_enabled {
            cf_names.extend_from_slice(um_order_update::required_column_families());
        } 

        let store = Arc::new(RocksDbStore::open(
            &cfg.rocksdb.path,
            &cf_names,
            cfg.rocksdb.sync_writes,
        )?); 

        if signal_enabled { 
            info!(
                "signal persistence enabled on channel {}",
                cfg.signal.channel 
            ); 
            let worker = SignalPersistor::new(cfg.signal.clone(), store.clone())?;
            tokio::task::spawn_local(async move {
                if let Err(err) = worker.run().await {
                    warn!("signal persistor exited with error: {err:#}");
                } 
            }); 
        } else {
            info!("signal persistence disabled by configuration");
        } 

        if execution_enabled {
            info!(
                "execution persistence enabled on channel {}",
                cfg.execution.channel
            );
            let worker = ExecutionPersistor::new(cfg.execution.clone(), store.clone())?;
            tokio::task::spawn_local(async move {
                if let Err(err) = worker.run().await {
                    warn!("execution persistor exited with error: {err:#}");
                }
            });
        } else {
            info!("execution persistence disabled by configuration");
        }

        if um_execution_enabled {
            info!(
                "UM order update persistence enabled on channel {}",
                cfg.um_execution.channel 
            );
            let worker = UmOrderUpdatePersistor::new(cfg.um_execution.clone(), store.clone())?;
            tokio::task::spawn_local(async move {
                if let Err(err) = worker.run().await {
                    warn!("UM order update persistor exited with error: {err:#}");
                }
            });
        } else {
            info!("UM order update persistence disabled by configuration");
        } 

        let http_store = store.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = http::serve(http_cfg, http_store).await {
                warn!("persist_manager http server exited: {err:#}");
            }
        });

        tokio::signal::ctrl_c().await?;
        info!("persist_manager received shutdown signal, exiting");
        Ok(())
    }
}

pub use signal::required_column_families as signal_required_column_families;
pub use storage::RocksDbConfig;
