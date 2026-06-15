mod bbo_spread;
pub mod exporter;
mod iceoryx;
mod order_update;
pub mod parquet;
mod polling;
pub mod read_server;
mod runtime_common;
mod storage;
pub mod sync;
mod trade_update;
pub mod unified_order;
mod uniform_order_persist;

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;
use log::info;

use bbo_spread::BboSpreadRuntime;
use order_update::{OrderUpdatePersistor, OrderUpdateUnmatchedPersistor};
use polling::PollStats;
use sync::{serve_sync_source, PersistSyncConfig};
use trade_update::{TradeUpdatePersistor, TradeUpdateUnmatchedPersistor};
use uniform_order_persist::{PendingUniformOrder, UniformOrderPersistor};

pub use storage::{RocksDbStore, RocksDbTuning};

// 固定配置（需要调整就改这里）
pub const DEFAULT_DB_PATH: &str = "data/persist_manager";
const ROCKSDB_SYNC_WRITES: bool = false; // 异步写入，性能更好
const ROCKSDB_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const ROCKSDB_DB_WRITE_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const ROCKSDB_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const ROCKSDB_MAX_WRITE_BUFFER_NUMBER: i32 = 2;

pub fn required_column_families() -> Vec<&'static str> {
    let mut cf_names: Vec<&'static str> = Vec::new();
    cf_names.extend_from_slice(trade_update::required_column_families());
    cf_names.extend_from_slice(order_update::required_column_families());
    cf_names.extend_from_slice(uniform_order_persist::required_column_families());
    cf_names
}

pub fn default_tuning() -> RocksDbTuning {
    RocksDbTuning {
        block_cache_bytes: Some(ROCKSDB_BLOCK_CACHE_BYTES),
        write_buffer_size_bytes: Some(ROCKSDB_WRITE_BUFFER_BYTES),
        db_write_buffer_size_bytes: Some(ROCKSDB_DB_WRITE_BUFFER_BYTES),
        max_write_buffer_number: Some(ROCKSDB_MAX_WRITE_BUFFER_NUMBER),
    }
}

pub struct PersistManager {}

impl Default for PersistManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistManager {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(self) -> Result<()> {
        let mut cf_names = required_column_families();
        cf_names.extend_from_slice(sync::sync_column_families());
        let tuning = default_tuning();
        let sync_config = PersistSyncConfig::from_env()?;
        let sync_enabled = sync_config.as_ref().is_some_and(PersistSyncConfig::enabled);

        // 打开 RocksDB
        let store = Arc::new(RocksDbStore::open_with_tuning(
            DEFAULT_DB_PATH,
            &cf_names,
            ROCKSDB_SYNC_WRITES,
            &tuning,
        )?);

        if let Some(config) = sync_config.clone() {
            if let Some(addr) = config.bind_addr {
                let sync_store = store.clone();
                tokio::task::spawn_local(async move {
                    if let Err(err) = serve_sync_source(sync_store, addr, config.source_id).await {
                        log::error!("persist sync source exited: {err:#}");
                    }
                });
            } else if sync_enabled {
                info!("persist sync outbox enabled without source server bind");
            }
        }

        let bbo_runtime = BboSpreadRuntime::start_from_env().await;

        info!("starting trade update persistor");
        let trade_update = TradeUpdatePersistor::new(store.clone(), sync_enabled)?;

        info!("starting trade update unmatched persistor");
        let trade_update_unmatched =
            TradeUpdateUnmatchedPersistor::new(store.clone(), sync_enabled)?;

        info!("starting order update persistor");
        let order_update = OrderUpdatePersistor::new(store.clone(), sync_enabled)?;

        info!("starting order update unmatched persistor");
        let order_update_unmatched =
            OrderUpdateUnmatchedPersistor::new(store.clone(), sync_enabled)?;

        info!("starting uniform order persistor");
        let uniform_order = if let Some(runtime) = bbo_runtime {
            UniformOrderPersistor::new_with_bbo_spread(
                store.clone(),
                runtime.store,
                runtime.enrich_delay,
                sync_enabled,
            )?
        } else {
            UniformOrderPersistor::new(store.clone(), sync_enabled)?
        };
        tokio::task::spawn_local(async move {
            run_persistors(
                trade_update,
                trade_update_unmatched,
                order_update,
                order_update_unmatched,
                uniform_order,
            )
            .await;
        });

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("persist_manager shutdown");
                Ok(())
            }
        }
    }
}

async fn run_persistors(
    trade_update: TradeUpdatePersistor,
    trade_update_unmatched: TradeUpdateUnmatchedPersistor,
    order_update: OrderUpdatePersistor,
    order_update_unmatched: OrderUpdateUnmatchedPersistor,
    uniform_order: UniformOrderPersistor,
) {
    info!(
        "persistors polling unified: max_drain_per_channel={} idle_sleep_ms={}",
        polling::MAX_DRAIN_PER_CHANNEL,
        polling::idle_sleep().as_millis()
    );
    let mut uniform_pending: VecDeque<PendingUniformOrder> = VecDeque::new();

    loop {
        let mut stats = PollStats::default();
        stats.merge(trade_update.poll_available());
        stats.merge(trade_update_unmatched.poll_available());
        stats.merge(order_update.poll_available());
        stats.merge(order_update_unmatched.poll_available());
        stats.merge(uniform_order.poll_available(&mut uniform_pending));

        if stats.receive_error {
            tokio::time::sleep(polling::receive_error_sleep()).await;
        } else if stats.received == 0 {
            tokio::time::sleep(uniform_order_persist::next_idle_sleep(&uniform_pending)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}
