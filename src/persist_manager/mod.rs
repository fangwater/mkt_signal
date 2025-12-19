mod http;
mod iceoryx;
mod order_update;
mod signal;
mod storage;
mod trade_update;

use std::sync::Arc;

use anyhow::Result;
use log::info;

use order_update::OrderUpdatePersistor;
use signal::SignalPersistor;
use storage::{RocksDbStore, RocksDbTuning};
use trade_update::TradeUpdatePersistor;

// 固定配置（需要调整就改这里）
const ROCKSDB_PATH: &str = "data/persist_manager";
const ROCKSDB_SYNC_WRITES: bool = false; // 异步写入，性能更好
const ROCKSDB_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const ROCKSDB_DB_WRITE_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const ROCKSDB_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const ROCKSDB_MAX_WRITE_BUFFER_NUMBER: i32 = 2;

pub struct PersistManager {
    port: u16,
}

impl PersistManager {
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    pub async fn run(self) -> Result<()> {
        // 收集所有列族
        let mut cf_names: Vec<&'static str> = Vec::new();
        cf_names.extend_from_slice(signal::required_column_families());
        cf_names.extend_from_slice(trade_update::required_column_families());
        cf_names.extend_from_slice(order_update::required_column_families());

        let tuning = RocksDbTuning {
            block_cache_bytes: Some(ROCKSDB_BLOCK_CACHE_BYTES),
            write_buffer_size_bytes: Some(ROCKSDB_WRITE_BUFFER_BYTES),
            db_write_buffer_size_bytes: Some(ROCKSDB_DB_WRITE_BUFFER_BYTES),
            max_write_buffer_number: Some(ROCKSDB_MAX_WRITE_BUFFER_NUMBER),
        };

        // 打开 RocksDB
        let store = Arc::new(RocksDbStore::open_with_tuning(
            ROCKSDB_PATH,
            &cf_names,
            ROCKSDB_SYNC_WRITES,
            &tuning,
        )?);

        // 启动所有持久化器
        info!("starting signal persistor");
        let s1 = SignalPersistor::new(store.clone())?;
        tokio::task::spawn_local(async move {
            let _ = s1.run().await;
        });

        info!("starting trade update persistor");
        let s2 = TradeUpdatePersistor::new(store.clone())?;
        tokio::task::spawn_local(async move {
            let _ = s2.run().await;
        });

        info!("starting order update persistor");
        let s3 = OrderUpdatePersistor::new(store.clone())?;
        tokio::task::spawn_local(async move {
            let _ = s3.run().await;
        });

        // 启动 HTTP 服务器（端口冲突要直接 panic）
        let bind_addr = format!("0.0.0.0:{}", self.port);
        info!("starting http server on {}", bind_addr);
        let store_for_http = store.clone();

        tokio::select! {
            res = http::serve(&bind_addr, store_for_http) => {
                match res {
                    Ok(()) => {
                        panic!("persist_manager http server exited unexpectedly");
                    }
                    Err(err) => {
                        panic!("persist_manager failed to start http server on {}: {:#}", bind_addr, err);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("persist_manager shutdown");
                Ok(())
            }
        }
    }
}
