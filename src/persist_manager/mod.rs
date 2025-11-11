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
use storage::RocksDbStore;
use trade_update::TradeUpdatePersistor;

// 硬编码配置
const ROCKSDB_PATH: &str = "data/persist_manager";
const ROCKSDB_SYNC_WRITES: bool = false; // 异步写入，性能更好

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

        // 打开 RocksDB
        let store = Arc::new(RocksDbStore::open(
            ROCKSDB_PATH,
            &cf_names,
            ROCKSDB_SYNC_WRITES,
        )?);

        // 启动所有持久化器
        info!("starting signal persistor");
        let s1 = SignalPersistor::new(store.clone())?;
        tokio::task::spawn_local(async move { let _ = s1.run().await; });

        info!("starting trade update persistor");
        let s2 = TradeUpdatePersistor::new(store.clone())?;
        tokio::task::spawn_local(async move { let _ = s2.run().await; });

        info!("starting order update persistor");
        let s3 = OrderUpdatePersistor::new(store.clone())?;
        tokio::task::spawn_local(async move { let _ = s3.run().await; });

        // 启动 HTTP 服务器（使用命令行指定的端口）
        let bind_addr = format!("0.0.0.0:{}", self.port);
        info!("starting http server on {}", bind_addr);
        tokio::task::spawn_local(async move {
            let _ = http::serve(&bind_addr, store).await;
        });

        tokio::signal::ctrl_c().await?;
        info!("persist_manager shutdown");
        Ok(())
    }
}
