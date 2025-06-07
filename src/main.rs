mod connection;
mod sub_msg;
use crate::connection::manager::MktConnectionManager;
mod cfg;
mod forwarder;
mod mkt_msg;
mod parser;
mod proxy;
mod receiver;
use cfg::Config;
use proxy::Proxy;
use forwarder::ZmqForwarder;
use receiver::ZmqReceiver;
use log::error;
use tokio::signal;
use tokio::sync::watch;
#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cfg = Config::load_config("/root/project/crypto_proxy/mkt_cfg.yaml").await.unwrap();
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    //建立connection，并启动
    let mut mkt_connection_manager = MktConnectionManager::new(&cfg, &global_shutdown_tx).await;
    //建立forwarder
    let forwarder = match ZmqForwarder::new(&cfg) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to create forwarder: {}", e);
            return Err(e.into());
        }
    };
    //建立ipc receiver，用于测试
    //启动receiver
    log::info!("Starting receiver......");
    let global_shutdown_rx_clone = global_shutdown_rx.clone();
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(async move {
            let mut receiver: ZmqReceiver = ZmqReceiver::new(&cfg, global_shutdown_rx_clone).unwrap();
            receiver.start_receiving().await;
        })
    });

    log::info!("Starting proxy......");
    //启动proxy
    let mkt_inc_rx = mkt_connection_manager.get_inc_tx().subscribe();
    let mkt_trade_rx = mkt_connection_manager.get_trade_tx().subscribe();
    
    //使用tokio spwan运行proxy
    let global_shutdown_rx_clone = global_shutdown_rx.clone();
    tokio::spawn(
        async move {
        let mut proxy: Proxy = Proxy::new(forwarder, mkt_inc_rx, mkt_trade_rx, global_shutdown_rx_clone);
        proxy.run().await;
    });
    //建立connection，并启动
    log::info!("Starting all connections......");
    mkt_connection_manager.start_all_connections().await;

    // 主循环等待关闭信号
    // 等待 SIGINT (Ctrl+C) 或 SIGTERM
    let ctrl_c = signal::ctrl_c();
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = ctrl_c => {
            log::info!("SIGINT (Ctrl+C) received");
            // 发送关闭信号给所有任务
            let _ = global_shutdown_tx.send(true);
            match mkt_connection_manager.shutdown(&global_shutdown_tx).await {
                Ok(_) => log::info!("MktConnectionManager shutdown successfully"),
                Err(e) => error!("Failed to shutdown MktConnectionManager: {}", e),
            }
        }
        _ = sigterm.recv() => {
            log::info!("SIGTERM received");
            // 发送关闭信号给所有任务
            let _ = global_shutdown_tx.send(true);
            match mkt_connection_manager.shutdown(&global_shutdown_tx).await {
                Ok(_) => log::info!("MktConnectionManager shutdown successfully"),
                Err(e) => error!("Failed to shutdown MktConnectionManager: {}", e),
            }
        }
    }
    Ok(())
}