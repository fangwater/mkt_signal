mod connection;
mod sub_msg;
mod cfg;
mod forwarder;
mod mkt_msg;
mod parser;
mod proxy;
mod receiver;
mod restart_checker;
use cfg::Config;
use proxy::Proxy;
use forwarder::ZmqForwarder;
// use receiver::ZmqReceiver;
use log::error;
use tokio::signal;
use tokio::sync::watch;
use restart_checker::RestartChecker;
use crate::connection::mkt_manager::MktDataConnectionManager;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio::sync::OnceCell;
use clap::Parser;

#[derive(Parser)]
#[command(name = "crypto_proxy")]
#[command(about = "config file path")]
struct Args {
    /// 配置文件路径
    #[arg(short, long)]
    config: String,
}

fn format_status_table(next_restart: u64) -> String {
    format!("\n\
         |-------------------------|-------------|\n\
         | Next Restart            | {:>10}s     |\n\
         |-------------------------|-------------|",
        next_restart
    )
}

async fn perform_restart(
    mkt_connection_manager: &mut MktDataConnectionManager,
    global_shutdown_tx: &watch::Sender<bool>,
    reason: &str,
) -> anyhow::Result<()> {
    log::info!("Triggering {} restart...", reason);
    
    // 发送关闭信号给所有任务
    let _ = global_shutdown_tx.send(true);
    
    // 关闭连接
    match mkt_connection_manager.shutdown(global_shutdown_tx).await {
        Ok(_) => log::info!("MktConnectionManager shutdown successfully"),
        Err(e) => error!("Failed to shutdown MktConnectionManager: {}", e),
    }
    
    // 重置关闭信号
    let _ = global_shutdown_tx.send(false);
    
    // 更新订阅消息
    match mkt_connection_manager.update_subscribe_msgs().await {
        Ok(_) => log::info!("Update subscribe msgs successfully"),
        Err(e) => error!("Failed to update subscribe msgs: {}", e),
    }
    //每次重启后，向inc和trade的broadcast发送一条tp-reset-msg, 提示下游时间戳可能不连续
    //
    
    // 启动所有连接
    mkt_connection_manager.start_all_connections().await;
    log::info!("Restarting all connections successfully");
    
    Ok(())
}

#[tokio::main(worker_threads = 3)]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "INFO");
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    let config_path = args.config;

    static CFG: OnceCell<Config> = OnceCell::const_new();

    async fn get_config(config_path: &str) -> &'static Config {
        CFG.get_or_init(|| async {
            Config::load_config(config_path).await.unwrap()
        }).await
    }
    
    let cfg = get_config(&config_path).await;
    // 获取exchange
    let exchange = cfg.get_exchange();
    let (global_shutdown_tx, _) = watch::channel(false);
    let (proxy_shutdown_tx, proxy_shutdown_rx) = watch::channel(false);
    let restart_checker: RestartChecker = RestartChecker::new(cfg.is_primary, cfg.restart_duration_secs);
    //建立connection，并启动
    let mut mkt_connection_manager = MktDataConnectionManager::new(&cfg, &global_shutdown_tx).await;
    let tp_reset_notify = mkt_connection_manager.get_tp_reset_notify();
    let forwarder = match ZmqForwarder::new(&cfg) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to create forwarder: {}", e);
            return Err(e.into());
        }
    };

    log::info!("Starting proxy......");
    //启动proxy
    let mkt_rx = mkt_connection_manager.get_mkt_tx().subscribe();
    //使用tokio spwan运行proxy
    let proxy_shutdown_rx_clone: watch::Receiver<bool> = proxy_shutdown_rx.clone();
    tokio::spawn(
        async move {
        let mut proxy: Proxy = Proxy::new(forwarder, mkt_rx, proxy_shutdown_rx_clone, tp_reset_notify);
        proxy.run().await;
    });
    //建立connection，并启动
    log::info!("Starting all connections......");
    mkt_connection_manager.start_all_connections().await;

    // 主循环等待关闭信号
    // 等待 SIGINT (Ctrl+C) 或 SIGTERM
    //使用tokio的canceltoken检测关闭信号
    let token = CancellationToken::new();
    let token_for_ctrl_c = token.clone();
    let token_for_sigterm = token.clone();

    // 创建一个tokio的task，用于检测ctrl_c信号
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to wait for Ctrl+C");
        log::info!("SIGINT (Ctrl+C) received");
        // 发送关闭信号给所有任务
        token_for_ctrl_c.cancel();
    });

    tokio::spawn(async move {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
        sigterm.recv().await.expect("Failed to wait for SIGTERM");
        log::info!("SIGTERM received");
        token_for_sigterm.cancel();
    });

    let mut next_restart_instant = restart_checker.get_next_restart_instant();
    log::info!("Next restart instant: {:?}", next_restart_instant);
    let mut log_interval = tokio::time::interval(Duration::from_secs(3));
    log::info!("exchange: {}", exchange);
    while !token.is_cancelled() {
        tokio::select! {
            _ = log_interval.tick() => {
                let now_instant = Instant::now();
                log::info!("{}", format_status_table(
                    next_restart_instant.duration_since(now_instant).as_secs()
                ));
            }
            _ = tokio::time::sleep_until(next_restart_instant) => {
                next_restart_instant += tokio::time::Duration::from_secs(restart_checker.restart_duration_secs) * 2;
                if let Err(e) = perform_restart(&mut mkt_connection_manager, &global_shutdown_tx, "scheduled").await {
                    error!("Failed to perform scheduled restart: {}", e);
                }
            }
        }
    }
    log::info!("Shutdown signal received");
    // 发送关闭信号给所有任务
    let _ = global_shutdown_tx.send(true);
    match mkt_connection_manager.shutdown(&global_shutdown_tx).await {
        Ok(_) => log::info!("MktConnectionManager shutdown successfully"),
        Err(e) => error!("Failed to shutdown MktConnectionManager: {}", e),
    }
    //let _ = receiver_shutdown_tx.send(true);
    let _ = proxy_shutdown_tx.send(true);
    Ok(())
}