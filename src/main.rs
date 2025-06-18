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
use crate::connection::manager::MktConnectionManager;
use chrono::{Utc, TimeDelta, NaiveTime};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio::sync::OnceCell;
use crate::connection::binance_conn::BinanceFuturesSnapshotQuery;


pub fn next_target_instant(time_str: &str) -> Instant {
    if time_str == "--:--:--" {
        println!("WARNING: Using fallback time + 30 seconds from now");
        return Instant::now() + Duration::from_secs(5);
    }

    // 解析时间字符串
    let target_time = match NaiveTime::parse_from_str(time_str, "%H:%M:%S") {
        Ok(time) => time,
        Err(_) => {
            println!("WARNING: Invalid time format '{}', using 00:00:00", time_str);
            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        }
    };

    // 获取当前UTC时间
    let now = Utc::now();
    let now_naive = now.naive_utc();
    
    // 构造今天的目标时间
    let today_target = now.date_naive().and_time(target_time);
    
    // 计算目标时间（如果已过今天就取明天）
    let target = if now_naive < today_target {
        today_target
    } else {
        today_target + TimeDelta::days(1)
    };
    
    // 计算需要等待的时间长度
    let sleep_duration = target - now_naive;
    
    // 转换为std::time::Duration
    let sleep_std = sleep_duration.to_std().unwrap();
    
    // 返回目标时刻的tokio Instant
    Instant::now() + sleep_std
}

fn format_status_table(next_restart: u64, next_0030: u64) -> String {
    format!("\n\
         |---------------------|-------------|\n\
         | Next Restart        | {:>10}s |\n\
         | Next 00:30          | {:>10}s |\n\
         |---------------------|-------------|",
        next_restart, next_0030
    )

}

async fn perform_restart(
    mkt_connection_manager: &mut MktConnectionManager,
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

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "INFO");
    env_logger::init();

    static CFG: OnceCell<Config> = OnceCell::const_new();

    async fn get_config() -> &'static Config {
        CFG.get_or_init(|| async {
            Config::load_config("./mkt_cfg.yaml").await.unwrap()
        }).await
    }
    
    let cfg = get_config().await;
    // 获取exchange
    let exchange = cfg.get_exchange();
    let (global_shutdown_tx, _) = watch::channel(false);
    let (proxy_shutdown_tx, proxy_shutdown_rx) = watch::channel(false);
    let restart_checker: RestartChecker = RestartChecker::new(cfg.is_primary, cfg.restart_duration_secs);
    //建立connection，并启动
    let mut mkt_connection_manager = MktConnectionManager::new(&cfg, &global_shutdown_tx).await;
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
    let mkt_inc_rx = mkt_connection_manager.get_inc_tx().subscribe();
    let mkt_trade_rx = mkt_connection_manager.get_trade_tx().subscribe();
    let (binance_snapshot_tx, binance_snapshot_rx) = tokio::sync::broadcast::channel(100);
    //使用tokio spwan运行proxy
    let proxy_shutdown_rx_clone: watch::Receiver<bool> = proxy_shutdown_rx.clone();
    tokio::spawn(
        async move {
        let mut proxy: Proxy = Proxy::new(forwarder, mkt_inc_rx, mkt_trade_rx, binance_snapshot_rx, proxy_shutdown_rx_clone, tp_reset_notify);
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
    // primary额外在在utc时间0点30s重启
    let mut next_0030_instant = next_target_instant("00:30:00");
    let mut next_snapshot_query_instant = next_target_instant(cfg.binance_snapshot_requery_time.as_ref().unwrap());
    // log 
    let mut log_interval = tokio::time::interval(Duration::from_secs(3));
    log::info!("exchange: {}", exchange);
    while !token.is_cancelled() {
        tokio::select! {
            _ = log_interval.tick() => {
                let now_instant = Instant::now();
                log::info!("{}", format_status_table(
                    next_restart_instant.duration_since(now_instant).as_secs(),
                    next_0030_instant.duration_since(now_instant).as_secs()
                ));
            }
            _ = tokio::time::sleep_until(next_snapshot_query_instant) => {
                if exchange == "binance-futures" && restart_checker.is_primary {
                    //只有主节点且exchange为binance-futures时，才做depth快照修正
                    log::info!("Query depth snapshot at {:?}", next_snapshot_query_instant);
                    //用tokio的spawn运行，不会阻塞主循环
                    let binance_snapshot_tx_clone = binance_snapshot_tx.clone();
                    tokio::spawn(async move {
                        let symbols = cfg.get_symbols().await.unwrap();
                        BinanceFuturesSnapshotQuery::start_fetching_depth(symbols, binance_snapshot_tx_clone).await;
                        log::info!("Query depth snapshot for binance-futures successfully");
                    });
                }
                next_snapshot_query_instant += Duration::from_secs(24 * 60 * 60);
            }
            _ = tokio::time::sleep_until(next_0030_instant) => {
                next_0030_instant += tokio::time::Duration::from_secs(24 * 60 * 60);
                if restart_checker.is_primary {
                    if let Err(e) = perform_restart(&mut mkt_connection_manager, &global_shutdown_tx, "00:00:30").await {
                        error!("Failed to perform 00:00:30 restart: {}", e);
                    }
                }
                log::info!("00:00:30 restart for primary successfully");
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

// #[tokio::main(worker_threads = 1)]
// async fn main() -> anyhow::Result<()> {
//     std::env::set_var("RUST_LOG", "INFO");
//     env_logger::init();
//     let (tx, _) = tokio::sync::broadcast::channel(100);
//     // 获取所有symbol
//     let cfg = Config::load_config("/root/project/crypto_proxy/mkt_cfg.yaml").await.unwrap();
//     let symbols = cfg.get_symbols().await.unwrap();
//     //symbols取slice 前10个
//     BinanceFuturesSnapshotQuery::start_fetching_depth(symbols[..10].to_vec(), tx).await;
//     Ok(())
// }