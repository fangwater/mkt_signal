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
use receiver::ZmqReceiver;
use log::error;
use tokio::signal;
use tokio::sync::watch;
use restart_checker::RestartChecker;
use crate::connection::manager::MktConnectionManager;
use chrono::{Utc, TimeDelta};
use tokio::time::Instant;

pub fn next_target_instant(time_str: &str) -> Instant {
    // 处理特殊值：使用当前时间加1分钟
    if time_str == "--:--:--" {
        println!("WARNING: Using fallback time - 1 minute from now");
        return Instant::now() + Duration::from_secs(60);
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


#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cfg = Config::load_config("/root/project/crypto_proxy/mkt_cfg.yaml").await.unwrap();

    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    let restart_checker: RestartChecker = RestartChecker::new(cfg.is_primary, cfg.restart_duration);
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

    let mut next_restart_instant = restart_checker.get_next_restart_instant();
    // primary额外在在utc时间0点30s重启
    let mut next_0030_instant = next_0030_instant();
    tokio::select! {
        _ = tokio::time::sleep_until(next_0030_instant) => {
            log::info!("Triggering 0030 restart...");
            next_0030_instant += tokio::time::Duration::from_secs(24 * 60 * 60);
            if restart_checker.is_primary {
                // 发送关闭信号给所有任务
                let _ = global_shutdown_tx.send(true);
                match mkt_connection_manager.shutdown(&global_shutdown_tx).await {
                    Ok(_) => log::info!("MktConnectionManager shutdown successfully"),
                    Err(e) => error!("Failed to shutdown MktConnectionManager: {}", e),
                }
                //更新subscribe_msgs
                match mkt_connection_manager.update_subscribe_msgs().await {
                    Ok(_) => log::info!("Update subscribe msgs successfully"),
                    Err(e) => error!("Failed to update subscribe msgs: {}", e),
                }
                //启动所有connection
                mkt_connection_manager.start_all_connections().await;
                log::info!("Restarting all connections successfully");
            }
            log::info!("00:00:30 restart for primary successfully");
        }
        _ = tokio::time::sleep_until(next_restart_instant) => {
            //重启
            log::info!("Triggering restart...");
            //更新next_restart_instant
            next_restart_instant += tokio::time::Duration::from_secs(restart_checker.restart_duration_secs) * 2;
            // 发送关闭信号给所有任务
            let _ = global_shutdown_tx.send(true);
            match mkt_connection_manager.shutdown(&global_shutdown_tx).await {
                Ok(_) => log::info!("MktConnectionManager shutdown successfully"),
                Err(e) => error!("Failed to shutdown MktConnectionManager: {}", e),
            }
            //更新subscribe_msgs
            match mkt_connection_manager.update_subscribe_msgs().await {
                Ok(_) => log::info!("Update subscribe msgs successfully"),
                Err(e) => error!("Failed to update subscribe msgs: {}", e),
            }
            //启动所有connection
            mkt_connection_manager.start_all_connections().await;
            log::info!("Restarting all connections successfully");
        }
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