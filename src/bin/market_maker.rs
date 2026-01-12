//! Market Maker 做市商交易信号生成器
//!
//! 极简启动器：初始化所有单例 + 监听退出信号

use anyhow::Result;
use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio_util::sync::CancellationToken;

const PROCESS_NAME: &str = "market_maker";

/// 主运行循环
async fn run(token: CancellationToken) -> Result<()> {
    info!("{} 启动", PROCESS_NAME);

    // TODO: 初始化做市商相关单例

    info!("{} 启动完成，等待市场数据触发决策...", PROCESS_NAME);

    // 主循环：等待退出信号
    token.cancelled().await;
    info!("收到退出信号");

    info!("{} 退出", PROCESS_NAME);
    Ok(())
}

/// 设置信号处理器
fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut sigterm = unix_signal(SignalKind::terminate()).expect("failed to setup SIGTERM");
        let mut sigint = unix_signal(SignalKind::interrupt()).expect("failed to setup SIGINT");
        tokio::select! {
            _ = sigterm.recv() => info!("收到 SIGTERM"),
            _ = sigint.recv() => info!("收到 SIGINT (Ctrl+C)"),
        }
        token_clone.cancel();
    });
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // 初始化日志
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    info!("========== {} 初始化 ==========", PROCESS_NAME);

    // 设置信号处理器
    let token = CancellationToken::new();
    setup_signal_handlers(&token)?;

    // 使用 LocalSet 运行
    let local = tokio::task::LocalSet::new();
    local.run_until(run(token)).await
}
