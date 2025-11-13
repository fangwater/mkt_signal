//! RateFetcher Demo
//!
//! 演示 RateFetcher 的基本功能：
//! - 初始化并启动后台拉取任务
//! - 查询预测资金费率
//! - 查询借贷利率
//! - 打印三线表
//!
//! 环境变量：
//! - BINANCE_API_KEY: Binance API Key
//! - BINANCE_API_SECRET: Binance API Secret

use anyhow::Result;
use log::info;
use mkt_signal::funding_rate::{RateFetcher, SymbolList};
use tokio::time::{sleep, Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("========================================");
    info!("RateFetcher Demo 启动");
    info!("========================================");

    // 使用 LocalSet 来支持 spawn_local
    let local = tokio::task::LocalSet::new();
    local.run_until(async move {
        run_demo().await
    }).await
}

async fn run_demo() -> Result<()> {
    // 初始化 SymbolList（空的也可以，会使用默认 symbols）
    SymbolList::init_singleton()?;
    info!("SymbolList 初始化完成");

    // 初始化 RateFetcher（从环境变量获取 API Key）
    RateFetcher::init_singleton()?;
    info!("RateFetcher 初始化完成");

    // 等待一段时间让后台任务拉取数据
    info!("等待 30 秒拉取费率数据...");
    sleep(Duration::from_secs(30)).await;

    // 测试查询接口
    info!("\n========================================");
    info!("测试查询接口");
    info!("========================================");

    let test_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "SUIUSDT"];
    let period = mkt_signal::funding_rate::rate_fetcher::FundingRatePeriod::Hours8;

    for symbol in &test_symbols {
        info!("\n查询 {}:", symbol);

        // 查询预测资金费率
        let predicted_fr = RateFetcher::instance().get_binance_predicted_funding_rate(symbol, period);
        match predicted_fr {
            Some(rate) => info!("  预测资金费率 (8h): {:.6}%", rate * 100.0),
            None => info!("  预测资金费率: N/A"),
        }

        // 查询借贷利率
        let loan_rate = RateFetcher::instance().get_binance_loan_rate(symbol, period);
        match loan_rate {
            Some(rate) => info!("  借贷利率 (8h): {:.6}%", rate * 100.0),
            None => info!("  借贷利率: N/A"),
        }
    }

    info!("\n========================================");
    info!("三线表将在整点时自动打印");
    info!("继续运行 60 秒观察...");
    info!("========================================");

    // 继续运行一段时间，观察整点打印的三线表
    sleep(Duration::from_secs(60)).await;

    info!("\nDemo 结束");

    Ok(())
}
