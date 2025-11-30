//! RateFetcher Demo
//!
//! 演示 RateFetcher 的基本功能：
//! - 初始化并启动后台拉取任务
//! - 查询预测资金费率
//! - 查询借贷利率
//! - 打印详细计算逻辑
//!
//! 环境变量：
//! - BINANCE_API_KEY: Binance API Key
//! - BINANCE_API_SECRET: Binance API Secret

use anyhow::Result;
use log::info;
use mkt_signal::common::exchange::Exchange;
use mkt_signal::funding_rate::common::FundingRatePeriod;
use mkt_signal::funding_rate::{MktChannel, RateFetcher, SymbolList};
use mkt_signal::signal::common::TradingVenue;
use tokio::time::{sleep, Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("========================================");
    info!("RateFetcher Demo 启动");
    info!("========================================");

    // 使用 LocalSet 来支持 spawn_local
    let local = tokio::task::LocalSet::new();
    local.run_until(async move { run_demo().await }).await
}

async fn run_demo() -> Result<()> {
    // 初始化单例
    SymbolList::init_singleton()?;
    info!("SymbolList 初始化完成");

    MktChannel::init_singleton()?;
    info!("MktChannel 初始化完成");

    RateFetcher::init(Exchange::BinanceFutures)?;
    info!("RateFetcher 初始化完成");

    // 等待一段时间让后台任务拉取数据
    info!("等待 15 秒拉取费率数据...");
    sleep(Duration::from_secs(15)).await;

    // 测试查询接口
    info!("\n========================================");
    info!("测试查询接口");
    info!("========================================");

    let test_symbols = ["STRKUSDT", "XAIUSDT"];
    for symbol in &test_symbols {
        info!("\n查询 {}:", symbol);

        // 查询预测资金费率
        let predicted_fr =
            RateFetcher::instance().get_predicted_funding_rate(symbol, TradingVenue::BinanceUm);
        match predicted_fr {
            Some((period, rate)) => info!(
                "  预测资金费率 ({}): {:.6}%",
                format_period(period),
                rate * 100.0
            ),
            None => info!("  预测资金费率: N/A"),
        }

        // 查询借贷利率
        let loan_rate =
            RateFetcher::instance().get_predict_loan_rate(symbol, TradingVenue::BinanceUm);
        match loan_rate {
            Some((period, rate)) => info!(
                "  预测借贷利率 ({}): {:.6}%",
                format_period(period),
                rate * 100.0
            ),
            None => info!("  预测借贷利率: N/A"),
        }

        // 查询当前借贷利率
        let cur_loan =
            RateFetcher::instance().get_current_loan_rate(symbol, TradingVenue::BinanceUm);
        match cur_loan {
            Some((period, rate)) => info!(
                "  当前借贷利率 ({}): {:.6}%",
                format_period(period),
                rate * 100.0
            ),
            None => info!("  当前借贷利率: N/A"),
        }
    }

    // TODO: print_detailed_calculation 暂未实现
    // RateFetcher::instance().print_detailed_calculation("XAIUSDT", TradingVenue::BinanceUm);

    info!("Demo 结束");

    Ok(())
}

fn format_period(period: FundingRatePeriod) -> &'static str {
    period.as_str()
}
