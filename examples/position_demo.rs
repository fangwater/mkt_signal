use mkt_signal::position::{PositionManager, api::ApiConfig};
use mkt_signal::position::binance::BinanceApiClient;
use mkt_signal::common::exchange::Exchange;
use std::sync::Arc;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::init();
    
    // 方法1: 从环境变量读取配置
    let config = ApiConfig::from_env_binance()?;
    
    // 方法2: 手动创建配置（用于测试）
    // let config = ApiConfig {
    //     api_key: "your_api_key".to_string(),
    //     api_secret: "your_api_secret".to_string(),
    //     passphrase: None,
    //     testnet: false,
    //     base_url: None,
    // };
    
    // 创建币安API客户端
    let api_client = Arc::new(BinanceApiClient::new(config));
    
    // 创建仓位管理器
    let manager = PositionManager::new(Exchange::Binance, api_client);
    
    // 同步所有仓位
    println!("正在同步仓位...");
    manager.sync_positions().await?;
    
    // 获取仓位汇总
    let summary = manager.get_summary().await?;
    println!("\n仓位汇总:");
    println!("  总仓位数: {}", summary.total_positions);
    println!("  开仓数量: {}", summary.open_positions);
    println!("  总价值: ${:.2}", summary.total_value);
    println!("  未实现盈亏: ${:.2}", summary.total_unrealized_pnl);
    
    // 获取所有仓位
    let positions = manager.get_all_positions().await?;
    println!("\n当前持仓:");
    for position in positions.iter() {
        println!("  {} - {} {}:", 
            position.symbol, 
            position.position_type,
            position.side
        );
        println!("    数量: {:.4}", position.quantity);
        println!("    开仓价: ${:.2}", position.entry_price);
        if let Some(mark_price) = position.mark_price {
            println!("    标记价: ${:.2}", mark_price);
        }
        if let Some(pnl) = position.unrealized_pnl {
            let pnl_pct = position.pnl_percentage().unwrap_or(0.0);
            println!("    未实现盈亏: ${:.2} ({:.2}%)", pnl, pnl_pct);
        }
        if let Some(liq_price) = position.liquidation_price {
            println!("    强平价格: ${:.2}", liq_price);
        }
    }
    
    // 检查风险仓位（距离强平5%以内）
    let at_risk = manager.check_liquidation_risk(5.0).await?;
    if !at_risk.is_empty() {
        println!("\n⚠️ 风险警告 - 以下仓位接近强平:");
        for position in at_risk {
            println!("  {} - 强平价: ${:.2}, 当前价: ${:.2}", 
                position.symbol,
                position.liquidation_price.unwrap_or(0.0),
                position.mark_price.unwrap_or(0.0)
            );
        }
    }
    
    Ok(())
}