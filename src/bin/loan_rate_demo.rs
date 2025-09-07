use clap::Parser;
use mkt_signal::market_state::FundingRateManager;
use mkt_signal::exchange::Exchange;

#[derive(Parser, Debug)]
#[command(name = "loan_rate_demo", about = "Demo: fetch and print loan_rate_8h and predicted funding for symbols")] 
struct Args {
    /// Comma separated symbols, e.g. BTCUSDT,ETHUSDT
    #[arg(long, default_value = "BTCUSDT,ETHUSDT,BNBUSDT")]
    symbols: String,

    /// Exchange: binance|okex|bybit
    #[arg(long, default_value = "binance")]
    exchange: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::env::set_var("RUST_LOG", "DEBUG");
    env_logger::init();
    let symbols: Vec<String> = args
        .symbols
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // 刷新资金费率与借贷数据（会自动使用签名接口/覆盖/默认）
    let mgr = FundingRateManager::instance();
    let ex = match args.exchange.as_str() {
        "binance" => Exchange::Binance,
        "okex" | "okex-swap" => Exchange::OkexSwap,
        "bybit" => Exchange::Bybit,
        other => {
            eprintln!("Unsupported exchange: {}", other);
            return Ok(());
        }
    };
    mgr.refresh_exchange_rates(ex).await?;

    let now = chrono::Utc::now().timestamp_millis();

    // 打印基础币的借贷利率（根据传入的 symbols 推导基础币）
    use std::collections::HashMap;
    let mut coin_rates: HashMap<String, f64> = HashMap::new();
    for sym in &symbols {
        let coin = match ex {
            Exchange::OkexSwap | Exchange::Okex => {
                // OKX: BTC-USDT-SWAP -> BTC
                sym.split('-').next().unwrap_or("").to_uppercase()
            }
            _ => {
                // Binance / Bybit: BTCUSDT / 1000SHIBUSDT -> BTC / SHIB
                let s = sym.to_uppercase();
                if s.ends_with("USDT") {
                    let base = &s[..s.len()-4];
                    if base.starts_with("1000") { base[4..].to_string() } else { base.to_string() }
                } else { s }
            }
        };
        let d = mgr.get_rates_sync(sym, ex, now);
        // 只记录第一个非零，或覆盖为更大的值（便于观察）
        let entry = coin_rates.entry(coin).or_insert(0.0);
        if d.loan_rate_8h > 0.0 { *entry = d.loan_rate_8h; }
    }

    println!("coin,loan_rate_8h");
    for (coin, rate) in coin_rates.iter() {
        println!("{},{}", coin, format!("{:.8}", rate));
    }

    // 打印每个 symbol 的预测资金费率与借贷利率
    println!("symbol,predicted_funding_rate,loan_rate_8h");
    for sym in symbols {
        let d = mgr.get_rates_sync(&sym, ex, now);
        println!("{},{:.8},{:.8}", sym, d.predicted_funding_rate, d.loan_rate_8h);
    }
    Ok(())
}
