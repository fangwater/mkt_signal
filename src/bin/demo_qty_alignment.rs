use anyhow::{anyhow, Result};
use clap::Parser;
use log::info;

use mkt_signal::signal::common::TradingVenue;
use mkt_signal::signal::venue_min_qty_table::VenueMinQtyTable;

#[derive(Debug, Parser)]
#[command(name = "demo_qty_alignment", about = "信号层数量对齐演示（OKX vs Binance）")]
struct Args {
    /// 目标名义金额（U）
    #[arg(long, default_value_t = 200.0)]
    amount: f64,

    /// 价格（示例取日志中的 BTC 价格）
    #[arg(long, default_value_t = 88077.2)]
    price: f64,

    /// OKX 永续合约标的
    #[arg(long, default_value = "BTC-USDT-SWAP")]
    okex_symbol: String,

    /// Binance UM 永续标的
    #[arg(long, default_value = "BTCUSDT")]
    binance_symbol: String,

    /// 示例开仓合约数量（来自日志）
    #[arg(long, default_value_t = 0.2200)]
    example_open_qty: f64,

    /// 搜索窗口（步数）
    #[arg(long, default_value_t = 12)]
    search_window: i64,
}

#[derive(Debug, Clone)]
struct VenueSizing {
    venue: TradingVenue,
    symbol: String,
    symbol_key: String,
    price_raw: f64,
    price_tick: f64,
    price_aligned: f64,
    step_size: f64,
    min_qty: f64,
    min_notional: f64,
    contract_multiplier: f64,
    notional_per_step: f64,
    min_count: i64,
}

fn min_count(min_qty: f64, step: f64) -> i64 {
    if min_qty <= 0.0 || step <= 0.0 {
        return 0;
    }
    ((min_qty / step) - 1e-12).ceil() as i64
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if value <= 0.0 {
        return None;
    }
    let s = format!("{:.12}", value);
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        return None;
    }
    let frac = parts[1].trim_end_matches('0');
    if frac.is_empty() {
        return Some((1, 1));
    }
    let den = 10_i64.pow(frac.len() as u32);
    let num = frac.parse::<i64>().ok()?;
    Some((num, den))
}

fn align_price_floor(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) + 1e-9).floor();
    scaled * tick
}

fn normalize_symbol_key(venue: TradingVenue, symbol: &str) -> String {
    let mut key = symbol.trim().to_uppercase();
    if matches!(venue, TradingVenue::OkexFutures | TradingVenue::OkexMargin) {
        if let Some(stripped) = key.strip_suffix("-SWAP") {
            key = stripped.to_string();
        }
        key = key.replace('-', "");
    }
    key
}

async fn build_sizing(venue: TradingVenue, symbol: &str, price: f64) -> Result<VenueSizing> {
    let mut table = VenueMinQtyTable::new(venue);
    table.refresh().await?;

    let symbol_key = normalize_symbol_key(venue, symbol);
    let step_size = table
        .step_size(&symbol_key)
        .ok_or_else(|| anyhow!("missing step_size for {:?} {}", venue, symbol_key))?;
    let min_qty = table.min_qty(&symbol_key).unwrap_or(0.0);
    let min_notional = table.min_notional(&symbol_key).unwrap_or(0.0);
    let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
    let price_aligned = if price_tick > 0.0 {
        align_price_floor(price, price_tick)
    } else {
        price
    };
    let contract_multiplier = if matches!(venue, TradingVenue::OkexFutures) {
        table.contract_multiplier(&symbol_key)
    } else {
        1.0
    };

    let notional_per_step = match venue {
        TradingVenue::OkexFutures => step_size * contract_multiplier * price_aligned,
        TradingVenue::BinanceFutures => step_size * price_aligned,
        _ => step_size * price_aligned,
    };

    Ok(VenueSizing {
        venue,
        symbol: symbol.to_string(),
        symbol_key,
        price_raw: price,
        price_tick,
        price_aligned,
        step_size,
        min_qty,
        min_notional,
        contract_multiplier,
        notional_per_step,
        min_count: min_count(min_qty, step_size),
    })
}

#[derive(Debug, Clone)]
struct Candidate {
    count_open: i64,
    qty_open: f64,
    notional_open: f64,
    count_hedge: i64,
    qty_hedge: f64,
    notional_hedge: f64,
    mismatch: f64,
    deviation: f64,
}

fn better_candidate(a: &Candidate, b: &Candidate) -> bool {
    if a.mismatch < b.mismatch - 1e-9 {
        return true;
    }
    if (a.mismatch - b.mismatch).abs() <= 1e-9 {
        return a.deviation < b.deviation - 1e-9;
    }
    false
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    println!("=== Qty Alignment Demo (signal layer) ===\n");
    println!(
        "例子日志：\n[2025-12-21T07:27:17Z INFO  mkt_signal::strategy::hedge_arb_strategy] \
开仓订单已创建: strategy_id=1031767031 order_id=4431405655236018177 \
client_order_id=4431405655236018177 symbol=BTC-USDT-SWAP OkexFutures side=Buy \
qty=0.2200 price=88077.200000\n"
    );

    info!("刷新 OKX/币安 filters（真实 min_qty/step/tick）");
    let okex = build_sizing(TradingVenue::OkexFutures, &args.okex_symbol, args.price).await?;
    let binance =
        build_sizing(TradingVenue::BinanceFutures, &args.binance_symbol, args.price).await?;

    println!("目标名义金额 U = {:.4}\n", args.amount);

    println!("-- Open (OKX Futures) --");
    println!(
        "symbol={} key={} price_raw={:.6} price_tick={:.10} price_aligned={:.6}",
        okex.symbol, okex.symbol_key, okex.price_raw, okex.price_tick, okex.price_aligned
    );
    println!(
        "min_qty={:.10} step_size={:.10} min_notional={:.10}",
        okex.min_qty, okex.step_size, okex.min_notional
    );
    println!(
        "contract_multiplier={:.10} notional_per_step={:.6} min_count={}",
        okex.contract_multiplier, okex.notional_per_step, okex.min_count
    );
    println!(
        "每步 base_qty = step_size * contract_multiplier = {:.10}",
        okex.step_size * okex.contract_multiplier
    );
    println!();

    println!("-- Hedge (Binance UM Futures) --");
    println!(
        "symbol={} key={} price_raw={:.6} price_tick={:.10} price_aligned={:.6}",
        binance.symbol,
        binance.symbol_key,
        binance.price_raw,
        binance.price_tick,
        binance.price_aligned
    );
    println!(
        "min_qty={:.10} step_size={:.10} min_notional={:.10}",
        binance.min_qty, binance.step_size, binance.min_notional
    );
    println!(
        "contract_multiplier={:.10} notional_per_step={:.6} min_count={}",
        binance.contract_multiplier, binance.notional_per_step, binance.min_count
    );
    println!();

    if okex.notional_per_step <= 0.0 || binance.notional_per_step <= 0.0 {
        return Err(anyhow!(
            "invalid notional_per_step: okex={} binance={}",
            okex.notional_per_step,
            binance.notional_per_step
        ));
    }

    let ideal_open_count = (args.amount / okex.notional_per_step).round() as i64;
    let start = (ideal_open_count - args.search_window).max(okex.min_count).max(1);
    let end = (ideal_open_count + args.search_window).max(start);

    println!("-- 计算窗口 --");
    println!(
        "ideal_open_count=round(U / notional_per_step_open) = {}",
        ideal_open_count
    );
    println!("search_range=[{}, {}] (count_open)\n", start, end);

    let mut best: Option<Candidate> = None;
    println!("-- 候选明细 --");
    println!(
        "{:>6} {:>10} {:>12} | {:>6} {:>10} {:>12} | {:>10} {:>10}",
        "cA", "qtyA", "notionalA", "cB", "qtyB", "notionalB", "mismatch", "deviation"
    );

    for count_open in start..=end {
        let qty_open = count_open as f64 * okex.step_size;
        let notional_open = count_open as f64 * okex.notional_per_step;
        let count_hedge = (notional_open / binance.notional_per_step).round() as i64;
        if count_hedge < binance.min_count || count_hedge <= 0 {
            continue;
        }
        let qty_hedge = count_hedge as f64 * binance.step_size;
        let notional_hedge = count_hedge as f64 * binance.notional_per_step;
        let mismatch = (notional_open - notional_hedge).abs();
        let deviation = (notional_open - args.amount).abs() + (notional_hedge - args.amount).abs();

        println!(
            "{:>6} {:>10.6} {:>12.6} | {:>6} {:>10.6} {:>12.6} | {:>10.6} {:>10.6}",
            count_open, qty_open, notional_open, count_hedge, qty_hedge, notional_hedge, mismatch,
            deviation
        );

        let cand = Candidate {
            count_open,
            qty_open,
            notional_open,
            count_hedge,
            qty_hedge,
            notional_hedge,
            mismatch,
            deviation,
        };
        match &best {
            Some(best_cand) => {
                if better_candidate(&cand, best_cand) {
                    best = Some(cand);
                }
            }
            None => best = Some(cand),
        }
    }

    println!();
    if let Some(best_cand) = best {
        println!("-- 选择结果 --");
        println!(
            "open_qty={:.6} (cA={}) open_notional={:.6}",
            best_cand.qty_open, best_cand.count_open, best_cand.notional_open
        );
        println!(
            "hedge_qty={:.6} (cB={}) hedge_notional={:.6}",
            best_cand.qty_hedge, best_cand.count_hedge, best_cand.notional_hedge
        );
        println!(
            "mismatch={:.6} deviation={:.6}\n",
            best_cand.mismatch, best_cand.deviation
        );
    } else {
        println!("未找到可行候选（检查 min_qty/step_size 配置）。\n");
    }

    println!("-- 示例开仓数量对齐 --");
    let example_open_count = if okex.step_size > 0.0 {
        (args.example_open_qty / okex.step_size).round() as i64
    } else {
        0
    };
    let example_open_qty = example_open_count as f64 * okex.step_size;
    let example_open_notional = example_open_count as f64 * okex.notional_per_step;
    let example_base_qty = example_open_qty * okex.contract_multiplier;
    let example_hedge_count =
        (example_open_notional / binance.notional_per_step).round() as i64;
    let example_hedge_qty = example_hedge_count as f64 * binance.step_size;
    let example_hedge_notional = example_hedge_count as f64 * binance.notional_per_step;

    println!(
        "open_qty(OKX)={:.6} contracts -> base_qty={:.8}",
        example_open_qty, example_base_qty
    );
    println!(
        "open_notional={:.6} -> hedge_qty(Binance)={:.6} (notional={:.6})",
        example_open_notional, example_hedge_qty, example_hedge_notional
    );

    Ok(())
}
