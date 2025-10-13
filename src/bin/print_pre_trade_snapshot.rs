use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use env_logger;
use mkt_signal::pre_trade::{
    order_manager::{Order, OrderType, Side},
    store::{RedisStore, StrategyRecord},
};
use redis::AsyncCommands; 
use serde_json::json; 

#[derive(Parser, Debug)]
#[command(about = "打印 pre_trade Redis 持久化的订单与策略快照", version)] 
struct Args {
    /// Redis URL，例如 redis://:pwd@127.0.0.1:6379/0
    #[arg(long = "redis-url", default_value = "redis://127.0.0.1:6379/0")]
    redis_url: String,

    /// 持久化前缀，默认读取 PRE_TRADE_STORE_PREFIX 或 pre_trade
    #[arg(
        long,
        default_value = "pre_trade",
        help = "pre_trade 持久化使用的键名前缀"
    )]
    prefix: String,

    /// 以 JSON 原样输出 orders/strategies 
    #[arg(long, help = "输出原始 JSON 数据")] 
    raw: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let mut store = RedisStore::connect(&args.redis_url, &args.prefix)
        .await
        .context("connect redis store")?;

    let orders = store
        .load_orders()
        .await
        .context("load pre_trade orders from redis")?;
    let strategies = store
        .load_strategies()
        .await
        .context("load pre_trade strategies from redis")?;

    let saved_at_tick = load_saved_at(&args.redis_url, &args.prefix).await?;
    let saved_at_text = saved_at_tick
        .map(format_timestamp)
        .transpose()? // Option<Result<String>> -> Result<Option<String>>
        .unwrap_or_else(|| "-".to_string());

    println!(
        "[info] prefix={} orders={} strategies={} saved_at={}",
        args.prefix,
        orders.len(),
        strategies.len(),
        saved_at_text
    );

    if args.raw {
        let raw = json!({
            "orders": orders,
            "strategies": strategies,
        });
        println!("{}", serde_json::to_string_pretty(&raw)?);
        return Ok(());
    }

    print_orders(&orders)?;
    println!();
    print_strategies(&strategies);

    Ok(())
}

async fn load_saved_at(redis_url: &str, prefix: &str) -> Result<Option<i64>> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client
        .get_async_connection()
        .await
        .context("connect redis for saved_at")?;
    let key = format!("{prefix}:saved_at");
    let ts: Option<i64> = conn.get(key).await?;
    Ok(ts)
}

fn print_orders(orders: &[Order]) -> Result<()> {
    if orders.is_empty() {
        println!("[orders] 空");
        return Ok(());
    }

    let headers = vec![
        "order_id",
        "symbol",
        "side",
        "type",
        "qty",
        "cum_filled",
        "hedged_filled",
        "price",
        "status",
        "submit",
        "filled",
    ];
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(orders.len());
    for order in orders {
        let row = vec![
            order.order_id.to_string(),
            order.symbol.clone(),
            format_side(order.side).to_string(),
            format_order_type(order.order_type).to_string(),
            format_decimal(order.quantity),
            format_decimal(order.cumulative_filled_quantity),
            format_decimal(order.hedged_filled_quantity),
            format_decimal(order.price),
            order.status.as_str().to_string(),
            format_timestamp(order.submit_time)?,
            format_timestamp(order.filled_time)?,
        ];
        for (idx, cell) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(cell.len());
        }
        rows.push(row);
    }

    let header_line = headers
        .iter()
        .enumerate()
        .map(|(idx, h)| format!("{h:<width$}", width = widths[idx]))
        .collect::<Vec<_>>()
        .join(" | ");
    let sep_line = headers
        .iter()
        .enumerate()
        .map(|(idx, _)| "-".repeat(widths[idx]))
        .collect::<Vec<_>>()
        .join("-+-");

    println!("[orders]");
    println!("{header_line}");
    println!("{sep_line}");
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(idx, cell)| format!("{cell:<width$}", width = widths[idx]))
                .collect::<Vec<_>>()
                .join(" | ")
        );
    }
    Ok(())
}

fn print_strategies(records: &[StrategyRecord]) {
    if records.is_empty() {
        println!("[strategies] 空");
        return;
    }
    println!("[strategies]");
    for record in records {
        println!(
            "  id={} type={} payload_len={} bytes",
            record.id,
            record.type_name,
            record.payload.len()
        );
    }
}

fn format_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn format_order_type(order_type: OrderType) -> &'static str {
    order_type.as_str()
}

fn format_decimal(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else {
        format!("{value:.6}")
    }
}

fn format_timestamp(ts_us: i64) -> Result<String> {
    if ts_us <= 0 {
        return Ok("-".to_string());
    }
    let secs = ts_us / 1_000_000;
    let micros = (ts_us % 1_000_000).abs() as u32;
    let nanos = micros * 1_000;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("invalid timestamp {ts_us}"))?;
    Ok(dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
}
