use anyhow::{anyhow, Result};
use clap::Parser;
use std::thread;
use std::time::{Duration, Instant};

use mkt_signal::depth_pub::query_client::DepthQueryClient;
use mkt_signal::depth_pub::query_msg::{price_to_tick_index, tick_index_to_price};
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser, Debug)]
#[command(name = "demo_depth_query_tlen")]
#[command(about = "Depth query demo: request load_tlen(single/batch) from depth_pub")]
struct Args {
    /// Venue, e.g. binance-futures
    #[arg(long)]
    venue: TradingVenue,

    /// Symbol for query, e.g. BTCUSDT
    #[arg(long)]
    symbol: String,

    /// Single tick-index request
    #[arg(long)]
    tick_index: Option<i64>,

    /// Batch tick-index request in CSV, e.g. "1010,1015,1020"
    #[arg(long)]
    tick_indices: Option<String>,

    /// Single-price request (requires --price-tick)
    #[arg(long)]
    price: Option<f64>,

    /// Batch request prices in CSV (requires --price-tick)
    #[arg(long)]
    prices: Option<String>,

    /// Price tick for local float->tick_index conversion / display
    #[arg(long)]
    price_tick: Option<f64>,

    /// Query top5 tick_index+tlen and immediately re-query all 10 levels by batch
    #[arg(long, default_value_t = false)]
    top5: bool,

    /// Reuse a single persistent UDS connection for all requests
    #[arg(long, default_value_t = false)]
    persistent: bool,

    /// Repeat the query flow in-process this many times
    #[arg(long, default_value_t = 1)]
    repeat: usize,

    /// Sleep interval between repeated runs
    #[arg(long, default_value_t = 0)]
    interval_ms: u64,

    /// Only print benchmark summary for repeated runs
    #[arg(long, default_value_t = false)]
    summary_only: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct QueryRunStats {
    delta_count: usize,
    max_abs_delta: f64,
}

fn parse_csv_i64(raw: &str) -> Result<Vec<i64>> {
    let values: Vec<i64> = raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| {
            token
                .parse::<i64>()
                .map_err(|err| anyhow!("invalid tick_index '{token}': {err}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(anyhow!("tick_indices is empty"));
    }
    Ok(values)
}

fn parse_csv_f64(raw: &str) -> Result<Vec<f64>> {
    let values: Vec<f64> = raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| {
            token
                .parse::<f64>()
                .map_err(|err| anyhow!("invalid price '{token}': {err}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(anyhow!("prices is empty"));
    }
    Ok(values)
}

fn require_price_tick(args: &Args) -> Result<f64> {
    let Some(price_tick) = args.price_tick else {
        return Err(anyhow!(
            "--price or --prices requires --price-tick for tick_index conversion"
        ));
    };
    if !price_tick.is_finite() || price_tick <= 0.0 {
        return Err(anyhow!("invalid --price-tick: {}", price_tick));
    }
    Ok(price_tick)
}

fn maybe_format_price(tick_index: i64, price_tick: Option<f64>) -> Option<f64> {
    price_tick.and_then(|tick| tick_index_to_price(tick_index, tick))
}

fn percentile(sorted_values: &[f64], p: f64) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    if sorted_values.len() == 1 {
        return Some(sorted_values[0]);
    }
    let rank = (sorted_values.len() - 1) as f64 * p;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let frac = rank - lo as f64;
    Some(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)
}

fn create_client(args: &Args) -> Result<DepthQueryClient> {
    if args.persistent {
        DepthQueryClient::new_persistent(args.venue)
    } else {
        DepthQueryClient::new_short_lived(args.venue)
    }
}

fn execute_once(
    args: &Args,
    client: &DepthQueryClient,
    symbol: &str,
    single_tick_index: Option<i64>,
    batch_tick_indices: Option<&[i64]>,
    print_details: bool,
) -> Result<QueryRunStats> {
    if let Some(tick_index) = single_tick_index {
        let amount = client.query_single_tick_index(symbol, tick_index)?;
        if print_details {
            if let Some(price) = maybe_format_price(tick_index, args.price_tick) {
                println!(
                    "TLEN_SINGLE venue={} symbol={} tick_index={} price={} amount={}",
                    client.venue_slug(),
                    symbol,
                    tick_index,
                    price,
                    amount
                );
            } else {
                println!(
                    "TLEN_SINGLE venue={} symbol={} tick_index={} amount={}",
                    client.venue_slug(),
                    symbol,
                    tick_index,
                    amount
                );
            }
        }
    }

    if let Some(tick_indices) = batch_tick_indices {
        let amounts = client.query_batch_tick_indices(symbol, tick_indices)?;
        if print_details {
            println!(
                "TLEN_BATCH venue={} symbol={} count={}",
                client.venue_slug(),
                symbol,
                tick_indices.len()
            );
            for (tick_index, amount) in tick_indices.iter().zip(amounts.iter()) {
                if let Some(price) = maybe_format_price(*tick_index, args.price_tick) {
                    println!(
                        "  tick_index={} price={} amount={}",
                        tick_index, price, amount
                    );
                } else {
                    println!("  tick_index={} amount={}", tick_index, amount);
                }
            }
        }
    }

    let mut stats = QueryRunStats::default();
    if args.top5 {
        let top5 = client.query_top5(symbol)?;
        let mut tick_indices = Vec::with_capacity(top5.bids.len() + top5.asks.len());
        tick_indices.extend(top5.bids.iter().map(|(tick_index, _)| *tick_index));
        tick_indices.extend(top5.asks.iter().map(|(tick_index, _)| *tick_index));

        let rechecked = client.query_batch_tick_indices(symbol, &tick_indices)?;
        if print_details {
            println!(
                "TLEN_TOP5_RECHECK venue={} symbol={} bids={} asks={}",
                client.venue_slug(),
                symbol,
                top5.bids.len(),
                top5.asks.len()
            );
        }

        let mut idx = 0usize;
        for (level, (tick_index, tlen)) in top5.bids.iter().enumerate() {
            let batch_tlen = rechecked.get(idx).copied().unwrap_or(-1.0);
            let delta = batch_tlen - *tlen;
            stats.delta_count += 1;
            stats.max_abs_delta = stats.max_abs_delta.max(delta.abs());
            if print_details {
                if let Some(price) = maybe_format_price(*tick_index, args.price_tick) {
                    println!(
                        "  BID L{} tick_index={} price={} top5_tlen={} batch_tlen={} delta={}",
                        level + 1,
                        tick_index,
                        price,
                        tlen,
                        batch_tlen,
                        delta
                    );
                } else {
                    println!(
                        "  BID L{} tick_index={} top5_tlen={} batch_tlen={} delta={}",
                        level + 1,
                        tick_index,
                        tlen,
                        batch_tlen,
                        delta
                    );
                }
            }
            idx += 1;
        }

        for (level, (tick_index, tlen)) in top5.asks.iter().enumerate() {
            let batch_tlen = rechecked.get(idx).copied().unwrap_or(-1.0);
            let delta = batch_tlen - *tlen;
            stats.delta_count += 1;
            stats.max_abs_delta = stats.max_abs_delta.max(delta.abs());
            if print_details {
                if let Some(price) = maybe_format_price(*tick_index, args.price_tick) {
                    println!(
                        "  ASK L{} tick_index={} price={} top5_tlen={} batch_tlen={} delta={}",
                        level + 1,
                        tick_index,
                        price,
                        tlen,
                        batch_tlen,
                        delta
                    );
                } else {
                    println!(
                        "  ASK L{} tick_index={} top5_tlen={} batch_tlen={} delta={}",
                        level + 1,
                        tick_index,
                        tlen,
                        batch_tlen,
                        delta
                    );
                }
            }
            idx += 1;
        }
    }

    Ok(stats)
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if args.tick_index.is_none()
        && args.tick_indices.is_none()
        && args.price.is_none()
        && args.prices.is_none()
        && !args.top5
    {
        return Err(anyhow!(
            "at least one of --tick-index/--tick-indices/--price/--prices/--top5 must be provided"
        ));
    }
    if args.repeat == 0 {
        return Err(anyhow!("--repeat must be >= 1"));
    }

    let symbol = args.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return Err(anyhow!("symbol must not be empty"));
    }

    let single_tick_index = match (args.tick_index, args.price) {
        (Some(tick_index), None) => Some(tick_index),
        (None, Some(price)) => {
            let tick = require_price_tick(&args)?;
            Some(
                price_to_tick_index(price, tick)
                    .ok_or_else(|| anyhow!("failed to convert price={} to tick_index", price))?,
            )
        }
        (Some(_), Some(_)) => {
            return Err(anyhow!("use either --tick-index or --price, not both"));
        }
        (None, None) => None,
    };

    let batch_tick_indices = match (args.tick_indices.as_deref(), args.prices.as_deref()) {
        (Some(raw), None) => Some(parse_csv_i64(raw)?),
        (None, Some(raw)) => {
            let tick = require_price_tick(&args)?;
            let prices = parse_csv_f64(raw)?;
            let tick_indices = prices
                .into_iter()
                .map(|price| {
                    price_to_tick_index(price, tick)
                        .ok_or_else(|| anyhow!("failed to convert price={} to tick_index", price))
                })
                .collect::<Result<Vec<_>>>()?;
            Some(tick_indices)
        }
        (Some(_), Some(_)) => {
            return Err(anyhow!("use either --tick-indices or --prices, not both"));
        }
        (None, None) => None,
    };

    let client = create_client(&args)?;
    let print_details = !args.summary_only && args.repeat == 1;
    let sleep_interval = Duration::from_millis(args.interval_ms);

    let mut latencies_ms = Vec::with_capacity(args.repeat);
    let mut total_delta_count = 0usize;
    let mut max_abs_delta = 0.0f64;
    for run_idx in 0..args.repeat {
        let started = Instant::now();
        let run_stats = execute_once(
            &args,
            &client,
            &symbol,
            single_tick_index,
            batch_tick_indices.as_deref(),
            print_details,
        )?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        latencies_ms.push(elapsed_ms);
        total_delta_count += run_stats.delta_count;
        max_abs_delta = max_abs_delta.max(run_stats.max_abs_delta);

        if args.repeat > 1 && !args.summary_only {
            println!("RUN {} latency_ms={:.3}", run_idx + 1, elapsed_ms);
        }
        if run_idx + 1 < args.repeat && !sleep_interval.is_zero() {
            thread::sleep(sleep_interval);
        }
    }

    if args.repeat > 1 || args.summary_only {
        let mut sorted = latencies_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let avg_ms = latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64;
        let p50_ms = percentile(&sorted, 0.50).unwrap_or(0.0);
        let p95_ms = percentile(&sorted, 0.95).unwrap_or(0.0);
        println!(
            "BENCHMARK venue={} symbol={} repeat={} persistent={} interval_ms={}",
            client.venue_slug(),
            symbol,
            args.repeat,
            args.persistent,
            args.interval_ms
        );
        println!("AVG_MS={:.3}", avg_ms);
        println!("P50_MS={:.3}", p50_ms);
        println!("P95_MS={:.3}", p95_ms);
        println!("MIN_MS={:.3}", sorted.first().copied().unwrap_or(0.0));
        println!("MAX_MS={:.3}", sorted.last().copied().unwrap_or(0.0));
        if args.top5 {
            println!(
                "TOP5_BATCH_DELTA count={} max_abs_delta={}",
                total_delta_count, max_abs_delta
            );
        }
    }

    Ok(())
}
