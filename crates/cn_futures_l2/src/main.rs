//! CLI: one process, N TradDay workers, one RocksDB.

use anyhow::{bail, Result};
use clap::Parser;
use cn_futures_l2::{parse_day, Exchange, ReplayArgs, DEFAULT_LOOKBACK_DAYS, DEFAULT_ROCKSDB_DIR};
use std::path::PathBuf;

const EXCHANGES: &[&str] = &["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];

#[derive(Parser, Debug)]
#[command(about = "Replay Tonglian future L2 into a year+product RocksDB")]
struct Args {
    #[arg(long)]
    l2_root: Option<PathBuf>,
    #[arg(long)]
    msg_root: Option<PathBuf>,
    #[arg(long)]
    exchange: Option<String>,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value_t = 32)]
    workers: usize,
    #[arg(long)]
    overlap_cut: Option<String>,
    #[arg(long, default_value_t = DEFAULT_LOOKBACK_DAYS)]
    lookback_days: u64,
}

fn parse_exchanges(raw: Option<&str>) -> Result<Vec<Exchange>> {
    match raw {
        None => Ok(EXCHANGES
            .iter()
            .map(|code| Exchange::parse(code).expect("known exchange"))
            .collect()),
        Some(text) => {
            let mut out = Vec::new();
            for part in text.split(',') {
                let Some(exchange) = Exchange::parse(part) else {
                    bail!("unsupported exchange: {part}");
                };
                out.push(exchange);
            }
            if out.is_empty() {
                bail!("--exchange is empty");
            }
            Ok(out)
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.l2_root.is_none() && args.msg_root.is_none() {
        bail!("--l2-root or --msg-root is required");
    }
    let both_roots = args.l2_root.is_some() && args.msg_root.is_some();
    let replay = ReplayArgs {
        l2_root: args.l2_root,
        msg_root: args.msg_root,
        exchanges: parse_exchanges(args.exchange.as_deref())?,
        start: parse_day(&args.start)?,
        end: parse_day(&args.end)?,
        rocksdb_dir: args.rocksdb_dir,
        workers: args.workers,
        overlap_cut: ReplayArgs::overlap_cut_or_default(args.overlap_cut.as_deref(), both_roots)?,
        lookback_days: args.lookback_days,
    };
    let (stats, days) = cn_futures_l2::run_replay(replay)?;
    eprintln!(
        "cn_l2 replay ok days={days} trades={} depths={} oi={} queues={}",
        stats.trades, stats.depths, stats.open_ints, stats.queues
    );
    Ok(())
}
