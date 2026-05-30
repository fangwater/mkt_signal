//! Target position execution signal publisher.

use anyhow::{Context, Result};
use clap::{ArgGroup, Parser};
use log::info;
use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::signal::common::{SignalBytes, TradingVenue};
use mkt_signal::signal::exec_signal::ExecPositionTargetCtx;
use mkt_signal::signal::target_pos_exec_signal::{TargetPosExecSignal, TargetPosMode};
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};
use std::io::Read;
use std::path::PathBuf;

const DEFAULT_CHANNEL: &str = "trade_signal";

#[derive(Parser, Debug)]
#[command(name = "target_pos_exec_signal")]
#[command(about = "Publish target position execution signals")]
#[command(group(
    ArgGroup::new("input")
        .required(true)
        .args(["json", "file", "stdin"])
))]
struct Args {
    /// Inline JSON target position message.
    #[arg(long)]
    json: Option<String>,

    /// Read JSON target position message from file.
    #[arg(long)]
    file: Option<PathBuf>,

    /// Read JSON target position message from stdin.
    #[arg(long)]
    stdin: bool,

    /// Exec venue for the target position.
    #[arg(long, value_enum)]
    venue: TradingVenue,

    /// Signal channel under signal_pubs/.
    #[arg(long, default_value = DEFAULT_CHANNEL)]
    channel: String,

    /// Parse and print the normalized signal without publishing.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

fn read_input(args: &Args) -> Result<String> {
    if let Some(json) = &args.json {
        return Ok(json.clone());
    }
    if let Some(path) = &args.file {
        return std::fs::read_to_string(path)
            .with_context(|| format!("failed to read json file {}", path.display()));
    }
    if args.stdin {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .context("failed to read json from stdin")?;
        return Ok(input);
    }
    anyhow::bail!("missing input")
}

fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    let args = Args::parse();
    let input = read_input(&args)?;
    let generation_time = get_timestamp_us();
    let signal =
        TargetPosExecSignal::from_json_str(&input, generation_time).map_err(anyhow::Error::msg)?;
    if signal.mode != TargetPosMode::Qty {
        anyhow::bail!(
            "exec position target currently supports qty mode only, got {}",
            signal.mode.as_str()
        );
    }
    let normalized = serde_json::to_string_pretty(&signal.to_json_value())?;

    if args.dry_run {
        println!("{normalized}");
        return Ok(());
    }

    let publisher = SignalPublisher::new(&args.channel)
        .with_context(|| format!("failed to create signal publisher channel={}", args.channel))?;
    let mut published = 0usize;
    let mut total_bytes = 0usize;
    for target in &signal.targets {
        let mut ctx = ExecPositionTargetCtx::new();
        ctx.set_exec_venue(args.venue);
        ctx.set_exec_symbol(&target.symbol());
        ctx.target_qty = target.value();
        ctx.generation_time = signal.generation_time;
        ctx.set_from_key(b"target_pos_exec_signal".to_vec());
        let payload = TradeSignal::create(
            SignalType::ExecPositionTarget,
            signal.generation_time,
            signal.generation_time as f64,
            ctx.to_bytes(),
        )
        .to_bytes();
        total_bytes += payload.len();
        publisher
            .publish(&payload)
            .with_context(|| format!("failed to publish signal channel={}", args.channel))?;
        published += 1;
    }

    info!(
        "target position execution signal published: channel={} venue={:?} mode={} targets={} messages={} bytes={} generation_time={}",
        args.channel,
        args.venue,
        signal.mode.as_str(),
        signal.targets.len(),
        published,
        total_bytes,
        signal.generation_time
    );
    println!("{normalized}");

    Ok(())
}
