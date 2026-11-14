use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use usstock_lseg_raw_replay::parsed::repartition_parsed;

#[derive(Debug, Parser)]
#[command(about = "Rewrite completed direct RAW parsed segments into logical source-order shards")]
struct Args {
    #[arg(long)]
    input_dir: PathBuf,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 16)]
    workers: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let manifest = repartition_parsed(&args.input_dir, &args.output_dir, args.workers)
        .with_context(|| format!("repartition parsed RAW {}", args.input_dir.display()))?;
    println!(
        "repartitioned RAW complete messages={} segments={} encoded_bytes={} compressed_bytes={}",
        manifest.source_messages,
        manifest.segments.len(),
        manifest.encoded_bytes,
        manifest.compressed_bytes
    );
    Ok(())
}
