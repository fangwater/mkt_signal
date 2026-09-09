use anyhow::{bail, Context, Result};
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;
use usstock_lseg_raw_replay::{parsed::partition, Manifest, MANIFEST_FILE};

#[derive(Debug, Parser)]
#[command(about = "Parse RAW CSV shards into ordered per-RIC binary segments")]
struct Args {
    #[arg(long)]
    staging_dir: Option<PathBuf>,
    #[arg(long)]
    input: Vec<PathBuf>,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 32)]
    workers: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.staging_dir.is_some() == !args.input.is_empty() {
        bail!("provide exactly one of --staging-dir or one or more --input");
    }
    let inputs = if let Some(root) = &args.staging_dir {
        let manifest: Manifest = serde_json::from_reader(File::open(root.join(MANIFEST_FILE))?)?;
        manifest.validate(&manifest.period, true)?;
        manifest.shards.iter().map(|s| root.join(&s.file)).collect()
    } else {
        args.input
    };
    let manifest = partition(&inputs, &args.output_dir, args.workers)
        .with_context(|| format!("partition {} RAW shards", inputs.len()))?;
    println!(
        "parsed RAW complete messages={} segments={} encoded_bytes={} compressed_bytes={}",
        manifest.source_messages,
        manifest.segments.len(),
        manifest.encoded_bytes,
        manifest.compressed_bytes
    );
    Ok(())
}
