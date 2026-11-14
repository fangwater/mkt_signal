use anyhow::Result;
use clap::Parser;
use cme_tas_replay::causal_roll::{replay, Input};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
}
fn main() -> Result<()> {
    let a = Args::parse();
    let raw = fs::read(a.input)?;
    let input: Input = serde_json::from_slice(&raw)?;
    let decisions = replay(&input)?;
    if let Some(parent) = a.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(a.output)?;
    serde_json::to_writer_pretty(
        file,
        &serde_json::json!({"input_sha256":format!("{:x}",Sha256::digest(&raw)),"decisions":decisions}),
    )?;
    Ok(())
}
