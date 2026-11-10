use anyhow::{Context, Result};
use std::path::PathBuf;
fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut database = None;
    let mut output = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input-dir" => {
                database = Some(PathBuf::from(args.next().context("missing --input-dir")?))
            }
            "--output-dir" => {
                output = Some(PathBuf::from(args.next().context("missing --output-dir")?))
            }
            "--help" | "-h" => {
                println!("cn_order_export --input-dir /absolute/RocksDB --output-dir OUTPUT\nStop the native trader before exporting. Output: native orders, trade facts, evidence and metadata JSONL.");
                return Ok(());
            }
            _ => anyhow::bail!("unknown argument; use --help"),
        }
    }
    persist_manager::cn_futures::export(
        &database.context("--input-dir required")?,
        &output.context("--output-dir required")?,
    )
}
