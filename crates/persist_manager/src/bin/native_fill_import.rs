//! Independent offline native ordinary-fill import; default is read-only.
use anyhow::Result;
use clap::Parser;
use persist_manager::native_fill_import::Inputs;
use std::path::PathBuf;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    binding: PathBuf,
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    checklist: PathBuf,
    #[arg(long)]
    apply: bool,
    /// New, absolute full RocksDB checkpoint path, required before import writes.
    #[arg(long)]
    backup: Option<PathBuf>,
    /// Audit batch ID to undo; also defaults to dry-run, needs --apply to delete.
    #[arg(long)]
    rollback: Option<String>,
}
fn main() -> Result<()> {
    let a = Args::parse();
    let input = Inputs::load(&a.binding, &a.manifest, &a.checklist)?;
    let result = if let Some(id) = a.rollback {
        input.rollback(&id, a.apply)?
    } else {
        serde_json::to_value(input.run(a.apply, a.backup.as_deref())?)?
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
