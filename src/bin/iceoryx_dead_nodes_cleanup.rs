use anyhow::{Context, Result};
use clap::Parser;
use iceoryx2::config::Config;
use iceoryx2::node::{NodeState, NodeView};
use iceoryx2::prelude::*;

#[derive(Debug, Parser)]
#[command(
    name = "iceoryx_dead_nodes_cleanup",
    about = "Cleanup stale iceoryx resources owned by dead nodes only"
)]
struct Args {
    /// List dead nodes only, do not remove resources.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut dead_nodes = 0usize;
    let mut cleaned = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    Node::<ipc::Service>::list(Config::global_config(), |node_state| {
        if let NodeState::<ipc::Service>::Dead(dead) = node_state {
            dead_nodes += 1;

            let (name, exe) = dead
                .details()
                .as_ref()
                .map(|d| (d.name().as_str().to_owned(), d.executable().to_string()))
                .unwrap_or_else(|| ("<unknown>".to_owned(), "<unknown>".to_owned()));

            println!(
                "[dead-node] id={} pid={} name={} exe={}",
                dead.id().value(),
                dead.id().pid().value(),
                name,
                exe
            );

            if args.dry_run {
                return CallbackProgression::Continue;
            }

            match dead.remove_stale_resources() {
                Ok(true) => {
                    cleaned += 1;
                    println!("  -> cleaned");
                }
                Ok(false) => {
                    skipped += 1;
                    println!("  -> skipped (already cleaned or held by another cleaner)");
                }
                Err(err) => {
                    failed += 1;
                    println!("  -> failed: {err:?}");
                }
            }
        }
        CallbackProgression::Continue
    })
    .context("failed to list iceoryx nodes")?;

    println!(
        "summary: dead_nodes={}, cleaned={}, skipped={}, failed={}",
        dead_nodes, cleaned, skipped, failed
    );

    if failed > 0 {
        anyhow::bail!(
            "cleanup finished with {} failed dead-node cleanup(s)",
            failed
        );
    }

    Ok(())
}
