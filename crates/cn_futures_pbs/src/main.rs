use anyhow::{bail, Context, Result};
use clap::{ArgGroup, Parser};
use cn_futures_pbs::{
    config::Config,
    ipc::Publishers,
    msg::SOURCE_RESET,
    sources::{self, Source},
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(
    name = "cn-futures-pbs",
    about = "One native futures L2 IPC stream; source selection is exclusive"
)]
#[command(group(ArgGroup::new("source").required(true).args(["zmq","ctp","colo"])))]
struct Args {
    /// Company-intranet Tonglian MDL relay. No vendor SDK.
    #[arg(long)]
    zmq: bool,
    /// Official CTP MdApi; requires ctp-sdk and an authorized external host.
    #[arg(long)]
    ctp: bool,
    /// Reserved colo source. No adapter or endpoint has been supplied yet.
    #[arg(long)]
    colo: bool,
    #[arg(long)]
    config: PathBuf,
    /// Validate only; no connection, IPC or SDK initialization.
    #[arg(long)]
    check_config: bool,
    /// Optional bounded read-only run.
    #[arg(long)]
    seconds: Option<u64>,
    #[arg(long)]
    max_messages: Option<u64>,
}
fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    if args.colo {
        bail!("--colo is not implemented: supply the actual colo protocol and endpoint first; no network/SDK started");
    }
    if args.zmq {
        config
            .zmq
            .as_ref()
            .context("missing [zmq] settings")?
            .validate()?;
    }
    if args.ctp {
        anyhow::ensure!(
            config.ctp_config.as_ref().is_some_and(|p| p.is_absolute()),
            "--ctp requires an absolute ctp_config path"
        );
    }
    if args.check_config {
        println!("cn-futures-pbs configuration valid; no network, SDK or IPC started");
        return Ok(());
    }
    let mut source: Box<dyn Source> = if args.zmq {
        let z = config.zmq.as_ref().context("missing [zmq]")?;
        eprintln!(
            "cn-futures-pbs: company_intranet Tonglian relay {}; live lossy snapshots; no replay",
            z.endpoint
        );
        Box::new(sources::zmq::ZmqSource::new(&z.endpoint, z.receive_hwm)?)
    } else {
        #[cfg(feature = "ctp-sdk")]
        {
            Box::new(sources::ctp::CtpSource::new(
                config.ctp_config.as_ref().context("missing ctp_config")?,
            )?)
        }
        #[cfg(not(feature = "ctp-sdk"))]
        {
            bail!("--ctp requires a ctp-sdk build; CTP execution is permitted only on an explicitly authorized external host");
        }
    };
    let mut publishers = Publishers::new(&config.service_root)?;
    let start = Instant::now();
    let mut count = 0u64;
    let mut reset = 0u64;
    let mut cycles = 0u64;
    let mut stats = Instant::now();
    let mut generation = 0;
    let result = (|| -> Result<()> {
        loop {
            if args
                .seconds
                .is_some_and(|s| start.elapsed() >= Duration::from_secs(s))
                || args.max_messages.is_some_and(|n| count >= n)
            {
                break;
            }
            if let Some(msg) = source.poll()? {
                generation = msg.source_generation;
                if msg.flags & SOURCE_RESET != 0 {
                    reset += 1;
                } else {
                    count += 1;
                }
                publishers.publish(msg)?;
            } else {
                std::hint::spin_loop();
            }
            cycles = cycles.wrapping_add(1);
            if cycles.is_multiple_of(1024) && !publishers.running() {
                break;
            }
            if stats.elapsed() >= Duration::from_secs(10) {
                eprintln!("cn-futures-pbs: published={count} continuity_resets={reset}");
                stats = Instant::now();
            }
        }
        Ok(())
    })();
    let stopped = publishers.publish(cn_futures_pbs::msg::MarketDataMsg::reset(
        generation.saturating_add(1),
    ));
    if let Err(error) = &stopped {
        eprintln!("cn-futures-pbs: shutdown reset failed: {error:#}");
    }
    eprintln!("cn-futures-pbs: stopped published={count} continuity_resets={reset}");
    result.and(stopped)
}
