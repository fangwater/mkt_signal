use anyhow::{bail, Context, Result};
use cn_futures::ctp::market_data::Config;
use std::path::PathBuf;
#[cfg(feature = "ctp-sdk")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "ctp-sdk")]
static STOP: AtomicBool = AtomicBool::new(false);
#[cfg(all(feature = "ctp-sdk", target_os = "linux"))]
extern "C" fn stop(_: i32) {
    STOP.store(true, Ordering::Release);
}
fn main() {
    if let Err(error) = run() {
        eprintln!("ctp-md: {error:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config = None;
    let mut connect = false;
    let mut seconds = None;
    let mut sdk = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config = Some(PathBuf::from(
                    args.next().context("--config requires path")?,
                ))
            }
            "--connect" => connect = true,
            "--duration-seconds" => {
                let value: u64 = args.next().context("duration required")?.parse()?;
                anyhow::ensure!(value > 0, "duration must be positive");
                seconds = Some(value);
            }
            "--sdk-version" => sdk = Some(PathBuf::from(args.next().context("SDK path required")?)),
            "--help" | "-h" => {
                println!("ctp-md --config PATH [--connect] [--duration-seconds N]\nctp-md --sdk-version LIBRARY\nDefault validates only. --connect continuously logs in/subscribes and republishes JSON-line market data. Reconnects re-login and re-subscribe. No trading or settlement calls.");
                return Ok(());
            }
            _ => bail!("unknown option; use --help"),
        }
    }
    if let Some(path) = sdk {
        anyhow::ensure!(
            !connect && config.is_none() && seconds.is_none(),
            "SDK version is an offline standalone operation"
        );
        #[cfg(feature = "ctp-sdk")]
        {
            println!("{}", cn_futures::ctp::market_data::version(&path)?);
            return Ok(());
        }
        #[cfg(not(feature = "ctp-sdk"))]
        {
            let _ = path;
            bail!("build with --features ctp-sdk");
        }
    }
    let config = Config::load(&config.context("--config required")?)?;
    if !connect {
        println!(
            "CTP MD configuration valid; {} subscriptions; no network connection",
            config.symbols.len()
        );
        return Ok(());
    }
    #[cfg(feature = "ctp-sdk")]
    {
        #[cfg(target_os = "linux")]
        unsafe {
            libc::signal(libc::SIGINT, stop as *const () as libc::sighandler_t);
            libc::signal(libc::SIGTERM, stop as *const () as libc::sighandler_t);
        }
        cn_futures::ctp::market_data::run(
            config,
            seconds.map(std::time::Duration::from_secs),
            &STOP,
        )
    }
    #[cfg(not(feature = "ctp-sdk"))]
    {
        let _ = (config, seconds);
        bail!("build with --features ctp-sdk and official SDK headers");
    }
}
