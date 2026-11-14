use anyhow::{bail, Context, Result};
use cn_futures::config::Config;
use std::path::PathBuf;
#[cfg(feature = "ctp-sdk")]
static STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(all(feature = "ctp-sdk", target_os = "linux"))]
extern "C" fn stop(_: i32) {
    STOP.store(true, std::sync::atomic::Ordering::Release);
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ctp-trader: {error:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config = None;
    let mut market_data_config: Option<PathBuf> = None;
    let mut connect = false;
    let mut execute = false;
    let mut confirm_settlement = false;
    let mut check_config = false;
    let mut sdk_version: Option<PathBuf> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--market-data-config" => {
                market_data_config = Some(PathBuf::from(
                    args.next()
                        .context("--market-data-config requires a path")?,
                ));
            }
            "--config" => {
                config = Some(PathBuf::from(
                    args.next().context("--config requires a path")?,
                ))
            }
            "--sdk-version" => {
                sdk_version = Some(PathBuf::from(
                    args.next()
                        .context("--sdk-version requires an official SDK library path")?,
                ))
            }
            "--connect" => connect = true,
            "--execute" => execute = true,
            "--confirm-settlement" => confirm_settlement = true,
            "--check-config" => check_config = true,
            "--help" | "-h" => {
                println!("ctp-trader --config PATH [--market-data-config MD_PATH] [--check-config] [--connect [--execute] [--confirm-settlement]]\nctp-trader --sdk-version /absolute/path/libthosttraderapi_se.so\n\nDefault: validate configuration only; no network or credentials required.\n--market-data-config consumes MdApi on the trading business thread; built-in strategy is NoStrategy and never produces orders.\n--connect logs in and queries account state without confirming settlement.\n--confirm-settlement explicitly permits confirming the broker settlement.\n--execute additionally permits explicit JSON-line submit/cancel commands.\nWithout --execute, no order placement or cancellation is permitted.\nCommands: state, reconcile, submit, cancel, shutdown. EOF stops trade-only mode; combined MD mode continues. SIGINT/SIGTERM request shutdown.\nExisting venue orders are never automatically cancelled on startup or exit.");
                return Ok(());
            }
            _ => bail!("unknown argument; use --help"),
        }
    }
    anyhow::ensure!(
        !(check_config && connect),
        "--check-config cannot be combined with --connect"
    );
    if let Some(path) = sdk_version {
        anyhow::ensure!(
            !connect && !execute && config.is_none() && market_data_config.is_none(),
            "--sdk-version is an offline standalone operation"
        );
        #[cfg(feature = "ctp-sdk")]
        {
            println!("{}", cn_futures::ctp::sdk::version(&path)?);
            return Ok(());
        }
        #[cfg(not(feature = "ctp-sdk"))]
        {
            let _ = path;
            bail!("build with --features ctp-sdk and CTP_SDK_DIR to load the official SDK");
        }
    }
    let mut config = Config::load(&config.context("--config is required; use --help")?)?;
    let market_data = market_data_config
        .map(|path| cn_futures::ctp::market_data::Config::load(&path))
        .transpose()?;
    if let Some(md) = &market_data {
        config.validate_market_data(md)?;
    }
    if confirm_settlement {
        anyhow::ensure!(connect, "settlement confirmation requires --connect");
        config.confirm_settlement = true;
    }
    if !connect {
        anyhow::ensure!(!execute, "--execute also requires --connect");
        println!(
            "CTP configuration valid; {} domestic futures contracts; no connection started",
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
        cn_futures::runtime::run_with_strategy(
            config,
            market_data,
            execute,
            cn_futures::ctp::strategy::NoStrategy,
            &STOP,
        )
    }
    #[cfg(not(feature = "ctp-sdk"))]
    {
        let _ = (config, market_data, execute);
        bail!("build with --features ctp-sdk and CTP_SDK_DIR for CTP network access");
    }
}
