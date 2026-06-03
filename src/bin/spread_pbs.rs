use anyhow::{bail, Result};
use clap::Parser;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::path::PathBuf;
use tokio::sync::watch;

use mkt_signal::cfg::Config;
use mkt_signal::common::affinity::pin_to_core;
use mkt_signal::signal::common::TradingVenue;
use mkt_signal::spread_pbs::SpreadPbsApp;

#[derive(Parser)]
#[command(name = "spread_pbs")]
#[command(about = "Dedicated high-speed askbidspread publisher (pinned core).")]
struct Args {
    /// Trading venue. Also accepts <exchange>-both to run margin+futures in one process.
    #[arg(short, long, value_parser = parse_venue_selection)]
    venue: SpreadVenueSelection,

    /// 绑定到的 CPU 核心编号
    #[arg(short, long)]
    core: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    pin_to_core(args.core)?;

    let config_path = resolve_cfg_path()?;
    log::info!("spread_pbs cfg path: {}", config_path.display());
    let config_str = config_path.to_string_lossy();
    log::info!("spread_pbs venue selection: {}", args.venue.label());
    let configs = load_selected_configs(&config_str, &args.venue).await?;

    // current_thread runtime + spawn_local 需要 LocalSet 上下文
    let local = tokio::task::LocalSet::new();
    local.run_until(run_selected(configs)).await
}

#[derive(Debug, Clone, Copy)]
enum SpreadVenueSelection {
    Single(TradingVenue),
    Both {
        exchange: &'static str,
        margin: TradingVenue,
        futures: TradingVenue,
    },
}

impl SpreadVenueSelection {
    fn venues(self) -> Vec<TradingVenue> {
        match self {
            Self::Single(venue) => vec![venue],
            Self::Both {
                margin, futures, ..
            } => vec![margin, futures],
        }
    }

    fn label(self) -> String {
        match self {
            Self::Single(venue) => venue.data_pub_slug().to_string(),
            Self::Both { exchange, .. } => format!("{exchange}-both"),
        }
    }
}

fn parse_venue_selection(raw: &str) -> std::result::Result<SpreadVenueSelection, String> {
    let normalized = raw.trim().to_ascii_lowercase().replace('_', "-");
    if let Some(exchange) = normalized.strip_suffix("-both") {
        return both_selection_for_exchange(exchange).ok_or_else(|| {
            format!(
                "unsupported spread_pbs both venue '{raw}', expected one of binance-both/okex-both/bybit-both/bitget-both/gate-both"
            )
        });
    }
    single_venue_from_slug(&normalized)
        .map(SpreadVenueSelection::Single)
        .ok_or_else(|| format!("unsupported trading venue '{raw}'"))
}

fn both_selection_for_exchange(exchange: &str) -> Option<SpreadVenueSelection> {
    let (exchange, margin, futures) = match exchange {
        "binance" => (
            "binance",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        ),
        "okex" | "okx" => ("okex", TradingVenue::OkexMargin, TradingVenue::OkexFutures),
        "bybit" => (
            "bybit",
            TradingVenue::BybitMargin,
            TradingVenue::BybitFutures,
        ),
        "bitget" => (
            "bitget",
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
        ),
        "gate" => ("gate", TradingVenue::GateMargin, TradingVenue::GateFutures),
        _ => return None,
    };
    Some(SpreadVenueSelection::Both {
        exchange,
        margin,
        futures,
    })
}

fn single_venue_from_slug(slug: &str) -> Option<TradingVenue> {
    match slug {
        "binance-margin" => Some(TradingVenue::BinanceMargin),
        "binance-futures" => Some(TradingVenue::BinanceFutures),
        "okex-margin" | "okx-margin" => Some(TradingVenue::OkexMargin),
        "okex-futures" | "okx-futures" => Some(TradingVenue::OkexFutures),
        "bybit-margin" => Some(TradingVenue::BybitMargin),
        "bybit-futures" => Some(TradingVenue::BybitFutures),
        "bitget-margin" => Some(TradingVenue::BitgetMargin),
        "bitget-futures" => Some(TradingVenue::BitgetFutures),
        "gate-margin" => Some(TradingVenue::GateMargin),
        "gate-futures" => Some(TradingVenue::GateFutures),
        "aster-margin" => Some(TradingVenue::AsterMargin),
        "aster-futures" => Some(TradingVenue::AsterFutures),
        "hyperliquid-margin" => Some(TradingVenue::HyperliquidMargin),
        "hyperliquid-futures" => Some(TradingVenue::HyperliquidFutures),
        _ => None,
    }
}

async fn load_selected_configs(
    config_path: &str,
    selection: &SpreadVenueSelection,
) -> Result<Vec<Config>> {
    let mut configs = Vec::new();
    for venue in selection.venues() {
        configs.push(Config::load_config(config_path, venue).await?);
    }
    Ok(configs)
}

async fn run_selected(configs: Vec<Config>) -> Result<()> {
    let labels: Vec<&'static str> = configs
        .iter()
        .map(|config| config.venue.data_pub_slug())
        .collect();
    log::info!("spread_pbs selected venues: {}", labels.join(","));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = FuturesUnordered::new();
    for config in configs {
        let venue_slug = config.venue.data_pub_slug();
        let rx = shutdown_rx.clone();
        let app = SpreadPbsApp::new(config);
        tasks.push(tokio::task::spawn_local(async move {
            (venue_slug, app.run_with_shutdown(rx).await)
        }));
    }

    let ctrl_c_tx = shutdown_tx.clone();
    let ctrl_c_task = tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::warn!("spread_pbs ctrl-c listener failed: {:#}", e);
        }
        let _ = ctrl_c_tx.send(true);
    });

    let mut first_err: Option<anyhow::Error> = None;
    while let Some(result) = tasks.next().await {
        match result {
            Ok((_, Ok(()))) => {}
            Ok((venue_slug, Err(e))) => {
                let _ = shutdown_tx.send(true);
                log::error!("spread_pbs[{}] exited with error: {:#}", venue_slug, e);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                let _ = shutdown_tx.send(true);
                log::error!("spread_pbs task join failed: {:#}", e);
                if first_err.is_none() {
                    first_err = Some(e.into());
                }
            }
        }
    }
    ctrl_c_task.abort();

    if let Some(e) = first_err {
        Err(e)
    } else {
        Ok(())
    }
}

/// 配置文件路径优先级：
/// 1) `$HOME/spread_pbs/config/mkt_cfg.yaml`（spread_pbs 自己的部署目录）
/// 2) `$HOME/dat_pbs/config/mkt_cfg.yaml`（兜底，复用 dat_pbs 那份）
fn resolve_cfg_path() -> Result<PathBuf> {
    let home = std::env::var("HOME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("USER").ok().map(|u| format!("/home/{}", u)));
    let Some(home) = home else {
        bail!("HOME/USER not set; cannot resolve mkt_cfg.yaml");
    };

    let primary = PathBuf::from(&home).join("spread_pbs/config/mkt_cfg.yaml");
    if primary.exists() {
        return Ok(primary);
    }
    let fallback = PathBuf::from(&home).join("dat_pbs/config/mkt_cfg.yaml");
    if fallback.exists() {
        return Ok(fallback);
    }
    bail!(
        "mkt_cfg.yaml not found at {} or {}",
        primary.display(),
        fallback.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_venue() {
        let selection = parse_venue_selection("bitget-futures").unwrap();
        assert!(matches!(
            selection,
            SpreadVenueSelection::Single(TradingVenue::BitgetFutures)
        ));
    }

    #[test]
    fn parses_both_venue() {
        let selection = parse_venue_selection("gate-both").unwrap();
        assert!(matches!(
            selection,
            SpreadVenueSelection::Both {
                exchange: "gate",
                margin: TradingVenue::GateMargin,
                futures: TradingVenue::GateFutures
            }
        ));
        assert_eq!(selection.label(), "gate-both");
        assert_eq!(
            selection.venues(),
            vec![TradingVenue::GateMargin, TradingVenue::GateFutures]
        );
    }

    #[test]
    fn parses_underscore_alias() {
        let selection = parse_venue_selection("okx_both").unwrap();
        assert!(matches!(
            selection,
            SpreadVenueSelection::Both {
                exchange: "okex",
                margin: TradingVenue::OkexMargin,
                futures: TradingVenue::OkexFutures
            }
        ));
    }
}
