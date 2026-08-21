use anyhow::{bail, Context, Result};
use clap::Parser;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::path::{Path, PathBuf};
use tokio::sync::watch;

use mkt_signal::cfg::Config;
use mkt_signal::spread_pbs::publisher::SpreadPbsPublishRoots;
use mkt_signal::spread_pbs::{BinanceFuturesRole, BybitRole, SpreadPbsApp};
use order_common::TradingVenue;
use runtime_common::affinity::pin_to_core;

#[derive(Parser)]
#[command(name = "spread_pbs")]
#[command(about = "Dedicated high-speed askbidspread publisher (pinned core).")]
struct Args {
    /// Trading venue. Also accepts <exchange>-both.
    #[arg(short, long, value_parser = parse_venue_selection)]
    venue: SpreadVenueSelection,

    /// 绑定到的 CPU 核心编号
    #[arg(short, long)]
    core: usize,

    /// Publish to isolated test channels instead of production market-data channels.
    #[arg(long)]
    test: bool,

    /// Binance futures only: full, market, or bookticker.
    #[arg(long, value_parser = parse_binance_futures_role, default_value = "full")]
    binance_futures_role: BinanceFuturesRole,

    /// Bybit only: full, market, or bookticker.
    #[arg(long, value_parser = parse_bybit_role, default_value = "full")]
    bybit_role: BybitRole,
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
    validate_role_selection(&args.venue, args.binance_futures_role, args.bybit_role)?;
    log::info!(
        "spread_pbs binance_futures_role={} bybit_role={}",
        args.binance_futures_role.as_str(),
        args.bybit_role.as_str(),
    );
    let publish_roots = if args.test {
        SpreadPbsPublishRoots::test()
    } else {
        SpreadPbsPublishRoots::production()
    };
    log::info!(
        "spread_pbs publish roots: spread_root={} dat_root={} test={}",
        publish_roots.spread_root(),
        publish_roots.dat_root(),
        args.test
    );
    let configs = load_selected_configs(&config_str, &args.venue).await?;

    // current_thread runtime + spawn_local 需要 LocalSet 上下文
    let local = tokio::task::LocalSet::new();
    local
        .run_until(run_selected(
            configs,
            publish_roots,
            args.binance_futures_role,
            args.bybit_role,
        ))
        .await
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

fn parse_binance_futures_role(raw: &str) -> std::result::Result<BinanceFuturesRole, String> {
    BinanceFuturesRole::parse(raw)
}

fn parse_bybit_role(raw: &str) -> std::result::Result<BybitRole, String> {
    BybitRole::parse(raw)
}

fn validate_role_selection(
    selection: &SpreadVenueSelection,
    role: BinanceFuturesRole,
    bybit_role: BybitRole,
) -> Result<()> {
    if role != BinanceFuturesRole::Full {
        match selection {
            SpreadVenueSelection::Single(TradingVenue::BinanceFutures) => {}
            _ => bail!(
                "--binance-futures-role={} only supports --venue binance-futures",
                role.as_str()
            ),
        }
    }

    if bybit_role != BybitRole::Full {
        match selection {
            SpreadVenueSelection::Single(
                TradingVenue::BybitMargin | TradingVenue::BybitFutures,
            )
            | SpreadVenueSelection::Both {
                exchange: "bybit", ..
            } => {}
            _ => bail!(
                "--bybit-role={} only supports --venue bybit-margin/bybit-futures/bybit-both",
                bybit_role.as_str()
            ),
        }
    }

    Ok(())
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
        "binance-coin-futures" => Some(TradingVenue::BinanceCoinFutures),
        "okex-margin" | "okx-margin" => Some(TradingVenue::OkexMargin),
        "okex-futures" | "okx-futures" => Some(TradingVenue::OkexFutures),
        "bybit-margin" => Some(TradingVenue::BybitMargin),
        "bybit-futures" => Some(TradingVenue::BybitFutures),
        "bitget-margin" => Some(TradingVenue::BitgetMargin),
        "bitget-futures" => Some(TradingVenue::BitgetFutures),
        "bitget-coin-futures" => Some(TradingVenue::BitgetCoinFutures),
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

async fn run_selected(
    configs: Vec<Config>,
    publish_roots: SpreadPbsPublishRoots,
    binance_futures_role: BinanceFuturesRole,
    bybit_role: BybitRole,
) -> Result<()> {
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
        let role = if config.venue == TradingVenue::BinanceFutures {
            binance_futures_role
        } else {
            BinanceFuturesRole::Full
        };
        let bybit_role = if matches!(
            config.venue,
            TradingVenue::BybitMargin | TradingVenue::BybitFutures
        ) {
            bybit_role
        } else {
            BybitRole::Full
        };
        let app = SpreadPbsApp::new_with_publish_roots_and_roles(
            config,
            publish_roots.clone(),
            role,
            bybit_role,
        );
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
/// 1) `./config/mkt_cfg.yaml`（当前 venue 部署目录）
/// 2) `$HOME/spread_pbs/config/mkt_cfg.yaml`（共享部署配置）
/// 3) `$HOME/dat_pbs/config/mkt_cfg.yaml`（兜底，复用 dat_pbs 那份）
fn resolve_cfg_path() -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("resolve spread_pbs current directory")?;
    resolve_cfg_path_from(&current_dir)
}

fn resolve_cfg_path_from(current_dir: &Path) -> Result<PathBuf> {
    let venue_config = current_dir.join("config/mkt_cfg.yaml");
    if venue_config.exists() {
        return Ok(venue_config);
    }

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
        "mkt_cfg.yaml not found at {}, {}, or {}",
        venue_config.display(),
        primary.display(),
        fallback.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_config_from_current_venue_directory() {
        let repo_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let resolved = resolve_cfg_path_from(&repo_dir).unwrap();

        assert_eq!(resolved, repo_dir.join("config/mkt_cfg.yaml"));
    }

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
    fn parses_binance_both() {
        let selection = parse_venue_selection("binance-both").unwrap();
        assert!(matches!(
            selection,
            SpreadVenueSelection::Both {
                exchange: "binance",
                margin: TradingVenue::BinanceMargin,
                futures: TradingVenue::BinanceFutures
            }
        ));
        assert_eq!(selection.label(), "binance-both");
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

    #[test]
    fn validates_binance_futures_split_roles() {
        let selection = parse_venue_selection("binance-futures").unwrap();
        validate_role_selection(&selection, BinanceFuturesRole::Market, BybitRole::Full).unwrap();
        validate_role_selection(&selection, BinanceFuturesRole::BookTicker, BybitRole::Full)
            .unwrap();

        let both = parse_venue_selection("binance-both").unwrap();
        assert!(
            validate_role_selection(&both, BinanceFuturesRole::Market, BybitRole::Full).is_err()
        );
    }
}
