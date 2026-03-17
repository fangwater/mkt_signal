use anyhow::{Context, Result};
use clap::Parser;
use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::exchange::Exchange;
use mkt_signal::fr_signal_dashboard::{run, FrDashboardConfig};
use mkt_signal::signal::common::TradingVenue;

const PROCESS_NAME: &str = "fr_signal_dashboard";

#[derive(Parser, Debug)]
#[command(name = "fr_signal_dashboard")]
#[command(about = "Read-only FR signal dashboard with websocket frontend")]
struct Args {
    #[arg(short, long)]
    exchange: Option<Exchange>,

    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    #[arg(long, default_value_t = 8811)]
    port: u16,

    #[arg(long, default_value = "/ws")]
    ws_path: String,

    #[arg(long, default_value_t = 1000)]
    refresh_ms: u64,
}

fn infer_namespace_and_key_suffix_from_cwd() -> Option<(String, String)> {
    use std::path::PathBuf;

    let cwd: PathBuf = std::env::current_dir().ok()?;
    let name = cwd.file_name()?.to_string_lossy().to_ascii_lowercase();
    parse_namespace_and_key_suffix(&name)
}

fn parse_namespace_and_key_suffix(name: &str) -> Option<(String, String)> {
    fn split_last_segment(input: &str) -> Option<(&str, &str)> {
        let idx = input.rfind(['-', '_'])?;
        let (head, tail_with_sep) = input.split_at(idx);
        let tail = tail_with_sep.get(1..)?;
        if head.is_empty() || tail.is_empty() {
            return None;
        }
        Some((head, tail))
    }

    let (base, _env_tag) = split_last_segment(name)?;
    let (prefix, ns) = split_last_segment(base)?;
    if prefix.is_empty() || ns.is_empty() {
        return None;
    }
    Some((ns.to_string(), prefix.to_string()))
}

fn normalize_exchange_str(raw: &str) -> &str {
    match raw {
        "okx" => "okex",
        other => other,
    }
}

fn venue_from_slug(raw: &str) -> Option<TradingVenue> {
    let slug = raw.trim().to_ascii_lowercase().replace('_', "-");
    match slug.as_str() {
        "binance-margin" => Some(TradingVenue::BinanceMargin),
        "binance-futures" => Some(TradingVenue::BinanceFutures),
        "okex-margin" => Some(TradingVenue::OkexMargin),
        "okex-futures" => Some(TradingVenue::OkexFutures),
        "bybit-margin" => Some(TradingVenue::BybitMargin),
        "bybit-futures" => Some(TradingVenue::BybitFutures),
        "bitget-margin" => Some(TradingVenue::BitgetMargin),
        "bitget-futures" => Some(TradingVenue::BitgetFutures),
        "gate-margin" => Some(TradingVenue::GateMargin),
        "gate-futures" => Some(TradingVenue::GateFutures),
        _ => None,
    }
}

fn infer_fr_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    let mut parts = suffix.split('_');
    let open = venue_from_slug(parts.next()?)?;
    let hedge = venue_from_slug(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((open, hedge))
}

fn fr_symbol_key_suffix(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut sigterm = unix_signal(SignalKind::terminate()).expect("failed to setup SIGTERM");
        let mut sigint = unix_signal(SignalKind::interrupt()).expect("failed to setup SIGINT");
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
        }
        token_clone.cancel();
    });
    Ok(())
}

fn resolve_config(args: Args) -> Result<FrDashboardConfig> {
    let (symbol_namespace, symbol_key_suffix, exchange, open_venue, hedge_venue) =
        match infer_namespace_and_key_suffix_from_cwd() {
            Some((ns, suffix)) if ns.eq_ignore_ascii_case("fr") => {
                let (open_venue, hedge_venue, inferred) =
                    if let Some((open, hedge)) = infer_fr_venues_from_key_suffix(&suffix) {
                        let open_ex = Exchange::from_str(open.trade_engine_exchange())
                            .context("invalid FR open venue exchange")?;
                        let hedge_ex = Exchange::from_str(hedge.trade_engine_exchange())
                            .context("invalid FR hedge venue exchange")?;
                        if open_ex != hedge_ex {
                            anyhow::bail!(
                                "FR venues must share the same exchange (open={:?}, hedge={:?})",
                                open,
                                hedge
                            );
                        }
                        (open, hedge, open_ex)
                    } else {
                        let expected = normalize_exchange_str(&suffix);
                        let inferred = Exchange::from_str(expected).with_context(|| {
                            format!(
                                "failed to infer exchange from CWD suffix='{}' (namespace=fr)",
                                suffix
                            )
                        })?;
                        let (open_venue, hedge_venue) =
                            mkt_signal::funding_rate::common::venue_pair_for_exchange(inferred);
                        (open_venue, hedge_venue, inferred)
                    };
                if let Some(cli_ex) = args.exchange {
                    if cli_ex != inferred {
                        anyhow::bail!(
                            "--exchange mismatch with CWD: cwd_exchange={} cli_exchange={}",
                            inferred,
                            cli_ex
                        );
                    }
                }
                (
                    ns,
                    fr_symbol_key_suffix(open_venue, hedge_venue),
                    inferred,
                    open_venue,
                    hedge_venue,
                )
            }
            _ => {
                let exchange = args.exchange.with_context(|| {
                    "missing --exchange and failed to infer FR namespace from CWD".to_string()
                })?;
                let (open_venue, hedge_venue) =
                    mkt_signal::funding_rate::common::venue_pair_for_exchange(exchange);
                (
                    "fr".to_string(),
                    fr_symbol_key_suffix(open_venue, hedge_venue),
                    exchange,
                    open_venue,
                    hedge_venue,
                )
            }
        };

    Ok(FrDashboardConfig {
        exchange,
        symbol_namespace,
        symbol_key_suffix,
        open_venue,
        hedge_venue,
        bind: args.bind,
        port: args.port,
        ws_path: args.ws_path,
        refresh_ms: args.refresh_ms,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    let cfg = resolve_config(args)?;

    info!(
        "========== {} init exchange={} ns={} suffix={} ==========",
        PROCESS_NAME, cfg.exchange, cfg.symbol_namespace, cfg.symbol_key_suffix
    );

    let token = CancellationToken::new();
    setup_signal_handlers(&token)?;

    let local = tokio::task::LocalSet::new();
    local.run_until(run(cfg, token)).await
}
