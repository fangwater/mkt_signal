//! Trade Signal 交易信号生成器（事件驱动版）
//!
//! 极简启动器：初始化所有单例 + 监听退出信号

use anyhow::{Context, Result};
use clap::Parser;
use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::signal::common::TradingVenue;

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    init_decision_branch, load_all_once_with_namespace, spawn_config_loader_with_namespace,
    DecisionBranch, FrDecision, FundingRateFactor, MktChannel, MmDecision, RateFetcher,
    SpreadFactor, SymbolList, XarbDecision,
};

const PROCESS_NAME: &str = "trade_signal";

#[derive(Parser, Debug)]
#[command(name = "trade_signal")]
#[command(about = "Trade Signal 交易信号生成器")]
struct Args {
    /// Exchange to use
    #[arg(short, long)]
    exchange: Option<Exchange>,
}

fn get_redis_settings() -> RedisSettings {
    RedisSettings {
        host: "127.0.0.1".to_string(),
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

fn infer_namespace_and_key_suffix_from_cwd() -> Option<(String, String)> {
    use std::path::PathBuf;

    let cwd: PathBuf = std::env::current_dir().ok()?;
    let name = cwd.file_name()?.to_string_lossy().to_ascii_lowercase();

    for env_suffix in ["_trade", "_test"] {
        if let Some(base) = name.strip_suffix(env_suffix) {
            let base = base.trim_end_matches('_');
            let (prefix, ns) = base.rsplit_once('_')?;
            if prefix.is_empty() || ns.is_empty() {
                return None;
            }
            return Some((ns.to_string(), prefix.to_string()));
        }
    }

    for env_suffix in ["-trade", "-test"] {
        if let Some(base) = name.strip_suffix(env_suffix) {
            let base = base.trim_end_matches('-');
            let (prefix, ns) = base.rsplit_once('-')?;
            if prefix.is_empty() || ns.is_empty() {
                return None;
            }
            return Some((ns.to_string(), prefix.to_string()));
        }
    }

    None
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

fn futures_venue_for_exchange(exchange: &str) -> Option<TradingVenue> {
    match exchange {
        "binance" => Some(TradingVenue::BinanceFutures),
        "okex" | "okx" => Some(TradingVenue::OkexFutures),
        "bybit" => Some(TradingVenue::BybitFutures),
        "bitget" => Some(TradingVenue::BitgetFutures),
        "gate" => Some(TradingVenue::GateFutures),
        _ => None,
    }
}

fn fr_symbol_key_suffix(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn infer_xarb_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    let mut parts = suffix.split('-');
    let open_ex = parts.next()?;
    let hedge_ex = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let open = futures_venue_for_exchange(open_ex)?;
    let hedge = futures_venue_for_exchange(hedge_ex)?;
    if open == hedge {
        return None;
    }
    Some((open, hedge))
}

fn key_suffix_looks_like_mm(key_suffix: &str) -> bool {
    let lower = key_suffix.trim().to_ascii_lowercase();
    lower.contains("_mm") || lower.contains("-mm")
}

fn infer_mm_venue_from_key_suffix(key_suffix: &str) -> Option<TradingVenue> {
    let raw = key_suffix.trim().to_ascii_lowercase().replace('_', "-");
    if raw.is_empty() {
        return None;
    }

    if let Some(venue) = venue_from_slug(&raw) {
        return Some(venue);
    }

    let exchange_candidate = if let Some((prefix, _)) = raw.split_once("-mm") {
        prefix.trim().to_string()
    } else {
        raw.split('-').next().unwrap_or("").trim().to_string()
    };
    if exchange_candidate.is_empty() {
        return None;
    }
    futures_venue_for_exchange(normalize_exchange_str(&exchange_candidate))
}

/// 主运行循环
async fn run(
    branch: DecisionBranch,
    exchange: Option<Exchange>,
    symbol_namespace: String,
    symbol_key_suffix: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    token: CancellationToken,
) -> Result<()> {
    info!("{} 启动（事件驱动模式） branch={:?}", PROCESS_NAME, branch);

    // 1️⃣ 初始化所有单例
    info!("初始化单例...");
    init_decision_branch(branch)?;
    SymbolList::init_singleton()?;
    MktChannel::init_singleton(open_venue, hedge_venue)?;
    RateFetcher::init_for_venues(open_venue, hedge_venue)?;
    match branch {
        DecisionBranch::Fr => {
            let exchange = exchange.context("missing exchange for FR branch")?;
            FrDecision::init_singleton(exchange).await?;
        }
        DecisionBranch::Xarb => {
            XarbDecision::init_singleton(open_venue, hedge_venue).await?;
        }
        DecisionBranch::Mm => {
            MmDecision::init_singleton(open_venue, hedge_venue).await?;
        }
    }
    info!("所有单例初始化完成");

    // SpreadFactor 和 FundingRateFactor 会在首次访问时自动初始化
    let spread_factor = SpreadFactor::instance();
    let _ = FundingRateFactor::instance();

    // 调试：延迟2秒后打印价差数据（等待盘口数据）
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    spread_factor.debug_print_stored_spreads(open_venue, hedge_venue);

    // 2️⃣ 立即加载所有配置（策略参数、符号列表、阈值等）
    let redis = get_redis_settings();
    info!("加载所有配置...");
    if let Err(err) = load_all_once_with_namespace(
        &redis,
        &symbol_namespace,
        &symbol_key_suffix,
        open_venue,
        hedge_venue,
    )
    .await
    {
        log::warn!("配置加载失败: {:?}，将使用默认值", err);
    }

    // 3️⃣ 启动统一配置定时重载器（60秒）
    spawn_config_loader_with_namespace(
        redis,
        symbol_namespace,
        symbol_key_suffix,
        open_venue,
        hedge_venue,
    );
    info!("配置加载器已启动（60秒定时重载）");

    // if matches!(branch, DecisionBranch::Fr) {
    //     tokio::task::spawn_local(async move {
    //         let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
    //         loop {
    //             interval.tick().await;
    //             let symbols = SymbolList::instance().get_online_symbols();
    //             if symbols.is_empty() {
    //                 continue;
    //             }
    //             RateFetcher::print_signal_table(&symbols);
    //         }
    //     });
    //     info!("资费信号表打印器已启动（10秒间隔）");
    // }

    info!("✅ {} 启动完成，等待市场数据触发决策...", PROCESS_NAME);

    // 4️⃣ 主循环：等待退出信号
    token.cancelled().await;
    info!("收到退出信号");

    info!("{} 退出", PROCESS_NAME);
    Ok(())
}

/// 设置信号处理器
fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut sigterm = unix_signal(SignalKind::terminate()).expect("failed to setup SIGTERM");
        let mut sigint = unix_signal(SignalKind::interrupt()).expect("failed to setup SIGINT");
        tokio::select! {
            _ = sigterm.recv() => info!("收到 SIGTERM"),
            _ = sigint.recv() => info!("收到 SIGINT (Ctrl+C)"),
        }
        token_clone.cancel();
    });
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // 初始化日志
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    let (branch, symbol_namespace, symbol_key_suffix, exchange, open_venue, hedge_venue) =
        match infer_namespace_and_key_suffix_from_cwd() {
            Some((ns, suffix)) if ns.eq_ignore_ascii_case("xarb") => {
                let (open_venue, hedge_venue) = infer_xarb_venues_from_key_suffix(&suffix)
                    .with_context(|| {
                        format!("failed to infer xarb venues from CWD suffix='{}'", suffix)
                    })?;
                (
                    DecisionBranch::Xarb,
                    ns,
                    suffix,
                    None,
                    open_venue,
                    hedge_venue,
                )
            }
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
                    DecisionBranch::Fr,
                    ns,
                    fr_symbol_key_suffix(open_venue, hedge_venue),
                    Some(inferred),
                    open_venue,
                    hedge_venue,
                )
            }
            Some((ns, suffix)) if ns.eq_ignore_ascii_case("mm") => {
                let venue = infer_mm_venue_from_key_suffix(&suffix).with_context(|| {
                    format!("failed to infer mm venue from CWD suffix='{}'", suffix)
                })?;
                (
                    DecisionBranch::Mm,
                    "mm".to_string(),
                    venue.data_pub_slug().to_string(),
                    None,
                    venue,
                    venue,
                )
            }
            Some((_ns, suffix)) if key_suffix_looks_like_mm(&suffix) => {
                let venue = infer_mm_venue_from_key_suffix(&suffix).with_context(|| {
                    format!("failed to infer mm venue from CWD suffix='{}'", suffix)
                })?;
                (
                    DecisionBranch::Mm,
                    "mm".to_string(),
                    venue.data_pub_slug().to_string(),
                    None,
                    venue,
                    venue,
                )
            }
            _ => {
                let exchange = args.exchange.with_context(|| {
                "missing --exchange and failed to infer from CWD; use a dir like '<exchange>_fr_trade' or pass --exchange"
                    .to_string()
            })?;
                let (open_venue, hedge_venue) =
                    mkt_signal::funding_rate::common::venue_pair_for_exchange(exchange);
                (
                    DecisionBranch::Fr,
                    "fr".to_string(),
                    fr_symbol_key_suffix(open_venue, hedge_venue),
                    Some(exchange),
                    open_venue,
                    hedge_venue,
                )
            }
        };

    info!(
        "========== {} 初始化 ========== branch={:?}",
        PROCESS_NAME, branch
    );

    // 设置信号处理器
    let token = CancellationToken::new();
    setup_signal_handlers(&token)?;

    // 使用 LocalSet 运行（thread-local 单例需要）
    let local = tokio::task::LocalSet::new();
    local
        .run_until(run(
            branch,
            exchange,
            symbol_namespace,
            symbol_key_suffix,
            open_venue,
            hedge_venue,
            token,
        ))
        .await
}
