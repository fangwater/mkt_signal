//! Trade Signal 交易信号生成器（事件驱动版）
//!
//! 极简启动器：初始化所有单例 + 监听退出信号

use anyhow::{Context, Result};
use clap::Parser;
use log::{debug, info};
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::time::{self, Duration, Instant};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::iceoryx_publisher::configure_signal_publish_dry_run;
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::signal::common::TradingVenue;

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    init_decision_branch, load_all_once_with_namespace, spawn_config_loader_with_namespace,
    ArbDecision, ArbMode, DecisionBranch, FundingRateFactor, MktChannel, MmDecision, RateFetcher,
    SpreadFactor, SymbolList,
};

const PROCESS_NAME: &str = "trade_signal";

#[derive(Parser, Debug)]
#[command(name = "trade_signal")]
#[command(about = "Trade Signal 交易信号生成器")]
struct Args {
    /// Exchange to use
    #[arg(short, long)]
    exchange: Option<Exchange>,

    /// Run full decision flow but do not emit trade_signal IPC messages
    #[arg(long, default_value_t = false)]
    no_emit_signals: bool,
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

fn margin_venue_for_exchange(exchange: &str) -> Option<TradingVenue> {
    match exchange {
        "binance" => Some(TradingVenue::BinanceMargin),
        "okex" | "okx" => Some(TradingVenue::OkexMargin),
        "bybit" => Some(TradingVenue::BybitMargin),
        "bitget" => Some(TradingVenue::BitgetMargin),
        "gate" => Some(TradingVenue::GateMargin),
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
        return Some((margin_venue_for_exchange(open_ex)?, hedge));
    }
    Some((open, hedge))
}

fn infer_xarb_venues_from_env() -> Option<(TradingVenue, TradingVenue)> {
    let open = std::env::var("OPEN_VENUE").ok()?;
    let hedge = std::env::var("HEDGE_VENUE").ok()?;
    let open_venue = venue_from_slug(&open)?;
    let hedge_venue = venue_from_slug(&hedge)?;
    if open_venue == hedge_venue {
        return None;
    }
    Some((open_venue, hedge_venue))
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

fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn next_mm_open_deadline() -> Instant {
    let now_us = get_timestamp_us();
    let deadline_us = MmDecision::with(|decision| decision.next_open_deadline_us(now_us));
    let delay_us = deadline_us.saturating_sub(now_us).max(1) as u64;
    Instant::now() + Duration::from_micros(delay_us)
}

fn spawn_mm_open_worker(token: CancellationToken) {
    tokio::task::spawn_local(async move {
        loop {
            let sleep = time::sleep_until(next_mm_open_deadline());
            tokio::pin!(sleep);
            tokio::select! {
                _ = token.cancelled() => break,
                _ = &mut sleep => {
                    MmDecision::with_mut(|decision| decision.process_open_interval());
                }
            }
        }
    });
}

fn spawn_mm_cancel_worker(token: CancellationToken) {
    tokio::task::spawn_local(async move {
        let mut interval = time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = interval.tick() => {
                    MmDecision::with_mut(|decision| decision.process_return_score_updates());
                }
            }
        }
    });
}

fn spawn_mm_cancel_trigger_worker(token: CancellationToken) {
    tokio::task::spawn_local(async move {
        let mut interval = time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = interval.tick() => {
                    MmDecision::with_mut(|decision| decision.process_cancel_trigger_interval());
                }
            }
        }
    });
}

fn spawn_arb_cancel_trigger_worker(token: CancellationToken) {
    tokio::task::spawn_local(async move {
        let mut interval = time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = interval.tick() => {
                    ArbDecision::process_cancel_trigger_interval();
                }
            }
        }
    });
}

/// 主运行循环
async fn run(
    branch: DecisionBranch,
    arb_mode: Option<ArbMode>,
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
    match branch {
        DecisionBranch::Arb => {
            let mode = arb_mode.context("missing arb_mode for Arb branch")?;
            ArbDecision::init_singleton(mode, open_venue, hedge_venue, exchange).await?;
        }
        DecisionBranch::Mm => {
            MmDecision::init_singleton(open_venue, hedge_venue).await?;
        }
    }

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

    MktChannel::init_singleton(open_venue, hedge_venue)?;
    RateFetcher::init_for_venues(open_venue, hedge_venue)?;

    // SpreadFactor 和 FundingRateFactor 会在首次访问时自动初始化
    let spread_factor = SpreadFactor::instance();
    let _ = FundingRateFactor::instance();
    info!("所有单例初始化完成");

    // 调试：延迟2秒后打印价差数据（等待盘口数据）
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    spread_factor.debug_print_stored_spreads(open_venue, hedge_venue);

    // 3️⃣ 启动统一配置定时重载器（60秒）
    spawn_config_loader_with_namespace(
        redis,
        symbol_namespace,
        symbol_key_suffix,
        open_venue,
        hedge_venue,
    );
    info!("配置加载器已启动（60秒定时重载）");

    if matches!(branch, DecisionBranch::Mm) {
        spawn_mm_open_worker(token.clone());
        spawn_mm_cancel_worker(token.clone());
        spawn_mm_cancel_trigger_worker(token.clone());
        debug!("MM workers started open=interval cancel=return_score_update");
    } else if matches!(branch, DecisionBranch::Arb) {
        spawn_arb_cancel_trigger_worker(token.clone());
        debug!(
            "ARB workers started cancel_trigger=tlen mode={:?}",
            arb_mode
        );
    }

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
    let no_emit_signals = args.no_emit_signals || env_flag("TRADE_SIGNAL_NO_EMIT");
    configure_signal_publish_dry_run(no_emit_signals);
    if no_emit_signals {
        info!(
            "{} dry-run enabled: trade_signal messages will be suppressed",
            PROCESS_NAME
        );
    }
    let (branch, arb_mode, symbol_namespace, symbol_key_suffix, exchange, open_venue, hedge_venue) =
        match infer_namespace_and_key_suffix_from_cwd() {
            Some((ns, suffix)) if ns.eq_ignore_ascii_case("xarb") => {
                let (open_venue, hedge_venue) = infer_xarb_venues_from_env()
                    .or_else(|| infer_xarb_venues_from_key_suffix(&suffix))
                    .with_context(|| {
                        format!(
                            "failed to infer xarb venues from env OPEN_VENUE/HEDGE_VENUE or CWD suffix='{}'",
                            suffix
                        )
                    })?;
                let arb_mode = ArbMode::from_venues(open_venue, hedge_venue);
                (
                    DecisionBranch::Arb,
                    Some(arb_mode),
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
                    DecisionBranch::Arb,
                    Some(ArbMode::FundingArb),
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
                    None,
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
                    None,
                    "mm".to_string(),
                    venue.data_pub_slug().to_string(),
                    None,
                    venue,
                    venue,
                )
            }
            _ => {
                let exchange = args.exchange.with_context(|| {
                    "missing --exchange and failed to infer from CWD; use a dir like '<prefix>_<namespace>_<env>' or pass --exchange"
                        .to_string()
                })?;
                let (open_venue, hedge_venue) =
                    mkt_signal::funding_rate::common::venue_pair_for_exchange(exchange);
                (
                    DecisionBranch::Arb,
                    Some(ArbMode::FundingArb),
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
            arb_mode,
            exchange,
            symbol_namespace,
            symbol_key_suffix,
            open_venue,
            hedge_venue,
            token,
        ))
        .await
}

#[cfg(test)]
mod tests {
    use super::parse_namespace_and_key_suffix;

    #[test]
    fn parse_xarb_dir_with_numeric_env_tag() {
        let parsed = parse_namespace_and_key_suffix("binance-binance-xarb-trade01");
        assert_eq!(
            parsed,
            Some(("xarb".to_string(), "binance-binance".to_string()))
        );
    }

    #[test]
    fn parse_fr_dir_with_numeric_env_tag() {
        let parsed = parse_namespace_and_key_suffix("binance_fr_trade03");
        assert_eq!(parsed, Some(("fr".to_string(), "binance".to_string())));
    }

    #[test]
    fn parse_xarb_dir_with_plain_env_tag() {
        let parsed = parse_namespace_and_key_suffix("okex-binance-xarb-test");
        assert_eq!(
            parsed,
            Some(("xarb".to_string(), "okex-binance".to_string()))
        );
    }
}
