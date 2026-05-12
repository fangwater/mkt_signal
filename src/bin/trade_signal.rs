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

use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::iceoryx_publisher::configure_signal_publish_dry_run;
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::common::symbol_util::normalize_symbol_for_venue;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::signal::common::TradingVenue;

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    init_decision_branch, load_all_once_with_namespace, load_unimmr_thresholds_from_redis,
    spawn_account_risk_listener, spawn_config_loader_with_namespace,
    start_unimmr_threshold_refresh, trigger_decision, ArbDecision, ArbMode, DecisionBranch,
    FundingRateFactor, MktChannel, MmDecision, RateFetcher, SpreadFactor, SymbolList,
};

const PROCESS_NAME: &str = "trade_signal";
const ARB_COOLDOWN_SWEEP_MIN_INTERVAL_MS: u64 = 100;
const ARB_COOLDOWN_SWEEP_MAX_INTERVAL_MS: u64 = 1_000;

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

/// 与 `pre_trade::infer_dir_prefix_from_cwd` 同口径，用作 Redis hash 的 env 前缀。
fn infer_env_dir_from_cwd() -> Option<String> {
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd.file_name()?.to_string_lossy().trim().to_string();
    if leaf.is_empty() {
        return None;
    }
    Some(leaf.to_ascii_lowercase())
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

fn infer_intra_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    if suffix.contains('-') || suffix.contains('_') {
        return None;
    }
    let ex = normalize_exchange_str(&suffix);
    Some((
        margin_venue_for_exchange(ex)?,
        futures_venue_for_exchange(ex)?,
    ))
}

fn infer_cross_venues_from_key_suffix(key_suffix: &str) -> Option<(TradingVenue, TradingVenue)> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    let mut parts = suffix.split('-');
    let open_ex = normalize_exchange_str(parts.next()?);
    let hedge_ex = normalize_exchange_str(parts.next()?);
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

fn infer_arb_venues_from_env() -> Option<(TradingVenue, TradingVenue)> {
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
    let symbols = SymbolList::instance().get_online_symbols();
    let deadline_us =
        MmDecision::with_mut(|decision| decision.next_open_deadline_us(now_us, &symbols));
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

fn arb_cooldown_sweep_interval() -> Duration {
    let cooldown_us = ArbDecision::signal_cooldown_us().unwrap_or(5_000_000);
    let cooldown_ms = if cooldown_us <= 0 {
        ARB_COOLDOWN_SWEEP_MAX_INTERVAL_MS
    } else {
        (cooldown_us as u64).div_ceil(1_000)
    };
    let interval_ms = cooldown_ms.clamp(
        ARB_COOLDOWN_SWEEP_MIN_INTERVAL_MS,
        ARB_COOLDOWN_SWEEP_MAX_INTERVAL_MS,
    );
    Duration::from_millis(interval_ms.max(1))
}

fn process_arb_cooldown_sweep(open_venue: TradingVenue, hedge_venue: TradingVenue) {
    let mut symbols = SymbolList::instance().get_online_symbols();
    if symbols.is_empty() {
        return;
    }
    symbols.sort_unstable();
    symbols.dedup();

    for symbol in symbols {
        let open_symbol = normalize_symbol_for_venue(&symbol, open_venue);
        let hedge_symbol = normalize_symbol_for_venue(&symbol, hedge_venue);
        trigger_decision(&open_symbol, &hedge_symbol, open_venue, hedge_venue);
    }
}

fn spawn_arb_cooldown_sweep_worker(
    token: CancellationToken,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) {
    tokio::task::spawn_local(async move {
        let mut next_interval = arb_cooldown_sweep_interval();
        loop {
            let sleep = time::sleep(next_interval);
            tokio::pin!(sleep);
            tokio::select! {
                _ = token.cancelled() => break,
                _ = &mut sleep => {
                    process_arb_cooldown_sweep(open_venue, hedge_venue);
                    next_interval = arb_cooldown_sweep_interval();
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
        spawn_arb_cooldown_sweep_worker(token.clone(), open_venue, hedge_venue);
        debug!(
            "ARB workers started cancel_trigger=tlen cooldown_sweep=true mode={:?}",
            arb_mode
        );

        // UniMMR 算法平仓状态机（仅 arb 分支 fr/intra/cross）：
        // - 从 `pre_trade_risk_params` 拉 trigger/recover 同步加载一次 + 60s 刷新
        // - 订阅 open/hedge 涉及到的 exchange 的 `account_pubs/<ex>_pm`，过滤
        //   AccountRisk 消息并喂入 `UnimmrCloseGate`，按 BasicAccountScope 维度
        //   维护带迟滞的状态（mr>recover→Normal / mr<trigger→CloseAllowed）
        // - 本次只听+维护，不接 close 触发
        let unimmr_redis = get_redis_settings();
        let unimmr_env_prefix = infer_env_dir_from_cwd();
        if let Err(err) = load_unimmr_thresholds_from_redis(
            &unimmr_redis,
            unimmr_env_prefix.as_deref(),
            open_venue,
            hedge_venue,
        )
        .await
        {
            log::warn!(
                "UnimmrCloseGate 阈值初次加载失败 prefix={:?}: {:#}，使用默认值",
                unimmr_env_prefix,
                err
            );
        }
        start_unimmr_threshold_refresh(unimmr_redis, unimmr_env_prefix, open_venue, hedge_venue);

        let open_exchange = Exchange::from_str(open_venue.trade_engine_exchange())
            .context("invalid open venue exchange for UnimmrCloseGate")?;
        let hedge_exchange = Exchange::from_str(hedge_venue.trade_engine_exchange())
            .context("invalid hedge venue exchange for UnimmrCloseGate")?;
        let mut listener_exchanges: Vec<Exchange> = vec![open_exchange];
        if hedge_exchange != open_exchange {
            listener_exchanges.push(hedge_exchange);
        }
        for ex in listener_exchanges {
            spawn_account_risk_listener(ex);
        }
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
            Some((ns, suffix)) if ns.eq_ignore_ascii_case("intra") => {
                let (open_venue, hedge_venue) = infer_intra_venues_from_key_suffix(&suffix)
                    .or_else(infer_arb_venues_from_env)
                    .with_context(|| {
                        format!(
                            "failed to infer intra venues from CWD suffix='{}' or env OPEN_VENUE/HEDGE_VENUE",
                            suffix
                        )
                    })?;
                (
                    DecisionBranch::Arb,
                    Some(ArbMode::IntraArb),
                    ns,
                    suffix,
                    None,
                    open_venue,
                    hedge_venue,
                )
            }
            Some((ns, suffix)) if ns.eq_ignore_ascii_case("cross") => {
                let (open_venue, hedge_venue) = infer_cross_venues_from_key_suffix(&suffix)
                    .or_else(infer_arb_venues_from_env)
                    .with_context(|| {
                        format!(
                            "failed to infer cross venues from CWD suffix='{}' or env OPEN_VENUE/HEDGE_VENUE",
                            suffix
                        )
                    })?;
                (
                    DecisionBranch::Arb,
                    Some(ArbMode::CrossArb),
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
    use super::{
        infer_cross_venues_from_key_suffix, infer_intra_venues_from_key_suffix,
        parse_namespace_and_key_suffix,
    };
    use mkt_signal::signal::common::TradingVenue;

    #[test]
    fn parse_intra_dir_with_numeric_env_tag() {
        let parsed = parse_namespace_and_key_suffix("binance-intra-trade01");
        assert_eq!(parsed, Some(("intra".to_string(), "binance".to_string())));
    }

    #[test]
    fn parse_cross_dir_with_numeric_env_tag() {
        let parsed = parse_namespace_and_key_suffix("binance-okex-cross-trade01");
        assert_eq!(
            parsed,
            Some(("cross".to_string(), "binance-okex".to_string()))
        );
    }

    #[test]
    fn parse_fr_dir_with_numeric_env_tag() {
        let parsed = parse_namespace_and_key_suffix("binance_fr_trade03");
        assert_eq!(parsed, Some(("fr".to_string(), "binance".to_string())));
    }

    #[test]
    fn parse_cross_dir_with_plain_env_tag() {
        let parsed = parse_namespace_and_key_suffix("okex-binance-cross-test");
        assert_eq!(
            parsed,
            Some(("cross".to_string(), "okex-binance".to_string()))
        );
    }

    #[test]
    fn intra_suffix_resolves_to_margin_futures() {
        let (open, hedge) = infer_intra_venues_from_key_suffix("binance").unwrap();
        assert_eq!(open, TradingVenue::BinanceMargin);
        assert_eq!(hedge, TradingVenue::BinanceFutures);
    }

    #[test]
    fn cross_suffix_resolves_to_futures_pair() {
        let (open, hedge) = infer_cross_venues_from_key_suffix("binance-okex").unwrap();
        assert_eq!(open, TradingVenue::BinanceFutures);
        assert_eq!(hedge, TradingVenue::OkexFutures);
    }

    #[test]
    fn intra_rejects_two_token_suffix() {
        assert!(infer_intra_venues_from_key_suffix("binance-okex").is_none());
    }

    #[test]
    fn cross_rejects_same_exchange_suffix() {
        assert!(infer_cross_venues_from_key_suffix("binance-binance").is_none());
    }
}
