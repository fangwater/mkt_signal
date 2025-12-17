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
use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::funding_rate::common::{ArbDirection, CompareOp, FactorMode, OperationType};
use mkt_signal::funding_rate::{
    init_decision_branch, load_all_once_with_namespace, spawn_config_loader_with_namespace,
    DecisionBranch, FrDecision, FrSignalStateBatch, FundingRateFactor, MktChannel, RateFetcher,
    SpreadFactor, SpreadType, SymbolList, XarbDecision, DEFAULT_STATE_CHANNEL,
};

const PROCESS_NAME: &str = "trade_signal";
const SPREAD_LOG_INTERVAL_SECS: u64 = 10;

#[derive(Parser, Debug)]
#[command(name = "trade_signal")]
#[command(about = "Trade Signal 交易信号生成器")]
struct Args {
    /// Exchange to use
    #[arg(short, long)]
    exchange: Option<Exchange>,
}

fn get_redis_settings() -> RedisSettings {
    let redis_host =
        std::env::var("FUNDING_RATE_REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    RedisSettings {
        host: redis_host,
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

    // 4️⃣ 定时打印 spread 信号状态（10 秒一次）
    {
        let cancel = token.clone();
        tokio::task::spawn_local(async move {
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(SPREAD_LOG_INTERVAL_SECS));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        print_spread_signal_state_table(branch, open_venue, hedge_venue);
                    }
                }
            }
        });
    }

    // 5️⃣ 定时发布信号状态快照（10 秒一次），仅 FR 分支支持
    if branch == DecisionBranch::Fr {
        let cancel = token.clone();
        tokio::task::spawn_local(async move {
            let state_pub = SignalPublisher::new_with_prefix("viz_pubs", DEFAULT_STATE_CHANNEL)
                .expect("failed to create fr_signal_state publisher");
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(SPREAD_LOG_INTERVAL_SECS));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let symbols = SymbolList::instance().get_online_symbols();
                        if symbols.is_empty() {
                            continue;
                        }
                        let entries = FrDecision::collect_signal_state_entries(&symbols);
                        let batch = FrSignalStateBatch::new(get_timestamp_us(), entries);
                        if let Err(err) = state_pub.publish(&batch.to_bytes()) {
                            log::warn!("发布 fr_signal_state 失败: {:?}", err);
                        }
                    }
                }
            }
        });
    }

    info!("✅ {} 启动完成，等待市场数据触发决策...", PROCESS_NAME);

    // 6️⃣ 主循环：等待退出信号
    token.cancelled().await;
    info!("收到退出信号");

    info!("{} 退出", PROCESS_NAME);
    Ok(())
}

#[derive(Debug, Clone)]
struct SpreadStateRow {
    symbol: String,
    state: &'static str,
    spread_type: &'static str,
    value_pct: Option<f64>,
    compare_op: &'static str,
    threshold_pct: Option<f64>,
}

fn compare_op_short(op: CompareOp) -> &'static str {
    match op {
        CompareOp::GreaterThan => ">",
        CompareOp::LessThan => "<",
    }
}

fn spread_type_short(ty: SpreadType) -> &'static str {
    match ty {
        SpreadType::BidAsk => "BidAsk",
        SpreadType::AskBid => "AskBid",
        SpreadType::SpreadRate => "Rate",
    }
}

fn fmt_opt_pct(v: Option<f64>, width: usize) -> String {
    match v {
        Some(x) if x.is_finite() => format!("{:>width$.3}", x, width = width),
        _ => format!("{:>width$}", "-", width = width),
    }
}

fn spread_state_to_detail_key(state: &str) -> Option<(ArbDirection, OperationType)> {
    match state {
        "FwdCancel" => Some((ArbDirection::Forward, OperationType::Cancel)),
        "BwdCancel" => Some((ArbDirection::Backward, OperationType::Cancel)),
        "FwdOpen" | "FlipBuy" | "BwdClose" => Some((ArbDirection::Forward, OperationType::Open)),
        "BwdOpen" | "FlipSell" | "FwdClose" => Some((ArbDirection::Backward, OperationType::Open)),
        _ => None,
    }
}

fn compute_spread_state_row(
    spread_factor: &SpreadFactor,
    symbol_list: &SymbolList,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    symbol: &str,
) -> Option<SpreadStateRow> {
    let in_dump = symbol_list.is_in_dump_list(symbol);

    let fwd_open = !in_dump
        && spread_factor.satisfy_forward_open(open_venue, symbol, hedge_venue, symbol)
        && symbol_list.is_in_fwd_trade_list(symbol);
    let bwd_open = !in_dump
        && spread_factor.satisfy_backward_open(open_venue, symbol, hedge_venue, symbol)
        && symbol_list.is_in_bwd_trade_list(symbol);

    let fwd_close = spread_factor.satisfy_forward_close(open_venue, symbol, hedge_venue, symbol);
    let bwd_close = spread_factor.satisfy_backward_close(open_venue, symbol, hedge_venue, symbol);

    let fwd_cancel = spread_factor.satisfy_forward_cancel(open_venue, symbol, hedge_venue, symbol);
    let bwd_cancel = spread_factor.satisfy_backward_cancel(open_venue, symbol, hedge_venue, symbol);

    let state = if fwd_cancel {
        "FwdCancel"
    } else if bwd_cancel {
        "BwdCancel"
    } else if fwd_close && bwd_open {
        "FlipSell"
    } else if bwd_close && fwd_open {
        "FlipBuy"
    } else if fwd_close {
        "FwdClose"
    } else if bwd_close {
        "BwdClose"
    } else if fwd_open {
        "FwdOpen"
    } else if bwd_open {
        "BwdOpen"
    } else {
        return None;
    };

    let (spread_type, value_pct, compare_op, threshold_pct) =
        if let Some((arb_direction, operation)) = spread_state_to_detail_key(state) {
            spread_factor
                .get_spread_check_detail(
                    open_venue,
                    symbol,
                    hedge_venue,
                    symbol,
                    arb_direction,
                    operation,
                )
                .map(|(value, threshold, op, spread_type)| {
                    (
                        spread_type_short(spread_type),
                        Some(value * 100.0),
                        compare_op_short(op),
                        Some(threshold * 100.0),
                    )
                })
                .unwrap_or(("-", None, "-", None))
        } else {
            ("-", None, "-", None)
        };

    Some(SpreadStateRow {
        symbol: symbol.to_string(),
        state,
        spread_type,
        value_pct,
        compare_op,
        threshold_pct,
    })
}

fn print_spread_signal_state_table(
    branch: DecisionBranch,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) {
    let spread_factor = SpreadFactor::instance();
    let symbol_list = SymbolList::instance();
    let mode: FactorMode = spread_factor.get_mode();

    let mut symbols = symbol_list.get_online_symbols();
    if symbols.is_empty() {
        info!(
            "SpreadSig: branch={:?} mode={:?} open={:?} hedge={:?} (no online symbols)",
            branch, mode, open_venue, hedge_venue
        );
        return;
    }
    symbols.sort_unstable();

    let mut rows: Vec<SpreadStateRow> = symbols
        .iter()
        .filter_map(|s| {
            compute_spread_state_row(&spread_factor, &symbol_list, open_venue, hedge_venue, s)
        })
        .collect();
    rows.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));

    info!(
        "SpreadSig snapshot: branch={:?} mode={:?} open={:?} hedge={:?} active={}/{}",
        branch,
        mode,
        open_venue,
        hedge_venue,
        rows.len(),
        symbols.len()
    );

    // 三线表：top / header-sep / bottom（不逐行加横线）
    const W_SYMBOL: usize = 14;
    const W_STATE: usize = 9;
    const W_TYPE: usize = 6;
    const W_VAL: usize = 8;
    const W_OP: usize = 1;
    const W_THR: usize = 8;

    let header_cells = vec![
        format!("{:<W_SYMBOL$}", "Symbol", W_SYMBOL = W_SYMBOL),
        format!("{:<W_STATE$}", "State", W_STATE = W_STATE),
        format!("{:<W_TYPE$}", "Type", W_TYPE = W_TYPE),
        format!("{:>W_VAL$}", "Val%", W_VAL = W_VAL),
        format!("{:<W_OP$}", "Op", W_OP = W_OP),
        format!("{:>W_THR$}", "Thr%", W_THR = W_THR),
    ];
    let header = format!("│ {} │", header_cells.join(" │ "));
    let width = header.chars().count();
    let top = format!("┌{}┐", "─".repeat(width.saturating_sub(2)));
    let mid = format!("├{}┤", "─".repeat(width.saturating_sub(2)));
    let bottom = format!("└{}┘", "─".repeat(width.saturating_sub(2)));

    info!("{}", top);
    info!("{}", header);
    info!("{}", mid);

    if rows.is_empty() {
        let empty_cells = vec![
            format!("{:<W_SYMBOL$}", "(none)", W_SYMBOL = W_SYMBOL),
            format!("{:<W_STATE$}", "-", W_STATE = W_STATE),
            format!("{:<W_TYPE$}", "-", W_TYPE = W_TYPE),
            fmt_opt_pct(None, W_VAL),
            format!("{:<W_OP$}", "-", W_OP = W_OP),
            fmt_opt_pct(None, W_THR),
        ];
        info!("│ {} │", empty_cells.join(" │ "));
        info!("{}", bottom);
        return;
    }

    for r in rows {
        let cells = vec![
            format!("{:<W_SYMBOL$}", r.symbol, W_SYMBOL = W_SYMBOL),
            format!("{:<W_STATE$}", r.state, W_STATE = W_STATE),
            format!("{:<W_TYPE$}", r.spread_type, W_TYPE = W_TYPE),
            fmt_opt_pct(r.value_pct, W_VAL),
            format!("{:<W_OP$}", r.compare_op, W_OP = W_OP),
            fmt_opt_pct(r.threshold_pct, W_THR),
        ];
        info!("│ {} │", cells.join(" │ "));
    }

    info!("{}", bottom);
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
                let expected = normalize_exchange_str(&suffix);
                let inferred = Exchange::from_str(expected).with_context(|| {
                    format!(
                        "failed to infer exchange from CWD suffix='{}' (namespace=fr)",
                        suffix
                    )
                })?;
                if let Some(cli_ex) = args.exchange {
                    if cli_ex != inferred {
                        anyhow::bail!(
                            "--exchange mismatch with CWD: cwd_exchange={} cli_exchange={}",
                            inferred,
                            cli_ex
                        );
                    }
                }
                let (open_venue, hedge_venue) =
                    mkt_signal::funding_rate::common::venue_pair_for_exchange(inferred);
                (
                    DecisionBranch::Fr,
                    ns,
                    suffix,
                    Some(inferred),
                    open_venue,
                    hedge_venue,
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
                    exchange.as_str().to_string(),
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
