//! OKEx 账户监控程序
//!
//! 功能：
//! - 连接到 OKEx 私有 WebSocket 频道
//! - 使用 API Key/Secret/Passphrase 进行登录鉴权
//! - 订阅订单、持仓、余额等频道
//! - 解析账户事件并通过 Iceoryx 转发
//! - 支持主备双路连接
//!
//! 使用方式：
//! ```bash
//! export OKX_API_KEY="your_api_key"
//! export OKX_API_SECRET="your_api_secret"
//! export OKX_PASSPHRASE="your_passphrase"
//! cargo run --bin okex_account_monitor
//! ```

use account_common::okex_auth::{
    build_account_subscribe_message, build_balance_and_position_subscribe_message,
    build_fills_subscribe_message, build_orders_subscribe_message,
    build_positions_subscribe_message, OkexCredentials, OkexPrivateWsUrls,
};
use account_monitor_common::okex_rest::{fetch_borrow_interest, okex_rest_get};
use account_monitor_common::okex_user_stream::OkexUserDataConnection;
use account_monitor_common::pm_forwarder::PmForwarder;
use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use log::{debug, error, info, warn};
use mkt_parsers::account_event::okex_account_event_parser::OkexAccountEventParser;
use mkt_parsers::account_event::{AccountEventSink, Parser as AccountEventParser};
use mkt_parsers::msg::basic_account_msg::{
    get_basic_event_type, split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType,
    BasicAccountRiskMsg, BasicAccountScope, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg, BasicTradeLiteMsg, BasicUmUnrealizedMsg, OkexOrderMsg,
};
use reqwest::Client;
use runtime_common::affinity::maybe_pin_current_thread;
use runtime_common::mkt_cfg::load_local_ips_preferring_trade_engine;
use runtime_common::ws_connection::{MktConnection, MktConnectionHandler};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;
use trade_engine::query_parsers::okex_positions_snapshot::parse_okex_positions_snapshot;

fn credential_edges(value: &str) -> (String, String, usize) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new(), 0);
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let len = chars.len();
    let prefix_len = len.min(4);
    let suffix_len = len.min(4);
    let first: String = chars.iter().take(prefix_len).collect();
    let last: String = chars.iter().skip(len.saturating_sub(suffix_len)).collect();
    (first, last, len)
}

fn log_credential_preview(label: &str, value: &str) {
    let (first4, last4, len) = credential_edges(value);
    if len == 0 {
        info!("{} not set or empty", label);
    } else {
        info!(
            "{} preview len={} first4='{}' last4='{}'",
            label, len, first4, last4
        );
    }
}

#[derive(Parser, Debug)]
#[command(name = "okex_account_monitor")]
#[command(about = "OKEx account monitor")]
struct Args {
    /// Bind the main runtime thread to a CPU core. Falls back to ACCOUNT_MONITOR_CORE.
    #[arg(long)]
    core: Option<usize>,
}

const OKEX_POSITIONS_SNAPSHOT_PATH: &str = "/api/v5/account/positions?instType=SWAP";

struct DirectAccountForwarder {
    forwarder: PmForwarder,
    deduper: AccountEventDeduper,
}

thread_local! {
    static DIRECT_FORWARDER: RefCell<Option<DirectAccountForwarder>> = RefCell::new(None);
}

#[derive(Clone, Copy)]
struct SourceAccountEventSink {
    source: &'static str,
}

impl AccountEventSink for SourceAccountEventSink {
    fn emit(&self, msg: Bytes) -> bool {
        emit_direct_account_event(msg, None, self.source)
    }

    fn emit_with_dedup_key(&self, msg: Bytes, dedup_key: u64) -> bool {
        emit_direct_account_event(msg, Some(dedup_key), self.source)
    }
}

fn emit_direct_account_event(msg: Bytes, dedup_key: Option<u64>, source: &str) -> bool {
    DIRECT_FORWARDER.with(|cell| {
        let mut state = cell.borrow_mut();
        let Some(state) = state.as_mut() else {
            warn!(
                "failed to forward OKX account event: source={} forwarder_uninitialized",
                source
            );
            return false;
        };
        let should_forward = match dedup_key {
            Some(key) => state.deduper.should_forward_key(key),
            None => state.deduper.should_forward(&msg),
        };
        if should_forward {
            let sent = state.forwarder.send_raw(&msg);
            log_parsed_event(&msg, source);
            sent
        } else {
            true
        }
    })
}

fn init_direct_forwarder(exchange: &str) -> Result<()> {
    let state = DirectAccountForwarder {
        forwarder: PmForwarder::new(exchange)?,
        deduper: AccountEventDeduper::new(8192),
    };
    DIRECT_FORWARDER.with(|cell| {
        *cell.borrow_mut() = Some(state);
    });
    Ok(())
}

fn send_wrapped_payload(payload: Bytes, source: &'static str) -> bool {
    let event_type = get_basic_event_type(payload.as_ref());
    let event = BasicAccountEventMsg::create(event_type, BasicAccountScope::OkexUnified, payload);
    if !emit_direct_account_event(event.to_bytes(), None, source) {
        warn!("failed to forward {}", source);
        false
    } else {
        true
    }
}

fn log_forwarder_stats() {
    DIRECT_FORWARDER.with(|cell| {
        if let Some(state) = cell.borrow_mut().as_mut() {
            state.forwarder.log_stats();
        }
    });
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "ACCOUNT_MONITOR_CORE")?;

    // 从环境变量加载 OKEx 凭证
    let credentials = OkexCredentials::from_env()?;
    log_credential_preview("OKX_API_KEY", &credentials.api_key);
    log_credential_preview("OKX_API_SECRET", &credentials.secret_key);
    log_credential_preview("OKX_PASSPHRASE", &credentials.passphrase);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // WebSocket URL 固定，跳过配置
    const OKEX_PM_WS: &str = OkexPrivateWsUrls::PRIVATE;
    let ws_url = OKEX_PM_WS.to_string();

    // IP 和会话设置
    let ((primary_ip, secondary_ip), ip_source) = load_local_ips_preferring_trade_engine().await?;
    let session_max = None;
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?} (local_ip_source: {})",
        primary_ip, secondary_ip, session_max, ip_source
    );

    // 基础订阅消息（不含 fills，fills 在 spawn 内按 VIP 标志动态拼入）。
    let base_subscribe_messages = vec![
        build_orders_subscribe_message("SPOT"),
        build_orders_subscribe_message("SWAP"),
        build_account_subscribe_message(),
        build_balance_and_position_subscribe_message(),
        build_positions_subscribe_message("SWAP"),
    ];
    // fills 频道需要 VIP4+；收到 64003 后永久关闭，避免无效重连循环。
    let (fills_disabled_tx, fills_disabled_rx) = watch::channel(false);

    // 创建 PM 转发器 (account_pubs/okex/pm)
    init_direct_forwarder("okex")?;
    let mut stats = tokio::time::interval(Duration::from_secs(30));
    let mut interest_poll = spawn_borrow_interest_poll(credentials.clone(), shutdown_rx.clone());
    let mut positions_poll = spawn_positions_poll(credentials.clone(), shutdown_rx.clone());

    // 启动主备双路连接
    let mut primary = spawn_okex_stream_path(
        "primary",
        &ws_url,
        primary_ip.clone(),
        credentials.clone(),
        base_subscribe_messages.clone(),
        fills_disabled_rx.clone(),
        fills_disabled_tx.clone(),
        shutdown_rx.clone(),
        session_max,
    );
    let mut secondary = spawn_okex_stream_path(
        "secondary",
        &ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        base_subscribe_messages.clone(),
        fills_disabled_rx.clone(),
        fills_disabled_tx.clone(),
        shutdown_rx.clone(),
        session_max,
    );

    // 主循环：接收事件、去重、转发
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => { break; }
            _ = stats.tick() => {
                log_forwarder_stats();
            }
            res = &mut interest_poll => {
                match res {
                    Ok(()) => warn!("interest poll task exited; restarting"),
                    Err(e) => warn!("interest poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    interest_poll = spawn_borrow_interest_poll(credentials.clone(), shutdown_rx.clone());
                }
            }
            res = &mut positions_poll => {
                match res {
                    Ok(()) => warn!("positions poll task exited; restarting"),
                    Err(e) => warn!("positions poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    positions_poll = spawn_positions_poll(credentials.clone(), shutdown_rx.clone());
                }
            }
            res = &mut primary => {
                match res {
                    Ok(()) => warn!("primary okex stream task exited; restarting"),
                    Err(e) => warn!("primary okex stream task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    primary = spawn_okex_stream_path(
                        "primary",
                        &ws_url,
                        primary_ip.clone(),
                        credentials.clone(),
                        base_subscribe_messages.clone(),
                        fills_disabled_rx.clone(),
                        fills_disabled_tx.clone(),
                        shutdown_rx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut secondary => {
                match res {
                    Ok(()) => warn!("secondary okex stream task exited; restarting"),
                    Err(e) => warn!("secondary okex stream task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    secondary = spawn_okex_stream_path(
                        "secondary",
                        &ws_url,
                        secondary_ip.clone(),
                        credentials.clone(),
                        base_subscribe_messages.clone(),
                        fills_disabled_rx.clone(),
                        fills_disabled_tx.clone(),
                        shutdown_rx.clone(),
                        session_max,
                    );
                }
            }
        }
    }

    info!("OKEx account monitor shutdown complete");
    Ok(())
}

fn setup_signals(shutdown_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            info!("Received Ctrl-C, shutting down...");
            let _ = shutdown_tx.send(true);
        }
    });
}

fn spawn_borrow_interest_poll(
    credentials: OkexCredentials,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    match fetch_borrow_interest(&client, &credentials).await {
                        Ok(items) => {
                            for msg in items {
                                let payload = msg.to_bytes();
                                let event = mkt_parsers::msg::basic_account_msg::BasicAccountEventMsg::create(
                                    msg.msg_type,
                                    mkt_parsers::msg::basic_account_msg::BasicAccountScope::OkexUnified,
                                    payload,
                                );
                                if !emit_direct_account_event(
                                    event.to_bytes(),
                                    None,
                                    "rest_interest_poll",
                                ) {
                                    warn!("failed to forward borrow interest msg");
                                }
                            }
                        }
                        Err(e) => {
                            warn!("fetch borrow interest failed: {:?}", e);
                        }
                    }
                }
            }
        }
        info!("borrow interest poller exiting");
    })
}

fn spawn_positions_poll(
    credentials: OkexCredentials,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        let mut previous_positions: HashSet<(String, char)> = HashSet::new();
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    match okex_rest_get(&client, &credentials, OKEX_POSITIONS_SNAPSHOT_PATH).await {
                        Ok((200, body)) => {
                            if let Some(msgs) = parse_okex_positions_snapshot(&body) {
                                let mut current_positions: HashSet<(String, char)> = HashSet::new();
                                let mut explicit_positions: HashSet<(String, char)> = HashSet::new();
                                for payload in msgs {
                                    if matches!(
                                        get_basic_event_type(payload.as_ref()),
                                        BasicAccountEventType::PositionUpdate
                                    ) {
                                        if let Ok(msg) = BasicPositionMsg::from_bytes(&payload) {
                                            let key = (msg.inst_id().to_string(), msg.position_side());
                                            explicit_positions.insert(key.clone());
                                            if msg.position_amount != 0.0 {
                                                current_positions.insert(key);
                                            }
                                        }
                                    }
                                    send_wrapped_payload(payload, "rest_positions_poll");
                                }

                                let mut stale_positions: Vec<(String, char)> = previous_positions
                                    .difference(&current_positions)
                                    .filter(|key| !explicit_positions.contains(*key))
                                    .cloned()
                                    .collect();
                                stale_positions.sort();
                                for (inst_id, side) in stale_positions {
                                    let ts = chrono::Utc::now().timestamp_millis();
                                    info!(
                                        "OKX REST position snapshot missing previously-seen position; emitting zero cleanup: inst_id={} side={}",
                                        inst_id, side
                                    );
                                    send_wrapped_payload(
                                        BasicPositionMsg::create(ts, inst_id.clone(), side, 0.0)
                                            .to_bytes(),
                                        "rest_positions_zero_cleanup",
                                    );
                                    send_wrapped_payload(
                                        BasicUmUnrealizedMsg::create(ts, inst_id, side, 0.0)
                                            .to_bytes(),
                                        "rest_positions_zero_pnl_cleanup",
                                    );
                                }
                                previous_positions = current_positions;
                            } else {
                                warn!("OKX position poll parse failed; body_len={}", body.len());
                            }
                        }
                        Ok((status, body)) => {
                            warn!("OKX position poll http {} body={}", status, body);
                        }
                        Err(err) => {
                            warn!("OKX position poll failed: {:?}", err);
                        }
                    }
                }
            }
        }
        info!("OKX position poller exiting");
    })
}

fn spawn_okex_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: OkexCredentials,
    base_subscribe_messages: Vec<serde_json::Value>,
    fills_disabled_rx: watch::Receiver<bool>,
    fills_disabled_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    session_max: Option<Duration>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = ws_url.to_string();
    tokio::spawn(async move {
        loop {
            info!(
                "[{}] connecting to {} (local_ip='{}')",
                name, ws_url, local_ip
            );

            let (raw_tx, _) = tokio::sync::broadcast::channel::<Bytes>(1);

            // 每次重连动态拼 fills（VIP 不足后不再加入）
            let mut subscribe_messages = base_subscribe_messages.clone();
            if !*fills_disabled_rx.borrow() {
                subscribe_messages.push(build_fills_subscribe_message());
            }

            let mut conn = MktConnection::new(
                ws_url.clone(),
                serde_json::json!({}),
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }

            let mut runner = OkexUserDataConnection::new(
                conn,
                credentials.clone(),
                subscribe_messages,
                session_max,
            );

            let parser = OkexAccountEventParser::new();
            let handler_name = name;
            let handler_local_ip = local_ip.clone();
            let handler_fills_disabled_tx = fills_disabled_tx.clone();
            runner.set_raw_handler(Box::new(move |b: Bytes| {
                if let Ok(s) = std::str::from_utf8(&b) {
                    debug!(
                        "[{}][ip={}] okex ws json: {}",
                        handler_name, handler_local_ip, s
                    );
                    if is_fills_vip_error(s) {
                        warn!("[{}] OKX fills channel requires VIP4+, disabling fills subscription (code 64003)", handler_name);
                        let _ = handler_fills_disabled_tx.send(true);
                    }
                } else {
                    debug!(
                        "[{}][ip={}] okex ws bin: {} bytes",
                        handler_name,
                        handler_local_ip,
                        b.len()
                    );
                }
                let source = if handler_name == "primary" {
                    "ws_primary"
                } else {
                    "ws_secondary"
                };
                let parsed = parser.parse(b, &SourceAccountEventSink { source });
                if parsed > 0 {
                    info!(
                        "OKX ws account event parsed: source={} parsed_count={}",
                        source, parsed
                    );
                }
            }));

            if let Err(e) = runner.start_ws().await {
                error!("[{}] connection error: {}", name, e);
            }

            if *shutdown_rx.borrow() {
                info!("[{}] shutdown signal received, exiting", name);
                break;
            }

            info!("[{}] connection closed, reconnecting in 2s...", name);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

/// 判断消息是否为 fills 频道 VIP 不足错误（code 64003）。
fn is_fills_vip_error(s: &str) -> bool {
    if !s.contains("64003") {
        return false;
    }
    // 快速路径：检查 event=error + code=64003 + channel=fills
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        let is_error = v.get("event").and_then(|e| e.as_str()) == Some("error");
        let code_ok = v.get("code").and_then(|c| c.as_str()) == Some("64003");
        let channel_ok = v
            .get("arg")
            .and_then(|a| a.get("channel"))
            .and_then(|c| c.as_str())
            .map(|c| c == "fills")
            .unwrap_or(true); // arg 缺失时保守处理（也认为是 fills 导致）
        return is_error && code_ok && channel_ok;
    }
    false
}

/// 打印解析后的账户事件
fn log_parsed_event(msg: &Bytes, source: &str) {
    let Some((okex_event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref())
    else {
        return;
    };

    if matches!(okex_event_type, BasicAccountEventType::Error) {
        return;
    }

    match okex_event_type {
        BasicAccountEventType::OrderUpdate => {
            if let Ok(m) = OkexOrderMsg::from_bytes(&payload) {
                let order_status = m.state;
                info!(
                    "OKEx basic OrderUpdate: source={} scope={} inst={} side={} state={} ord_id={} cli_id={} price={} qty={} filled={} update_time={}",
                    source,
                    account_scope.as_str(),
                    m.inst_id,
                    m.side,
                    OkexOrderMsg::state_to_str(order_status),
                    m.ord_id,
                    m.cl_ord_id,
                    m.price,
                    m.quantity,
                    m.cumulative_filled_quantity,
                    m.update_time
                );
            }
        }
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                info!(
                    "OKEx basic BalanceUpdate: source={} scope={} ts={} symbol={} wallet={}",
                    source,
                    account_scope.as_str(),
                    m.timestamp,
                    m.symbol,
                    m.wallet
                );
            }
        }
        BasicAccountEventType::PositionUpdate => {
            if let Ok(m) = BasicPositionMsg::from_bytes(&payload) {
                info!(
                    "OKEx basic PositionUpdate: source={} scope={} ts={} inst={} side={} amt={}",
                    source,
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.position_amount
                );
            }
        }
        BasicAccountEventType::BorrowInterest => {
            if let Ok(m) = BasicBorrowInterestMsg::from_bytes(&payload) {
                info!(
                    "OKEx basic BorrowInterest: source={} scope={} ts={} symbol={} borrowed={} interest={}",
                    source,
                    account_scope.as_str(),
                    m.timestamp,
                    m.symbol,
                    m.borrowed,
                    m.interest
                );
            }
        }
        BasicAccountEventType::UnrealizedPnlUpdate => {
            if let Ok(m) = BasicUmUnrealizedMsg::from_bytes(&payload) {
                info!(
                    "OKEx basic UnrealizedPnl: source={} scope={} ts={} inst={} side={} pnl={}",
                    source,
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.unrealized_pnl
                );
            }
        }
        BasicAccountEventType::AccountRisk => {
            if let Ok(m) = BasicAccountRiskMsg::from_bytes(&payload) {
                info!(
                    "OKEx basic AccountRisk: source={} scope={} ts={} adj_eq_usd={:.2} actual_eq_usd={:.2} maint_margin_usd={:.2} initial_margin_usd={:.2} margin_ratio={:.6}",
                    source,
                    account_scope.as_str(),
                    m.timestamp,
                    m.adj_equity_usd,
                    m.actual_equity_usd,
                    m.maintenance_margin_usd,
                    m.initial_margin_usd,
                    m.margin_ratio
                );
            }
        }
        BasicAccountEventType::TradeUpdateLite => {
            if let Ok(m) = BasicTradeLiteMsg::from_bytes(&payload) {
                info!(
                    "OKEx TradeUpdateLite: scope={} venue={} ts={} symbol={} cloid={} trade_id={} side={} maker={} last_px={} last_qty={}",
                    account_scope.as_str(),
                    m.venue,
                    m.event_time,
                    m.symbol,
                    m.client_order_id,
                    m.trade_id_str(),
                    m.side,
                    m.is_maker,
                    m.last_executed_price,
                    m.last_executed_quantity
                );
            }
        }
        _ => {
            info!(
                "OKEx basic msg: source={} scope={} type={:?}",
                source,
                account_scope.as_str(),
                okex_event_type
            );
        }
    }
}

/// 统一的账户事件去重器
struct AccountEventDeduper {
    seen: HashSet<u64>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl AccountEventDeduper {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn should_forward_key(&mut self, key: u64) -> bool {
        self.remember_key(key)
    }

    /// 检查是否应该转发此消息（返回 true 表示应该转发，false 表示重复消息）
    fn should_forward(&mut self, msg: &Bytes) -> bool {
        let Some((okex_event_type, account_scope, payload)) =
            split_basic_account_event(msg.as_ref())
        else {
            return true;
        };

        // 根据事件类型计算去重 key
        let key_opt = match okex_event_type {
            BasicAccountEventType::OrderUpdate => OkexOrderMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_order(&msg)),
            BasicAccountEventType::BalanceUpdate => BasicBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_balance(&msg)),
            BasicAccountEventType::PositionUpdate => BasicPositionMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_position(&msg)),
            BasicAccountEventType::BorrowInterest => BasicBorrowInterestMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_borrow_interest(&msg)),
            BasicAccountEventType::UnrealizedPnlUpdate => {
                BasicUmUnrealizedMsg::from_bytes(&payload)
                    .ok()
                    .map(|msg| self.key_okex_unrealized_pnl(&msg))
            }
            BasicAccountEventType::AccountRisk => BasicAccountRiskMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_account_risk(&msg)),
            BasicAccountEventType::TradeUpdateLite => BasicTradeLiteMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_trade_lite(&msg)),
            BasicAccountEventType::BinanceStdUmWalletSnapshot => return true,
            BasicAccountEventType::HyperliquidFill
            | BasicAccountEventType::HyperliquidSnapshotComplete
            | BasicAccountEventType::HyperliquidFactReplayControl => return true,
            BasicAccountEventType::Error => return true,
        };

        let Some(key) = key_opt else {
            return true; // 解析失败，直接转发
        };

        let key = self.hash64(&[account_scope as u32 as u64, key]);

        self.remember_key(key)
    }

    fn remember_key(&mut self, key: u64) -> bool {
        if self.seen.contains(&key) {
            return false;
        }

        self.seen.insert(key);
        self.order.push_back(key);

        if self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }

        true
    }

    fn hash64(&self, parts: &[u64]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for p in parts {
            p.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn hash_str64(&self, s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    fn key_okex_balance(&self, msg: &BasicBalanceMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::BalanceUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.symbol),
            msg.wallet.to_bits(),
        ])
    }

    fn key_okex_borrow_interest(&self, msg: &BasicBorrowInterestMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::BorrowInterest as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.symbol),
            msg.borrowed.to_bits(),
            msg.interest.to_bits(),
        ])
    }

    fn key_okex_position(&self, msg: &BasicPositionMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::PositionUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.position_amount.to_bits() as u64,
        ])
    }

    fn key_okex_unrealized_pnl(&self, msg: &BasicUmUnrealizedMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::UnrealizedPnlUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.unrealized_pnl.to_bits(),
        ])
    }

    fn key_okex_account_risk(&self, msg: &BasicAccountRiskMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::AccountRisk as u32 as u64,
            msg.timestamp as u64,
            msg.adj_equity_usd.to_bits(),
            msg.maintenance_margin_usd.to_bits(),
            msg.margin_ratio.to_bits(),
        ])
    }

    fn key_okex_order(&self, msg: &OkexOrderMsg) -> u64 {
        let order_status = msg.state;
        self.hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.ord_id as u64,
            msg.cl_ord_id as u64,
            msg.update_time as u64,
            order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ])
    }

    fn key_trade_lite(&self, msg: &BasicTradeLiteMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::TradeUpdateLite as u32 as u64,
            msg.client_order_id as u64,
            self.hash_str64(msg.trade_id_str()),
            msg.event_time as u64,
            msg.last_executed_price.to_bits(),
            msg.last_executed_quantity.to_bits(),
        ])
    }
}
