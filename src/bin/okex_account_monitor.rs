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

use anyhow::Result;
use bytes::Bytes;
use log::{debug, error, info, warn};
use mkt_signal::common::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountRiskMsg, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg, BasicUmUnrealizedMsg,
    OkexOrderMsg,
};
use mkt_signal::common::mkt_cfg::load_local_ips_preferring_trade_engine;
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::parser::okex_account_event_parser::OkexAccountEventParser;
use mkt_signal::portfolio_margin::okex_auth::{
    build_account_subscribe_message, build_balance_and_position_subscribe_message,
    build_fills_subscribe_message, build_orders_subscribe_message, OkexCredentials,
    OkexPrivateWsUrls,
};
use mkt_signal::portfolio_margin::okex_rest::fetch_borrow_interest;
use mkt_signal::portfolio_margin::okex_user_stream::OkexUserDataConnection;
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use reqwest::Client;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, watch};

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
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
    ];
    // fills 频道需要 VIP4+；收到 64003 后永久关闭，避免无效重连循环。
    let fills_disabled = Arc::new(AtomicBool::new(false));

    // 创建事件收集通道
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // 创建 PM 转发器 (account_pubs/okex/pm)
    let mut forwarder = PmForwarder::new("okex")?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));
    let mut interest_poll =
        spawn_borrow_interest_poll(credentials.clone(), evt_tx.clone(), shutdown_rx.clone());

    // 启动主备双路连接
    let mut primary = spawn_okex_stream_path(
        "primary",
        &ws_url,
        primary_ip.clone(),
        credentials.clone(),
        base_subscribe_messages.clone(),
        fills_disabled.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut secondary = spawn_okex_stream_path(
        "secondary",
        &ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        base_subscribe_messages.clone(),
        fills_disabled.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // 主循环：接收事件、去重、转发
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => { break; }
            Some(msg) = evt_rx.recv() => {
                // 统一去重后再发送
                if deduper.should_forward(&msg) {
                    // 打印解析后的消息
                    log_parsed_event(&msg);
                    forwarder.send_raw(&msg);
                }
            }
            _ = stats.tick() => {
                forwarder.log_stats();
            }
            res = &mut interest_poll => {
                match res {
                    Ok(()) => warn!("interest poll task exited; restarting"),
                    Err(e) => warn!("interest poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    interest_poll = spawn_borrow_interest_poll(credentials.clone(), evt_tx.clone(), shutdown_rx.clone());
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
                        fills_disabled.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
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
                        fills_disabled.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
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
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
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
                                let event = mkt_signal::common::basic_account_msg::BasicAccountEventMsg::create(
                                    msg.msg_type,
                                    mkt_signal::common::basic_account_msg::BasicAccountScope::OkexUnified,
                                    payload,
                                );
                                if let Err(e) = evt_tx.send(event.to_bytes()) {
                                    warn!("failed to send borrow interest msg: {}", e);
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

fn spawn_okex_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: OkexCredentials,
    base_subscribe_messages: Vec<serde_json::Value>,
    fills_disabled: Arc<AtomicBool>,
    shutdown_rx: watch::Receiver<bool>,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    session_max: Option<Duration>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = ws_url.to_string();
    tokio::spawn(async move {
        loop {
            info!(
                "[{}] connecting to {} (local_ip='{}')",
                name, ws_url, local_ip
            );

            let (raw_tx, mut raw_rx) = broadcast::channel::<Bytes>(8192);

            // 每次重连动态拼 fills（VIP 不足后不再加入）
            let mut subscribe_messages = base_subscribe_messages.clone();
            if !fills_disabled.load(Ordering::Relaxed) {
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

            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = OkexAccountEventParser::new();
            let fills_disabled_inner = fills_disabled.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] okex ws json: {}", name, local_ip_log, s);
                                        // 检测 fills VIP 不足错误（code 64003）
                                        if is_fills_vip_error(s) {
                                            warn!("[{}] OKX fills channel requires VIP4+, disabling fills subscription (code 64003)", name);
                                            fills_disabled_inner.store(true, Ordering::Relaxed);
                                        }
                                    } else {
                                        debug!("[{}][ip={}] okex ws bin: {} bytes", name, local_ip_log, b.len());
                                    }
                                    let _ = parser.parse(b, &evt_tx_clone);
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!("[{}] lagged: skipped {} msgs", name, skipped);
                                }
                            }
                        }
                        _ = consumer_shutdown.changed() => {
                            if *consumer_shutdown.borrow() { break; }
                        }
                    }
                }
            });

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
fn log_parsed_event(msg: &Bytes) {
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
                    "OKEx basic OrderUpdate: scope={} inst={} side={} state={} ord_id={} cli_id={} price={} qty={} filled={} update_time={}",
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
                    "OKEx basic BalanceUpdate: scope={} ts={} symbol={} wallet={}",
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
                    "OKEx basic PositionUpdate: scope={} ts={} inst={} side={} amt={}",
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
                    "OKEx basic BorrowInterest: scope={} ts={} symbol={} borrowed={} interest={}",
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
                    "OKEx basic UnrealizedPnl: scope={} ts={} inst={} side={} pnl={}",
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
                    "OKEx basic AccountRisk: scope={} ts={} adj_eq_usd={:.2} actual_eq_usd={:.2} maint_margin_usd={:.2} initial_margin_usd={:.2} margin_ratio={:.6}",
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
                "OKEx basic msg: scope={} type={:?}",
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
            BasicAccountEventType::Error => return true,
        };

        let Some(key) = key_opt else {
            return true; // 解析失败，直接转发
        };

        let key = self.hash64(&[account_scope as u32 as u64, key]);

        // 检查是否重复
        if self.seen.contains(&key) {
            return false; // 重复消息，不转发
        }

        // 记录新消息
        self.seen.insert(key);
        self.order.push_back(key);

        // 容量控制
        if self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }

        true // 新消息，转发
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
