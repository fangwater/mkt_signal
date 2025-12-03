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
use mkt_signal::common::account_msg::{
    get_event_type, AccountEventType, AccountPositionMsg, AccountUpdateBalanceMsg,
    AccountUpdateFlushMsg, AccountUpdatePositionMsg, BalanceUpdateMsg, ExecutionReportMsg,
    LiabilityChangeMsg, OrderTradeUpdateMsg,
};
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::parser::okex_account_event_parser::OkexAccountEventParser;
use mkt_signal::portfolio_margin::okex_auth::{
    build_balance_and_position_subscribe_message, build_orders_subscribe_message, OkexCredentials,
    OkexPrivateWsUrls,
};
use mkt_signal::portfolio_margin::okex_user_stream::OkexUserDataConnection;
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
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
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    // 加载配置
    let cfg = mkt_signal::portfolio_margin::pm_cfg::AccountTomlCfg::load("config/account_cfg.toml")
        .await?;

    // 从环境变量加载 OKEx 凭证
    let credentials = OkexCredentials::from_env()?;
    log_credential_preview("OKX_API_KEY", &credentials.api_key);
    log_credential_preview("OKX_API_SECRET", &credentials.secret_key);
    log_credential_preview("OKX_PASSPHRASE", &credentials.passphrase);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // 从配置读取 WebSocket URL，如果没有则使用默认值
    let ws_url = cfg
        .exchanges
        .okex
        .as_ref()
        .and_then(|e| e.ws.as_ref())
        .and_then(|w| w.pm.clone())
        .unwrap_or_else(|| OkexPrivateWsUrls::PRIVATE.to_string());

    info!("Config loaded. ws_url={}", ws_url);

    // IP 和会话设置
    let primary_ip = cfg
        .general
        .primary_local_ip
        .clone()
        .unwrap_or_else(|| "".to_string());
    let secondary_ip = cfg
        .general
        .secondary_local_ip
        .clone()
        .unwrap_or_else(|| "".to_string());
    let session_max = cfg.general.ws_session_max_secs.map(Duration::from_secs);
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?}",
        primary_ip, secondary_ip, session_max
    );

    // 构建订阅消息 - 订阅 SWAP 合约的订单和持仓
    let subscribe_messages = vec![
        build_orders_subscribe_message("SWAP"),
        build_balance_and_position_subscribe_message(),
    ];

    // 创建事件收集通道
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // 创建 PM 转发器 (account_pubs/okex/pm)
    let (pm_hist, pm_subs) = cfg
        .iceoryx
        .as_ref()
        .and_then(|i| i.pm.as_ref())
        .map(|c| (c.history_size, c.max_subscribers))
        .unwrap_or((None, None));
    let mut forwarder = PmForwarder::new("okex", pm_hist, pm_subs)?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));

    // 启动主备双路连接
    let mut primary = spawn_okex_stream_path(
        "primary",
        &ws_url,
        primary_ip,
        credentials.clone(),
        subscribe_messages.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut secondary = spawn_okex_stream_path(
        "secondary",
        &ws_url,
        secondary_ip,
        credentials.clone(),
        subscribe_messages.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // 主循环：接收事件、去重、转发
    loop {
        tokio::select! {
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
            _ = &mut primary => { warn!("primary okex stream task exited; continuing"); }
            _ = &mut secondary => { warn!("secondary okex stream task exited; continuing"); }
            _ = shutdown_rx.changed() => { break; }
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

fn spawn_okex_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: OkexCredentials,
    subscribe_messages: Vec<serde_json::Value>,
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

            // 创建原始消息广播通道
            let (raw_tx, mut raw_rx) = broadcast::channel::<Bytes>(8192);

            // 创建 MktConnection（注意：OKEx 不需要在 URL 中传递订阅消息）
            let mut conn = MktConnection::new(
                ws_url.clone(),
                serde_json::json!({}), // OKEx 在登录后发送订阅消息
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }

            // 创建 OKEx 用户数据连接
            let mut runner = OkexUserDataConnection::new(
                conn,
                credentials.clone(),
                subscribe_messages.clone(),
                session_max,
            );

            // 启动消费者任务
            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = OkexAccountEventParser::new();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] okex ws json: {}", name, local_ip_log, s);
                                    } else {
                                        debug!("[{}][ip={}] okex ws bin: {} bytes", name, local_ip_log, b.len());
                                    }
                                    // 解析并通过通道发送解析后的账户事件（二进制）
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

            // 运行连接直到退出（关闭或错误）
            if let Err(e) = runner.start_ws().await {
                error!("[{}] connection error: {}", name, e);
            }

            // 检查是否需要关闭
            if *shutdown_rx.borrow() {
                info!("[{}] shutdown signal received, exiting", name);
                break;
            }

            // 等待2秒后重连
            info!("[{}] connection closed, reconnecting in 2s...", name);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

/// 打印解析后的账户事件
fn log_parsed_event(msg: &Bytes) {
    if msg.len() < 8 {
        return;
    }
    let event_type = get_event_type(msg.as_ref());
    let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
    if msg.len() < 8 + payload_len {
        return;
    }
    let payload = msg.slice(8..8 + payload_len);

    match event_type {
        AccountEventType::ExecutionReport => {
            if let Ok(m) = ExecutionReportMsg::from_bytes(&payload) {
                info!(
                    "ExecutionReport: sym={} side={} status={} cli_id={} ord_id={} price={} qty={} filled={}",
                    m.symbol, m.side, m.order_status, m.client_order_id, m.order_id, m.price, m.quantity, m.cumulative_filled_quantity
                );
            }
        }
        AccountEventType::OrderTradeUpdate => {
            if let Ok(m) = OrderTradeUpdateMsg::from_bytes(&payload) {
                info!(
                    "OKEx OrderTradeUpdate: sym={} side={} status={} cli_id={} ord_id={} price={} qty={} filled={}",
                    m.symbol, m.side, m.order_status, m.client_order_id, m.order_id, m.price, m.quantity, m.cumulative_filled_quantity
                );
            }
        }
        AccountEventType::BalanceUpdate => {
            if let Ok(m) = BalanceUpdateMsg::from_bytes(&payload) {
                info!("OKEx BalanceUpdate: asset={} delta={}", m.asset, m.delta);
            }
        }
        AccountEventType::AccountUpdateBalance => {
            if let Ok(m) = AccountUpdateBalanceMsg::from_bytes(&payload) {
                info!(
                    "OKEx AccountUpdateBalance: asset={} wallet={} cross_wallet={}",
                    m.asset, m.wallet_balance, m.cross_wallet_balance
                );
            }
        }
        AccountEventType::LiabilityChange => {
            if let Ok(m) = LiabilityChangeMsg::from_bytes(&payload) {
                info!(
                    "OKEx LiabilityChange: asset={} type={} principal={} interest={} total={}",
                    m.asset, m.liability_type, m.principal, m.interest, m.total_liability
                );
            }
        }
        AccountEventType::AccountUpdatePosition => {
            if let Ok(m) = AccountUpdatePositionMsg::from_bytes(&payload) {
                info!(
                    "OKEx AccountUpdatePosition: sym={} side={} amt={} entry={} upnl={} reason={} bu={}",
                    m.symbol, m.position_side, m.position_amount, m.entry_price, m.unrealized_pnl, m.reason, m.business_unit
                );
            }
        }
        _ => {
            debug!("OKEx PM event: type={:?} len={}", event_type, payload_len);
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
        if msg.len() < 8 {
            return true; // 格式不对的消息直接转发
        }

        let event_type = get_event_type(msg.as_ref());
        let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
        if msg.len() < 8 + payload_len {
            return true; // 格式不对的消息直接转发
        }

        let payload = msg.slice(8..8 + payload_len);

        // 根据事件类型计算去重 key
        let key_opt = match event_type {
            AccountEventType::AccountPosition => AccountPositionMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_account_position(&msg)),
            AccountEventType::BalanceUpdate => BalanceUpdateMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_balance_update(&msg)),
            AccountEventType::AccountUpdateBalance => AccountUpdateBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_account_update_balance(&msg)),
            AccountEventType::AccountUpdatePosition => {
                AccountUpdatePositionMsg::from_bytes(&payload)
                    .ok()
                    .map(|msg| self.key_account_update_position(&msg))
            }
            AccountEventType::AccountUpdateFlush => AccountUpdateFlushMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_account_update_flush(&msg)),
            AccountEventType::ExecutionReport => ExecutionReportMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_execution_report(&msg)),
            AccountEventType::OrderTradeUpdate => OrderTradeUpdateMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_order_trade_update(&msg)),
            _ => {
                return true; // 其他事件类型不去重，直接转发
            }
        };

        let Some(key) = key_opt else {
            return true; // 解析失败，直接转发
        };

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

    fn key_account_position(&self, msg: &AccountPositionMsg) -> u64 {
        self.hash64(&[
            AccountEventType::AccountPosition as u32 as u64,
            msg.update_id as u64,
            msg.event_time as u64,
            self.hash_str64(&msg.asset),
        ])
    }

    fn key_balance_update(&self, msg: &BalanceUpdateMsg) -> u64 {
        self.hash64(&[
            AccountEventType::BalanceUpdate as u32 as u64,
            msg.update_id as u64,
            msg.event_time as u64,
        ])
    }

    fn key_account_update_balance(&self, msg: &AccountUpdateBalanceMsg) -> u64 {
        self.hash64(&[
            AccountEventType::AccountUpdateBalance as u32 as u64,
            msg.event_time as u64,
            msg.transaction_time as u64,
            self.hash_str64(&msg.asset),
        ])
    }

    fn key_account_update_position(&self, msg: &AccountUpdatePositionMsg) -> u64 {
        self.hash64(&[
            AccountEventType::AccountUpdatePosition as u32 as u64,
            msg.event_time as u64,
            msg.transaction_time as u64,
            self.hash_str64(&msg.symbol),
            msg.position_side as u8 as u64,
        ])
    }

    fn key_account_update_flush(&self, msg: &AccountUpdateFlushMsg) -> u64 {
        self.hash64(&[
            AccountEventType::AccountUpdateFlush as u32 as u64,
            msg.hash,
            self.hash_str64(&msg.scope),
        ])
    }

    fn key_execution_report(&self, msg: &ExecutionReportMsg) -> u64 {
        self.hash64(&[
            AccountEventType::ExecutionReport as u32 as u64,
            msg.order_id as u64,
            msg.trade_id as u64,
            msg.update_id as u64,
            msg.event_time as u64,
        ])
    }

    fn key_order_trade_update(&self, msg: &OrderTradeUpdateMsg) -> u64 {
        self.hash64(&[
            AccountEventType::OrderTradeUpdate as u32 as u64,
            msg.order_id as u64,
            msg.trade_id as u64,
            msg.event_time as u64,
        ])
    }
}
