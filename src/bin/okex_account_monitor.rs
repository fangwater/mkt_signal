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
use mkt_signal::common::okex_account_msg::{
    get_okex_event_type, OkexAccountEventType, OkexBalanceMsg, OkexOrderMsg, OkexPositionMsg,
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

    // 从环境变量加载 OKEx 凭证
    let credentials = OkexCredentials::from_env()?;
    log_credential_preview("OKX_API_KEY", &credentials.api_key);
    log_credential_preview("OKX_API_SECRET", &credentials.secret_key);
    log_credential_preview("OKX_PASSPHRASE", &credentials.passphrase);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // WebSocket URL 固定，跳过配置
    const OKEX_PM_WS: &str = OkexPrivateWsUrls::PRIVATE;
    const OKEX_PRIMARY_IP: &str = "172.31.33.133";
    const OKEX_SECONDARY_IP: &str = "172.31.46.90";
    let ws_url = OKEX_PM_WS.to_string();

    // IP 和会话设置
    let primary_ip = OKEX_PRIMARY_IP.to_string();
    let secondary_ip = OKEX_SECONDARY_IP.to_string();
    let session_max = None;
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
    let mut forwarder = PmForwarder::new("okex")?;
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
    let okex_event_type = get_okex_event_type(msg.as_ref());
    let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
    if msg.len() < 8 + payload_len {
        return;
    }
    let payload = msg.slice(8..8 + payload_len);

    if matches!(okex_event_type, OkexAccountEventType::Error) {
        debug!(
            "OKEx account event unknown type={} len={}",
            u32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]),
            payload_len
        );
        return;
    }

    match okex_event_type {
        OkexAccountEventType::OrderUpdate => {
            if let Ok(m) = OkexOrderMsg::from_bytes(&payload) {
                info!(
                    "OKEx OrderUpdate: inst={} side={} state={} cancel_src={} amend_src={} cli_id={} ord_id={} price={} qty={} cumulative_filled={} create_time={} update_time={} fill_time={}",
                    m.inst_id,
                    m.side,
                    OkexOrderMsg::state_to_str(m.state),
                    OkexOrderMsg::cancel_source_to_str(m.cancel_source),
                    OkexOrderMsg::amend_source_to_str(m.amend_source),
                    m.cl_ord_id,
                    m.ord_id,
                    m.price,
                    m.quantity,
                    m.cumulative_filled_quantity,
                    m.create_time,
                    m.update_time,
                    m.fill_time
                );
            }
        }
        OkexAccountEventType::BalanceUpdate => {
            if let Ok(m) = OkexBalanceMsg::from_bytes(&payload) {
                info!(
                    "OKEx BalanceUpdate: ts={} balance={}",
                    m.timestamp, m.balance
                );
            }
        }
        OkexAccountEventType::PositionUpdate => {
            if let Ok(m) = OkexPositionMsg::from_bytes(&payload) {
                info!(
                    "OKEx PositionUpdate: ts={} inst={} side={} amt={}",
                    m.timestamp, m.inst_id, m.position_side, m.position_amount
                );
            }
        }
        _ => {
            debug!(
                "OKEx account event (okex type={:?}) len={}",
                okex_event_type, payload_len
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
        if msg.len() < 8 {
            return true; // 格式不对的消息直接转发
        }

        let okex_event_type = get_okex_event_type(msg.as_ref());
        let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
        if msg.len() < 8 + payload_len {
            return true; // 格式不对的消息直接转发
        }

        let payload = msg.slice(8..8 + payload_len);

        // 根据事件类型计算去重 key
        let key_opt = match okex_event_type {
            OkexAccountEventType::OrderUpdate => OkexOrderMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_order(&msg)),
            OkexAccountEventType::BalanceUpdate => OkexBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_balance(&msg)),
            OkexAccountEventType::PositionUpdate => OkexPositionMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_okex_position(&msg)),
            OkexAccountEventType::Error => return true,
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

    fn key_okex_balance(&self, msg: &OkexBalanceMsg) -> u64 {
        self.hash64(&[
            OkexAccountEventType::BalanceUpdate as u32 as u64,
            msg.timestamp as u64,
            msg.balance.to_bits(),
        ])
    }

    fn key_okex_position(&self, msg: &OkexPositionMsg) -> u64 {
        self.hash64(&[
            OkexAccountEventType::PositionUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
        ])
    }

    fn key_okex_order(&self, msg: &OkexOrderMsg) -> u64 {
        self.hash64(&[
            OkexAccountEventType::OrderUpdate as u32 as u64,
            msg.ord_id as u64,
            msg.cl_ord_id as u64,
            msg.ord_type as u64,
            msg.update_time as u64,
        ])
    }
}
