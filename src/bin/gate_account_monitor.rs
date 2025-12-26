//! Gate.io 账户监控程序
//!
//! 功能：
//! - 连接到 Gate.io 统一账户 WebSocket 频道
//! - 使用 API Key/Secret 进行鉴权
//! - 订阅 unified.asset_detail 频道
//! - 解析账户事件并通过 Iceoryx 转发
//! - 支持主备双路连接
//!
//! 使用方式：
//! ```bash
//! export GATE_API_KEY="your_api_key"
//! export GATE_API_SECRET="your_api_secret"
//! cargo run --bin gate_account_monitor
//! ```

use anyhow::Result;
use bytes::Bytes;
use log::{debug, error, info, warn};
use mkt_signal::common::basic_account_msg::{
    get_basic_event_type, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    GateBasicOrderMsg,
};
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::parser::gate_account_event_parser::GateAccountEventParser;
use mkt_signal::portfolio_margin::gate_auth::{GateCredentials, GatePrivateWsUrls};
use mkt_signal::portfolio_margin::gate_user_stream::{GateUserDataConnection, SubscribeChannel};
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
    env_logger::init();

    // 从环境变量加载 Gate.io 凭证
    let credentials = GateCredentials::from_env()?;
    log_credential_preview("GATE_API_KEY", &credentials.api_key);
    log_credential_preview("GATE_API_SECRET", &credentials.secret_key);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // WebSocket URLs
    let unified_ws_url = GatePrivateWsUrls::UNIFIED.to_string();
    let futures_ws_url = GatePrivateWsUrls::FUTURES_USDT.to_string();

    // IP 设置（可选，如果需要主备双路）
    const GATE_PRIMARY_IP: &str = "172.31.33.133";
    const GATE_SECONDARY_IP: &str = "172.31.46.90";
    let primary_ip = GATE_PRIMARY_IP.to_string();
    let secondary_ip = GATE_SECONDARY_IP.to_string();
    let session_max = None; // Gate.io 没有明确的会话时长限制
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?}",
        primary_ip, secondary_ip, session_max
    );

    // 统一账户频道 (unified.asset_detail + spot.orders_v2)
    let unified_channels = vec![
        SubscribeChannel {
            channel: "unified.asset_detail".to_string(),
            payload: vec!["!all".to_string()],
        },
        SubscribeChannel {
            channel: "spot.orders_v2".to_string(),
            payload: vec!["!all".to_string()],
        },
    ];

    // 合约频道 (futures.orders)
    let futures_channels = vec![
        SubscribeChannel {
            channel: "futures.orders".to_string(),
            payload: vec!["!all".to_string()],
        },
    ];

    // 创建事件收集通道
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // 创建 PM 转发器 (account_pubs/gate/pm)
    let mut forwarder = PmForwarder::new("gate")?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));

    // 启动统一账户主备双路连接 (unified.asset_detail + spot.orders_v2)
    let mut unified_primary = spawn_gate_stream_path(
        "unified-primary",
        &unified_ws_url,
        primary_ip.clone(),
        credentials.clone(),
        unified_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut unified_secondary = spawn_gate_stream_path(
        "unified-secondary",
        &unified_ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        unified_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // 启动合约主备双路连接 (futures.orders)
    let mut futures_primary = spawn_gate_stream_path(
        "futures-primary",
        &futures_ws_url,
        primary_ip,
        credentials.clone(),
        futures_channels.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut futures_secondary = spawn_gate_stream_path(
        "futures-secondary",
        &futures_ws_url,
        secondary_ip,
        credentials.clone(),
        futures_channels.clone(),
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
            // 统一账户连接重启
            res = &mut unified_primary => {
                match res {
                    Ok(()) => warn!("unified-primary task exited; restarting"),
                    Err(e) => warn!("unified-primary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    unified_primary = spawn_gate_stream_path(
                        "unified-primary",
                        &unified_ws_url,
                        GATE_PRIMARY_IP.to_string(),
                        credentials.clone(),
                        unified_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut unified_secondary => {
                match res {
                    Ok(()) => warn!("unified-secondary task exited; restarting"),
                    Err(e) => warn!("unified-secondary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    unified_secondary = spawn_gate_stream_path(
                        "unified-secondary",
                        &unified_ws_url,
                        GATE_SECONDARY_IP.to_string(),
                        credentials.clone(),
                        unified_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            // 合约连接重启
            res = &mut futures_primary => {
                match res {
                    Ok(()) => warn!("futures-primary task exited; restarting"),
                    Err(e) => warn!("futures-primary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    futures_primary = spawn_gate_stream_path(
                        "futures-primary",
                        &futures_ws_url,
                        GATE_PRIMARY_IP.to_string(),
                        credentials.clone(),
                        futures_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut futures_secondary => {
                match res {
                    Ok(()) => warn!("futures-secondary task exited; restarting"),
                    Err(e) => warn!("futures-secondary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    futures_secondary = spawn_gate_stream_path(
                        "futures-secondary",
                        &futures_ws_url,
                        GATE_SECONDARY_IP.to_string(),
                        credentials.clone(),
                        futures_channels.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
        }
    }

    info!("Gate account monitor shutdown complete");
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

fn spawn_gate_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: GateCredentials,
    channels: Vec<SubscribeChannel>,
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

            // 创建 MktConnection
            let mut conn = MktConnection::new(
                ws_url.clone(),
                serde_json::json!({}), // Gate.io 在连接后发送订阅消息
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }

            // 创建 Gate 用户数据连接（每次重连时会重新生成带新签名的订阅消息）
            let mut runner = GateUserDataConnection::new(
                conn,
                credentials.clone(),
                channels.clone(),
                session_max,
            );

            // 启动消费者任务
            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = GateAccountEventParser::new();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] gate ws json: {}", name, local_ip_log, s);
                                    } else {
                                        debug!("[{}][ip={}] gate ws bin: {} bytes", name, local_ip_log, b.len());
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
    let event_type = get_basic_event_type(msg.as_ref());
    let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
    if msg.len() < 8 + payload_len {
        return;
    }
    let payload = msg.slice(8..8 + payload_len);

    if matches!(event_type, BasicAccountEventType::Error) {
        info!(
            "Gate basic msg: type={} payload_len={}",
            u32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]),
            payload_len
        );
        return;
    }

    match event_type {
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                info!(
                    "Gate BalanceUpdate: ts={} symbol={} balance={}",
                    m.timestamp, m.symbol, m.balance
                );
            }
        }
        BasicAccountEventType::BorrowInterest => {
            if let Ok(m) = BasicBorrowInterestMsg::from_bytes(&payload) {
                info!(
                    "Gate BorrowInterest: ts={} symbol={} borrowed={} interest={}",
                    m.timestamp, m.symbol, m.borrowed, m.interest
                );
            }
        }
        BasicAccountEventType::OrderUpdate => {
            if let Ok(m) = GateBasicOrderMsg::from_bytes(&payload) {
                info!(
                    "Gate OrderUpdate: venue={} ts={} symbol={} oid={} cloid={} side={} type={} exec={} status={} maker={} px={} qty={} filled={} fill_px={}",
                    m.venue, m.event_time, m.symbol, m.order_id, m.client_order_id,
                    m.side, m.order_type, m.execution_type, m.order_status,
                    m.is_maker, m.price, m.quantity, m.cumulative_filled_quantity, m.last_executed_price
                );
            }
        }
        _ => {
            info!(
                "Gate basic msg: type={:?} payload_len={}",
                event_type, payload_len
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

        let event_type = get_basic_event_type(msg.as_ref());
        let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
        if msg.len() < 8 + payload_len {
            return true; // 格式不对的消息直接转发
        }

        let payload = msg.slice(8..8 + payload_len);

        // 根据事件类型计算去重 key
        let key_opt = match event_type {
            BasicAccountEventType::BalanceUpdate => BasicBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_balance(&msg)),
            BasicAccountEventType::BorrowInterest => BasicBorrowInterestMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_borrow_interest(&msg)),
            BasicAccountEventType::OrderUpdate => GateBasicOrderMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_order(&msg)),
            _ => return true, // 其他类型直接转发
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

    fn key_balance(&self, msg: &BasicBalanceMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::BalanceUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.symbol),
            msg.balance.to_bits(),
        ])
    }

    fn key_borrow_interest(&self, msg: &BasicBorrowInterestMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::BorrowInterest as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.symbol),
            msg.borrowed.to_bits(),
            msg.interest.to_bits(),
        ])
    }

    fn key_order(&self, msg: &GateBasicOrderMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.event_time as u64,
            self.hash_str64(&msg.symbol),
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.execution_type as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ])
    }
}
