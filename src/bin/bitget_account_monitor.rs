//! Bitget UTA 账户监控程序
//!
//! 功能：
//! - 连接 Bitget UTA 私有 WebSocket 频道
//! - 使用 API Key/Secret/Passphrase 登录鉴权
//! - 订阅 account / positions / orders 频道
//! - 解析账户事件并通过 Iceoryx 转发
//! - 支持主备双路连接

use anyhow::Result;
use bytes::Bytes;
use log::{debug, error, info, warn};
use mkt_signal::common::basic_account_msg::{
    get_basic_event_type, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg, BasicUmUnrealizedMsg,
};
use mkt_signal::common::bitget_account_msg::BitgetBasicOrderMsg;
use mkt_signal::common::mkt_cfg::{home_mkt_cfg_path, load_local_ips_from_path};
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::bitget_account_event_parser::BitgetAccountEventParser;
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::portfolio_margin::bitget_auth::{
    build_account_subscribe_message, build_orders_subscribe_message,
    build_positions_subscribe_message, BitgetCredentials, BitgetPrivateWsUrls,
};
use mkt_signal::portfolio_margin::bitget_user_stream::BitgetUserDataConnection;
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

    let credentials = BitgetCredentials::from_env()?;
    log_credential_preview("BITGET_API_KEY", &credentials.api_key);
    log_credential_preview("BITGET_API_SECRET", &credentials.secret_key);
    log_credential_preview("BITGET_API_PASSPHRASE", &credentials.passphrase);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    let ws_url = BitgetPrivateWsUrls::PRIVATE.to_string();

    let cfg_path = home_mkt_cfg_path()?;
    let (primary_ip, secondary_ip) = load_local_ips_from_path(&cfg_path).await?;
    let session_max = None;
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?} (mkt_cfg: {})",
        primary_ip,
        secondary_ip,
        session_max,
        cfg_path.display()
    );

    let mut subscribe_messages = vec![
        build_account_subscribe_message(),
        build_positions_subscribe_message(),
    ];
    subscribe_messages.extend(build_orders_subscribe_message());

    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let mut forwarder = PmForwarder::new("bitget")?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));

    let mut primary = spawn_bitget_stream_path(
        "primary",
        &ws_url,
        primary_ip.clone(),
        credentials.clone(),
        subscribe_messages.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut secondary = spawn_bitget_stream_path(
        "secondary",
        &ws_url,
        secondary_ip.clone(),
        credentials.clone(),
        subscribe_messages.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => { break; }
            Some(msg) = evt_rx.recv() => {
                if deduper.should_forward(&msg) {
                    log_parsed_event(&msg);
                    forwarder.send_raw(&msg);
                }
            }
            _ = stats.tick() => {
                forwarder.log_stats();
            }
            res = &mut primary => {
                match res {
                    Ok(()) => warn!("primary task exited; restarting"),
                    Err(e) => warn!("primary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    primary = spawn_bitget_stream_path(
                        "primary",
                        &ws_url,
                        primary_ip.clone(),
                        credentials.clone(),
                        subscribe_messages.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
            res = &mut secondary => {
                match res {
                    Ok(()) => warn!("secondary task exited; restarting"),
                    Err(e) => warn!("secondary task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    secondary = spawn_bitget_stream_path(
                        "secondary",
                        &ws_url,
                        secondary_ip.clone(),
                        credentials.clone(),
                        subscribe_messages.clone(),
                        shutdown_rx.clone(),
                        evt_tx.clone(),
                        session_max,
                    );
                }
            }
        }
    }

    info!("Bitget account monitor shutdown complete");
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

fn spawn_bitget_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: BitgetCredentials,
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

            let (raw_tx, mut raw_rx) = broadcast::channel::<Bytes>(8192);

            let mut conn = MktConnection::new(
                ws_url.clone(),
                serde_json::json!({}),
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }

            let mut runner = BitgetUserDataConnection::new(
                conn,
                credentials.clone(),
                subscribe_messages.clone(),
                session_max,
            );

            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = BitgetAccountEventParser::new();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] bitget ws json: {}", name, local_ip_log, s);
                                    } else {
                                        debug!("[{}][ip={}] bitget ws bin: {} bytes", name, local_ip_log, b.len());
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
        return;
    }

    match event_type {
        BasicAccountEventType::OrderUpdate => {
            if let Ok(m) = BitgetBasicOrderMsg::from_bytes(&payload) {
                info!(
                    "Bitget OrderUpdate: venue={} ts={} symbol={} oid={} cloid={} side={} type={} exec={} status={} maker={} px={} qty={} filled={} fill_px={}",
                    m.venue, m.event_time, m.symbol, m.order_id, m.client_order_id,
                    m.side, m.order_type, m.execution_type, m.order_status,
                    m.is_maker, m.price, m.quantity, m.cumulative_filled_quantity, m.last_executed_price
                );
            }
        }
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                info!(
                    "Bitget BalanceUpdate: ts={} symbol={} balance={}",
                    m.timestamp, m.symbol, m.balance
                );
            }
        }
        BasicAccountEventType::PositionUpdate => {
            if let Ok(m) = BasicPositionMsg::from_bytes(&payload) {
                info!(
                    "Bitget PositionUpdate: ts={} inst={} side={} pos={}",
                    m.timestamp, m.inst_id, m.position_side, m.position_amount
                );
            }
        }
        BasicAccountEventType::UnrealizedPnlUpdate => {
            if let Ok(m) = BasicUmUnrealizedMsg::from_bytes(&payload) {
                info!(
                    "Bitget UnrealizedPnl: ts={} inst={} side={} pnl={}",
                    m.timestamp, m.inst_id, m.position_side, m.unrealized_pnl
                );
            }
        }
        BasicAccountEventType::BorrowInterest => {
            if let Ok(m) = BasicBorrowInterestMsg::from_bytes(&payload) {
                info!(
                    "Bitget BorrowInterest: ts={} symbol={} borrowed={} interest={}",
                    m.timestamp, m.symbol, m.borrowed, m.interest
                );
            }
        }
        _ => {}
    }
}

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

    fn should_forward(&mut self, msg: &Bytes) -> bool {
        if msg.len() < 8 {
            return true;
        }

        let event_type = get_basic_event_type(msg.as_ref());
        let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
        if msg.len() < 8 + payload_len {
            return true;
        }

        let payload = msg.slice(8..8 + payload_len);
        let key_opt = match event_type {
            BasicAccountEventType::OrderUpdate => BitgetBasicOrderMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_order(&msg)),
            BasicAccountEventType::BalanceUpdate => BasicBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_balance(&msg)),
            BasicAccountEventType::PositionUpdate => BasicPositionMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_position(&msg)),
            BasicAccountEventType::UnrealizedPnlUpdate => {
                BasicUmUnrealizedMsg::from_bytes(&payload)
                    .ok()
                    .map(|msg| self.key_unrealized_pnl(&msg))
            }
            BasicAccountEventType::BorrowInterest => BasicBorrowInterestMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_borrow_interest(&msg)),
            BasicAccountEventType::Error => return true,
        };

        let Some(key) = key_opt else {
            return true;
        };

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

    fn key_position(&self, msg: &BasicPositionMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::PositionUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.position_amount.to_bits() as u64,
        ])
    }

    fn key_unrealized_pnl(&self, msg: &BasicUmUnrealizedMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::UnrealizedPnlUpdate as u32 as u64,
            msg.timestamp as u64,
            self.hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.unrealized_pnl.to_bits(),
        ])
    }

    fn key_order(&self, msg: &BitgetBasicOrderMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ])
    }
}
