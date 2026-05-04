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
    split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use mkt_signal::common::bitget_account_msg::BitgetBasicOrderMsg;
use mkt_signal::common::mkt_cfg::load_local_ips_preferring_trade_engine;
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::bitget_account_event_parser::BitgetAccountEventParser;
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::portfolio_margin::bitget_auth::{
    build_account_subscribe_message, build_orders_subscribe_message,
    build_positions_subscribe_message, BitgetCredentials, BitgetPrivateWsUrls,
};
use mkt_signal::portfolio_margin::bitget_user_stream::BitgetUserDataConnection;
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use mkt_signal::trade_engine::query_parsers::bitget_account_balance_snapshot::parse_bitget_account_balance_snapshot;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
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

    let ((primary_ip, secondary_ip), ip_source) = load_local_ips_preferring_trade_engine().await?;
    let session_max = None;
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?} (local_ip_source: {})",
        primary_ip, secondary_ip, session_max, ip_source
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
    let mut balance_poll = spawn_bitget_balance_poll(
        credentials.clone(),
        primary_ip.clone(),
        evt_tx.clone(),
        shutdown_rx.clone(),
    );

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
            res = &mut balance_poll => {
                match res {
                    Ok(()) => warn!("balance poll task exited; restarting"),
                    Err(e) => warn!("balance poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    balance_poll = spawn_bitget_balance_poll(
                        credentials.clone(),
                        primary_ip.clone(),
                        evt_tx.clone(),
                        shutdown_rx.clone(),
                    );
                }
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

fn build_bitget_rest_client(local_ip: Option<&str>, timeout: Duration) -> Result<reqwest::Client> {
    let builder = reqwest::Client::builder().timeout(timeout);
    let builder = match local_ip.map(str::trim).filter(|ip| !ip.is_empty()) {
        Some(ip) if ip != "0.0.0.0" => {
            let parsed: IpAddr = ip
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid Bitget REST local_ip {}: {}", ip, e))?;
            builder.local_address(parsed)
        }
        _ => builder,
    };
    builder
        .build()
        .map_err(|e| anyhow::anyhow!("build Bitget REST client failed: {}", e))
}

fn wrap_basic_payload(account_scope: BasicAccountScope, payload: Bytes) -> Option<Bytes> {
    let event_type = mkt_signal::common::basic_account_msg::get_basic_event_type(&payload);
    if matches!(event_type, BasicAccountEventType::Error) {
        return None;
    }
    Some(BasicAccountEventMsg::create(event_type, account_scope, payload).to_bytes())
}

fn sign_bitget_rest_request(
    credentials: &BitgetCredentials,
    timestamp_ms: i64,
    method: &str,
    request_path: &str,
) -> String {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;
    let payload = format!("{}{}{}", timestamp_ms, method.to_uppercase(), request_path);
    let mut mac = HmacSha256::new_from_slice(credentials.secret_key.as_bytes())
        .expect("HMAC can take any size");
    mac.update(payload.as_bytes());
    BASE64.encode(mac.finalize().into_bytes())
}

async fn bitget_rest_get_account_assets(
    client: &reqwest::Client,
    credentials: &BitgetCredentials,
) -> Result<String> {
    let timestamp_ms = chrono::Utc::now().timestamp_millis();
    let path = "/api/v3/account/assets";
    let sign = sign_bitget_rest_request(credentials, timestamp_ms, "GET", &path);
    let url = format!("https://api.bitget.com{}", path);

    let resp = client
        .get(url)
        .header("ACCESS-KEY", &credentials.api_key)
        .header("ACCESS-SIGN", sign)
        .header("ACCESS-TIMESTAMP", timestamp_ms.to_string())
        .header("ACCESS-PASSPHRASE", &credentials.passphrase)
        .header("locale", "zh-CN")
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!(
            "bitget account assets GET failed: status={} body={}",
            status.as_u16(),
            body
        );
    }
    Ok(body)
}

fn parse_bitget_account_assets_snapshot(json: &str) -> Option<Vec<Bytes>> {
    parse_bitget_account_balance_snapshot(json)
}

fn spawn_bitget_balance_poll(
    credentials: BitgetCredentials,
    local_ip: String,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = match build_bitget_rest_client(Some(&local_ip), Duration::from_secs(10)) {
            Ok(c) => c,
            Err(e) => {
                warn!("build Bitget REST client failed: {:?}", e);
                return;
            }
        };
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        let mut previous_borrow_symbols: HashSet<String> = HashSet::new();
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    match bitget_rest_get_account_assets(&client, &credentials).await {
                        Ok(body) => {
                            if let Some(msgs) = parse_bitget_account_assets_snapshot(&body) {
                                let mut current_borrow_symbols: HashSet<String> = HashSet::new();
                                for payload in msgs {
                                    if let Ok(msg) = BasicBorrowInterestMsg::from_bytes(&payload) {
                                        current_borrow_symbols.insert(msg.symbol.clone());
                                    }
                                    if let Some(wrapped) =
                                        wrap_basic_payload(BasicAccountScope::BitgetUnified, payload)
                                    {
                                        if let Err(e) = evt_tx.send(wrapped) {
                                            warn!("failed to send Bitget REST balance msg: {}", e);
                                        }
                                    }
                                }
                                let mut stale_symbols: Vec<String> = previous_borrow_symbols
                                    .difference(&current_borrow_symbols)
                                    .cloned()
                                    .collect();
                                stale_symbols.sort();
                                let now_ms = chrono::Utc::now().timestamp_millis();
                                for symbol in stale_symbols {
                                    info!(
                                        "Bitget REST balance snapshot missing previously-seen borrow; emitting zero cleanup: symbol={}",
                                        symbol
                                    );
                                    let payload = BasicBorrowInterestMsg::create(
                                        now_ms,
                                        symbol,
                                        0.0,
                                        0.0,
                                    )
                                    .to_bytes();
                                    if let Some(wrapped) =
                                        wrap_basic_payload(BasicAccountScope::BitgetUnified, payload)
                                    {
                                        if let Err(e) = evt_tx.send(wrapped) {
                                            warn!(
                                                "failed to send Bitget REST zero borrow cleanup: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                                previous_borrow_symbols = current_borrow_symbols;
                            }
                        }
                        Err(e) => {
                            warn!("Bitget balance poll failed: {:?}", e);
                        }
                    }
                }
            }
        }
        info!("Bitget balance poller exiting");
    })
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
    let Some((event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref()) else {
        return;
    };

    if matches!(event_type, BasicAccountEventType::Error) {
        return;
    }

    match event_type {
        BasicAccountEventType::OrderUpdate => {
            if let Ok(m) = BitgetBasicOrderMsg::from_bytes(&payload) {
                info!(
                    "Bitget OrderUpdate: scope={} venue={} ts={} symbol={} oid={} cloid={} side={} type={} exec={} status={} maker={} px={} qty={} filled={} fill_px={}",
                    account_scope.as_str(),
                    m.venue, m.event_time, m.symbol, m.order_id, m.client_order_id,
                    m.side, m.order_type, m.execution_type, m.order_status,
                    m.is_maker, m.price, m.quantity, m.cumulative_filled_quantity, m.last_executed_price
                );
            }
        }
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                info!(
                    "Bitget BalanceUpdate: scope={} ts={} symbol={} balance={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.symbol,
                    m.balance
                );
            }
        }
        BasicAccountEventType::PositionUpdate => {
            if let Ok(m) = BasicPositionMsg::from_bytes(&payload) {
                info!(
                    "Bitget PositionUpdate: scope={} ts={} inst={} side={} pos={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.position_amount
                );
            }
        }
        BasicAccountEventType::UnrealizedPnlUpdate => {
            if let Ok(m) = BasicUmUnrealizedMsg::from_bytes(&payload) {
                info!(
                    "Bitget UnrealizedPnl: scope={} ts={} inst={} side={} pnl={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.unrealized_pnl
                );
            }
        }
        BasicAccountEventType::BorrowInterest => {
            if let Ok(m) = BasicBorrowInterestMsg::from_bytes(&payload) {
                info!(
                    "Bitget BorrowInterest: scope={} ts={} symbol={} borrowed={} interest={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.symbol,
                    m.borrowed,
                    m.interest
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
        let Some((event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref())
        else {
            return true;
        };
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
            BasicAccountEventType::TradeUpdateLite => return true,
            BasicAccountEventType::Error => return true,
        };

        let Some(key) = key_opt else {
            return true;
        };

        let key = self.hash64(&[account_scope as u32 as u64, key]);

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
