//! Bybit V5 账户监控程序
//!
//! 功能：
//! - 连接 Bybit 私有 WebSocket 频道
//! - 使用 API Key/Secret 进行 auth 鉴权
//! - 订阅 wallet / position / order 频道
//! - 解析账户事件并通过 Iceoryx 转发
//! - 支持主备双路连接

use anyhow::Result;
use bytes::Bytes;
use log::{debug, error, info, warn};
use mkt_signal::common::basic_account_msg::{
    split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg,
    BasicAccountScope, BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg,
    BasicTradeLiteMsg, BasicUmUnrealizedMsg,
};
use mkt_signal::common::bybit_account_msg::BybitBasicOrderMsg;
use mkt_signal::common::mkt_cfg::load_local_ips_preferring_trade_engine;
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::bybit_account_event_parser::BybitAccountEventParser;
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::portfolio_margin::bybit_auth::{
    build_fast_execution_subscribe_message, build_order_subscribe_message,
    build_position_subscribe_message, build_wallet_subscribe_message, BybitCredentials,
    BybitPrivateWsUrls,
};
use mkt_signal::portfolio_margin::bybit_user_stream::BybitUserDataConnection;
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use mkt_signal::trade_engine::bybit_query::bybit_rest_get;
use mkt_signal::trade_engine::query_parsers::bybit_account_balance_snapshot::parse_bybit_account_balance_snapshot;
use mkt_signal::trade_engine::query_parsers::bybit_positions_snapshot::parse_bybit_positions_snapshot;
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

    let credentials = BybitCredentials::from_env()?;
    log_credential_preview("BYBIT_API_KEY", &credentials.api_key);
    log_credential_preview("BYBIT_API_SECRET", &credentials.secret_key);

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    let ws_url = BybitPrivateWsUrls::PRIVATE.to_string();

    let ((primary_ip, secondary_ip), ip_source) = load_local_ips_preferring_trade_engine().await?;
    let session_max = None;
    info!(
        "Primary IP='{}', Secondary IP='{}', session_max={:?} (local_ip_source: {})",
        primary_ip, secondary_ip, session_max, ip_source
    );

    let subscribe_messages = vec![
        build_wallet_subscribe_message(),
        build_position_subscribe_message(),
        build_order_subscribe_message(),
        build_fast_execution_subscribe_message(),
    ];

    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let mut forwarder = PmForwarder::new("bybit")?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));
    let mut balance_poll = spawn_bybit_balance_poll(
        credentials.clone(),
        primary_ip.clone(),
        evt_tx.clone(),
        shutdown_rx.clone(),
    );
    let mut position_poll = spawn_bybit_position_poll(
        credentials.clone(),
        primary_ip.clone(),
        evt_tx.clone(),
        shutdown_rx.clone(),
    );

    let mut primary = spawn_bybit_stream_path(
        "primary",
        &ws_url,
        primary_ip.clone(),
        credentials.clone(),
        subscribe_messages.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut secondary = spawn_bybit_stream_path(
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
                    balance_poll = spawn_bybit_balance_poll(
                        credentials.clone(),
                        primary_ip.clone(),
                        evt_tx.clone(),
                        shutdown_rx.clone(),
                    );
                }
            }
            res = &mut position_poll => {
                match res {
                    Ok(()) => warn!("position poll task exited; restarting"),
                    Err(e) => warn!("position poll task join error: {}; restarting", e),
                }
                if !*shutdown_rx.borrow() {
                    position_poll = spawn_bybit_position_poll(
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
                    primary = spawn_bybit_stream_path(
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
                    secondary = spawn_bybit_stream_path(
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

    info!("Bybit account monitor shutdown complete");
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

fn build_bybit_rest_client(local_ip: Option<&str>, timeout: Duration) -> Result<reqwest::Client> {
    let builder = reqwest::Client::builder().timeout(timeout);
    let builder = match local_ip.map(str::trim).filter(|ip| !ip.is_empty()) {
        Some(ip) if ip != "0.0.0.0" => {
            let parsed: std::net::IpAddr = ip
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid Bybit REST local_ip {}: {}", ip, e))?;
            builder.local_address(parsed)
        }
        _ => builder,
    };
    builder
        .build()
        .map_err(|e| anyhow::anyhow!("build Bybit REST client failed: {}", e))
}

fn wrap_basic_payload(account_scope: BasicAccountScope, payload: Bytes) -> Option<Bytes> {
    let event_type = mkt_signal::common::basic_account_msg::get_basic_event_type(&payload);
    if matches!(event_type, BasicAccountEventType::Error) {
        return None;
    }
    Some(BasicAccountEventMsg::create(event_type, account_scope, payload).to_bytes())
}

fn send_wrapped_payload(
    evt_tx: &tokio::sync::mpsc::UnboundedSender<Bytes>,
    payload: Bytes,
    context: &str,
) {
    if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BybitUnified, payload) {
        if let Err(e) = evt_tx.send(wrapped) {
            warn!("failed to send {}: {}", context, e);
        }
    }
}

fn spawn_bybit_balance_poll(
    credentials: BybitCredentials,
    local_ip: String,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = match build_bybit_rest_client(Some(&local_ip), Duration::from_secs(10)) {
            Ok(c) => c,
            Err(e) => {
                warn!("build Bybit REST client failed: {:?}", e);
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
                    match bybit_rest_get(
                        &client,
                        &credentials,
                        "/v5/account/wallet-balance",
                        "accountType=UNIFIED",
                    ).await {
                        Ok((status, body)) if status == 200 => {
                            if let Some(msgs) = parse_bybit_account_balance_snapshot(&body) {
                                let mut current_borrow_symbols: HashSet<String> = HashSet::new();
                                for payload in msgs {
                                    if let Ok(msg) = BasicBorrowInterestMsg::from_bytes(&payload) {
                                        current_borrow_symbols.insert(msg.symbol.clone());
                                    }
                                    send_wrapped_payload(
                                        &evt_tx,
                                        payload,
                                        "Bybit REST balance msg",
                                    );
                                }
                                let mut stale_symbols: Vec<String> = previous_borrow_symbols
                                    .difference(&current_borrow_symbols)
                                    .cloned()
                                    .collect();
                                stale_symbols.sort();
                                let ts = chrono::Utc::now().timestamp_millis();
                                for symbol in stale_symbols {
                                    info!(
                                        "Bybit REST balance snapshot missing previously-seen borrow; emitting zero cleanup: symbol={}",
                                        symbol
                                    );
                                    send_wrapped_payload(
                                        &evt_tx,
                                        BasicBorrowInterestMsg::create(ts, symbol, 0.0, 0.0)
                                            .to_bytes(),
                                        "Bybit REST zero borrow cleanup",
                                    );
                                }
                                previous_borrow_symbols = current_borrow_symbols;
                            }
                        }
                        Ok((status, body)) => {
                            warn!(
                                "Bybit balance poll returned non-200: status={} body={}",
                                status, body
                            );
                        }
                        Err(e) => {
                            warn!("Bybit balance poll failed: {:?}", e);
                        }
                    }
                }
            }
        }
        info!("Bybit balance poller exiting");
    })
}

fn spawn_bybit_position_poll(
    credentials: BybitCredentials,
    local_ip: String,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = match build_bybit_rest_client(Some(&local_ip), Duration::from_secs(10)) {
            Ok(c) => c,
            Err(e) => {
                warn!("build Bybit REST client failed: {:?}", e);
                return;
            }
        };
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        let mut previous_positions: HashSet<(String, char)> = HashSet::new();
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    match bybit_rest_get(
                        &client,
                        &credentials,
                        "/v5/position/list",
                        "category=linear&settleCoin=USDT",
                    ).await {
                        Ok((status, body)) if status == 200 => {
                            if let Some(msgs) = parse_bybit_positions_snapshot(&body) {
                                let mut current_positions: HashSet<(String, char)> = HashSet::new();
                                for payload in msgs {
                                    let event_type =
                                        mkt_signal::common::basic_account_msg::get_basic_event_type(&payload);
                                    if matches!(event_type, BasicAccountEventType::PositionUpdate) {
                                        if let Ok(msg) = BasicPositionMsg::from_bytes(&payload) {
                                            current_positions.insert((
                                                msg.inst_id().to_string(),
                                                msg.position_side(),
                                            ));
                                        }
                                    }
                                    send_wrapped_payload(
                                        &evt_tx,
                                        payload,
                                        "Bybit REST position msg",
                                    );
                                }

                                let mut stale_positions: Vec<(String, char)> = previous_positions
                                    .difference(&current_positions)
                                    .cloned()
                                    .collect();
                                stale_positions.sort();
                                for (inst_id, side) in stale_positions {
                                    let ts = chrono::Utc::now().timestamp_millis();
                                    info!(
                                        "Bybit REST position snapshot missing previously-seen position; emitting zero cleanup: inst_id={} side={}",
                                        inst_id, side
                                    );
                                    send_wrapped_payload(
                                        &evt_tx,
                                        BasicPositionMsg::create(ts, inst_id.clone(), side, 0.0)
                                            .to_bytes(),
                                        "Bybit REST zero position cleanup",
                                    );
                                    send_wrapped_payload(
                                        &evt_tx,
                                        BasicUmUnrealizedMsg::create(ts, inst_id, side, 0.0)
                                            .to_bytes(),
                                        "Bybit REST zero pnl cleanup",
                                    );
                                }
                                previous_positions = current_positions;
                            }
                        }
                        Ok((status, body)) => {
                            warn!(
                                "Bybit position poll returned non-200: status={} body={}",
                                status, body
                            );
                        }
                        Err(e) => {
                            warn!("Bybit position poll failed: {:?}", e);
                        }
                    }
                }
            }
        }
        info!("Bybit position poller exiting");
    })
}

fn spawn_bybit_stream_path(
    name: &'static str,
    ws_url: &str,
    local_ip: String,
    credentials: BybitCredentials,
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

            let mut runner = BybitUserDataConnection::new(
                format!("[{}][ip={}]", name, local_ip),
                conn,
                credentials.clone(),
                subscribe_messages.clone(),
                session_max,
            );

            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = BybitAccountEventParser::new();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] bybit ws json: {}", name, local_ip_log, s);
                                    } else {
                                        debug!("[{}][ip={}] bybit ws bin: {} bytes", name, local_ip_log, b.len());
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
            if let Ok(m) = BybitBasicOrderMsg::from_bytes(&payload) {
                let label = if m.execution_type == 5 {
                    "Bybit TradeUpdate"
                } else {
                    "Bybit OrderUpdate"
                };
                info!(
                    "{}: scope={} venue={} ts={} symbol={} oid={} oid_str={} cloid={} cloid_str={} side={} type={} exec={} status={} maker={} px={} qty={} filled={} fill_px={}",
                    label,
                    account_scope.as_str(),
                    m.venue, m.event_time, m.symbol, m.order_id, m.order_id_str,
                    m.client_order_id, m.client_order_id_str, m.side, m.order_type, m.execution_type, m.order_status,
                    m.is_maker, m.price, m.quantity, m.cumulative_filled_quantity, m.last_executed_price
                );
            }
        }
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                debug!(
                    "Bybit BalanceUpdate: scope={} ts={} symbol={} wallet={}",
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
                    "Bybit PositionUpdate: scope={} ts={} inst={} side={} pos={}",
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
                    "Bybit UnrealizedPnl: scope={} ts={} inst={} side={} pnl={}",
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
                    "Bybit BorrowInterest: scope={} ts={} symbol={} borrowed={} interest={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.symbol,
                    m.borrowed,
                    m.interest
                );
            }
        }
        BasicAccountEventType::TradeUpdateLite => {
            if let Ok(m) = BasicTradeLiteMsg::from_bytes(&payload) {
                info!(
                    "Bybit TradeUpdateLite: scope={} venue={} ts={} trade_ts={} symbol={} cloid={} trade_id={} side={} maker={} last_px={} last_qty={}",
                    account_scope.as_str(),
                    m.venue,
                    m.event_time,
                    m.trade_time,
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
        BasicAccountEventType::AccountRisk => {
            if let Ok(m) = BasicAccountRiskMsg::from_bytes(&payload) {
                info!(
                    "Bybit AccountRisk: scope={} ts={} adj_eq_usd={:.2} actual_eq_usd={:.2} maint_margin_usd={:.2} initial_margin_usd={:.2} margin_ratio={:.6}",
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
            BasicAccountEventType::OrderUpdate => BybitBasicOrderMsg::from_bytes(&payload)
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
            BasicAccountEventType::AccountRisk => BasicAccountRiskMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_account_risk(&msg)),
            BasicAccountEventType::TradeUpdateLite => BasicTradeLiteMsg::from_bytes(&payload)
                .ok()
                .map(|msg| self.key_trade_lite(&msg)),
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
            msg.wallet.to_bits(),
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

    fn key_account_risk(&self, msg: &BasicAccountRiskMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::AccountRisk as u32 as u64,
            msg.timestamp as u64,
            msg.adj_equity_usd.to_bits(),
            msg.maintenance_margin_usd.to_bits(),
            msg.margin_ratio.to_bits(),
        ])
    }

    fn key_order(&self, msg: &BybitBasicOrderMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.execution_type as u64,
            msg.order_status as u64,
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
