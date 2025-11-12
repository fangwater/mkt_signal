use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::{debug, error, info, warn};
use mkt_signal::account::execution_record::{
    ExecutionRecordMessage, MARGIN_EXECUTION_RECORD_CHANNEL,
};
use mkt_signal::account::order_update_record::{
    OrderUpdateRecordMessage, UM_ORDER_UPDATE_RECORD_CHANNEL,
};
use mkt_signal::common::account_msg::{
    get_event_type, AccountEventType, ExecutionReportMsg, OrderTradeUpdateMsg,
};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::binance_account_event_parser::BinanceAccountEventParser;
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::portfolio_margin::binance_user_stream::BinanceUserDataConnection;
use mkt_signal::portfolio_margin::listen_key::BinanceListenKeyService;
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, watch};

/// 构造最终的用户数据 WS URL。
/// 如果配置的 `ws_base` 已经以 `/ws` 结尾（例如 `.../pm/ws`），则直接追加 listenKey；
/// 否则默认追加 `/ws/{listenKey}`。
fn build_ws_url(ws_base: &str, listen_key: &str) -> String {
    let base = ws_base.trim_end_matches('/');
    if base.ends_with("/ws") {
        format!("{}/{}", base, listen_key)
    } else {
        format!("{}/ws/{}", base, listen_key)
    }
}

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
    // Load TOML config (fixed path)
    let cfg = mkt_signal::portfolio_margin::pm_cfg::AccountTomlCfg::load("config/account_cfg.toml")
        .await?;

    let api_key_raw = std::env::var("BINANCE_API_KEY").map_err(|_| {
        anyhow::anyhow!("BINANCE_API_KEY not set. Export it before running account_monitor")
    })?;
    let api_key = api_key_raw.trim().to_string();
    log_credential_preview("BINANCE_API_KEY", &api_key);

    match std::env::var("BINANCE_API_SECRET") {
        Ok(secret_raw) => {
            let secret = secret_raw.trim().to_string();
            log_credential_preview("BINANCE_API_SECRET", &secret);
        }
        Err(_) => info!("BINANCE_API_SECRET not set or empty"),
    }

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // Resolve endpoints from config
    let ws_pm = cfg
        .exchanges
        .binance
        .as_ref()
        .and_then(|e| e.ws.as_ref())
        .and_then(|w| w.pm.clone())
        .unwrap_or_else(|| "wss://fstream.binance.com/pm".to_string());
    let rest_pm = cfg
        .exchanges
        .binance
        .as_ref()
        .and_then(|e| e.rest.as_ref())
        .and_then(|r| r.pm.clone())
        .unwrap_or_else(|| "https://papi.binance.com".to_string());
    info!("Config loaded. ws_pm={}, rest_pm={}", ws_pm, rest_pm);

    // IP and session settings
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

    // Start listenKey service
    let listen_key_rx = BinanceListenKeyService::new(rest_pm.clone(), api_key)
        .start(shutdown_rx.clone())
        .await?;

    // Channel to collect events from both paths and forward via Iceoryx
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // Create PM forwarder (account_pubs/binance/pm)
    let (pm_hist, pm_subs) = cfg
        .iceoryx
        .as_ref()
        .and_then(|i| i.pm.as_ref())
        .map(|c| (c.history_size, c.max_subscribers))
        .unwrap_or((None, None));
    let mut forwarder = PmForwarder::new("binance", pm_hist, pm_subs)?;
    let execution_publisher = BinanceMarginUpdatePublisher::new(MARGIN_EXECUTION_RECORD_CHANNEL)?;
    let um_order_publisher = BinanceUmUpdatePublisher::new(UM_ORDER_UPDATE_RECORD_CHANNEL)?;
    let mut execution_deduper = ExecutionDeduper::new(8192);
    let mut um_order_deduper = OrderUpdateDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));

    // Spawn primary and secondary paths
    let mut primary = spawn_user_stream_path(
        "primary",
        &ws_pm,
        primary_ip.clone(),
        listen_key_rx.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );
    let mut secondary = spawn_user_stream_path(
        "secondary",
        &ws_pm,
        secondary_ip.clone(),
        listen_key_rx.clone(),
        shutdown_rx.clone(),
        evt_tx.clone(),
        session_max,
    );

    // Forwarding loop with periodic stats logging runs in the main task

    loop {
        tokio::select! {
            Some(msg) = evt_rx.recv() => {
                forwarder.send_raw(&msg);
                if let Err(err) = maybe_publish_execution_record(
                    &msg,
                    &execution_publisher,
                    &mut execution_deduper,
                ) {
                    warn!("failed to publish execution record: {err:#}");
                }
                if let Err(err) = maybe_publish_um_order_update(
                    &msg,
                    &um_order_publisher,
                    &mut um_order_deduper,
                ) {
                    warn!("failed to publish UM order update: {err:#}");
                }
            }
            _ = stats.tick() => {
                forwarder.log_stats();
            }
            _ = &mut primary => { warn!("primary user-data task exited; continuing"); }
            _ = &mut secondary => { warn!("secondary user-data task exited; continuing"); }
            _ = shutdown_rx.changed() => { break; }
        }
    }

    Ok(())
}

fn setup_signals(shutdown_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
    });
}

fn spawn_user_stream_path(
    name: &'static str,
    ws_base: &str,
    local_ip: String,
    mut listen_key_rx: watch::Receiver<String>,
    shutdown_rx: watch::Receiver<bool>,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    session_max: Option<Duration>,
) -> tokio::task::JoinHandle<()> {
    let ws_base = ws_base.to_string();
    tokio::spawn(async move {
        loop {
            // wait for non-empty listenKey
            let mut listen_key = listen_key_rx.borrow().clone();
            while listen_key.is_empty() {
                if listen_key_rx.changed().await.is_ok() {
                    listen_key = listen_key_rx.borrow().clone();
                }
            }

            let url = build_ws_url(&ws_base, &listen_key);
            info!("[{}] connecting to {} (local_ip='{}')", name, url, local_ip);
            let (raw_tx, mut raw_rx) = broadcast::channel::<Bytes>(8192);
            let mut conn = MktConnection::new(
                url,
                serde_json::json!({}),
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }
            let mut runner = BinanceUserDataConnection::new(conn, session_max);

            // consumer
            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let parser = BinanceAccountEventParser::new();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] ws json: {}", name, local_ip_log, s);
                                    } else {
                                        debug!("[{}][ip={}] ws bin: {} bytes", name, local_ip_log, b.len());
                                    }
                                    // 解析并通过通道发送解析后的账户事件（二进制）
                                    let _ = parser.parse(b, &evt_tx_clone);
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(skipped)) => { warn!("[{}] lagged: skipped {} msgs", name, skipped); }
                            }
                        }
                        _ = consumer_shutdown.changed() => {
                            if *consumer_shutdown.borrow() { break; }
                        }
                    }
                }
            });

            // run connection until it exits (closed or error)
            if let Err(e) = runner.start_ws().await {
                error!("[{}] connection error: {}", name, e);
            }

            // if listenKey changed, reconnect immediately; otherwise wait 2s and retry
            let prev = listen_key;
            tokio::select! {
                _ = listen_key_rx.changed() => {
                    let new_key = listen_key_rx.borrow().clone();
                    if new_key != prev { info!("[{}] detected listenKey rotation -> reconnect", name); }
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    })
}

fn maybe_publish_execution_record(
    msg: &Bytes,
    publisher: &BinanceMarginUpdatePublisher,
    deduper: &mut ExecutionDeduper,
) -> anyhow::Result<()> {
    if msg.len() < 8 {
        return Ok(());
    }
    if get_event_type(msg.as_ref()) != AccountEventType::ExecutionReport {
        return Ok(());
    }
    let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
    if msg.len() < 8 + payload_len {
        return Err(anyhow!(
            "execution report payload truncated: expected {} got {}",
            payload_len,
            msg.len().saturating_sub(8)
        ));
    }
    let payload = msg.slice(8..8 + payload_len);
    let report = ExecutionReportMsg::from_bytes(&payload)?;
    if !deduper.should_publish(&report) {
        return Ok(());
    }

    let recv_ts = get_timestamp_us();
    let strategy_id = (report.client_order_id >> 32) as i32;
    let client_order_id_str = if report.client_order_id_str.is_empty() {
        None
    } else {
        Some(report.client_order_id_str.clone())
    };

    let record = ExecutionRecordMessage::new(
        recv_ts,
        report.event_time,
        report.transaction_time,
        report.order_id,
        report.trade_id,
        report.order_creation_time,
        report.working_time,
        report.update_id,
        report.symbol.clone(),
        report.client_order_id,
        client_order_id_str,
        strategy_id,
        report.side,
        report.is_maker,
        report.is_working,
        report.price,
        report.quantity,
        report.last_executed_quantity,
        report.cumulative_filled_quantity,
        report.last_executed_price,
        report.commission_amount,
        report.commission_asset.clone(),
        report.cumulative_quote,
        report.last_quote,
        report.quote_order_quantity,
        report.order_type.clone(),
        report.time_in_force.clone(),
        report.execution_type.clone(),
        report.order_status.clone(),
    );
    let bytes = record.to_bytes();
    publisher.publish(&bytes)?;
    Ok(())
}

fn maybe_publish_um_order_update(
    msg: &Bytes,
    publisher: &BinanceUmUpdatePublisher,
    deduper: &mut OrderUpdateDeduper,
) -> anyhow::Result<()> {
    if msg.len() < 8 {
        return Ok(());
    }
    if get_event_type(msg.as_ref()) != AccountEventType::OrderTradeUpdate {
        return Ok(());
    }
    let payload_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;
    if msg.len() < 8 + payload_len {
        return Err(anyhow!(
            "order trade update payload truncated: expected {} got {}",
            payload_len,
            msg.len().saturating_sub(8)
        ));
    }
    let payload = msg.slice(8..8 + payload_len);
    let update = OrderTradeUpdateMsg::from_bytes(&payload)?;
    if !update.business_unit.eq_ignore_ascii_case("UM") {
        return Ok(());
    }
    if !deduper.should_publish(&update) {
        return Ok(());
    }

    let recv_ts = get_timestamp_us();
    let client_order_id_str = if update.client_order_id_str.is_empty() {
        None
    } else {
        Some(update.client_order_id_str.clone())
    };
    let derived_strategy_id = ((update.client_order_id >> 32) as i32) as i64;
    let account_recv_ts_us = recv_ts;

    let record = OrderUpdateRecordMessage::new(
        recv_ts,
        account_recv_ts_us,
        update.event_time,
        update.transaction_time,
        update.order_id,
        update.trade_id,
        update.strategy_id,
        derived_strategy_id,
        update.symbol.clone(),
        update.client_order_id,
        client_order_id_str,
        update.side,
        update.position_side,
        update.is_maker,
        update.reduce_only,
        update.price,
        update.quantity,
        update.average_price,
        update.stop_price,
        update.last_executed_quantity,
        update.cumulative_filled_quantity,
        update.last_executed_price,
        update.commission_amount,
        update.buy_notional,
        update.sell_notional,
        update.realized_profit,
        update.order_type.clone(),
        update.time_in_force.clone(),
        update.execution_type.clone(),
        update.order_status.clone(),
        update.commission_asset.clone(),
        update.strategy_type.clone(),
        update.business_unit.clone(),
    );

    let bytes = record.to_bytes();
    publisher.publish(&bytes)?;
    Ok(())
}

struct ExecutionDeduper {
    seen: HashSet<u64>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl ExecutionDeduper {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn should_publish(&mut self, report: &ExecutionReportMsg) -> bool {
        let key = execution_dedup_key(report);
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
}

fn execution_dedup_key(report: &ExecutionReportMsg) -> u64 {
    let mut hasher = DefaultHasher::new();
    report.order_id.hash(&mut hasher);
    report.trade_id.hash(&mut hasher);
    report.update_id.hash(&mut hasher);
    report.execution_type.hash(&mut hasher);
    hasher.finish()
}

struct OrderUpdateDeduper {
    seen: HashSet<u64>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl OrderUpdateDeduper {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn should_publish(&mut self, update: &OrderTradeUpdateMsg) -> bool {
        let key = order_update_dedup_key(update);
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
}

fn order_update_dedup_key(update: &OrderTradeUpdateMsg) -> u64 {
    let mut hasher = DefaultHasher::new();
    update.order_id.hash(&mut hasher);
    update.trade_id.hash(&mut hasher);
    update.execution_type.hash(&mut hasher);
    update.event_time.hash(&mut hasher);
    hasher.finish()
}
