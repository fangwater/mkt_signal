use anyhow::{Context, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use log::{debug, error, info, warn};
use mkt_signal::common::basic_account_msg::{
    get_basic_event_type, split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType,
    BasicAccountScope, BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg,
    BasicUmUnrealizedMsg, BinanceBasicOrderMsg, BinanceTradeLiteMsg,
};
use mkt_signal::common::binance_account_mode::{init_binance_account_mode, BinanceAccountMode};
use mkt_signal::common::mkt_cfg::load_local_ips_preferring_trade_engine;
use mkt_signal::connection::connection::{MktConnection, MktConnectionHandler};
use mkt_signal::parser::binance_basic_account_event_parser::BinanceBasicAccountEventParser;
use mkt_signal::parser::default_parser::Parser;
use mkt_signal::portfolio_margin::binance_spot_ws_api_user_stream::BinanceSpotWsApiUserDataConnection;
use mkt_signal::portfolio_margin::binance_user_stream::{
    BinanceUserDataConnection, SessionRestartPolicy,
};
use mkt_signal::portfolio_margin::listen_key::BinanceListenKeyService;
use mkt_signal::portfolio_margin::pm_forwarder::PmForwarder;
use mkt_signal::pre_trade::order_manager::Side;
use mkt_signal::signal::common::{ExecutionType, OrderStatus};
use mkt_signal::trade_engine::query_parsers::binance_spot_account_snapshot_std::parse_binance_spot_account_snapshot_std;
use mkt_signal::trade_engine::query_parsers::binance_um_account_snapshot::parse_binance_um_account_snapshot;
use mkt_signal::trade_engine::query_parsers::binance_um_balance_snapshot_std::parse_binance_um_balance_snapshot_std;
use reqwest::Client;
use sha2::Sha256;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, watch};
use url::form_urlencoded;

type HmacSha256 = Hmac<Sha256>;

/// 构造最终的用户数据 WS URL。
/// - 新版 private 入口优先使用 `.../private/ws?listenKey=...`
fn build_ws_url(ws_base: &str, listen_key: &str) -> String {
    let base = ws_base.trim_end_matches('/');
    if base.ends_with("/private/ws") {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("listenKey", listen_key);
        format!("{}?{}", base, serializer.finish())
    } else if base.ends_with("/private") {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("listenKey", listen_key);
        format!("{}/ws?{}", base, serializer.finish())
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

async fn signed_get_binance(
    client: &Client,
    base_url: &str,
    path: &str,
    api_key: &str,
    api_secret: &str,
) -> Result<String> {
    let mut params = BTreeMap::new();
    params.insert("recvWindow".to_string(), "5000".to_string());
    params.insert(
        "timestamp".to_string(),
        chrono::Utc::now().timestamp_millis().to_string(),
    );

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (k, v) in &params {
        serializer.append_pair(k, v);
    }
    let query = serializer.finish();

    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid binance api secret"))?;
    mac.update(query.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let url = format!(
        "{}{}?{}&signature={}",
        base_url.trim_end_matches('/'),
        path,
        query,
        signature
    );

    let resp = client
        .get(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!(
            "binance signed GET failed: path={} status={} body={}",
            path,
            status.as_u16(),
            body
        );
    }
    Ok(body)
}

fn build_binance_rest_client(local_ip: Option<&str>, timeout: Duration) -> Result<Client> {
    let builder = Client::builder().timeout(timeout);
    let builder = match local_ip.map(str::trim).filter(|ip| !ip.is_empty()) {
        Some(ip) if ip != "0.0.0.0" => {
            let parsed: IpAddr = ip
                .parse()
                .with_context(|| format!("invalid Binance REST local_ip: {}", ip))?;
            builder.local_address(parsed)
        }
        _ => builder,
    };
    builder.build().context("build Binance REST client failed")
}

fn wrap_basic_payload(account_scope: BasicAccountScope, payload: Bytes) -> Option<Bytes> {
    let event_type = get_basic_event_type(&payload);
    if matches!(event_type, BasicAccountEventType::Error) {
        return None;
    }
    Some(BasicAccountEventMsg::create(event_type, account_scope, payload).to_bytes())
}

async fn bootstrap_standard_snapshots(
    api_key: &str,
    api_secret: &str,
    local_ip: Option<&str>,
    evt_tx: &tokio::sync::mpsc::UnboundedSender<Bytes>,
) -> Result<()> {
    let client = build_binance_rest_client(local_ip, Duration::from_secs(10))?;
    let mut emitted = 0usize;
    info!(
        "bootstrap standard snapshots via local_ip={}",
        local_ip.unwrap_or("system-default")
    );

    let um_balance_body = signed_get_binance(
        &client,
        "https://fapi.binance.com",
        "/fapi/v2/balance",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_um_balance_snapshot_std(&um_balance_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceStdUm, payload) {
                let _ = evt_tx.send(wrapped);
                emitted += 1;
            }
        }
    }

    let um_account_body = signed_get_binance(
        &client,
        "https://fapi.binance.com",
        "/fapi/v2/account",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_um_account_snapshot(&um_account_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceStdUm, payload) {
                let _ = evt_tx.send(wrapped);
                emitted += 1;
            }
        }
    }

    let spot_account_body = signed_get_binance(
        &client,
        "https://api.binance.com",
        "/api/v3/account",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_spot_account_snapshot_std(&spot_account_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceStdSpot, payload) {
                let _ = evt_tx.send(wrapped);
                emitted += 1;
            }
        }
    }

    info!(
        "bootstrap standard snapshots emitted {} basic account event(s)",
        emitted
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();
    let binance_account_mode = init_binance_account_mode("binance_account_monitor");
    info!("BINANCE_ACCOUNT_MODE={}", binance_account_mode.as_str());
    let api_key_raw = std::env::var("BINANCE_API_KEY").map_err(|_| {
        anyhow::anyhow!("BINANCE_API_KEY not set. Export it before running account_monitor")
    })?;
    let api_key = api_key_raw.trim().to_string();
    log_credential_preview("BINANCE_API_KEY", &api_key);

    let api_secret = match std::env::var("BINANCE_API_SECRET") {
        Ok(secret_raw) => {
            let secret = secret_raw.trim().to_string();
            log_credential_preview("BINANCE_API_SECRET", &secret);
            secret
        }
        Err(_) => {
            info!("BINANCE_API_SECRET not set or empty");
            String::new()
        }
    };

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    setup_signals(shutdown_tx.clone());

    // Resolve endpoints from config
    const BINANCE_PM_WS: &str = "wss://fstream.binance.com/pm";
    const BINANCE_PM_REST: &str = "https://papi.binance.com";
    const BINANCE_STD_FAPI_WS: &str = "wss://fstream.binance.com/private";
    const BINANCE_STD_FAPI_REST: &str = "https://fapi.binance.com";
    const BINANCE_STD_SPOT_WS_API: &str = "wss://ws-api.binance.com:443/ws-api/v3";
    const BINANCE_STD_SPOT_REST: &str = "https://api.binance.com";
    let binance_is_standard = binance_account_mode == BinanceAccountMode::Standard;
    let mut stream_cfgs: Vec<UserStreamConfig> = if binance_is_standard {
        vec![
            UserStreamConfig {
                stream_label: "fapi",
                ws_base: BINANCE_STD_FAPI_WS.to_string(),
                rest_base: BINANCE_STD_FAPI_REST.to_string(),
                listen_key_path: "/fapi/v1/listenKey".to_string(),
                parse_balances_from_account_update: true,
                account_scope: BasicAccountScope::BinanceStdUm,
                stream_kind: UserStreamKind::ListenKeyUrl,
                primary_listen_key_rx: None,
                secondary_listen_key_rx: None,
            },
            UserStreamConfig {
                stream_label: "spot-ws-api",
                ws_base: BINANCE_STD_SPOT_WS_API.to_string(),
                rest_base: BINANCE_STD_SPOT_REST.to_string(),
                listen_key_path: "/api/v3/userDataStream".to_string(),
                parse_balances_from_account_update: false,
                account_scope: BasicAccountScope::BinanceStdSpot,
                stream_kind: UserStreamKind::SpotWsApiSignature {
                    api_key: api_key.clone(),
                    api_secret: api_secret.clone(),
                },
                primary_listen_key_rx: None,
                secondary_listen_key_rx: None,
            },
        ]
    } else {
        vec![UserStreamConfig {
            stream_label: "papi",
            ws_base: BINANCE_PM_WS.to_string(),
            rest_base: BINANCE_PM_REST.to_string(),
            listen_key_path: "/papi/v1/listenKey".to_string(),
            parse_balances_from_account_update: false,
            account_scope: BasicAccountScope::BinanceUnified,
            stream_kind: UserStreamKind::ListenKeyUrl,
            primary_listen_key_rx: None,
            secondary_listen_key_rx: None,
        }]
    };

    if binance_is_standard && api_secret.is_empty() {
        return Err(anyhow::anyhow!(
            "BINANCE_API_SECRET not set. STANDARD mode needs spot ws-api signature subscription"
        ));
    }

    for cfg in &stream_cfgs {
        info!(
            "binance account stream [{}]: ws_base={} rest_base={} listen_key_path={} kind={}",
            cfg.stream_label,
            cfg.ws_base,
            cfg.rest_base,
            cfg.listen_key_path,
            cfg.stream_kind.as_str()
        );
    }

    // IP and session settings
    let ((primary_ip, secondary_ip), ip_source) = load_local_ips_preferring_trade_engine().await?;
    info!(
        "Primary IP='{}', Secondary IP='{}', session_restart=primary_odd_2h_boundary_secondary_even_2h_boundary (local_ip_source: {})",
        primary_ip, secondary_ip, ip_source
    );

    // Start listenKey services
    for cfg in stream_cfgs.iter_mut() {
        if matches!(cfg.stream_kind, UserStreamKind::ListenKeyUrl) {
            let primary_listen_key_rx = BinanceListenKeyService::new(
                cfg.rest_base.clone(),
                api_key.clone(),
                cfg.listen_key_path.clone(),
                Some(primary_ip.clone()),
            )?
            .start(shutdown_rx.clone())
            .await?;
            let secondary_listen_key_rx = BinanceListenKeyService::new(
                cfg.rest_base.clone(),
                api_key.clone(),
                cfg.listen_key_path.clone(),
                Some(secondary_ip.clone()),
            )?
            .start(shutdown_rx.clone())
            .await?;
            cfg.primary_listen_key_rx = Some(primary_listen_key_rx);
            cfg.secondary_listen_key_rx = Some(secondary_listen_key_rx);
        }
    }

    // Channel to collect events from both paths and forward via Iceoryx
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    if binance_is_standard {
        match bootstrap_standard_snapshots(&api_key, &api_secret, Some(&primary_ip), &evt_tx).await
        {
            Ok(()) => info!("bootstrap standard snapshots completed"),
            Err(err) => warn!("bootstrap standard snapshots failed: {err:#}"),
        }
    }

    // Create PM forwarder (account_pubs/binance/pm)
    let mut forwarder = PmForwarder::new("binance")?;
    let mut deduper = AccountEventDeduper::new(8192);
    let mut stats = tokio::time::interval(Duration::from_secs(30));

    // Spawn primary and secondary paths for each enabled stream.
    for cfg in stream_cfgs {
        let primary_name = format!("{}-primary", cfg.stream_label);
        spawn_user_stream_path(
            primary_name,
            cfg.ws_base.clone(),
            primary_ip.clone(),
            cfg.primary_listen_key_rx.clone(),
            shutdown_rx.clone(),
            evt_tx.clone(),
            Some(SessionRestartPolicy::OddTwoHourBoundary),
            cfg.parse_balances_from_account_update,
            cfg.account_scope,
            cfg.stream_kind.clone(),
        );

        let secondary_name = format!("{}-secondary", cfg.stream_label);
        spawn_user_stream_path(
            secondary_name,
            cfg.ws_base,
            secondary_ip.clone(),
            cfg.secondary_listen_key_rx,
            shutdown_rx.clone(),
            evt_tx.clone(),
            Some(SessionRestartPolicy::EvenTwoHourBoundary),
            cfg.parse_balances_from_account_update,
            cfg.account_scope,
            cfg.stream_kind,
        );
    }

    // Forwarding loop with periodic stats logging runs in the main task

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
            _ = shutdown_rx.changed() => { break; }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_ws_url;

    #[test]
    fn build_ws_url_uses_private_query_format_for_new_private_base() {
        assert_eq!(
            build_ws_url("wss://fstream.binance.com/private", "abc123"),
            "wss://fstream.binance.com/private/ws?listenKey=abc123"
        );
        assert_eq!(
            build_ws_url("wss://fstream.binance.com/private/ws", "abc123"),
            "wss://fstream.binance.com/private/ws?listenKey=abc123"
        );
    }

    #[test]
    fn build_ws_url_keeps_pm_path_format() {
        assert_eq!(
            build_ws_url("wss://fstream.binance.com/pm", "abc123"),
            "wss://fstream.binance.com/pm/ws/abc123"
        );
    }
}

fn setup_signals(shutdown_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
    });
}

#[derive(Clone)]
enum UserStreamKind {
    ListenKeyUrl,
    SpotWsApiSignature { api_key: String, api_secret: String },
}

impl UserStreamKind {
    fn as_str(&self) -> &'static str {
        match self {
            UserStreamKind::ListenKeyUrl => "listen_key_url",
            UserStreamKind::SpotWsApiSignature { .. } => "spot_ws_api_signature",
        }
    }
}

struct UserStreamConfig {
    stream_label: &'static str,
    ws_base: String,
    rest_base: String,
    listen_key_path: String,
    parse_balances_from_account_update: bool,
    account_scope: BasicAccountScope,
    stream_kind: UserStreamKind,
    primary_listen_key_rx: Option<watch::Receiver<String>>,
    secondary_listen_key_rx: Option<watch::Receiver<String>>,
}

fn spawn_user_stream_path(
    name: String,
    ws_base: String,
    local_ip: String,
    mut listen_key_rx: Option<watch::Receiver<String>>,
    shutdown_rx: watch::Receiver<bool>,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    restart_policy: Option<SessionRestartPolicy>,
    parse_balances_from_account_update: bool,
    account_scope: BasicAccountScope,
    stream_kind: UserStreamKind,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let listen_key = match &stream_kind {
                UserStreamKind::ListenKeyUrl => {
                    let Some(rx) = listen_key_rx.as_mut() else {
                        error!(
                            "[{}] missing listenKey receiver for ListenKeyUrl stream",
                            name
                        );
                        break;
                    };

                    let mut key = rx.borrow().clone();
                    while key.is_empty() {
                        if rx.changed().await.is_err() {
                            return;
                        }
                        key = rx.borrow().clone();
                    }
                    key
                }
                UserStreamKind::SpotWsApiSignature { .. } => String::new(),
            };

            let url = match &stream_kind {
                UserStreamKind::ListenKeyUrl => build_ws_url(&ws_base, &listen_key),
                UserStreamKind::SpotWsApiSignature { .. } => ws_base.clone(),
            };
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
            let mut runner: Box<dyn MktConnectionHandler> = match &stream_kind {
                UserStreamKind::ListenKeyUrl => {
                    Box::new(BinanceUserDataConnection::new(conn, restart_policy))
                }
                UserStreamKind::SpotWsApiSignature {
                    api_key,
                    api_secret,
                } => Box::new(BinanceSpotWsApiUserDataConnection::new(
                    conn,
                    api_key.clone(),
                    api_secret.clone(),
                )),
            };

            // consumer
            let mut consumer_shutdown = shutdown_rx.clone();
            let evt_tx_clone = evt_tx.clone();
            let local_ip_log = local_ip.clone();
            let consumer_name = name.clone();
            let parser = BinanceBasicAccountEventParser::new(
                parse_balances_from_account_update,
                account_scope,
            );
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = raw_rx.recv() => {
                            match msg {
                                Ok(b) => {
                                    if let Ok(s) = std::str::from_utf8(&b) {
                                        debug!("[{}][ip={}] ws json: {}", consumer_name, local_ip_log, s);
                                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(s) {
                                            let event = value.get("event").filter(|v| v.is_object()).unwrap_or(&value);
                                            if event.get("e").and_then(|v| v.as_str()) == Some("outboundAccountPosition") {
                                                info!("[{}][ip={}] outboundAccountPosition raw: {}", consumer_name, local_ip_log, s);
                                            }
                                        }
                                    } else {
                                        debug!("[{}][ip={}] ws bin: {} bytes", consumer_name, local_ip_log, b.len());
                                    }
                                    // 解析并通过通道发送解析后的账户事件（二进制）
                                    let _ = parser.parse(b, &evt_tx_clone);
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(skipped)) => { warn!("[{}] lagged: skipped {} msgs", consumer_name, skipped); }
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

            // for listenKey streams, reconnect quickly on key rotation; otherwise short backoff
            match &stream_kind {
                UserStreamKind::ListenKeyUrl => {
                    let prev = listen_key;
                    if let Some(rx) = listen_key_rx.as_mut() {
                        tokio::select! {
                            _ = rx.changed() => {
                                let new_key = rx.borrow().clone();
                                if new_key != prev { info!("[{}] detected listenKey rotation -> reconnect", name); }
                            }
                            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                        }
                    } else {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
                UserStreamKind::SpotWsApiSignature { .. } => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    })
}

/// 打印解析后的账户事件（basic）
fn log_parsed_event(msg: &Bytes) {
    let Some((event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref()) else {
        return;
    };

    match event_type {
        BasicAccountEventType::OrderUpdate => {
            if let Ok(m) = BinanceBasicOrderMsg::from_bytes(&payload) {
                let venue = match m.venue {
                    BinanceBasicOrderMsg::VENUE_MARGIN => "margin",
                    BinanceBasicOrderMsg::VENUE_UM => "um",
                    _ => "unknown",
                };
                info!(
                    "Binance OrderUpdate: scope={} venue={} sym={} side={:?} x={} X={} cli_id={} ord_id={} price={} qty={} last_qty={} filled={}",
                    account_scope.as_str(),
                    venue,
                    m.symbol,
                    Side::from_u8(m.side).unwrap_or(Side::Buy),
                    ExecutionType::from_u8(m.execution_type)
                        .unwrap_or(ExecutionType::New)
                        .as_str(),
                    OrderStatus::from_u8(m.order_status)
                        .unwrap_or(OrderStatus::New)
                        .as_str(),
                    m.client_order_id,
                    m.order_id,
                    m.price,
                    m.quantity,
                    m.last_executed_quantity,
                    m.cumulative_filled_quantity
                );
            }
        }
        BasicAccountEventType::TradeUpdateLite => {
            if let Ok(m) = BinanceTradeLiteMsg::from_bytes(&payload) {
                info!(
                    "Binance TradeUpdateLite: scope={} venue=um sym={} side={:?} cli_id={} ord_id={} trade_id={} last_px={} last_qty={} maker={}",
                    account_scope.as_str(),
                    m.symbol,
                    Side::from_u8(m.side).unwrap_or(Side::Buy),
                    m.client_order_id,
                    m.order_id,
                    m.trade_id,
                    m.last_executed_price,
                    m.last_executed_quantity,
                    m.is_maker != 0
                );
            }
        }
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                info!(
                    "Binance BalanceUpdate: scope={} ts={} symbol={} balance={}",
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
                    "Binance PositionUpdate: scope={} ts={} inst={} side={} amt={}",
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
                    "Binance BorrowInterest: scope={} ts={} symbol={} borrowed={} interest={}",
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
                    "Binance UnrealizedPnl: scope={} ts={} inst={} side={} pnl={}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.inst_id,
                    m.position_side,
                    m.unrealized_pnl
                );
            }
        }
        BasicAccountEventType::Error => {}
    }
}

/// 统一的账户事件去重器（basic）
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
        let Some((event_type, account_scope, payload)) = split_basic_account_event(msg.as_ref())
        else {
            return true;
        };

        let key_opt = match event_type {
            BasicAccountEventType::BalanceUpdate => BasicBalanceMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_balance(&m)),
            BasicAccountEventType::PositionUpdate => BasicPositionMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_position(&m)),
            BasicAccountEventType::BorrowInterest => BasicBorrowInterestMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_borrow_interest(&m)),
            BasicAccountEventType::UnrealizedPnlUpdate => {
                BasicUmUnrealizedMsg::from_bytes(&payload)
                    .ok()
                    .map(|m| self.key_unrealized_pnl(&m))
            }
            BasicAccountEventType::TradeUpdateLite => BinanceTradeLiteMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_binance_trade_lite(&m)),
            BasicAccountEventType::OrderUpdate => BinanceBasicOrderMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_binance_basic_order(&m)),
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

    fn key_binance_basic_order(&self, msg: &BinanceBasicOrderMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ])
    }

    fn key_binance_trade_lite(&self, msg: &BinanceTradeLiteMsg) -> u64 {
        self.hash64(&[
            BasicAccountEventType::TradeUpdateLite as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.trade_id as u64,
            msg.event_time as u64,
            msg.last_executed_price.to_bits(),
            msg.last_executed_quantity.to_bits(),
        ])
    }
}
