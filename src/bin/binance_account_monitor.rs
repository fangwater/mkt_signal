use account_common::{init_binance_account_mode, BinanceAccountMode};
use account_monitor_common::binance_spot_ws_api_user_stream::BinanceSpotWsApiUserDataConnection;
use account_monitor_common::binance_user_stream::{
    BinanceUserDataConnection, SessionRestartPolicy,
};
use account_monitor_common::listen_key::BinanceListenKeyService;
use account_monitor_common::pm_forwarder::PmForwarder;
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use hmac::{Hmac, Mac};
use log::{debug, error, info, warn};
use mkt_parsers::account_event::binance_basic_account_event_parser::BinanceBasicAccountEventParser;
use mkt_parsers::account_event::{AccountEventSink, Parser as AccountEventParser};
use mkt_parsers::msg::basic_account_msg::{
    get_basic_event_type, split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType,
    BasicAccountRiskMsg, BasicAccountScope, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg, BasicTradeLiteMsg, BasicUmUnrealizedMsg, BinanceBasicOrderMsg,
};
use order_common::Side;
use order_common::{ExecutionType, OrderStatus};
use reqwest::Client;
use runtime_common::affinity::maybe_pin_current_thread;
use runtime_common::mkt_cfg::{
    binance_um_ip_whitelist_mode_enabled,
    load_trade_engine_local_ip_config_preferring_trade_engine,
    validate_binance_um_whitelist_ip_config,
};
use runtime_common::ws_connection::{MktConnection, MktConnectionHandler};
use sha2::Sha256;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use trade_engine::query_parsers::binance_pm_account_risk::parse_binance_pm_account_risk;
use trade_engine::query_parsers::binance_spot_account_snapshot_std::parse_binance_spot_account_snapshot_std;
use trade_engine::query_parsers::binance_um_account_snapshot::parse_binance_um_account_snapshot;
use trade_engine::query_parsers::binance_um_balance_snapshot_std::parse_binance_um_balance_snapshot_std;
use url::form_urlencoded;

type HmacSha256 = Hmac<Sha256>;

#[derive(Parser, Debug)]
#[command(name = "binance_account_monitor")]
#[command(about = "Binance account monitor")]
struct Args {
    /// Bind the main runtime thread to a CPU core. Falls back to ACCOUNT_MONITOR_CORE.
    #[arg(long)]
    core: Option<usize>,
}

struct DirectAccountForwarder {
    forwarder: PmForwarder,
    deduper: AccountEventDeduper,
}

thread_local! {
    static DIRECT_FORWARDER: RefCell<Option<DirectAccountForwarder>> = RefCell::new(None);
}

#[derive(Clone, Copy)]
struct DirectAccountEventSink;

impl AccountEventSink for DirectAccountEventSink {
    fn emit(&self, msg: Bytes) -> bool {
        emit_direct_account_event(msg, None)
    }

    fn emit_with_dedup_key(&self, msg: Bytes, dedup_key: u64) -> bool {
        emit_direct_account_event(msg, Some(dedup_key))
    }
}

fn emit_direct_account_event(msg: Bytes, dedup_key: Option<u64>) -> bool {
    DIRECT_FORWARDER.with(|cell| {
        let mut state = cell.borrow_mut();
        let Some(state) = state.as_mut() else {
            return false;
        };
        let should_forward = match dedup_key {
            Some(key) => state.deduper.should_forward_key(key),
            None => state.deduper.should_forward(&msg),
        };
        if should_forward {
            let sent = state.forwarder.send_raw(&msg);
            log_parsed_event(&msg);
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

fn forward_account_event(msg: Bytes) -> bool {
    DirectAccountEventSink.emit(msg)
}

fn log_forwarder_stats() {
    DIRECT_FORWARDER.with(|cell| {
        if let Some(state) = cell.borrow_mut().as_mut() {
            state.forwarder.log_stats();
        }
    });
}

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

    let query = {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (k, v) in &params {
            serializer.append_pair(k, v);
        }
        serializer.finish()
    };

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
    fapi_rest_base: &str,
    fapi_local_ip: Option<&str>,
    spot_local_ip: Option<&str>,
) -> Result<()> {
    let fapi_client = build_binance_rest_client(fapi_local_ip, Duration::from_secs(10))?;
    let spot_client = build_binance_rest_client(spot_local_ip, Duration::from_secs(10))?;
    let mut emitted = 0usize;
    info!(
        "bootstrap standard snapshots via fapi_rest_base={} fapi_local_ip={} spot_local_ip={}",
        fapi_rest_base,
        fapi_local_ip.unwrap_or("system-default"),
        spot_local_ip.unwrap_or("system-default")
    );

    let um_balance_body = signed_get_binance(
        &fapi_client,
        fapi_rest_base,
        "/fapi/v2/balance",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_um_balance_snapshot_std(&um_balance_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceStdUm, payload) {
                let _ = forward_account_event(wrapped);
                emitted += 1;
            }
        }
    }

    let um_account_body = signed_get_binance(
        &fapi_client,
        fapi_rest_base,
        "/fapi/v2/account",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_um_account_snapshot(&um_account_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceStdUm, payload) {
                let _ = forward_account_event(wrapped);
                emitted += 1;
            }
        }
    }

    let spot_account_body = signed_get_binance(
        &spot_client,
        "https://api.binance.com",
        "/api/v3/account",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_spot_account_snapshot_std(&spot_account_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceStdSpot, payload) {
                let _ = forward_account_event(wrapped);
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

async fn bootstrap_unified_snapshots(
    api_key: &str,
    api_secret: &str,
    local_ip: Option<&str>,
) -> Result<()> {
    let client = build_binance_rest_client(local_ip, Duration::from_secs(10))?;
    let mut emitted = 0usize;
    info!(
        "bootstrap unified snapshots via local_ip={}",
        local_ip.unwrap_or("system-default")
    );

    let um_account_body = signed_get_binance(
        &client,
        "https://papi.binance.com",
        "/papi/v1/um/account",
        api_key,
        api_secret,
    )
    .await?;
    if let Some(msgs) = parse_binance_um_account_snapshot(&um_account_body) {
        for payload in msgs {
            if let Some(wrapped) = wrap_basic_payload(BasicAccountScope::BinanceUnified, payload) {
                let _ = forward_account_event(wrapped);
                emitted += 1;
            }
        }
    }

    info!(
        "bootstrap unified snapshots emitted {} basic account event(s)",
        emitted
    );
    Ok(())
}

fn spawn_pm_risk_poller(
    api_key: String,
    api_secret: String,
    local_ip: Option<String>,
    interval_secs: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let client = match build_binance_rest_client(local_ip.as_deref(), Duration::from_secs(10)) {
            Ok(client) => client,
            Err(err) => {
                error!("pm_risk_poller: build client failed: {err:#}");
                return;
            }
        };

        info!(
            "pm_risk_poller started: interval={}s local_ip={}",
            interval_secs,
            local_ip.as_deref().unwrap_or("system-default")
        );

        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("pm_risk_poller shutting down");
                        break;
                    }
                }
                _ = tick.tick() => {
                    match signed_get_binance(
                        &client,
                        "https://papi.binance.com",
                        "/papi/v1/account",
                        &api_key,
                        &api_secret,
                    )
                    .await
                    {
                        Ok(body) => {
                            if let Some(payload) = parse_binance_pm_account_risk(&body) {
                                let event = BasicAccountEventMsg::create(
                                    BasicAccountEventType::AccountRisk,
                                    BasicAccountScope::BinanceUnified,
                                    payload,
                                );
                                if !forward_account_event(event.to_bytes()) {
                                    warn!("pm_risk_poller: failed to forward account event");
                                }
                            } else {
                                warn!("pm_risk_poller: parse failed body_len={}", body.len());
                            }
                        }
                        Err(err) => warn!("pm_risk_poller: /papi/v1/account failed: {err:#}"),
                    }
                }
            }
        }
    });
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "ACCOUNT_MONITOR_CORE")?;

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
    const BINANCE_STD_FAPI_MM_REST: &str = "https://fapi-mm.binance.com";
    const BINANCE_STD_SPOT_WS_API: &str = "wss://ws-api.binance.com:443/ws-api/v3";
    const BINANCE_STD_SPOT_REST: &str = "https://api.binance.com";
    let binance_is_standard = binance_account_mode == BinanceAccountMode::Standard;
    let binance_um_ip_whitelist_mode = binance_um_ip_whitelist_mode_enabled();
    // Binance's MM REST host accepts listenKey/snapshot requests from the whitelist IP,
    // but the futures user-data WS is served by the normal private stream host.
    let std_fapi_ws = BINANCE_STD_FAPI_WS;
    let std_fapi_rest = if binance_um_ip_whitelist_mode {
        BINANCE_STD_FAPI_MM_REST
    } else {
        BINANCE_STD_FAPI_REST
    };
    let std_fapi_secondary_ws = binance_um_ip_whitelist_mode.then_some(BINANCE_STD_FAPI_WS);
    let std_fapi_secondary_rest = binance_um_ip_whitelist_mode.then_some(BINANCE_STD_FAPI_REST);
    if binance_is_standard && binance_um_ip_whitelist_mode {
        info!(
            "binance UM IP whitelist mode enabled; standard FAPI primary ws_base={} rest_base={} secondary ws_base={} rest_base={}",
            std_fapi_ws,
            std_fapi_rest,
            std_fapi_secondary_ws.unwrap_or(std_fapi_ws),
            std_fapi_secondary_rest.unwrap_or(std_fapi_rest)
        );
    }
    let mut stream_cfgs: Vec<UserStreamConfig> = if binance_is_standard {
        vec![
            UserStreamConfig {
                stream_label: "fapi",
                ws_base: std_fapi_ws.to_string(),
                rest_base: std_fapi_rest.to_string(),
                secondary_ws_base: std_fapi_secondary_ws.map(str::to_string),
                secondary_rest_base: std_fapi_secondary_rest.map(str::to_string),
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
                secondary_ws_base: None,
                secondary_rest_base: None,
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
            secondary_ws_base: None,
            secondary_rest_base: None,
            listen_key_path: "/papi/v1/listenKey".to_string(),
            parse_balances_from_account_update: false,
            account_scope: BasicAccountScope::BinanceUnified,
            stream_kind: UserStreamKind::ListenKeyUrl,
            primary_listen_key_rx: None,
            secondary_listen_key_rx: None,
        }]
    };

    if api_secret.is_empty() {
        return Err(anyhow::anyhow!(
            "BINANCE_API_SECRET not set. binance_account_monitor uses signed account endpoints"
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
    let (local_ip_cfg, ip_source) =
        load_trade_engine_local_ip_config_preferring_trade_engine().await?;
    if local_ip_cfg.local_ips.len() < 2 {
        return Err(anyhow::anyhow!(
            "trade_engine config {} must provide at least 2 local IPs for account monitors",
            ip_source
        ));
    }
    validate_binance_um_whitelist_ip_config(
        &local_ip_cfg.local_ips,
        local_ip_cfg.binance_um_whitelist_ip.as_deref(),
        binance_um_ip_whitelist_mode,
        &ip_source,
        "binance_account_monitor",
    );
    let primary_ip = local_ip_cfg.local_ips[0].clone();
    let secondary_ip = local_ip_cfg.local_ips[1].clone();
    let binance_um_whitelist_ip = if binance_is_standard && binance_um_ip_whitelist_mode {
        Some(
            local_ip_cfg
                .binance_um_whitelist_ip
                .as_deref()
                .expect("validate_binance_um_whitelist_ip_config must require whitelist ip")
                .trim()
                .to_string(),
        )
    } else {
        None
    };
    info!(
        "Primary IP='{}', Secondary IP='{}', session_restart=primary_odd_2h_boundary_secondary_even_2h_boundary (local_ip_source: {})",
        primary_ip, secondary_ip, ip_source
    );
    if let Some(ip) = binance_um_whitelist_ip.as_deref() {
        info!(
            "binance UM IP whitelist mode enabled; standard FAPI primary listenKey/user-stream pinned to whitelist local_ip={}, secondary keeps normal local_ip={}",
            ip, secondary_ip
        );
    }
    let (non_um_primary_ip, non_um_secondary_ip) = if let Some(whitelist_ip) =
        binance_um_whitelist_ip.as_deref()
    {
        info!(
            "binance UM IP whitelist mode enabled; standard non-UM account streams keep primary={} secondary={} while FAPI is pinned to {}",
            primary_ip, secondary_ip, whitelist_ip
        );
        (primary_ip.clone(), secondary_ip.clone())
    } else {
        (primary_ip.clone(), secondary_ip.clone())
    };

    // Start listenKey services
    for cfg in stream_cfgs.iter_mut() {
        let (cfg_primary_ip, cfg_secondary_ip) = stream_local_ips(
            cfg.stream_label,
            &non_um_primary_ip,
            &non_um_secondary_ip,
            binance_um_whitelist_ip.as_deref(),
        );
        if matches!(cfg.stream_kind, UserStreamKind::ListenKeyUrl) {
            let secondary_rest_base = cfg
                .secondary_rest_base
                .clone()
                .unwrap_or_else(|| cfg.rest_base.clone());
            let primary_listen_key_rx = BinanceListenKeyService::new(
                cfg.rest_base.clone(),
                api_key.clone(),
                cfg.listen_key_path.clone(),
                Some(cfg_primary_ip.clone()),
            )?
            .start(shutdown_rx.clone())
            .await?;
            let secondary_listen_key_rx = BinanceListenKeyService::new(
                secondary_rest_base,
                api_key.clone(),
                cfg.listen_key_path.clone(),
                Some(cfg_secondary_ip.clone()),
            )?
            .start(shutdown_rx.clone())
            .await?;
            cfg.primary_listen_key_rx = Some(primary_listen_key_rx);
            cfg.secondary_listen_key_rx = Some(secondary_listen_key_rx);
        }
    }

    init_direct_forwarder("binance")?;

    if binance_is_standard {
        let fapi_snapshot_ip = binance_um_whitelist_ip.as_deref().unwrap_or(&primary_ip);
        match bootstrap_standard_snapshots(
            &api_key,
            &api_secret,
            std_fapi_rest,
            Some(fapi_snapshot_ip),
            Some(&non_um_primary_ip),
        )
        .await
        {
            Ok(()) => info!("bootstrap standard snapshots completed"),
            Err(err) => warn!("bootstrap standard snapshots failed: {err:#}"),
        }
    } else {
        match bootstrap_unified_snapshots(&api_key, &api_secret, Some(&primary_ip)).await {
            Ok(()) => info!("bootstrap unified snapshots completed"),
            Err(err) => warn!("bootstrap unified snapshots failed: {err:#}"),
        }
        let interval_secs = std::env::var("BINANCE_PM_RISK_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5);
        spawn_pm_risk_poller(
            api_key.clone(),
            api_secret.clone(),
            Some(primary_ip.clone()),
            interval_secs,
            shutdown_rx.clone(),
        );
    }

    let mut stats = tokio::time::interval(Duration::from_secs(30));

    // Spawn primary and secondary paths for each enabled stream.
    for cfg in stream_cfgs {
        let (cfg_primary_ip, cfg_secondary_ip) = stream_local_ips(
            cfg.stream_label,
            &non_um_primary_ip,
            &non_um_secondary_ip,
            binance_um_whitelist_ip.as_deref(),
        );
        let primary_name = format!("{}-primary", cfg.stream_label);
        let secondary_ws_base = cfg
            .secondary_ws_base
            .clone()
            .unwrap_or_else(|| cfg.ws_base.clone());
        spawn_user_stream_path(
            primary_name,
            cfg.ws_base.clone(),
            cfg_primary_ip,
            cfg.primary_listen_key_rx.clone(),
            shutdown_rx.clone(),
            Some(SessionRestartPolicy::OddTwoHourBoundary),
            cfg.parse_balances_from_account_update,
            cfg.account_scope,
            cfg.stream_kind.clone(),
        );

        let secondary_name = format!("{}-secondary", cfg.stream_label);
        spawn_user_stream_path(
            secondary_name,
            secondary_ws_base,
            cfg_secondary_ip,
            cfg.secondary_listen_key_rx,
            shutdown_rx.clone(),
            Some(SessionRestartPolicy::EvenTwoHourBoundary),
            cfg.parse_balances_from_account_update,
            cfg.account_scope,
            cfg.stream_kind,
        );
    }

    // Forwarding loop with periodic stats logging runs in the main task

    loop {
        tokio::select! {
            _ = stats.tick() => {
                log_forwarder_stats();
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
    secondary_ws_base: Option<String>,
    secondary_rest_base: Option<String>,
    listen_key_path: String,
    parse_balances_from_account_update: bool,
    account_scope: BasicAccountScope,
    stream_kind: UserStreamKind,
    primary_listen_key_rx: Option<watch::Receiver<String>>,
    secondary_listen_key_rx: Option<watch::Receiver<String>>,
}

fn stream_local_ips(
    stream_label: &str,
    non_um_primary_ip: &str,
    non_um_secondary_ip: &str,
    binance_um_whitelist_ip: Option<&str>,
) -> (String, String) {
    if stream_label == "fapi" {
        if let Some(ip) = binance_um_whitelist_ip {
            let secondary_ip = if non_um_primary_ip != ip {
                non_um_primary_ip
            } else {
                non_um_secondary_ip
            };
            return (ip.to_string(), secondary_ip.to_string());
        }
    }
    (
        non_um_primary_ip.to_string(),
        non_um_secondary_ip.to_string(),
    )
}

fn spawn_user_stream_path(
    name: String,
    ws_base: String,
    local_ip: String,
    mut listen_key_rx: Option<watch::Receiver<String>>,
    shutdown_rx: watch::Receiver<bool>,
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
            let (raw_tx, _) = tokio::sync::broadcast::channel::<Bytes>(1);
            let mut conn = MktConnection::new(
                url,
                serde_json::json!({}),
                raw_tx.clone(),
                shutdown_rx.clone(),
            );
            if !local_ip.is_empty() {
                conn.local_ip = Some(local_ip.clone());
            }
            let parser = BinanceBasicAccountEventParser::new(
                parse_balances_from_account_update,
                account_scope,
            );
            let handler_name = name.clone();
            let handler_local_ip = local_ip.clone();
            let raw_handler = Box::new(move |b: Bytes| {
                if let Ok(s) = std::str::from_utf8(&b) {
                    debug!("[{}][ip={}] ws json: {}", handler_name, handler_local_ip, s);
                } else {
                    debug!(
                        "[{}][ip={}] ws bin: {} bytes",
                        handler_name,
                        handler_local_ip,
                        b.len()
                    );
                }
                let _ = parser.parse(b, &DirectAccountEventSink);
            });

            let mut runner: Box<dyn MktConnectionHandler> = match &stream_kind {
                UserStreamKind::ListenKeyUrl => {
                    let mut runner = BinanceUserDataConnection::new(conn, restart_policy)
                        .with_connection_label(name.clone());
                    runner.set_raw_handler(raw_handler);
                    Box::new(runner)
                }
                UserStreamKind::SpotWsApiSignature {
                    api_key,
                    api_secret,
                } => {
                    let mut runner = BinanceSpotWsApiUserDataConnection::new(
                        conn,
                        api_key.clone(),
                        api_secret.clone(),
                    );
                    runner.set_raw_handler(raw_handler);
                    Box::new(runner)
                }
            };

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
            if let Ok(m) = BasicTradeLiteMsg::from_bytes(&payload) {
                info!(
                    "Binance TradeUpdateLite: scope={} venue=um sym={} side={:?} cli_id={} trade_id={} last_px={} last_qty={} maker={}",
                    account_scope.as_str(),
                    m.symbol,
                    Side::from_u8(m.side).unwrap_or(Side::Buy),
                    m.client_order_id,
                    m.trade_id_str(),
                    m.last_executed_price,
                    m.last_executed_quantity,
                    m.is_maker != 0
                );
            }
        }
        BasicAccountEventType::BalanceUpdate => {
            if let Ok(m) = BasicBalanceMsg::from_bytes(&payload) {
                info!(
                    "Binance BalanceUpdate: scope={} ts={} symbol={} wallet={}",
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
        BasicAccountEventType::AccountRisk => {
            if let Ok(m) = BasicAccountRiskMsg::from_bytes(&payload) {
                let calc_margin_ratio = if m.maintenance_margin_usd.abs() > f64::EPSILON {
                    m.adj_equity_usd / m.maintenance_margin_usd
                } else {
                    0.0
                };
                let diff = m.margin_ratio - calc_margin_ratio;
                info!(
                    "Binance AccountRisk: scope={} ts={} adj_eq_usd={:.2} actual_eq_usd={:.2} maint_margin_usd={:.2} initial_margin_usd={:.2} margin_ratio={:.6} calc_margin_ratio={:.6} diff={:.6}",
                    account_scope.as_str(),
                    m.timestamp,
                    m.adj_equity_usd,
                    m.actual_equity_usd,
                    m.maintenance_margin_usd,
                    m.initial_margin_usd,
                    m.margin_ratio,
                    calc_margin_ratio,
                    diff
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

    fn should_forward_key(&mut self, key: u64) -> bool {
        self.remember_key(key)
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
            BasicAccountEventType::TradeUpdateLite => BasicTradeLiteMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_trade_lite(&m)),
            BasicAccountEventType::AccountRisk => BasicAccountRiskMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_account_risk(&m)),
            BasicAccountEventType::OrderUpdate => BinanceBasicOrderMsg::from_bytes(&payload)
                .ok()
                .map(|m| self.key_binance_basic_order(&m)),
            BasicAccountEventType::Error => return true,
        };

        let Some(key) = key_opt else {
            return true;
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
