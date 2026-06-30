use account_common::ApiKey;
use account_common::{init_binance_account_mode, BinanceAccountMode};
use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use hmac::{Hmac, Mac};
use log::{error, info};
use runtime_common::affinity::{maybe_pin_current_thread, resolve_core};
use runtime_common::exchange::Exchange;
use runtime_common::mkt_cfg::{
    binance_um_ip_whitelist_mode_enabled, find_trade_engine_local_cfg_path, home_mkt_cfg_path,
    load_local_ips_from_path, load_trade_engine_local_ip_config_from_toml_path,
    validate_binance_um_whitelist_ip_config, BinanceUmWsHealthConfig,
    BINANCE_UM_IP_WHITELIST_MODE_ENV,
};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::net::IpAddr;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use trade_engine::binance_ws::BINANCE_ED25519_PRIVATE_KEY_PATH_ENV;
use trade_engine::config::RestConstants;
use trade_engine::exec_backend::ExecBackend;
use trade_engine::TradeEngine;

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

#[derive(Parser, Debug)]
#[command(name = "trade_engine", about = "Unified trade execution engine")]
struct Args {
    /// Target exchange (binance, okex, bybit, bitget, gate)
    #[arg(long, value_enum)]
    exchange: TradeEngineTarget,

    /// 绑定主线程到指定 CPU 核（可选）；未提供则尝试 TRADE_ENGINE_CORE 环境变量
    #[arg(long)]
    core: Option<usize>,

    /// 绑定 te-ipc 线程（iceoryx 桥接 busy-poll）到指定 CPU 核（可选）；
    /// 未提供则尝试 TRADE_ENGINE_IPC_CORE 环境变量
    #[arg(long)]
    ipc_core: Option<usize>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TradeEngineTarget {
    Binance,
    Okex,
    Bybit,
    Bitget,
    Gate,
}

impl TradeEngineTarget {
    fn as_str(&self) -> &'static str {
        match self {
            TradeEngineTarget::Binance => "binance",
            TradeEngineTarget::Okex => "okex",
            TradeEngineTarget::Bybit => "bybit",
            TradeEngineTarget::Bitget => "bitget",
            TradeEngineTarget::Gate => "gate",
        }
    }
}

fn setup_signal_handlers(token: &CancellationToken) {
    let ctrl_c = token.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            error!("trade_engine: listen ctrl_c failed: {}", e);
            return;
        }
        info!("trade_engine: received Ctrl+C");
        ctrl_c.cancel();
    });

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let term = token.clone();
        tokio::spawn(async move {
            match signal(SignalKind::terminate()) {
                Ok(mut sig) => {
                    if sig.recv().await.is_some() {
                        info!("trade_engine: received SIGTERM");
                        term.cancel();
                    }
                }
                Err(e) => error!("trade_engine: listen SIGTERM failed: {}", e),
            }
        });
    }
}

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug)]
struct TradeEngineLocalIpRuntimeConfig {
    local_ips: Vec<IpAddr>,
    local_ip_values: Vec<String>,
    binance_um_whitelist_ip_value: Option<String>,
    binance_um_ws_direct_ip_values: Vec<String>,
    binance_um_ws_health: BinanceUmWsHealthConfig,
    binance_um_ws_route: runtime_common::mkt_cfg::BinanceUmWsRouteConfig,
    source: String,
}

async fn load_trade_engine_local_ip_config() -> Result<TradeEngineLocalIpRuntimeConfig> {
    if let Some(path) = find_trade_engine_local_cfg_path()? {
        let cfg = load_trade_engine_local_ip_config_from_toml_path(&path).await?;
        let local_ip_values = cfg.local_ips;
        let local_ips = local_ip_values
            .iter()
            .enumerate()
            .map(|(idx, ip)| {
                ip.parse().with_context(|| {
                    format!(
                        "invalid local_ips[{}] in trade_engine config {}: {}",
                        idx,
                        path.display(),
                        ip
                    )
                })
            })
            .collect::<Result<Vec<IpAddr>>>()?;
        let binance_um_whitelist_ip_value = cfg.binance_um_whitelist_ip;
        let _binance_um_whitelist_ip: Option<IpAddr> = binance_um_whitelist_ip_value
            .as_ref()
            .map(|ip| {
                ip.parse().with_context(|| {
                    format!(
                        "invalid binance_um_whitelist_ip in trade_engine config {}: {}",
                        path.display(),
                        ip
                    )
                })
            })
            .transpose()?;
        let _binance_um_ws_direct_ips = cfg
            .binance_um_ws_direct_ips
            .iter()
            .enumerate()
            .map(|(idx, ip)| {
                ip.parse::<IpAddr>().with_context(|| {
                    format!(
                        "invalid binance_um_ws_direct_ips[{}] in trade_engine config {}: {}",
                        idx,
                        path.display(),
                        ip
                    )
                })
            })
            .collect::<Result<Vec<IpAddr>>>()?;
        return Ok(TradeEngineLocalIpRuntimeConfig {
            local_ips,
            local_ip_values,
            binance_um_whitelist_ip_value,
            binance_um_ws_direct_ip_values: cfg.binance_um_ws_direct_ips,
            binance_um_ws_health: cfg.binance_um_ws_health,
            binance_um_ws_route: cfg.binance_um_ws_route,
            source: path.display().to_string(),
        });
    }

    let cfg_path = home_mkt_cfg_path()?;
    let (primary_ip_raw, secondary_ip_raw) = load_local_ips_from_path(&cfg_path).await?;
    let primary_ip: IpAddr = primary_ip_raw
        .parse()
        .with_context(|| format!("invalid primary_local_ip: {}", primary_ip_raw))?;
    let secondary_ip: IpAddr = secondary_ip_raw
        .parse()
        .with_context(|| format!("invalid secondary_local_ip: {}", secondary_ip_raw))?;
    Ok(TradeEngineLocalIpRuntimeConfig {
        local_ips: vec![primary_ip, secondary_ip],
        local_ip_values: vec![primary_ip_raw, secondary_ip_raw],
        binance_um_whitelist_ip_value: None,
        binance_um_ws_direct_ip_values: Vec::new(),
        binance_um_ws_health: BinanceUmWsHealthConfig::default(),
        binance_um_ws_route: runtime_common::mkt_cfg::BinanceUmWsRouteConfig::default(),
        source: format!("{} (fallback mkt_cfg.yaml)", cfg_path.display()),
    })
}

fn sign_binance_query(params: &BTreeMap<String, String>, api_secret: &str) -> Result<String> {
    let mut ser = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in params.iter() {
        ser.append_pair(k, v);
    }
    let query = ser.finish();

    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid binance secret"))?;
    mac.update(query.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    Ok(format!("{}&signature={}", query, sig))
}

fn build_binance_fee_burn_client(local_ip: Option<IpAddr>) -> Result<reqwest::Client> {
    let builder = reqwest::Client::builder();
    let builder = if let Some(ip) = local_ip {
        builder.local_address(ip)
    } else {
        builder
    };
    builder
        .build()
        .context("build feeBurn reqwest client failed")
}

async fn fetch_binance_fee_burn(
    api_key: &str,
    api_secret: &str,
    base_url: &str,
    local_ip: Option<IpAddr>,
) -> Result<bool> {
    let mut params: BTreeMap<String, String> = BTreeMap::new();
    params.insert(
        "timestamp".to_string(),
        chrono::Utc::now().timestamp_millis().to_string(),
    );
    params.insert(
        "recvWindow".to_string(),
        RestConstants::RECV_WINDOW_MS.to_string(),
    );
    let query = sign_binance_query(&params, api_secret)?;
    let full_url = format!("{}/fapi/v1/feeBurn?{}", base_url, query);
    let client = build_binance_fee_burn_client(local_ip)?;
    info!(
        "feeBurn check request local_ip={} url={}",
        local_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "system-default".to_string()),
        full_url
    );
    let resp = client
        .get(&full_url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "feeBurn check http status={} body={}",
            status.as_u16(),
            body
        ));
    }
    info!("feeBurn check response: {}", body);
    let v: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("feeBurn parse json error: {e} body={body}"))?;
    let fee_burn = match v.get("feeBurn") {
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::String(s)) => {
            matches!(s.trim(), "true" | "TRUE" | "True")
        }
        _ => {
            return Err(anyhow::anyhow!(
                "feeBurn field missing or invalid: {}",
                body
            ))
        }
    };
    Ok(fee_burn)
}

async fn enable_binance_fee_burn(
    api_key: &str,
    api_secret: &str,
    base_url: &str,
    local_ip: Option<IpAddr>,
) -> Result<()> {
    let mut params: BTreeMap<String, String> = BTreeMap::new();
    params.insert("feeBurn".to_string(), "true".to_string());
    params.insert(
        "timestamp".to_string(),
        chrono::Utc::now().timestamp_millis().to_string(),
    );
    params.insert(
        "recvWindow".to_string(),
        RestConstants::RECV_WINDOW_MS.to_string(),
    );
    let query = sign_binance_query(&params, api_secret)?;
    let full_url = format!("{}/fapi/v1/feeBurn?{}", base_url, query);
    let client = build_binance_fee_burn_client(local_ip)?;
    info!(
        "feeBurn enable request local_ip={} url={}",
        local_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "system-default".to_string()),
        full_url
    );
    let resp = client
        .post(&full_url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "feeBurn enable http status={} body={}",
            status.as_u16(),
            body
        ));
    }
    info!("feeBurn enable response: {}", body);
    let v: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("feeBurn enable parse json error: {e} body={body}"))?;
    let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
    if code != 200 {
        return Err(anyhow::anyhow!("feeBurn enable failed: {}", body));
    }
    Ok(())
}

async fn check_binance_fee_burn(
    api_key: &str,
    api_secret: &str,
    base_url: &str,
    local_ip: Option<IpAddr>,
) -> Result<()> {
    let fee_burn = fetch_binance_fee_burn(api_key, api_secret, base_url, local_ip).await?;
    if fee_burn {
        return Ok(());
    }
    info!("feeBurn=false detected; attempting to enable...");
    enable_binance_fee_burn(api_key, api_secret, base_url, local_ip).await?;
    let fee_burn_after = fetch_binance_fee_burn(api_key, api_secret, base_url, local_ip).await?;
    if !fee_burn_after {
        return Err(anyhow::anyhow!("feeBurn still false after enable attempt"));
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "TRADE_ENGINE_CORE")?;
    let ipc_core = resolve_core(args.ipc_core, "TRADE_ENGINE_IPC_CORE");
    let exchange_name = args.exchange.as_str();
    let exchange = Exchange::from_str(exchange_name)
        .ok_or_else(|| anyhow::anyhow!("Invalid exchange name: {}", exchange_name))?;
    let exec_backend = ExecBackend::for_exchange(exchange);
    info!(
        "trade_engine starting (exchange={} exec_backend={})",
        exchange_name,
        exec_backend.as_str()
    );
    let binance_account_mode = if exchange_name == "binance" && exec_backend != ExecBackend::Ltp {
        let mode = init_binance_account_mode("trade_engine");
        info!("BINANCE_ACCOUNT_MODE={}", mode.as_str());
        Some(mode)
    } else {
        None
    };

    let local_ip_cfg = load_trade_engine_local_ip_config().await?;
    let local_ips = local_ip_cfg.local_ips;
    let binance_um_ip_whitelist_mode =
        exec_backend != ExecBackend::Ltp && binance_um_ip_whitelist_mode_enabled();
    validate_binance_um_whitelist_ip_config(
        &local_ip_cfg.local_ip_values,
        local_ip_cfg.binance_um_whitelist_ip_value.as_deref(),
        exchange_name == "binance" && binance_um_ip_whitelist_mode,
        &local_ip_cfg.source,
        "trade_engine",
    );
    let binance_um_whitelist_ip = if exchange_name == "binance" && binance_um_ip_whitelist_mode {
        Some(
            local_ip_cfg
                .binance_um_whitelist_ip_value
                .as_deref()
                .expect("validate_binance_um_whitelist_ip_config must require whitelist ip")
                .trim()
                .parse::<IpAddr>()
                .with_context(|| "parse binance_um_whitelist_ip for trade_engine dispatch")?,
        )
    } else {
        None
    };
    let binance_um_ws_direct_ips = local_ip_cfg
        .binance_um_ws_direct_ip_values
        .iter()
        .enumerate()
        .map(|(idx, ip)| {
            ip.parse::<IpAddr>().with_context(|| {
                format!(
                    "parse binance_um_ws_direct_ips[{}] for trade_engine dispatch: {}",
                    idx, ip
                )
            })
        })
        .collect::<Result<Vec<IpAddr>>>()?;
    info!(
        "using local IPs from {}: {}",
        local_ip_cfg.source,
        local_ips
            .iter()
            .map(|ip| ip.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    if !local_ips.is_empty() {
        info!(
            "configured local IPs: {}",
            local_ips
                .iter()
                .map(|ip| ip.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if exchange_name == "binance" && !binance_um_ws_direct_ips.is_empty() {
        info!(
            "configured Binance UM WS direct IPs (+1 DNS fallback): {}",
            binance_um_ws_direct_ips
                .iter()
                .map(|ip| ip.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if exchange_name == "binance" {
        let cfg = &local_ip_cfg.binance_um_ws_health;
        info!(
            "configured Binance UM WS health: new_rolling_window={} new_min_period={} cancel_rolling_window={} cancel_min_period={} percentile={} pause_ms={} select_recent={} cancel_probe_rate_limit_guard_pct={} inflight_block_threshold=rolling_percentile",
            cfg.new_rolling_window,
            cfg.new_min_period,
            cfg.cancel_rolling_window,
            cfg.cancel_min_period,
            cfg.percentile,
            cfg.pause_ms,
            cfg.select_recent,
            cfg.cancel_probe_rate_limit_guard_pct
        );
        let route_cfg = &local_ip_cfg.binance_um_ws_route;
        info!(
            "configured Binance UM WS route: route={} redis_env={:?} redis_key_suffix={} write_interval_ms={} read_interval_ms={} score_half_life_ms={} score_window_ms={} route_family={} min_samples={}",
            route_cfg.route.as_str(),
            route_cfg.redis_env,
            route_cfg.redis_key_suffix,
            route_cfg.write_interval_ms,
            route_cfg.read_interval_ms,
            route_cfg.score_half_life_ms,
            route_cfg.score_window_ms,
            route_cfg.route_family,
            route_cfg.min_samples
        );
    }

    // OKEx 不需要从环境变量读取 API key，因为在 WebSocket 客户端中会自动处理
    let accounts = if exec_backend == ExecBackend::Ltp {
        info!(
            "LTP backend mode: native exchange API credentials are not loaded here; ws client requires LTP_API_KEY and LTP_API_SECRET"
        );
        info!(
            "Optional env vars: TRADE_ENGINE_EXEC_BACKEND_MAP, LTP_WS_URL, LTP_WS_PING_INTERVAL_MS, LTP_WS_ONLY_TRADE"
        );
        vec![]
    } else if exchange_name == "okex" {
        info!("OKEx mode: API credentials will be loaded from OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE environment variables");
        vec![] // OKEx 不需要在这里配置
    } else {
        // Binance 等其他交易所需要从环境变量读取
        let env_prefix = exchange_name.to_ascii_uppercase();
        let api_key_var = format!("{}_API_KEY", env_prefix);
        let api_secret_var = format!("{}_API_SECRET", env_prefix);
        let api_name_var = format!("{}_API_NAME", env_prefix);

        let binance_standard_mode = exchange_name == "binance"
            && matches!(binance_account_mode, Some(BinanceAccountMode::Standard));
        let mut required_env = vec![api_key_var.clone()];
        if exchange_name == "binance" {
            required_env.push("BINANCE_ACCOUNT_MODE".to_string());
            if binance_standard_mode {
                required_env.push(format!(
                    "{} or {}",
                    api_secret_var, BINANCE_ED25519_PRIVATE_KEY_PATH_ENV
                ));
            } else {
                required_env.push(api_secret_var.clone());
            }
        } else {
            required_env.push(api_secret_var.clone());
        }
        info!("Required env vars: {}", required_env.join(", "));
        let optional_env = if exchange_name == "binance" {
            format!(
                "{}, {} (for HMAC REST/WS fallback), {} (Ed25519 WS; wins when both signers are set), {} (on/off, default=off)",
                api_name_var,
                api_secret_var,
                BINANCE_ED25519_PRIVATE_KEY_PATH_ENV,
                BINANCE_UM_IP_WHITELIST_MODE_ENV
            )
        } else {
            format!("{} (default=\"default\")", api_name_var)
        };
        info!(
            "Optional local config: ./trade_engine.toml or ./trade engine.toml; optional env vars: {}",
            optional_env
        );

        // 从环境变量读取账户配置（必须）
        let api_key_raw = std::env::var(&api_key_var).map_err(|_| {
            anyhow::anyhow!(
                "{} not set. Export it before running trade_engine",
                api_key_var
            )
        })?;
        let api_key = api_key_raw.trim().to_string();

        let api_secret = if binance_standard_mode {
            std::env::var(&api_secret_var)
                .ok()
                .map(|v| v.trim().to_string())
                .unwrap_or_default()
        } else {
            let api_secret_raw = std::env::var(&api_secret_var).map_err(|_| {
                anyhow::anyhow!(
                    "{} not set. Export it before running trade_engine",
                    api_secret_var
                )
            })?;
            api_secret_raw.trim().to_string()
        };

        if binance_standard_mode {
            let ed25519_path = std::env::var(BINANCE_ED25519_PRIVATE_KEY_PATH_ENV)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            if api_secret.is_empty() && ed25519_path.is_none() {
                return Err(anyhow::anyhow!(
                    "Binance STANDARD trade_engine requires either {} or {}",
                    api_secret_var,
                    BINANCE_ED25519_PRIVATE_KEY_PATH_ENV
                ));
            }
            if let Some(path) = ed25519_path.as_ref() {
                info!(
                    "Binance WS signer source: {}={} (preferred over {} when both are set)",
                    BINANCE_ED25519_PRIVATE_KEY_PATH_ENV, path, api_secret_var
                );
            } else {
                info!("Binance WS signer source: {}", api_secret_var);
            }
        }

        let api_name = std::env::var(&api_name_var).unwrap_or_else(|_| "default".to_string());

        info!("trade_engine account name: {}", api_name);
        log_credential_preview(&api_key_var, &api_key);
        if !api_secret.is_empty() {
            log_credential_preview(&api_secret_var, &api_secret);
        } else {
            info!(
                "{} not set; Binance WS must use Ed25519 signing",
                api_secret_var
            );
        }

        if binance_standard_mode && !api_secret.is_empty() {
            let default_fapi_base_url = if binance_um_ip_whitelist_mode {
                RestConstants::BINANCE_FAPI_MM_BASE_URL
            } else {
                RestConstants::BINANCE_FAPI_BASE_URL
            };
            let base_url = std::env::var("BINANCE_FAPI_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default_fapi_base_url.to_string());
            let fee_burn_local_ip = if binance_um_ip_whitelist_mode {
                binance_um_whitelist_ip.or_else(|| local_ips.first().copied())
            } else {
                local_ips.first().copied()
            };
            info!(
                "checking binance feeBurn (STANDARD mode) base_url={} local_ip={}",
                base_url,
                fee_burn_local_ip
                    .map(|ip| ip.to_string())
                    .unwrap_or_else(|| "system-default".to_string())
            );
            if let Err(err) =
                check_binance_fee_burn(&api_key, &api_secret, &base_url, fee_burn_local_ip).await
            {
                panic!("binance feeBurn check failed: {err}");
            }
            info!("binance feeBurn enabled");
        } else if binance_standard_mode {
            info!("skip binance feeBurn check: {} not set", api_secret_var);
        }

        vec![ApiKey {
            name: api_name,
            key: api_key,
            secret: api_secret,
        }]
    };

    info!("trade_engine initialized");
    let engine = TradeEngine::new(
        local_ips,
        accounts,
        ipc_core,
        binance_um_whitelist_ip,
        binance_um_ws_direct_ips,
        local_ip_cfg.binance_um_ws_health,
        local_ip_cfg.binance_um_ws_route,
    );

    let local = tokio::task::LocalSet::new();
    let shutdown = CancellationToken::new();
    setup_signal_handlers(&shutdown);
    local
        .run_until(engine.run_with_shutdown(exchange, shutdown.clone()))
        .await
}
