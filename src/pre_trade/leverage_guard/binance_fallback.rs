//! Opt-in native STANDARD USD-M leverage policy. Exchange I/O runs outside the
//! target publication path. The exchange remains the final order-risk gate.
use super::*;
use crate::strategy::batch_exec_strategy::BatchExecTarget;
use serde::{Deserialize, Serialize};

pub const STATUS_KEY: &str = "exec_leverage_guard";
const ENABLE_ENV: &str = "EXEC_BINANCE_UM_LEVERAGE_FALLBACK";
static STARTED: OnceLock<()> = OnceLock::new();
static CAP_REJECTIONS: OnceLock<std::sync::Mutex<BTreeMap<String, i64>>> = OnceLock::new();
static INITIALIZATION_ERRORS: OnceLock<std::sync::Mutex<BTreeMap<String, String>>> =
    OnceLock::new();
static MUTATION: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) fn enabled() -> bool {
    std::env::var(ENABLE_ENV).is_ok_and(|v| v == "1")
}

/// Capture authoritative order-rejection evidence without any exchange I/O on
/// the execution callback. The periodic worker consumes it after verification.
pub(crate) fn report_cap_rejection(venue: TradingVenue, symbol: &str, code: i64) {
    if enabled() && venue == TradingVenue::BinanceFutures && code == -2027 {
        CAP_REJECTIONS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(symbol.to_string(), get_timestamp_us());
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Status {
    updated_at_us: i64,
    error: Option<String>,
    symbols: BTreeMap<String, SymbolStatus>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SymbolStatus {
    leverage: u8,
    max_notional: f64,
    required_notional: f64,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct Position {
    qty: f64,
    mark: f64,
    leverage: u8,
    cap: f64,
}

struct Api {
    client: Client,
    base: String,
    key: String,
    secret: String,
}

#[derive(Debug)]
struct Rejected {
    status: u16,
    code: Option<i64>,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Binance leverage request rejected: HTTP {}, code {:?}",
            self.status, self.code
        )
    }
}
impl std::error::Error for Rejected {}

fn can_fallback(error: &anyhow::Error) -> bool {
    // Never lower leverage for authentication, network, rate limit or margin
    // errors. -2028 explicitly means insufficient margin, not a leverage cap.
    error
        .downcast_ref::<Rejected>()
        .is_some_and(|e| e.status == 400 && matches!(e.code, Some(-4028 | -2027)))
}

fn number(value: &Value, field: &str) -> Result<f64> {
    value
        .get(field)
        .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
        .filter(|v| v.is_finite())
        .with_context(|| format!("invalid Binance {field}"))
}

fn parse_positions(value: &Value) -> Result<BTreeMap<String, Position>> {
    let mut result = BTreeMap::new();
    for row in value.as_array().context("positionRisk must be an array")? {
        let symbol = row["symbol"].as_str().context("missing position symbol")?;
        anyhow::ensure!(
            row["positionSide"] == "BOTH",
            "one-way position required: {symbol}"
        );
        let leverage = number(row, "leverage")?;
        let position = Position {
            qty: number(row, "positionAmt")?,
            mark: number(row, "markPrice")?,
            leverage: leverage as u8,
            cap: number(row, "maxNotionalValue")?,
        };
        anyhow::ensure!(
            leverage.fract() == 0.
                && (1. ..=125.).contains(&leverage)
                && position.cap >= 0.
                && position.mark >= 0.,
            "invalid position risk: {symbol}"
        );
        anyhow::ensure!(
            result.insert(symbol.to_owned(), position).is_none(),
            "duplicate position: {symbol}"
        );
    }
    Ok(result)
}

fn bracket_cap(value: &Value, symbol: &str, leverage: u8) -> Result<f64> {
    let rows = value
        .as_array()
        .context("leverageBracket must be an array")?;
    anyhow::ensure!(
        rows.len() == 1 && rows[0]["symbol"] == symbol,
        "bracket symbol mismatch"
    );
    let brackets = rows[0]["brackets"]
        .as_array()
        .filter(|b| !b.is_empty())
        .context("missing brackets")?;
    let mut cap: f64 = 0.;
    for bracket in brackets {
        let initial = number(bracket, "initialLeverage")?;
        let candidate = number(bracket, "notionalCap")?;
        anyhow::ensure!(
            initial >= 1. && initial.fract() == 0. && candidate > 0.,
            "invalid bracket"
        );
        if initial >= f64::from(leverage) {
            cap = cap.max(candidate);
        }
    }
    anyhow::ensure!(cap > 0., "no bracket for {symbol} at {leverage}x");
    // Binance's signed response already includes account-specific bracket caps.
    Ok(cap)
}

fn peak_notional(position: &Position, target: f64, orders: &Value, symbol: &str) -> Result<f64> {
    anyhow::ensure!(
        position.mark > 0. && target.is_finite(),
        "missing fresh positive mark: {symbol}"
    );
    let (mut buys, mut sells) = (0., 0.);
    for order in orders.as_array().context("openOrders must be an array")? {
        anyhow::ensure!(
            order["symbol"] == symbol && order["positionSide"] == "BOTH",
            "open order identity mismatch"
        );
        let original = number(order, "origQty")?;
        let executed = number(order, "executedQty")?;
        anyhow::ensure!(
            original >= 0. && executed >= 0. && executed <= original,
            "invalid open order quantity"
        );
        match order["side"].as_str() {
            Some("BUY") => buys += original - executed,
            Some("SELL") => sells += original - executed,
            _ => bail!("invalid open order side"),
        }
    }
    let peak = position
        .qty
        .abs()
        .max(target.abs())
        .max((position.qty + buys).abs())
        .max((position.qty - sells).abs());
    let notional = peak * position.mark;
    anyhow::ensure!(notional.is_finite(), "nonfinite required notional");
    Ok(notional)
}

fn margin_needed(position: &Position, required: f64, leverage: u8) -> f64 {
    // Reserve enough for both the existing holding's leverage change and the
    // complete requested position. Do not spend another symbol's released margin.
    (required / f64::from(leverage)
        - position.qty.abs() * position.mark / f64::from(position.leverage))
    .max(0.)
}

impl Api {
    fn from_env(client: Client) -> Result<Self> {
        Ok(Self {
            client,
            base: env_or("BINANCE_FAPI_URL", "https://fapi.binance.com"),
            key: required_env("BINANCE_API_KEY")?,
            secret: required_env("BINANCE_API_SECRET")?,
        })
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value> {
        let mut query: BTreeMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        query.insert(
            "timestamp".into(),
            Utc::now().timestamp_millis().to_string(),
        );
        query.insert("recvWindow".into(), "5000".into());
        let query = build_query(&query);
        let signature = hmac_sha256_hex(&self.secret, &query);
        let response = self
            .client
            .request(
                method,
                format!(
                    "{}{path}?{query}&signature={signature}",
                    self.base.trim_end_matches('/')
                ),
            )
            .header("X-MBX-APIKEY", &self.key)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        let status = response.status().as_u16();
        let value: Value = response
            .json()
            .await
            .map_err(reqwest::Error::without_url)
            .context("invalid Binance risk response")?;
        if !(200..300).contains(&status)
            || value
                .get("code")
                .and_then(Value::as_i64)
                .is_some_and(|c| c < 0)
        {
            return Err(Rejected {
                status,
                code: value["code"].as_i64(),
            }
            .into());
        }
        Ok(value)
    }

    async fn positions(&self, symbol: Option<&str>) -> Result<BTreeMap<String, Position>> {
        let params = symbol
            .map(|s| vec![("symbol", s.to_string())])
            .unwrap_or_default();
        parse_positions(
            &self
                .request(reqwest::Method::GET, "/fapi/v2/positionRisk", &params)
                .await?,
        )
    }

    async fn position(&self, symbol: &str) -> Result<Position> {
        self.positions(Some(symbol))
            .await?
            .remove(symbol)
            .context("positionRisk omitted requested symbol")
    }

    async fn set(&self, symbol: &str, leverage: u8) -> Result<Position> {
        let result = self
            .request(
                reqwest::Method::POST,
                "/fapi/v1/leverage",
                &[
                    ("symbol", symbol.into()),
                    ("leverage", leverage.to_string()),
                ],
            )
            .await;
        // Always read back after a POST, including a timeout. Never submit a
        // lower level while the outcome of the previous mutation is unknown.
        let actual = self.position(symbol).await.map_err(|error| {
            anyhow!("leverage POST readback failed; outcome unconfirmed: {error:#}")
        })?;
        match result {
            Ok(ack) => {
                anyhow::ensure!(
                    ack["symbol"] == symbol
                        && number(&ack, "leverage")? == f64::from(leverage)
                        && actual.leverage == leverage
                        && number(&ack, "maxNotionalValue")? == actual.cap,
                    "leverage acknowledgement/readback mismatch"
                );
            }
            Err(err) if actual.leverage != leverage => return Err(err),
            Err(_) => {
                info!("Binance leverage POST confirmed by signed readback: symbol={symbol} leverage={leverage}");
            }
        }
        Ok(actual)
    }

    async fn lower(
        &self,
        symbol: &str,
        position: &Position,
        required: f64,
        first: u8,
    ) -> Result<Position> {
        let brackets = self
            .request(
                reqwest::Method::GET,
                "/fapi/v1/leverageBracket",
                &[("symbol", symbol.into())],
            )
            .await?;
        for leverage in [4, 3]
            .into_iter()
            .filter(|l| *l <= first && *l < position.leverage)
        {
            if bracket_cap(&brackets, symbol, leverage)? < required {
                continue;
            }
            // Refresh both the holding and spendable collateral for EACH mutation.
            let current = self.position(symbol).await?;
            anyhow::ensure!(
                current.leverage == position.leverage,
                "concurrent leverage change; defer {symbol}"
            );
            let account = self
                .request(reqwest::Method::GET, "/fapi/v3/account", &[])
                .await?;
            let available = number(&account, "availableBalance")?;
            let required_now = if required == 0. {
                0.
            } else {
                anyhow::ensure!(
                    position.mark > 0. && current.mark > 0.,
                    "missing positive fallback mark"
                );
                required * (current.mark / position.mark).max(1.)
            };
            let required_now = required_now.max(current.qty.abs() * current.mark);
            anyhow::ensure!(required_now.is_finite(), "invalid refreshed notional");
            if bracket_cap(&brackets, symbol, leverage)? < required_now {
                continue;
            }
            let needed = margin_needed(
                &current,
                required_now.max(current.qty.abs() * current.mark),
                leverage,
            );
            anyhow::ensure!(available >= needed * 1.01, "insufficient available margin: {symbol} needs {needed:.2}, available {available:.2}");
            match self.set(symbol, leverage).await {
                Ok(after) => {
                    anyhow::ensure!(
                        after.cap >= required_now,
                        "readback position cap still insufficient: {symbol}"
                    );
                    warn!("Binance leverage automatic fallback confirmed: symbol={symbol} {}x -> {leverage}x cap={} required={required}", position.leverage, after.cap);
                    return Ok(after);
                }
                Err(error) if can_fallback(&error) => continue,
                Err(error) => return Err(error),
            }
        }
        bail!("no permitted leverage through 3x covers {symbol}, required notional {required:.2}")
    }
}

pub(super) async fn initialize(client: Client, symbol: &str) -> Result<LeverageInitOutcome> {
    let result = initialize_inner(client, symbol).await;
    let mut errors = INITIALIZATION_ERRORS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    match &result {
        Ok(_) => {
            errors.remove(symbol);
        }
        Err(error) => {
            errors.insert(
                symbol.to_string(),
                format!("leverage initialization failed: {error:#}"),
            );
        }
    }
    result
}

async fn initialize_inner(client: Client, symbol: &str) -> Result<LeverageInitOutcome> {
    let _lock = MUTATION.lock().await;
    let api = Api::from_env(client)?;
    initialize_api(&api, symbol).await
}

async fn initialize_api(api: &Api, symbol: &str) -> Result<LeverageInitOutcome> {
    match api.set(symbol, 5).await {
        Ok(_) => Ok(LeverageInitOutcome::Set),
        Err(error) if can_fallback(&error) => {
            let position = api.position(symbol).await?;
            // A rejected 5x setting may leave the exchange's default (e.g.20x).
            let required = position.qty.abs() * position.mark;
            let actual = if (3..=4).contains(&position.leverage) && position.cap >= required {
                position
            } else {
                api.lower(symbol, &position, required, 4).await?
            };
            Ok(LeverageInitOutcome::Fallback(actual.leverage))
        }
        Err(error) => Err(error),
    }
}

#[derive(Deserialize)]
struct TargetConfig {
    targets: BTreeMap<String, BatchExecTarget>,
}

async fn load_targets(redis: &mut RedisClient) -> Result<BTreeMap<String, f64>> {
    let mut targets = BTreeMap::new();
    for family in ["batch_exec", "chase_exec"] {
        let names: Vec<String> = redis
            .get_json(&format!("{family}:strategy_names"))
            .await?
            .unwrap_or_default();
        let mut seen = BTreeSet::new();
        for name in names {
            crate::pre_trade::batch_exec_config::validate_config_strategy_name(&name)?;
            anyhow::ensure!(seen.insert(name.clone()), "duplicate strategy index");
            let config: TargetConfig = redis
                .get_json(&format!("{family}:{name}"))
                .await?
                .context("indexed target missing")?;
            for (symbol, target) in config.targets {
                runtime_common::exec_market::validate_symbol(
                    TradingVenue::BinanceFutures,
                    &symbol,
                )?;
                *targets.entry(symbol).or_insert(0.) += target.qty;
            }
        }
    }
    anyhow::ensure!(
        targets.values().all(|q| q.is_finite()),
        "nonfinite aggregate target"
    );
    Ok(targets)
}

async fn check_symbol(
    api: &Api,
    symbol: &str,
    target: f64,
    position: &Position,
    orders: &Value,
    cap_rejected: bool,
) -> Result<Position> {
    let required = peak_notional(position, target, orders, symbol)?;
    anyhow::ensure!(
        required.is_finite() && position.mark > 0.,
        "missing positive mark: {symbol}"
    );
    if required <= position.cap && !cap_rejected {
        return Ok(position.clone());
    }
    anyhow::ensure!(
        (3..=5).contains(&position.leverage),
        "leverage outside automatic policy: {symbol}"
    );
    let _lock = MUTATION.lock().await;
    let orders = api
        .request(
            reqwest::Method::GET,
            "/fapi/v1/openOrders",
            &[("symbol", symbol.into())],
        )
        .await?;
    let current = api.position(symbol).await?;
    let required = peak_notional(&current, target, &orders, symbol)?;
    if required <= current.cap && !cap_rejected {
        return Ok(current);
    }
    api.lower(
        symbol,
        &current,
        required,
        current.leverage.saturating_sub(1),
    )
    .await
}

async fn poll(api: &Api, redis: &mut RedisClient, status: &mut Status) -> Result<()> {
    let targets = load_targets(redis).await?;
    let positions = api.positions(None).await?;
    let all_orders = api
        .request(reqwest::Method::GET, "/fapi/v1/openOrders", &[])
        .await?;
    let all_orders = all_orders
        .as_array()
        .context("openOrders must be an array")?;
    let overrides =
        binance_um_leverage_overrides(TradingVenue::BinanceFutures, ExecBackend::Native)?;
    let mut symbols = BTreeMap::new();
    let cap_rejections = {
        let mut pending = CAP_REJECTIONS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        pending.retain(|_, ts| get_timestamp_us().saturating_sub(*ts) <= 120_000_000);
        pending.clone()
    };
    let mut targets = targets;
    for symbol in cap_rejections.keys() {
        targets.entry(symbol.clone()).or_insert(0.);
    }
    for (symbol, position) in &positions {
        if position.qty != 0. || (3..=4).contains(&position.leverage) {
            targets.entry(symbol.clone()).or_insert(0.);
        }
    }
    for (symbol, target) in targets {
        if overrides.contains_key(&symbol) {
            continue;
        }
        let orders = Value::Array(
            all_orders
                .iter()
                .filter(|o| o["symbol"] == symbol)
                .cloned()
                .collect(),
        );
        let mut item = SymbolStatus::default();
        let result = match positions.get(&symbol) {
            Some(position) => {
                item.leverage = position.leverage;
                item.max_notional = position.cap;
                item.required_notional =
                    peak_notional(position, target, &orders, &symbol).unwrap_or(0.);
                if !cap_rejections.contains_key(&symbol)
                    && target == 0.
                    && position.qty == 0.
                    && orders.as_array().is_some_and(|o| o.is_empty())
                {
                    Ok(position.clone())
                } else {
                    check_symbol(
                        api,
                        &symbol,
                        target,
                        position,
                        &orders,
                        cap_rejections.contains_key(&symbol),
                    )
                    .await
                }
            }
            None => Err(anyhow!("positionRisk omitted target symbol {symbol}")),
        };
        match result {
            Ok(after) => {
                item.leverage = after.leverage;
                item.max_notional = after.cap;
                if let Some(observed) = cap_rejections.get(&symbol) {
                    let mut pending = CAP_REJECTIONS
                        .get()
                        .unwrap()
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if pending.get(&symbol) == Some(observed) {
                        pending.remove(&symbol);
                    }
                }
            }
            Err(error) => item.error = Some(format!("{error:#}")),
        }
        symbols.insert(symbol, item);
    }
    if let Some(errors) = INITIALIZATION_ERRORS.get() {
        for (symbol, error) in errors.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            symbols.entry(symbol.clone()).or_default().error = Some(error.clone());
        }
    }
    status.symbols = symbols;
    Ok(())
}

pub(crate) fn spawn(
    redis: RedisSettings,
    venue: TradingVenue,
    mode: Option<BinanceAccountMode>,
) -> Result<()> {
    if !enabled() {
        return Ok(());
    }
    anyhow::ensure!(
        venue == TradingVenue::BinanceFutures
            && mode == Some(BinanceAccountMode::Standard)
            && ExecBackend::for_exchange(Exchange::Binance)? == ExecBackend::Native,
        "automatic leverage fallback supports native STANDARD Binance USD-M only"
    );
    anyhow::ensure!(
        redis.prefix.as_deref().is_some_and(|p| !p.is_empty()),
        "leverage guard requires source/venue-scoped Redis prefix"
    );
    if STARTED.get().is_some() {
        return Ok(());
    }
    let local_ip = binance_exec_leverage_local_ip()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .local_address(local_ip)
        .build()?;
    let api = Api::from_env(client)?;
    if STARTED.set(()).is_err() {
        return Ok(());
    }
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(60));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut status = Status::default();
        loop {
            timer.tick().await;
            // Separate connection: neither a slow API nor Redis retry holds the
            // reloader/publication lock or delays an already admitted target.
            match RedisClient::connect(redis.clone()).await {
                Ok(mut connection) => {
                    let result = match binance_exec_leverage_local_ip() {
                        Ok(current) if current == local_ip => poll(&api, &mut connection, &mut status).await,
                        _ => Err(anyhow!("trading IP changed or unreadable; leverage checks stopped until restart")),
                    };
                    status.error = result.err().map(|e| format!("{e:#}"));
                    status.updated_at_us = get_timestamp_us();
                    if let Err(error) = connection.set_json(STATUS_KEY, &status).await {
                        warn!("leverage guard status save failed: {error:#}");
                    }
                }
                Err(_) => warn!("leverage guard Redis connection failed; retry in 60s"),
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    fn position(leverage: u8, cap: f64) -> Value {
        json!([{"symbol":"ONEUSDT","positionSide":"BOTH","positionAmt":"-40810204",
            "markPrice":"0.00126515","leverage":leverage,"maxNotionalValue":cap}])
    }
    fn brackets() -> Value {
        json!([{"symbol":"ONEUSDT","brackets":[
            {"initialLeverage":50,"notionalCap":2000}, {"initialLeverage":20,"notionalCap":5000},
            {"initialLeverage":10,"notionalCap":12000}, {"initialLeverage":5,"notionalCap":50000},
            {"initialLeverage":4,"notionalCap":200000}, {"initialLeverage":3,"notionalCap":500000},
            {"initialLeverage":2,"notionalCap":2500000}]}])
    }
    fn current() -> Position {
        parse_positions(&position(5, 50000.))
            .unwrap()
            .remove("ONEUSDT")
            .unwrap()
    }
    fn get(path: &str, value: Value) -> (String, u16, Value) {
        (format!("GET {path}?"), 200, value)
    }
    fn post(level: u8, status: u16, value: Value) -> (String, u16, Value) {
        (
            format!("POST /fapi/v1/leverage?leverage={level}&"),
            status,
            value,
        )
    }
    fn ack(level: u8, cap: f64) -> Value {
        json!({"symbol":"ONEUSDT","leverage":level,"maxNotionalValue":cap})
    }

    #[test]
    fn clear_targets_have_zero_quantity() {
        let config: TargetConfig = serde_json::from_value(json!({"targets": {
            "ACEUSDT": {"qty": "clear", "signal": 0},
            "TUTUSDT": "clear"
        }}))
        .unwrap();
        assert_eq!(config.targets["ACEUSDT"].qty, 0.);
        assert_eq!(config.targets["TUTUSDT"].qty, 0.);
    }

    #[test]
    fn numeric_targets_keep_their_quantity() {
        let config: TargetConfig = serde_json::from_value(json!({"targets": {
            "ONEUSDT": {"qty": -15.5, "signal": 0},
            "BTCUSDT": 1.25
        }}))
        .unwrap();
        assert_eq!(config.targets["ONEUSDT"].qty, -15.5);
        assert_eq!(config.targets["BTCUSDT"].qty, 1.25);
    }

    #[test]
    fn invalid_target_quantity_fails_parsing() {
        for qty in [json!("CLEAR"), json!("bad"), json!("1.25"), Value::Null] {
            let config = json!({"targets": {"ONEUSDT": {"qty": qty, "signal": 0}}});
            assert!(serde_json::from_value::<TargetConfig>(config).is_err());
        }
    }

    fn fixture(steps: Vec<(String, u16, Value)>) -> (Api, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let handle = std::thread::spawn(move || {
            for (expected, status, value) in steps {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    if let Ok((stream, _)) = listener.accept() {
                        break stream;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "expected request not received: {expected}"
                    );
                    std::thread::sleep(Duration::from_millis(2));
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(line.starts_with(&expected), "unexpected request route");
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                if status == 0 {
                    continue;
                } // Lost POST response; peer closed.
                let body = value.to_string();
                write!(stream,"HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
            }
        });
        (
            Api {
                client: Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(2))
                    .build()
                    .unwrap(),
                base,
                key: "fixture-key".into(),
                secret: "fixture-secret".into(),
            },
            handle,
        )
    }

    #[test]
    fn real_one_case_selects_minimal_fallback_and_margin() {
        let p = current();
        let required = peak_notional(&p, -43626349.5493215, &json!([]), "ONEUSDT").unwrap();
        assert!(required > 55000. && required < 55200.);
        assert_eq!(bracket_cap(&brackets(), "ONEUSDT", 5).unwrap(), 50000.);
        assert_eq!(bracket_cap(&brackets(), "ONEUSDT", 4).unwrap(), 200000.);
        assert_eq!(bracket_cap(&brackets(), "ONEUSDT", 3).unwrap(), 500000.);
        assert!(margin_needed(&p, required, 4) > 3400.);
    }

    #[test]
    fn reject_wrong_identity_invalid_values_and_hedge_positions() {
        assert!(bracket_cap(&brackets(), "BTCUSDT", 4).is_err());
        let mut value = position(5, 50000.);
        value[0]["positionSide"] = json!("LONG");
        assert!(parse_positions(&value).is_err());
        value[0]["positionSide"] = json!("BOTH");
        value[0]["positionAmt"] = json!("NaN");
        assert!(parse_positions(&value).is_err());
        assert!(parse_positions(&json!({})).is_err());
        assert!(
            peak_notional(&current(), 1., &json!([{ "symbol":"BTCUSDT" }]), "ONEUSDT").is_err()
        );
    }

    #[test]
    fn only_explicit_leverage_or_cap_rejections_allow_fallback() {
        for code in [-2027, -4028] {
            assert!(can_fallback(
                &Rejected {
                    status: 400,
                    code: Some(code)
                }
                .into()
            ));
        }
        for code in [-2028, -2015, -1021, -1003, -4003] {
            assert!(!can_fallback(
                &Rejected {
                    status: 400,
                    code: Some(code)
                }
                .into()
            ));
        }
        assert!(!can_fallback(
            &Rejected {
                status: 429,
                code: Some(-2027)
            }
            .into()
        ));
        assert!(!can_fallback(&anyhow!("timeout")));
    }

    #[test]
    fn peak_includes_both_order_directions_and_reversal_target() {
        let p = Position {
            qty: 10.,
            mark: 100.,
            leverage: 5,
            cap: 2000.,
        };
        let orders = json!([
            {"symbol":"ONEUSDT","positionSide":"BOTH","side":"BUY","origQty":"20","executedQty":"5"},
            {"symbol":"ONEUSDT","positionSide":"BOTH","side":"SELL","origQty":"50","executedQty":"0"}]);
        assert_eq!(peak_notional(&p, -30., &orders, "ONEUSDT").unwrap(), 4000.);
    }

    #[tokio::test]
    async fn initialization_prefers_five_and_falls_back_only_after_explicit_rejection() {
        let (api, h) = fixture(vec![
            post(5, 200, ack(5, 50000.)),
            get("/fapi/v2/positionRisk", position(5, 50000.)),
        ]);
        assert_eq!(
            initialize_api(&api, "ONEUSDT").await.unwrap(),
            LeverageInitOutcome::Set
        );
        h.join().unwrap();
        let (api, h) = fixture(vec![
            post(5, 400, json!({"code":-4028})),
            get("/fapi/v2/positionRisk", position(20, 5000.)),
            get("/fapi/v2/positionRisk", position(20, 5000.)),
            get("/fapi/v1/leverageBracket", brackets()),
            get("/fapi/v2/positionRisk", position(20, 5000.)),
            get("/fapi/v3/account", json!({"availableBalance":"1585605"})),
            post(4, 200, ack(4, 200000.)),
            get("/fapi/v2/positionRisk", position(4, 200000.)),
        ]);
        assert_eq!(
            initialize_api(&api, "ONEUSDT").await.unwrap(),
            LeverageInitOutcome::Fallback(4)
        );
        h.join().unwrap();
    }

    #[tokio::test]
    async fn cap_fallback_to_four_verifies_ack_and_signed_readback() {
        let (api, h) = fixture(vec![
            get("/fapi/v1/leverageBracket", brackets()),
            get("/fapi/v2/positionRisk", position(5, 50000.)),
            get("/fapi/v3/account", json!({"availableBalance":"1585605"})),
            post(4, 200, ack(4, 200000.)),
            get("/fapi/v2/positionRisk", position(4, 200000.)),
        ]);
        assert_eq!(
            api.lower("ONEUSDT", &current(), 55200., 4)
                .await
                .unwrap()
                .leverage,
            4
        );
        h.join().unwrap();
    }

    #[tokio::test]
    async fn four_insufficient_cap_selects_three_directly() {
        let (api, h) = fixture(vec![
            get("/fapi/v1/leverageBracket", brackets()),
            get("/fapi/v2/positionRisk", position(5, 50000.)),
            get("/fapi/v3/account", json!({"availableBalance":"1585605"})),
            post(3, 200, ack(3, 500000.)),
            get("/fapi/v2/positionRisk", position(3, 500000.)),
        ]);
        assert_eq!(
            api.lower("ONEUSDT", &current(), 250000., 4)
                .await
                .unwrap()
                .leverage,
            3
        );
        h.join().unwrap();
    }

    #[tokio::test]
    async fn rejected_four_allows_three_but_margin_or_auth_errors_do_not() {
        for (code, expected) in [(-4028, Some(3)), (-2028, None), (-2015, None)] {
            let mut steps = vec![
                get("/fapi/v1/leverageBracket", brackets()),
                get("/fapi/v2/positionRisk", position(5, 50000.)),
                get("/fapi/v3/account", json!({"availableBalance":"1585605"})),
                post(4, 400, json!({"code":code})),
                get("/fapi/v2/positionRisk", position(5, 50000.)),
            ];
            if expected.is_some() {
                steps.extend([
                    get("/fapi/v2/positionRisk", position(5, 50000.)),
                    get("/fapi/v3/account", json!({"availableBalance":"1585605"})),
                    post(3, 200, ack(3, 500000.)),
                    get("/fapi/v2/positionRisk", position(3, 500000.)),
                ]);
            }
            let (api, h) = fixture(steps);
            let result = api.lower("ONEUSDT", &current(), 55200., 4).await;
            assert_eq!(result.ok().map(|p| p.leverage), expected);
            h.join().unwrap();
        }
    }

    #[tokio::test]
    async fn insufficient_margin_and_three_cap_fail_without_posting() {
        let (api, h) = fixture(vec![
            get("/fapi/v1/leverageBracket", brackets()),
            get("/fapi/v2/positionRisk", position(5, 50000.)),
            get("/fapi/v3/account", json!({"availableBalance":"1"})),
        ]);
        assert!(api
            .lower("ONEUSDT", &current(), 55200., 4)
            .await
            .unwrap_err()
            .to_string()
            .contains("insufficient"));
        h.join().unwrap();
        let (api, h) = fixture(vec![get("/fapi/v1/leverageBracket", brackets())]);
        assert!(api.lower("ONEUSDT", &current(), 600000., 4).await.is_err());
        h.join().unwrap();
    }

    #[tokio::test]
    async fn default_five_rejected_is_classified_only_after_readback() {
        let (api, h) = fixture(vec![
            post(5, 400, json!({"code":-4028})),
            get("/fapi/v2/positionRisk", position(20, 5000.)),
        ]);
        assert!(can_fallback(&api.set("ONEUSDT", 5).await.unwrap_err()));
        h.join().unwrap();
    }

    #[tokio::test]
    async fn uncertain_post_requires_readback_and_mismatched_ack_never_falls_back() {
        let (api, h) = fixture(vec![
            post(4, 503, json!({"code":-1000})),
            get("/fapi/v2/positionRisk", position(4, 200000.)),
        ]);
        assert_eq!(api.set("ONEUSDT", 4).await.unwrap().leverage, 4);
        h.join().unwrap();
        let (api, h) = fixture(vec![
            post(4, 200, ack(3, 500000.)),
            get("/fapi/v2/positionRisk", position(4, 200000.)),
        ]);
        assert!(!can_fallback(&api.set("ONEUSDT", 4).await.unwrap_err()));
        h.join().unwrap();
    }

    #[tokio::test]
    async fn explicit_cap_rejection_handles_price_boundary_even_when_mark_estimate_fits() {
        let p = Position {
            qty: 1.,
            mark: 100.,
            leverage: 5,
            cap: 50000.,
        };
        let (api, h) = fixture(vec![
            get("/fapi/v1/openOrders", json!([])),
            get("/fapi/v2/positionRisk", position(5, 50000.)),
            get("/fapi/v1/leverageBracket", brackets()),
            get("/fapi/v2/positionRisk", position(5, 50000.)),
            get("/fapi/v3/account", json!({"availableBalance":"1585605"})),
            post(4, 200, ack(4, 200000.)),
            get("/fapi/v2/positionRisk", position(4, 200000.)),
        ]);
        assert_eq!(
            check_symbol(&api, "ONEUSDT", 1., &p, &json!([]), true)
                .await
                .unwrap()
                .leverage,
            4
        );
        h.join().unwrap();
    }

    #[tokio::test]
    async fn lost_post_response_is_reconciled_and_failed_readback_blocks_retry() {
        let (api, h) = fixture(vec![
            post(4, 0, json!(null)),
            get("/fapi/v2/positionRisk", position(4, 200000.)),
        ]);
        assert_eq!(api.set("ONEUSDT", 4).await.unwrap().leverage, 4);
        h.join().unwrap();
        let (api, h) = fixture(vec![
            post(4, 400, json!({"code":-4028})),
            (
                "GET /fapi/v2/positionRisk?".into(),
                503,
                json!({"code":-1000}),
            ),
        ]);
        assert!(!can_fallback(&api.set("ONEUSDT", 4).await.unwrap_err()));
        h.join().unwrap();
    }

    #[tokio::test]
    async fn existing_fallback_is_preserved_and_cannot_lower_below_three() {
        let (api, h) = fixture(vec![]);
        let p = parse_positions(&position(4, 200000.))
            .unwrap()
            .remove("ONEUSDT")
            .unwrap();
        assert_eq!(
            check_symbol(&api, "ONEUSDT", -43626349., &p, &json!([]), false)
                .await
                .unwrap()
                .leverage,
            4
        );
        h.join().unwrap();
        let p = parse_positions(&position(3, 500000.))
            .unwrap()
            .remove("ONEUSDT")
            .unwrap();
        let (api, h) = fixture(vec![get("/fapi/v1/leverageBracket", brackets())]);
        assert!(api.lower("ONEUSDT", &p, 600000., 2).await.is_err());
        h.join().unwrap();
    }
}
