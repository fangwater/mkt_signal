use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State as AxumState;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use log::{info, warn};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::common::exchange::Exchange;
use crate::common::redis_client::RedisSettings;
use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::common::{
    ArbDirection, CompareOp, FactorMode, FundingRatePeriod, OperationType,
};
use crate::funding_rate::funding_rate_factor::{FrThresholdConfig, BWD_OPEN_LOAN_RATE_MULTIPLIER};
use crate::funding_rate::{
    load_all_once_with_namespace, spawn_config_loader_with_namespace, ArbDecision, ArbSignalKind,
    FundingRateFactor, MktChannel, RateFetcher, SymbolList,
};
use crate::signal::common::TradingVenue;

#[derive(Debug, Clone)]
pub struct FrDashboardConfig {
    pub exchange: Exchange,
    pub symbol_namespace: String,
    pub symbol_key_suffix: String,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub bind: String,
    pub port: u16,
    pub ws_path: String,
    pub refresh_ms: u64,
}

#[derive(Clone)]
struct DashboardHub {
    tx: broadcast::Sender<String>,
    latest: Arc<RwLock<FrDashboardSnapshot>>,
}

#[derive(Clone)]
struct HttpState {
    hub: DashboardHub,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrDashboardSnapshot {
    #[serde(rename = "type")]
    pub kind: String,
    pub ts_ms: i64,
    pub exchange: String,
    pub factor_mode: String,
    pub symbol_namespace: String,
    pub symbol_key_suffix: String,
    pub open_venue: String,
    pub hedge_venue: String,
    pub summary: FrDashboardSummary,
    pub thresholds: Vec<FrDashboardThresholdLegend>,
    pub rows: Vec<FrDashboardRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrDashboardSummary {
    pub total_symbols: usize,
    pub active_signals: usize,
    pub forward_open: usize,
    pub forward_close: usize,
    pub backward_open: usize,
    pub backward_close: usize,
    pub neutral: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrDashboardThresholdLegend {
    pub period: String,
    pub signal: String,
    pub compare_op: String,
    pub expression: String,
    pub threshold_pct: f64,
    pub tone: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrDashboardListFlags {
    pub in_dump: bool,
    pub in_forward_trade: bool,
    pub in_backward_trade: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrDashboardRuleState {
    pub key: String,
    pub label: String,
    pub compare_op: Option<String>,
    pub expression: String,
    pub threshold_pct: Option<f64>,
    pub value_pct: Option<f64>,
    pub hit: bool,
    pub active: bool,
    pub tone: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FrDashboardRow {
    pub symbol: String,
    pub period: String,
    pub signal: String,
    pub signal_tone: String,
    pub lists: FrDashboardListFlags,
    pub latest_fr_pct: Option<f64>,
    pub pred_fr_pct: Option<f64>,
    pub fr_ma_pct: Option<f64>,
    pub pred_loan_pct: Option<f64>,
    pub cur_loan_pct: Option<f64>,
    pub fr_plus_pred_loan_pct: Option<f64>,
    pub ma_plus_cur_loan_pct: Option<f64>,
    pub rules: Vec<FrDashboardRuleState>,
}

impl DashboardHub {
    fn new(capacity: usize, initial: FrDashboardSnapshot) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            tx,
            latest: Arc::new(RwLock::new(initial)),
        }
    }

    fn publish_snapshot(&self, snapshot: FrDashboardSnapshot) {
        if let Ok(msg) = serde_json::to_string(&snapshot) {
            *self.latest.write() = snapshot;
            let _ = self.tx.send(msg);
        }
    }

    fn snapshot(&self) -> FrDashboardSnapshot {
        self.latest.read().clone()
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

pub async fn run(cfg: FrDashboardConfig, token: CancellationToken) -> Result<()> {
    info!(
        "fr_signal_dashboard starting exchange={} ns={} suffix={} open_venue={:?} hedge_venue={:?} bind={} port={}",
        cfg.exchange,
        cfg.symbol_namespace,
        cfg.symbol_key_suffix,
        cfg.open_venue,
        cfg.hedge_venue,
        cfg.bind,
        cfg.port
    );

    SymbolList::init_singleton()?;
    MktChannel::init_singleton_readonly(cfg.open_venue, cfg.hedge_venue)?;
    RateFetcher::init_for_venues(cfg.open_venue, cfg.hedge_venue)?;
    let _ = FundingRateFactor::instance();

    let redis = default_redis_settings();
    if let Err(err) = load_all_once_with_namespace(
        &redis,
        &cfg.symbol_namespace,
        &cfg.symbol_key_suffix,
        cfg.open_venue,
        cfg.hedge_venue,
    )
    .await
    {
        warn!("fr_signal_dashboard initial config load failed, using defaults: {err:#}");
    }
    spawn_config_loader_with_namespace(
        redis,
        cfg.symbol_namespace.clone(),
        cfg.symbol_key_suffix.clone(),
        cfg.open_venue,
        cfg.hedge_venue,
    );

    let initial_snapshot = build_snapshot(&cfg);
    let hub = DashboardHub::new(128, initial_snapshot.clone());
    hub.publish_snapshot(initial_snapshot);

    let http_cfg = cfg.clone();
    let http_hub = hub.clone();
    let mut server_task =
        tokio::task::spawn_local(async move { serve_http(http_cfg, http_hub).await });

    let poll_cfg = cfg.clone();
    let poll_hub = hub.clone();
    let poll_token = token.clone();
    tokio::task::spawn_local(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(poll_cfg.refresh_ms.max(200)));
        loop {
            tokio::select! {
                _ = poll_token.cancelled() => break,
                _ = interval.tick() => {
                    poll_hub.publish_snapshot(build_snapshot(&poll_cfg));
                }
            }
        }
    });

    tokio::select! {
        _ = token.cancelled() => {
            info!("fr_signal_dashboard received shutdown signal");
            server_task.abort();
            let _ = server_task.await;
            Ok(())
        }
        server_result = &mut server_task => {
            match server_result {
                Ok(result) => result,
                Err(err) => Err(anyhow::anyhow!("fr_signal_dashboard http task join failed: {err}")),
            }
        }
    }
}

fn default_redis_settings() -> RedisSettings {
    RedisSettings {
        host: "127.0.0.1".to_string(),
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

fn build_snapshot(cfg: &FrDashboardConfig) -> FrDashboardSnapshot {
    let mut symbols = SymbolList::instance().get_online_symbols();
    symbols.sort_unstable();
    symbols.dedup();

    let rows: Vec<FrDashboardRow> = symbols
        .iter()
        .map(|symbol| build_row(cfg, symbol))
        .collect();
    let thresholds = build_threshold_legends();
    let summary = build_summary(&rows);

    FrDashboardSnapshot {
        kind: "fr_dashboard".to_string(),
        ts_ms: (get_timestamp_us() / 1000) as i64,
        exchange: cfg.exchange.as_str().to_string(),
        factor_mode: factor_mode_label(FundingRateFactor::instance().get_mode()).to_string(),
        symbol_namespace: cfg.symbol_namespace.clone(),
        symbol_key_suffix: cfg.symbol_key_suffix.clone(),
        open_venue: cfg.open_venue.data_pub_slug().to_string(),
        hedge_venue: cfg.hedge_venue.data_pub_slug().to_string(),
        summary,
        thresholds,
        rows,
    }
}

fn build_summary(rows: &[FrDashboardRow]) -> FrDashboardSummary {
    let mut summary = FrDashboardSummary {
        total_symbols: rows.len(),
        active_signals: 0,
        forward_open: 0,
        forward_close: 0,
        backward_open: 0,
        backward_close: 0,
        neutral: 0,
    };

    for row in rows {
        match row.signal.as_str() {
            "FwdOpen" => {
                summary.forward_open += 1;
                summary.active_signals += 1;
            }
            "FwdClose" => {
                summary.forward_close += 1;
                summary.active_signals += 1;
            }
            "BwdOpen" => {
                summary.backward_open += 1;
                summary.active_signals += 1;
            }
            "BwdClose" => {
                summary.backward_close += 1;
                summary.active_signals += 1;
            }
            _ => {
                summary.neutral += 1;
            }
        }
    }

    summary
}

fn build_threshold_legends() -> Vec<FrDashboardThresholdLegend> {
    let factor = FundingRateFactor::instance();
    let mut items = Vec::new();

    for period in [
        FundingRatePeriod::Hours1,
        FundingRatePeriod::Hours2,
        FundingRatePeriod::Hours4,
        FundingRatePeriod::Hours6,
        FundingRatePeriod::Hours8,
    ] {
        push_threshold_legend(
            &mut items,
            factor.get_threshold_config(period, ArbDirection::Forward, OperationType::Open),
        );
        push_threshold_legend(
            &mut items,
            factor.get_threshold_config(period, ArbDirection::Forward, OperationType::Close),
        );
        push_threshold_legend(
            &mut items,
            factor.get_threshold_config(period, ArbDirection::Backward, OperationType::Open),
        );
        push_threshold_legend(
            &mut items,
            factor.get_threshold_config(period, ArbDirection::Backward, OperationType::Close),
        );
    }

    items
}

fn push_threshold_legend(
    items: &mut Vec<FrDashboardThresholdLegend>,
    cfg: Option<FrThresholdConfig>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    let signal = signal_name(cfg.arb_direction, cfg.operation);
    items.push(FrDashboardThresholdLegend {
        period: cfg.period.as_str().to_string(),
        signal: signal.to_string(),
        compare_op: compare_op_label(cfg.compare_op).to_string(),
        expression: rule_expression(signal).to_string(),
        threshold_pct: cfg.threshold * 100.0,
        tone: signal_tone_from_name(signal).to_string(),
    });
}

fn build_row(cfg: &FrDashboardConfig, symbol: &str) -> FrDashboardRow {
    let factor = FundingRateFactor::instance();
    let rate_fetcher = RateFetcher::instance();
    let mkt_channel = MktChannel::instance();
    let symbol_list = SymbolList::instance();

    let period = rate_fetcher.get_period(symbol, cfg.hedge_venue);
    let latest_fr = mkt_channel.get_latest_funding_rate(symbol, cfg.hedge_venue);
    let pred_fr = rate_fetcher
        .get_predicted_funding_rate(symbol, cfg.hedge_venue)
        .map(|(_, value)| value);
    let pred_loan = rate_fetcher
        .get_predict_loan_rate(symbol, cfg.hedge_venue)
        .map(|(_, value)| value);
    let cur_loan = rate_fetcher
        .get_current_loan_rate(symbol, cfg.hedge_venue)
        .map(|(_, value)| value);
    let fr_ma = mkt_channel.get_funding_rate_mean(symbol, cfg.hedge_venue);
    let fr_plus_pred_loan = pred_fr
        .zip(pred_loan)
        .map(|(fr, loan)| fr + loan * BWD_OPEN_LOAN_RATE_MULTIPLIER);
    let ma_plus_cur_loan = fr_ma.zip(cur_loan).map(|(fr, loan)| fr + loan);

    let signal = ArbDecision::evaluate_funding_rate_signal(symbol, cfg.hedge_venue)
        .ok()
        .flatten();
    let signal_name = signal.map(|item| item.as_str()).unwrap_or("-");

    let rules = vec![
        build_rule_state(
            "fwd_open",
            "FwdOpen",
            factor.get_threshold_config(period, ArbDirection::Forward, OperationType::Open),
            pred_fr,
            signal,
        ),
        build_rule_state(
            "fwd_close",
            "FwdClose",
            factor.get_threshold_config(period, ArbDirection::Forward, OperationType::Close),
            fr_ma,
            signal,
        ),
        build_rule_state(
            "bwd_open",
            "BwdOpen",
            factor.get_threshold_config(period, ArbDirection::Backward, OperationType::Open),
            fr_plus_pred_loan,
            signal,
        ),
        build_rule_state(
            "bwd_close",
            "BwdClose",
            factor.get_threshold_config(period, ArbDirection::Backward, OperationType::Close),
            ma_plus_cur_loan,
            signal,
        ),
    ];

    FrDashboardRow {
        symbol: symbol.to_string(),
        period: period.as_str().to_string(),
        signal: signal_name.to_string(),
        signal_tone: signal.map(signal_tone).unwrap_or("neutral").to_string(),
        lists: FrDashboardListFlags {
            in_dump: symbol_list.is_in_dump_list(symbol),
            in_forward_trade: symbol_list.is_in_fwd_trade_list(symbol),
            in_backward_trade: symbol_list.is_in_bwd_trade_list(symbol),
        },
        latest_fr_pct: to_pct(latest_fr),
        pred_fr_pct: to_pct(pred_fr),
        fr_ma_pct: to_pct(fr_ma),
        pred_loan_pct: to_pct(pred_loan),
        cur_loan_pct: to_pct(cur_loan),
        fr_plus_pred_loan_pct: to_pct(fr_plus_pred_loan),
        ma_plus_cur_loan_pct: to_pct(ma_plus_cur_loan),
        rules,
    }
}

fn build_rule_state(
    key: &str,
    label: &str,
    cfg: Option<FrThresholdConfig>,
    value: Option<f64>,
    active_signal: Option<ArbSignalKind>,
) -> FrDashboardRuleState {
    let compare_op = cfg.as_ref().map(|item| item.compare_op);
    let threshold = cfg.as_ref().map(|item| item.threshold);
    let hit = compare_op
        .zip(threshold)
        .zip(value)
        .map(|((op, thr), val)| op.check(val, thr))
        .unwrap_or(false);

    FrDashboardRuleState {
        key: key.to_string(),
        label: label.to_string(),
        compare_op: compare_op.map(|item| compare_op_label(item).to_string()),
        expression: rule_expression(label).to_string(),
        threshold_pct: threshold.map(|item| item * 100.0),
        value_pct: value.map(|item| item * 100.0),
        hit,
        active: active_signal.map(|item| item.as_str()) == Some(label),
        tone: signal_tone_from_name(label).to_string(),
    }
}

fn to_pct(value: Option<f64>) -> Option<f64> {
    value.map(|item| item * 100.0)
}

fn factor_mode_label(mode: FactorMode) -> &'static str {
    match mode {
        FactorMode::MM => "MM",
        FactorMode::MT => "MT",
    }
}

fn compare_op_label(op: CompareOp) -> &'static str {
    match op {
        CompareOp::GreaterThan => ">",
        CompareOp::LessThan => "<",
    }
}

fn signal_name(direction: ArbDirection, operation: OperationType) -> &'static str {
    match (direction, operation) {
        (ArbDirection::Forward, OperationType::Open) => "FwdOpen",
        (ArbDirection::Forward, OperationType::Close) => "FwdClose",
        (ArbDirection::Backward, OperationType::Open) => "BwdOpen",
        (ArbDirection::Backward, OperationType::Close) => "BwdClose",
        _ => "-",
    }
}

fn signal_tone(signal: ArbSignalKind) -> &'static str {
    signal_tone_from_name(signal.as_str())
}

fn signal_tone_from_name(signal: &str) -> &'static str {
    match signal {
        "FwdOpen" => "forward-open",
        "FwdClose" => "forward-close",
        "BwdOpen" => "backward-open",
        "BwdClose" => "backward-close",
        _ => "neutral",
    }
}

fn rule_expression(signal: &str) -> &'static str {
    match signal {
        "FwdOpen" => "Pred FR > Threshold",
        "FwdClose" => "FR MA < Threshold",
        "BwdOpen" => "Pred FR + Pred Loan x 1.2 < Threshold",
        "BwdClose" => "FR MA + Cur Loan > Threshold",
        _ => "-",
    }
}

async fn serve_http(cfg: FrDashboardConfig, hub: DashboardHub) -> Result<()> {
    let state = HttpState { hub };
    let index_html = render_index_html(&cfg.ws_path);

    let app = Router::new()
        .route(
            "/",
            get({
                let html = index_html.clone();
                move || {
                    let html = html.clone();
                    async move { Html(html) }
                }
            }),
        )
        .route(
            "/healthz",
            get(|| async {
                Json(serde_json::json!({"ok": true, "ts_ms": get_timestamp_us() / 1000}))
            }),
        )
        .route("/snapshot", get(snapshot_route))
        .route(&cfg.ws_path, get(ws_route))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse()?;
    info!(
        "fr_signal_dashboard listening at http://{}{}",
        addr, cfg.ws_path
    );
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn snapshot_route(AxumState(state): AxumState<HttpState>) -> impl IntoResponse {
    Json(state.hub.snapshot())
}

async fn ws_route(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<HttpState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_handler(socket, state.hub))
}

async fn ws_handler(mut socket: WebSocket, hub: DashboardHub) {
    if let Ok(msg) = serde_json::to_string(&hub.snapshot()) {
        if socket.send(Message::Text(msg.into())).await.is_err() {
            return;
        }
    }

    let mut rx = hub.subscribe();
    while let Ok(msg) = rx.recv().await {
        if socket.send(Message::Text(msg.into())).await.is_err() {
            break;
        }
    }
}

fn render_index_html(ws_path: &str) -> String {
    INDEX_HTML.replace("__WS_PATH__", ws_path)
}

const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>FR Signal Dashboard</title>
  <style>
    :root {
      --bg: #07111a;
      --bg-soft: #0d1b28;
      --panel: rgba(8, 20, 31, 0.84);
      --panel-strong: rgba(10, 24, 38, 0.96);
      --border: rgba(139, 168, 194, 0.18);
      --text: #edf6ff;
      --muted: #8ea8bc;
      --neutral: #50606e;
      --forward-open: #2db783;
      --forward-close: #ffb648;
      --backward-open: #36b5e8;
      --backward-close: #ff6b7d;
      --shadow: 0 24px 70px rgba(0, 0, 0, 0.38);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(45, 183, 131, 0.16), transparent 34%),
        radial-gradient(circle at top right, rgba(54, 181, 232, 0.14), transparent 28%),
        linear-gradient(180deg, #07111a 0%, #081722 48%, #050d13 100%);
      min-height: 100vh;
    }

    .shell {
      max-width: 1520px;
      margin: 0 auto;
      padding: 28px 20px 56px;
    }

    .hero {
      display: grid;
      gap: 18px;
      grid-template-columns: 1.45fr 1fr;
      margin-bottom: 18px;
    }

    .hero-card,
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(18px);
    }

    .hero-card {
      padding: 28px;
      position: relative;
      overflow: hidden;
    }

    .hero-card::after {
      content: "";
      position: absolute;
      right: -80px;
      top: -110px;
      width: 260px;
      height: 260px;
      border-radius: 999px;
      background: radial-gradient(circle, rgba(255, 182, 72, 0.22), transparent 65%);
      pointer-events: none;
    }

    .eyebrow {
      color: #9dc6de;
      letter-spacing: 0.18em;
      font-size: 12px;
      text-transform: uppercase;
      margin-bottom: 10px;
    }

    h1 {
      margin: 0;
      font-size: clamp(32px, 4vw, 54px);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }

    .subtitle {
      color: var(--muted);
      margin-top: 12px;
      max-width: 760px;
      line-height: 1.6;
    }

    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }

    .chip,
    .rule-chip,
    .legend-chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      border: 1px solid var(--border);
      padding: 8px 12px;
      font-size: 12px;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.03);
      white-space: nowrap;
    }

    .metrics {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      padding: 22px;
    }

    .metric-card {
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 18px;
    }

    .metric-card span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }

    .metric-card strong {
      display: block;
      margin-top: 10px;
      font-size: 34px;
      line-height: 1;
      letter-spacing: -0.05em;
    }

    .grid {
      display: grid;
      gap: 18px;
      grid-template-columns: 1fr;
    }

    .panel {
      padding: 20px;
    }

    .panel-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-end;
      margin-bottom: 14px;
    }

    .panel-head h2 {
      margin: 0;
      font-size: 20px;
      letter-spacing: -0.03em;
    }

    .panel-head p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
    }

    .legend,
    .thresholds {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .legend-chip::before,
    .signal-badge::before {
      content: "";
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
      background: var(--neutral);
      box-shadow: 0 0 0 6px rgba(255, 255, 255, 0.02);
    }

    .tone-forward-open,
    .signal-badge.tone-forward-open,
    .legend-chip.tone-forward-open { color: #dffbef; }
    .tone-forward-open::before,
    .signal-badge.tone-forward-open::before,
    .legend-chip.tone-forward-open::before { background: var(--forward-open); }

    .tone-forward-close,
    .signal-badge.tone-forward-close,
    .legend-chip.tone-forward-close { color: #ffe4b2; }
    .tone-forward-close::before,
    .signal-badge.tone-forward-close::before,
    .legend-chip.tone-forward-close::before { background: var(--forward-close); }

    .tone-backward-open,
    .signal-badge.tone-backward-open,
    .legend-chip.tone-backward-open { color: #d7f5ff; }
    .tone-backward-open::before,
    .signal-badge.tone-backward-open::before,
    .legend-chip.tone-backward-open::before { background: var(--backward-open); }

    .tone-backward-close,
    .signal-badge.tone-backward-close,
    .legend-chip.tone-backward-close { color: #ffd7de; }
    .tone-backward-close::before,
    .signal-badge.tone-backward-close::before,
    .legend-chip.tone-backward-close::before { background: var(--backward-close); }

    .table-wrap {
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: var(--panel-strong);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1350px;
    }

    thead th {
      position: sticky;
      top: 0;
      z-index: 2;
      background: #0c1b28;
      color: #9eb9cf;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      font-size: 11px;
    }

    th, td {
      padding: 14px 12px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
      text-align: left;
      vertical-align: top;
    }

    tbody tr:hover {
      background: rgba(255, 255, 255, 0.025);
    }

    .symbol-cell strong {
      display: block;
      font-size: 15px;
      letter-spacing: 0.02em;
    }

    .mini {
      color: var(--muted);
      font-size: 12px;
      margin-top: 6px;
    }

    .signal-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      border: 1px solid var(--border);
      padding: 7px 12px;
      background: rgba(255, 255, 255, 0.03);
      font-size: 12px;
      white-space: nowrap;
    }

    .metric-value {
      font-variant-numeric: tabular-nums;
      font-size: 14px;
      padding: 8px 10px;
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.02);
      display: inline-block;
      min-width: 92px;
    }

    .metric-value.tone-forward-open {
      color: #dffbef;
      background: rgba(45, 183, 131, 0.14);
      box-shadow: inset 0 0 0 1px rgba(45, 183, 131, 0.24);
    }

    .metric-value.tone-forward-close {
      color: #ffe4b2;
      background: rgba(255, 182, 72, 0.16);
      box-shadow: inset 0 0 0 1px rgba(255, 182, 72, 0.26);
    }

    .metric-value.tone-backward-open {
      color: #d7f5ff;
      background: rgba(54, 181, 232, 0.16);
      box-shadow: inset 0 0 0 1px rgba(54, 181, 232, 0.24);
    }

    .metric-value.tone-backward-close {
      color: #ffd7de;
      background: rgba(255, 107, 125, 0.16);
      box-shadow: inset 0 0 0 1px rgba(255, 107, 125, 0.22);
    }

    .rule-stack {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      min-width: 320px;
    }

    .rule-chip {
      padding: 7px 10px;
      font-size: 11px;
    }

    .rule-chip.active {
      background: rgba(255, 255, 255, 0.08);
    }

    .rule-chip.hit.tone-forward-open {
      border-color: rgba(45, 183, 131, 0.34);
      color: #dffbef;
      background: rgba(45, 183, 131, 0.12);
    }

    .rule-chip.hit.tone-forward-close {
      border-color: rgba(255, 182, 72, 0.34);
      color: #ffe4b2;
      background: rgba(255, 182, 72, 0.12);
    }

    .rule-chip.hit.tone-backward-open {
      border-color: rgba(54, 181, 232, 0.34);
      color: #d7f5ff;
      background: rgba(54, 181, 232, 0.12);
    }

    .rule-chip.hit.tone-backward-close {
      border-color: rgba(255, 107, 125, 0.34);
      color: #ffd7de;
      background: rgba(255, 107, 125, 0.12);
    }

    .status {
      color: var(--muted);
      font-size: 13px;
    }

    @media (max-width: 1080px) {
      .hero {
        grid-template-columns: 1fr;
      }
      .metrics {
        grid-template-columns: 1fr 1fr;
      }
    }

    @media (max-width: 720px) {
      .shell {
        padding: 18px 14px 42px;
      }
      .metrics {
        grid-template-columns: 1fr;
      }
      .hero-card,
      .panel {
        border-radius: 18px;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <article class="hero-card">
        <div class="eyebrow">Realtime Funding Rate Monitor</div>
        <h1>FR Signal Dashboard</h1>
        <div class="subtitle">
          Mirror the FR branch in <code>trade_signal</code>, keep it read-only, and push live symbol rows over websocket.
        </div>
        <div class="meta" id="meta"></div>
      </article>
      <article class="hero-card">
        <div class="metrics">
          <div class="metric-card">
            <span>Tracked Symbols</span>
            <strong id="totalSymbols">0</strong>
          </div>
          <div class="metric-card">
            <span>Active Signals</span>
            <strong id="activeSignals">0</strong>
          </div>
          <div class="metric-card">
            <span>Last Update</span>
            <strong id="lastUpdate">--</strong>
          </div>
          <div class="metric-card">
            <span>Mode</span>
            <strong id="factorMode">--</strong>
          </div>
          <div class="metric-card">
            <span>Forward Open</span>
            <strong id="fwdOpenCount">0</strong>
          </div>
          <div class="metric-card">
            <span>Backward Open</span>
            <strong id="bwdOpenCount">0</strong>
          </div>
        </div>
      </article>
    </section>

    <section class="grid">
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Signal Legend</h2>
            <p>Colors map directly to the active FR signal and the metric that triggered it.</p>
          </div>
          <div class="status" id="socketStatus">socket: connecting</div>
        </div>
        <div class="legend">
          <span class="legend-chip tone-forward-open">FwdOpen</span>
          <span class="legend-chip tone-forward-close">FwdClose</span>
          <span class="legend-chip tone-backward-open">BwdOpen</span>
          <span class="legend-chip tone-backward-close">BwdClose</span>
          <span class="legend-chip">Neutral</span>
        </div>
      </article>

      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Thresholds</h2>
            <p>Loaded from the same FR threshold source as <code>trade_signal</code>.</p>
          </div>
        </div>
        <div class="thresholds" id="thresholds"></div>
      </article>

      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Per Symbol Snapshot</h2>
            <p>Focus on funding-rate fields only: current FR, predicted FR, FR MA, predicted loan, current loan, and combined factors.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Signal</th>
                <th>Latest FR %</th>
                <th>Pred FR %</th>
                <th>FR MA %</th>
                <th>Pred Loan %</th>
                <th>Cur Loan %</th>
                <th>FR+PLoan %</th>
                <th>MA+CLoan %</th>
                <th>Rules</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
      </article>
    </section>
  </div>

  <script>
    const WS_PATH = "__WS_PATH__";
    const toneMap = {
      FwdOpen: "tone-forward-open",
      FwdClose: "tone-forward-close",
      BwdOpen: "tone-backward-open",
      BwdClose: "tone-backward-close",
    };

    function formatPct(value) {
      return value === null || value === undefined ? "--" : value.toFixed(4);
    }

    function formatTs(tsMs) {
      if (!tsMs) return "--";
      return new Date(tsMs).toISOString().replace("T", " ").replace("Z", " UTC");
    }

    function metricTone(metric, signal) {
      const active = {
        FwdOpen: ["pred_fr_pct"],
        FwdClose: ["fr_ma_pct"],
        BwdOpen: ["pred_loan_pct", "fr_plus_pred_loan_pct"],
        BwdClose: ["cur_loan_pct", "ma_plus_cur_loan_pct"],
      };
      return (active[signal] || []).includes(metric) ? (toneMap[signal] || "") : "";
    }

    function renderMeta(snapshot) {
      const meta = document.getElementById("meta");
      meta.innerHTML = [
        ["exchange", snapshot.exchange],
        ["mode", snapshot.factor_mode],
        ["namespace", snapshot.symbol_namespace],
        ["suffix", snapshot.symbol_key_suffix],
        ["venues", `${snapshot.open_venue} -> ${snapshot.hedge_venue}`],
      ]
        .map(([k, v]) => `<span class="chip"><strong>${k}</strong> ${v}</span>`)
        .join("");
    }

    function renderThresholds(snapshot) {
      const root = document.getElementById("thresholds");
      root.innerHTML = snapshot.thresholds.map(item => `
        <span class="legend-chip ${item.tone}">
          ${item.period} ${item.signal}
          <strong>${item.expression}</strong>
          <strong>${item.compare_op} ${formatPct(item.threshold_pct)}%</strong>
        </span>
      `).join("");
    }

    function renderRows(snapshot) {
      const root = document.getElementById("rows");
      root.innerHTML = snapshot.rows.map(row => {
        const signalTone = toneMap[row.signal] || "";
        const listFlags = [];
        if (row.lists.in_forward_trade) listFlags.push("FWD");
        if (row.lists.in_backward_trade) listFlags.push("BWD");
        if (row.lists.in_dump) listFlags.push("DUMP");
        const rules = row.rules.map(rule => `
          <span class="rule-chip ${rule.hit ? `hit ${rule.tone}` : ""} ${rule.active ? "active" : ""}">
            ${rule.label}
            ${rule.value_pct === null || rule.value_pct === undefined ? "--" : rule.value_pct.toFixed(4)}%
            ${rule.compare_op || ""}
            ${rule.threshold_pct === null || rule.threshold_pct === undefined ? "--" : rule.threshold_pct.toFixed(4)}%
          </span>
        `).join("");

        return `
          <tr>
            <td class="symbol-cell">
              <strong>${row.symbol}</strong>
              <div class="mini">${row.period} ${listFlags.join(" / ") || "UNLISTED"}</div>
            </td>
            <td><span class="signal-badge ${signalTone}">${row.signal}</span></td>
            <td><span class="metric-value ${metricTone("latest_fr_pct", row.signal)}">${formatPct(row.latest_fr_pct)}</span></td>
            <td><span class="metric-value ${metricTone("pred_fr_pct", row.signal)}">${formatPct(row.pred_fr_pct)}</span></td>
            <td><span class="metric-value ${metricTone("fr_ma_pct", row.signal)}">${formatPct(row.fr_ma_pct)}</span></td>
            <td><span class="metric-value ${metricTone("pred_loan_pct", row.signal)}">${formatPct(row.pred_loan_pct)}</span></td>
            <td><span class="metric-value ${metricTone("cur_loan_pct", row.signal)}">${formatPct(row.cur_loan_pct)}</span></td>
            <td><span class="metric-value ${metricTone("fr_plus_pred_loan_pct", row.signal)}">${formatPct(row.fr_plus_pred_loan_pct)}</span></td>
            <td><span class="metric-value ${metricTone("ma_plus_cur_loan_pct", row.signal)}">${formatPct(row.ma_plus_cur_loan_pct)}</span></td>
            <td><div class="rule-stack">${rules}</div></td>
          </tr>
        `;
      }).join("");
    }

    function renderSnapshot(snapshot) {
      if (!snapshot) return;
      document.getElementById("totalSymbols").textContent = snapshot.summary.total_symbols;
      document.getElementById("activeSignals").textContent = snapshot.summary.active_signals;
      document.getElementById("lastUpdate").textContent = formatTs(snapshot.ts_ms);
      document.getElementById("factorMode").textContent = snapshot.factor_mode;
      document.getElementById("fwdOpenCount").textContent = snapshot.summary.forward_open;
      document.getElementById("bwdOpenCount").textContent = snapshot.summary.backward_open;
      renderMeta(snapshot);
      renderThresholds(snapshot);
      renderRows(snapshot);
    }

    async function loadSnapshot() {
      const response = await fetch("/snapshot", { cache: "no-store" });
      if (!response.ok) throw new Error(`snapshot http ${response.status}`);
      return response.json();
    }

    function connectSocket() {
      const scheme = window.location.protocol === "https:" ? "wss" : "ws";
      const url = `${scheme}://${window.location.host}${WS_PATH}`;
      const status = document.getElementById("socketStatus");
      const socket = new WebSocket(url);

      socket.onopen = () => {
        status.textContent = "socket: live";
      };

      socket.onmessage = event => {
        const data = JSON.parse(event.data);
        if (data.type === "fr_dashboard") {
          renderSnapshot(data);
        }
      };

      socket.onclose = () => {
        status.textContent = "socket: reconnecting";
        setTimeout(connectSocket, 1200);
      };

      socket.onerror = () => {
        status.textContent = "socket: error";
      };
    }

    loadSnapshot().then(renderSnapshot).catch(() => {
      document.getElementById("socketStatus").textContent = "socket: waiting for snapshot";
    });
    connectSocket();
  </script>
</body>
</html>
"#;
