use account_monitor_common::pm_forwarder::PmForwarder;
use anyhow::{bail, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use mkt_parsers::msg::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
};
use runtime_common::exchange::Exchange;
use runtime_common::execution_backend::{account_stream_slug, ExecBackend};
use runtime_common::ws_connection::WsConnector;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use trade_engine::ltp_account::{parse_account_push, parse_order_push, PositionSnapshotState};
use trade_engine::ltp_finance::PortfolioFinancialSnapshot;
use trade_engine::ltp_journal::{write_snapshot, AccountJournal};
use trade_engine::ltp_rest::LtpRestClient;
use trade_engine::ltp_snapshot::{AccountSnapshotState, RecoveryReadiness};
use trade_engine::ltp_ws::{LtpCredentials, LtpWsResponse, DEFAULT_WS_URL};

#[derive(Parser)]
struct Args {
    #[arg(long, value_parser = ["binance", "okex"])]
    exchange: String,
    #[arg(long)]
    local_ip: Option<String>,
    #[arg(long, default_value = "data/rapidx_account")]
    journal_dir: PathBuf,
    #[arg(long)]
    core: Option<usize>,
    /// Initial execution recovery window; subsequent runs resume the durable checkpoint.
    #[arg(long, default_value_t = 24, value_parser = clap::value_parser!(u32).range(1..=2136))]
    history_lookback_hours: u32,
}

struct Forwarder {
    ipc: PmForwarder,
    journal: AccountJournal,
    portfolio: String,
    exchange: &'static str,
    scope: BasicAccountScope,
    positions: PositionSnapshotState,
    assets: AccountSnapshotState,
    readiness: RecoveryReadiness,
    financial: PortfolioFinancialSnapshot,
    financial_path: PathBuf,
    last_risk_timestamp: i64,
}

impl Forwarder {
    fn process(
        &mut self,
        payload: &str,
        source: &str,
        snapshot_started_ms: Option<i64>,
    ) -> Result<()> {
        let value: Value = serde_json::from_str(payload).context("decode RapidX private push")?;
        let Some(channel) = value.get("channel").and_then(Value::as_str) else {
            return Ok(());
        };
        self.journal.record_message(&value, source)?;
        if matches!(channel, "Orders" | "Trades")
            && value["data"]["exchangeType"]
                .as_str()
                .is_some_and(|source| source != self.exchange)
        {
            return Ok(());
        }
        if channel == "Trades" {
            self.journal.record_execution(&value["data"], false)?;
            return Ok(());
        }
        let observed_ms = chrono::Utc::now().timestamp_millis();
        self.financial.apply_account_push(
            payload,
            source == "rest_snapshot" && channel == "Assets",
            snapshot_started_ms.unwrap_or(observed_ms),
        )?;
        let mut events = if channel == "Orders" {
            parse_order_push(payload, &self.portfolio, self.exchange)?
                .into_iter()
                .collect()
        } else {
            parse_account_push(payload, &self.portfolio, self.exchange)?
        };
        self.positions.reconcile(
            &mut events,
            source == "rest_snapshot" && channel == "Positions",
            snapshot_started_ms.unwrap_or(observed_ms),
        )?;
        if channel == "Assets" {
            events = self.assets.reconcile_assets_snapshot(
                self.scope,
                source == "rest_snapshot",
                &events,
                snapshot_started_ms.unwrap_or(observed_ms),
            )?;
        }
        if source == "rest_snapshot" {
            self.readiness.mark_complete(channel);
        }
        for event in events.drain(..) {
            if let Some((BasicAccountEventType::AccountRisk, _, data)) =
                mkt_parsers::msg::basic_account_msg::split_basic_account_event(&event)
            {
                let risk = BasicAccountRiskMsg::from_bytes(data)?;
                if risk.timestamp < self.last_risk_timestamp {
                    continue;
                }
                self.last_risk_timestamp = risk.timestamp;
            }
            if mkt_parsers::msg::basic_account_msg::split_basic_account_event(&event)
                .is_some_and(|(kind, _, _)| kind == BasicAccountEventType::AccountRisk)
                && (!self.readiness.is_ready()
                    || self.financial.loan_status.as_deref() != Some("NORMAL")
                    || self
                        .financial
                        .raw_account
                        .as_ref()
                        .and_then(|row| row["accountStatus"].as_str())
                        != Some("NORMAL")
                    || (channel == "MarginCall"
                        && value["data"]["accountStatus"].as_str() != Some("NORMAL")))
            {
                self.publish_invalid_risk()?;
                continue;
            }
            if !self.ipc.send_raw(&event) {
                bail!("RapidX account IPC delivery failed");
            }
        }
        self.write_financial()?;
        Ok(())
    }

    fn invalidate(&mut self) -> Result<()> {
        self.readiness = RecoveryReadiness::default();
        self.publish_invalid_risk()?;
        self.write_financial()?;
        self.journal.ensure_healthy()
    }

    fn write_financial(&self) -> Result<()> {
        write_snapshot(
            &self.financial_path,
            &json!({
                "recovery_ready":self.readiness.is_ready(),
                "history_end_ms":self.journal.history_end_ms,
                "financial":self.financial
            }),
        )
    }

    fn publish_invalid_risk(&mut self) -> Result<()> {
        let risk = BasicAccountRiskMsg::ratio_only(chrono::Utc::now().timestamp_millis(), 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::AccountRisk,
            self.scope,
            risk.to_bytes(),
        )
        .to_bytes();
        if !self.ipc.send_raw(&event) {
            bail!("RapidX account invalidation could not be delivered");
        }
        Ok(())
    }

    fn apply_refresh(&mut self, refresh: Refresh) -> Result<()> {
        match refresh {
            Refresh::Account(payload, started_ms) => {
                self.process(&payload, "rest_snapshot", Some(started_ms))?
            }
            Refresh::Financial(channel, response) => {
                self.journal.record_message(
                    &json!({"channel":channel,"data":response}),
                    "rest_financial",
                )?;
                let now = chrono::Utc::now().timestamp_millis();
                if channel == "LoanInfo" {
                    self.financial.apply_loan_info(&response, now)?;
                    if self.financial.loan_status.as_deref() != Some("NORMAL") {
                        self.publish_invalid_risk()?;
                    }
                } else {
                    self.financial.apply_loan_capacity(&response, now)?;
                }
                self.readiness.mark_complete(channel);
            }
            Refresh::History(end, rows) => {
                self.journal.record_history(&rows)?;
                self.journal.complete_history(end)?;
                // Catch-up windows are complete individually, but the account is
                // not recovered until the checkpoint reaches the recent tail.
                if chrono::Utc::now().timestamp_millis() - end < 60_000 {
                    self.readiness.mark_complete("Trades");
                }
            }
        }
        self.write_financial()
    }
}

enum Refresh {
    Account(String, i64),
    Financial(&'static str, Value),
    History(i64, Vec<Value>),
}

async fn refresh_accounts(
    rest: LtpRestClient,
    exchange: &'static str,
    tx: tokio::sync::mpsc::Sender<Result<Refresh>>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut index = 0usize;
    loop {
        tick.tick().await;
        let started_ms = chrono::Utc::now().timestamp_millis();
        let result = match index {
            0..=2 => {
                let channel = ["Assets", "Positions", "Accounts"][index];
                match tokio::time::timeout(
                    Duration::from_secs(20),
                    rest.fetch_account_push(channel, exchange),
                )
                .await
                {
                    Ok(result) => result.map(|value| Refresh::Account(value, started_ms)),
                    Err(error) => Err(error).context("RapidX complete account snapshot deadline"),
                }
            }
            3 => rest
                .fetch_loan_info()
                .await
                .map(|value| Refresh::Financial("LoanInfo", value)),
            _ => rest
                .fetch_loan_capacity(exchange)
                .await
                .map(|value| Refresh::Financial("LoanCapacity", value)),
        };
        let failed = result.is_err();
        if tx.send(result).await.is_err() || failed {
            return;
        }
        index = (index + 1) % 5;
    }
}

async fn refresh_history(
    rest: LtpRestClient,
    exchange: &'static str,
    start_ms: i64,
    tx: tokio::sync::mpsc::Sender<Result<Refresh>>,
) {
    let mut begin = start_ms;
    loop {
        let now = chrono::Utc::now().timestamp_millis();
        if begin < now - 90 * 86_400_000 {
            let _ = tx
                .send(Err(anyhow::anyhow!(
                    "RapidX checkpoint predates 90-day archive; refusing to skip missing history"
                )))
                .await;
            return;
        }
        let end = (begin + 3_600_000).min(now);
        let result = rest
            .fetch_transaction_history(exchange, begin, end)
            .await
            .map(|rows| Refresh::History(end, rows));
        let failed = result.is_err();
        if tx.send(result).await.is_err() || failed {
            return;
        }
        begin = (end - 60_000).max(start_ms);
        tokio::time::sleep(if end == now {
            Duration::from_secs(30)
        } else {
            Duration::from_millis(2100)
        })
        .await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::init();
    runtime_common::affinity::maybe_pin_current_thread(args.core, "ACCOUNT_MONITOR_CORE")?;
    let exchange = Exchange::from_str(&args.exchange).context("unsupported exchange")?;
    if ExecBackend::for_exchange(exchange)? != ExecBackend::Ltp {
        bail!("rapidx_account_monitor requires the RapidX execution backend for its exchange");
    }
    let local_ip = match args.local_ip {
        Some(ip) => ip,
        None if runtime_common::mkt_cfg::find_trade_engine_local_cfg_path()?.is_some() => {
            runtime_common::mkt_cfg::load_primary_local_ip_from_trade_engine_sync()?.0
        }
        None => "0.0.0.0".into(),
    };
    let rest = LtpRestClient::from_env_with_local_ip(Some(
        local_ip
            .parse()
            .context("invalid account monitor source IP")?,
    ))?;
    let credentials = LtpCredentials::from_env()?;
    let slug = account_stream_slug(exchange)?;
    let dir = args.journal_dir.join(&slug);
    std::fs::create_dir_all(&dir).context("create RapidX account journal directory")?;
    let wire_exchange = if exchange == Exchange::Binance {
        "BINANCE"
    } else {
        "OKX"
    };
    let mut positions = PositionSnapshotState::default();
    let mut assets = AccountSnapshotState::default();
    // Restore identities, not readiness or balances. Only fresh REST snapshots
    // may clear an absent balance/position and make this session ready.
    let journal = AccountJournal::open(&dir, rest.portfolio_id(), wire_exchange, |message| {
        let channel = message.get("channel").and_then(Value::as_str);
        if matches!(channel, Some("Assets" | "Positions")) {
            let mut events =
                parse_account_push(&message.to_string(), rest.portfolio_id(), wire_exchange)?;
            if channel == Some("Assets") {
                for event in events {
                    assets.observe(&event)?;
                }
            } else {
                positions.reconcile(&mut events, false, 0)?;
            }
        }
        Ok(())
    })?;
    let mut forwarder = Forwarder {
        ipc: PmForwarder::new_non_overflowing(&slug)?,
        journal,
        portfolio: rest.portfolio_id().to_string(),
        exchange: wire_exchange,
        scope: if exchange == Exchange::Binance {
            BasicAccountScope::BinanceUnified
        } else {
            BasicAccountScope::OkexUnified
        },
        positions,
        assets,
        readiness: RecoveryReadiness::default(),
        financial: PortfolioFinancialSnapshot::new(
            rest.portfolio_id().into(),
            wire_exchange.into(),
        )?,
        financial_path: dir.join("latest_financial.json"),
        last_risk_timestamp: 0,
    };
    let url = std::env::var("LTP_WS_URL").unwrap_or_else(|_| DEFAULT_WS_URL.to_string());
    loop {
        forwarder.invalidate()?;
        let connection = tokio::time::timeout(
            Duration::from_secs(15),
            WsConnector::connect_with_local_ip_raw(&url, &local_ip),
        )
        .await;
        if let Ok(Ok(connection)) = connection {
            let mut ws = connection.ws_stream.lock().await;
            let mut workers = Vec::new();
            let session: Result<()> = async {
                ws.send(Message::Text(credentials.build_login_payload(false)?)).await?;
                let login = tokio::time::timeout(Duration::from_secs(10), ws.next()).await?
                    .context("RapidX login connection closed")??;
                let response = LtpWsResponse::from_json_str(login.to_text()?).context("invalid RapidX login response")?;
                if !response.is_login() || !response.is_success() { bail!("RapidX account login rejected"); }
                let (tx, mut rx) = tokio::sync::mpsc::channel(8);
                let initial_begin = chrono::Utc::now().timestamp_millis() - i64::from(args.history_lookback_hours) * 3_600_000;
                let begin = forwarder.journal.history_end_ms.map(|end| end - 60_000).unwrap_or(initial_begin);
                workers.push(tokio::spawn(refresh_accounts(rest.clone(), wire_exchange, tx.clone())));
                workers.push(tokio::spawn(refresh_history(rest.clone(), wire_exchange, begin, tx)));
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut waiting_pong = false;
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => { return Ok(()); }
                        message = ws.next() => {
                            match message.context("RapidX account disconnected")?? {
                                Message::Text(text) => {
                                    if trade_engine::ltp_ws::is_text_pong(&text) { waiting_pong = false; }
                                    else { forwarder.process(&text, "ws", None)?; }
                                }
                                Message::Binary(bytes) => {
                                    let text = std::str::from_utf8(&bytes)?;
                                    if trade_engine::ltp_ws::is_text_pong(text) { waiting_pong = false; }
                                    else { forwarder.process(text, "ws", None)?; }
                                },
                                Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
                                Message::Close(_) => bail!("RapidX account websocket closed"),
                                _ => {}
                            }
                        }
                        update = rx.recv() => forwarder.apply_refresh(update.context("RapidX recovery worker stopped")??)?,
                        _ = heartbeat.tick() => {
                            if waiting_pong { bail!("RapidX account heartbeat expired"); }
                            ws.send(Message::Text("ping".into())).await?;
                            waiting_pong = true;
                        }
                    }
                }
            }.await;
            for worker in workers {
                worker.abort();
                let _ = worker.await;
            }
            forwarder.invalidate()?;
            let _ = ws.close(None).await;
            if session.is_ok() {
                break;
            }
            log::warn!(
                "RapidX account session ended; refreshing on reconnect: {:#}",
                session.unwrap_err()
            );
        } else {
            log::warn!("RapidX account connection unavailable");
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(Duration::from_secs(3)) => {}
        }
    }
    Ok(())
}
