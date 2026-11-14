use account_monitor_common::pm_forwarder::PmForwarder;
use anyhow::{bail, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use mkt_parsers::msg::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
};
use persist_common::{
    rapidx_execution::{ExecutionEvidence, ExecutionRecord},
    rapidx_role::ExecutionRoleRecord,
    rapidx_statement::StatementRecord,
};
use runtime_common::exchange::Exchange;
use runtime_common::execution_backend::{account_stream_slug, ExecBackend};
use runtime_common::ws_connection::WsConnector;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use trade_engine::ltp_account::{parse_account_push, parse_order_push, PositionSnapshotState};
use trade_engine::ltp_delivery::Delivery;
use trade_engine::ltp_finance::PortfolioFinancialSnapshot;
use trade_engine::ltp_outbox::{DeliveryStore, Fact, Identity};
use trade_engine::ltp_rest::LtpRestClient;
use trade_engine::ltp_snapshot::{AccountSnapshotState, RecoveryReadiness};
use trade_engine::ltp_statement::STATEMENT_REQUEST_INTERVAL;
use trade_engine::ltp_ws::{LtpCredentials, LtpWsResponse, DEFAULT_WS_URL};

#[derive(Parser)]
struct Args {
    #[arg(long, value_parser = ["binance", "okex"])]
    exchange: String,
    #[arg(long)]
    local_ip: Option<String>,
    #[arg(long, default_value = "data/rapidx_delivery")]
    delivery_dir: PathBuf,
    /// Import legacy facts without exchange access; rerun after stopping the old monitor to finalize its tail.
    #[arg(long)]
    import_legacy: Option<PathBuf>,
    #[arg(long, requires = "import_legacy")]
    dry_run: bool,
    #[arg(long)]
    core: Option<usize>,
}

struct Forwarder {
    ipc: PmForwarder,
    delivery: Delivery,
    portfolio: String,
    exchange: &'static str,
    scope: BasicAccountScope,
    positions: PositionSnapshotState,
    assets: AccountSnapshotState,
    readiness: RecoveryReadiness,
    financial: PortfolioFinancialSnapshot,
    last_risk_timestamp: i64,
}

fn decode_private_push(payload: &str) -> Result<Option<Value>> {
    if payload.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(payload)
        .with_context(|| {
            format!(
                "decode RapidX private push len={} first_byte={:?}",
                payload.len(),
                payload.as_bytes().first().copied()
            )
        })
        .map(Some)
}

impl Forwarder {
    async fn process(
        &mut self,
        payload: &str,
        source: &str,
        snapshot_started_ms: Option<i64>,
    ) -> Result<()> {
        let Some(value) = decode_private_push(payload)? else {
            return Ok(());
        };
        let Some(channel) = value.get("channel").and_then(Value::as_str) else {
            return Ok(());
        };
        if matches!(channel, "Orders" | "Trades")
            && value["data"]["exchangeType"]
                .as_str()
                .is_some_and(|source| source != self.exchange)
        {
            return Ok(());
        }
        if channel == "Trades" {
            let record = ExecutionRecord {
                portfolio: self.portfolio.clone(),
                exchange: self.exchange.into(),
                execution: ExecutionEvidence::parse(
                    &value["data"],
                    &self.portfolio,
                    self.exchange,
                    false,
                )?,
            };
            let mut facts = vec![Fact::Execution(record.clone())];
            if let Some(role) = ExecutionRoleRecord::from_row(record, &value["data"])? {
                facts.push(Fact::Role(role));
            }
            self.delivery.save(facts, vec![], None).await?;
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
        let identities = events
            .iter()
            .map(|e| Identity::from_event(e))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        self.delivery.save(vec![], identities, None).await?;
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
        if source == "ws" && matches!(channel, "Accounts" | "MarginCall") {
            self.publish_financial()?;
        }
        Ok(())
    }

    fn invalidate(&mut self) -> Result<()> {
        self.readiness = RecoveryReadiness::default();
        self.publish_invalid_risk()?;
        self.publish_financial()?;
        Ok(())
    }

    fn publish_financial(&mut self) -> Result<()> {
        let pending = &self.delivery.store.state().pending;
        let body = serde_json::to_vec(&json!({
            "portfolio":self.portfolio,"exchange":self.exchange,
            "published_ms":chrono::Utc::now().timestamp_millis(),
            "recovery_ready":self.readiness.is_ready(),
            "accounts_observed_ms":self.financial.source_observed_ms.get("Accounts"),
            "frozen_margin":self.financial.frozen_margin,
            "execution_persistence_pending":pending.values().filter(|f| matches!(f, Fact::Execution(_) | Fact::Role(_))).count(),
            "statement_persistence_pending":pending.values().filter(|f| matches!(f, Fact::Statement(_))).count(),
            "statement_history_end_ms":self.delivery.store.state().statement_history_end_ms,
            "execution_history_end_ms":self.delivery.store.state().execution_history_end_ms,
        }))?;
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::RapidXFinancial,
            self.scope,
            body.into(),
        )
        .to_bytes();
        if !self.ipc.send_raw(&event) {
            bail!("RapidX financial IPC delivery failed");
        }
        Ok(())
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

    async fn apply_refresh(&mut self, refresh: Refresh) -> Result<()> {
        match refresh {
            Refresh::Account(payload, started_ms) => {
                self.process(&payload, "rest_snapshot", Some(started_ms))
                    .await?
            }
            Refresh::Financial(channel, response) => {
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
            Refresh::Executions(end, rows) => {
                for chunk in rows.chunks(64) {
                    let mut facts = vec![];
                    for row in chunk {
                        let record = ExecutionRecord {
                            portfolio: self.portfolio.clone(),
                            exchange: self.exchange.into(),
                            execution: ExecutionEvidence::parse(
                                row,
                                &self.portfolio,
                                self.exchange,
                                true,
                            )?,
                        };
                        facts.push(Fact::Execution(record.clone()));
                        if let Some(role) = ExecutionRoleRecord::from_row(record, row)? {
                            facts.push(Fact::Role(role));
                        }
                    }
                    self.delivery.save(facts, vec![], None).await?;
                }
                self.delivery.store.execution_checkpoint(end).await?;
                if chrono::Utc::now().timestamp_millis() - end < 60_000 {
                    self.readiness.mark_complete("Executions");
                }
            }
            Refresh::Statements(end, rows) => {
                for chunk in rows.chunks(128) {
                    self.delivery
                        .save(
                            chunk.iter().cloned().map(Fact::Statement).collect(),
                            vec![],
                            None,
                        )
                        .await?;
                }
                self.delivery.save(vec![], vec![], Some(end)).await?;
                if chrono::Utc::now().timestamp_millis() - end < 60_000 {
                    self.readiness.mark_complete("Statements");
                }
            }
        }
        self.publish_financial()
    }
}

enum Refresh {
    Account(String, i64),
    Financial(&'static str, Value),
    Statements(i64, Vec<StatementRecord>),
    Executions(i64, Vec<Value>),
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

async fn refresh_statements(
    rest: LtpRestClient,
    exchange: &'static str,
    start_ms: i64,
    tx: tokio::sync::mpsc::Sender<Result<Refresh>>,
) {
    let mut begin = start_ms;
    loop {
        let now = chrono::Utc::now().timestamp_millis();
        let end = (begin + 3_600_000).min(now);
        let result = rest
            .fetch_statement_history(exchange, begin, end)
            .await
            .map(|rows| Refresh::Statements(end, rows));
        let failed = result.is_err();
        if tx.send(result).await.is_err() || failed {
            return;
        }
        begin = (end - 60_000).max(start_ms);
        tokio::time::sleep(if end == now {
            Duration::from_secs(30)
        } else {
            STATEMENT_REQUEST_INTERVAL
        })
        .await;
    }
}

async fn refresh_executions(
    rest: LtpRestClient,
    exchange: &'static str,
    start_ms: i64,
    tx: tokio::sync::mpsc::Sender<Result<Refresh>>,
) {
    let mut begin = start_ms;
    loop {
        let now = chrono::Utc::now().timestamp_millis();
        let end = (begin + 3_600_000).min(now);
        let result = rest
            .fetch_transaction_history(exchange, begin, end)
            .await
            .map(|rows| Refresh::Executions(end, rows));
        let failed = result.is_err();
        if tx.send(result).await.is_err() || failed {
            return;
        }
        begin = (end - 60_000).max(start_ms);
        tokio::time::sleep(if end == now {
            Duration::from_secs(30)
        } else {
            STATEMENT_REQUEST_INTERVAL
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
    let dir = args.delivery_dir.join(&slug);
    let wire_exchange = if exchange == Exchange::Binance {
        "BINANCE"
    } else {
        "OKX"
    };
    let mut positions = PositionSnapshotState::default();
    let mut assets = AccountSnapshotState::default();
    if args.dry_run {
        trade_engine::ltp_legacy::validate(
            args.import_legacy
                .as_deref()
                .context("missing legacy import directory")?,
            rest.portfolio_id(),
            wire_exchange,
        )?;
        return Ok(());
    }
    let mut delivery = Delivery::new(DeliveryStore::open(
        &dir,
        rest.portfolio_id(),
        wire_exchange,
    )?)?;
    if let Some(legacy) = args.import_legacy {
        trade_engine::ltp_legacy::import(&legacy, &mut delivery).await?;
        delivery.drain(Duration::from_secs(120)).await?;
        log::info!("RapidX legacy import acknowledged; original archives retained");
        return Ok(());
    }
    // Recover only identities. Fresh complete REST snapshots supply all values.
    for identity in &delivery.store.state().identities {
        let event = identity.event()?;
        assets.observe(&event)?;
        positions.reconcile(&mut vec![event], false, 0)?;
    }
    let mut forwarder = Forwarder {
        ipc: PmForwarder::new_non_overflowing(&slug)?,
        delivery,
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
        last_risk_timestamp: 0,
    };
    let url = std::env::var("LTP_WS_URL").unwrap_or_else(|_| DEFAULT_WS_URL.to_string());
    let mut persist_tick = tokio::time::interval(Duration::from_millis(25));
    persist_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut financial_tick = tokio::time::interval(Duration::from_secs(5));
    financial_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut shutdown = false;
    loop {
        forwarder.invalidate()?;
        let connect = tokio::time::timeout(
            Duration::from_secs(15),
            WsConnector::connect_with_local_ip_raw(&url, &local_ip),
        );
        tokio::pin!(connect);
        let connection = loop {
            tokio::select! {
                result = &mut connect => break Some(result),
                _ = persist_tick.tick() => forwarder.delivery.poll().await?,
                _ = financial_tick.tick() => forwarder.publish_financial()?,
                _ = tokio::signal::ctrl_c() => break None,
            }
        };
        let Some(connection) = connection else {
            break;
        };
        if let Ok(Ok(connection)) = connection {
            let mut ws = connection.ws_stream.lock().await;
            let mut workers = Vec::new();
            let session: Result<()> = async {
                ws.send(Message::Text(credentials.build_login_payload(false)?)).await?;
                let login = tokio::time::timeout(Duration::from_secs(10), ws.next()).await?
                    .context("RapidX login connection closed")??;
                let response = LtpWsResponse::from_json_str(login.to_text()?).context("invalid RapidX login response")?;
                if !response.is_login() || !response.is_success() { bail!("RapidX account login rejected"); }
                log::info!("RapidX account websocket login successful exchange={wire_exchange}");
                let (tx, mut rx) = tokio::sync::mpsc::channel(8);
                workers.push(tokio::spawn(refresh_accounts(rest.clone(), wire_exchange, tx.clone())));
                let statement_begin = forwarder.delivery.store.state().statement_history_end_ms
                    .map(|end| end - 60_000)
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                workers.push(tokio::spawn(refresh_statements(rest.clone(), wire_exchange, statement_begin, tx.clone())));
                let execution_begin = forwarder.delivery.store.state().execution_history_end_ms.saturating_sub(60_000);
                workers.push(tokio::spawn(refresh_executions(rest.clone(), wire_exchange, execution_begin, tx)));
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut waiting_pong = false;
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => { return Ok(()); }
                        message = ws.next(), if forwarder.delivery.store.has_capacity() => {
                            match message.context("RapidX account disconnected")?? {
                                Message::Text(text) => {
                                    if trade_engine::ltp_ws::is_text_pong(&text) { waiting_pong = false; }
                                    else { forwarder.process(&text, "ws", None).await?; }
                                }
                                Message::Binary(bytes) => {
                                    let text = std::str::from_utf8(&bytes)?;
                                    if trade_engine::ltp_ws::is_text_pong(text) { waiting_pong = false; }
                                    else { forwarder.process(text, "ws", None).await?; }
                                },
                                Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
                                Message::Close(_) => bail!("RapidX account websocket closed"),
                                _ => {}
                            }
                        }
                        update = rx.recv() => forwarder.apply_refresh(update.context("RapidX recovery worker stopped")??).await?,
                        _ = persist_tick.tick() => forwarder.delivery.poll().await?,
                _ = financial_tick.tick() => forwarder.publish_financial()?,
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
        let backoff = tokio::time::sleep(Duration::from_secs(3));
        tokio::pin!(backoff);
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => { shutdown = true; break; },
                _ = &mut backoff => break,
                _ = persist_tick.tick() => forwarder.delivery.poll().await?,
                _ = financial_tick.tick() => forwarder.publish_financial()?,
            }
        }
        if shutdown {
            break;
        }
    }
    if let Err(error) = forwarder.delivery.drain(Duration::from_secs(5)).await {
        log::warn!("RapidX shutdown retains pending facts: {error:#}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::decode_private_push;

    #[test]
    fn empty_private_frames_are_ignored() {
        assert!(decode_private_push("").unwrap().is_none());
        assert!(decode_private_push(" \r\n\t").unwrap().is_none());
    }

    #[test]
    fn nonempty_invalid_private_frames_still_fail() {
        assert!(decode_private_push("not-json").is_err());
    }
}
