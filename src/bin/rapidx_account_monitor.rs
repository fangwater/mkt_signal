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
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use trade_engine::ltp_account::{parse_account_push, parse_order_push, PositionSnapshotState};
use trade_engine::ltp_rest::LtpRestClient;
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
}

struct Forwarder {
    ipc: PmForwarder,
    journal: File,
    portfolio: String,
    exchange: &'static str,
    scope: BasicAccountScope,
    positions: PositionSnapshotState,
}

impl Forwarder {
    fn process(&mut self, payload: &str, source: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload).context("decode RapidX private push")?;
        let Some(channel) = value.get("channel").and_then(Value::as_str) else {
            return Ok(());
        };
        // Retain exact exchange facts, including fee currency and maker rebates, before conversion.
        serde_json::to_writer(
            &mut self.journal,
            &json!({"received_us":chrono::Utc::now().timestamp_micros(),"source":source,"message":value}),
        )?;
        self.journal.write_all(b"\n")?;
        self.journal.sync_data()?;
        if channel == "Orders"
            && value["data"]["exchangeType"]
                .as_str()
                .is_some_and(|source| source != self.exchange)
        {
            return Ok(());
        }
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
            chrono::Utc::now().timestamp_millis(),
        )?;
        for event in events.drain(..) {
            if !self.ipc.send_raw(&event) {
                bail!("RapidX account IPC delivery failed");
            }
        }
        Ok(())
    }

    fn invalidate(&mut self) -> Result<()> {
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
    // Recover position identities, not balances: the next complete REST snapshot
    // can clear positions that disappeared while this monitor was down.
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        if entry.path().extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        for line in BufReader::new(File::open(entry.path())?).split(b'\n') {
            let line = line?;
            let Ok(record) = serde_json::from_slice::<Value>(&line) else {
                continue;
            };
            let Some(message) = record.get("message") else {
                continue;
            };
            if message.get("channel").and_then(Value::as_str) != Some("Positions") {
                continue;
            }
            let mut events =
                parse_account_push(&message.to_string(), rest.portfolio_id(), wire_exchange)?;
            positions.reconcile(&mut events, false, 0)?;
        }
    }
    let journal = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join(format!(
            "{}.jsonl",
            chrono::Utc::now().format("%Y%m%dT%H%M%S%.6f")
        )))
        .context("open RapidX account journal")?;
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
            let session: Result<()> = async {
                ws.send(Message::Text(credentials.build_login_payload(false)?)).await?;
                let login = tokio::time::timeout(Duration::from_secs(10), ws.next()).await?
                    .context("RapidX login connection closed")??;
                let response = LtpWsResponse::from_json_str(login.to_text()?).context("invalid RapidX login response")?;
                if !response.is_login() || !response.is_success() { bail!("RapidX account login rejected"); }
                let mut refresh = tokio::time::interval(Duration::from_secs(5));
                refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let channels = ["Assets", "Positions", "Accounts"];
                let mut index = 0usize;
                let mut waiting_pong = false;
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => { return Ok(()); }
                        message = ws.next() => {
                            match message.context("RapidX account disconnected")?? {
                                Message::Text(text) => {
                                    if trade_engine::ltp_ws::is_text_pong(&text) { waiting_pong = false; }
                                    else { forwarder.process(&text, "ws")?; }
                                }
                                Message::Binary(bytes) => forwarder.process(std::str::from_utf8(&bytes)?, "ws")?,
                                Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
                                Message::Close(_) => bail!("RapidX account websocket closed"),
                                _ => {}
                            }
                        }
                        _ = refresh.tick() => {
                            let payload = rest.fetch_account_push(channels[index], forwarder.exchange).await?;
                            forwarder.process(&payload, "rest_snapshot")?;
                            index = (index + 1) % channels.len();
                        }
                        _ = heartbeat.tick() => {
                            if waiting_pong { bail!("RapidX account heartbeat expired"); }
                            ws.send(Message::Text("ping".into())).await?;
                            waiting_pong = true;
                        }
                    }
                }
            }.await;
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
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
    Ok(())
}
