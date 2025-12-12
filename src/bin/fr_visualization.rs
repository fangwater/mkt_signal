//! fr_visualization - Funding rate signal state consumer + WS backend.
//!
//! Subscribes to `fr_signal_state_{exchange}` channels published by fr_signal,
//! decodes FrSignalStateBatch, converts to JSON, and broadcasts to websocket clients.

use anyhow::Result;
use clap::Parser;
use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio_util::sync::CancellationToken;

use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::iceoryx_publisher::SIGNAL_PAYLOAD;
use mkt_signal::common::iceoryx_subscriber::GenericSignalSubscriber;
use mkt_signal::common::ipc_service_name::build_service_name;
use mkt_signal::common::viz::config::HttpCfg;
use mkt_signal::common::viz::server::{serve_http, WsHub};
use mkt_signal::funding_rate::{
    CompareOpTag, FrSignalStateBatch, FrSignalStateEntry, SignalTag, SpreadTypeTag,
    DEFAULT_STATE_CHANNEL,
};
use serde_json::json;

#[derive(Parser, Debug)]
#[command(name = "fr_visualization")]
#[command(about = "Funding Rate 信号状态可视化进程（订阅 fr_signal_state）")]
struct Args {
    /// Websocket listen port
    #[arg(long, default_value_t = 8811)]
    port: u16,
}

fn create_state_subscriber(
    node: &Node<ipc::Service>,
    channel_name: &str,
) -> Result<GenericSignalSubscriber> {
    let service_name = build_service_name(&format!("signal_pubs/{}", channel_name));
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
        .max_publishers(1)
        .max_subscribers(32)
        .history_size(128)
        .subscriber_max_buffer_size(256)
        .open_or_create()?;

    let subscriber = service.subscriber_builder().create()?;
    Ok(GenericSignalSubscriber::Size4K(subscriber))
}

fn tag_to_str(tag: SignalTag) -> &'static str {
    match tag {
        SignalTag::None => "-",
        SignalTag::FwdOpen => "FwdOpen",
        SignalTag::FwdClose => "FwdClose",
        SignalTag::BwdOpen => "BwdOpen",
        SignalTag::BwdClose => "BwdClose",
        SignalTag::FwdCancel => "FwdCancel",
        SignalTag::BwdCancel => "BwdCancel",
    }
}

fn compare_op_to_str(tag: CompareOpTag) -> &'static str {
    match tag {
        CompareOpTag::GreaterThan => "GreaterThan",
        CompareOpTag::LessThan => "LessThan",
        CompareOpTag::Unknown => "Unknown",
    }
}

fn spread_type_to_str(tag: SpreadTypeTag) -> &'static str {
    match tag {
        SpreadTypeTag::BidAsk => "BidAsk",
        SpreadTypeTag::AskBid => "AskBid",
        SpreadTypeTag::SpreadRate => "SpreadRate",
        SpreadTypeTag::Unknown => "Unknown",
    }
}

fn entry_symbol(e: &FrSignalStateEntry) -> String {
    String::from_utf8_lossy(&e.symbol)
        .trim_end_matches('\0')
        .to_string()
}

fn batch_to_json(exchange: Exchange, batch: &FrSignalStateBatch) -> String {
    let entries = batch
        .entries
        .iter()
        .map(|e| {
            json!({
                "symbol": entry_symbol(e),
                "pred_fr_pct": e.pred_fr_pct,
                "fr_ma_pct": e.fr_ma_pct,
                "pred_loan_pct": e.pred_loan_pct,
                "cur_loan_pct": e.cur_loan_pct,
                "fr_plus_pred_loan_pct": e.fr_plus_pred_loan_pct,
                "ma_plus_cur_loan_pct": e.ma_plus_cur_loan_pct,
                "fr_sig": tag_to_str(e.fr_sig),
                "spread_sig": tag_to_str(e.spread_sig),
                "spread_threshold": e.spread_threshold,
                "spread_used_value": e.spread_used_value,
                "spread_compare_op": compare_op_to_str(e.spread_compare_op),
                "spread_type": spread_type_to_str(e.spread_type),
                "spread_bidask": e.spread_bidask,
                "spread_askbid": e.spread_askbid,
                "spread_rate": e.spread_rate,
                "final_sig": tag_to_str(e.final_sig),
            })
        })
        .collect::<Vec<_>>();

    json!({
        "exchange": exchange.as_str(),
        "ts_us": batch.ts_us,
        "entries": entries
    })
    .to_string()
}

async fn run(port: u16, token: CancellationToken) -> Result<()> {
    let node_name = NodeName::new("fr_visualization")?;
    let node = NodeBuilder::new()
        .name(&node_name)
        .create::<ipc::Service>()?;

    let exchanges = [
        Exchange::Binance,
        Exchange::Okex,
        Exchange::Bybit,
        Exchange::Bitget,
        Exchange::Gate,
    ];
    let mut subs: Vec<(Exchange, GenericSignalSubscriber)> = Vec::new();
    for ex in exchanges {
        let channel_name = format!("{}_{}", DEFAULT_STATE_CHANNEL, ex.as_str());
        let sub = create_state_subscriber(&node, &channel_name)?;
        info!("fr_visualization subscribed on '{}'", channel_name);
        subs.push((ex, sub));
    }

    let hub = WsHub::new(256, None);

    // multi-subscriber -> json (with exchange) -> hub broadcaster
    {
        let hub_clone = hub.clone();
        let cancel = token.clone();
        tokio::task::spawn_local(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(50)) => {
                        for (ex, sub) in subs.iter() {
                            if let Ok(Some(data)) = sub.receive_msg() {
                                if let Ok(batch) = FrSignalStateBatch::from_bytes(data) {
                                    let json_str = batch_to_json(*ex, &batch);
                                    hub_clone.broadcast(json_str);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    let http_cfg = HttpCfg {
        bind: "0.0.0.0".to_string(),
        port,
        ws_path: "/ws".to_string(),
        cors_origins: None,
        auth_token: None,
    };
    info!(
        "fr_visualization websocket listening on 0.0.0.0:{}{}",
        port, http_cfg.ws_path
    );
    serve_http(http_cfg, hub).await
}

fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    #[cfg(unix)]
    {
        let token_clone = token.clone();
        tokio::spawn(async move {
            let mut sigterm =
                unix_signal(SignalKind::terminate()).expect("failed to setup SIGTERM");
            let mut sigint = unix_signal(SignalKind::interrupt()).expect("failed to setup SIGINT");
            tokio::select! {
                _ = sigterm.recv() => info!("收到 SIGTERM"),
                _ = sigint.recv() => info!("收到 SIGINT (Ctrl+C)"),
            }
            token_clone.cancel();
        });
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();

    let token = CancellationToken::new();
    setup_signal_handlers(&token)?;
    let local = tokio::task::LocalSet::new();
    local.run_until(run(args.port, token)).await
}
