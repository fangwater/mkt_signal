use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use serde_json::json;
use std::time::Duration;

use crate::common::iceoryx_publisher::SIGNAL_PAYLOAD;
use crate::funding_rate::{
    CompareOpTag, FrSignalStateBatch, FrSignalStateEntry, SignalTag, SpreadTypeTag,
    DEFAULT_STATE_CHANNEL,
};
use crate::viz::config::{FrStateSrcCfg, VizServerCfg};
use crate::viz::server::WsHub;

fn resolve_namespaces(server: &VizServerCfg, fr: &FrStateSrcCfg) -> Result<Vec<String>> {
    if !fr.namespaces.is_empty() {
        return Ok(fr.namespaces.clone());
    }
    if !server.namespaces.is_empty() {
        return Ok(server.namespaces.clone());
    }
    Err(anyhow::anyhow!(
        "viz: missing required config `namespaces` (or `fr_state.namespaces` override)"
    ))
}

fn infer_exchange_label_from_namespace(namespace: &str) -> &str {
    namespace
        .split(|c: char| c == '_' || c == '-')
        .next()
        .unwrap_or(namespace)
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

fn batch_to_json(exchange: &str, batch: &FrSignalStateBatch) -> serde_json::Value {
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
        "exchange": exchange,
        "ts_us": batch.ts_us,
        "entries": entries
    })
}

pub fn spawn_fr_state_listeners(hub: WsHub, server: &VizServerCfg) -> Result<()> {
    let fr = &server.fr_state;
    if !fr.enabled {
        return Ok(());
    }

    let namespaces = resolve_namespaces(server, fr)?;

    for ns in namespaces {
        let service_name = format!("{}/viz_pubs/{}", ns, DEFAULT_STATE_CHANNEL);
        let port = server.http.port;
        let node_name = format!("viz_fr_state_{}_{}", sanitize_node_component(&ns), port);

        let hub = hub.clone();
        let ns_for_msg = ns.clone();
        tokio::task::spawn_local(async move {
            let ns_for_log = ns_for_msg.clone();
            let result = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;
                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
                    .max_publishers(1)
                    .max_subscribers(32)
                    .history_size(128)
                    .subscriber_max_buffer_size(256)
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "viz fr_state subscribed: service={} node={}",
                    service.name(),
                    node_name
                );

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            if payload.iter().all(|&b| b == 0) {
                                continue;
                            }
                            let bytes = bytes::Bytes::copy_from_slice(payload);
                            if let Ok(batch) = FrSignalStateBatch::from_bytes(bytes) {
                                let exchange =
                                    infer_exchange_label_from_namespace(ns_for_msg.as_str());
                                let msg = json!({
                                    "type": "fr_signal_state",
                                    "namespace": ns_for_msg.as_str(),
                                    "entry": batch_to_json(exchange, &batch),
                                });
                                if let Ok(s) = serde_json::to_string(&msg) {
                                    hub.broadcast(s);
                                }
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("viz fr_state receive error (ns={}): {err}", ns_for_msg);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }

                #[allow(unreachable_code)]
                Ok::<(), anyhow::Error>(())
            };

            if let Err(err) = result.await {
                warn!("viz fr_state listener exited (ns={}): {err:?}", ns_for_log);
            }
        });
    }

    Ok(())
}

fn sanitize_node_component(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
