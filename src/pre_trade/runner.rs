use crate::common::signal_event::{decode_trade_signal, SignalEvent, SignalEventType};
use crate::pre_trade::config::{
    AccountStreamCfg, PreTradeCfg, SignalSubscriptionsCfg, TradeEngineRespCfg,
};
use crate::pre_trade::event::{AccountEvent, PreTradeEvent, TradeEngineResponse, TradeSignalEvent};
use anyhow::{anyhow, Result};
use chrono::Utc;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, error, info, warn};
use std::borrow::Cow;
use tokio::sync::mpsc::UnboundedSender;

const ACCOUNT_PAYLOAD: usize = 16_384;
const TRADE_RESP_PAYLOAD: usize = 16_384;
const SIGNAL_PAYLOAD: usize = 1_024;
const NODE_PRE_TRADE_ACCOUNT: &str = "pre_trade_account";
const NODE_PRE_TRADE_TRADE_RESP: &str = "pre_trade_trade_resp";
const NODE_PRE_TRADE_SIGNAL_PREFIX: &str = "pre_trade_signal_";

pub struct PreTrade {
    cfg: PreTradeCfg,
}

impl PreTrade {
    pub fn new(cfg: PreTradeCfg) -> Self {
        Self { cfg }
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade starting");

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PreTradeEvent>();

        spawn_account_stream_worker(&self.cfg.account_stream, tx.clone())?;
        spawn_trade_response_worker(&self.cfg.trade_engine, tx.clone())?;
        spawn_signal_workers(&self.cfg.signals, tx.clone())?;

        // Drop extra sender reference held locally to ensure shutdown after workers exit.
        drop(tx);

        let shutdown = tokio::signal::ctrl_c();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("shutdown signal received");
                    break;
                }
                Some(evt) = rx.recv() => {
                    log_event(evt);
                }
                else => { break; }
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

fn spawn_account_stream_worker(
    cfg: &AccountStreamCfg,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    if cfg.max_payload_bytes != ACCOUNT_PAYLOAD {
        warn!(
            "account_stream.max_payload_bytes={} unsupported, using {}",
            cfg.max_payload_bytes, ACCOUNT_PAYLOAD
        );
    }

    let service = cfg.service.clone();
    let node_name = cfg
        .label
        .as_deref()
        .map(|label| format!("{}_{}", NODE_PRE_TRADE_ACCOUNT, sanitize_suffix(label)))
        .unwrap_or_else(|| NODE_PRE_TRADE_ACCOUNT.to_string());

    tokio::task::spawn_local(async move {
        if let Err(err) = account_stream_loop(node_name, service, tx).await {
            error!("account stream worker exited: {err:?}");
        }
    });
    Ok(())
}

fn spawn_trade_response_worker(
    cfg: &TradeEngineRespCfg,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    if cfg.max_payload_bytes != TRADE_RESP_PAYLOAD {
        warn!(
            "trade_engine.max_payload_bytes={} unsupported, using {}",
            cfg.max_payload_bytes, TRADE_RESP_PAYLOAD
        );
    }

    let service = cfg.service.clone();
    let node_name = cfg
        .label
        .as_deref()
        .map(|label| format!("{}_{}", NODE_PRE_TRADE_TRADE_RESP, sanitize_suffix(label)))
        .unwrap_or_else(|| NODE_PRE_TRADE_TRADE_RESP.to_string());

    tokio::task::spawn_local(async move {
        if let Err(err) = trade_response_loop(node_name, service, tx).await {
            error!("trade response worker exited: {err:?}");
        }
    });
    Ok(())
}

fn spawn_signal_workers(
    cfg: &SignalSubscriptionsCfg,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    if cfg.channels.is_empty() {
        info!("no signal channels configured");
        return Ok(());
    }
    if cfg.max_payload_bytes != SIGNAL_PAYLOAD {
        warn!(
            "signals.max_payload_bytes={} unsupported, using {}",
            cfg.max_payload_bytes, SIGNAL_PAYLOAD
        );
    }

    for channel in &cfg.channels {
        let channel_name = channel.clone();
        let node_name = signal_node_name(channel);
        let svc_name = format!("signal_pubs/{}", channel);
        let tx_clone = tx.clone();

        tokio::task::spawn_local(async move {
            if let Err(err) = signal_loop(node_name, svc_name, channel_name, tx_clone).await {
                error!("signal worker for channel exited: {err:?}");
            }
        });
    }

    Ok(())
}

async fn account_stream_loop(
    node_name: String,
    service: String,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    let service_name = service.name().to_string();

    info!("account stream subscribed: service={}", service_name);

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let payload = trim_payload(sample.payload());
                let received_at = Utc::now();
                let payload_len = payload.len();
                let (event_type, event_time_ms) = extract_account_metadata(&payload);
                let event = AccountEvent {
                    service: service_name.clone(),
                    received_at,
                    payload,
                    payload_len,
                    event_type,
                    event_time_ms,
                };
                if tx.send(PreTradeEvent::Account(event)).is_err() {
                    break;
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("account stream receive error: {err}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

async fn trade_response_loop(
    node_name: String,
    service: String,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    let service_name = service.name().to_string();

    info!("trade response subscribed: service={}", service_name);

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => match parse_trade_response(sample.payload(), &service_name) {
                Ok(event) => {
                    if tx.send(PreTradeEvent::TradeResponse(event)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    warn!("failed to parse trade response: {err}");
                }
            },
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("trade response receive error: {err}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

async fn signal_loop(
    node_name: String,
    service: String,
    channel_name: String,
    tx: UnboundedSender<PreTradeEvent>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    let service_name = service.name().to_string();

    info!(
        "signal subscribed: service={}, channel={}",
        service_name, channel_name
    );

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let frame = trim_payload(sample.payload());
                let received_at = Utc::now();
                let frame_len = frame.len();
                match SignalEvent::parse(&frame) {
                    Ok(parsed) => {
                        let header = parsed.header;
                        if let Some(kind) = header.event_type() {
                            if kind == SignalEventType::TradeSignal {
                                match decode_trade_signal(&parsed) {
                                    Ok(data) => {
                                        drop(parsed);
                                        let event = TradeSignalEvent {
                                            channel: channel_name.clone(),
                                            received_at,
                                            frame_len,
                                            frame,
                                            header,
                                            data,
                                        };
                                        if tx.send(PreTradeEvent::Signal(event)).is_err() {
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            "failed to decode trade signal payload from channel {}: {}",
                                            channel_name, err
                                        );
                                    }
                                }
                            } else {
                                warn!(
                                    "unsupported signal event type {:?} on channel {}",
                                    kind, channel_name
                                );
                            }
                        } else {
                            warn!(
                                "unknown signal event type {} on channel {}",
                                header.event_type, channel_name
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            "failed to parse signal frame from channel {}: {}",
                            channel_name, err
                        );
                    }
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("signal receive error (channel={}): {err}", channel_name);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

fn parse_trade_response(
    payload: &[u8; TRADE_RESP_PAYLOAD],
    service_name: &str,
) -> Result<TradeEngineResponse> {
    let actual_len = payload
        .iter()
        .rposition(|&x| x != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);

    if actual_len < 40 {
        return Err(anyhow!(
            "trade response payload too short ({} bytes) from service {}",
            actual_len,
            service_name
        ));
    }

    let read_u32 = |offset: usize| -> u32 {
        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
    };
    let read_i64 = |offset: usize| -> i64 {
        i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
    };
    let read_u16 = |offset: usize| -> u16 {
        u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap())
    };

    let req_type = read_u32(0);
    let local_recv_time = read_i64(4);
    let client_order_id = read_i64(12);
    let exchange = read_u32(20);
    let status = read_u16(24);
    let ip_used_weight_1m = read_u32(28);
    let order_count_1m = read_u32(32);
    let body_len = read_u32(36) as usize;

    let available = actual_len.saturating_sub(40);
    let take = std::cmp::min(available, body_len);
    let body_truncated = take < body_len;
    let mut body = Vec::with_capacity(take);
    body.extend_from_slice(&payload[40..40 + take]);

    Ok(TradeEngineResponse {
        service: service_name.to_string(),
        received_at: Utc::now(),
        payload_len: actual_len,
        req_type,
        local_recv_time,
        client_order_id,
        exchange,
        status,
        ip_used_weight_1m: remap_u32(ip_used_weight_1m),
        order_count_1m: remap_u32(order_count_1m),
        body,
        body_truncated,
    })
}

fn remap_u32(value: u32) -> Option<u32> {
    if value == u32::MAX {
        None
    } else {
        Some(value)
    }
}

fn trim_payload<T: AsRef<[u8]>>(payload: T) -> Vec<u8> {
    let bytes = payload.as_ref();
    let actual_len = bytes
        .iter()
        .rposition(|&x| x != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);
    bytes[..actual_len].to_vec()
}

fn extract_account_metadata(payload: &[u8]) -> (Option<String>, Option<i64>) {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(serde_json::Value::Object(map)) => {
            let event_type = map.get("e").and_then(|v| v.as_str()).map(|s| s.to_string());
            let event_time = map.get("E").and_then(|v| v.as_i64());
            (event_type, event_time)
        }
        _ => (None, None),
    }
}

fn signal_node_name(channel: &str) -> String {
    format!(
        "{}{}",
        NODE_PRE_TRADE_SIGNAL_PREFIX,
        sanitize_suffix(channel)
    )
}

fn sanitize_suffix(raw: &str) -> Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    Cow::Owned(sanitized)
}

fn is_valid_node_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn log_event(event: PreTradeEvent) {
    match event {
        PreTradeEvent::Account(evt) => {
            debug!(
                "account msg: service={} received_at={} type={:?} account_ts={:?} bytes={}",
                evt.service, evt.received_at, evt.event_type, evt.event_time_ms, evt.payload_len
            );
        }
        PreTradeEvent::TradeResponse(evt) => {
            debug!(
                "trade resp: service={} received_at={} req_type={} status={} client_order_id={} exchange={} bytes={} body_truncated={}",
                evt.service,
                evt.received_at,
                evt.req_type,
                evt.status,
                evt.client_order_id,
                evt.exchange,
                evt.payload_len,
                evt.body_truncated
            );
        }
        PreTradeEvent::Signal(evt) => {
            let evt_kind = evt
                .header
                .event_type()
                .map(|k| format!("{:?}", k))
                .unwrap_or_else(|| format!("unknown({})", evt.header.event_type));
            debug!(
                "signal: channel={} received_at={} kind={} frame_len={} payload_len={} event_ts={} publish_ts={} symbol={} exchange={}",
                evt.channel,
                evt.received_at,
                evt_kind,
                evt.frame_len,
                evt.header.payload_len,
                evt.header.event_ts_ms,
                evt.header.publish_ts_ms,
                evt.data.symbol,
                evt.data.exchange
            );
        }
    }
}
