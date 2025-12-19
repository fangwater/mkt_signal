use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use serde_json::json;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::time::Instant;

use crate::common::iceoryx_publisher::RESAMPLE_PAYLOAD as ICEORYX_RESAMPLE_PAYLOAD;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::resample_channel::{DEFAULT_EXPOSURE_CHANNEL, DEFAULT_RISK_CHANNEL};
use crate::viz::config::{PreTradeSrcCfg, VizServerCfg};
use crate::viz::resample::{
    PreTradeExposureResampleEntry, PreTradeRiskResampleEntry,
};
use crate::viz::server::WsHub;

const PRE_TRADE_EXPOSURE_CHANNEL: &str = DEFAULT_EXPOSURE_CHANNEL;
const PRE_TRADE_RISK_CHANNEL: &str = DEFAULT_RISK_CHANNEL;

pub const RESAMPLE_PAYLOAD: usize = ICEORYX_RESAMPLE_PAYLOAD;

fn resolve_default_namespaces(server: &VizServerCfg) -> Result<Vec<String>> {
    if server.namespaces.is_empty() {
        return Err(anyhow::anyhow!("viz: missing required config `namespaces`"));
    }
    Ok(server.namespaces.clone())
}

fn resolve_pre_trade_namespaces(
    server: &VizServerCfg,
    pre_trade: &PreTradeSrcCfg,
) -> Result<Vec<String>> {
    if !pre_trade.namespaces.is_empty() {
        return Ok(pre_trade.namespaces.clone());
    }
    resolve_default_namespaces(server)
}

/// 订阅 pre-trade 三路重采样并转发到 WS（不维护任何本地状态）
pub fn spawn_pre_trade_resample_listeners_with_cfg(
    hub: WsHub,
    server: &VizServerCfg,
) -> Result<()> {
    let pre_trade = &server.pre_trade;
    if !pre_trade.enabled {
        return Ok(());
    }

    if !pre_trade.instances.is_empty() {
        for inst in &pre_trade.instances {
            let target_namespaces: Vec<String> = if let Some(ns) = inst.namespace.as_ref() {
                vec![ns.clone()]
            } else {
                resolve_pre_trade_namespaces(server, pre_trade).map_err(|err| {
                    anyhow::anyhow!(
                        "viz: pre_trade instance `{}` missing `namespace` and no `namespaces` configured: {err}",
                        inst.label
                    )
                })?
            };
            for ns in target_namespaces {
                spawn_pre_trade_exposure_listener(
                    hub.clone(),
                    &ns,
                    inst.exposure_channel.as_str(),
                    Some(inst.label.as_str()),
                )?;
                spawn_pre_trade_risk_listener(
                    hub.clone(),
                    &ns,
                    inst.risk_channel.as_str(),
                    Some(inst.label.as_str()),
                )?;
            }
        }
        return Ok(());
    }

    let namespaces = resolve_pre_trade_namespaces(server, pre_trade)?;
    // default single-instance behavior (subscribe each namespace)
    for ns in namespaces {
        spawn_pre_trade_exposure_listener(hub.clone(), &ns, PRE_TRADE_EXPOSURE_CHANNEL, None)?;
        spawn_pre_trade_risk_listener(hub.clone(), &ns, PRE_TRADE_RISK_CHANNEL, None)?;
    }
    Ok(())
}

fn spawn_pre_trade_exposure_listener(
    hub: WsHub,
    namespace: &str,
    channel: &str,
    label: Option<&str>,
) -> Result<()> {
    let namespace = namespace.to_string();
    let channel = channel.to_string();
    let channel_for_msg = channel.clone();
    let label = label.map(|s| s.to_string());
    let label_for_msg = label.clone();
    let namespace_for_msg = namespace.clone();

    let service_name = format!("{}/viz_pubs/{}", namespace, channel);
    spawn_resample_channel(
        &format!(
            "viz_pretrade_exposure{}_{}",
            label
                .as_deref()
                .map(|s| format!("_{}", s))
                .unwrap_or_default(),
            sanitize_node_component(&namespace),
        ),
        &service_name,
        move |entry: PreTradeExposureResampleEntry, hub: WsHub| {
            let now_ts_ms = (get_timestamp_us() / 1000) as i64;
            if let Ok(msg) = serde_json::to_string(&json!({
                "type": "pre_trade_exposure",
                "namespace": namespace_for_msg.as_str(),
                "source": label_for_msg.as_deref(),
                "channel": channel_for_msg.as_str(),
                "ts_ms": now_ts_ms,
                "entry": entry,
            })) {
                hub.broadcast(msg);
            }
        },
        hub,
    )
}

fn spawn_pre_trade_risk_listener(
    hub: WsHub,
    namespace: &str,
    channel: &str,
    label: Option<&str>,
) -> Result<()> {
    let namespace = namespace.to_string();
    let channel = channel.to_string();
    let channel_for_msg = channel.clone();
    let label = label.map(|s| s.to_string());
    let label_for_msg = label.clone();
    let namespace_for_msg = namespace.clone();

    let service_name = format!("{}/viz_pubs/{}", namespace, channel);
    spawn_resample_channel(
        &format!(
            "viz_pretrade_risk{}_{}",
            label
                .as_deref()
                .map(|s| format!("_{}", s))
                .unwrap_or_default(),
            sanitize_node_component(&namespace),
        ),
        &service_name,
        move |entry: PreTradeRiskResampleEntry, hub: WsHub| {
            let now_ts_ms = (get_timestamp_us() / 1000) as i64;
            if let Ok(msg) = serde_json::to_string(&json!({
                "type": "pre_trade_risk",
                "namespace": namespace_for_msg.as_str(),
                "source": label_for_msg.as_deref(),
                "channel": channel_for_msg.as_str(),
                "ts_ms": now_ts_ms,
                "entry": entry,
            })) {
                hub.broadcast(msg);
            }
        },
        hub,
    )
}

fn spawn_resample_channel<T, F>(
    node_suffix: &str,
    service_name: &str,
    on_entry: F,
    hub: WsHub,
) -> Result<()>
where
    T: serde::de::DeserializeOwned + Clone + 'static,
    F: Fn(T, WsHub) + 'static,
{
    let node_suffix = node_suffix.to_string();
    let service_name = service_name.to_string();

    struct Stats {
        window_start: Instant,
        count: usize,
        dropped: usize,
    }
    let stats = Rc::new(RefCell::new(Stats {
        window_start: Instant::now(),
        count: 0,
        dropped: 0,
    }));

    tokio::task::spawn_local(async move {
        let stats = stats;
        let channel_label: &'static str = Box::leak(service_name.clone().into_boxed_str());
        let node_name = format!("{}_{}", node_suffix, sanitize_node_component(channel_label));
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; RESAMPLE_PAYLOAD]>()
                .max_publishers(1)
                .max_subscribers(32)
                .history_size(128)
                .subscriber_max_buffer_size(256)
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; RESAMPLE_PAYLOAD], ()> =
                service.subscriber_builder().create()?;
            info!(
                "viz resample relay subscribed: service={} node={}",
                service.name(),
                node_name
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        if payload.len() < 4 {
                            stats.borrow_mut().dropped += 1;
                            continue;
                        }
                        let mut len_bytes = [0u8; 4];
                        len_bytes.copy_from_slice(&payload[..4]);
                        let data_len = u32::from_le_bytes(len_bytes) as usize;
                        if data_len == 0 || 4 + data_len > payload.len() {
                            stats.borrow_mut().dropped += 1;
                            continue;
                        }
                        let data = &payload[4..4 + data_len];
                        match bincode::deserialize::<T>(data) {
                            Ok(entry) => {
                                on_entry(entry.clone(), hub.clone());

                                let mut st = stats.borrow_mut();
                                st.count += 1;
                                if st.window_start.elapsed() >= Duration::from_secs(3) {
                                    info!(
                                        "viz resample relay {} received count={} dropped={}",
                                        channel_label, st.count, st.dropped
                                    );
                                    st.window_start = Instant::now();
                                    st.count = 0;
                                    st.dropped = 0;
                                }
                            }
                            Err(err) => {
                                stats.borrow_mut().dropped += 1;
                                warn!("viz resample decode failed ({}): {err:#}", channel_label);
                            }
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("viz resample receive error ({}): {err}", channel_label);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = result.await {
            warn!(
                "viz resample relay listener exited ({}): {err:?}",
                channel_label
            );
        }
    });

    Ok(())
}

fn sanitize_node_component(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
