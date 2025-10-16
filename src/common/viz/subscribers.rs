use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::time::Instant;

use serde_json::json;

use super::state::SharedState;
use crate::common::time_util::get_timestamp_us;
use crate::common::viz::server::WsHub;
use crate::signal::resample::{
    FundingRateArbResampleEntry, PreTradeExposureResampleEntry, PreTradePositionResampleEntry,
    PreTradeRiskResampleEntry, FR_RESAMPLE_MSG_CHANNEL, PRE_TRADE_EXPOSURE_CHANNEL,
    PRE_TRADE_POSITIONS_CHANNEL, PRE_TRADE_RISK_CHANNEL,
};

pub const RESAMPLE_PAYLOAD: usize = 4096;

/// 订阅 funding resample 流式切片（用于验证解码；当前不更新 UI 状态）
pub fn spawn_fr_resample_listener(state: SharedState, hub: WsHub) -> Result<()> {
    let service_name = format!("signal_pubs/{}", FR_RESAMPLE_MSG_CHANNEL);
    struct Stats {
        window_start: Instant,
        count: usize,
        last_ts_ms: i64,
        dropped: usize,
    }
    let stats = Rc::new(RefCell::new(Stats {
        window_start: Instant::now(),
        count: 0,
        last_ts_ms: 0,
        dropped: 0,
    }));
    tokio::task::spawn_local(async move {
        let stats = stats;
        let node_name = "viz_fr_resample".to_string();
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; RESAMPLE_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; RESAMPLE_PAYLOAD], ()> =
                service.subscriber_builder().create()?;
            info!("viz fr_resample subscribed: service={}", service.name());
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
                        match FundingRateArbResampleEntry::from_bytes(data) {
                            Ok(entry) => {
                                let entry_ts = entry.ts_ms;
                                state.update_resample_entry(entry.clone());
                                let now_ts_ms = (get_timestamp_us() / 1000) as i64;
                                if let Ok(msg) = serde_json::to_string(&json!({
                                    "type": "fr_resample_entry",
                                    "ts_ms": now_ts_ms,
                                    "entry": entry,
                                })) {
                                    hub.broadcast(msg);
                                }
                                {
                                    let mut st = stats.borrow_mut();
                                    st.count += 1;
                                    st.last_ts_ms = entry_ts;
                                    if st.window_start.elapsed() >= Duration::from_secs(3) {
                                        info!(
                                            "viz fr_resample received count={} dropped={} last_ts_ms={}",
                                            st.count, st.dropped, st.last_ts_ms
                                        );
                                        st.window_start = Instant::now();
                                        st.count = 0;
                                        st.last_ts_ms = 0;
                                        st.dropped = 0;
                                    }
                                }
                            }
                            Err(err) => warn!("viz fr_resample decode failed: {err:#}"),
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("viz fr_resample receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        if let Err(err) = result.await {
            warn!("viz fr_resample listener exited: {err:?}");
        }
    });
    Ok(())
}

pub fn spawn_pre_trade_resample_listeners(state: SharedState, hub: WsHub) -> Result<()> {
    spawn_pre_trade_positions_listener(state.clone(), hub.clone())?;
    spawn_pre_trade_exposure_listener(state.clone(), hub.clone())?;
    spawn_pre_trade_risk_listener(state, hub)?;
    Ok(())
}

fn spawn_pre_trade_positions_listener(state: SharedState, hub: WsHub) -> Result<()> {
    spawn_pre_trade_channel(
        "viz_pretrade_positions",
        PRE_TRADE_POSITIONS_CHANNEL,
        move |entry: PreTradePositionResampleEntry, state: SharedState, hub: WsHub| {
            state.update_pretrade_positions(entry.clone());
            let now_ts_ms = (get_timestamp_us() / 1000) as i64;
            if let Ok(msg) = serde_json::to_string(&json!({
                "type": "pre_trade_positions",
                "ts_ms": now_ts_ms,
                "entry": entry,
            })) {
                hub.broadcast(msg);
            }
        },
        state,
        hub,
    )
}

fn spawn_pre_trade_exposure_listener(state: SharedState, hub: WsHub) -> Result<()> {
    spawn_pre_trade_channel(
        "viz_pretrade_exposure",
        PRE_TRADE_EXPOSURE_CHANNEL,
        move |entry: PreTradeExposureResampleEntry, state: SharedState, hub: WsHub| {
            state.update_pretrade_exposure(entry.clone());
            let now_ts_ms = (get_timestamp_us() / 1000) as i64;
            if let Ok(msg) = serde_json::to_string(&json!({
                "type": "pre_trade_exposure",
                "ts_ms": now_ts_ms,
                "entry": entry,
            })) {
                hub.broadcast(msg);
            }
        },
        state,
        hub,
    )
}

fn spawn_pre_trade_risk_listener(state: SharedState, hub: WsHub) -> Result<()> {
    spawn_pre_trade_channel(
        "viz_pretrade_risk",
        PRE_TRADE_RISK_CHANNEL,
        move |entry: PreTradeRiskResampleEntry, state: SharedState, hub: WsHub| {
            state.update_pretrade_risk(entry.clone());
            let now_ts_ms = (get_timestamp_us() / 1000) as i64;
            if let Ok(msg) = serde_json::to_string(&json!({
                "type": "pre_trade_risk",
                "ts_ms": now_ts_ms,
                "entry": entry,
            })) {
                hub.broadcast(msg);
            }
        },
        state,
        hub,
    )
}

fn spawn_pre_trade_channel<T, F>(
    node_suffix: &str,
    channel: &str,
    on_entry: F,
    state: SharedState,
    hub: WsHub,
) -> Result<()>
where
    T: serde::de::DeserializeOwned + Clone + 'static,
    F: Fn(T, SharedState, WsHub) + Copy + 'static,
{
    let node_suffix = node_suffix.to_string();
    let channel_name = channel.to_string();
    let service_name = format!("signal_pubs/{}", channel_name);
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
        let channel_label: &'static str = Box::leak(channel_name.clone().into_boxed_str());
        let node_name = format!("{}_{}", node_suffix, channel_label);
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; RESAMPLE_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; RESAMPLE_PAYLOAD], ()> =
                service.subscriber_builder().create()?;
            info!(
                "viz pre_trade resample subscribed: service={} node={}",
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
                                on_entry(entry.clone(), state.clone(), hub.clone());
                                {
                                    let mut st = stats.borrow_mut();
                                    st.count += 1;
                                    if st.window_start.elapsed() >= Duration::from_secs(3) {
                                        info!(
                                            "viz pre_trade {} received count={} dropped={}",
                                            channel_label, st.count, st.dropped
                                        );
                                        st.window_start = Instant::now();
                                        st.count = 0;
                                        st.dropped = 0;
                                    }
                                }
                            }
                            Err(err) => warn!(
                                "viz pre_trade resample decode failed ({}): {err:#}",
                                channel_label
                            ),
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!(
                            "viz pre_trade resample receive error ({}): {err}",
                            channel_label
                        );
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        if let Err(err) = result.await {
            warn!(
                "viz pre_trade resample listener exited (channel={}): {err:?}",
                channel_label
            );
        }
    });
    Ok(())
}
