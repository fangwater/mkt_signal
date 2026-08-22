use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use runtime_common::ipc_service_name::build_service_name;

pub const RELOAD_NOTIFY_PAYLOAD: usize = 512;
const RELOAD_NOTIFY_MAX_PUBLISHERS: usize = 4;
const RELOAD_NOTIFY_MAX_SUBSCRIBERS: usize = 8;
const RELOAD_NOTIFY_HISTORY_SIZE: usize = 32;
const RELOAD_NOTIFY_SUBSCRIBER_BUFFER: usize = 32;
const RELOAD_NOTIFY_SERVICE: &str = "batch_exec_pubs/reload_notify";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadNotify {
    pub strategy_name: String,
    pub updated_at_us: i64,
}

pub struct BatchExecReloadNotify {
    service_name: String,
    subscriber: Subscriber<ipc::Service, [u8; RELOAD_NOTIFY_PAYLOAD], ()>,
}

impl BatchExecReloadNotify {
    pub fn try_open() -> Option<Self> {
        match Self::open() {
            Ok(channel) => Some(channel),
            Err(err) => {
                warn!("BatchExec reload notify unavailable: {err:#}");
                None
            }
        }
    }

    pub fn open() -> Result<Self> {
        let service_name = build_service_name(RELOAD_NOTIFY_SERVICE);
        let node = NodeBuilder::new()
            .name(&NodeName::new("exec_pre_trade_reload_notify")?)
            .create::<ipc::Service>()
            .context("create BatchExec reload-notify iceoryx node")?;
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; RELOAD_NOTIFY_PAYLOAD]>()
            .max_publishers(RELOAD_NOTIFY_MAX_PUBLISHERS)
            .max_subscribers(RELOAD_NOTIFY_MAX_SUBSCRIBERS)
            .history_size(RELOAD_NOTIFY_HISTORY_SIZE)
            .subscriber_max_buffer_size(RELOAD_NOTIFY_SUBSCRIBER_BUFFER)
            .open_or_create()
            .with_context(|| format!("open BatchExec reload-notify service {service_name}"))?;
        let subscriber = service
            .subscriber_builder()
            .buffer_size(RELOAD_NOTIFY_SUBSCRIBER_BUFFER)
            .create()
            .with_context(|| format!("subscribe BatchExec reload-notify {service_name}"))?;
        info!("BatchExec reload notify subscribed: service={service_name}");
        Ok(Self {
            service_name,
            subscriber,
        })
    }

    pub fn drain(&self) -> Option<ReloadNotify> {
        let mut latest = None;
        loop {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    if let Some(notify) = decode_reload_notify(sample.payload()) {
                        latest = Some(notify);
                    } else {
                        warn!(
                            "BatchExec reload notify ignored invalid payload: service={}",
                            self.service_name
                        );
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        "BatchExec reload notify receive failed: service={} err={err:?}",
                        self.service_name
                    );
                    break;
                }
            }
        }
        latest
    }
}

pub fn encode_reload_notify(notify: &ReloadNotify) -> Result<[u8; RELOAD_NOTIFY_PAYLOAD]> {
    if notify.updated_at_us <= 0 {
        anyhow::bail!("updated_at_us must be positive");
    }
    let name = notify.strategy_name.as_bytes();
    if name.len() > 255 {
        anyhow::bail!("strategy_name exceeds reload notify payload");
    }
    let mut bytes = [0u8; RELOAD_NOTIFY_PAYLOAD];
    bytes[..8].copy_from_slice(&notify.updated_at_us.to_le_bytes());
    bytes[8] = name.len() as u8;
    bytes[9..9 + name.len()].copy_from_slice(name);
    Ok(bytes)
}

pub fn decode_reload_notify(bytes: &[u8; RELOAD_NOTIFY_PAYLOAD]) -> Option<ReloadNotify> {
    let updated_at_us = i64::from_le_bytes(bytes[0..8].try_into().ok()?);
    if updated_at_us <= 0 {
        return None;
    }
    let name_len = usize::from(bytes[8]);
    let name_bytes = bytes.get(9..9 + name_len)?;
    let strategy_name = std::str::from_utf8(name_bytes).ok()?.to_string();
    Some(ReloadNotify {
        strategy_name,
        updated_at_us,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_notify_round_trips() {
        let notify = ReloadNotify {
            strategy_name: "CTA_A".to_string(),
            updated_at_us: 42,
        };
        let encoded = encode_reload_notify(&notify).unwrap();
        assert_eq!(decode_reload_notify(&encoded), Some(notify));
    }

    #[test]
    fn reload_notify_ignores_uncommitted_version() {
        let mut bytes = encode_reload_notify(&ReloadNotify {
            strategy_name: "CTA_A".to_string(),
            updated_at_us: 7,
        })
        .unwrap();
        bytes[..8].copy_from_slice(&0i64.to_le_bytes());
        assert_eq!(decode_reload_notify(&bytes), None);
    }
}
