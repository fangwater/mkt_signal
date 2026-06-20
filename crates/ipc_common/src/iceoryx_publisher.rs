use anyhow::{anyhow, Result};
use iceoryx2::config::Config;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use runtime_common::ipc_service_name::build_service_name;
use signal_common::trade_signal::{SignalType, TradeSignal, TRADE_SIGNAL_HEADER_LEN};

pub const SIGNAL_PAYLOAD: usize = 4_096;
pub const TRADE_SIGNAL_PAYLOAD: usize = 1_024;
pub const RESAMPLE_PAYLOAD: usize = 32 * 1024;
pub const BINANCE_MARGIN_UPDATE_PAYLOAD: usize = SIGNAL_PAYLOAD;
pub const BINANCE_UM_UPDATE_PAYLOAD: usize = SIGNAL_PAYLOAD;
pub const QUERY_REQ_PAYLOAD: usize = 256;
// Query responses now also carry standard-account snapshot events, which are
// materially larger than compact order-query payloads.
pub const QUERY_RESP_PAYLOAD: usize = 4_096;

#[repr(C)]
pub struct TradeSignalIpcPayload {
    len: u32,
    buf: MaybeUninit<[u8; TRADE_SIGNAL_PAYLOAD]>,
}

unsafe impl ZeroCopySend for TradeSignalIpcPayload {}

impl std::fmt::Debug for TradeSignalIpcPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeSignalIpcPayload")
            .field("signal_len", &self.signal_len())
            .finish_non_exhaustive()
    }
}

impl TradeSignalIpcPayload {
    pub const CAPACITY: usize = TRADE_SIGNAL_PAYLOAD;
    pub const MIN_SIGNAL_LEN: usize = TRADE_SIGNAL_HEADER_LEN;

    pub fn write_raw_to_uninit_slot(slot: &mut MaybeUninit<Self>, raw: &[u8]) -> Option<()> {
        if raw.len() > Self::CAPACITY {
            return None;
        }
        unsafe {
            let ptr = slot.as_mut_ptr();
            std::ptr::addr_of_mut!((*ptr).len).write(raw.len() as u32);
            std::ptr::copy_nonoverlapping(
                raw.as_ptr(),
                std::ptr::addr_of_mut!((*ptr).buf).cast::<u8>(),
                raw.len(),
            );
        }
        Some(())
    }

    pub fn write_parts_to_uninit_slot(
        slot: &mut MaybeUninit<Self>,
        signal_type: SignalType,
        generation_time: i64,
        handle_time: f64,
        context: &[u8],
    ) -> Option<usize> {
        let total_len = TRADE_SIGNAL_HEADER_LEN.checked_add(context.len())?;
        if total_len > Self::CAPACITY || context.len() > u32::MAX as usize {
            return None;
        }
        unsafe {
            let ptr = slot.as_mut_ptr();
            std::ptr::addr_of_mut!((*ptr).len).write(total_len as u32);
            let buf = std::ptr::addr_of_mut!((*ptr).buf).cast::<u8>();
            std::ptr::copy_nonoverlapping((signal_type as u32).to_le_bytes().as_ptr(), buf, 4);
            std::ptr::copy_nonoverlapping(generation_time.to_le_bytes().as_ptr(), buf.add(4), 8);
            std::ptr::copy_nonoverlapping(handle_time.to_le_bytes().as_ptr(), buf.add(12), 8);
            std::ptr::copy_nonoverlapping(
                (context.len() as u32).to_le_bytes().as_ptr(),
                buf.add(20),
                4,
            );
            std::ptr::copy_nonoverlapping(
                context.as_ptr(),
                buf.add(TRADE_SIGNAL_HEADER_LEN),
                context.len(),
            );
        }
        Some(total_len)
    }

    pub fn as_signal_slice(&self) -> Option<&[u8]> {
        let len = self.len as usize;
        if len > Self::CAPACITY {
            return None;
        }
        Some(self.initialized_prefix(len))
    }

    fn signal_len(&self) -> Option<usize> {
        let raw = self.as_signal_slice()?;
        let encoded_len = TradeSignal::encoded_len(raw)?;
        (encoded_len == raw.len()).then_some(encoded_len)
    }

    fn initialized_prefix(&self, len: usize) -> &[u8] {
        debug_assert!(len <= Self::CAPACITY);
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr().cast::<u8>(), len) }
    }
}

static SIGNAL_PUBLISH_DRY_RUN: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy)]
enum PublisherServiceMode {
    OpenOrCreate,
    Create,
    Open,
}

pub struct GenericPublisher<const PAYLOAD: usize> {
    publisher: Option<Publisher<ipc::Service, [u8; PAYLOAD], ()>>,
    full_service: String,
    suppress_trade_signal_publish: bool,
    suppressed_publish_count: AtomicU64,
}

pub fn configure_signal_publish_dry_run(enabled: bool) {
    SIGNAL_PUBLISH_DRY_RUN.store(enabled, Ordering::Relaxed);
}

fn signal_publish_dry_run_enabled() -> bool {
    SIGNAL_PUBLISH_DRY_RUN.load(Ordering::Relaxed)
}

fn is_corrupted_service_err(err_text: &str) -> bool {
    err_text.contains("ServiceInCorruptedState")
}

fn is_publisher_capacity_err(err_text: &str) -> bool {
    err_text.contains("ExceedsMaxSupportedPublishers")
}

fn describe_signal_payload(data: &[u8]) -> String {
    let signal_type = TradeSignal::get_signal_type(data)
        .map(|v| format!("{:?}", v))
        .unwrap_or_else(|| "unknown".to_string());
    let generation_time = TradeSignal::get_generation_time(data)
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!(
        "signal_type={} generation_time={} payload_bytes={}",
        signal_type,
        generation_time,
        data.len()
    )
}

impl<const PAYLOAD: usize> GenericPublisher<PAYLOAD> {
    pub fn new(channel_name: &str) -> Result<Self> {
        Self::new_with_prefix("signal_pubs", channel_name)
    }

    pub fn create(channel_name: &str) -> Result<Self> {
        Self::new_with_prefix_mode("signal_pubs", channel_name, PublisherServiceMode::Create)
    }

    pub fn open(channel_name: &str) -> Result<Self> {
        Self::new_with_prefix_mode("signal_pubs", channel_name, PublisherServiceMode::Open)
    }

    pub fn new_with_prefix(prefix: &str, channel_name: &str) -> Result<Self> {
        Self::new_with_prefix_mode(prefix, channel_name, PublisherServiceMode::OpenOrCreate)
    }

    fn new_with_prefix_mode(
        prefix: &str,
        channel_name: &str,
        service_mode: PublisherServiceMode,
    ) -> Result<Self> {
        let full_service = build_service_name(&format!("{}/{}", prefix, channel_name));
        let suppress_trade_signal_publish =
            prefix == "signal_pubs" && channel_name == "trade_signal";
        if suppress_trade_signal_publish && signal_publish_dry_run_enabled() {
            info!(
                "IceOryx publisher dry-run bypass: {} (service not created)",
                full_service
            );
            return Ok(Self {
                publisher: None,
                full_service,
                suppress_trade_signal_publish,
                suppressed_publish_count: AtomicU64::new(0),
            });
        }

        let service_name = ServiceName::new(&full_service)?;

        let node_id = format!("{}_{}", prefix, channel_name);
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_id)?)
            .create::<ipc::Service>()?;

        let service_builder = || {
            node.service_builder(&service_name)
                .publish_subscribe::<[u8; PAYLOAD]>()
                .max_publishers(1)
                .max_subscribers(32)
                .history_size(128)
                .subscriber_max_buffer_size(256)
        };

        let service = match service_mode {
            PublisherServiceMode::OpenOrCreate => match service_builder().open_or_create() {
                Ok(service) => service,
                Err(create_err) => {
                    let create_text = format!("{:?}", create_err);
                    if is_corrupted_service_err(&create_text) {
                        warn!(
                            "Signal publisher hit ServiceInCorruptedState, attempting dead-node cleanup: service='{}' err={:?}",
                            full_service, create_err
                        );
                        let cleanup =
                            Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                        warn!(
                            "dead-node cleanup completed for signal publisher: service='{}' cleanups={} failed_cleanups={}",
                            full_service, cleanup.cleanups, cleanup.failed_cleanups
                        );
                        service_builder().open_or_create().map_err(|retry_err| {
                            anyhow!(
                                "failed to open/create signal publisher service after dead-node cleanup: service='{}', err={:?}, retry_err={:?}",
                                full_service,
                                create_err,
                                retry_err
                            )
                        })?
                    } else {
                        return Err(create_err.into());
                    }
                }
            },
            PublisherServiceMode::Create => service_builder().create().map_err(|err| {
                anyhow!(
                    "failed to create signal publisher service: service='{}', err={:?}",
                    full_service,
                    err
                )
            })?,
            PublisherServiceMode::Open => service_builder().open().map_err(|err| {
                anyhow!(
                    "failed to open signal publisher service: service='{}', err={:?}",
                    full_service,
                    err
                )
            })?,
        };

        let publisher = match service.publisher_builder().create() {
            Ok(publisher) => publisher,
            Err(err) => {
                let err_text = format!("{:?}", err);
                if is_publisher_capacity_err(&err_text) {
                    warn!(
                        "Signal publisher hit max-publishers, attempting dead-node cleanup: service='{}' err={:?}",
                        full_service, err
                    );
                    let cleanup = Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                    warn!(
                        "dead-node cleanup completed for signal publisher: service='{}' cleanups={} failed_cleanups={}",
                        full_service, cleanup.cleanups, cleanup.failed_cleanups
                    );
                    service.publisher_builder().create().map_err(|retry_err| {
                        anyhow!(
                            "failed to create signal publisher after dead-node cleanup: service='{}', err={:?}, retry_err={:?}",
                            full_service,
                            err,
                            retry_err
                        )
                    })?
                } else {
                    return Err(err.into());
                }
            }
        };

        info!(
            "IceOryx publisher ready: {} mode={:?}",
            full_service, service_mode
        );

        Ok(Self {
            publisher: Some(publisher),
            full_service,
            suppress_trade_signal_publish,
            suppressed_publish_count: AtomicU64::new(0),
        })
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        self.publish_inner(
            data.len(),
            |out| {
                out.copy_from_slice(data);
            },
            || describe_signal_payload(data),
        )
    }

    pub fn publish_with<F>(&self, len: usize, write: F) -> Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        self.publish_inner(len, write, || format!("payload_bytes={}", len))
    }

    fn publish_inner<F, D>(&self, len: usize, write: F, describe_dry_run: D) -> Result<()>
    where
        F: FnOnce(&mut [u8]),
        D: FnOnce() -> String,
    {
        if len > PAYLOAD {
            anyhow::bail!("Data size exceeds {} bytes", PAYLOAD);
        }

        if self.suppress_trade_signal_publish && signal_publish_dry_run_enabled() {
            let suppressed = self
                .suppressed_publish_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if suppressed == 1 || suppressed.is_multiple_of(100) {
                warn!(
                    "Signal publish suppressed (dry-run): service={} count={} {}",
                    self.full_service,
                    suppressed,
                    describe_dry_run()
                );
            }
            return Ok(());
        }

        let publisher = self.publisher.as_ref().ok_or_else(|| {
            anyhow::anyhow!("publisher unavailable for service={}", self.full_service)
        })?;
        let mut sample = publisher.loan_uninit()?;
        unsafe {
            let payload = sample.payload_mut().as_mut_ptr().cast::<u8>();
            let out = std::slice::from_raw_parts_mut(payload, len);
            write(out);
            if len < PAYLOAD {
                std::ptr::write_bytes(payload.add(len), 0, PAYLOAD - len);
            }
            let sample = sample.assume_init();
            sample.send()?;
        }

        // 生产环境去除发布详情 DEBUG 日志，避免刷屏

        Ok(())
    }
}

pub struct TradeSignalPublisher {
    publisher: TradeSignalPublisherInner,
}

struct TradeSignalPublisherInner {
    publisher: Option<Publisher<ipc::Service, TradeSignalIpcPayload, ()>>,
    full_service: String,
    suppress_trade_signal_publish: bool,
    suppressed_publish_count: AtomicU64,
}

impl TradeSignalPublisherInner {
    fn new_with_prefix_mode(
        prefix: &str,
        channel_name: &str,
        service_mode: PublisherServiceMode,
    ) -> Result<Self> {
        let full_service = build_service_name(&format!("{}/{}", prefix, channel_name));
        let suppress_trade_signal_publish =
            prefix == "signal_pubs" && channel_name == "trade_signal";
        if suppress_trade_signal_publish && signal_publish_dry_run_enabled() {
            info!(
                "IceOryx publisher dry-run bypass: {} (service not created)",
                full_service
            );
            return Ok(Self {
                publisher: None,
                full_service,
                suppress_trade_signal_publish,
                suppressed_publish_count: AtomicU64::new(0),
            });
        }

        let service_name = ServiceName::new(&full_service)?;
        let node_id = format!("{}_{}", prefix, channel_name);
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_id)?)
            .create::<ipc::Service>()?;

        let service_builder = || {
            node.service_builder(&service_name)
                .publish_subscribe::<TradeSignalIpcPayload>()
                .max_publishers(1)
                .max_subscribers(32)
                .history_size(128)
                .subscriber_max_buffer_size(256)
        };

        let service = match service_mode {
            PublisherServiceMode::OpenOrCreate => match service_builder().open_or_create() {
                Ok(service) => service,
                Err(create_err) => {
                    let create_text = format!("{:?}", create_err);
                    if is_corrupted_service_err(&create_text) {
                        warn!(
                            "Signal publisher hit ServiceInCorruptedState, attempting dead-node cleanup: service='{}' err={:?}",
                            full_service, create_err
                        );
                        let cleanup =
                            Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                        warn!(
                            "dead-node cleanup completed for signal publisher: service='{}' cleanups={} failed_cleanups={}",
                            full_service, cleanup.cleanups, cleanup.failed_cleanups
                        );
                        service_builder().open_or_create().map_err(|retry_err| {
                            anyhow!(
                                "failed to open/create signal publisher service after dead-node cleanup: service='{}', err={:?}, retry_err={:?}",
                                full_service,
                                create_err,
                                retry_err
                            )
                        })?
                    } else {
                        return Err(create_err.into());
                    }
                }
            },
            PublisherServiceMode::Create => service_builder().create().map_err(|err| {
                anyhow!(
                    "failed to create signal publisher service: service='{}', err={:?}",
                    full_service,
                    err
                )
            })?,
            PublisherServiceMode::Open => service_builder().open().map_err(|err| {
                anyhow!(
                    "failed to open signal publisher service: service='{}', err={:?}",
                    full_service,
                    err
                )
            })?,
        };

        let publisher = match service.publisher_builder().create() {
            Ok(publisher) => publisher,
            Err(err) => {
                let err_text = format!("{:?}", err);
                if is_publisher_capacity_err(&err_text) {
                    warn!(
                        "Signal publisher hit max-publishers, attempting dead-node cleanup: service='{}' err={:?}",
                        full_service, err
                    );
                    let cleanup = Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                    warn!(
                        "dead-node cleanup completed for signal publisher: service='{}' cleanups={} failed_cleanups={}",
                        full_service, cleanup.cleanups, cleanup.failed_cleanups
                    );
                    service.publisher_builder().create().map_err(|retry_err| {
                        anyhow!(
                            "failed to create signal publisher after dead-node cleanup: service='{}', err={:?}, retry_err={:?}",
                            full_service,
                            err,
                            retry_err
                        )
                    })?
                } else {
                    return Err(err.into());
                }
            }
        };

        info!(
            "IceOryx publisher ready: {} mode={:?}",
            full_service, service_mode
        );

        Ok(Self {
            publisher: Some(publisher),
            full_service,
            suppress_trade_signal_publish,
            suppressed_publish_count: AtomicU64::new(0),
        })
    }
}

impl TradeSignalPublisher {
    pub fn new(channel_name: &str) -> Result<Self> {
        Self::new_with_prefix_mode(
            "signal_pubs",
            channel_name,
            PublisherServiceMode::OpenOrCreate,
        )
    }

    pub fn create(channel_name: &str) -> Result<Self> {
        Self::new_with_prefix_mode("signal_pubs", channel_name, PublisherServiceMode::Create)
    }

    pub fn open(channel_name: &str) -> Result<Self> {
        Self::new_with_prefix_mode("signal_pubs", channel_name, PublisherServiceMode::Open)
    }

    fn new_with_prefix_mode(
        prefix: &str,
        channel_name: &str,
        service_mode: PublisherServiceMode,
    ) -> Result<Self> {
        Ok(Self {
            publisher: TradeSignalPublisherInner::new_with_prefix_mode(
                prefix,
                channel_name,
                service_mode,
            )?,
        })
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        if data.len() > TRADE_SIGNAL_PAYLOAD {
            anyhow::bail!("Data size exceeds {} bytes", TRADE_SIGNAL_PAYLOAD);
        }

        if self.publisher.suppress_trade_signal_publish && signal_publish_dry_run_enabled() {
            let suppressed = self
                .publisher
                .suppressed_publish_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if suppressed == 1 || suppressed.is_multiple_of(100) {
                warn!(
                    "Signal publish suppressed (dry-run): service={} count={} {}",
                    self.publisher.full_service,
                    suppressed,
                    describe_signal_payload(data)
                );
            }
            return Ok(());
        }

        let publisher = self.publisher.publisher.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "publisher unavailable for service={}",
                self.publisher.full_service
            )
        })?;
        let mut sample = publisher.loan_uninit()?;
        TradeSignalIpcPayload::write_raw_to_uninit_slot(sample.payload_mut(), data)
            .ok_or_else(|| anyhow!("trade signal exceeds ipc payload: len={}", data.len()))?;
        let sample = unsafe { sample.assume_init() };
        sample.send()?;
        Ok(())
    }

    pub fn publish_trade_signal_parts(
        &self,
        signal_type: SignalType,
        generation_time: i64,
        handle_time: f64,
        context: &[u8],
    ) -> Result<()> {
        let total_len = signal_common::trade_signal::TRADE_SIGNAL_HEADER_LEN
            .checked_add(context.len())
            .ok_or_else(|| anyhow!("trade signal payload size overflow"))?;
        if total_len > TRADE_SIGNAL_PAYLOAD {
            anyhow::bail!(
                "trade signal payload {} exceeds {} bytes",
                total_len,
                TRADE_SIGNAL_PAYLOAD
            );
        }

        if self.publisher.suppress_trade_signal_publish && signal_publish_dry_run_enabled() {
            let suppressed = self
                .publisher
                .suppressed_publish_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if suppressed == 1 || suppressed.is_multiple_of(100) {
                warn!(
                    "Signal publish suppressed (dry-run): service={} count={} signal_type={} generation_time={} payload_bytes={}",
                    self.publisher.full_service,
                    suppressed,
                    signal_type.as_str(),
                    generation_time,
                    total_len
                );
            }
            return Ok(());
        }

        let publisher = self.publisher.publisher.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "publisher unavailable for service={}",
                self.publisher.full_service
            )
        })?;
        let mut sample = publisher.loan_uninit()?;
        TradeSignalIpcPayload::write_parts_to_uninit_slot(
            sample.payload_mut(),
            signal_type,
            generation_time,
            handle_time,
            context,
        )
        .ok_or_else(|| anyhow!("trade signal payload size invalid: len={}", total_len))?;
        let sample = unsafe { sample.assume_init() };
        sample.send()?;
        Ok(())
    }
}

pub type SignalPublisher = GenericPublisher<SIGNAL_PAYLOAD>;
pub type ResamplePublisher = GenericPublisher<RESAMPLE_PAYLOAD>;
// 通用持久化发布器（支持所有交易所）
pub type TradeUpdatePublisher = GenericPublisher<SIGNAL_PAYLOAD>;
pub type OrderUpdatePublisher = GenericPublisher<SIGNAL_PAYLOAD>;
pub type UniformOrderPublisher = GenericPublisher<SIGNAL_PAYLOAD>;
