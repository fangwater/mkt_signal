#[cfg(test)]
use account_monitor_common::hyperliquid_account::subscription_messages;
use account_monitor_common::hyperliquid_account::subscription_messages_for_catalog;
use account_monitor_common::hyperliquid_account::{
    fetch_frontend_open_orders, fetch_historical_orders, fetch_twap_history,
    fetch_user_abstraction, fetch_user_abstraction_raw, fetch_user_fills_by_time,
    fetch_user_funding_by_time, fetch_user_non_funding_ledger_updates_by_time, fetch_user_role,
    fetch_user_twap_slice_fills_by_time, normalize_hyperliquid_address, resolve_user_abstraction,
    FillSnapshotContext, FillSnapshotPolicy, HyperliquidAccountMode, HyperliquidAccountProcessor,
    HyperliquidAssetCatalog, HyperliquidFactWatermarks, HyperliquidSubscriptionAcks,
    HyperliquidSubscriptionControl, HyperliquidUserRole,
};
use account_monitor_common::pm_forwarder::PmForwarder;
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use futures_util::future::try_join_all;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::{NodeBuilder, NodeName, ServiceName};
use iceoryx2::service::ipc;
use log::{error, info, warn};
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope,
};
use mkt_parsers::msg::hyperliquid_account_msg::{
    hyperliquid_account_identity_hash, HyperliquidBasicFillMsg, HyperliquidBasicOrderMsg,
    HyperliquidFactIdentity, HyperliquidFactReplayControlMsg, HyperliquidFactReplayPhase,
    HyperliquidFactReplayRequestMsg, HyperliquidFundingMsg, HyperliquidLedgerMsg,
    HyperliquidSnapshotCompleteMsg, HyperliquidSnapshotPath, HyperliquidSnapshotPhase,
    HyperliquidTwapHistoryMsg, HyperliquidTwapSliceFillMsg, HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN,
    HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN, HYPERLIQUID_FACT_REPLAY_REQUEST_SERVICE,
};
use mkt_parsers::msg::hyperliquid_native_msg::HyperliquidNativeEventMsg;
use mkt_signal::connection::hyperliquid_conn::{
    parse_connection_generation_notification, HyperliquidConnection,
};
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::mkt_cfg::load_local_ips_preferring_trade_engine;
use runtime_common::ws_connection::{MktConnection, MktConnectionHandler};
use serde_json::Value;
use signal_common::hyperliquid::HyperliquidEndpoints;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};

const DEFAULT_PRIMARY_STATE_SILENCE_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_ACCOUNT_SNAPSHOT_TTL: Duration = Duration::from_secs(60);
const DEFAULT_STATE_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RUNTIME_DRIFT_CHECK_INTERVAL: Duration = Duration::from_secs(45);
const RUNTIME_DRIFT_RETRY_DELAY: Duration = Duration::from_secs(5);
const RUNTIME_DRIFT_MIN_REQUEST_BUDGET: Duration = Duration::from_secs(2);
const PRIVATE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_RESTART_DELAY: Duration = Duration::from_millis(250);
const BOOTSTRAP_RETRY_DELAY: Duration = Duration::from_secs(1);
const BOOTSTRAP_BUFFER_MESSAGE_CAPACITY: usize = 8_192;
const BOOTSTRAP_BUFFER_BYTE_CAPACITY: usize = 32 * 1024 * 1024;
const USER_FILLS_PAGE_CAPACITY: usize = 2_000;
const USER_FILLS_RETENTION_CAPACITY: usize = 10_000;
const TWAP_SLICE_FILLS_PAGE_CAPACITY: usize = 2_000;
const TIME_HISTORY_PAGE_CAPACITY: usize = 500;
const TIME_HISTORY_MAX_PAGES: usize = 256;
const TIME_HISTORY_MAX_ROWS: usize = TIME_HISTORY_PAGE_CAPACITY * TIME_HISTORY_MAX_PAGES;
const DEFAULT_FACT_RECOVERY_LOOKBACK: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const FACT_RECOVERY_INCLUSIVE_OVERLAP: Duration = Duration::from_secs(60);
const FACT_REPLAY_RING_MESSAGE_CAPACITY: usize = 32_768;
const FACT_REPLAY_RING_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
const FACT_REPLAY_REQUEST_HISTORY_SIZE: usize = 64;
const FACT_REPLAY_REQUEST_MAX_PUBLISHERS: usize = 8;
const FACT_REPLAY_REQUEST_SUBSCRIBER_BUFFER: usize = 256;
const FACT_REPLAY_TRANSACTION_MAX_FACTS: u64 = 256;

#[derive(Parser, Debug)]
#[command(name = "hyperliquid_account_monitor")]
#[command(about = "Hyperliquid public-address account monitor")]
struct Args {
    /// Bind the runtime thread to a CPU core. Falls back to ACCOUNT_MONITOR_CORE.
    #[arg(long)]
    core: Option<usize>,
}

struct DirectAccountState {
    processor: HyperliquidAccountProcessor,
    forwarder: PmForwarder,
    factual_outbox: VecDeque<Bytes>,
    fact_replay: HyperliquidFactReplayProducer,
    fact_replay_requests: HyperliquidFactReplayRequestReceiver,
    state_sources: StateSourceArbiter,
    path_generations: [u64; 2],
}

#[derive(Debug, Clone)]
struct HyperliquidFactReplayEntry {
    seq: u64,
    payload: Bytes,
}

#[derive(Debug, Clone)]
struct HyperliquidFactReplayProducer {
    account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    monitor_id: u64,
    next_seq: u64,
    ring: VecDeque<HyperliquidFactReplayEntry>,
    ring_bytes: usize,
    message_capacity: usize,
    byte_capacity: usize,
    control_scope: BasicAccountScope,
}

struct HyperliquidFactReplayRequestReceiver {
    subscriber: Subscriber<ipc::Service, [u8; HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN], ()>,
}

impl HyperliquidFactReplayProducer {
    fn new(
        account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
        monitor_id: u64,
        account_mode: HyperliquidAccountMode,
    ) -> Self {
        Self {
            account_hash,
            monitor_id,
            next_seq: 1,
            ring: VecDeque::new(),
            ring_bytes: 0,
            message_capacity: FACT_REPLAY_RING_MESSAGE_CAPACITY,
            byte_capacity: FACT_REPLAY_RING_BYTE_CAPACITY,
            control_scope: account_mode.perp_scope(),
        }
    }

    fn stamp_events(&mut self, events: Vec<Bytes>) -> Result<Vec<Bytes>> {
        let mut stamped = Vec::with_capacity(events.len());
        for event in events {
            let Some((event_type, scope, body)) = split_basic_account_event(&event) else {
                anyhow::bail!("processor emitted malformed Hyperliquid account event");
            };
            let stamped_event = match event_type {
                BasicAccountEventType::OrderUpdate => {
                    let msg = HyperliquidBasicOrderMsg::from_bytes(body)
                        .context("decode Hyperliquid factual order before sequencing")?;
                    self.stamp_order(scope, msg)?
                }
                BasicAccountEventType::HyperliquidFill => {
                    let msg = HyperliquidBasicFillMsg::from_bytes(body)
                        .context("decode Hyperliquid factual fill before sequencing")?;
                    self.stamp_fill(scope, msg)?
                }
                BasicAccountEventType::HyperliquidFunding => {
                    let msg = HyperliquidFundingMsg::from_bytes(body)
                        .context("decode Hyperliquid factual funding before sequencing")?;
                    self.stamp_funding(scope, msg)?
                }
                BasicAccountEventType::HyperliquidLedger => {
                    let msg = HyperliquidLedgerMsg::from_bytes(body)
                        .context("decode Hyperliquid factual ledger before sequencing")?;
                    self.stamp_ledger(scope, msg)?
                }
                BasicAccountEventType::HyperliquidTwapSliceFill => {
                    let msg = HyperliquidTwapSliceFillMsg::from_bytes(body)
                        .context("decode Hyperliquid factual TWAP slice before sequencing")?;
                    self.stamp_twap_slice_fill(scope, msg)?
                }
                BasicAccountEventType::HyperliquidTwapHistory => {
                    let msg = HyperliquidTwapHistoryMsg::from_bytes(body)
                        .context("decode Hyperliquid factual TWAP history before sequencing")?;
                    self.stamp_twap_history(scope, msg)?
                }
                BasicAccountEventType::HyperliquidNativeEvent => {
                    let msg = HyperliquidNativeEventMsg::from_bytes(body)?;
                    self.ensure_unstamped(msg.identity)?;
                    let identity = self.allocate_identity()?;
                    let event = BasicAccountEventMsg::create(
                        event_type,
                        scope,
                        msg.with_fact_identity(identity).to_bytes(),
                    )
                    .to_bytes();
                    self.retain(identity.fact_seq, event.clone())?;
                    event
                }
                _ => event,
            };
            stamped.push(stamped_event);
        }
        Ok(stamped)
    }

    fn stamp_order(
        &mut self,
        scope: BasicAccountScope,
        msg: HyperliquidBasicOrderMsg,
    ) -> Result<Bytes> {
        self.ensure_unstamped(msg.fact_identity())?;
        let identity = self.allocate_identity()?;
        let body = msg.with_fact_identity(identity).to_bytes();
        let event = BasicAccountEventMsg::create(BasicAccountEventType::OrderUpdate, scope, body)
            .to_bytes();
        self.retain(identity.fact_seq, event.clone())?;
        Ok(event)
    }

    fn stamp_fill(
        &mut self,
        scope: BasicAccountScope,
        msg: HyperliquidBasicFillMsg,
    ) -> Result<Bytes> {
        self.ensure_unstamped(msg.fact_identity())?;
        let identity = self.allocate_identity()?;
        let body = msg.with_fact_identity(identity).to_bytes();
        let event =
            BasicAccountEventMsg::create(BasicAccountEventType::HyperliquidFill, scope, body)
                .to_bytes();
        self.retain(identity.fact_seq, event.clone())?;
        Ok(event)
    }

    fn stamp_funding(
        &mut self,
        scope: BasicAccountScope,
        msg: HyperliquidFundingMsg,
    ) -> Result<Bytes> {
        self.ensure_unstamped(msg.fact_identity())?;
        let identity = self.allocate_identity()?;
        let body = msg.with_fact_identity(identity).to_bytes();
        let event =
            BasicAccountEventMsg::create(BasicAccountEventType::HyperliquidFunding, scope, body)
                .to_bytes();
        self.retain(identity.fact_seq, event.clone())?;
        Ok(event)
    }

    fn stamp_ledger(
        &mut self,
        scope: BasicAccountScope,
        msg: HyperliquidLedgerMsg,
    ) -> Result<Bytes> {
        self.ensure_unstamped(msg.fact_identity())?;
        let identity = self.allocate_identity()?;
        let body = msg.with_fact_identity(identity).to_bytes();
        let event =
            BasicAccountEventMsg::create(BasicAccountEventType::HyperliquidLedger, scope, body)
                .to_bytes();
        self.retain(identity.fact_seq, event.clone())?;
        Ok(event)
    }

    fn stamp_twap_slice_fill(
        &mut self,
        scope: BasicAccountScope,
        msg: HyperliquidTwapSliceFillMsg,
    ) -> Result<Bytes> {
        self.ensure_unstamped(msg.fact_identity())?;
        let identity = self.allocate_identity()?;
        let body = msg.with_fact_identity(identity).to_bytes();
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidTwapSliceFill,
            scope,
            body,
        )
        .to_bytes();
        self.retain(identity.fact_seq, event.clone())?;
        Ok(event)
    }

    fn stamp_twap_history(
        &mut self,
        scope: BasicAccountScope,
        msg: HyperliquidTwapHistoryMsg,
    ) -> Result<Bytes> {
        self.ensure_unstamped(msg.fact_identity())?;
        let identity = self.allocate_identity()?;
        let body = msg.with_fact_identity(identity).to_bytes();
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidTwapHistory,
            scope,
            body,
        )
        .to_bytes();
        self.retain(identity.fact_seq, event.clone())?;
        Ok(event)
    }

    fn ensure_unstamped(&self, identity: HyperliquidFactIdentity) -> Result<()> {
        if identity.monitor_id != 0
            || identity.fact_seq != 0
            || identity.account_hash != [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]
        {
            anyhow::bail!("processor emitted an already-stamped Hyperliquid factual event");
        }
        Ok(())
    }

    fn allocate_identity(&mut self) -> Result<HyperliquidFactIdentity> {
        let fact_seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .context("Hyperliquid factual sequence exhausted")?;
        Ok(HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq,
        })
    }

    fn retain(&mut self, seq: u64, payload: Bytes) -> Result<()> {
        if payload.len() > self.byte_capacity {
            anyhow::bail!(
                "Hyperliquid factual event exceeds replay byte capacity: {} > {}",
                payload.len(),
                self.byte_capacity
            );
        }
        self.ring_bytes = self
            .ring_bytes
            .checked_add(payload.len())
            .context("Hyperliquid factual replay byte count overflow")?;
        self.ring
            .push_back(HyperliquidFactReplayEntry { seq, payload });
        while self.ring.len() > self.message_capacity || self.ring_bytes > self.byte_capacity {
            let evicted = self
                .ring
                .pop_front()
                .expect("non-empty replay ring while enforcing bounds");
            self.ring_bytes = self.ring_bytes.saturating_sub(evicted.payload.len());
        }
        Ok(())
    }

    fn head_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    fn earliest_seq(&self) -> u64 {
        self.ring
            .front()
            .map(|entry| entry.seq)
            .unwrap_or_else(|| self.next_seq)
    }

    fn control_event(
        &self,
        phase: HyperliquidFactReplayPhase,
        request: &HyperliquidFactReplayRequestMsg,
        first_seq: u64,
        last_seq: u64,
        head_seq: u64,
    ) -> Bytes {
        let control = HyperliquidFactReplayControlMsg::create(
            phase,
            self.account_hash,
            self.monitor_id,
            request.consumer_id,
            request.request_id,
            first_seq,
            last_seq,
            head_seq,
        );
        BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidFactReplayControl,
            self.control_scope,
            control.to_bytes(),
        )
        .to_bytes()
    }

    fn serve_request(
        &self,
        forwarder: &mut PmForwarder,
        request: &HyperliquidFactReplayRequestMsg,
    ) -> bool {
        let Ok((requested_first, transaction_last, head)) = self.replay_transaction_range(request)
        else {
            return self.send_gap(forwarder, request);
        };

        let begin = self.control_event(
            HyperliquidFactReplayPhase::Begin,
            request,
            requested_first,
            transaction_last,
            head,
        );
        if !forwarder.send_raw(&begin) {
            return false;
        }
        for entry in self
            .ring
            .iter()
            .filter(|entry| entry.seq >= requested_first && entry.seq <= transaction_last)
        {
            if !forwarder.send_raw(&entry.payload) {
                return false;
            }
        }
        let complete = self.control_event(
            HyperliquidFactReplayPhase::Complete,
            request,
            requested_first,
            transaction_last,
            head,
        );
        forwarder.send_raw(&complete)
    }

    fn replay_transaction_range(
        &self,
        request: &HyperliquidFactReplayRequestMsg,
    ) -> std::result::Result<(u64, u64, u64), ()> {
        let (requested_first, head) = self.replay_range(request)?;
        let transaction_last = if requested_first <= head {
            requested_first
                .saturating_add(FACT_REPLAY_TRANSACTION_MAX_FACTS - 1)
                .min(head)
        } else {
            head
        };
        Ok((requested_first, transaction_last, head))
    }

    fn replay_range(
        &self,
        request: &HyperliquidFactReplayRequestMsg,
    ) -> std::result::Result<(u64, u64), ()> {
        let head = self.head_seq();
        let requested_first = if request.last_monitor_id == self.monitor_id {
            match request.last_fact_seq.checked_add(1) {
                Some(value) if request.last_fact_seq <= head => value,
                _ => return Err(()),
            }
        } else if request.last_monitor_id == 0 && request.last_fact_seq == 0 {
            1
        } else {
            return Err(());
        };
        if request.account_hash != self.account_hash
            || (requested_first <= head && self.earliest_seq() > requested_first)
        {
            Err(())
        } else {
            Ok((requested_first, head))
        }
    }

    fn send_gap(
        &self,
        forwarder: &mut PmForwarder,
        request: &HyperliquidFactReplayRequestMsg,
    ) -> bool {
        let gap = self.control_event(
            HyperliquidFactReplayPhase::Gap,
            request,
            self.earliest_seq(),
            self.head_seq(),
            self.head_seq(),
        );
        forwarder.send_raw(&gap)
    }
}

impl HyperliquidFactReplayRequestReceiver {
    fn new() -> Result<Self> {
        let service_name = build_service_name(HYPERLIQUID_FACT_REPLAY_REQUEST_SERVICE);
        let node = NodeBuilder::new()
            .name(&NodeName::new("hyperliquid_fact_replay_responder")?)
            .create::<ipc::Service>()?;
        let service_name = ServiceName::new(&service_name)?;
        let service = node
            .service_builder(&service_name)
            .publish_subscribe::<[u8; HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN]>()
            .max_publishers(FACT_REPLAY_REQUEST_MAX_PUBLISHERS)
            .max_subscribers(1)
            .history_size(FACT_REPLAY_REQUEST_HISTORY_SIZE)
            .subscriber_max_buffer_size(FACT_REPLAY_REQUEST_SUBSCRIBER_BUFFER)
            .open_or_create()?;
        Ok(Self {
            subscriber: service.subscriber_builder().create()?,
        })
    }

    fn drain(&self, limit: usize) -> Vec<HyperliquidFactReplayRequestMsg> {
        let mut requests = Vec::new();
        while requests.len() < limit {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    match HyperliquidFactReplayRequestMsg::from_ipc_payload(sample.payload()) {
                        Ok(request) if request.consumer_id != 0 && request.request_id != 0 => {
                            requests.push(request)
                        }
                        Ok(_) => warn!("drop Hyperliquid fact replay request with zero identity"),
                        Err(err) => warn!("drop invalid Hyperliquid fact replay request: {err:#}"),
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("Hyperliquid fact replay request receive failed: {err}");
                    break;
                }
            }
        }
        requests
    }
}

struct InitialStreamSession {
    raw_rx: broadcast::Receiver<Bytes>,
    runner_handle: tokio::task::JoinHandle<()>,
    protocol: InitialSessionProtocol,
}

struct InitialSessionProtocol {
    subscription_acks: HyperliquidSubscriptionAcks,
    generation: u64,
    batch_id: u64,
}

struct PrimaryBootstrap {
    processor: HyperliquidAccountProcessor,
    processed_frames: Vec<BootstrapProcessedFrame>,
    initial_session: InitialStreamSession,
    historical_seed_count: usize,
    frontend_seed_count: usize,
    recovered_fill_count: usize,
    recovered_funding_count: usize,
    recovered_ledger_count: usize,
    buffered_frame_count: usize,
    buffered_bytes: usize,
}

struct BootstrapCut {
    processor: HyperliquidAccountProcessor,
    processed_frames: Vec<BootstrapProcessedFrame>,
    subscription_acks: HyperliquidSubscriptionAcks,
    generation: u64,
    historical_seed_count: usize,
    frontend_seed_count: usize,
    recovered_fill_count: usize,
    recovered_funding_count: usize,
    recovered_ledger_count: usize,
    buffered_frame_count: usize,
    buffered_bytes: usize,
}

struct BootstrapProcessedFrame {
    state_stream: Option<AccountStateStream>,
    events: Vec<Bytes>,
}

struct BootstrapHttpSnapshot {
    historical_orders: Value,
    frontend_open_orders: Vec<(String, Value)>,
}

struct BootstrapHttpCut {
    orders: BootstrapHttpSnapshot,
    facts: RecoveredAccountFacts,
    borrowing: Option<(Value, i64)>,
}

struct ConnectionHttpCut {
    cut: BootstrapHttpCut,
    required_active_order_ids: HashSet<i64>,
}

#[derive(Debug)]
struct AppliedConnectionCut {
    historical_seed_count: usize,
    frontend_seed_count: usize,
    recovered_fill_count: usize,
    recovered_funding_count: usize,
    recovered_ledger_count: usize,
}

struct AppliedBootstrapCut {
    processor: HyperliquidAccountProcessor,
    processed_frames: Vec<BootstrapProcessedFrame>,
    historical_seed_count: usize,
    frontend_seed_count: usize,
    recovered_fill_count: usize,
    recovered_funding_count: usize,
    recovered_ledger_count: usize,
}

#[derive(Debug)]
struct RecoveredAccountFacts {
    fills: Value,
    fundings: Vec<Value>,
    ledger_updates: Vec<Value>,
    twap_slice_fills: Vec<Value>,
    twap_history: Vec<Value>,
}

#[derive(Debug, Clone, Copy)]
enum TimeHistoryKind {
    Funding,
    NonFundingLedger,
}

impl TimeHistoryKind {
    fn label(self) -> &'static str {
        match self {
            Self::Funding => "userFunding",
            Self::NonFundingLedger => "userNonFundingLedgerUpdates",
        }
    }
}

#[derive(Default)]
struct BootstrapFrameBuffer {
    frames: Vec<Bytes>,
    bytes: usize,
}

impl BootstrapFrameBuffer {
    fn push(&mut self, payload: Bytes) -> Result<()> {
        if self.frames.len() >= BOOTSTRAP_BUFFER_MESSAGE_CAPACITY {
            anyhow::bail!(
                "Hyperliquid bootstrap private-frame buffer reached its hard message cap ({BOOTSTRAP_BUFFER_MESSAGE_CAPACITY})"
            );
        }
        let next_bytes = self
            .bytes
            .checked_add(payload.len())
            .context("Hyperliquid bootstrap private-frame byte count overflow")?;
        if next_bytes > BOOTSTRAP_BUFFER_BYTE_CAPACITY {
            anyhow::bail!(
                "Hyperliquid bootstrap private-frame buffer exceeded its hard byte cap ({BOOTSTRAP_BUFFER_BYTE_CAPACITY})"
            );
        }
        self.bytes = next_bytes;
        self.frames.push(payload);
        Ok(())
    }
}

struct BootstrapProtocolObserver {
    subscription_acks: HyperliquidSubscriptionAcks,
    connection_generation: Option<u64>,
    initial_user_fills_snapshot_seen: bool,
    user: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamPath {
    Primary,
    Secondary,
}

impl StreamPath {
    fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        }
    }

    fn snapshot_path(self) -> HyperliquidSnapshotPath {
        match self {
            Self::Primary => HyperliquidSnapshotPath::Primary,
            Self::Secondary => HyperliquidSnapshotPath::Secondary,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Primary => 0,
            Self::Secondary => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountStateStream {
    Spot,
    Perp,
}

impl AccountStateStream {
    fn from_channel(channel: &str) -> Option<Self> {
        match channel {
            "spotState" => Some(Self::Spot),
            "clearinghouseState" | "allDexsClearinghouseState" => Some(Self::Perp),
            _ => None,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Spot => 0,
            Self::Perp => 1,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spotState",
            Self::Perp => "perpState",
        }
    }

    fn venue(self) -> TradingVenue {
        match self {
            Self::Spot => TradingVenue::HyperliquidMargin,
            Self::Perp => TradingVenue::HyperliquidFutures,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateSourceTransition {
    None,
    SecondaryActivated,
    PrimaryRestored,
}

#[derive(Debug)]
struct StateSourceArbiter {
    primary_last_success: [Option<Instant>; 2],
    secondary_active: [bool; 2],
    primary_silence_timeout: Duration,
}

impl StateSourceArbiter {
    fn new(primary_silence_timeout: Duration) -> Self {
        Self {
            primary_last_success: [None, None],
            secondary_active: [false, false],
            primary_silence_timeout,
        }
    }

    fn allows(&self, path: StreamPath, stream: AccountStateStream, now: Instant) -> bool {
        if path == StreamPath::Primary {
            return true;
        }
        self.primary_last_success[stream.index()].is_none_or(|baseline| {
            now.saturating_duration_since(baseline) >= self.primary_silence_timeout
        })
    }

    fn record_success(
        &mut self,
        path: StreamPath,
        stream: AccountStateStream,
        now: Instant,
    ) -> StateSourceTransition {
        let index = stream.index();
        match path {
            StreamPath::Primary => {
                self.primary_last_success[index] = Some(now);
                if std::mem::take(&mut self.secondary_active[index]) {
                    StateSourceTransition::PrimaryRestored
                } else {
                    StateSourceTransition::None
                }
            }
            StreamPath::Secondary => {
                if self.secondary_active[index] {
                    StateSourceTransition::None
                } else {
                    self.secondary_active[index] = true;
                    StateSourceTransition::SecondaryActivated
                }
            }
        }
    }

    fn invalidate_path(&mut self, path: StreamPath) {
        match path {
            StreamPath::Primary => self.primary_last_success = [None, None],
            StreamPath::Secondary => self.secondary_active = [false, false],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionDisposition {
    Continue,
    StateObserved(AccountStateStream),
    Restart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeDriftReason {
    AccountRoleChanged,
    AccountModeChanged,
    AssetCatalogChanged,
    ValidationExpired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeDriftDecision {
    Continue,
    Restart(RuntimeDriftReason),
}

#[derive(Debug)]
struct RuntimeDriftGuard {
    startup_role: HyperliquidUserRole,
    startup_mode: HyperliquidAccountMode,
    last_success_at: Instant,
    validation_ttl: Duration,
}

impl RuntimeDriftGuard {
    fn new(
        startup_role: HyperliquidUserRole,
        startup_mode: HyperliquidAccountMode,
        last_success_at: Instant,
        validation_ttl: Duration,
    ) -> Self {
        Self {
            startup_role,
            startup_mode,
            last_success_at,
            validation_ttl,
        }
    }

    fn observe_role(&self, current_role: HyperliquidUserRole) -> RuntimeDriftDecision {
        if current_role != self.startup_role {
            RuntimeDriftDecision::Restart(RuntimeDriftReason::AccountRoleChanged)
        } else {
            RuntimeDriftDecision::Continue
        }
    }

    fn observe_success(
        &mut self,
        now: Instant,
        current_mode: HyperliquidAccountMode,
        catalog_unchanged: bool,
    ) -> RuntimeDriftDecision {
        if current_mode != self.startup_mode {
            return RuntimeDriftDecision::Restart(RuntimeDriftReason::AccountModeChanged);
        }
        if !catalog_unchanged {
            return RuntimeDriftDecision::Restart(RuntimeDriftReason::AssetCatalogChanged);
        }
        self.last_success_at = now;
        RuntimeDriftDecision::Continue
    }

    fn observe_failure(&self, now: Instant) -> RuntimeDriftDecision {
        if now.saturating_duration_since(self.last_success_at) >= self.validation_ttl {
            RuntimeDriftDecision::Restart(RuntimeDriftReason::ValidationExpired)
        } else {
            RuntimeDriftDecision::Continue
        }
    }

    fn validation_age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.last_success_at)
    }

    fn validation_remaining(&self, now: Instant) -> Duration {
        self.validation_ttl.saturating_sub(self.validation_age(now))
    }

    fn validation_deadline(&self) -> Instant {
        self.last_success_at
            .checked_add(self.validation_ttl)
            .unwrap_or(self.last_success_at)
    }
}

thread_local! {
    static DIRECT_STATE: RefCell<Option<DirectAccountState>> = const { RefCell::new(None) };
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "ACCOUNT_MONITOR_CORE")?;

    let user = normalize_hyperliquid_address(
        &std::env::var("HYPERLIQUID_ACCOUNT_ADDRESS")
            .context("HYPERLIQUID_ACCOUNT_ADDRESS is required")?,
    )?;
    let endpoints = HyperliquidEndpoints::from_env()?;
    let account_hash = hyperliquid_account_identity_hash(&user, endpoints.testnet)?;
    let primary_state_silence_timeout = env_duration_ms(
        "HYPERLIQUID_PRIMARY_STATE_SILENCE_TIMEOUT_MS",
        DEFAULT_PRIMARY_STATE_SILENCE_TIMEOUT,
    )?;
    let snapshot_ttl = env_duration_ms(
        "HYPERLIQUID_ACCOUNT_SNAPSHOT_TTL_MS",
        DEFAULT_ACCOUNT_SNAPSHOT_TTL,
    )?;
    let state_refresh_timeout = env_duration_ms(
        "HYPERLIQUID_ACCOUNT_STATE_REFRESH_MS",
        DEFAULT_STATE_REFRESH_TIMEOUT,
    )?;
    let runtime_drift_check_interval = env_duration_ms(
        "HYPERLIQUID_ACCOUNT_DRIFT_CHECK_MS",
        DEFAULT_RUNTIME_DRIFT_CHECK_INTERVAL,
    )?;
    let fact_recovery_lookback = env_duration_ms(
        "HYPERLIQUID_FACT_RECOVERY_LOOKBACK_MS",
        DEFAULT_FACT_RECOVERY_LOOKBACK,
    )?;
    if state_refresh_timeout >= snapshot_ttl {
        anyhow::bail!(
            "HYPERLIQUID_ACCOUNT_STATE_REFRESH_MS ({}) must be less than HYPERLIQUID_ACCOUNT_SNAPSHOT_TTL_MS ({})",
            state_refresh_timeout.as_millis(),
            snapshot_ttl.as_millis()
        );
    }
    validate_runtime_drift_interval(runtime_drift_check_interval, snapshot_ttl)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("build Hyperliquid info client")?;
    let (catalog, user_role) = tokio::try_join!(
        HyperliquidAssetCatalog::fetch(&client, &endpoints.info_url),
        fetch_user_role(&client, &endpoints.info_url, &user),
    )?;
    let detected_mode =
        fetch_user_abstraction(&client, &endpoints.info_url, &user, user_role).await?;
    info!(
        "Hyperliquid account mode detected: role={} mode={} testnet={} instruments={}",
        user_role.as_str(),
        detected_mode.as_str(),
        endpoints.testnet,
        catalog.len()
    );
    // Process initial-session userFills snapshots by default. Explicit false
    // keeps baseline-only startup; reconnect snapshots still recover unseen
    // fills through stable venue identities.
    let fill_snapshot_setting = std::env::var("HYPERLIQUID_PROCESS_FILL_SNAPSHOT").ok();
    let fill_snapshot_policy =
        fill_snapshot_policy_from_env_value(fill_snapshot_setting.as_deref())?;
    let subscriptions = subscription_messages_for_catalog(&user, detected_mode, &catalog)?;
    let ((primary_ip, secondary_ip), ip_source) = load_local_ips_preferring_trade_engine().await?;
    validate_private_subscription_budget(subscriptions.len(), &primary_ip, &secondary_ip)?;
    let monitor_id = monitor_instance_id();
    info!(
        "Hyperliquid account streams configured: primary_ip='{}' secondary_ip='{}' source={} primary_state_silence_timeout_ms={} state_refresh_ms={} snapshot_ttl_ms={} drift_check_ms={} fact_recovery_lookback_ms={} monitor_id={}",
        primary_ip,
        secondary_ip,
        ip_source,
        primary_state_silence_timeout.as_millis(),
        state_refresh_timeout.as_millis(),
        snapshot_ttl.as_millis(),
        runtime_drift_check_interval.as_millis(),
        fact_recovery_lookback.as_millis(),
        monitor_id,
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let perp_dexes = catalog.perp_dexes();
    let mut bootstrap = bootstrap_primary(
        &client,
        &endpoints.info_url,
        &endpoints.ws_url,
        &primary_ip,
        &subscriptions,
        shutdown_rx.clone(),
        &user,
        &catalog,
        &perp_dexes,
        detected_mode,
        fill_snapshot_policy,
        fact_recovery_lookback,
    )
    .await?;

    let runtime_epoch_verified_at = Instant::now();
    let initial_generation = bootstrap.initial_session.protocol.generation;
    let mut direct_state = DirectAccountState {
        processor: bootstrap.processor,
        forwarder: PmForwarder::new_non_overflowing("hyperliquid")?,
        factual_outbox: VecDeque::new(),
        fact_replay: HyperliquidFactReplayProducer::new(account_hash, monitor_id, detected_mode),
        fact_replay_requests: HyperliquidFactReplayRequestReceiver::new()?,
        state_sources: StateSourceArbiter::new(primary_state_silence_timeout),
        path_generations: [initial_generation, 0],
    };
    let initial_batch_id = publish_bootstrap_frames(
        &mut direct_state,
        std::mem::take(&mut bootstrap.processed_frames),
        detected_mode,
        monitor_id,
        initial_generation,
        snapshot_ttl,
    )?;
    bootstrap.initial_session.protocol.batch_id = initial_batch_id;
    info!(
        "Hyperliquid authoritative startup cut accepted: historical_internal_orders={} frontend_internal_orders={} recovered_fills={} recovered_fundings={} recovered_ledger_updates={} buffered_private_frames={} buffered_bytes={} generation={}",
        bootstrap.historical_seed_count,
        bootstrap.frontend_seed_count,
        bootstrap.recovered_fill_count,
        bootstrap.recovered_funding_count,
        bootstrap.recovered_ledger_count,
        bootstrap.buffered_frame_count,
        bootstrap.buffered_bytes,
        bootstrap.initial_session.protocol.generation,
    );
    DIRECT_STATE.with(|cell| {
        *cell.borrow_mut() = Some(direct_state);
    });

    let mut handles = Vec::new();
    handles.extend(spawn_stream_path(
        StreamPath::Primary,
        endpoints.ws_url.clone(),
        primary_ip,
        subscriptions.clone(),
        shutdown_rx.clone(),
        detected_mode,
        monitor_id,
        state_refresh_timeout,
        snapshot_ttl,
        client.clone(),
        endpoints.info_url.clone(),
        user.clone(),
        perp_dexes.clone(),
        fact_recovery_lookback,
        Some(bootstrap.initial_session),
    )?);
    handles.extend(spawn_stream_path(
        StreamPath::Secondary,
        endpoints.ws_url.clone(),
        secondary_ip,
        subscriptions,
        shutdown_rx,
        detected_mode,
        monitor_id,
        state_refresh_timeout,
        snapshot_ttl,
        client.clone(),
        endpoints.info_url.clone(),
        user.clone(),
        perp_dexes,
        fact_recovery_lookback,
        None,
    )?);

    let mut stats = tokio::time::interval(Duration::from_secs(30));
    let mut pending_fill_tick = tokio::time::interval(Duration::from_secs(1));
    let mut fact_replay_tick = tokio::time::interval(Duration::from_millis(20));
    let mut runtime_drift_tick = tokio::time::interval(runtime_drift_check_interval);
    let mut borrow_lend_tick = tokio::time::interval(Duration::from_secs(30));
    borrow_lend_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    runtime_drift_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut runtime_drift_guard = RuntimeDriftGuard::new(
        user_role,
        detected_mode,
        runtime_epoch_verified_at,
        snapshot_ttl,
    );
    let mut runtime_drift_error = None;
    stats.tick().await;
    pending_fill_tick.tick().await;
    fact_replay_tick.tick().await;
    loop {
        let runtime_validation_deadline =
            tokio::time::Instant::from_std(runtime_drift_guard.validation_deadline());
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                info!("received Ctrl-C; stopping Hyperliquid account monitor");
                break;
            }
            _ = stats.tick() => {
                DIRECT_STATE.with(|cell| {
                    if let Some(state) = cell.borrow_mut().as_mut() {
                        state.forwarder.log_stats();
                    }
                });
            }
            _ = pending_fill_tick.tick() => flush_pending_fills(),
            _ = borrow_lend_tick.tick(), if detected_mode == HyperliquidAccountMode::PortfolioMargin => {
                let fetch = |body: Value| {
                    let client = &client;
                    let url = &endpoints.info_url;
                    async move { client.post(url).json(&body).send().await?.error_for_status()?.json::<Value>().await }
                };
                match tokio::try_join!(
                    fetch(serde_json::json!({"type":"borrowLendUserState","user":user})),
                    fetch(serde_json::json!({"type":"allBorrowLendReserveStates"})),
                ) {
                    Ok((user_state, reserves)) => DIRECT_STATE.with(|cell| {
                        if let Some(state) = cell.borrow_mut().as_mut() {
                            if !flush_direct_factual_outbox(state) { return; }
                            match state.processor.process_borrow_lend_snapshot(&user_state, &reserves, chrono::Utc::now().timestamp_millis()) {
                                Ok(events) => { send_or_queue_direct_factual_events(state, events); }
                                Err(err) => warn!("Hyperliquid PM borrow/lend snapshot rejected: {err:#}"),
                            }
                        }
                    }),
                    Err(err) => warn!("Hyperliquid PM borrow/lend snapshot unavailable; account readiness expires with the last borrowing snapshot: {err:#}"),
                }
            }
            _ = fact_replay_tick.tick() => service_fact_replay_requests(),
            _ = tokio::time::sleep_until(runtime_validation_deadline) => {
                let reason = format!(
                    "Hyperliquid runtime account role/mode/catalog validation reached the snapshot TTL without a successful refresh: snapshot_ttl_ms={}",
                    snapshot_ttl.as_millis(),
                );
                begin_runtime_drift_restart(
                    &reason,
                    &shutdown_tx,
                    detected_mode,
                    monitor_id,
                    snapshot_ttl,
                );
                runtime_drift_error = Some(anyhow::anyhow!(reason));
                break;
            }
            _ = runtime_drift_tick.tick() => {
                let validation_started_at = Instant::now();
                let remaining = runtime_drift_guard.validation_remaining(validation_started_at);
                let refresh = tokio::time::timeout(remaining, async {
                    tokio::try_join!(
                        HyperliquidAssetCatalog::fetch(&client, &endpoints.info_url),
                        fetch_user_abstraction_raw(&client, &endpoints.info_url, &user),
                        fetch_user_role(&client, &endpoints.info_url, &user),
                    )
                })
                .await;
                let now = Instant::now();
                let mut restart = None;
                match refresh {
                    Ok(Ok((refreshed_catalog, abstraction, refreshed_role))) => {
                        if matches!(
                            runtime_drift_guard.observe_role(refreshed_role),
                            RuntimeDriftDecision::Restart(RuntimeDriftReason::AccountRoleChanged)
                        ) {
                            restart = Some(format!(
                                "Hyperliquid account role changed after startup: startup={} current={}",
                                user_role.as_str(),
                                refreshed_role.as_str(),
                            ));
                        } else {
                            match resolve_user_abstraction(
                                &abstraction,
                                refreshed_role,
                            ) {
                                Ok(refreshed_mode) => {
                                    let catalog_unchanged = refreshed_catalog == catalog;
                                    match runtime_drift_guard.observe_success(
                                        now,
                                        refreshed_mode,
                                        catalog_unchanged,
                                    ) {
                                        RuntimeDriftDecision::Continue => {
                                            log::debug!(
                                                "Hyperliquid runtime account role/mode/catalog validation succeeded"
                                            );
                                            runtime_drift_tick.reset();
                                        }
                                        RuntimeDriftDecision::Restart(
                                            RuntimeDriftReason::AccountModeChanged,
                                        ) => {
                                            restart = Some(format!(
                                                "Hyperliquid account mode changed after startup: startup={} current={}",
                                                detected_mode.as_str(),
                                                refreshed_mode.as_str(),
                                            ));
                                        }
                                        RuntimeDriftDecision::Restart(
                                            RuntimeDriftReason::AssetCatalogChanged,
                                        ) => {
                                            restart = Some(format!(
                                                "Hyperliquid account catalog changed after startup: instruments={} -> {} perp_dexes={:?} -> {:?}",
                                                catalog.len(),
                                                refreshed_catalog.len(),
                                                catalog.perp_dexes(),
                                                refreshed_catalog.perp_dexes(),
                                            ));
                                        }
                                        RuntimeDriftDecision::Restart(reason) => {
                                            restart = Some(format!(
                                                "unexpected Hyperliquid runtime drift decision after successful validation: {reason:?}"
                                            ));
                                        }
                                    }
                                }
                                Err(err) => {
                                    restart = Some(format!(
                                        "Hyperliquid account mode became unresolvable after startup: {err:#}"
                                    ));
                                }
                            }
                        }
                    }
                    Ok(Err(err)) => {
                        let age = runtime_drift_guard.validation_age(now);
                        match runtime_drift_guard.observe_failure(now) {
                            RuntimeDriftDecision::Continue => {
                                warn!(
                                    "Hyperliquid runtime account role/mode/catalog validation failed; retrying before snapshot TTL: validation_age_ms={} snapshot_ttl_ms={} err={err:#}",
                                    age.as_millis(),
                                    snapshot_ttl.as_millis(),
                                );
                                if let Some(delay) = runtime_drift_retry_delay(
                                    runtime_drift_guard.validation_remaining(now),
                                ) {
                                    runtime_drift_tick.reset_after(delay);
                                }
                            }
                            RuntimeDriftDecision::Restart(RuntimeDriftReason::ValidationExpired) => {
                                restart = Some(format!(
                                    "Hyperliquid runtime account role/mode/catalog validation remained unavailable for the snapshot TTL: validation_age_ms={} snapshot_ttl_ms={} last_error={err:#}",
                                    age.as_millis(),
                                    snapshot_ttl.as_millis(),
                                ));
                            }
                            RuntimeDriftDecision::Restart(reason) => {
                                restart = Some(format!(
                                    "unexpected Hyperliquid runtime drift decision after validation failure: {reason:?}"
                                ));
                            }
                        }
                    }
                    Err(_) => {
                        let age = runtime_drift_guard.validation_age(now);
                        match runtime_drift_guard.observe_failure(now) {
                            RuntimeDriftDecision::Continue => {
                                warn!(
                                    "Hyperliquid runtime account role/mode/catalog validation timed out; retrying before snapshot TTL: validation_age_ms={} snapshot_ttl_ms={}",
                                    age.as_millis(),
                                    snapshot_ttl.as_millis(),
                                );
                                if let Some(delay) = runtime_drift_retry_delay(
                                    runtime_drift_guard.validation_remaining(now),
                                ) {
                                    runtime_drift_tick.reset_after(delay);
                                }
                            }
                            RuntimeDriftDecision::Restart(RuntimeDriftReason::ValidationExpired) => {
                                restart = Some(format!(
                                    "Hyperliquid runtime account role/mode/catalog validation timed out at the snapshot TTL: validation_age_ms={} snapshot_ttl_ms={}",
                                    age.as_millis(),
                                    snapshot_ttl.as_millis(),
                                ));
                            }
                            RuntimeDriftDecision::Restart(reason) => {
                                restart = Some(format!(
                                    "unexpected Hyperliquid runtime drift decision after validation timeout: {reason:?}"
                                ));
                            }
                        }
                    }
                }
                if let Some(reason) = restart {
                    begin_runtime_drift_restart(
                        &reason,
                        &shutdown_tx,
                        detected_mode,
                        monitor_id,
                        snapshot_ttl,
                    );
                    runtime_drift_error = Some(anyhow::anyhow!(reason));
                    break;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    for handle in handles {
        match tokio::time::timeout(Duration::from_secs(5), handle).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!("Hyperliquid account stream task join failed: {err}"),
            Err(_) => warn!("Hyperliquid account stream task did not stop within 5 seconds"),
        }
    }
    info!("Hyperliquid account monitor stopped");
    if let Some(err) = runtime_drift_error {
        return Err(err).context(
            "Hyperliquid runtime epoch changed or could no longer be validated; restart required",
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn bootstrap_primary(
    client: &reqwest::Client,
    info_url: &str,
    ws_url: &str,
    primary_ip: &str,
    subscriptions: &[Value],
    mut shutdown_rx: watch::Receiver<bool>,
    user: &str,
    catalog: &HyperliquidAssetCatalog,
    perp_dexes: &[String],
    account_mode: HyperliquidAccountMode,
    fill_snapshot_policy: FillSnapshotPolicy,
    fact_recovery_lookback: Duration,
) -> Result<PrimaryBootstrap> {
    let mut attempt = 0_u64;
    loop {
        if *shutdown_rx.borrow() {
            anyhow::bail!("Hyperliquid account monitor stopped during startup bootstrap");
        }
        attempt = attempt.checked_add(1).unwrap_or(1);
        let (mut raw_rx, runner_handle) = start_account_connection(
            StreamPath::Primary,
            ws_url.to_string(),
            primary_ip.to_string(),
            subscriptions.to_vec(),
            shutdown_rx.clone(),
            BOOTSTRAP_BUFFER_MESSAGE_CAPACITY,
        );
        let result = perform_bootstrap_cut(
            client,
            info_url,
            &mut raw_rx,
            &mut shutdown_rx,
            subscriptions,
            user,
            catalog,
            perp_dexes,
            account_mode,
            fill_snapshot_policy,
            fact_recovery_lookback,
        )
        .await;
        match result {
            Ok(cut) => {
                info!(
                    "Hyperliquid primary startup bootstrap completed its authoritative HTTP/WS cut on attempt {}",
                    attempt
                );
                return Ok(PrimaryBootstrap {
                    processor: cut.processor,
                    processed_frames: cut.processed_frames,
                    initial_session: InitialStreamSession {
                        raw_rx,
                        runner_handle,
                        protocol: InitialSessionProtocol {
                            subscription_acks: cut.subscription_acks,
                            generation: cut.generation,
                            batch_id: 0,
                        },
                    },
                    historical_seed_count: cut.historical_seed_count,
                    frontend_seed_count: cut.frontend_seed_count,
                    recovered_fill_count: cut.recovered_fill_count,
                    recovered_funding_count: cut.recovered_funding_count,
                    recovered_ledger_count: cut.recovered_ledger_count,
                    buffered_frame_count: cut.buffered_frame_count,
                    buffered_bytes: cut.buffered_bytes,
                });
            }
            Err(err) => {
                runner_handle.abort();
                let _ = runner_handle.await;
                if *shutdown_rx.borrow() {
                    return Err(err).context("Hyperliquid startup bootstrap interrupted");
                }
                warn!(
                    "Hyperliquid primary startup bootstrap attempt {} rejected without committing processor state: {err:#}",
                    attempt
                );
                wait_bootstrap_retry(&mut shutdown_rx).await?;
            }
        }
    }
}

async fn wait_bootstrap_retry(shutdown_rx: &mut watch::Receiver<bool>) -> Result<()> {
    tokio::select! {
        _ = tokio::time::sleep(BOOTSTRAP_RETRY_DELAY) => Ok(()),
        changed = shutdown_rx.changed() => {
            if changed.is_err() || *shutdown_rx.borrow() {
                anyhow::bail!("Hyperliquid account monitor stopped during startup bootstrap");
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn perform_bootstrap_cut(
    client: &reqwest::Client,
    info_url: &str,
    raw_rx: &mut broadcast::Receiver<Bytes>,
    shutdown_rx: &mut watch::Receiver<bool>,
    subscriptions: &[Value],
    user: &str,
    catalog: &HyperliquidAssetCatalog,
    perp_dexes: &[String],
    account_mode: HyperliquidAccountMode,
    fill_snapshot_policy: FillSnapshotPolicy,
    fact_recovery_lookback: Duration,
) -> Result<BootstrapCut> {
    let mut observer = BootstrapProtocolObserver {
        subscription_acks: HyperliquidSubscriptionAcks::from_requests(subscriptions)?,
        connection_generation: None,
        initial_user_fills_snapshot_seen: false,
        user: user.to_string(),
    };
    let mut buffer = BootstrapFrameBuffer::default();
    wait_for_bootstrap_connection_generation(raw_rx, shutdown_rx, &mut observer, &mut buffer)
        .await?;
    let protocol_deadline = Instant::now() + PRIVATE_ACK_TIMEOUT;

    wait_for_bootstrap_protocol_ready(
        raw_rx,
        shutdown_rx,
        &mut observer,
        &mut buffer,
        protocol_deadline,
    )
    .await?;
    let http_cut = fetch_bootstrap_http_cut_while_buffering(
        client,
        info_url,
        user,
        perp_dexes,
        account_mode,
        fact_recovery_lookback,
        raw_rx,
        shutdown_rx,
        &mut observer,
        &mut buffer,
    )
    .await
    .context("fetch authoritative Hyperliquid startup HTTP cut")?;

    let buffered_frame_count = buffer.frames.len();
    let buffered_bytes = buffer.bytes;
    let applied = apply_bootstrap_http_cut(
        user,
        catalog,
        account_mode,
        fill_snapshot_policy,
        http_cut,
        buffer.frames,
    )?;
    let connection_generation = observer
        .connection_generation
        .context("Hyperliquid bootstrap connection generation disappeared")?;
    Ok(BootstrapCut {
        processor: applied.processor,
        processed_frames: applied.processed_frames,
        subscription_acks: observer.subscription_acks,
        generation: composite_generation(1, connection_generation),
        historical_seed_count: applied.historical_seed_count,
        frontend_seed_count: applied.frontend_seed_count,
        recovered_fill_count: applied.recovered_fill_count,
        recovered_funding_count: applied.recovered_funding_count,
        recovered_ledger_count: applied.recovered_ledger_count,
        buffered_frame_count,
        buffered_bytes,
    })
}

fn apply_bootstrap_http_cut(
    user: &str,
    catalog: &HyperliquidAssetCatalog,
    account_mode: HyperliquidAccountMode,
    fill_snapshot_policy: FillSnapshotPolicy,
    http_cut: BootstrapHttpCut,
    buffered_frames: Vec<Bytes>,
) -> Result<AppliedBootstrapCut> {
    let BootstrapHttpCut {
        orders,
        facts,
        borrowing,
    } = http_cut;
    let recovered_fill_count = facts
        .fills
        .as_array()
        .map(Vec::len)
        .context("recovered Hyperliquid user fills must be an array")?;
    let recovered_funding_count = facts.fundings.len();
    let recovered_ledger_count = facts.ledger_updates.len();

    let mut processor = HyperliquidAccountProcessor::new(
        user,
        catalog.clone(),
        account_mode,
        fill_snapshot_policy,
    )?;
    if let Some((borrowing, observed_at_ms)) = borrowing {
        processor.seed_borrow_lend_user_state(&borrowing, observed_at_ms)?;
    }
    let order_recovery = processor
        .recover_order_lifecycle_cut(
            &orders.historical_orders,
            &orders.frontend_open_orders,
            &HashSet::new(),
        )
        .context("apply authoritative Hyperliquid startup order cut")?;
    let historical_seed_count = order_recovery.historical_seed_count;
    let frontend_seed_count = order_recovery.frontend_seed_count;

    // Seed every HTTP fact before replaying the overlapping websocket window.
    // Processor-level venue identities make the overlap idempotent, while the
    // buffer supplies every fact strictly after the HTTP time boundary.
    let recovery_frames = recovered_factual_frames(user, facts)?;
    let mut processed_frames = Vec::with_capacity(
        buffered_frames
            .len()
            .saturating_add(recovery_frames.len())
            .saturating_add(1),
    );
    if !order_recovery.events.is_empty() {
        processed_frames.push(BootstrapProcessedFrame {
            state_stream: None,
            events: order_recovery.events,
        });
    }
    for recovery_frame in recovery_frames {
        let recovery_events = processor
            .process_value_at_with_fill_snapshot_context(
                &recovery_frame,
                chrono::Utc::now().timestamp_millis(),
                FillSnapshotContext::Initial,
            )
            .context("process authoritative Hyperliquid startup account facts")?;
        processed_frames.push(BootstrapProcessedFrame {
            state_stream: None,
            events: recovery_events,
        });
    }
    processed_frames.extend(replay_bootstrap_frames(&mut processor, buffered_frames)?);
    processor
        .validate_active_internal_fill_coverage()
        .context("validate Hyperliquid active-order factual fill coverage")?;

    Ok(AppliedBootstrapCut {
        processor,
        processed_frames,
        historical_seed_count,
        frontend_seed_count,
        recovered_fill_count,
        recovered_funding_count,
        recovered_ledger_count,
    })
}

async fn wait_for_bootstrap_connection_generation(
    raw_rx: &mut broadcast::Receiver<Bytes>,
    shutdown_rx: &mut watch::Receiver<bool>,
    observer: &mut BootstrapProtocolObserver,
    buffer: &mut BootstrapFrameBuffer,
) -> Result<()> {
    while observer.connection_generation.is_none() {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    anyhow::bail!("Hyperliquid account monitor stopped before bootstrap connection");
                }
            }
            message = raw_rx.recv() => {
                let payload = receive_bootstrap_payload(message)?;
                observer.observe(payload, buffer)?;
            }
        }
    }
    Ok(())
}

async fn wait_for_bootstrap_protocol_ready(
    raw_rx: &mut broadcast::Receiver<Bytes>,
    shutdown_rx: &mut watch::Receiver<bool>,
    observer: &mut BootstrapProtocolObserver,
    buffer: &mut BootstrapFrameBuffer,
    deadline: Instant,
) -> Result<()> {
    while !observer.is_ready() {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    anyhow::bail!("Hyperliquid account monitor stopped before bootstrap protocol readiness");
                }
            }
            _ = tokio::time::sleep_until(deadline.into()) => {
                anyhow::bail!(
                    "Hyperliquid bootstrap timed out waiting for every subscription ACK and the initial userFills snapshot"
                );
            }
            message = raw_rx.recv() => {
                let payload = receive_bootstrap_payload(message)?;
                observer.observe(payload, buffer)?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn fetch_bootstrap_http_cut_while_buffering(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    perp_dexes: &[String],
    account_mode: HyperliquidAccountMode,
    fact_recovery_lookback: Duration,
    raw_rx: &mut broadcast::Receiver<Bytes>,
    shutdown_rx: &mut watch::Receiver<bool>,
    observer: &mut BootstrapProtocolObserver,
    buffer: &mut BootstrapFrameBuffer,
) -> Result<BootstrapHttpCut> {
    let fetch = fetch_bootstrap_http_cut(
        client,
        info_url,
        user,
        perp_dexes,
        account_mode,
        fact_recovery_lookback,
    );
    tokio::pin!(fetch);
    loop {
        tokio::select! {
            result = &mut fetch => return result,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    anyhow::bail!("Hyperliquid account monitor stopped during bootstrap HTTP cut");
                }
            }
            message = raw_rx.recv() => {
                let payload = receive_bootstrap_payload(message)?;
                observer.observe(payload, buffer)?;
            }
        }
    }
}

async fn fetch_bootstrap_http_snapshot(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    perp_dexes: &[String],
) -> Result<BootstrapHttpSnapshot> {
    let frontend_requests = perp_dexes.iter().map(|dex| async move {
        let rows = fetch_frontend_open_orders(client, info_url, user, dex).await?;
        Ok::<_, anyhow::Error>((dex.clone(), rows))
    });
    let (historical_orders, frontend_open_orders) = tokio::try_join!(
        fetch_historical_orders(client, info_url, user),
        try_join_all(frontend_requests),
    )?;
    Ok(BootstrapHttpSnapshot {
        historical_orders,
        frontend_open_orders,
    })
}

#[allow(clippy::too_many_arguments)]
async fn fetch_bootstrap_http_cut(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    perp_dexes: &[String],
    account_mode: HyperliquidAccountMode,
    fact_recovery_lookback: Duration,
) -> Result<BootstrapHttpCut> {
    fetch_account_http_cut(
        client,
        info_url,
        user,
        perp_dexes,
        account_mode,
        HyperliquidFactWatermarks::default(),
        fact_recovery_lookback,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_account_http_cut(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    perp_dexes: &[String],
    account_mode: HyperliquidAccountMode,
    watermarks: HyperliquidFactWatermarks,
    fact_recovery_lookback: Duration,
) -> Result<BootstrapHttpCut> {
    // Take the order seed first, then place the fixed time-history boundary
    // after every order response. Any fill reflected in an order's cumulative
    // quantity is therefore covered by the HTTP facts below. The websocket is
    // already live and remains bounded-buffered around both legs, covering the
    // overlap and every fact after that time boundary.
    let orders = fetch_bootstrap_http_snapshot(client, info_url, user, perp_dexes).await?;
    let facts =
        fetch_recoverable_account_facts(client, info_url, user, watermarks, fact_recovery_lookback)
            .await?;
    let borrowing = if account_mode == HyperliquidAccountMode::PortfolioMargin {
        let state = client
            .post(info_url)
            .json(&serde_json::json!({"type":"borrowLendUserState", "user":user}))
            .send()
            .await
            .context("fetch Hyperliquid borrowing during account cut")?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Some((state, chrono::Utc::now().timestamp_millis()))
    } else {
        None
    };
    Ok(BootstrapHttpCut {
        orders,
        facts,
        borrowing,
    })
}

async fn fetch_recoverable_account_facts(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    watermarks: HyperliquidFactWatermarks,
    lookback: Duration,
) -> Result<RecoveredAccountFacts> {
    let end_time_ms = chrono::Utc::now().timestamp_millis().max(0);
    let lookback_ms = i64::try_from(lookback.as_millis())
        .context("HYPERLIQUID_FACT_RECOVERY_LOOKBACK_MS exceeds i64")?;
    let overlap_ms = i64::try_from(FACT_RECOVERY_INCLUSIVE_OVERLAP.as_millis())
        .expect("fixed recovery overlap fits i64");
    let funding_start = recovery_start_time(
        end_time_ms,
        watermarks.funding_time_ms,
        lookback_ms,
        overlap_ms,
    );
    let ledger_start = recovery_start_time(
        end_time_ms,
        watermarks.ledger_time_ms,
        lookback_ms,
        overlap_ms,
    );
    let twap_slice_start = watermarks
        .twap_slice_time_ms
        .map(|watermark| watermark.saturating_sub(overlap_ms).clamp(0, end_time_ms))
        .unwrap_or(0);
    // userFillsByTime is itself bounded to the venue's retained 10,000 rows.
    // Start from zero without a watermark; at the cap, accept a reconnect only
    // when the returned window strictly overlaps the prior watermark.
    let fill_start = watermarks
        .fill_time_ms
        .map(|watermark| watermark.saturating_sub(overlap_ms).clamp(0, end_time_ms))
        .unwrap_or(0);
    let (fills, fundings, ledger_updates, twap_slice_fills, twap_history) = tokio::try_join!(
        fetch_recoverable_user_fills(
            client,
            info_url,
            user,
            fill_start,
            end_time_ms,
            watermarks.fill_time_ms,
        ),
        fetch_bounded_time_history(
            client,
            info_url,
            user,
            TimeHistoryKind::Funding,
            funding_start,
            end_time_ms,
        ),
        fetch_bounded_time_history(
            client,
            info_url,
            user,
            TimeHistoryKind::NonFundingLedger,
            ledger_start,
            end_time_ms,
        ),
        fetch_recoverable_twap_slice_fills(client, info_url, user, twap_slice_start, end_time_ms,),
        fetch_recoverable_twap_history(client, info_url, user, watermarks.twap_history_time_s,),
    )?;
    Ok(RecoveredAccountFacts {
        fills,
        fundings,
        ledger_updates,
        twap_slice_fills,
        twap_history,
    })
}

fn recovery_start_time(
    end_time_ms: i64,
    watermark: Option<i64>,
    lookback_ms: i64,
    overlap_ms: i64,
) -> i64 {
    watermark
        .map(|value| value.saturating_sub(overlap_ms))
        .unwrap_or_else(|| end_time_ms.saturating_sub(lookback_ms))
        .clamp(0, end_time_ms)
}

async fn fetch_recoverable_user_fills(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    mut start_time_ms: i64,
    end_time_ms: i64,
    coverage_watermark_ms: Option<i64>,
) -> Result<Value> {
    let mut rows = Vec::with_capacity(USER_FILLS_RETENTION_CAPACITY);
    let mut identities = HashSet::with_capacity(USER_FILLS_RETENTION_CAPACITY);

    loop {
        let page =
            fetch_user_fills_by_time(client, info_url, user, start_time_ms, end_time_ms).await?;
        let page = page
            .as_array()
            .context("Hyperliquid userFillsByTime response must be an array")?;
        if page.len() > USER_FILLS_PAGE_CAPACITY {
            anyhow::bail!(
                "Hyperliquid userFillsByTime page exceeded documented limit: {} > {}",
                page.len(),
                USER_FILLS_PAGE_CAPACITY
            );
        }

        let mut max_time_ms = None;
        let mut inserted = 0_usize;
        for row in page {
            let (time_ms, identity) = bootstrap_fill_identity(row)?;
            if time_ms < start_time_ms || time_ms > end_time_ms {
                anyhow::bail!(
                    "Hyperliquid userFillsByTime row is outside requested range: row_time={time_ms} start={start_time_ms} end={end_time_ms}"
                );
            }
            max_time_ms = Some(max_time_ms.map_or(time_ms, |current: i64| current.max(time_ms)));
            if identities.insert(identity.clone()) {
                rows.push((time_ms, identity, row.clone()));
                inserted = inserted.saturating_add(1);
                if rows.len() > USER_FILLS_RETENTION_CAPACITY {
                    anyhow::bail!(
                        "Hyperliquid userFillsByTime returned more than the documented {}-fill retention window",
                        USER_FILLS_RETENTION_CAPACITY
                    );
                }
            }
        }

        if page.len() < USER_FILLS_PAGE_CAPACITY {
            break;
        }
        let max_time_ms = max_time_ms
            .context("Hyperliquid userFillsByTime returned a full page without a timestamp")?;
        if max_time_ms <= start_time_ms && inserted == 0 {
            anyhow::bail!(
                "Hyperliquid userFillsByTime pagination cannot advance at timestamp {start_time_ms}; refusing an ambiguous fill gap"
            );
        }
        start_time_ms = max_time_ms;
        if start_time_ms >= end_time_ms && inserted == 0 {
            break;
        }
    }

    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    validate_user_fill_retention_coverage(
        rows.len(),
        rows.first().map(|row| row.0),
        coverage_watermark_ms,
    )?;
    Ok(Value::Array(
        rows.into_iter().map(|(_, _, row)| row).collect(),
    ))
}

fn validate_user_fill_retention_coverage(
    unique_row_count: usize,
    earliest_time_ms: Option<i64>,
    coverage_watermark_ms: Option<i64>,
) -> Result<()> {
    if unique_row_count < USER_FILLS_RETENTION_CAPACITY {
        return Ok(());
    }
    if unique_row_count > USER_FILLS_RETENTION_CAPACITY {
        anyhow::bail!(
            "Hyperliquid userFillsByTime returned more than the documented {}-fill retention window",
            USER_FILLS_RETENTION_CAPACITY
        );
    }

    let earliest_time_ms = earliest_time_ms
        .context("Hyperliquid userFillsByTime reached its retention cap without any fill")?;
    let coverage_watermark_ms = coverage_watermark_ms.with_context(|| {
        format!(
            "Hyperliquid userFillsByTime reached the venue retention cap of {} without a prior fill watermark; refusing an ambiguous historical gap",
            USER_FILLS_RETENTION_CAPACITY
        )
    })?;
    if earliest_time_ms >= coverage_watermark_ms {
        anyhow::bail!(
            "Hyperliquid userFillsByTime reached the venue retention cap of {} but its earliest fill at {} does not precede the recovery watermark {}; refusing an ambiguous fill gap",
            USER_FILLS_RETENTION_CAPACITY,
            earliest_time_ms,
            coverage_watermark_ms
        );
    }
    Ok(())
}

async fn fetch_recoverable_twap_slice_fills(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    mut start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut identities = HashSet::new();
    let mut page_count = 0_usize;
    loop {
        page_count = page_count.saturating_add(1);
        let response =
            fetch_user_twap_slice_fills_by_time(client, info_url, user, start_time_ms, end_time_ms)
                .await?;
        let page = response
            .as_array()
            .context("Hyperliquid userTwapSliceFillsByTime response must be an array")?;
        if page.len() > TWAP_SLICE_FILLS_PAGE_CAPACITY {
            anyhow::bail!(
                "Hyperliquid userTwapSliceFillsByTime page exceeded limit: {} > {}",
                page.len(),
                TWAP_SLICE_FILLS_PAGE_CAPACITY
            );
        }

        let mut max_time_ms = None;
        let mut inserted = 0_usize;
        for row in page {
            let (time_ms, identity) = bootstrap_twap_slice_identity(row)?;
            if time_ms < start_time_ms || time_ms > end_time_ms {
                anyhow::bail!(
                    "Hyperliquid userTwapSliceFillsByTime row is outside requested range: row_time={time_ms} start={start_time_ms} end={end_time_ms}"
                );
            }
            max_time_ms = Some(max_time_ms.map_or(time_ms, |current: i64| current.max(time_ms)));
            if identities.insert(identity.clone()) {
                rows.push((time_ms, identity, row.clone()));
                inserted = inserted.saturating_add(1);
                if rows.len() > TIME_HISTORY_MAX_ROWS {
                    anyhow::bail!(
                        "Hyperliquid userTwapSliceFillsByTime exceeded hard recovery row cap {}; refusing to truncate",
                        TIME_HISTORY_MAX_ROWS
                    );
                }
            }
        }

        if page.len() < TWAP_SLICE_FILLS_PAGE_CAPACITY {
            break;
        }
        if page_count >= TIME_HISTORY_MAX_PAGES {
            anyhow::bail!(
                "Hyperliquid userTwapSliceFillsByTime reached hard recovery page cap {}; refusing to truncate",
                TIME_HISTORY_MAX_PAGES
            );
        }
        let max_time_ms = max_time_ms.context(
            "Hyperliquid userTwapSliceFillsByTime returned a full page without a timestamp",
        )?;
        if max_time_ms <= start_time_ms && inserted == 0 {
            anyhow::bail!(
                "Hyperliquid userTwapSliceFillsByTime pagination cannot advance at timestamp {start_time_ms}; refusing an ambiguous TWAP slice gap"
            );
        }
        start_time_ms = max_time_ms;
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(rows.into_iter().map(|(_, _, row)| row).collect())
}

async fn fetch_recoverable_twap_history(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    coverage_watermark_s: Option<i64>,
) -> Result<Vec<Value>> {
    let response = fetch_twap_history(client, info_url, user).await?;
    let rows = response
        .as_array()
        .context("Hyperliquid twapHistory response must be an array")?;
    if rows.len() > TIME_HISTORY_MAX_ROWS {
        anyhow::bail!(
            "Hyperliquid twapHistory exceeded hard recovery row cap {}; refusing to truncate",
            TIME_HISTORY_MAX_ROWS
        );
    }
    let mut recovered = Vec::with_capacity(rows.len());
    let mut earliest = None;
    for row in rows {
        let time_s = bootstrap_integer(
            row.get("time")
                .context("Hyperliquid twapHistory row missing time")?,
            "TWAP history time",
        )?;
        if time_s < 0 {
            anyhow::bail!("Hyperliquid twapHistory row time must be nonnegative");
        }
        earliest = Some(earliest.map_or(time_s, |current: i64| current.min(time_s)));
        recovered.push((time_s, canonical_json(row), row.clone()));
    }
    if let Some(watermark) = coverage_watermark_s {
        let earliest = earliest.context(
            "Hyperliquid twapHistory omitted all rows despite a prior lifecycle watermark",
        )?;
        if earliest > watermark {
            anyhow::bail!(
                "Hyperliquid twapHistory earliest row {earliest} is newer than prior watermark {watermark}; endpoint retention cannot prove lifecycle coverage"
            );
        }
    }
    recovered.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(recovered.into_iter().map(|(_, _, row)| row).collect())
}

async fn fetch_bounded_time_history(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    kind: TimeHistoryKind,
    mut start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut identities = HashSet::new();
    let mut page_count = 0_usize;

    loop {
        page_count = page_count.saturating_add(1);
        let response = match kind {
            TimeHistoryKind::Funding => {
                fetch_user_funding_by_time(client, info_url, user, start_time_ms, end_time_ms)
                    .await?
            }
            TimeHistoryKind::NonFundingLedger => {
                fetch_user_non_funding_ledger_updates_by_time(
                    client,
                    info_url,
                    user,
                    start_time_ms,
                    end_time_ms,
                )
                .await?
            }
        };
        let page = response
            .as_array()
            .with_context(|| format!("Hyperliquid {} response must be an array", kind.label()))?;
        if page.len() > TIME_HISTORY_PAGE_CAPACITY {
            anyhow::bail!(
                "Hyperliquid {} page exceeded documented limit: {} > {}",
                kind.label(),
                page.len(),
                TIME_HISTORY_PAGE_CAPACITY,
            );
        }

        let mut max_time_ms = None;
        let mut inserted = 0_usize;
        for row in page {
            let time_ms = bootstrap_integer(
                row.get("time")
                    .with_context(|| format!("Hyperliquid {} row missing time", kind.label()))?,
                "history time",
            )?;
            if time_ms < start_time_ms || time_ms > end_time_ms {
                anyhow::bail!(
                    "Hyperliquid {} row is outside requested range: row_time={} start={} end={}",
                    kind.label(),
                    time_ms,
                    start_time_ms,
                    end_time_ms,
                );
            }
            max_time_ms = Some(max_time_ms.map_or(time_ms, |current: i64| current.max(time_ms)));
            let identity = canonical_json(row);
            if identities.insert(identity.clone()) {
                rows.push((time_ms, identity, row.clone()));
                inserted = inserted.saturating_add(1);
                if rows.len() > TIME_HISTORY_MAX_ROWS {
                    anyhow::bail!(
                        "Hyperliquid {} exceeded hard recovery row cap {}; refusing to truncate",
                        kind.label(),
                        TIME_HISTORY_MAX_ROWS,
                    );
                }
            }
        }

        let Some(next_start) = next_time_history_start(
            kind,
            start_time_ms,
            page.len(),
            max_time_ms,
            inserted,
            page_count,
        )?
        else {
            break;
        };
        start_time_ms = next_start;
    }

    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(rows.into_iter().map(|(_, _, row)| row).collect())
}

fn next_time_history_start(
    kind: TimeHistoryKind,
    current_start: i64,
    page_len: usize,
    max_time_ms: Option<i64>,
    inserted: usize,
    page_count: usize,
) -> Result<Option<i64>> {
    if page_len < TIME_HISTORY_PAGE_CAPACITY {
        return Ok(None);
    }
    if page_count >= TIME_HISTORY_MAX_PAGES {
        anyhow::bail!(
            "Hyperliquid {} reached hard recovery page cap {}; refusing to truncate",
            kind.label(),
            TIME_HISTORY_MAX_PAGES,
        );
    }
    let max_time_ms = max_time_ms.with_context(|| {
        format!(
            "Hyperliquid {} returned a full page without a timestamp",
            kind.label()
        )
    })?;
    if max_time_ms <= current_start && inserted == 0 {
        anyhow::bail!(
            "Hyperliquid {} pagination cannot advance at timestamp {}; refusing an ambiguous factual gap",
            kind.label(),
            current_start,
        );
    }
    Ok(Some(max_time_ms))
}

fn recovered_factual_frames(user: &str, facts: RecoveredAccountFacts) -> Result<Vec<Value>> {
    let funding_rows = facts
        .fundings
        .iter()
        .map(normalize_http_funding_row)
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![
        serde_json::json!({
            "channel": "userFills",
            "data": {"user": user, "isSnapshot": true, "fills": facts.fills},
        }),
        serde_json::json!({
            "channel": "userFundings",
            "data": {"user": user, "isSnapshot": true, "fundings": funding_rows},
        }),
        serde_json::json!({
            "channel": "userNonFundingLedgerUpdates",
            "data": {
                "user": user,
                "isSnapshot": true,
                "nonFundingLedgerUpdates": facts.ledger_updates,
            },
        }),
        serde_json::json!({
            "channel": "userTwapSliceFills",
            "data": {
                "user": user,
                "isSnapshot": true,
                "twapSliceFills": facts.twap_slice_fills,
            },
        }),
        serde_json::json!({
            "channel": "userTwapHistory",
            "data": {
                "user": user,
                "isSnapshot": true,
                "history": facts.twap_history,
            },
        }),
    ])
}

fn normalize_http_funding_row(row: &Value) -> Result<Value> {
    let time = row
        .get("time")
        .context("Hyperliquid HTTP funding row missing time")?
        .clone();
    bootstrap_integer(&time, "funding time")?;
    let transaction_hash = row
        .get("hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("Hyperliquid HTTP funding row missing hash")?;
    let delta = row
        .get("delta")
        .and_then(Value::as_object)
        .context("Hyperliquid HTTP funding row missing delta object")?;
    if delta.get("type").and_then(Value::as_str) != Some("funding") {
        anyhow::bail!("Hyperliquid HTTP funding delta.type must be funding");
    }
    let required_delta_string = |field: &str| -> Result<&str> {
        delta
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("Hyperliquid HTTP funding delta missing {field}"))
    };
    Ok(serde_json::json!({
        "time": time,
        "coin": required_delta_string("coin")?,
        "usdc": required_delta_string("usdc")?,
        "szi": required_delta_string("szi")?,
        "fundingRate": required_delta_string("fundingRate")?,
        "hash": transaction_hash,
    }))
}

fn bootstrap_fill_identity(row: &Value) -> Result<(i64, String)> {
    let time_ms = bootstrap_integer(
        row.get("time")
            .context("Hyperliquid user fill missing time")?,
        "fill time",
    )?;
    if time_ms < 0 {
        anyhow::bail!("Hyperliquid user fill time must be nonnegative");
    }
    let coin = row
        .get("coin")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("Hyperliquid user fill missing coin")?;
    let tid = bootstrap_integer(
        row.get("tid")
            .context("Hyperliquid user fill missing tid")?,
        "fill tid",
    )?;
    Ok((time_ms, format!("{time_ms}:{coin}:{tid}")))
}

fn bootstrap_twap_slice_identity(row: &Value) -> Result<(i64, String)> {
    let twap_id = bootstrap_integer(
        row.get("twapId")
            .context("Hyperliquid TWAP slice row missing twapId")?,
        "TWAP slice id",
    )?;
    if twap_id < 0 {
        anyhow::bail!("Hyperliquid TWAP slice id must be nonnegative");
    }
    let fill = row
        .get("fill")
        .context("Hyperliquid TWAP slice row missing fill")?;
    let (time_ms, fill_identity) = bootstrap_fill_identity(fill)?;
    Ok((time_ms, format!("{fill_identity}:{twap_id}")))
}

fn bootstrap_integer(value: &Value, field: &str) -> Result<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        .with_context(|| {
            format!("Hyperliquid bootstrap {field} must be an integer or integer string")
        })
}

fn receive_bootstrap_payload(
    message: std::result::Result<Bytes, broadcast::error::RecvError>,
) -> Result<Bytes> {
    match message {
        Ok(payload) => Ok(payload),
        Err(broadcast::error::RecvError::Lagged(count)) => anyhow::bail!(
            "Hyperliquid bootstrap receiver lagged by {count} messages; authoritative cut is unknowable"
        ),
        Err(broadcast::error::RecvError::Closed) => {
            anyhow::bail!("Hyperliquid bootstrap connection closed")
        }
    }
}

impl BootstrapProtocolObserver {
    fn is_ready(&self) -> bool {
        self.subscription_acks.is_complete() && self.initial_user_fills_snapshot_seen
    }

    fn observe(&mut self, payload: Bytes, buffer: &mut BootstrapFrameBuffer) -> Result<()> {
        if let Some(generation) = parse_connection_generation_notification(&payload) {
            if self.connection_generation.replace(generation).is_some() {
                anyhow::bail!(
                    "Hyperliquid primary connection generation changed during startup cut"
                );
            }
            self.subscription_acks.reset();
            self.initial_user_fills_snapshot_seen = false;
            return Ok(());
        }
        if self.connection_generation.is_none() {
            anyhow::bail!(
                "Hyperliquid private payload arrived before the connection generation marker"
            );
        }

        let root: Value = serde_json::from_slice(&payload)
            .context("decode Hyperliquid bootstrap private-stream JSON")?;
        match self.subscription_acks.observe(&root)? {
            HyperliquidSubscriptionControl::Acknowledged { .. } => return Ok(()),
            HyperliquidSubscriptionControl::NotControl => {}
        }
        let channel = root.get("channel").and_then(Value::as_str);
        if channel.is_some_and(is_private_subscription_channel) {
            let channel = channel.unwrap_or_default();
            if !self.subscription_acks.has_acknowledged_frame(&root) {
                anyhow::bail!(
                    "Hyperliquid bootstrap received private channel {channel:?} before its validated acknowledgement"
                );
            }
            if channel == "userFills"
                && root
                    .get("data")
                    .and_then(|data| data.get("isSnapshot"))
                    .and_then(Value::as_bool)
                    == Some(true)
            {
                let snapshot_user = root
                    .get("data")
                    .and_then(|data| data.get("user"))
                    .and_then(Value::as_str)
                    .context("Hyperliquid bootstrap userFills snapshot missing user")?;
                if normalize_hyperliquid_address(snapshot_user)? != self.user {
                    anyhow::bail!(
                        "Hyperliquid bootstrap userFills snapshot belongs to a different account"
                    );
                }
                self.initial_user_fills_snapshot_seen = true;
            }
            buffer.push(payload)?;
        }
        Ok(())
    }
}

fn canonical_json(value: &Value) -> String {
    fn append(value: &Value, output: &mut String) {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => output.push_str(&value.to_string()),
            Value::String(value) => output.push_str(
                &serde_json::to_string(value).expect("serializing a JSON string cannot fail"),
            ),
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    append(value, output);
                }
                output.push(']');
            }
            Value::Object(values) => {
                output.push('{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    output.push_str(
                        &serde_json::to_string(key)
                            .expect("serializing a JSON object key cannot fail"),
                    );
                    output.push(':');
                    append(&values[key], output);
                }
                output.push('}');
            }
        }
    }

    let mut output = String::new();
    append(value, &mut output);
    output
}

fn replay_bootstrap_frames(
    processor: &mut HyperliquidAccountProcessor,
    frames: Vec<Bytes>,
) -> Result<Vec<BootstrapProcessedFrame>> {
    let mut processed = Vec::with_capacity(frames.len());
    for payload in frames {
        let root: Value = serde_json::from_slice(&payload)
            .context("decode buffered Hyperliquid bootstrap frame for replay")?;
        let state_stream = root
            .get("channel")
            .and_then(Value::as_str)
            .and_then(AccountStateStream::from_channel);
        let events = processor
            .process_value_at_with_fill_snapshot_context(
                &root,
                chrono::Utc::now().timestamp_millis(),
                FillSnapshotContext::Initial,
            )
            .context("replay buffered Hyperliquid bootstrap frame")?;
        processed.push(BootstrapProcessedFrame {
            state_stream,
            events,
        });
    }
    Ok(processed)
}

fn publish_bootstrap_frames(
    state: &mut DirectAccountState,
    frames: Vec<BootstrapProcessedFrame>,
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    generation: u64,
    snapshot_ttl: Duration,
) -> Result<u64> {
    let mut batch_id = 0_u64;
    for frame in frames {
        if let Some(stream) = frame.state_stream {
            batch_id = batch_id.checked_add(1).unwrap_or(1);
            let account_hash = state.fact_replay.account_hash;
            forward_snapshot_batch(
                &mut state.forwarder,
                &frame.events,
                StreamPath::Primary,
                stream,
                account_mode,
                account_hash,
                monitor_id,
                generation,
                batch_id,
                snapshot_ttl,
            )
            .context("publish buffered Hyperliquid bootstrap state snapshot")?;
            state
                .state_sources
                .record_success(StreamPath::Primary, stream, Instant::now());
        } else {
            if !send_or_queue_direct_factual_events(state, frame.events) {
                warn!(
                    "Hyperliquid bootstrap factual IPC send failed; committed events remain queued in order"
                );
            }
        }
    }
    Ok(batch_id)
}

fn start_account_connection(
    path: StreamPath,
    ws_url: String,
    local_ip: String,
    subscriptions: Vec<Value>,
    shutdown_rx: watch::Receiver<bool>,
    channel_capacity: usize,
) -> (broadcast::Receiver<Bytes>, tokio::task::JoinHandle<()>) {
    let (raw_tx, raw_rx) = broadcast::channel::<Bytes>(channel_capacity);
    let mut connection =
        MktConnection::new(ws_url, Value::Array(subscriptions), raw_tx, shutdown_rx);
    if !local_ip.trim().is_empty() {
        connection.local_ip = Some(local_ip);
    }
    let mut runner =
        HyperliquidConnection::new(connection).with_connection_generation_notifications();
    let runner_handle = tokio::spawn(async move {
        if let Err(err) = runner.start_ws().await {
            error!(
                "Hyperliquid {} account stream stopped with error: {err:#}",
                path.as_str()
            );
        }
    });
    (raw_rx, runner_handle)
}

fn spawn_stream_path(
    path: StreamPath,
    ws_url: String,
    local_ip: String,
    subscriptions: Vec<Value>,
    shutdown_rx: watch::Receiver<bool>,
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    state_refresh_timeout: Duration,
    snapshot_ttl: Duration,
    info_client: reqwest::Client,
    info_url: String,
    user: String,
    perp_dexes: Vec<String>,
    fact_recovery_lookback: Duration,
    initial_session: Option<InitialStreamSession>,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    HyperliquidSubscriptionAcks::from_requests(&subscriptions)?;
    let supervisor_handle = tokio::spawn(async move {
        let mut supervisor_shutdown = shutdown_rx;
        let mut supervisor_session = 0_u64;
        let mut initial_session = initial_session;
        loop {
            if *supervisor_shutdown.borrow() {
                break;
            }
            supervisor_session = supervisor_session.checked_add(1).unwrap_or(1);
            let (raw_rx, runner_handle, initial_protocol) =
                if let Some(initial) = initial_session.take() {
                    (
                        initial.raw_rx,
                        initial.runner_handle,
                        Some(initial.protocol),
                    )
                } else {
                    let (raw_rx, runner_handle) = start_account_connection(
                        path,
                        ws_url.clone(),
                        local_ip.clone(),
                        subscriptions.clone(),
                        supervisor_shutdown.clone(),
                        4_096,
                    );
                    (raw_rx, runner_handle, None)
                };
            let shutdown = consume_stream_session(
                path,
                raw_rx,
                supervisor_shutdown.clone(),
                &subscriptions,
                account_mode,
                monitor_id,
                state_refresh_timeout,
                snapshot_ttl,
                supervisor_session,
                initial_protocol,
                info_client.clone(),
                &info_url,
                &user,
                &perp_dexes,
                fact_recovery_lookback,
            )
            .await;
            if !runner_handle.is_finished() {
                runner_handle.abort();
            }
            let _ = runner_handle.await;
            if shutdown || *supervisor_shutdown.borrow() {
                break;
            }

            warn!(
                "Hyperliquid {} account socket path restarting after protocol/health failure",
                path.as_str()
            );
            tokio::select! {
                _ = tokio::time::sleep(SESSION_RESTART_DELAY) => {}
                changed = supervisor_shutdown.changed() => {
                    if changed.is_err() || *supervisor_shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });
    Ok(vec![supervisor_handle])
}

#[allow(clippy::too_many_arguments)]
async fn consume_stream_session(
    path: StreamPath,
    mut raw_rx: broadcast::Receiver<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
    subscriptions: &[Value],
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    state_refresh_timeout: Duration,
    snapshot_ttl: Duration,
    supervisor_session: u64,
    initial_protocol: Option<InitialSessionProtocol>,
    info_client: reqwest::Client,
    info_url: &str,
    user: &str,
    perp_dexes: &[String],
    fact_recovery_lookback: Duration,
) -> bool {
    let now = Instant::now();
    let (
        mut subscription_acks,
        mut subscription_identity_valid,
        mut fill_snapshot_context,
        mut generation,
        mut batch_id,
        mut ack_deadline,
        mut state_deadlines,
    ) = if let Some(protocol) = initial_protocol {
        (
            protocol.subscription_acks,
            true,
            FillSnapshotContext::Reconnect,
            protocol.generation,
            protocol.batch_id,
            None,
            [
                Some(now + state_refresh_timeout),
                Some(now + state_refresh_timeout),
            ],
        )
    } else {
        let subscription_acks = match HyperliquidSubscriptionAcks::from_requests(subscriptions) {
            Ok(acks) => acks,
            Err(err) => {
                error!(
                    "Hyperliquid {} failed to initialize subscription ACK tracker: {err:#}",
                    path.as_str()
                );
                return false;
            }
        };
        (
            subscription_acks,
            false,
            FillSnapshotContext::Reconnect,
            0,
            0,
            None,
            [None; 2],
        )
    };
    let mut health_tick = tokio::time::interval(Duration::from_secs(1));
    health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    health_tick.tick().await;
    let mut cut_recovery: Option<tokio::task::JoinHandle<Result<ConnectionHttpCut>>> = None;
    let mut recovery_required = false;
    let mut recovery_buffer = BootstrapFrameBuffer::default();

    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    if let Some(handle) = cut_recovery.take() {
                        handle.abort();
                    }
                    invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                    return true;
                }
            }
            recovery_result = await_connection_cut(&mut cut_recovery), if cut_recovery.is_some() => {
                let _ = cut_recovery.take();
                let recovered = match recovery_result {
                    Ok(recovered) => recovered,
                    Err(err) => {
                        error!(
                            "Hyperliquid {} account HTTP cut failed for generation {}: {err:#}; reconnecting path",
                            path.as_str(),
                            generation,
                        );
                        invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                        return false;
                    }
                };
                let applied = match apply_connection_http_cut(
                    user,
                    recovered.cut,
                    &recovered.required_active_order_ids,
                ) {
                    Ok(applied) => applied,
                    Err(err) => {
                        error!(
                            "Hyperliquid {} account HTTP cut application failed for generation {}: {err:#}; reconnecting path",
                            path.as_str(),
                            generation,
                        );
                        invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                        return false;
                    }
                };
                recovery_required = false;

                let now = Instant::now();
                state_deadlines = [
                    Some(now + state_refresh_timeout),
                    Some(now + state_refresh_timeout),
                ];
                let buffered_ws_frames = recovery_buffer.frames.len();
                for payload in std::mem::take(&mut recovery_buffer).frames {
                    match process_payload(
                        path,
                        &payload,
                        &mut subscription_acks,
                        fill_snapshot_context,
                        account_mode,
                        monitor_id,
                        generation,
                        &mut batch_id,
                        snapshot_ttl,
                    ) {
                        SessionDisposition::Continue => {}
                        SessionDisposition::StateObserved(stream) => {
                            state_deadlines[stream.index()] =
                                Some(Instant::now() + state_refresh_timeout);
                        }
                        SessionDisposition::Restart => {
                            invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                            return false;
                        }
                    }
                }
                ack_deadline = None;
                info!(
                    "Hyperliquid {} account HTTP cut completed for generation {}: historical_orders={} frontend_open_orders={} fills={} fundings={} ledger_updates={} buffered_ws_frames={}",
                    path.as_str(),
                    generation,
                    applied.historical_seed_count,
                    applied.frontend_seed_count,
                    applied.recovered_fill_count,
                    applied.recovered_funding_count,
                    applied.recovered_ledger_count,
                    buffered_ws_frames,
                );
            }
            _ = health_tick.tick() => {
                let now = Instant::now();
                if recovery_required
                    && cut_recovery.is_none()
                    && ack_deadline.is_some_and(|deadline| now >= deadline)
                    && !subscription_acks.is_complete()
                {
                    error!(
                        "Hyperliquid {} private subscription acknowledgement timeout; reconnecting path",
                        path.as_str()
                    );
                    invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                    return false;
                }
                if generation > 0 && !recovery_required {
                    for stream in [AccountStateStream::Spot, AccountStateStream::Perp] {
                        if state_deadlines[stream.index()].is_some_and(|deadline| now >= deadline) {
                            warn!(
                                "Hyperliquid {} {} freshness timeout; reconnecting path for a new complete snapshot",
                                path.as_str(),
                                stream.as_str()
                            );
                            invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                            return false;
                        }
                    }
                }
            }
            message = raw_rx.recv() => {
                match message {
                    Ok(payload) => {
                        if let Some(connection_generation) =
                            parse_connection_generation_notification(&payload)
                        {
                            if let Some(handle) = cut_recovery.take() {
                                handle.abort();
                            }
                            recovery_required = true;
                            recovery_buffer = BootstrapFrameBuffer::default();
                            generation = composite_generation(
                                supervisor_session,
                                connection_generation,
                            );
                            batch_id = 0;
                            fill_snapshot_context = reset_path_protocol_for_generation(
                                &mut subscription_acks,
                                &mut subscription_identity_valid,
                                if supervisor_session == 1 {
                                    connection_generation
                                } else {
                                    2
                                },
                            );
                            ack_deadline = Some(Instant::now() + PRIVATE_ACK_TIMEOUT);
                            state_deadlines = [None; 2];
                            if !invalidate_stream_path(
                                path,
                                account_mode,
                                monitor_id,
                                generation,
                                snapshot_ttl,
                            ) {
                                return false;
                            }
                            info!(
                                "Hyperliquid {} account stream entered connection generation {}; buffering validated private frames until every subscription ACK precedes the HTTP order/fact cut",
                                path.as_str(),
                                generation,
                            );
                        } else if recovery_required {
                            if let Err(err) = observe_connection_recovery_payload(
                                path,
                                payload,
                                &mut subscription_acks,
                                &mut recovery_buffer,
                            ) {
                                error!(
                                    "Hyperliquid {} account cut WS buffer/protocol failed: {err:#}; reconnecting path",
                                    path.as_str(),
                                );
                                if let Some(handle) = cut_recovery.take() {
                                    handle.abort();
                                }
                                invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
                                return false;
                            }
                            if subscription_acks.is_complete() && cut_recovery.is_none() {
                                ack_deadline = None;
                                let Some((watermarks, required_active_order_ids)) = direct_recovery_anchor() else {
                                    error!("Hyperliquid {} cannot start account HTTP cut without processor state", path.as_str());
                                    return false;
                                };
                                let pinned_count = required_active_order_ids.len();
                                let recovery_client = info_client.clone();
                                let recovery_info_url = info_url.to_string();
                                let recovery_user = user.to_string();
                                let recovery_dexes = perp_dexes.to_vec();
                                cut_recovery = Some(tokio::spawn(async move {
                                    let cut = fetch_account_http_cut(
                                        &recovery_client,
                                        &recovery_info_url,
                                        &recovery_user,
                                        &recovery_dexes,
                                        account_mode,
                                        watermarks,
                                        fact_recovery_lookback,
                                    )
                                    .await?;
                                    Ok(ConnectionHttpCut {
                                        cut,
                                        required_active_order_ids,
                                    })
                                }));
                                info!(
                                    "Hyperliquid {} observed every private ACK for generation {}; account HTTP cut starts from fill={:?} funding={:?} ledger={:?} pinned_active_orders={}",
                                    path.as_str(),
                                    generation,
                                    watermarks.fill_time_ms,
                                    watermarks.funding_time_ms,
                                    watermarks.ledger_time_ms,
                                    pinned_count,
                                );
                            }
                        } else if subscription_identity_valid {
                            match process_payload(
                                path,
                                &payload,
                                &mut subscription_acks,
                                fill_snapshot_context,
                                account_mode,
                                monitor_id,
                                generation,
                                &mut batch_id,
                                snapshot_ttl,
                            ) {
                                SessionDisposition::Continue => {}
                                SessionDisposition::StateObserved(stream) => {
                                    state_deadlines[stream.index()] =
                                        Some(Instant::now() + state_refresh_timeout);
                                }
                                SessionDisposition::Restart => {
                                    invalidate_stream_path(
                                        path,
                                        account_mode,
                                        monitor_id,
                                        generation,
                                        snapshot_ttl,
                                    );
                                    return false;
                                }
                            }
                            if subscription_acks.is_complete() {
                                ack_deadline = None;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        error!(
                            "Hyperliquid {} account stream lagged by {} messages; invalidating snapshots and reconnecting path",
                            path.as_str(),
                            count
                        );
                        if let Some(handle) = cut_recovery.take() {
                            handle.abort();
                        }
                        invalidate_stream_path(
                            path,
                            account_mode,
                            monitor_id,
                            generation,
                            snapshot_ttl,
                        );
                        return false;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        if let Some(handle) = cut_recovery.take() {
                            handle.abort();
                        }
                        invalidate_stream_path(
                            path,
                            account_mode,
                            monitor_id,
                            generation,
                            snapshot_ttl,
                        );
                        return false;
                    }
                }
            }
        }
    }
}

async fn await_connection_cut(
    recovery: &mut Option<tokio::task::JoinHandle<Result<ConnectionHttpCut>>>,
) -> Result<ConnectionHttpCut> {
    recovery
        .as_mut()
        .context("Hyperliquid account HTTP cut handle disappeared")?
        .await
        .context("Hyperliquid account HTTP cut task failed")?
}

fn direct_recovery_anchor() -> Option<(HyperliquidFactWatermarks, HashSet<i64>)> {
    DIRECT_STATE.with(|cell| {
        cell.borrow().as_ref().map(|state| {
            (
                state.processor.fact_watermarks(),
                state.processor.active_order_ids_snapshot(),
            )
        })
    })
}

fn observe_connection_recovery_payload(
    path: StreamPath,
    payload: Bytes,
    subscription_acks: &mut HyperliquidSubscriptionAcks,
    buffer: &mut BootstrapFrameBuffer,
) -> Result<()> {
    let root: Value = serde_json::from_slice(&payload)
        .context("decode Hyperliquid private-stream JSON while buffering account cut")?;
    match subscription_acks.observe(&root)? {
        HyperliquidSubscriptionControl::Acknowledged {
            subscription_type,
            completed_now,
        } => {
            if completed_now {
                info!(
                    "Hyperliquid {} observed every expected private subscription acknowledgement",
                    path.as_str()
                );
            } else {
                log::debug!(
                    "Hyperliquid {} acknowledged private subscription {}",
                    path.as_str(),
                    subscription_type
                );
            }
            return Ok(());
        }
        HyperliquidSubscriptionControl::NotControl => {}
    }

    let channel = root.get("channel").and_then(Value::as_str);
    if channel.is_some_and(is_private_subscription_channel) {
        let channel = channel.unwrap_or_default();
        if !subscription_acks.has_acknowledged_frame(&root) {
            anyhow::bail!(
                "Hyperliquid {} received private channel {channel:?} before its validated acknowledgement",
                path.as_str()
            );
        }
        buffer.push(payload)?;
    }
    Ok(())
}

fn apply_connection_http_cut(
    user: &str,
    http_cut: BootstrapHttpCut,
    required_active_order_ids: &HashSet<i64>,
) -> Result<AppliedConnectionCut> {
    let BootstrapHttpCut {
        orders,
        facts,
        borrowing,
    } = http_cut;
    let recovered_fill_count = facts
        .fills
        .as_array()
        .map(Vec::len)
        .context("recovered Hyperliquid user fills must be an array")?;
    let recovered_funding_count = facts.fundings.len();
    let recovered_ledger_count = facts.ledger_updates.len();
    let recovery_frames = recovered_factual_frames(user, facts)?;

    DIRECT_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        let state = state
            .as_mut()
            .context("Hyperliquid direct account state is not initialized")?;
        if !flush_direct_factual_outbox(state) {
            anyhow::bail!("Hyperliquid factual IPC outbox is blocked");
        }

        // Construct and validate the complete order/fact cut on a private
        // processor clone. No dedup key, pin, or watermark becomes visible if
        // any later HTTP row is malformed or cannot prove fill coverage.
        let mut candidate = state.processor.clone();
        if let Some((borrowing, observed_at_ms)) = borrowing {
            candidate.seed_borrow_lend_user_state(&borrowing, observed_at_ms)?;
        }
        let order_recovery = candidate
            .recover_order_lifecycle_cut(
                &orders.historical_orders,
                &orders.frontend_open_orders,
                required_active_order_ids,
            )
            .context("apply recovered Hyperliquid order lifecycle cut")?;
        let mut events = order_recovery.events;
        for root in recovery_frames {
            events.extend(candidate.process_value_at_with_fill_snapshot_context(
                &root,
                chrono::Utc::now().timestamp_millis(),
                FillSnapshotContext::Reconnect,
            )?);
        }
        candidate
            .validate_active_internal_fill_coverage()
            .context("validate recovered Hyperliquid active-order fill coverage")?;

        let mut fact_replay = state.fact_replay.clone();
        let events = fact_replay
            .stamp_events(events)
            .context("sequence recovered Hyperliquid account cut")?;
        state.processor = candidate;
        state.fact_replay = fact_replay;
        if !send_or_queue_direct_stamped_factual_events(state, events) {
            anyhow::bail!(
                "Hyperliquid recovered account cut IPC send failed; unsent suffix remains queued"
            );
        }
        Ok(AppliedConnectionCut {
            historical_seed_count: order_recovery.historical_seed_count,
            frontend_seed_count: order_recovery.frontend_seed_count,
            recovered_fill_count,
            recovered_funding_count,
            recovered_ledger_count,
        })
    })
}

#[allow(clippy::too_many_arguments)]
fn process_payload(
    path: StreamPath,
    payload: &[u8],
    subscription_acks: &mut HyperliquidSubscriptionAcks,
    fill_snapshot_context: FillSnapshotContext,
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    generation: u64,
    batch_id: &mut u64,
    snapshot_ttl: Duration,
) -> SessionDisposition {
    let root: Value = match serde_json::from_slice(payload) {
        Ok(root) => root,
        Err(err) => {
            error!(
                "Hyperliquid {} account message rejected: decode WS JSON: {err}; reconnecting path",
                path.as_str()
            );
            return SessionDisposition::Restart;
        }
    };
    match subscription_acks.observe(&root) {
        Ok(HyperliquidSubscriptionControl::Acknowledged {
            subscription_type,
            completed_now,
        }) => {
            if completed_now {
                info!(
                    "Hyperliquid {} observed every expected private subscription acknowledgement",
                    path.as_str()
                );
            } else {
                log::debug!(
                    "Hyperliquid {} acknowledged private subscription {}",
                    path.as_str(),
                    subscription_type
                );
            }
            return SessionDisposition::Continue;
        }
        Ok(HyperliquidSubscriptionControl::NotControl) => {}
        Err(err) => {
            error!(
                "Hyperliquid {} private subscription identity rejected; disabling this path: {err:#}",
                path.as_str()
            );
            return SessionDisposition::Restart;
        }
    }

    let channel = root.get("channel").and_then(Value::as_str);
    if channel.is_some_and(is_private_subscription_channel)
        && !subscription_acks.has_acknowledged_frame(&root)
    {
        error!(
            "Hyperliquid {} received private channel {:?} before its validated acknowledgement; disabling this path",
            path.as_str(),
            channel
        );
        return SessionDisposition::Restart;
    }
    let state_stream = channel.and_then(AccountStateStream::from_channel);
    let factual_channel = channel.is_some_and(is_private_factual_channel);
    let recognized_private_channel = channel.is_some_and(is_private_subscription_channel);
    let received_at = Instant::now();

    DIRECT_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        let Some(state) = state.as_mut() else {
            return SessionDisposition::Continue;
        };
        if factual_channel && !flush_direct_factual_outbox(state) {
            error!(
                "Hyperliquid {} factual IPC outbox is still blocked; new {:?} frame was not processed and the path will reconnect",
                path.as_str(),
                channel
            );
            return SessionDisposition::Restart;
        }
        if let Some(stream) = state_stream {
            if !state.state_sources.allows(path, stream, received_at) {
                return SessionDisposition::StateObserved(stream);
            }
        }
        match state
            .processor
            .process_value_at_with_fill_snapshot_context(
                &root,
                chrono::Utc::now().timestamp_millis(),
                fill_snapshot_context,
            )
        {
            Ok(events) => {
                if let Some(stream) = state_stream {
                    *batch_id = batch_id.checked_add(1).unwrap_or(1);
                    let account_hash = state.fact_replay.account_hash;
                    if let Err(err) = forward_snapshot_batch(
                        &mut state.forwarder,
                        &events,
                        path,
                        stream,
                        account_mode,
                        account_hash,
                        monitor_id,
                        generation,
                        *batch_id,
                        snapshot_ttl,
                    ) {
                        error!(
                            "Hyperliquid {} {} snapshot batch failed: {err:#}; COMPLETE suppressed and path will reconnect",
                            path.as_str(),
                            stream.as_str()
                        );
                        return SessionDisposition::Restart;
                    }
                    match state.state_sources.record_success(path, stream, received_at) {
                        StateSourceTransition::SecondaryActivated => warn!(
                            "Hyperliquid {} selected the secondary state stream because primary has no accepted snapshot or exceeded its silence timeout",
                            stream.as_str()
                        ),
                        StateSourceTransition::PrimaryRestored => info!(
                            "Hyperliquid {} restored the primary state stream",
                            stream.as_str()
                        ),
                        StateSourceTransition::None => {}
                    }
                    return SessionDisposition::StateObserved(stream);
                }
                if !send_or_queue_direct_factual_events(state, events) {
                    error!(
                        "Hyperliquid {} factual IPC send failed; the failed event and every remaining event are queued in order, and the path will reconnect",
                        path.as_str()
                    );
                    return SessionDisposition::Restart;
                }
            }
            Err(err) => {
                warn!(
                    "Hyperliquid {} account message rejected: {err:#}",
                    path.as_str()
                );
                if recognized_private_channel {
                    return SessionDisposition::Restart;
                }
            }
        }
        SessionDisposition::Continue
    })
}

#[allow(clippy::too_many_arguments)]
fn forward_snapshot_batch(
    forwarder: &mut PmForwarder,
    events: &[Bytes],
    path: StreamPath,
    stream: AccountStateStream,
    account_mode: HyperliquidAccountMode,
    account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    monitor_id: u64,
    generation: u64,
    batch_id: u64,
    snapshot_ttl: Duration,
) -> Result<()> {
    let mut completion_count = 0_usize;
    let mut borrowing_valid_until = i64::MAX;
    for event in events {
        if let Some((BasicAccountEventType::HyperliquidSnapshotComplete, scope, body)) =
            split_basic_account_event(event)
        {
            let marker = HyperliquidSnapshotCompleteMsg::from_bytes(body)
                .context("decode processor snapshot completion")?;
            let expected_scope = snapshot_scope(account_mode, stream);
            if scope != expected_scope || marker.venue != stream.venue().to_u8() {
                anyhow::bail!(
                    "processor snapshot completion identity mismatch: expected_scope={} actual_scope={} expected_venue={:?} actual_venue={}",
                    expected_scope.as_str(),
                    scope.as_str(),
                    stream.venue(),
                    marker.venue
                );
            }
            completion_count += 1;
            if account_mode == HyperliquidAccountMode::PortfolioMargin
                && stream == AccountStateStream::Spot
            {
                borrowing_valid_until = marker.timestamp.saturating_add(
                    account_monitor_common::hyperliquid_account::HYPERLIQUID_BORROW_SNAPSHOT_TTL_MS,
                );
            }
        }
    }
    if completion_count != 1 {
        anyhow::bail!(
            "processor snapshot batch must contain exactly one completion, got {completion_count}"
        );
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let valid_until = snapshot_valid_until(now_ms, snapshot_ttl).min(borrowing_valid_until);
    if valid_until <= now_ms {
        anyhow::bail!("Hyperliquid PM borrow snapshot expired before publishing account batch");
    }
    let begin = snapshot_control_event(
        HyperliquidSnapshotPhase::Begin,
        path,
        stream,
        account_mode,
        account_hash,
        monitor_id,
        generation,
        batch_id,
        now_ms,
        valid_until,
    );
    if !forwarder.send_raw(&begin) {
        anyhow::bail!("failed to publish snapshot BEGIN");
    }

    for event in events {
        if matches!(
            split_basic_account_event(event),
            Some((BasicAccountEventType::HyperliquidSnapshotComplete, _, _))
        ) {
            continue;
        }
        if !forwarder.send_raw(event) {
            anyhow::bail!("failed to publish snapshot row");
        }
    }

    let complete = snapshot_control_event(
        HyperliquidSnapshotPhase::Complete,
        path,
        stream,
        account_mode,
        account_hash,
        monitor_id,
        generation,
        batch_id,
        now_ms,
        valid_until,
    );
    if !forwarder.send_raw(&complete) {
        anyhow::bail!("failed to publish snapshot COMPLETE");
    }
    Ok(())
}

fn invalidate_stream_path(
    path: StreamPath,
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    generation: u64,
    snapshot_ttl: Duration,
) -> bool {
    DIRECT_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        let Some(state) = state.as_mut() else {
            return false;
        };
        state.path_generations[path.index()] = state.path_generations[path.index()].max(generation);
        state.state_sources.invalidate_path(path);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let valid_until = snapshot_valid_until(now_ms, snapshot_ttl);
        let account_hash = state.fact_replay.account_hash;
        let mut sent = true;
        for stream in [AccountStateStream::Spot, AccountStateStream::Perp] {
            let event = snapshot_control_event(
                HyperliquidSnapshotPhase::Invalidate,
                path,
                stream,
                account_mode,
                account_hash,
                monitor_id,
                generation,
                0,
                now_ms,
                valid_until,
            );
            sent &= state.forwarder.send_raw(&event);
        }
        if !sent {
            error!(
                "Hyperliquid {} failed to publish one or more snapshot invalidations",
                path.as_str()
            );
        }
        sent
    })
}

fn invalidate_all_stream_paths(
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    snapshot_ttl: Duration,
) -> bool {
    let path_generations =
        DIRECT_STATE.with(|cell| cell.borrow().as_ref().map(|state| state.path_generations));
    let Some(path_generations) = path_generations else {
        return false;
    };

    let mut sent = true;
    let mut found_generation = false;
    for path in [StreamPath::Primary, StreamPath::Secondary] {
        let generation = path_generations[path.index()];
        if generation == 0 {
            continue;
        }
        found_generation = true;
        sent &= invalidate_stream_path(path, account_mode, monitor_id, generation, snapshot_ttl);
    }
    found_generation && sent
}

fn begin_runtime_drift_restart(
    reason: &str,
    shutdown_tx: &watch::Sender<bool>,
    account_mode: HyperliquidAccountMode,
    monitor_id: u64,
    snapshot_ttl: Duration,
) {
    error!("{reason}; stopping both stream paths and invalidating every live account snapshot before restart");
    let _ = shutdown_tx.send(true);
    if !invalidate_all_stream_paths(account_mode, monitor_id, snapshot_ttl) {
        error!("failed to publish one or more terminal Hyperliquid snapshot invalidations");
    }
}

#[allow(clippy::too_many_arguments)]
fn snapshot_control_event(
    phase: HyperliquidSnapshotPhase,
    path: StreamPath,
    stream: AccountStateStream,
    account_mode: HyperliquidAccountMode,
    account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    monitor_id: u64,
    generation: u64,
    batch_id: u64,
    timestamp: i64,
    valid_until: i64,
) -> Bytes {
    let marker = HyperliquidSnapshotCompleteMsg::create_control(
        phase,
        path.snapshot_path(),
        stream.venue().to_u8(),
        monitor_id,
        generation,
        batch_id,
        timestamp,
        valid_until,
    )
    .with_account_hash(account_hash);
    BasicAccountEventMsg::create(
        BasicAccountEventType::HyperliquidSnapshotComplete,
        snapshot_scope(account_mode, stream),
        marker.to_bytes(),
    )
    .to_bytes()
}

fn snapshot_scope(
    account_mode: HyperliquidAccountMode,
    stream: AccountStateStream,
) -> mkt_parsers::msg::basic_account_msg::BasicAccountScope {
    match stream {
        AccountStateStream::Spot => account_mode.spot_scope(),
        AccountStateStream::Perp => account_mode.perp_scope(),
    }
}

fn snapshot_valid_until(timestamp: i64, snapshot_ttl: Duration) -> i64 {
    let ttl_ms = i64::try_from(snapshot_ttl.as_millis()).unwrap_or(i64::MAX);
    timestamp.saturating_add(ttl_ms)
}

fn monitor_instance_id() -> u64 {
    let now = chrono::Utc::now().timestamp_micros().unsigned_abs();
    (now ^ u64::from(std::process::id()).rotate_left(32)).max(1)
}

fn composite_generation(supervisor_session: u64, connection_generation: u64) -> u64 {
    let session = supervisor_session.min(u64::from(u32::MAX));
    let connection = connection_generation.min(u64::from(u32::MAX));
    (session << 32) | connection
}

fn fill_snapshot_context_for_generation(_generation: u64) -> FillSnapshotContext {
    // The only Initial snapshot is the buffered primary snapshot replayed
    // inside the authoritative startup cut. Every supervised frame is post-cut recovery,
    // including the secondary path's first connection.
    FillSnapshotContext::Reconnect
}

fn reset_path_protocol_for_generation(
    subscription_acks: &mut HyperliquidSubscriptionAcks,
    subscription_identity_valid: &mut bool,
    generation: u64,
) -> FillSnapshotContext {
    subscription_acks.reset();
    *subscription_identity_valid = true;
    fill_snapshot_context_for_generation(generation)
}

fn is_private_subscription_channel(channel: &str) -> bool {
    matches!(
        channel,
        "orderUpdates"
            | "userFills"
            | "spotState"
            | "clearinghouseState"
            | "allDexsClearinghouseState"
            | "userFundings"
            | "userNonFundingLedgerUpdates"
            | "userTwapSliceFills"
            | "userTwapHistory"
            | "user"
            | "twapStates"
            | "activeAssetData"
            | "notification"
            | "webData3"
    )
}

fn validate_private_subscription_budget(
    count: usize,
    primary: &str,
    secondary: &str,
) -> Result<()> {
    let bound_ip = |address: &str| -> Result<Option<std::net::IpAddr>> {
        if address.is_empty() {
            return Ok(None);
        }
        let ip = address
            .parse::<std::net::IpAddr>()
            .context("invalid Hyperliquid private source IP")?;
        Ok((!ip.is_unspecified()).then_some(ip))
    };
    let separate =
        matches!((bound_ip(primary)?, bound_ip(secondary)?), (Some(a), Some(b)) if a != b);
    let per_ip = count.saturating_mul(if separate { 1 } else { 2 });
    if per_ip > 1_000 {
        anyhow::bail!("Hyperliquid private subscriptions exceed per-IP capacity: {per_ip}; configure distinct public egress sources");
    }
    // Distinct source addresses still require an operational check that NAT
    // does not merge them, and other processes consume this same venue quota.
    Ok(())
}

fn is_private_factual_channel(channel: &str) -> bool {
    matches!(
        channel,
        "orderUpdates"
            | "userFills"
            | "userFundings"
            | "userNonFundingLedgerUpdates"
            | "userTwapSliceFills"
            | "userTwapHistory"
            | "user"
            | "twapStates"
            | "activeAssetData"
            | "notification"
            | "webData3"
    )
}

fn flush_factual_outbox_with<F>(outbox: &mut VecDeque<Bytes>, mut send: F) -> bool
where
    F: FnMut(&Bytes) -> bool,
{
    while let Some(event) = outbox.front() {
        if !send(event) {
            return false;
        }
        outbox.pop_front();
    }
    true
}

fn send_or_queue_factual_events_with<F>(
    outbox: &mut VecDeque<Bytes>,
    events: Vec<Bytes>,
    mut send: F,
) -> bool
where
    F: FnMut(&Bytes) -> bool,
{
    if !flush_factual_outbox_with(outbox, &mut send) {
        outbox.extend(events);
        return false;
    }

    let mut events = events.into_iter();
    while let Some(event) = events.next() {
        if send(&event) {
            continue;
        }
        outbox.push_back(event);
        outbox.extend(events);
        return false;
    }
    true
}

fn flush_direct_factual_outbox(state: &mut DirectAccountState) -> bool {
    let forwarder = &mut state.forwarder;
    flush_factual_outbox_with(&mut state.factual_outbox, |event| forwarder.send_raw(event))
}

fn send_or_queue_direct_factual_events(state: &mut DirectAccountState, events: Vec<Bytes>) -> bool {
    let events = match state.fact_replay.stamp_events(events) {
        Ok(events) => events,
        Err(err) => {
            error!("failed to stamp Hyperliquid factual event: {err:#}");
            return false;
        }
    };
    send_or_queue_direct_stamped_factual_events(state, events)
}

fn send_or_queue_direct_stamped_factual_events(
    state: &mut DirectAccountState,
    events: Vec<Bytes>,
) -> bool {
    let forwarder = &mut state.forwarder;
    send_or_queue_factual_events_with(&mut state.factual_outbox, events, |event| {
        forwarder.send_raw(event)
    })
}

fn service_fact_replay_requests() {
    DIRECT_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        let Some(state) = state.as_mut() else {
            return;
        };
        // One bounded replay transaction per tick keeps aggregate replay traffic
        // below the regular account subscriber buffer even after stale request
        // history or several consumers attach together.
        let requests = state.fact_replay_requests.drain(1);
        if requests.is_empty() {
            return;
        }
        if !flush_direct_factual_outbox(state) {
            warn!(
                "Hyperliquid factual outbox blocked replay request handling; consumers will retry"
            );
            return;
        }
        for request in requests {
            if !state
                .fact_replay
                .serve_request(&mut state.forwarder, &request)
            {
                warn!(
                    "Hyperliquid fact replay response publish failed: consumer_id={} request_id={}",
                    request.consumer_id, request.request_id
                );
                break;
            }
        }
    });
}

fn flush_pending_fills() {
    DIRECT_STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        let Some(state) = state.as_mut() else {
            return;
        };
        if !flush_direct_factual_outbox(state) {
            warn!(
                "Hyperliquid factual IPC outbox remains blocked; pending fills stay in processor state"
            );
            return;
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        match state.processor.flush_pending_fills(now_ms, 5_000) {
            Ok(events) => {
                if !send_or_queue_direct_factual_events(state, events) {
                    warn!(
                        "failed to forward unmatched Hyperliquid fill; unsent events remain queued in factual IPC order"
                    );
                }
            }
            Err(err) => warn!("failed to flush pending Hyperliquid fills: {err:#}"),
        }
    });
}

fn env_duration_ms(name: &str, default: Duration) -> Result<Duration> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    let millis = value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer number of milliseconds"))?;
    if millis == 0 {
        anyhow::bail!("{name} must be greater than zero");
    }
    Ok(Duration::from_millis(millis))
}

fn validate_runtime_drift_interval(interval: Duration, snapshot_ttl: Duration) -> Result<()> {
    if interval >= snapshot_ttl {
        anyhow::bail!(
            "HYPERLIQUID_ACCOUNT_DRIFT_CHECK_MS ({}) must be less than HYPERLIQUID_ACCOUNT_SNAPSHOT_TTL_MS ({})",
            interval.as_millis(),
            snapshot_ttl.as_millis(),
        );
    }
    Ok(())
}

fn runtime_drift_retry_delay(remaining: Duration) -> Option<Duration> {
    if remaining <= RUNTIME_DRIFT_MIN_REQUEST_BUDGET {
        return None;
    }
    let delay = RUNTIME_DRIFT_RETRY_DELAY
        .min(remaining / 2)
        .min(remaining - RUNTIME_DRIFT_MIN_REQUEST_BUDGET);
    (!delay.is_zero()).then_some(delay)
}

#[cfg(test)]
fn env_bool(name: &str, default: bool) -> Result<bool> {
    let value = std::env::var(name).ok();
    parse_optional_bool(name, value.as_deref(), default)
}

fn parse_optional_bool(name: &str, value: Option<&str>, default: bool) -> Result<bool> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{name} must be a boolean"),
    }
}

fn fill_snapshot_policy_from_env_value(value: Option<&str>) -> Result<FillSnapshotPolicy> {
    if parse_optional_bool("HYPERLIQUID_PROCESS_FILL_SNAPSHOT", value, true)? {
        Ok(FillSnapshotPolicy::Process)
    } else {
        Ok(FillSnapshotPolicy::Ignore)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_common::hyperliquid::{
        HYPERLIQUID_MAINNET_INFO_URL, HYPERLIQUID_MAINNET_WS_URL, HYPERLIQUID_TESTNET_INFO_URL,
    };

    fn factual_order_event(order_id: i64) -> Bytes {
        let msg = HyperliquidBasicOrderMsg::create(
            TradingVenue::HyperliquidFutures.to_u8(),
            1_725_000_000_000 + order_id,
            "BTCUSDC".to_string(),
            order_id,
            order_id,
            format!("0x{order_id:032x}"),
            1,
            1,
            1,
            1,
            1,
            63_000.0,
            0.1,
            0.0,
            "open".to_string(),
        );
        BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::HyperliquidStdPerp,
            msg.to_bytes(),
        )
        .to_bytes()
    }

    fn factual_funding_event() -> Bytes {
        let msg = HyperliquidFundingMsg::create(
            1_725_000_000_100,
            "BTC".to_string(),
            "-0.125".to_string(),
            "1.5".to_string(),
            "0.0001".to_string(),
        );
        BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidFunding,
            BasicAccountScope::HyperliquidUnified,
            msg.to_bytes(),
        )
        .to_bytes()
    }

    fn factual_ledger_event() -> Bytes {
        let msg = HyperliquidLedgerMsg::create(
            1_725_000_000_200,
            "0xabc".to_string(),
            "deposit".to_string(),
            r#"{"type":"deposit","usdc":"10"}"#.to_string(),
        );
        BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidLedger,
            BasicAccountScope::HyperliquidUnified,
            msg.to_bytes(),
        )
        .to_bytes()
    }

    #[test]
    fn private_subscription_budget_handles_unbound_and_equivalent_ipv6_sources() {
        assert!(validate_private_subscription_budget(500, "", "").is_ok());
        assert!(validate_private_subscription_budget(501, "::", "10.0.0.2").is_err());
        assert!(validate_private_subscription_budget(501, "2001:db8::1", "2001:0db8::1").is_err());
        assert!(validate_private_subscription_budget(1000, "10.0.0.1", "10.0.0.2").is_ok());
        assert!(validate_private_subscription_budget(1001, "10.0.0.1", "10.0.0.2").is_err());
        assert!(validate_private_subscription_budget(1, "invalid", "").is_err());
    }

    #[test]
    fn factual_producer_retains_native_event_and_stamps_identity() {
        use mkt_parsers::msg::hyperliquid_native_msg::HyperliquidNativeSource;
        let mut producer =
            HyperliquidFactReplayProducer::new([5; 32], 90, HyperliquidAccountMode::Unified);
        let msg = HyperliquidNativeEventMsg::create(
            1000,
            HyperliquidNativeSource::NonUserCancel,
            "BTC:7".into(),
            &serde_json::json!({"coin":"BTC","oid":7}),
        )
        .unwrap();
        let envelope = BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidNativeEvent,
            BasicAccountScope::HyperliquidUnified,
            msg.to_bytes(),
        )
        .to_bytes();
        let events = producer.stamp_events(vec![envelope]).unwrap();
        let (_, _, body) = split_basic_account_event(&events[0]).unwrap();
        let stamped = HyperliquidNativeEventMsg::from_bytes(body).unwrap();
        assert_eq!(
            stamped.identity,
            HyperliquidFactIdentity {
                account_hash: [5; 32],
                monitor_id: 90,
                fact_seq: 1
            }
        );
        assert_eq!(stamped.payload_json, msg.payload_json);
        assert_eq!(producer.ring[0].payload, events[0]);
    }

    #[test]
    fn factual_producer_sequences_and_retains_funding_and_ledger() {
        let account_hash = [5; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let mut producer =
            HyperliquidFactReplayProducer::new(account_hash, 90, HyperliquidAccountMode::Unified);
        let events = producer
            .stamp_events(vec![factual_funding_event(), factual_ledger_event()])
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(producer.ring.len(), 2);
        assert_eq!(producer.head_seq(), 2);

        let (kind, scope, body) = split_basic_account_event(&events[0]).unwrap();
        assert_eq!(kind, BasicAccountEventType::HyperliquidFunding);
        assert_eq!(scope, BasicAccountScope::HyperliquidUnified);
        let funding = HyperliquidFundingMsg::from_bytes(body).unwrap();
        assert_eq!(funding.account_hash, account_hash);
        assert_eq!(funding.monitor_id, 90);
        assert_eq!(funding.fact_seq, 1);

        let (kind, scope, body) = split_basic_account_event(&events[1]).unwrap();
        assert_eq!(kind, BasicAccountEventType::HyperliquidLedger);
        assert_eq!(scope, BasicAccountScope::HyperliquidUnified);
        let ledger = HyperliquidLedgerMsg::from_bytes(body).unwrap();
        assert_eq!(ledger.account_hash, account_hash);
        assert_eq!(ledger.monitor_id, 90);
        assert_eq!(ledger.fact_seq, 2);
        assert_eq!(producer.ring[0].payload, events[0]);
        assert_eq!(producer.ring[1].payload, events[1]);
    }

    #[test]
    fn factual_producer_stamps_monotonic_identity_and_detects_ring_gaps() {
        let account_hash = [6; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let mut producer =
            HyperliquidFactReplayProducer::new(account_hash, 91, HyperliquidAccountMode::Standard);
        producer.message_capacity = 2;
        for order_id in 1..=3 {
            let events = producer
                .stamp_events(vec![factual_order_event(order_id)])
                .unwrap();
            let (_, _, body) = split_basic_account_event(&events[0]).unwrap();
            let decoded = HyperliquidBasicOrderMsg::from_bytes(body).unwrap();
            assert_eq!(decoded.account_hash, account_hash);
            assert_eq!(decoded.monitor_id, 91);
            assert_eq!(decoded.fact_seq, order_id as u64);
        }
        assert_eq!(producer.head_seq(), 3);
        assert_eq!(producer.earliest_seq(), 2);

        let brand_new = HyperliquidFactReplayRequestMsg {
            account_hash,
            consumer_id: 1,
            request_id: 1,
            last_monitor_id: 0,
            last_fact_seq: 0,
        };
        assert!(producer.replay_range(&brand_new).is_err());
        assert_eq!(
            producer.replay_range(&HyperliquidFactReplayRequestMsg {
                last_monitor_id: 91,
                last_fact_seq: 1,
                ..brand_new
            }),
            Ok((2, 3))
        );
        assert!(producer
            .replay_range(&HyperliquidFactReplayRequestMsg {
                last_monitor_id: 90,
                last_fact_seq: 3,
                ..brand_new
            })
            .is_err());
        assert!(producer
            .replay_range(&HyperliquidFactReplayRequestMsg {
                account_hash: [7; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
                last_monitor_id: 91,
                last_fact_seq: 1,
                ..brand_new
            })
            .is_err());
    }

    #[test]
    fn factual_replay_transactions_are_bounded_below_the_account_ipc_buffer() {
        let account_hash = [8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let mut producer =
            HyperliquidFactReplayProducer::new(account_hash, 92, HyperliquidAccountMode::Unified);
        for _ in 0..300 {
            let identity = producer.allocate_identity().unwrap();
            producer
                .retain(identity.fact_seq, Bytes::from_static(b"fact"))
                .unwrap();
        }
        let request = HyperliquidFactReplayRequestMsg {
            account_hash,
            consumer_id: 2,
            request_id: 3,
            last_monitor_id: 0,
            last_fact_seq: 0,
        };
        assert_eq!(
            producer.replay_transaction_range(&request),
            Ok((1, FACT_REPLAY_TRANSACTION_MAX_FACTS, 300))
        );
    }

    #[test]
    fn secondary_only_idle_startup_is_ready_and_primary_recovers_immediately() {
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut arbiter = StateSourceArbiter::new(timeout);

        assert!(arbiter.allows(StreamPath::Secondary, AccountStateStream::Spot, start));
        assert_eq!(
            arbiter.record_success(StreamPath::Secondary, AccountStateStream::Spot, start),
            StateSourceTransition::SecondaryActivated
        );
        assert_eq!(
            arbiter.record_success(
                StreamPath::Primary,
                AccountStateStream::Spot,
                start + Duration::from_secs(1)
            ),
            StateSourceTransition::PrimaryRestored
        );
        assert!(!arbiter.allows(
            StreamPath::Secondary,
            AccountStateStream::Spot,
            start + Duration::from_secs(10)
        ));
        assert!(arbiter.allows(
            StreamPath::Secondary,
            AccountStateStream::Spot,
            start + Duration::from_secs(11)
        ));
    }

    #[test]
    fn spot_and_perp_state_fail_over_independently_while_facts_bypass_the_gate() {
        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        let mut arbiter = StateSourceArbiter::new(timeout);
        arbiter.record_success(
            StreamPath::Primary,
            AccountStateStream::Spot,
            start + Duration::from_secs(4),
        );

        let now = start + Duration::from_secs(6);
        assert!(!arbiter.allows(StreamPath::Secondary, AccountStateStream::Spot, now));
        assert!(arbiter.allows(StreamPath::Secondary, AccountStateStream::Perp, now));
        assert_eq!(AccountStateStream::from_channel("orderUpdates"), None);
        assert_eq!(AccountStateStream::from_channel("userFills"), None);
        assert!(is_private_subscription_channel("orderUpdates"));
        assert!(is_private_subscription_channel("userFills"));
    }

    #[test]
    fn invalidating_primary_allows_secondary_immediately() {
        let start = Instant::now();
        let mut arbiter = StateSourceArbiter::new(Duration::from_secs(30));
        arbiter.record_success(StreamPath::Primary, AccountStateStream::Spot, start);
        assert!(!arbiter.allows(
            StreamPath::Secondary,
            AccountStateStream::Spot,
            start + Duration::from_secs(1)
        ));
        arbiter.invalidate_path(StreamPath::Primary);
        assert!(arbiter.allows(
            StreamPath::Secondary,
            AccountStateStream::Spot,
            start + Duration::from_secs(1)
        ));
    }

    #[test]
    fn runtime_drift_guard_unchanged_refresh_renews_validation_deadline() {
        let start = Instant::now();
        let mut guard = RuntimeDriftGuard::new(
            HyperliquidUserRole::User,
            HyperliquidAccountMode::Standard,
            start,
            Duration::from_secs(10),
        );
        assert_eq!(
            guard.observe_role(HyperliquidUserRole::User),
            RuntimeDriftDecision::Continue
        );
        assert_eq!(
            guard.observe_success(
                start + Duration::from_secs(4),
                HyperliquidAccountMode::Standard,
                true,
            ),
            RuntimeDriftDecision::Continue
        );
        assert_eq!(
            guard.observe_failure(start + Duration::from_secs(13)),
            RuntimeDriftDecision::Continue
        );
        assert_eq!(
            guard.observe_failure(start + Duration::from_secs(14)),
            RuntimeDriftDecision::Restart(RuntimeDriftReason::ValidationExpired)
        );
    }

    #[test]
    fn runtime_drift_guard_restarts_on_role_mode_or_catalog_change() {
        let start = Instant::now();
        let role_guard = RuntimeDriftGuard::new(
            HyperliquidUserRole::User,
            HyperliquidAccountMode::Standard,
            start,
            Duration::from_secs(10),
        );
        for changed_role in [
            HyperliquidUserRole::Agent,
            HyperliquidUserRole::Vault,
            HyperliquidUserRole::SubAccount,
            HyperliquidUserRole::Missing,
        ] {
            assert_eq!(
                role_guard.observe_role(changed_role),
                RuntimeDriftDecision::Restart(RuntimeDriftReason::AccountRoleChanged)
            );
        }

        let mut mode_guard = RuntimeDriftGuard::new(
            HyperliquidUserRole::User,
            HyperliquidAccountMode::Standard,
            start,
            Duration::from_secs(10),
        );
        assert_eq!(
            mode_guard.observe_success(
                start + Duration::from_secs(1),
                HyperliquidAccountMode::Unified,
                true,
            ),
            RuntimeDriftDecision::Restart(RuntimeDriftReason::AccountModeChanged)
        );

        let mut catalog_guard = RuntimeDriftGuard::new(
            HyperliquidUserRole::User,
            HyperliquidAccountMode::Standard,
            start,
            Duration::from_secs(10),
        );
        assert_eq!(
            catalog_guard.observe_success(
                start + Duration::from_secs(1),
                HyperliquidAccountMode::Standard,
                false,
            ),
            RuntimeDriftDecision::Restart(RuntimeDriftReason::AssetCatalogChanged)
        );
    }

    #[test]
    fn runtime_drift_guard_failure_expires_at_snapshot_ttl() {
        let start = Instant::now();
        let guard = RuntimeDriftGuard::new(
            HyperliquidUserRole::User,
            HyperliquidAccountMode::Unified,
            start,
            Duration::from_secs(60),
        );
        assert_eq!(
            guard.observe_failure(start + Duration::from_secs(59)),
            RuntimeDriftDecision::Continue
        );
        assert_eq!(
            guard.observe_failure(start + Duration::from_secs(60)),
            RuntimeDriftDecision::Restart(RuntimeDriftReason::ValidationExpired)
        );
        assert_eq!(
            guard.validation_remaining(start + Duration::from_secs(61)),
            Duration::ZERO
        );
        assert_eq!(guard.validation_deadline(), start + Duration::from_secs(60));
    }

    #[test]
    fn runtime_drift_interval_must_be_shorter_than_snapshot_ttl() {
        let ttl = Duration::from_secs(60);
        assert!(validate_runtime_drift_interval(Duration::from_secs(45), ttl).is_ok());
        assert!(validate_runtime_drift_interval(ttl, ttl).is_err());
        assert!(validate_runtime_drift_interval(Duration::from_secs(61), ttl).is_err());
    }

    #[test]
    fn runtime_drift_retry_is_bounded_by_deadline_request_budget() {
        assert_eq!(
            runtime_drift_retry_delay(Duration::from_secs(20)),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            runtime_drift_retry_delay(Duration::from_secs(8)),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            runtime_drift_retry_delay(Duration::from_secs(3)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(runtime_drift_retry_delay(Duration::from_secs(2)), None);
    }

    #[test]
    fn supervisor_restarts_keep_control_generations_monotonic() {
        assert!(composite_generation(1, 2) > composite_generation(1, 1));
        assert!(composite_generation(2, 1) > composite_generation(1, 99));
    }

    #[test]
    fn known_mainnet_testnet_endpoint_mix_is_rejected() {
        assert!(HyperliquidEndpoints::resolve(
            false,
            Some(HYPERLIQUID_TESTNET_INFO_URL),
            Some(HYPERLIQUID_MAINNET_WS_URL),
        )
        .is_err());
        assert!(HyperliquidEndpoints::resolve(
            true,
            Some(HYPERLIQUID_MAINNET_INFO_URL),
            Some(HYPERLIQUID_MAINNET_WS_URL),
        )
        .is_err());
    }

    #[test]
    fn initial_fill_snapshot_defaults_to_process_and_false_selects_baseline_only() {
        assert_eq!(
            fill_snapshot_policy_from_env_value(None).unwrap(),
            FillSnapshotPolicy::Process
        );
        assert_eq!(
            fill_snapshot_policy_from_env_value(Some("true")).unwrap(),
            FillSnapshotPolicy::Process
        );
        assert_eq!(
            fill_snapshot_policy_from_env_value(Some("false")).unwrap(),
            FillSnapshotPolicy::Ignore
        );
        assert!(fill_snapshot_policy_from_env_value(Some("maybe")).is_err());
    }

    #[test]
    fn every_supervised_generation_recovers_fill_snapshots() {
        assert_eq!(
            fill_snapshot_context_for_generation(1),
            FillSnapshotContext::Reconnect
        );
        assert_eq!(
            fill_snapshot_context_for_generation(2),
            FillSnapshotContext::Reconnect
        );
    }

    #[test]
    fn new_connection_generation_resets_acknowledgements_and_reenables_path() {
        const USER: &str = "0x1111111111111111111111111111111111111111";
        let requests = subscription_messages(USER, HyperliquidAccountMode::Standard).unwrap();
        let mut acks = HyperliquidSubscriptionAcks::from_requests(&requests).unwrap();
        for request in &requests {
            acks.observe(&serde_json::json!({
                "channel": "subscriptionResponse",
                "data": request
            }))
            .unwrap();
        }
        assert!(acks.is_complete());

        let mut path_valid = false;
        assert_eq!(
            reset_path_protocol_for_generation(&mut acks, &mut path_valid, 2),
            FillSnapshotContext::Reconnect
        );
        assert!(path_valid);
        assert!(!acks.is_complete());
        assert!(!acks.has_acknowledged("orderUpdates"));
    }

    #[test]
    fn connection_cut_buffers_private_frames_only_after_their_ack_and_waits_for_all_acks() {
        const USER: &str = "0x1111111111111111111111111111111111111111";
        let requests = subscription_messages(USER, HyperliquidAccountMode::Standard).unwrap();
        let order_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "channel": "orderUpdates",
                "data": []
            }))
            .unwrap(),
        );

        let mut unacknowledged = HyperliquidSubscriptionAcks::from_requests(&requests).unwrap();
        assert!(observe_connection_recovery_payload(
            StreamPath::Secondary,
            order_frame.clone(),
            &mut unacknowledged,
            &mut BootstrapFrameBuffer::default(),
        )
        .is_err());

        let mut acks = HyperliquidSubscriptionAcks::from_requests(&requests).unwrap();
        let mut buffer = BootstrapFrameBuffer::default();
        let first_ack = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "channel": "subscriptionResponse",
                "data": requests[0]
            }))
            .unwrap(),
        );
        observe_connection_recovery_payload(
            StreamPath::Secondary,
            first_ack,
            &mut acks,
            &mut buffer,
        )
        .unwrap();
        observe_connection_recovery_payload(
            StreamPath::Secondary,
            order_frame,
            &mut acks,
            &mut buffer,
        )
        .unwrap();
        assert!(!acks.is_complete());
        assert_eq!(buffer.frames.len(), 1);

        for request in requests.iter().skip(1) {
            let ack = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "channel": "subscriptionResponse",
                    "data": request
                }))
                .unwrap(),
            );
            observe_connection_recovery_payload(StreamPath::Secondary, ack, &mut acks, &mut buffer)
                .unwrap();
        }
        assert!(acks.is_complete());
        assert_eq!(buffer.frames.len(), 1);
        let buffered: Value = serde_json::from_slice(&buffer.frames[0]).unwrap();
        assert_eq!(buffered["channel"], "orderUpdates");
    }

    #[test]
    fn bool_env_parser_rejects_ambiguous_values() {
        let name = "HYPERLIQUID_ACCOUNT_MONITOR_TEST_BOOL";
        std::env::set_var(name, "true");
        assert!(env_bool(name, false).unwrap());
        std::env::set_var(name, "maybe");
        assert!(env_bool(name, false).is_err());
        std::env::remove_var(name);
    }

    #[test]
    fn single_http_cut_replays_active_order_changes_without_losing_attribution() {
        const USER: &str = "0x1111111111111111111111111111111111111111";
        const CLOID: &str = "0x6d6b745f73696731000000000000002a";
        let catalog = HyperliquidAssetCatalog::from_meta(
            &serde_json::json!({"universe": [{"name": "BTC", "szDecimals": 5}]}),
            &serde_json::json!({
                "tokens": [{"name": "USDC", "index": 0}],
                "universe": []
            }),
        )
        .unwrap();
        let partial_order = serde_json::json!({
            "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
            "oid": 9, "timestamp": 1000, "origSz": "2", "cloid": CLOID
        });
        let first_fill = serde_json::json!({
            "coin": "BTC", "px": "60000", "sz": "1", "side": "B",
            "time": 1100, "hash": "0xfill1", "oid": 9,
            "crossed": true, "tid": 101
        });
        let second_fill = serde_json::json!({
            "coin": "BTC", "px": "60001", "sz": "1", "side": "B",
            "time": 1200, "hash": "0xfill2", "oid": 9,
            "crossed": true, "tid": 102
        });
        let http_cut = BootstrapHttpCut {
            borrowing: None,
            orders: BootstrapHttpSnapshot {
                historical_orders: serde_json::json!([{
                    "order": partial_order.clone(),
                    "status": "open",
                    "statusTimestamp": 1050
                }]),
                frontend_open_orders: vec![(
                    String::new(),
                    Value::Array(vec![partial_order.clone()]),
                )],
            },
            facts: RecoveredAccountFacts {
                fills: Value::Array(vec![first_fill.clone()]),
                fundings: Vec::new(),
                ledger_updates: Vec::new(),
                twap_slice_fills: Vec::new(),
                twap_history: Vec::new(),
            },
        };
        let buffered_frames = vec![
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "channel": "userFills",
                    "data": {"user": USER, "isSnapshot": true, "fills": [first_fill]}
                }))
                .unwrap(),
            ),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "channel": "userFills",
                    "data": {"user": USER, "fills": [second_fill]}
                }))
                .unwrap(),
            ),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "channel": "orderUpdates",
                    "data": [{
                        "order": {
                            "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0",
                            "oid": 9, "timestamp": 1000, "origSz": "2", "cloid": CLOID
                        },
                        "status": "filled",
                        "statusTimestamp": 1300
                    }]
                }))
                .unwrap(),
            ),
        ];

        let applied = apply_bootstrap_http_cut(
            USER,
            &catalog,
            HyperliquidAccountMode::Standard,
            FillSnapshotPolicy::Process,
            http_cut,
            buffered_frames,
        )
        .unwrap();
        assert_eq!(applied.historical_seed_count, 1);
        assert_eq!(applied.frontend_seed_count, 1);
        assert_eq!(applied.recovered_fill_count, 1);

        let fills = applied
            .processed_frames
            .iter()
            .flat_map(|frame| frame.events.iter())
            .filter_map(|event| {
                let (event_type, _, body) = split_basic_account_event(event)?;
                (event_type == BasicAccountEventType::HyperliquidFill)
                    .then(|| HyperliquidBasicFillMsg::from_bytes(body).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].venue_trade_id, 101);
        assert_eq!(fills[1].venue_trade_id, 102);
        assert!(fills.iter().all(|fill| fill.order_id == 9));
        assert!(fills.iter().all(|fill| fill.client_order_id == 42));
        assert!(fills.iter().all(|fill| fill.cloid == CLOID));
        assert_eq!(fills[0].cumulative_filled_quantity, 1.0);
        assert_eq!(fills[1].cumulative_filled_quantity, 2.0);
        let order_updates = applied
            .processed_frames
            .iter()
            .flat_map(|frame| frame.events.iter())
            .filter_map(|event| {
                let (event_type, _, body) = split_basic_account_event(event)?;
                (event_type == BasicAccountEventType::OrderUpdate)
                    .then(|| HyperliquidBasicOrderMsg::from_bytes(body).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(order_updates.len(), 1);
        assert_eq!(
            order_updates[0].time_in_force,
            order_common::TimeInForce::GTX.to_u8()
        );
    }

    #[test]
    fn single_http_cut_still_fails_closed_on_incomplete_active_fill_history() {
        const USER: &str = "0x1111111111111111111111111111111111111111";
        const CLOID: &str = "0x6d6b745f73696731000000000000002a";
        let catalog = HyperliquidAssetCatalog::from_meta(
            &serde_json::json!({"universe": [{"name": "BTC", "szDecimals": 5}]}),
            &serde_json::json!({
                "tokens": [{"name": "USDC", "index": 0}],
                "universe": []
            }),
        )
        .unwrap();
        let partial_order = serde_json::json!({
            "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
            "oid": 9, "timestamp": 1000, "origSz": "2", "cloid": CLOID
        });
        let http_cut = BootstrapHttpCut {
            borrowing: None,
            orders: BootstrapHttpSnapshot {
                historical_orders: serde_json::json!([{
                    "order": partial_order.clone(),
                    "status": "open",
                    "statusTimestamp": 1050
                }]),
                frontend_open_orders: vec![(String::new(), Value::Array(vec![partial_order]))],
            },
            facts: RecoveredAccountFacts {
                fills: serde_json::json!([]),
                fundings: Vec::new(),
                ledger_updates: Vec::new(),
                twap_slice_fills: Vec::new(),
                twap_history: Vec::new(),
            },
        };
        let error = apply_bootstrap_http_cut(
            USER,
            &catalog,
            HyperliquidAccountMode::Standard,
            FillSnapshotPolicy::Process,
            http_cut,
            vec![Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "channel": "userFills",
                    "data": {"user": USER, "isSnapshot": true, "fills": []}
                }))
                .unwrap(),
            )],
        )
        .err()
        .expect("incomplete active fill history must reject the cut");
        assert!(format!("{error:#}").contains("factual fill history does not cover active"));
    }

    #[test]
    fn bootstrap_private_buffer_has_a_hard_message_cap() {
        let mut buffer = BootstrapFrameBuffer::default();
        for _ in 0..BOOTSTRAP_BUFFER_MESSAGE_CAPACITY {
            buffer.push(Bytes::new()).unwrap();
        }
        assert!(buffer.push(Bytes::new()).is_err());
    }

    #[test]
    fn factual_recovery_uses_inclusive_watermark_overlap() {
        assert_eq!(recovery_start_time(10_000, None, 2_000, 60), 8_000);
        assert_eq!(recovery_start_time(10_000, Some(9_000), 2_000, 60), 8_940);
        assert_eq!(recovery_start_time(10_000, Some(20), 2_000, 60), 0);
    }

    #[test]
    fn full_fill_retention_window_requires_strict_watermark_coverage() {
        assert!(validate_user_fill_retention_coverage(
            USER_FILLS_RETENTION_CAPACITY - 1,
            Some(2_000),
            None,
        )
        .is_ok());
        assert!(validate_user_fill_retention_coverage(
            USER_FILLS_RETENTION_CAPACITY,
            Some(2_000),
            None,
        )
        .is_err());
        assert!(validate_user_fill_retention_coverage(
            USER_FILLS_RETENTION_CAPACITY,
            Some(2_000),
            Some(2_000),
        )
        .is_err());
        assert!(validate_user_fill_retention_coverage(
            USER_FILLS_RETENTION_CAPACITY,
            Some(1_999),
            Some(2_000),
        )
        .is_ok());
    }

    #[test]
    fn time_history_pagination_fails_closed_when_an_inclusive_page_stalls() {
        assert!(next_time_history_start(
            TimeHistoryKind::Funding,
            1_000,
            TIME_HISTORY_PAGE_CAPACITY,
            Some(1_000),
            0,
            2,
        )
        .is_err());
        assert_eq!(
            next_time_history_start(
                TimeHistoryKind::Funding,
                1_000,
                TIME_HISTORY_PAGE_CAPACITY,
                Some(1_001),
                12,
                2,
            )
            .unwrap(),
            Some(1_001)
        );
        assert_eq!(
            next_time_history_start(
                TimeHistoryKind::NonFundingLedger,
                1_000,
                TIME_HISTORY_PAGE_CAPACITY - 1,
                Some(1_001),
                1,
                2,
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn http_funding_rows_normalize_to_the_ws_typed_contract() {
        const USER: &str = "0x1111111111111111111111111111111111111111";
        let frames = recovered_factual_frames(
            USER,
            RecoveredAccountFacts {
                fills: serde_json::json!([]),
                fundings: vec![serde_json::json!({
                    "time": 1_725_000_000_123_i64,
                    "hash": "0xfunding",
                    "delta": {
                        "type": "funding",
                        "coin": "xyz:FOO",
                        "usdc": "-0.1250",
                        "szi": "2.5",
                        "fundingRate": "0.0001",
                        "nSamples": null
                    }
                })],
                ledger_updates: vec![serde_json::json!({
                    "time": 1_725_000_000_124_i64,
                    "hash": "0xledger",
                    "delta": {"type": "deposit", "usdc": "10.0"}
                })],
                twap_slice_fills: Vec::new(),
                twap_history: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(frames.len(), 5);
        assert_eq!(frames[0]["channel"], "userFills");
        assert_eq!(frames[1]["channel"], "userFundings");
        assert_eq!(frames[1]["data"]["fundings"][0]["hash"], "0xfunding");
        assert_eq!(frames[1]["data"]["fundings"][0]["coin"], "xyz:FOO");
        assert!(frames[1]["data"]["fundings"][0].get("nSamples").is_none());
        assert_eq!(frames[2]["channel"], "userNonFundingLedgerUpdates");
        assert_eq!(frames[3]["channel"], "userTwapSliceFills");
        assert_eq!(frames[4]["channel"], "userTwapHistory");
    }

    #[test]
    fn bootstrap_protocol_requires_all_acks_and_the_account_fill_snapshot() {
        const USER: &str = "0x1111111111111111111111111111111111111111";
        let subscriptions = subscription_messages(USER, HyperliquidAccountMode::Standard).unwrap();
        let mut observer = BootstrapProtocolObserver {
            subscription_acks: HyperliquidSubscriptionAcks::from_requests(&subscriptions).unwrap(),
            connection_generation: Some(1),
            initial_user_fills_snapshot_seen: false,
            user: USER.to_string(),
        };
        let mut buffer = BootstrapFrameBuffer::default();
        for request in &subscriptions {
            let ack = serde_json::json!({
                "channel": "subscriptionResponse",
                "data": request,
            });
            observer
                .observe(Bytes::from(serde_json::to_vec(&ack).unwrap()), &mut buffer)
                .unwrap();
        }
        assert!(!observer.is_ready());

        let snapshot = serde_json::json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": true, "fills": []},
        });
        observer
            .observe(
                Bytes::from(serde_json::to_vec(&snapshot).unwrap()),
                &mut buffer,
            )
            .unwrap();
        assert!(observer.is_ready());
        assert_eq!(buffer.frames.len(), 1);
    }

    #[test]
    fn factual_outbox_keeps_the_failed_event_and_suffix_in_order() {
        let mut outbox = VecDeque::new();
        let events = vec![
            Bytes::from_static(b"first"),
            Bytes::from_static(b"second"),
            Bytes::from_static(b"third"),
        ];
        let mut attempted = Vec::new();
        let sent = send_or_queue_factual_events_with(&mut outbox, events, |event| {
            attempted.push(event.clone());
            event.as_ref() != b"second"
        });
        assert!(!sent);
        assert_eq!(
            attempted,
            vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
        );
        assert_eq!(
            outbox,
            VecDeque::from([Bytes::from_static(b"second"), Bytes::from_static(b"third")])
        );

        let mut recovered = Vec::new();
        assert!(flush_factual_outbox_with(&mut outbox, |event| {
            recovered.push(event.clone());
            true
        }));
        assert_eq!(
            recovered,
            vec![Bytes::from_static(b"second"), Bytes::from_static(b"third")]
        );
        assert!(outbox.is_empty());
    }

    #[test]
    fn blocked_factual_outbox_appends_new_events_without_reordering() {
        let mut outbox = VecDeque::from([
            Bytes::from_static(b"old-first"),
            Bytes::from_static(b"old-second"),
        ]);
        let mut attempted = Vec::new();
        let sent = send_or_queue_factual_events_with(
            &mut outbox,
            vec![Bytes::from_static(b"new")],
            |event| {
                attempted.push(event.clone());
                false
            },
        );
        assert!(!sent);
        assert_eq!(attempted, vec![Bytes::from_static(b"old-first")]);
        assert_eq!(
            outbox,
            VecDeque::from([
                Bytes::from_static(b"old-first"),
                Bytes::from_static(b"old-second"),
                Bytes::from_static(b"new"),
            ])
        );
    }
}
