use crate::{
    decode::Decoded,
    wire::{Coin, Event, Header, Kind, EVENT_BYTES, HEADER_BYTES, MAX_CHUNK_BYTES},
    Decoder,
};
use ahash::{AHashMap, AHasher};
use anyhow::{bail, ensure, Context, Result};
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use std::{
    hash::Hasher,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
};

#[derive(Args, Clone, Debug)]
pub struct Options {
    /// Node-side order_book_server endpoint; the public API has no l4Book.
    #[arg(long, env = "HYPERLIQUID_L4_WS_URL", hide_env_values = true)]
    pub ws_url: String,
    /// Compatible standby source. A switch always starts a new snapshot epoch.
    #[arg(long, env = "HYPERLIQUID_L4_FALLBACK_WS_URL", hide_env_values = true)]
    pub fallback_ws_url: Option<String>,
    /// Native perpetual wire coins, e.g. BTC,ETH,xyz:TSLA. Partition across pinned processes.
    #[arg(
        long,
        env = "HYPERLIQUID_L4_COINS",
        value_delimiter = ',',
        required = true
    )]
    pub coins: Vec<String>,
    #[arg(long, default_value_t = 4096)]
    pub reserve_events: usize,
    #[arg(long, default_value_t = 1_000_000)]
    pub max_events: usize,
    #[arg(long, default_value_t = 67_108_864)]
    pub max_frame_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub chunk_events: usize,
    /// Refresh snapshots for consumers that joined after startup.
    #[arg(long, default_value_t = 300)]
    pub resnapshot_secs: u64,
    /// Reject stale/future node blocks. Zero is for explicit historical replay.
    #[arg(long, default_value_t = 10_000)]
    pub max_source_lag_ms: u64,
}
pub trait Sink {
    /// Encode directly into transport-owned storage where available.
    fn publish(&mut self, header: &Header, events: &[Event]) -> Result<()>;
}
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u64::MAX as u128) as u64
}

pub async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result=tokio::signal::ctrl_c()=>{result?;},
            _=terminate.recv()=>{},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

impl Options {
    pub fn validate(&self) -> Result<Vec<Coin>> {
        ensure!(
            !self.coins.is_empty() && self.coins.len() <= 1000,
            "L4 requires 1..=1000 explicit wire coins"
        );
        ensure!(
            self.max_events > 0
                && self.max_events <= 10_000_000
                && self.reserve_events <= self.max_events,
            "invalid event capacity"
        );
        ensure!(
            self.max_frame_bytes >= 1024 && self.max_frame_bytes <= 1_073_741_824,
            "frame bound must be 1KiB..1GiB"
        );
        ensure!(
            self.chunk_events > 0
                && self
                    .chunk_events
                    .checked_mul(EVENT_BYTES)
                    .and_then(|v| v.checked_add(HEADER_BYTES))
                    .context("chunk capacity overflow")?
                    <= MAX_CHUNK_BYTES,
            "invalid chunk event count"
        );
        ensure!(
            self.resnapshot_secs >= 30,
            "resnapshot interval must be at least 30 seconds"
        );
        ensure!(
            self.max_source_lag_ms <= 600_000,
            "source lag bound exceeds ten minutes"
        );
        for endpoint in std::iter::once(&self.ws_url).chain(self.fallback_ws_url.iter()) {
            let request = endpoint
                .as_str()
                .into_client_request()
                .context("invalid node L4 WebSocket URL")?;
            ensure!(
                matches!(request.uri().scheme_str(), Some("ws" | "wss")),
                "L4 URL must be ws/wss"
            );
            ensure!(
                !matches!(
                    request.uri().host(),
                    Some("api.hyperliquid.xyz" | "api.hyperliquid-testnet.xyz")
                ),
                "public Hyperliquid API does not serve l4Book; configure a node-side endpoint"
            );
        }
        let mut coins = Vec::with_capacity(self.coins.len());
        for text in &self.coins {
            let coin = Coin::new(text)?;
            ensure!(
                !text.starts_with('@') && !text.contains('/'),
                "upstream l4Book service does not support spot books"
            );
            ensure!(!coins.contains(&coin), "duplicate L4 coin");
            coins.push(coin);
        }
        Ok(coins)
    }
}
#[derive(Default)]
struct Progress {
    ack: bool,
    snapshot: bool,
    sequence: u64,
    height: u64,
    time: u64,
    digest: u64,
}

#[derive(Default)]
struct Stats {
    frames: u64,
    events: u64,
    chunks: u64,
    duplicates: u64,
}

fn publish_frame<S: Sink>(
    options: &Options,
    epoch: [u8; 16],
    raw: &[u8],
    decoder: &mut Decoder,
    progress: &mut AHashMap<Coin, Progress>,
    sink: &mut S,
    stats: &mut Stats,
) -> Result<bool> {
    let receipt_time_us = now_us();
    match decoder.decode(raw)? {
        Decoded::Ack(coin) => {
            let state = progress
                .get_mut(&coin)
                .context("L4 ACK for unrequested coin")?;
            ensure!(!state.ack, "duplicate L4 ACK");
            state.ack = true;
        }
        Decoded::Pong => return Ok(true),
        Decoded::Batch(batch) => {
            let state = progress
                .get_mut(&batch.coin)
                .context("L4 data for unrequested coin")?;
            ensure!(state.ack, "L4 data before matching acknowledgement");
            if options.max_source_lag_ms > 0 {
                ensure!(
                    batch.event_time_us <= receipt_time_us.saturating_add(5_000_000),
                    "L4 node block time is in the future"
                );
                ensure!(
                    receipt_time_us.saturating_sub(batch.event_time_us)
                        <= options.max_source_lag_ms * 1000,
                    "L4 node block is stale"
                );
            }
            let mut hasher = AHasher::default();
            hasher.write(raw);
            let digest = hasher.finish();
            if state.snapshot && batch.height == state.height && digest == state.digest {
                stats.duplicates += 1;
                return Ok(false);
            }
            if batch.kind == Kind::Snapshot {
                ensure!(!state.snapshot, "unexpected second snapshot in one epoch");
            } else {
                ensure!(state.snapshot, "L4 update before snapshot");
                ensure!(
                    batch.height > state.height && batch.event_time_us >= state.time,
                    "conflicting or out-of-order L4 update"
                );
            }
            let sequence = state
                .sequence
                .checked_add(1)
                .context("L4 sequence overflow")?;
            let count = batch.events.len().max(1).div_ceil(options.chunk_events);
            for chunk_index in 0..count {
                let start = (chunk_index * options.chunk_events).min(batch.events.len());
                let end = (start + options.chunk_events).min(batch.events.len());
                let header = Header {
                    epoch,
                    sequence,
                    height: batch.height,
                    previous_height: state.height,
                    event_time_us: batch.event_time_us,
                    receipt_time_us,
                    chunk_index: chunk_index as u32,
                    chunk_count: count as u32,
                    kind: batch.kind,
                    coin: batch.coin,
                };
                sink.publish(&header, &batch.events[start..end])?;
            }
            stats.frames += 1;
            stats.events += batch.events.len() as u64;
            stats.chunks += count as u64;
            if batch.kind == Kind::Snapshot {
                log::info!(
                    "L4 snapshot ready coin={} height={} orders={}",
                    batch.coin.as_str(),
                    batch.height,
                    batch.events.len()
                );
            }
            state.sequence = sequence;
            state.height = batch.height;
            state.time = batch.event_time_us;
            state.digest = digest;
            state.snapshot = true;
        }
    }
    Ok(false)
}

fn reset<S: Sink>(sink: &mut S, coins: &[Coin], epoch: [u8; 16]) -> Result<()> {
    let receipt_time_us = now_us();
    for &coin in coins {
        sink.publish(
            &Header {
                epoch,
                sequence: 0,
                height: 0,
                previous_height: 0,
                event_time_us: 0,
                receipt_time_us,
                chunk_index: 0,
                chunk_count: 1,
                kind: Kind::Reset,
                coin,
            },
            &[],
        )?;
    }
    Ok(())
}

/// One ordered source per collector: no per-order tasks, locks or intermediate queues.
/// Failover never splices two nodes' histories; consumers first receive Reset + Snapshot.
pub async fn run<S: Sink>(
    options: Options,
    mut sink: S,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let coins = options.validate()?;
    let mut decoder = Decoder::new(options.reserve_events, options.max_events)?;
    let mut attempt = 0_u64;
    loop {
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return Ok(());
        }
        let mut epoch = [0; 16];
        getrandom::fill(&mut epoch).map_err(|e| anyhow::anyhow!("L4 epoch entropy: {e}"))?;
        reset(&mut sink, &coins, epoch)?;
        let endpoint = if attempt % 2 == 1 {
            options
                .fallback_ws_url
                .as_deref()
                .unwrap_or(&options.ws_url)
        } else {
            &options.ws_url
        };
        let outcome = session(
            &options,
            &coins,
            endpoint,
            epoch,
            &mut decoder,
            &mut sink,
            &mut shutdown,
        )
        .await;
        // Invalidate immediately, including on graceful shutdown and partial publication.
        reset(&mut sink, &coins, epoch)?;
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return Ok(());
        }
        if let Err(error) = outcome {
            log::warn!("Hyperliquid L4 session ended: {error:#}");
        }
        attempt = attempt.wrapping_add(1);
        tokio::select! {changed=shutdown.changed()=>{if changed.is_err()||*shutdown.borrow(){return Ok(());}},_=tokio::time::sleep(Duration::from_secs(3))=>{}}
    }
}
async fn session<S: Sink>(
    options: &Options,
    coins: &[Coin],
    endpoint: &str,
    epoch: [u8; 16],
    decoder: &mut Decoder,
    sink: &mut S,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(options.max_frame_bytes))
        .max_frame_size(Some(options.max_frame_bytes));
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_with_config(endpoint, Some(config), true),
    )
    .await
    .context("L4 connect timeout")??;
    let mut progress: AHashMap<Coin, Progress> = coins
        .iter()
        .map(|&coin| (coin, Progress::default()))
        .collect();
    for coin in coins {
        // Coin is validated ASCII without quotes/backslashes; cold-path only.
        let request = format!(
            "{{\"method\":\"subscribe\",\"subscription\":{{\"type\":\"l4Book\",\"coin\":\"{}\"}}}}",
            coin.as_str()
        );
        socket.send(Message::Text(request.into())).await?;
    }
    let started = Instant::now();
    let mut last_pong = Instant::now();
    let mut ping_at = None;
    let mut health = tokio::time::interval(Duration::from_secs(1));
    health.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut next_ping = Instant::now() + Duration::from_secs(30);
    let mut next_stats = Instant::now() + Duration::from_secs(30);
    let mut stats = Stats::default();
    loop {
        tokio::select! {
            changed=shutdown.changed()=>{
                if changed.is_err()||*shutdown.borrow() {
                    let _=socket.close(None).await;
                    return Ok(());
                }
            },
            _=health.tick()=>{
                ensure!(started.elapsed()<Duration::from_secs(5)||progress.values().all(|p|p.ack),"L4 subscription ACK timeout");
                ensure!(started.elapsed()<Duration::from_secs(30)||progress.values().all(|p|p.snapshot),"L4 initial snapshot timeout");
                if started.elapsed()>=Duration::from_secs(options.resnapshot_secs) {
                    let _=socket.close(None).await;
                    return Ok(());
                }
                if let Some(sent)=ping_at {
                    ensure!(last_pong>=sent||sent.elapsed()<Duration::from_secs(10),"L4 heartbeat timeout");
                }
                if Instant::now()>=next_ping {
                    socket.send(Message::Ping(Vec::new().into())).await?;
                    ping_at=Some(Instant::now());next_ping=Instant::now()+Duration::from_secs(30);
                }
                if Instant::now()>=next_stats {
                    log::info!("L4 session frames={} events={} chunks={} duplicates={}",stats.frames,stats.events,stats.chunks,stats.duplicates);
                    next_stats=Instant::now()+Duration::from_secs(30);
                }
            },
            frame=socket.next()=>{
                let frame=frame.context("L4 source closed")??;
                let raw=match frame {
                    Message::Text(ref text)=>text.as_bytes(),
                    Message::Binary(ref bytes)=>bytes.as_ref(),
                    Message::Ping(payload)=>{
                        socket.send(Message::Pong(payload)).await?;
                        continue;
                    },
                    Message::Pong(_)=>{
                        last_pong=Instant::now();
                        continue;
                    },
                    Message::Close(_)=>bail!("L4 source closed"),
                    _=>continue,
                };
                if publish_frame(options,epoch,raw,decoder,&mut progress,sink,&mut stats)? {last_pong=Instant::now();}
            }
        }
    }
}
