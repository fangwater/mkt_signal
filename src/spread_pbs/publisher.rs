use anyhow::Result;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use std::cell::RefCell;

use mkt_parsers::msg::mkt_msg::{Level, MktMsgType};
use rolling_common::latency_snapshot::LATENCY_SNAPSHOT_PAYLOAD_LEN;

/// AskBidSpreadMsg wire format 实测占用：4B msg_type + 4B symbol_len + N(symbol)
/// + 8B ts + 4×8B = 至多 ~80 字节。预留到 128 与 dat_pbs 对齐，便于
/// 未来扩展且和 forwarder.rs 的 SPREAD_MAX_BYTES 一致。
pub const SPREAD_PAYLOAD_BYTES: usize = 128;
pub const TRADE_PAYLOAD_BYTES: usize = 128;
pub const DERIVATIVES_PAYLOAD_BYTES: usize = 128;
pub const INCREMENTAL_PAYLOAD_BYTES: usize = 2048;

const HISTORY_SIZE: usize = 100;
const SUBSCRIBER_MAX_BUFFER: usize = 8192;

fn publish_padded<const N: usize>(
    publisher: &Publisher<ipc::Service, [u8; N], ()>,
    data: &[u8],
    kind: &str,
) -> Result<()> {
    anyhow::ensure!(
        data.len() <= N,
        "{} payload {} exceeds {}",
        kind,
        data.len(),
        N
    );

    let mut sample = publisher.loan_uninit()?;
    sample.payload_mut().write([0u8; N]);
    let mut sample = unsafe { sample.assume_init() };
    sample.payload_mut()[..data.len()].copy_from_slice(data);
    sample.send()?;
    Ok(())
}

#[inline]
fn write_u32_le(buf: &mut [u8], offset: &mut usize, value: u32) {
    buf[*offset..*offset + 4].copy_from_slice(&value.to_le_bytes());
    *offset += 4;
}

#[inline]
fn write_i64_le(buf: &mut [u8], offset: &mut usize, value: i64) {
    buf[*offset..*offset + 8].copy_from_slice(&value.to_le_bytes());
    *offset += 8;
}

#[inline]
fn write_f64_le(buf: &mut [u8], offset: &mut usize, value: f64) {
    buf[*offset..*offset + 8].copy_from_slice(&value.to_le_bytes());
    *offset += 8;
}

pub trait PayloadLevel {
    fn price(&self) -> f64;
    fn amount(&self) -> f64;
}

impl PayloadLevel for Level {
    fn price(&self) -> f64 {
        self.price
    }

    fn amount(&self) -> f64 {
        self.amount
    }
}

impl PayloadLevel for mkt_parsers::binance::Level {
    fn price(&self) -> f64 {
        self.price
    }

    fn amount(&self) -> f64 {
        self.amount
    }
}

#[inline]
fn write_symbol(buf: &mut [u8], offset: &mut usize, symbol: &str) -> Result<()> {
    anyhow::ensure!(
        symbol.len() <= u32::MAX as usize,
        "symbol too long: {} bytes",
        symbol.len()
    );
    write_u32_le(buf, offset, symbol.len() as u32);
    let end = *offset + symbol.len();
    buf[*offset..end].copy_from_slice(symbol.as_bytes());
    *offset = end;
    Ok(())
}

fn publish_write<const N: usize>(
    publisher: &Publisher<ipc::Service, [u8; N], ()>,
    min_len: usize,
    kind: &str,
    write: impl FnOnce(&mut [u8]) -> Result<usize>,
) -> Result<()> {
    anyhow::ensure!(min_len <= N, "{} payload {} exceeds {}", kind, min_len, N);

    let mut sample = publisher.loan_uninit()?;
    sample.payload_mut().write([0u8; N]);
    let mut sample = unsafe { sample.assume_init() };
    let written = write(sample.payload_mut())?;
    anyhow::ensure!(written <= N, "{} payload {} exceeds {}", kind, written, N);
    sample.send()?;
    Ok(())
}

#[inline]
fn bbo_payload_len(symbol: &str) -> usize {
    4 + 4 + symbol.len() + 8 + 32
}

#[cfg(test)]
fn write_bbo_payload(
    buf: &mut [u8],
    symbol: &str,
    timestamp_us: i64,
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
) -> Result<usize> {
    let mut off = 0usize;
    write_u32_le(buf, &mut off, MktMsgType::AskBidSpread as u32);
    write_symbol(buf, &mut off, symbol)?;
    write_i64_le(buf, &mut off, timestamp_us);
    write_f64_le(buf, &mut off, bid_price);
    write_f64_le(buf, &mut off, bid_amount);
    write_f64_le(buf, &mut off, ask_price);
    write_f64_le(buf, &mut off, ask_amount);
    Ok(off)
}

#[derive(Clone)]
struct BboPayloadPrefix {
    bytes: [u8; SPREAD_PAYLOAD_BYTES],
    len: usize,
    total_len: usize,
}

impl BboPayloadPrefix {
    fn new(symbol: &str) -> Result<Self> {
        let len = 4 + 4 + symbol.len();
        let total_len = bbo_payload_len(symbol);
        anyhow::ensure!(
            total_len <= SPREAD_PAYLOAD_BYTES,
            "spread payload {} exceeds {}",
            total_len,
            SPREAD_PAYLOAD_BYTES
        );
        let mut bytes = [0u8; SPREAD_PAYLOAD_BYTES];
        let mut off = 0usize;
        write_u32_le(&mut bytes, &mut off, MktMsgType::AskBidSpread as u32);
        write_symbol(&mut bytes, &mut off, symbol)?;
        Ok(Self {
            bytes,
            len,
            total_len,
        })
    }
}

fn write_bbo_payload_with_prefix(
    buf: &mut [u8],
    prefix: &BboPayloadPrefix,
    timestamp_us: i64,
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
) -> usize {
    buf[..prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
    let mut off = prefix.len;
    write_i64_le(buf, &mut off, timestamp_us);
    write_f64_le(buf, &mut off, bid_price);
    write_f64_le(buf, &mut off, bid_amount);
    write_f64_le(buf, &mut off, ask_price);
    write_f64_le(buf, &mut off, ask_amount);
    off
}

#[inline]
fn trade_payload_len(symbol: &str) -> usize {
    4 + 4 + symbol.len() + 8 + 8 + 1 + 7 + 8 + 8
}

fn write_trade_payload(
    buf: &mut [u8],
    symbol: &str,
    id: i64,
    timestamp_us: i64,
    side: char,
    price: f64,
    amount: f64,
) -> Result<usize> {
    let mut off = 0usize;
    write_u32_le(buf, &mut off, MktMsgType::TradeInfo as u32);
    write_symbol(buf, &mut off, symbol)?;
    write_i64_le(buf, &mut off, id);
    write_i64_le(buf, &mut off, timestamp_us);
    buf[off] = side as u8;
    off += 8;
    write_f64_le(buf, &mut off, price);
    write_f64_le(buf, &mut off, amount);
    Ok(off)
}

#[inline]
fn incremental_payload_len(symbol: &str, bids_count: usize, asks_count: usize) -> usize {
    4 + 4 + symbol.len() + 8 + 8 + 8 + 1 + 7 + 4 + 4 + (bids_count + asks_count) * 16
}

#[inline]
fn mark_price_payload_len(symbol: &str) -> usize {
    4 + 4 + symbol.len() + 8 + 8
}

#[inline]
fn funding_rate_payload_len(symbol: &str) -> usize {
    4 + 4 + symbol.len() + 8 + 8 + 8
}

#[inline]
fn liquidation_payload_len(symbol: &str) -> usize {
    4 + 4 + symbol.len() + 1 + 8 + 8 + 8
}

#[allow(clippy::too_many_arguments)]
fn write_incremental_payload(
    buf: &mut [u8],
    symbol: &str,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids: &[Level],
    bids_start: usize,
    bids_count: usize,
    asks: &[Level],
    asks_start: usize,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> Result<usize> {
    write_incremental_payload_from_levels(
        buf,
        symbol,
        first_update_id,
        final_update_id,
        timestamp,
        is_snapshot,
        bids,
        bids_start,
        bids_count,
        asks,
        asks_start,
        asks_count,
        chunk_idx,
        total_chunks,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_incremental_payload_from_levels<B: PayloadLevel, A: PayloadLevel>(
    buf: &mut [u8],
    symbol: &str,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids: &[B],
    bids_start: usize,
    bids_count: usize,
    asks: &[A],
    asks_start: usize,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> Result<usize> {
    let mut off = 0usize;
    write_u32_le(buf, &mut off, MktMsgType::OrderBookInc as u32);
    write_symbol(buf, &mut off, symbol)?;
    write_i64_le(buf, &mut off, first_update_id);
    write_i64_le(buf, &mut off, final_update_id);
    write_i64_le(buf, &mut off, timestamp);
    buf[off] = u8::from(is_snapshot);
    buf[off + 1] = u8::from(chunk_idx == total_chunks - 1);
    buf[off + 2] = chunk_idx as u8;
    off += 8;
    write_u32_le(buf, &mut off, bids_count as u32);
    write_u32_le(buf, &mut off, asks_count as u32);
    for level in &bids[bids_start..bids_start + bids_count] {
        write_f64_le(buf, &mut off, level.price());
        write_f64_le(buf, &mut off, level.amount());
    }
    for level in &asks[asks_start..asks_start + asks_count] {
        write_f64_le(buf, &mut off, level.price());
        write_f64_le(buf, &mut off, level.amount());
    }
    Ok(off)
}

fn write_price_payload(
    buf: &mut [u8],
    msg_type: MktMsgType,
    symbol: &str,
    price: f64,
    timestamp: i64,
) -> Result<usize> {
    let mut off = 0usize;
    write_u32_le(buf, &mut off, msg_type as u32);
    write_symbol(buf, &mut off, symbol)?;
    write_f64_le(buf, &mut off, price);
    write_i64_le(buf, &mut off, timestamp);
    Ok(off)
}

fn write_funding_rate_payload(
    buf: &mut [u8],
    symbol: &str,
    funding_rate: f64,
    next_funding_time: i64,
    timestamp: i64,
) -> Result<usize> {
    let mut off = 0usize;
    write_u32_le(buf, &mut off, MktMsgType::FundingRate as u32);
    write_symbol(buf, &mut off, symbol)?;
    write_f64_le(buf, &mut off, funding_rate);
    write_i64_le(buf, &mut off, next_funding_time);
    write_i64_le(buf, &mut off, timestamp);
    Ok(off)
}

fn write_liquidation_payload(
    buf: &mut [u8],
    symbol: &str,
    side: char,
    amount: f64,
    price: f64,
    timestamp: i64,
) -> Result<usize> {
    let mut off = 0usize;
    write_u32_le(buf, &mut off, MktMsgType::LiquidationOrder as u32);
    write_symbol(buf, &mut off, symbol)?;
    buf[off] = side as u8;
    off += 1;
    write_f64_le(buf, &mut off, amount);
    write_f64_le(buf, &mut off, price);
    write_i64_le(buf, &mut off, timestamp);
    Ok(off)
}

/// `spread_pbs/<venue>/ask_bid_spread` 服务的 publisher 包装。
///
/// `max_subscribers = 64` 与 `max_publishers = 1` 与 plan 约定一致，
/// 与 dat_pbs 的同名 channel 完全独立。
pub struct SpreadPublisher {
    publisher: Publisher<ipc::Service, [u8; SPREAD_PAYLOAD_BYTES], ()>,
    service_name: String,
    bbo_prefix_by_symbol: RefCell<FastHashMap<String, BboPayloadPrefix>>,
}

/// `spread_pbs/<venue>/latency` 服务的 publisher。这个 service 不经过
/// `IPC_NAMESPACE`，因为 spread_pbs 在单机上按 venue 唯一部署。
pub struct SpreadLatencyPublisher {
    publisher: Publisher<ipc::Service, [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN], ()>,
    service_name: String,
}

/// spread_pbs 直接替代旧 `dat_pbs/<venue>/trade` 的 publisher。
///
/// 使用 open_or_create，允许进程重启/中途替换复用已存在 service；`max_publishers=1`
/// 仍然避免两个活跃 publisher 同时写同一通道。
pub struct SpreadTradePublisher {
    publisher: Publisher<ipc::Service, [u8; TRADE_PAYLOAD_BYTES], ()>,
    service_name: String,
}

/// spread_pbs 直接替代旧 `dat_pbs/<venue>/incremental` 的 publisher。
///
/// 与 trade replacement 一样 open_or_create；同名活跃 publisher 由 max_publishers 限制。
pub struct SpreadIncrementalPublisher {
    publisher: Publisher<ipc::Service, [u8; INCREMENTAL_PAYLOAD_BYTES], ()>,
    service_name: String,
}

/// spread_pbs 直接替代旧 `dat_pbs/<venue>/derivatives` 的 publisher。
pub struct SpreadDerivativesPublisher {
    publisher: Publisher<ipc::Service, [u8; DERIVATIVES_PAYLOAD_BYTES], ()>,
    service_name: String,
}

impl SpreadPublisher {
    /// `venue_slug` 直接使用 `data_pub_slug()`（如 `okex-futures`）。
    pub fn new(venue_slug: &str) -> Result<Self> {
        let service_name = format!("spread_pbs/{}/ask_bid_spread", venue_slug);
        let node_name = format!("spread_pbs_{}", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SPREAD_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs publisher ready: service={} max_subscribers=64 payload={}B",
            service_name,
            SPREAD_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
            bbo_prefix_by_symbol: RefCell::new(fast_hash_map()),
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// 同步 publish。`data` 长度需 ≤ `SPREAD_PAYLOAD_BYTES`。
    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "spread")
    }

    pub fn publish_bbo(
        &self,
        symbol: &str,
        timestamp_us: i64,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
    ) -> Result<()> {
        let cache = self.bbo_prefix_by_symbol.borrow();
        if let Some(prefix) = cache.get(symbol) {
            return self.publish_bbo_with_prefix(
                prefix,
                timestamp_us,
                bid_price,
                bid_amount,
                ask_price,
                ask_amount,
            );
        }
        drop(cache);

        let mut cache = self.bbo_prefix_by_symbol.borrow_mut();
        if !cache.contains_key(symbol) {
            cache.insert(symbol.to_string(), BboPayloadPrefix::new(symbol)?);
        }
        let prefix = cache.get(symbol).expect("prefix inserted");
        self.publish_bbo_with_prefix(
            prefix,
            timestamp_us,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        )
    }

    fn publish_bbo_with_prefix(
        &self,
        prefix: &BboPayloadPrefix,
        timestamp_us: i64,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
    ) -> Result<()> {
        publish_write(&self.publisher, prefix.total_len, "spread", |buf| {
            Ok(write_bbo_payload_with_prefix(
                buf,
                prefix,
                timestamp_us,
                bid_price,
                bid_amount,
                ask_price,
                ask_amount,
            ))
        })
    }
}

impl SpreadLatencyPublisher {
    pub fn new(venue_slug: &str) -> Result<Self> {
        let service_name = format!("spread_pbs/{}/latency", venue_slug);
        let node_name = format!("spread_pbs_{}_latency", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs latency publisher ready: service={} max_subscribers=64 payload={}B",
            service_name,
            LATENCY_SNAPSHOT_PAYLOAD_LEN
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: [u8; LATENCY_SNAPSHOT_PAYLOAD_LEN]) -> Result<()> {
        self.publisher.send_copy(data)?;
        Ok(())
    }
}

impl SpreadTradePublisher {
    pub fn new_open_or_create(venue_slug: &str) -> Result<Self> {
        let service_name = format!("dat_pbs/{}/trade", venue_slug);
        let node_name = format!("spread_pbs_{}_trade", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(HISTORY_SIZE)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs trade publisher ready: service={} mode=open-or-create max_publishers=1 max_subscribers=64 payload={}B",
            service_name,
            TRADE_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "trade")
    }

    pub fn publish_trade(
        &self,
        symbol: &str,
        id: i64,
        timestamp_us: i64,
        side: char,
        price: f64,
        amount: f64,
    ) -> Result<()> {
        let min_len = trade_payload_len(symbol);
        publish_write(&self.publisher, min_len, "trade", |buf| {
            write_trade_payload(buf, symbol, id, timestamp_us, side, price, amount)
        })
    }
}

impl SpreadIncrementalPublisher {
    pub fn new_open_or_create(venue_slug: &str) -> Result<Self> {
        let service_name = format!("dat_pbs/{}/incremental", venue_slug);
        let node_name = format!("spread_pbs_{}_incremental", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; INCREMENTAL_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(100)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs incremental publisher ready: service={} mode=open-or-create max_publishers=1 max_subscribers=10 payload={}B",
            service_name,
            INCREMENTAL_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "incremental")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_chunk(
        &self,
        symbol: &str,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        is_snapshot: bool,
        bids: &[Level],
        bids_start: usize,
        bids_count: usize,
        asks: &[Level],
        asks_start: usize,
        asks_count: usize,
        chunk_idx: usize,
        total_chunks: usize,
    ) -> Result<()> {
        let min_len = incremental_payload_len(symbol, bids_count, asks_count);
        publish_write(&self.publisher, min_len, "incremental", |buf| {
            write_incremental_payload(
                buf,
                symbol,
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                bids,
                bids_start,
                bids_count,
                asks,
                asks_start,
                asks_count,
                chunk_idx,
                total_chunks,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_chunk_from_levels<B: PayloadLevel, A: PayloadLevel>(
        &self,
        symbol: &str,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        is_snapshot: bool,
        bids: &[B],
        bids_start: usize,
        bids_count: usize,
        asks: &[A],
        asks_start: usize,
        asks_count: usize,
        chunk_idx: usize,
        total_chunks: usize,
    ) -> Result<()> {
        let min_len = incremental_payload_len(symbol, bids_count, asks_count);
        publish_write(&self.publisher, min_len, "incremental", |buf| {
            write_incremental_payload_from_levels(
                buf,
                symbol,
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                bids,
                bids_start,
                bids_count,
                asks,
                asks_start,
                asks_count,
                chunk_idx,
                total_chunks,
            )
        })
    }
}

impl SpreadDerivativesPublisher {
    pub fn new_open_or_create(venue_slug: &str) -> Result<Self> {
        let service_name = format!("dat_pbs/{}/derivatives", venue_slug);
        let node_name = format!("spread_pbs_{}_derivatives", venue_slug.replace('-', "_"));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD_BYTES]>()
            .max_publishers(1)
            .max_subscribers(64)
            .history_size(50)
            .subscriber_max_buffer_size(SUBSCRIBER_MAX_BUFFER)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;

        log::info!(
            "spread_pbs derivatives publisher ready: service={} mode=open-or-create max_publishers=1 max_subscribers=64 payload={}B",
            service_name,
            DERIVATIVES_PAYLOAD_BYTES
        );
        Ok(Self {
            publisher,
            service_name,
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "derivatives")
    }

    pub fn publish_mark_price(&self, symbol: &str, price: f64, timestamp: i64) -> Result<()> {
        let min_len = mark_price_payload_len(symbol);
        publish_write(&self.publisher, min_len, "derivatives", |buf| {
            write_price_payload(buf, MktMsgType::MarkPrice, symbol, price, timestamp)
        })
    }

    pub fn publish_index_price(&self, symbol: &str, price: f64, timestamp: i64) -> Result<()> {
        let min_len = mark_price_payload_len(symbol);
        publish_write(&self.publisher, min_len, "derivatives", |buf| {
            write_price_payload(buf, MktMsgType::IndexPrice, symbol, price, timestamp)
        })
    }

    pub fn publish_funding_rate(
        &self,
        symbol: &str,
        funding_rate: f64,
        next_funding_time: i64,
        timestamp: i64,
    ) -> Result<()> {
        let min_len = funding_rate_payload_len(symbol);
        publish_write(&self.publisher, min_len, "derivatives", |buf| {
            write_funding_rate_payload(buf, symbol, funding_rate, next_funding_time, timestamp)
        })
    }

    pub fn publish_liquidation(
        &self,
        symbol: &str,
        side: char,
        amount: f64,
        price: f64,
        timestamp: i64,
    ) -> Result<()> {
        let min_len = liquidation_payload_len(symbol);
        publish_write(&self.publisher, min_len, "derivatives", |buf| {
            write_liquidation_payload(buf, symbol, side, amount, price, timestamp)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{
        AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, LiquidationMsg, MarkPriceMsg,
        TradeMsg,
    };

    #[test]
    fn direct_bbo_writer_matches_ask_bid_spread_msg_bytes() {
        let expected = AskBidSpreadMsg::create(
            "BTCUSDT".to_string(),
            1_700_000_000_123_456,
            100.1,
            2.3,
            100.2,
            3.4,
        )
        .to_bytes();
        let mut buf = [0u8; SPREAD_PAYLOAD_BYTES];
        let written = write_bbo_payload(
            &mut buf,
            "BTCUSDT",
            1_700_000_000_123_456,
            100.1,
            2.3,
            100.2,
            3.4,
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
        assert!(buf[written..].iter().all(|b| *b == 0));
    }

    #[test]
    fn cached_bbo_prefix_writer_matches_ask_bid_spread_msg_bytes() {
        let expected = AskBidSpreadMsg::create(
            "ETHUSDT".to_string(),
            1_700_000_000_654_321,
            2000.1,
            5.6,
            2000.2,
            7.8,
        )
        .to_bytes();
        let prefix = BboPayloadPrefix::new("ETHUSDT").unwrap();
        let mut buf = [0u8; SPREAD_PAYLOAD_BYTES];
        let written = write_bbo_payload_with_prefix(
            &mut buf,
            &prefix,
            1_700_000_000_654_321,
            2000.1,
            5.6,
            2000.2,
            7.8,
        );
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
        assert!(buf[written..].iter().all(|b| *b == 0));
    }

    #[test]
    fn direct_trade_writer_matches_trade_msg_bytes() {
        let expected = TradeMsg::create(
            "ETHUSDT".to_string(),
            9001,
            1_700_000_000_123_456,
            'S',
            2000.5,
            0.75,
        )
        .to_bytes();
        let mut buf = [0u8; TRADE_PAYLOAD_BYTES];
        let written = write_trade_payload(
            &mut buf,
            "ETHUSDT",
            9001,
            1_700_000_000_123_456,
            'S',
            2000.5,
            0.75,
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
        assert!(buf[written..].iter().all(|b| *b == 0));
    }

    #[test]
    fn direct_incremental_writer_matches_inc_msg_bytes() {
        let bids = vec![
            Level::from_values(100.0, 1.0),
            Level::from_values(99.5, 2.0),
        ];
        let asks = vec![
            Level::from_values(101.0, 3.0),
            Level::from_values(101.5, 4.0),
        ];
        let mut msg = IncMsg::create("BTCUSDT".to_string(), 10, 11, 123_456, false, 1, 2);
        msg.set_chunk_index(2);
        msg.set_is_last(true);
        msg.set_bid_level(0, bids[1]);
        msg.set_ask_level(0, asks[0]);
        msg.set_ask_level(1, asks[1]);
        let expected = msg.to_bytes();

        let mut buf = [0u8; INCREMENTAL_PAYLOAD_BYTES];
        let written = write_incremental_payload(
            &mut buf, "BTCUSDT", 10, 11, 123_456, false, &bids, 1, 1, &asks, 0, 2, 2, 3,
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
        assert!(buf[written..].iter().all(|b| *b == 0));
    }

    #[test]
    fn direct_derivatives_writers_match_msg_bytes() {
        let mut buf = [0u8; DERIVATIVES_PAYLOAD_BYTES];

        let expected =
            MarkPriceMsg::create("BTCUSDT".to_string(), 25.0, 1_700_000_000_001_000).to_bytes();
        let written = write_price_payload(
            &mut buf,
            MktMsgType::MarkPrice,
            "BTCUSDT",
            25.0,
            1_700_000_000_001_000,
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);

        let expected =
            IndexPriceMsg::create("BTCUSDT".to_string(), 24.9, 1_700_000_000_001_000).to_bytes();
        let written = write_price_payload(
            &mut buf,
            MktMsgType::IndexPrice,
            "BTCUSDT",
            24.9,
            1_700_000_000_001_000,
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);

        let expected = FundingRateMsg::create(
            "BTCUSDT".to_string(),
            0.0001,
            1_700_003_600_000_000,
            1_700_000_000_001_000,
        )
        .to_bytes();
        let written = write_funding_rate_payload(
            &mut buf,
            "BTCUSDT",
            0.0001,
            1_700_003_600_000_000,
            1_700_000_000_001_000,
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);

        let expected = LiquidationMsg::create(
            "BTCUSDT".to_string(),
            'S',
            10.0,
            25.2,
            1_700_000_000_000_000,
        )
        .to_bytes();
        let written =
            write_liquidation_payload(&mut buf, "BTCUSDT", 'S', 10.0, 25.2, 1_700_000_000_000_000)
                .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
    }
}
