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

const SYMBOL_PREFIX_BYTES: usize = 128;
const HISTORY_SIZE: usize = 100;
const SUBSCRIBER_MAX_BUFFER: usize = 8192;

pub const DEFAULT_SPREAD_SERVICE_ROOT: &str = "spread_pbs";
pub const TEST_SPREAD_SERVICE_ROOT: &str = "spread_pbs_test";
pub const DEFAULT_DAT_SERVICE_ROOT: &str = "dat_pbs";
pub const TEST_DAT_SERVICE_ROOT: &str = "dat_pbs_test";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpreadPbsPublishRoots {
    spread_root: String,
    dat_root: String,
}

impl SpreadPbsPublishRoots {
    pub fn production() -> Self {
        Self {
            spread_root: DEFAULT_SPREAD_SERVICE_ROOT.to_string(),
            dat_root: DEFAULT_DAT_SERVICE_ROOT.to_string(),
        }
    }

    pub fn test() -> Self {
        Self {
            spread_root: TEST_SPREAD_SERVICE_ROOT.to_string(),
            dat_root: TEST_DAT_SERVICE_ROOT.to_string(),
        }
    }

    pub fn new(spread_root: impl Into<String>, dat_root: impl Into<String>) -> Result<Self> {
        Ok(Self {
            spread_root: clean_service_root(spread_root.into())?,
            dat_root: clean_service_root(dat_root.into())?,
        })
    }

    pub fn spread_root(&self) -> &str {
        &self.spread_root
    }

    pub fn dat_root(&self) -> &str {
        &self.dat_root
    }
}

impl Default for SpreadPbsPublishRoots {
    fn default() -> Self {
        Self::production()
    }
}

fn clean_service_root(root: String) -> Result<String> {
    let root = root.trim().trim_matches('/').to_string();
    anyhow::ensure!(!root.is_empty(), "iceoryx service root cannot be empty");
    anyhow::ensure!(
        !root.contains('/'),
        "iceoryx service root must be a single path component: {}",
        root
    );
    Ok(root)
}

fn service_name(root: &str, venue_slug: &str, channel: &str) -> Result<String> {
    let root = clean_service_root(root.to_string())?;
    Ok(format!("{}/{}/{}", root, venue_slug, channel))
}

fn sanitize_node_component(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn publisher_node_name(
    default_root: &str,
    default_prefix: &str,
    root: &str,
    venue_slug: &str,
    suffix: &str,
) -> Result<String> {
    let root = clean_service_root(root.to_string())?;
    let prefix = if root == default_root {
        default_prefix.to_string()
    } else {
        sanitize_node_component(&root)
    };
    let venue = venue_slug.replace('-', "_");
    if suffix.is_empty() {
        Ok(format!("{}_{}", prefix, venue))
    } else {
        Ok(format!("{}_{}_{}", prefix, venue, suffix))
    }
}

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

fn ensure_bbo_prefix_at_index(
    cache: &mut FastHashMap<String, BboPayloadPrefix>,
    by_index: &mut Vec<Option<BboPayloadPrefix>>,
    symbol: &str,
    index: usize,
) -> Result<()> {
    if index < by_index.len() && by_index[index].is_some() {
        return Ok(());
    }

    let prefix = if let Some(prefix) = cache.get(symbol) {
        prefix.clone()
    } else {
        let prefix = BboPayloadPrefix::new(symbol)?;
        cache.insert(symbol.to_string(), prefix.clone());
        prefix
    };

    if by_index.len() <= index {
        by_index.resize_with(index + 1, || None);
    }
    by_index[index] = Some(prefix);
    Ok(())
}

fn seed_bbo_prefixes(
    cache: &mut FastHashMap<String, BboPayloadPrefix>,
    by_index: &mut Vec<Option<BboPayloadPrefix>>,
    symbols: &[String],
) -> Result<()> {
    for symbol in symbols {
        if !cache.contains_key(symbol.as_str()) {
            let index = by_index.len();
            ensure_bbo_prefix_at_index(cache, by_index, symbol, index)?;
        }
    }
    Ok(())
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

#[derive(Clone)]
struct TradePayloadPrefix {
    bytes: [u8; TRADE_PAYLOAD_BYTES],
    len: usize,
    total_len: usize,
}

impl TradePayloadPrefix {
    fn new(symbol: &str) -> Result<Self> {
        let len = 4 + 4 + symbol.len();
        let total_len = trade_payload_len(symbol);
        anyhow::ensure!(
            total_len <= TRADE_PAYLOAD_BYTES,
            "trade payload {} exceeds {}",
            total_len,
            TRADE_PAYLOAD_BYTES
        );
        let mut bytes = [0u8; TRADE_PAYLOAD_BYTES];
        let mut off = 0usize;
        write_u32_le(&mut bytes, &mut off, MktMsgType::TradeInfo as u32);
        write_symbol(&mut bytes, &mut off, symbol)?;
        Ok(Self {
            bytes,
            len,
            total_len,
        })
    }
}

fn write_trade_payload_with_prefix(
    buf: &mut [u8],
    prefix: &TradePayloadPrefix,
    id: i64,
    timestamp_us: i64,
    side: char,
    price: f64,
    amount: f64,
) -> usize {
    buf[..prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
    let mut off = prefix.len;
    write_i64_le(buf, &mut off, id);
    write_i64_le(buf, &mut off, timestamp_us);
    buf[off] = side as u8;
    off += 8;
    write_f64_le(buf, &mut off, price);
    write_f64_le(buf, &mut off, amount);
    off
}

fn ensure_trade_prefix_at_index(
    cache: &mut FastHashMap<String, TradePayloadPrefix>,
    by_index: &mut Vec<Option<TradePayloadPrefix>>,
    symbol: &str,
    index: usize,
) -> Result<()> {
    if index < by_index.len() && by_index[index].is_some() {
        return Ok(());
    }
    let prefix = if let Some(prefix) = cache.get(symbol) {
        prefix.clone()
    } else {
        let prefix = TradePayloadPrefix::new(symbol)?;
        cache.insert(symbol.to_string(), prefix.clone());
        prefix
    };

    if by_index.len() <= index {
        by_index.resize_with(index + 1, || None);
    }
    by_index[index] = Some(prefix);
    Ok(())
}

fn seed_trade_prefixes(
    cache: &mut FastHashMap<String, TradePayloadPrefix>,
    by_index: &mut Vec<Option<TradePayloadPrefix>>,
    symbols: &[String],
) -> Result<()> {
    for symbol in symbols {
        if !cache.contains_key(symbol.as_str()) {
            let index = by_index.len();
            ensure_trade_prefix_at_index(cache, by_index, symbol, index)?;
        }
    }
    Ok(())
}

#[inline]
fn incremental_payload_len(symbol: &str, bids_count: usize, asks_count: usize) -> usize {
    4 + 4 + symbol.len() + 8 + 8 + 8 + 1 + 7 + 4 + 4 + (bids_count + asks_count) * 16
}

#[derive(Clone)]
struct IncrementalPayloadPrefix {
    bytes: [u8; SYMBOL_PREFIX_BYTES],
    len: usize,
    fixed_len: usize,
}

impl IncrementalPayloadPrefix {
    fn new(symbol: &str) -> Result<Self> {
        let len = 4 + 4 + symbol.len();
        let fixed_len = incremental_payload_len(symbol, 0, 0);
        anyhow::ensure!(
            len <= SYMBOL_PREFIX_BYTES,
            "incremental prefix {} exceeds {}",
            len,
            SYMBOL_PREFIX_BYTES
        );
        anyhow::ensure!(
            fixed_len <= INCREMENTAL_PAYLOAD_BYTES,
            "incremental payload {} exceeds {}",
            fixed_len,
            INCREMENTAL_PAYLOAD_BYTES
        );
        let mut bytes = [0u8; SYMBOL_PREFIX_BYTES];
        let mut off = 0usize;
        write_u32_le(&mut bytes, &mut off, MktMsgType::OrderBookInc as u32);
        write_symbol(&mut bytes, &mut off, symbol)?;
        Ok(Self {
            bytes,
            len,
            fixed_len,
        })
    }

    #[inline]
    fn total_len(&self, bids_count: usize, asks_count: usize) -> usize {
        self.fixed_len + (bids_count + asks_count) * 16
    }
}

fn ensure_incremental_prefix_at_index(
    cache: &mut FastHashMap<String, IncrementalPayloadPrefix>,
    by_index: &mut Vec<Option<IncrementalPayloadPrefix>>,
    symbol: &str,
    index: usize,
) -> Result<()> {
    if index < by_index.len() && by_index[index].is_some() {
        return Ok(());
    }
    let prefix = if let Some(prefix) = cache.get(symbol) {
        prefix.clone()
    } else {
        let prefix = IncrementalPayloadPrefix::new(symbol)?;
        cache.insert(symbol.to_string(), prefix.clone());
        prefix
    };

    if by_index.len() <= index {
        by_index.resize_with(index + 1, || None);
    }
    by_index[index] = Some(prefix);
    Ok(())
}

fn seed_incremental_prefixes(
    cache: &mut FastHashMap<String, IncrementalPayloadPrefix>,
    by_index: &mut Vec<Option<IncrementalPayloadPrefix>>,
    symbols: &[String],
) -> Result<()> {
    for symbol in symbols {
        if !cache.contains_key(symbol.as_str()) {
            let index = by_index.len();
            ensure_incremental_prefix_at_index(cache, by_index, symbol, index)?;
        }
    }
    Ok(())
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

#[allow(clippy::too_many_arguments)]
fn write_incremental_header_with_prefix(
    buf: &mut [u8],
    prefix: &IncrementalPayloadPrefix,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids_count: usize,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> usize {
    buf[..prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
    let mut off = prefix.len;
    write_i64_le(buf, &mut off, first_update_id);
    write_i64_le(buf, &mut off, final_update_id);
    write_i64_le(buf, &mut off, timestamp);
    buf[off] = u8::from(is_snapshot);
    buf[off + 1] = u8::from(chunk_idx == total_chunks - 1);
    buf[off + 2] = chunk_idx as u8;
    off += 8;
    write_u32_le(buf, &mut off, bids_count as u32);
    write_u32_le(buf, &mut off, asks_count as u32);
    off
}

#[allow(clippy::too_many_arguments)]
fn write_incremental_payload_from_levels_with_prefix<B: PayloadLevel, A: PayloadLevel>(
    buf: &mut [u8],
    prefix: &IncrementalPayloadPrefix,
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
) -> usize {
    let mut off = write_incremental_header_with_prefix(
        buf,
        prefix,
        first_update_id,
        final_update_id,
        timestamp,
        is_snapshot,
        bids_count,
        asks_count,
        chunk_idx,
        total_chunks,
    );
    for level in &bids[bids_start..bids_start + bids_count] {
        write_f64_le(buf, &mut off, level.price());
        write_f64_le(buf, &mut off, level.amount());
    }
    for level in &asks[asks_start..asks_start + asks_count] {
        write_f64_le(buf, &mut off, level.price());
        write_f64_le(buf, &mut off, level.amount());
    }
    off
}

#[allow(clippy::too_many_arguments)]
fn write_incremental_payload_from_iter<B, A>(
    buf: &mut [u8],
    symbol: &str,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids: B,
    bids_count: usize,
    asks: A,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> Result<usize>
where
    B: IntoIterator<Item = mkt_parsers::binance::Level>,
    A: IntoIterator<Item = mkt_parsers::binance::Level>,
{
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
    for level in bids {
        write_f64_le(buf, &mut off, level.price);
        write_f64_le(buf, &mut off, level.amount);
    }
    for level in asks {
        write_f64_le(buf, &mut off, level.price);
        write_f64_le(buf, &mut off, level.amount);
    }
    Ok(off)
}

#[allow(clippy::too_many_arguments)]
fn write_incremental_payload_from_iter_with_prefix<B, A>(
    buf: &mut [u8],
    prefix: &IncrementalPayloadPrefix,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids: B,
    bids_count: usize,
    asks: A,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> usize
where
    B: IntoIterator<Item = mkt_parsers::binance::Level>,
    A: IntoIterator<Item = mkt_parsers::binance::Level>,
{
    let mut off = write_incremental_header_with_prefix(
        buf,
        prefix,
        first_update_id,
        final_update_id,
        timestamp,
        is_snapshot,
        bids_count,
        asks_count,
        chunk_idx,
        total_chunks,
    );
    for level in bids {
        write_f64_le(buf, &mut off, level.price);
        write_f64_le(buf, &mut off, level.amount);
    }
    for level in asks {
        write_f64_le(buf, &mut off, level.price);
        write_f64_le(buf, &mut off, level.amount);
    }
    off
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

#[derive(Clone)]
struct DerivativePayloadPrefix {
    bytes: [u8; SYMBOL_PREFIX_BYTES],
    len: usize,
    total_len: usize,
}

impl DerivativePayloadPrefix {
    fn new(msg_type: MktMsgType, symbol: &str, total_len: usize) -> Result<Self> {
        let len = 4 + 4 + symbol.len();
        anyhow::ensure!(
            len <= SYMBOL_PREFIX_BYTES,
            "derivatives prefix {} exceeds {}",
            len,
            SYMBOL_PREFIX_BYTES
        );
        anyhow::ensure!(
            total_len <= DERIVATIVES_PAYLOAD_BYTES,
            "derivatives payload {} exceeds {}",
            total_len,
            DERIVATIVES_PAYLOAD_BYTES
        );
        let mut bytes = [0u8; SYMBOL_PREFIX_BYTES];
        let mut off = 0usize;
        write_u32_le(&mut bytes, &mut off, msg_type as u32);
        write_symbol(&mut bytes, &mut off, symbol)?;
        Ok(Self {
            bytes,
            len,
            total_len,
        })
    }
}

#[derive(Clone)]
struct DerivativesPayloadPrefixes {
    mark: DerivativePayloadPrefix,
    index: DerivativePayloadPrefix,
    funding: DerivativePayloadPrefix,
    liquidation: DerivativePayloadPrefix,
}

impl DerivativesPayloadPrefixes {
    fn new(symbol: &str) -> Result<Self> {
        Ok(Self {
            mark: DerivativePayloadPrefix::new(
                MktMsgType::MarkPrice,
                symbol,
                mark_price_payload_len(symbol),
            )?,
            index: DerivativePayloadPrefix::new(
                MktMsgType::IndexPrice,
                symbol,
                mark_price_payload_len(symbol),
            )?,
            funding: DerivativePayloadPrefix::new(
                MktMsgType::FundingRate,
                symbol,
                funding_rate_payload_len(symbol),
            )?,
            liquidation: DerivativePayloadPrefix::new(
                MktMsgType::LiquidationOrder,
                symbol,
                liquidation_payload_len(symbol),
            )?,
        })
    }
}

fn ensure_derivatives_prefix_at_index(
    cache: &mut FastHashMap<String, DerivativesPayloadPrefixes>,
    by_index: &mut Vec<Option<DerivativesPayloadPrefixes>>,
    symbol: &str,
    index: usize,
) -> Result<()> {
    if index < by_index.len() && by_index[index].is_some() {
        return Ok(());
    }
    let prefix = if let Some(prefix) = cache.get(symbol) {
        prefix.clone()
    } else {
        let prefix = DerivativesPayloadPrefixes::new(symbol)?;
        cache.insert(symbol.to_string(), prefix.clone());
        prefix
    };

    if by_index.len() <= index {
        by_index.resize_with(index + 1, || None);
    }
    by_index[index] = Some(prefix);
    Ok(())
}

fn seed_derivatives_prefixes(
    cache: &mut FastHashMap<String, DerivativesPayloadPrefixes>,
    by_index: &mut Vec<Option<DerivativesPayloadPrefixes>>,
    symbols: &[String],
) -> Result<()> {
    for symbol in symbols {
        if !cache.contains_key(symbol.as_str()) {
            let index = by_index.len();
            ensure_derivatives_prefix_at_index(cache, by_index, symbol, index)?;
        }
    }
    Ok(())
}

fn derivatives_prefixes_for_slot<'a>(
    cache: &mut FastHashMap<String, DerivativesPayloadPrefixes>,
    by_index: &'a mut Vec<Option<DerivativesPayloadPrefixes>>,
    symbol: &str,
    index: usize,
) -> Result<&'a DerivativesPayloadPrefixes> {
    ensure_derivatives_prefix_at_index(cache, by_index, symbol, index)?;
    Ok(by_index[index].as_ref().expect("prefix inserted"))
}

fn write_price_payload_with_prefix(
    buf: &mut [u8],
    prefix: &DerivativePayloadPrefix,
    price: f64,
    timestamp: i64,
) -> usize {
    buf[..prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
    let mut off = prefix.len;
    write_f64_le(buf, &mut off, price);
    write_i64_le(buf, &mut off, timestamp);
    off
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

fn write_funding_rate_payload_with_prefix(
    buf: &mut [u8],
    prefix: &DerivativePayloadPrefix,
    funding_rate: f64,
    next_funding_time: i64,
    timestamp: i64,
) -> usize {
    buf[..prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
    let mut off = prefix.len;
    write_f64_le(buf, &mut off, funding_rate);
    write_i64_le(buf, &mut off, next_funding_time);
    write_i64_le(buf, &mut off, timestamp);
    off
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

fn write_liquidation_payload_with_prefix(
    buf: &mut [u8],
    prefix: &DerivativePayloadPrefix,
    side: char,
    amount: f64,
    price: f64,
    timestamp: i64,
) -> usize {
    buf[..prefix.len].copy_from_slice(&prefix.bytes[..prefix.len]);
    let mut off = prefix.len;
    buf[off] = side as u8;
    off += 1;
    write_f64_le(buf, &mut off, amount);
    write_f64_le(buf, &mut off, price);
    write_i64_le(buf, &mut off, timestamp);
    off
}

/// `spread_pbs/<venue>/ask_bid_spread` 服务的 publisher 包装。
///
/// `max_subscribers = 64` 与 `max_publishers = 1` 与 plan 约定一致，
/// 与 dat_pbs 的同名 channel 完全独立。
pub struct SpreadPublisher {
    publisher: Publisher<ipc::Service, [u8; SPREAD_PAYLOAD_BYTES], ()>,
    service_name: String,
    bbo_prefix_by_symbol: RefCell<FastHashMap<String, BboPayloadPrefix>>,
    bbo_prefix_by_index: RefCell<Vec<Option<BboPayloadPrefix>>>,
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
    trade_prefix_by_symbol: RefCell<FastHashMap<String, TradePayloadPrefix>>,
    trade_prefix_by_index: RefCell<Vec<Option<TradePayloadPrefix>>>,
}

/// spread_pbs 直接替代旧 `dat_pbs/<venue>/incremental` 的 publisher。
///
/// 与 trade replacement 一样 open_or_create；同名活跃 publisher 由 max_publishers 限制。
pub struct SpreadIncrementalPublisher {
    publisher: Publisher<ipc::Service, [u8; INCREMENTAL_PAYLOAD_BYTES], ()>,
    service_name: String,
    incremental_prefix_by_symbol: RefCell<FastHashMap<String, IncrementalPayloadPrefix>>,
    incremental_prefix_by_index: RefCell<Vec<Option<IncrementalPayloadPrefix>>>,
}

/// spread_pbs 直接替代旧 `dat_pbs/<venue>/derivatives` 的 publisher。
pub struct SpreadDerivativesPublisher {
    publisher: Publisher<ipc::Service, [u8; DERIVATIVES_PAYLOAD_BYTES], ()>,
    service_name: String,
    derivatives_prefix_by_symbol: RefCell<FastHashMap<String, DerivativesPayloadPrefixes>>,
    derivatives_prefix_by_index: RefCell<Vec<Option<DerivativesPayloadPrefixes>>>,
}

impl SpreadPublisher {
    /// `venue_slug` 直接使用 `data_pub_slug()`（如 `okex-futures`）。
    pub fn new(venue_slug: &str) -> Result<Self> {
        Self::new_with_root(venue_slug, DEFAULT_SPREAD_SERVICE_ROOT)
    }

    /// Same BBO payload, but published under a caller-selected service root.
    pub fn new_with_root(venue_slug: &str, service_root: &str) -> Result<Self> {
        let service_name = service_name(service_root, venue_slug, "ask_bid_spread")?;
        let node_name = publisher_node_name(
            DEFAULT_SPREAD_SERVICE_ROOT,
            "spread_pbs",
            service_root,
            venue_slug,
            "",
        )?;

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
            bbo_prefix_by_index: RefCell::new(Vec::new()),
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn seed_symbols(&self, symbols: &[String]) -> Result<()> {
        let mut cache = self.bbo_prefix_by_symbol.borrow_mut();
        let mut by_index = self.bbo_prefix_by_index.borrow_mut();
        seed_bbo_prefixes(&mut cache, &mut by_index, symbols)
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

    #[allow(clippy::too_many_arguments)]
    pub fn publish_bbo_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        timestamp_us: i64,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
    ) -> Result<()> {
        let cache = self.bbo_prefix_by_index.borrow();
        if let Some(Some(prefix)) = cache.get(slot_index) {
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
        let mut by_index = self.bbo_prefix_by_index.borrow_mut();
        ensure_bbo_prefix_at_index(&mut cache, &mut by_index, symbol, slot_index)?;
        let prefix = by_index[slot_index].as_ref().expect("prefix inserted");
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
        Self::new_with_root(venue_slug, DEFAULT_SPREAD_SERVICE_ROOT)
    }

    pub fn new_with_root(venue_slug: &str, service_root: &str) -> Result<Self> {
        let service_name = service_name(service_root, venue_slug, "latency")?;
        let node_name = publisher_node_name(
            DEFAULT_SPREAD_SERVICE_ROOT,
            "spread_pbs",
            service_root,
            venue_slug,
            "latency",
        )?;

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
        Self::new_open_or_create_with_root(venue_slug, DEFAULT_DAT_SERVICE_ROOT)
    }

    pub fn new_open_or_create_with_root(venue_slug: &str, service_root: &str) -> Result<Self> {
        let service_name = service_name(service_root, venue_slug, "trade")?;
        let node_name = publisher_node_name(
            DEFAULT_DAT_SERVICE_ROOT,
            "spread_pbs",
            service_root,
            venue_slug,
            "trade",
        )?;

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
            trade_prefix_by_symbol: RefCell::new(fast_hash_map()),
            trade_prefix_by_index: RefCell::new(Vec::new()),
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "trade")
    }

    pub fn seed_symbols(&self, symbols: &[String]) -> Result<()> {
        let mut cache = self.trade_prefix_by_symbol.borrow_mut();
        let mut by_index = self.trade_prefix_by_index.borrow_mut();
        seed_trade_prefixes(&mut cache, &mut by_index, symbols)
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

    #[allow(clippy::too_many_arguments)]
    pub fn publish_trade_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        id: i64,
        timestamp_us: i64,
        side: char,
        price: f64,
        amount: f64,
    ) -> Result<()> {
        let cache = self.trade_prefix_by_index.borrow();
        if let Some(Some(prefix)) = cache.get(slot_index) {
            return publish_write(&self.publisher, prefix.total_len, "trade", |buf| {
                Ok(write_trade_payload_with_prefix(
                    buf,
                    prefix,
                    id,
                    timestamp_us,
                    side,
                    price,
                    amount,
                ))
            });
        }
        drop(cache);

        let mut by_symbol = self.trade_prefix_by_symbol.borrow_mut();
        let mut by_index = self.trade_prefix_by_index.borrow_mut();
        ensure_trade_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefix = by_index[slot_index].as_ref().expect("prefix inserted");
        publish_write(&self.publisher, prefix.total_len, "trade", |buf| {
            Ok(write_trade_payload_with_prefix(
                buf,
                prefix,
                id,
                timestamp_us,
                side,
                price,
                amount,
            ))
        })
    }
}

impl SpreadIncrementalPublisher {
    pub fn new_open_or_create(venue_slug: &str) -> Result<Self> {
        Self::new_open_or_create_with_root(venue_slug, DEFAULT_DAT_SERVICE_ROOT)
    }

    pub fn new_open_or_create_with_root(venue_slug: &str, service_root: &str) -> Result<Self> {
        let service_name = service_name(service_root, venue_slug, "incremental")?;
        let node_name = publisher_node_name(
            DEFAULT_DAT_SERVICE_ROOT,
            "spread_pbs",
            service_root,
            venue_slug,
            "incremental",
        )?;

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
            incremental_prefix_by_symbol: RefCell::new(fast_hash_map()),
            incremental_prefix_by_index: RefCell::new(Vec::new()),
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "incremental")
    }

    pub fn seed_symbols(&self, symbols: &[String]) -> Result<()> {
        let mut cache = self.incremental_prefix_by_symbol.borrow_mut();
        let mut by_index = self.incremental_prefix_by_index.borrow_mut();
        seed_incremental_prefixes(&mut cache, &mut by_index, symbols)
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

    #[allow(clippy::too_many_arguments)]
    pub fn publish_chunk_for_slot<B: PayloadLevel, A: PayloadLevel>(
        &self,
        slot_index: usize,
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
        let cache = self.incremental_prefix_by_index.borrow();
        if let Some(Some(prefix)) = cache.get(slot_index) {
            let min_len = prefix.total_len(bids_count, asks_count);
            return publish_write(&self.publisher, min_len, "incremental", |buf| {
                Ok(write_incremental_payload_from_levels_with_prefix(
                    buf,
                    prefix,
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
                ))
            });
        }
        drop(cache);

        let mut by_symbol = self.incremental_prefix_by_symbol.borrow_mut();
        let mut by_index = self.incremental_prefix_by_index.borrow_mut();
        ensure_incremental_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefix = by_index[slot_index].as_ref().expect("prefix inserted");
        let min_len = prefix.total_len(bids_count, asks_count);
        publish_write(&self.publisher, min_len, "incremental", |buf| {
            Ok(write_incremental_payload_from_levels_with_prefix(
                buf,
                prefix,
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
            ))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_chunk_from_iter<B, A>(
        &self,
        symbol: &str,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        is_snapshot: bool,
        bids: B,
        bids_count: usize,
        asks: A,
        asks_count: usize,
        chunk_idx: usize,
        total_chunks: usize,
    ) -> Result<()>
    where
        B: IntoIterator<Item = mkt_parsers::binance::Level>,
        A: IntoIterator<Item = mkt_parsers::binance::Level>,
    {
        let min_len = incremental_payload_len(symbol, bids_count, asks_count);
        publish_write(&self.publisher, min_len, "incremental", |buf| {
            write_incremental_payload_from_iter(
                buf,
                symbol,
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                bids,
                bids_count,
                asks,
                asks_count,
                chunk_idx,
                total_chunks,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_chunk_from_iter_for_slot<B, A>(
        &self,
        slot_index: usize,
        symbol: &str,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        is_snapshot: bool,
        bids: B,
        bids_count: usize,
        asks: A,
        asks_count: usize,
        chunk_idx: usize,
        total_chunks: usize,
    ) -> Result<()>
    where
        B: IntoIterator<Item = mkt_parsers::binance::Level>,
        A: IntoIterator<Item = mkt_parsers::binance::Level>,
    {
        let cache = self.incremental_prefix_by_index.borrow();
        if let Some(Some(prefix)) = cache.get(slot_index) {
            let min_len = prefix.total_len(bids_count, asks_count);
            return publish_write(&self.publisher, min_len, "incremental", |buf| {
                Ok(write_incremental_payload_from_iter_with_prefix(
                    buf,
                    prefix,
                    first_update_id,
                    final_update_id,
                    timestamp,
                    is_snapshot,
                    bids,
                    bids_count,
                    asks,
                    asks_count,
                    chunk_idx,
                    total_chunks,
                ))
            });
        }
        drop(cache);

        let mut by_symbol = self.incremental_prefix_by_symbol.borrow_mut();
        let mut by_index = self.incremental_prefix_by_index.borrow_mut();
        ensure_incremental_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefix = by_index[slot_index].as_ref().expect("prefix inserted");
        let min_len = prefix.total_len(bids_count, asks_count);
        publish_write(&self.publisher, min_len, "incremental", |buf| {
            Ok(write_incremental_payload_from_iter_with_prefix(
                buf,
                prefix,
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                bids,
                bids_count,
                asks,
                asks_count,
                chunk_idx,
                total_chunks,
            ))
        })
    }
}

impl SpreadDerivativesPublisher {
    pub fn new_open_or_create(venue_slug: &str) -> Result<Self> {
        Self::new_open_or_create_with_root(venue_slug, DEFAULT_DAT_SERVICE_ROOT)
    }

    pub fn new_open_or_create_with_root(venue_slug: &str, service_root: &str) -> Result<Self> {
        let service_name = service_name(service_root, venue_slug, "derivatives")?;
        let node_name = publisher_node_name(
            DEFAULT_DAT_SERVICE_ROOT,
            "spread_pbs",
            service_root,
            venue_slug,
            "derivatives",
        )?;

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
            derivatives_prefix_by_symbol: RefCell::new(fast_hash_map()),
            derivatives_prefix_by_index: RefCell::new(Vec::new()),
        })
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn publish(&self, data: &[u8]) -> Result<()> {
        publish_padded(&self.publisher, data, "derivatives")
    }

    pub fn seed_symbols(&self, symbols: &[String]) -> Result<()> {
        let mut cache = self.derivatives_prefix_by_symbol.borrow_mut();
        let mut by_index = self.derivatives_prefix_by_index.borrow_mut();
        seed_derivatives_prefixes(&mut cache, &mut by_index, symbols)
    }

    pub fn publish_mark_price(&self, symbol: &str, price: f64, timestamp: i64) -> Result<()> {
        let min_len = mark_price_payload_len(symbol);
        publish_write(&self.publisher, min_len, "derivatives", |buf| {
            write_price_payload(buf, MktMsgType::MarkPrice, symbol, price, timestamp)
        })
    }

    pub fn publish_mark_price_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        price: f64,
        timestamp: i64,
    ) -> Result<()> {
        let cache = self.derivatives_prefix_by_index.borrow();
        if let Some(Some(prefixes)) = cache.get(slot_index) {
            return publish_write(
                &self.publisher,
                prefixes.mark.total_len,
                "derivatives",
                |buf| {
                    Ok(write_price_payload_with_prefix(
                        buf,
                        &prefixes.mark,
                        price,
                        timestamp,
                    ))
                },
            );
        }
        drop(cache);

        let mut by_symbol = self.derivatives_prefix_by_symbol.borrow_mut();
        let mut by_index = self.derivatives_prefix_by_index.borrow_mut();
        ensure_derivatives_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefixes = by_index[slot_index].as_ref().expect("prefix inserted");
        publish_write(
            &self.publisher,
            prefixes.mark.total_len,
            "derivatives",
            |buf| {
                Ok(write_price_payload_with_prefix(
                    buf,
                    &prefixes.mark,
                    price,
                    timestamp,
                ))
            },
        )
    }

    pub fn publish_index_price(&self, symbol: &str, price: f64, timestamp: i64) -> Result<()> {
        let min_len = mark_price_payload_len(symbol);
        publish_write(&self.publisher, min_len, "derivatives", |buf| {
            write_price_payload(buf, MktMsgType::IndexPrice, symbol, price, timestamp)
        })
    }

    pub fn publish_index_price_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        price: f64,
        timestamp: i64,
    ) -> Result<()> {
        let cache = self.derivatives_prefix_by_index.borrow();
        if let Some(Some(prefixes)) = cache.get(slot_index) {
            return publish_write(
                &self.publisher,
                prefixes.index.total_len,
                "derivatives",
                |buf| {
                    Ok(write_price_payload_with_prefix(
                        buf,
                        &prefixes.index,
                        price,
                        timestamp,
                    ))
                },
            );
        }
        drop(cache);

        let mut by_symbol = self.derivatives_prefix_by_symbol.borrow_mut();
        let mut by_index = self.derivatives_prefix_by_index.borrow_mut();
        ensure_derivatives_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefixes = by_index[slot_index].as_ref().expect("prefix inserted");
        publish_write(
            &self.publisher,
            prefixes.index.total_len,
            "derivatives",
            |buf| {
                Ok(write_price_payload_with_prefix(
                    buf,
                    &prefixes.index,
                    price,
                    timestamp,
                ))
            },
        )
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

    pub fn publish_funding_rate_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        funding_rate: f64,
        next_funding_time: i64,
        timestamp: i64,
    ) -> Result<()> {
        let cache = self.derivatives_prefix_by_index.borrow();
        if let Some(Some(prefixes)) = cache.get(slot_index) {
            return publish_write(
                &self.publisher,
                prefixes.funding.total_len,
                "derivatives",
                |buf| {
                    Ok(write_funding_rate_payload_with_prefix(
                        buf,
                        &prefixes.funding,
                        funding_rate,
                        next_funding_time,
                        timestamp,
                    ))
                },
            );
        }
        drop(cache);

        let mut by_symbol = self.derivatives_prefix_by_symbol.borrow_mut();
        let mut by_index = self.derivatives_prefix_by_index.borrow_mut();
        ensure_derivatives_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefixes = by_index[slot_index].as_ref().expect("prefix inserted");
        publish_write(
            &self.publisher,
            prefixes.funding.total_len,
            "derivatives",
            |buf| {
                Ok(write_funding_rate_payload_with_prefix(
                    buf,
                    &prefixes.funding,
                    funding_rate,
                    next_funding_time,
                    timestamp,
                ))
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_mark_price_bundle_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        mark_price: Option<f64>,
        index_price: Option<f64>,
        funding_rate: Option<f64>,
        next_funding_time: Option<i64>,
        timestamp: i64,
    ) -> Result<usize> {
        let mut count = 0usize;
        let cache = self.derivatives_prefix_by_index.borrow();
        if let Some(Some(prefixes)) = cache.get(slot_index) {
            if let Some(price) = mark_price {
                publish_write(
                    &self.publisher,
                    prefixes.mark.total_len,
                    "derivatives",
                    |buf| {
                        Ok(write_price_payload_with_prefix(
                            buf,
                            &prefixes.mark,
                            price,
                            timestamp,
                        ))
                    },
                )?;
                count += 1;
            }
            if let Some(price) = index_price {
                publish_write(
                    &self.publisher,
                    prefixes.index.total_len,
                    "derivatives",
                    |buf| {
                        Ok(write_price_payload_with_prefix(
                            buf,
                            &prefixes.index,
                            price,
                            timestamp,
                        ))
                    },
                )?;
                count += 1;
            }
            if let (Some(rate), Some(next_time)) = (funding_rate, next_funding_time) {
                publish_write(
                    &self.publisher,
                    prefixes.funding.total_len,
                    "derivatives",
                    |buf| {
                        Ok(write_funding_rate_payload_with_prefix(
                            buf,
                            &prefixes.funding,
                            rate,
                            next_time,
                            timestamp,
                        ))
                    },
                )?;
                count += 1;
            }
            return Ok(count);
        }
        drop(cache);

        let mut by_symbol = self.derivatives_prefix_by_symbol.borrow_mut();
        let mut by_index = self.derivatives_prefix_by_index.borrow_mut();
        let prefixes =
            derivatives_prefixes_for_slot(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        if let Some(price) = mark_price {
            publish_write(
                &self.publisher,
                prefixes.mark.total_len,
                "derivatives",
                |buf| {
                    Ok(write_price_payload_with_prefix(
                        buf,
                        &prefixes.mark,
                        price,
                        timestamp,
                    ))
                },
            )?;
            count += 1;
        }
        if let Some(price) = index_price {
            publish_write(
                &self.publisher,
                prefixes.index.total_len,
                "derivatives",
                |buf| {
                    Ok(write_price_payload_with_prefix(
                        buf,
                        &prefixes.index,
                        price,
                        timestamp,
                    ))
                },
            )?;
            count += 1;
        }
        if let (Some(rate), Some(next_time)) = (funding_rate, next_funding_time) {
            publish_write(
                &self.publisher,
                prefixes.funding.total_len,
                "derivatives",
                |buf| {
                    Ok(write_funding_rate_payload_with_prefix(
                        buf,
                        &prefixes.funding,
                        rate,
                        next_time,
                        timestamp,
                    ))
                },
            )?;
            count += 1;
        }
        Ok(count)
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

    pub fn publish_liquidation_for_slot(
        &self,
        slot_index: usize,
        symbol: &str,
        side: char,
        amount: f64,
        price: f64,
        timestamp: i64,
    ) -> Result<()> {
        let cache = self.derivatives_prefix_by_index.borrow();
        if let Some(Some(prefixes)) = cache.get(slot_index) {
            return publish_write(
                &self.publisher,
                prefixes.liquidation.total_len,
                "derivatives",
                |buf| {
                    Ok(write_liquidation_payload_with_prefix(
                        buf,
                        &prefixes.liquidation,
                        side,
                        amount,
                        price,
                        timestamp,
                    ))
                },
            );
        }
        drop(cache);

        let mut by_symbol = self.derivatives_prefix_by_symbol.borrow_mut();
        let mut by_index = self.derivatives_prefix_by_index.borrow_mut();
        ensure_derivatives_prefix_at_index(&mut by_symbol, &mut by_index, symbol, slot_index)?;
        let prefixes = by_index[slot_index].as_ref().expect("prefix inserted");
        publish_write(
            &self.publisher,
            prefixes.liquidation.total_len,
            "derivatives",
            |buf| {
                Ok(write_liquidation_payload_with_prefix(
                    buf,
                    &prefixes.liquidation,
                    side,
                    amount,
                    price,
                    timestamp,
                ))
            },
        )
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
    fn cached_trade_prefix_writer_matches_trade_msg_bytes() {
        let expected = TradeMsg::create(
            "ETHUSDT".to_string(),
            9001,
            1_700_000_000_123_456,
            'S',
            2000.5,
            0.75,
        )
        .to_bytes();
        let prefix = TradePayloadPrefix::new("ETHUSDT").unwrap();
        let mut buf = [0u8; TRADE_PAYLOAD_BYTES];
        let written = write_trade_payload_with_prefix(
            &mut buf,
            &prefix,
            9001,
            1_700_000_000_123_456,
            'S',
            2000.5,
            0.75,
        );
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
        assert!(buf[written..].iter().all(|b| *b == 0));
    }

    #[test]
    fn bbo_prefix_seed_is_idempotent() {
        let mut cache = fast_hash_map();
        let mut by_index = Vec::new();
        let symbols = ["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        seed_bbo_prefixes(&mut cache, &mut by_index, &symbols).unwrap();
        seed_bbo_prefixes(&mut cache, &mut by_index, &symbols).unwrap();

        assert_eq!(cache.len(), 2);
        assert_eq!(by_index.len(), 2);
        assert_eq!(cache.get("BTCUSDT").unwrap().len, 15);
        assert_eq!(cache.get("ETHUSDT").unwrap().len, 15);
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
    fn cached_incremental_prefix_writer_matches_inc_msg_bytes() {
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

        let prefix = IncrementalPayloadPrefix::new("BTCUSDT").unwrap();
        let mut buf = [0u8; INCREMENTAL_PAYLOAD_BYTES];
        let written = write_incremental_payload_from_levels_with_prefix(
            &mut buf, &prefix, 10, 11, 123_456, false, &bids, 1, 1, &asks, 0, 2, 2, 3,
        );
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
        assert!(buf[written..].iter().all(|b| *b == 0));
    }

    #[test]
    fn direct_incremental_iter_writer_matches_inc_msg_bytes() {
        let bids = [
            mkt_parsers::binance::Level {
                price: 100.0,
                amount: 1.0,
            },
            mkt_parsers::binance::Level {
                price: 99.5,
                amount: 2.0,
            },
        ];
        let asks = [
            mkt_parsers::binance::Level {
                price: 101.0,
                amount: 3.0,
            },
            mkt_parsers::binance::Level {
                price: 101.5,
                amount: 4.0,
            },
        ];
        let mut msg = IncMsg::create("BTCUSDT".to_string(), 10, 11, 123_456, false, 1, 2);
        msg.set_chunk_index(2);
        msg.set_is_last(true);
        msg.set_bid_level(0, Level::from_values(99.5, 2.0));
        msg.set_ask_level(0, Level::from_values(101.0, 3.0));
        msg.set_ask_level(1, Level::from_values(101.5, 4.0));
        let expected = msg.to_bytes();

        let mut buf = [0u8; INCREMENTAL_PAYLOAD_BYTES];
        let written = write_incremental_payload_from_iter(
            &mut buf,
            "BTCUSDT",
            10,
            11,
            123_456,
            false,
            bids.into_iter().skip(1).take(1),
            1,
            asks.into_iter().take(2),
            2,
            2,
            3,
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

    #[test]
    fn cached_derivatives_prefix_writers_match_msg_bytes() {
        let mut buf = [0u8; DERIVATIVES_PAYLOAD_BYTES];
        let prefixes = DerivativesPayloadPrefixes::new("BTCUSDT").unwrap();

        let expected =
            MarkPriceMsg::create("BTCUSDT".to_string(), 25.0, 1_700_000_000_001_000).to_bytes();
        let written =
            write_price_payload_with_prefix(&mut buf, &prefixes.mark, 25.0, 1_700_000_000_001_000);
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);

        let expected =
            IndexPriceMsg::create("BTCUSDT".to_string(), 24.9, 1_700_000_000_001_000).to_bytes();
        let written =
            write_price_payload_with_prefix(&mut buf, &prefixes.index, 24.9, 1_700_000_000_001_000);
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);

        let expected = FundingRateMsg::create(
            "BTCUSDT".to_string(),
            0.0001,
            1_700_003_600_000_000,
            1_700_000_000_001_000,
        )
        .to_bytes();
        let written = write_funding_rate_payload_with_prefix(
            &mut buf,
            &prefixes.funding,
            0.0001,
            1_700_003_600_000_000,
            1_700_000_000_001_000,
        );
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
        let written = write_liquidation_payload_with_prefix(
            &mut buf,
            &prefixes.liquidation,
            'S',
            10.0,
            25.2,
            1_700_000_000_000_000,
        );
        assert_eq!(written, expected.len());
        assert_eq!(&buf[..written], &expected[..]);
    }
}
