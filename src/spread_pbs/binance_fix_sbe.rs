//! Binance Spot FIX market-data transport with SBE responses.
//!
//! Port 9001 accepts FIX 4.4 requests and returns FIX/SBE frames. This keeps
//! session setup and subscriptions simple while using the same SOFH + SBE
//! market-data hot path as port 9002.

use anyhow::{anyhow, bail, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use log::{debug, info, warn};
use openssl::pkey::{Id as PKeyId, PKey, Private};
use openssl::sign::Signer;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::sync::watch;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use uuid::Uuid;

use crate::spread_pbs::ws::{shared_rustls_config, FrameHandler};
use mkt_parsers::binance::Level;
use runtime_common::socket_tuning::{tune_tcp_stream, TcpSocketTuning, DEFAULT_WS_BUSY_POLL_US};
use runtime_common::time_util::get_timestamp_us;

pub const ENV_BINANCE_SPOT_TRANSPORT: &str = "SPREAD_PBS_BINANCE_SPOT_TRANSPORT";
pub const ENV_BINANCE_FIX_MD_URL: &str = "BINANCE_FIX_MD_URL";
pub const ENV_BINANCE_FIX_MD_API_KEY: &str = "BINANCE_FIX_MD_API_KEY";
pub const ENV_BINANCE_FIX_MD_PRIVATE_KEY_PATH: &str = "BINANCE_FIX_MD_PRIVATE_KEY_PATH";

const DEFAULT_FIX_MD_URL: &str = "tcp+tls://fix-md.binance.com:9001";
const TARGET_COMP_ID: &str = "SPOT";
const HEARTBEAT_SECS: u64 = 10;
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const SOH: char = '\x01';
const SOFH_LEN: usize = 6;
const SBE_HEADER_LEN: usize = 20;
const FIX_SBE_SCHEMA_ID: u16 = 1;
const FIX_SBE_SCHEMA_VERSION: u16 = 1;
const FIX_SBE_ENCODING_LE: u16 = 0xeb50;
const TEMPLATE_HEARTBEAT: u16 = 20001;
const TEMPLATE_TEST_REQUEST: u16 = 20002;
const TEMPLATE_REJECT: u16 = 20003;
const TEMPLATE_LOGOUT: u16 = 20004;
const TEMPLATE_LOGON_ACK: u16 = 20009;
const TEMPLATE_MD_REQUEST_REJECT: u16 = 203;
const TEMPLATE_NEWS: u16 = 20100;
pub(crate) const MAX_FIX_SBE_LEVELS: usize = 64;

type FixMdStream = TlsStream<TcpStream>;
type FixField = (u32, String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceSpotTransport {
    WsSbe,
    FixSbe,
}

impl BinanceSpotTransport {
    pub fn from_env() -> Result<Self> {
        let Some(raw) = std::env::var(ENV_BINANCE_SPOT_TRANSPORT)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
        else {
            return Ok(Self::WsSbe);
        };
        match raw.as_str() {
            "ws_sbe" | "ws-sbe" => Ok(Self::WsSbe),
            "fix_sbe" | "fix-sbe" => Ok(Self::FixSbe),
            _ => bail!(
                "invalid {}={:?}; expected ws_sbe or fix_sbe",
                ENV_BINANCE_SPOT_TRANSPORT,
                raw
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixMdStreamKind {
    Bbo,
    Trade,
    Depth,
}

impl FixMdStreamKind {
    fn label(self) -> &'static str {
        match self {
            Self::Bbo => "bbo",
            Self::Trade => "trade",
            Self::Depth => "depth",
        }
    }
}

pub struct FixMdLoopParams {
    pub label: String,
    pub local_ip: String,
    pub symbols: Vec<String>,
    pub kind: FixMdStreamKind,
    pub depth: u16,
}

#[derive(Clone, Copy, Debug)]
pub struct FixSbeHeader {
    pub block_length: usize,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
    pub seq_num: u32,
    pub sending_time_us: i64,
}

#[derive(Clone, Copy, Debug)]
pub enum FixSbeMarketEvent<'a> {
    Bbo {
        symbol: &'a str,
        timestamp_us: i64,
        seq_id: i64,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
    },
    Trade {
        symbol: &'a str,
        timestamp_us: i64,
        seq_id: i64,
        trade_id: i64,
        side: char,
        price: f64,
        amount: f64,
    },
    Book {
        symbol: &'a str,
        timestamp_us: i64,
        seq_id: i64,
        first_update_id: i64,
        final_update_id: i64,
        is_snapshot: bool,
        bids: &'a [Level],
        asks: &'a [Level],
    },
}

#[derive(Default)]
struct BboState {
    by_symbol: HashMap<String, BboValues>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BboValues {
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
    has_bid: bool,
    has_ask: bool,
}

struct FixMdCredentials {
    api_key: String,
    private_key: PKey<Private>,
}

pub async fn run_fix_sbe_md(
    params: FixMdLoopParams,
    handler: FrameHandler,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let credentials = match load_credentials() {
        Ok(credentials) => Rc::new(credentials),
        Err(err) => {
            log::error!(
                "spread_pbs fix-md[{}] credentials invalid: {err:#}",
                params.label
            );
            return;
        }
    };

    while !*shutdown_rx.borrow() {
        let session_result =
            run_fix_md_session(&params, credentials.as_ref(), &handler, &mut shutdown_rx).await;
        if *shutdown_rx.borrow() {
            break;
        }
        match session_result {
            Ok(()) => warn!("spread_pbs fix-md[{}] session ended", params.label),
            Err(err) => warn!(
                "spread_pbs fix-md[{}] session failed: {err:#}",
                params.label
            ),
        }
        tokio::select! {
            _ = shutdown_rx.changed() => {},
            _ = tokio::time::sleep(RECONNECT_DELAY) => {},
        }
    }
    info!("spread_pbs fix-md[{}] stopped", params.label);
}

async fn run_fix_md_session(
    params: &FixMdLoopParams,
    credentials: &FixMdCredentials,
    handler: &FrameHandler,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<()> {
    let url = std::env::var(ENV_BINANCE_FIX_MD_URL)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_FIX_MD_URL.to_string());
    let (host, port) = parse_endpoint(&url)?;
    let source_ip = parse_source_ip(&params.local_ip)?;
    let tcp = connect_tcp(&host, port, source_ip).await?;
    tune_tcp_stream(
        &tcp,
        "spread_pbs Binance FIX MD",
        TcpSocketTuning {
            busy_poll_us: Some(DEFAULT_WS_BUSY_POLL_US),
            ..TcpSocketTuning::default()
        },
    );
    let config = shared_rustls_config().context("build TLS config for Binance FIX MD")?;
    let connector = TlsConnector::from(config);
    let server_name = ServerName::try_from(host.clone())
        .with_context(|| format!("invalid TLS server name {host}"))?;
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .with_context(|| format!("TLS connect Binance FIX MD host={host}"))?;

    let sender_comp_id = generate_sender_comp_id();
    let mut next_seq_num = 1u32;
    let logon = build_logon(
        credentials,
        &sender_comp_id,
        next_seq_num,
        FIX_SBE_SCHEMA_ID,
        FIX_SBE_SCHEMA_VERSION,
    )?;
    next_seq_num += 1;
    stream.write_all(logon.as_bytes()).await?;

    let mut frame = Vec::with_capacity(64 * 1024);
    loop {
        read_sofh_frame(&mut stream, &mut frame).await?;
        let header = parse_sbe_header(&frame)?;
        match header.template_id {
            TEMPLATE_LOGON_ACK => break,
            TEMPLATE_REJECT | TEMPLATE_LOGOUT => {
                bail!(
                    "FIX MD logon rejected template_id={} seq={}",
                    header.template_id,
                    header.seq_num
                )
            }
            other => debug!("FIX MD pre-logon ignored template_id={other}"),
        }
    }
    info!(
        "spread_pbs fix-md[{}] logged on url={} source_ip={} sender_comp_id={} kind={} symbols={}",
        params.label,
        url,
        source_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "system-default".to_string()),
        sender_comp_id,
        params.kind.label(),
        params.symbols.len(),
    );

    let subscription = build_market_data_request(
        &sender_comp_id,
        next_seq_num,
        params.kind,
        params.depth,
        &params.symbols,
        &params.label,
    );
    next_seq_num += 1;
    stream.write_all(subscription.as_bytes()).await?;

    let mut heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    let logout = build_session_message(
                        "5",
                        &sender_comp_id,
                        next_seq_num,
                        &[(58, "client shutdown".to_string())],
                    );
                    let _ = stream.write_all(logout.as_bytes()).await;
                    return Ok(());
                }
            }
            _ = heartbeat.tick() => {
                let msg = build_session_message("0", &sender_comp_id, next_seq_num, &[]);
                next_seq_num += 1;
                stream.write_all(msg.as_bytes()).await?;
            }
            result = read_sofh_frame(&mut stream, &mut frame) => {
                result?;
                let recv_us = get_timestamp_us();
                let header = parse_sbe_header(&frame)?;
                match header.template_id {
                    TEMPLATE_HEARTBEAT => {},
                    TEMPLATE_TEST_REQUEST => {
                        let test_req_id = parse_control_var_string8(&frame, header.block_length)
                            .unwrap_or_default();
                        let fields = if test_req_id.is_empty() {
                            Vec::new()
                        } else {
                            vec![(112, test_req_id.to_string())]
                        };
                        let msg = build_session_message(
                            "0",
                            &sender_comp_id,
                            next_seq_num,
                            &fields,
                        );
                        next_seq_num += 1;
                        stream.write_all(msg.as_bytes()).await?;
                    }
                    TEMPLATE_MD_REQUEST_REJECT => {
                        warn!(
                            "spread_pbs fix-md[{}] subscription rejected seq={}",
                            params.label,
                            header.seq_num
                        );
                    }
                    TEMPLATE_REJECT => {
                        warn!(
                            "spread_pbs fix-md[{}] session reject seq={}",
                            params.label,
                            header.seq_num
                        );
                    }
                    TEMPLATE_LOGOUT => bail!("Binance FIX MD server logout"),
                    TEMPLATE_NEWS => bail!("Binance FIX MD maintenance news; reconnecting"),
                    _ => handler(recv_us, &frame),
                }
            }
        }
    }
}

pub(crate) fn decode_market_frame(
    frame: &[u8],
    kind: FixMdStreamKind,
    bbo_state: &mut HashMap<String, BboValues>,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'_>),
) -> Result<()> {
    let header = parse_sbe_header(frame)?;
    if header.schema_id != FIX_SBE_SCHEMA_ID {
        bail!("unexpected Binance FIX/SBE schema id {}", header.schema_id);
    }
    match (kind, header.template_id) {
        (FixMdStreamKind::Bbo, 204) => decode_snapshot_bbo(frame, header, bbo_state, emit),
        (FixMdStreamKind::Bbo, 206) => decode_incremental_bbo(frame, header, bbo_state, emit),
        (FixMdStreamKind::Trade, 205) => decode_trade(frame, header, emit),
        (FixMdStreamKind::Depth, 204) => decode_snapshot_depth(frame, header, emit),
        (FixMdStreamKind::Depth, 207) => decode_incremental_depth(frame, header, emit),
        _ => Ok(()),
    }
}

pub(crate) fn new_bbo_state() -> HashMap<String, BboValues> {
    BboState::default().by_symbol
}

fn decode_snapshot_bbo(
    frame: &[u8],
    header: FixSbeHeader,
    state: &mut HashMap<String, BboValues>,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'_>),
) -> Result<()> {
    let root = root_slice(frame, header)?;
    require_len(root, 10, "MarketDataSnapshot root")?;
    let seq_id = read_i64(root, 0)?;
    let price_exp = root[8] as i8;
    let qty_exp = root[9] as i8;
    let mut offset = SOFH_LEN + SBE_HEADER_LEN + header.block_length;
    let mut bids = [Level {
        price: 0.0,
        amount: 0.0,
    }; MAX_FIX_SBE_LEVELS];
    let mut asks = bids;
    let bid_count = read_level_group16(frame, &mut offset, price_exp, qty_exp, &mut bids)?;
    let ask_count = read_level_group16(frame, &mut offset, price_exp, qty_exp, &mut asks)?;
    let symbol = read_var_string8(frame, &mut offset)?;
    update_and_emit_bbo(
        state,
        symbol,
        header.sending_time_us,
        seq_id,
        bids.first().copied().filter(|_| bid_count > 0),
        asks.first().copied().filter(|_| ask_count > 0),
        emit,
    );
    Ok(())
}

fn decode_incremental_bbo(
    frame: &[u8],
    header: FixSbeHeader,
    state: &mut HashMap<String, BboValues>,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'_>),
) -> Result<()> {
    let root = root_slice(frame, header)?;
    require_len(root, 10, "MarketDataIncrementalBookTicker root")?;
    let seq_id = read_i64(root, 0)?;
    let price_exp = root[8] as i8;
    let qty_exp = root[9] as i8;
    let mut offset = SOFH_LEN + SBE_HEADER_LEN + header.block_length;
    let mut bids = [Level {
        price: 0.0,
        amount: 0.0,
    }; MAX_FIX_SBE_LEVELS];
    let mut asks = bids;
    let bid_count = read_level_group8(frame, &mut offset, price_exp, qty_exp, &mut bids)?;
    let ask_count = read_level_group8(frame, &mut offset, price_exp, qty_exp, &mut asks)?;
    let symbol = read_var_string8(frame, &mut offset)?;
    update_and_emit_bbo(
        state,
        symbol,
        header.sending_time_us,
        seq_id,
        bids.first().copied().filter(|_| bid_count > 0),
        asks.first().copied().filter(|_| ask_count > 0),
        emit,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_and_emit_bbo<'a>(
    state: &mut HashMap<String, BboValues>,
    symbol: &'a str,
    timestamp_us: i64,
    seq_id: i64,
    bid: Option<Level>,
    ask: Option<Level>,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'a>),
) {
    if !state.contains_key(symbol) {
        state.insert(symbol.to_owned(), BboValues::default());
    }
    let values = state
        .get_mut(symbol)
        .expect("BBO state was inserted for symbol");
    if let Some(bid) = bid {
        values.bid_price = bid.price;
        values.bid_amount = bid.amount;
        values.has_bid = bid.amount > 0.0;
    }
    if let Some(ask) = ask {
        values.ask_price = ask.price;
        values.ask_amount = ask.amount;
        values.has_ask = ask.amount > 0.0;
    }
    if values.has_bid && values.has_ask {
        emit(FixSbeMarketEvent::Bbo {
            symbol,
            timestamp_us,
            seq_id,
            bid_price: values.bid_price,
            bid_amount: values.bid_amount,
            ask_price: values.ask_price,
            ask_amount: values.ask_amount,
        });
    }
}

fn decode_trade(
    frame: &[u8],
    header: FixSbeHeader,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'_>),
) -> Result<()> {
    let root = root_slice(frame, header)?;
    require_len(root, 10, "MarketDataIncrementalTrade root")?;
    let timestamp_us = read_i64(root, 0)?;
    let price_exp = root[8] as i8;
    let qty_exp = root[9] as i8;
    let group_offset = SOFH_LEN + SBE_HEADER_LEN + header.block_length;
    require_range(frame, group_offset, 6, "trade group header")?;
    let block_len = read_u16(frame, group_offset)? as usize;
    let count = read_u32(frame, group_offset + 2)? as usize;
    let entries_offset = group_offset + 6;
    require_range(
        frame,
        entries_offset,
        block_len.saturating_mul(count),
        "trade entries",
    )?;
    let mut symbol_offset = entries_offset + block_len * count;
    let symbol = read_var_string8(frame, &mut symbol_offset)?;
    for idx in 0..count {
        let entry = entries_offset + idx * block_len;
        require_range(frame, entry, 25, "trade entry")?;
        let trade_id = read_i64(frame, entry)?;
        let price = decimal(read_i64(frame, entry + 8)?, price_exp);
        let amount = decimal(read_i64(frame, entry + 16)?, qty_exp);
        let side = match frame[entry + 24] {
            b'1' => 'B',
            b'2' => 'S',
            _ => '?',
        };
        emit(FixSbeMarketEvent::Trade {
            symbol,
            timestamp_us,
            seq_id: trade_id,
            trade_id,
            side,
            price,
            amount,
        });
    }
    Ok(())
}

fn decode_snapshot_depth(
    frame: &[u8],
    header: FixSbeHeader,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'_>),
) -> Result<()> {
    let root = root_slice(frame, header)?;
    require_len(root, 10, "MarketDataSnapshot root")?;
    let seq_id = read_i64(root, 0)?;
    let price_exp = root[8] as i8;
    let qty_exp = root[9] as i8;
    let mut offset = SOFH_LEN + SBE_HEADER_LEN + header.block_length;
    let mut bids = [Level {
        price: 0.0,
        amount: 0.0,
    }; MAX_FIX_SBE_LEVELS];
    let mut asks = bids;
    let bid_count = read_level_group16(frame, &mut offset, price_exp, qty_exp, &mut bids)?;
    let ask_count = read_level_group16(frame, &mut offset, price_exp, qty_exp, &mut asks)?;
    let symbol = read_var_string8(frame, &mut offset)?;
    emit(FixSbeMarketEvent::Book {
        symbol,
        timestamp_us: header.sending_time_us,
        seq_id,
        first_update_id: seq_id,
        final_update_id: seq_id,
        is_snapshot: true,
        bids: &bids[..bid_count],
        asks: &asks[..ask_count],
    });
    Ok(())
}

fn decode_incremental_depth(
    frame: &[u8],
    header: FixSbeHeader,
    emit: &mut dyn FnMut(FixSbeMarketEvent<'_>),
) -> Result<()> {
    let root = root_slice(frame, header)?;
    require_len(root, 18, "MarketDataIncrementalDepth root")?;
    let first_update_id = read_i64(root, 0)?;
    let final_update_id = read_i64(root, 8)?;
    let price_exp = root[16] as i8;
    let qty_exp = root[17] as i8;
    let mut offset = SOFH_LEN + SBE_HEADER_LEN + header.block_length;
    let mut bids = [Level {
        price: 0.0,
        amount: 0.0,
    }; MAX_FIX_SBE_LEVELS];
    let mut asks = bids;
    let bid_count = read_level_group16(frame, &mut offset, price_exp, qty_exp, &mut bids)?;
    let ask_count = read_level_group16(frame, &mut offset, price_exp, qty_exp, &mut asks)?;
    let symbol = read_var_string8(frame, &mut offset)?;
    emit(FixSbeMarketEvent::Book {
        symbol,
        timestamp_us: header.sending_time_us,
        seq_id: final_update_id,
        first_update_id,
        final_update_id,
        is_snapshot: false,
        bids: &bids[..bid_count],
        asks: &asks[..ask_count],
    });
    Ok(())
}

fn read_level_group16(
    frame: &[u8],
    offset: &mut usize,
    price_exp: i8,
    qty_exp: i8,
    out: &mut [Level; MAX_FIX_SBE_LEVELS],
) -> Result<usize> {
    require_range(frame, *offset, 3, "level group16 header")?;
    let block_len = frame[*offset] as usize;
    let count = read_u16(frame, *offset + 1)? as usize;
    *offset += 3;
    read_level_entries(frame, offset, block_len, count, price_exp, qty_exp, out)
}

fn read_level_group8(
    frame: &[u8],
    offset: &mut usize,
    price_exp: i8,
    qty_exp: i8,
    out: &mut [Level; MAX_FIX_SBE_LEVELS],
) -> Result<usize> {
    require_range(frame, *offset, 2, "level group8 header")?;
    let block_len = frame[*offset] as usize;
    let count = frame[*offset + 1] as usize;
    *offset += 2;
    read_level_entries(frame, offset, block_len, count, price_exp, qty_exp, out)
}

fn read_level_entries(
    frame: &[u8],
    offset: &mut usize,
    block_len: usize,
    count: usize,
    price_exp: i8,
    qty_exp: i8,
    out: &mut [Level; MAX_FIX_SBE_LEVELS],
) -> Result<usize> {
    if block_len < 16 {
        bail!(
            "FIX/SBE level block length {} is smaller than 16",
            block_len
        );
    }
    require_range(
        frame,
        *offset,
        block_len.saturating_mul(count),
        "level entries",
    )?;
    let kept = count.min(out.len());
    for (idx, level) in out.iter_mut().take(kept).enumerate() {
        let entry = *offset + idx * block_len;
        let price = decimal(read_i64(frame, entry)?, price_exp);
        let qty_raw = read_i64(frame, entry + 8)?;
        let amount = if qty_raw == i64::MIN {
            0.0
        } else {
            decimal(qty_raw, qty_exp)
        };
        *level = Level { price, amount };
    }
    *offset += block_len * count;
    Ok(kept)
}

fn decimal(mantissa: i64, exponent: i8) -> f64 {
    mantissa as f64 * 10f64.powi(exponent as i32)
}

fn parse_sbe_header(frame: &[u8]) -> Result<FixSbeHeader> {
    require_range(frame, 0, SOFH_LEN + SBE_HEADER_LEN, "SOFH + SBE header")?;
    let message_len = read_u32(frame, 0)? as usize;
    if message_len != frame.len() {
        bail!(
            "FIX/SBE SOFH length mismatch declared={} actual={}",
            message_len,
            frame.len()
        );
    }
    let encoding = read_u16(frame, 4)?;
    if encoding != FIX_SBE_ENCODING_LE {
        bail!("unsupported FIX/SBE SOFH encoding 0x{encoding:04x}");
    }
    Ok(FixSbeHeader {
        block_length: read_u16(frame, 6)? as usize,
        template_id: read_u16(frame, 8)?,
        schema_id: read_u16(frame, 10)?,
        version: read_u16(frame, 12)?,
        seq_num: read_u32(frame, 14)?,
        sending_time_us: read_i64(frame, 18)?,
    })
}

fn root_slice(frame: &[u8], header: FixSbeHeader) -> Result<&[u8]> {
    let start = SOFH_LEN + SBE_HEADER_LEN;
    require_range(frame, start, header.block_length, "SBE root block")?;
    Ok(&frame[start..start + header.block_length])
}

async fn read_sofh_frame(stream: &mut FixMdStream, frame: &mut Vec<u8>) -> Result<()> {
    frame.clear();
    frame.resize(SOFH_LEN, 0);
    stream
        .read_exact(frame)
        .await
        .context("read Binance FIX MD SOFH")?;
    let message_len = read_u32(frame, 0)? as usize;
    if message_len < SOFH_LEN + SBE_HEADER_LEN || message_len > 8 * 1024 * 1024 {
        bail!("invalid FIX/SBE SOFH message length {message_len}");
    }
    frame.resize(message_len, 0);
    stream
        .read_exact(&mut frame[SOFH_LEN..])
        .await
        .context("read Binance FIX MD SBE payload")?;
    Ok(())
}

fn parse_control_var_string8(frame: &[u8], block_length: usize) -> Option<&str> {
    let mut offset = SOFH_LEN + SBE_HEADER_LEN + block_length;
    read_var_string8(frame, &mut offset).ok()
}

fn build_logon(
    credentials: &FixMdCredentials,
    sender_comp_id: &str,
    seq_num: u32,
    schema_id: u16,
    schema_version: u16,
) -> Result<String> {
    let sending_time = current_fix_time();
    let payload =
        format!("A{SOH}{sender_comp_id}{SOH}{TARGET_COMP_ID}{SOH}{seq_num}{SOH}{sending_time}");
    let raw_data = sign_ed25519_base64(&credentials.private_key, payload.as_bytes())?;
    Ok(build_fix_message(&[
        (35, "A".to_string()),
        (34, seq_num.to_string()),
        (49, sender_comp_id.to_string()),
        (52, sending_time),
        (56, TARGET_COMP_ID.to_string()),
        (25000, "5000".to_string()),
        (95, raw_data.len().to_string()),
        (96, raw_data),
        (98, "0".to_string()),
        (108, HEARTBEAT_SECS.to_string()),
        (141, "Y".to_string()),
        (553, credentials.api_key.clone()),
        (25035, "2".to_string()),
        (25050, schema_id.to_string()),
        (25051, schema_version.to_string()),
    ]))
}

fn build_market_data_request(
    sender_comp_id: &str,
    seq_num: u32,
    kind: FixMdStreamKind,
    depth: u16,
    symbols: &[String],
    label: &str,
) -> String {
    let sending_time = current_fix_time();
    let mut fields = vec![
        (35, "V".to_string()),
        (34, seq_num.to_string()),
        (49, sender_comp_id.to_string()),
        (52, sending_time),
        (56, TARGET_COMP_ID.to_string()),
        (262, format!("{}-{}", kind.label(), label)),
        (263, "1".to_string()),
        (
            264,
            match kind {
                FixMdStreamKind::Bbo | FixMdStreamKind::Trade => 1,
                FixMdStreamKind::Depth => depth.clamp(2, 5000),
            }
            .to_string(),
        ),
        (266, "Y".to_string()),
        (146, symbols.len().to_string()),
    ];
    fields.extend(symbols.iter().cloned().map(|symbol| (55, symbol)));
    match kind {
        FixMdStreamKind::Trade => {
            fields.push((267, "1".to_string()));
            fields.push((269, "2".to_string()));
        }
        FixMdStreamKind::Bbo | FixMdStreamKind::Depth => {
            fields.push((267, "2".to_string()));
            fields.push((269, "0".to_string()));
            fields.push((269, "1".to_string()));
        }
    }
    build_fix_message(&fields)
}

fn build_session_message(
    msg_type: &str,
    sender_comp_id: &str,
    seq_num: u32,
    extra: &[FixField],
) -> String {
    let mut fields = vec![
        (35, msg_type.to_string()),
        (34, seq_num.to_string()),
        (49, sender_comp_id.to_string()),
        (52, current_fix_time()),
        (56, TARGET_COMP_ID.to_string()),
    ];
    fields.extend_from_slice(extra);
    build_fix_message(&fields)
}

fn build_fix_message(fields: &[FixField]) -> String {
    let mut body = String::new();
    for (tag, value) in fields {
        push_fix_field(&mut body, *tag, value);
    }
    let mut out = String::new();
    push_fix_field(&mut out, 8, "FIX.4.4");
    push_fix_field(&mut out, 9, &body.len().to_string());
    out.push_str(&body);
    let checksum = out
        .as_bytes()
        .iter()
        .fold(0u32, |sum, byte| sum + u32::from(*byte))
        % 256;
    push_fix_field(&mut out, 10, &format!("{checksum:03}"));
    out
}

fn push_fix_field(out: &mut String, tag: u32, value: &str) {
    out.push_str(&tag.to_string());
    out.push('=');
    out.push_str(value);
    out.push(SOH);
}

fn load_credentials() -> Result<FixMdCredentials> {
    let api_key = env_first_nonempty(&[ENV_BINANCE_FIX_MD_API_KEY, "BINANCE_ED25519_API_KEY"])
        .ok_or_else(|| {
            anyhow!(
                "{}=fix_sbe requires {} or BINANCE_ED25519_API_KEY",
                ENV_BINANCE_SPOT_TRANSPORT,
                ENV_BINANCE_FIX_MD_API_KEY
            )
        })?;
    let key_path = env_first_nonempty(&[
        ENV_BINANCE_FIX_MD_PRIVATE_KEY_PATH,
        "BINANCE_ED25519_PRIVATE_KEY_PATH",
    ])
    .ok_or_else(|| {
        anyhow!(
            "{}=fix_sbe requires {} or BINANCE_ED25519_PRIVATE_KEY_PATH",
            ENV_BINANCE_SPOT_TRANSPORT,
            ENV_BINANCE_FIX_MD_PRIVATE_KEY_PATH
        )
    })?;
    let pem = std::fs::read(&key_path)
        .with_context(|| format!("read Binance FIX MD Ed25519 key {key_path}"))?;
    let passphrase = std::env::var("BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE").unwrap_or_default();
    let private_key = if passphrase.is_empty() {
        PKey::private_key_from_pem(&pem)
    } else {
        PKey::private_key_from_pem_passphrase(&pem, passphrase.as_bytes())
    }
    .with_context(|| format!("parse Binance FIX MD Ed25519 key {key_path}"))?;
    if private_key.id() != PKeyId::ED25519 {
        bail!("Binance FIX MD private key is not Ed25519: {key_path}");
    }
    Ok(FixMdCredentials {
        api_key,
        private_key,
    })
}

fn env_first_nonempty(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn sign_ed25519_base64(key: &PKey<Private>, payload: &[u8]) -> Result<String> {
    let mut signer = Signer::new_without_digest(key).context("create Ed25519 signer")?;
    let mut signature = [0u8; 64];
    let len = signer
        .sign_oneshot(&mut signature, payload)
        .context("sign Binance FIX MD Logon")?;
    Ok(BASE64_STANDARD.encode(&signature[..len]))
}

fn parse_endpoint(raw: &str) -> Result<(String, u16)> {
    let parsed = url::Url::parse(raw).with_context(|| format!("parse {ENV_BINANCE_FIX_MD_URL}"))?;
    match parsed.scheme() {
        "tcp+tls" | "tls" | "ssl" => {}
        scheme => bail!("unsupported Binance FIX MD URL scheme {scheme}"),
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("Binance FIX MD URL missing host"))?
        .to_string();
    Ok((host, parsed.port().unwrap_or(9001)))
}

fn parse_source_ip(raw: &str) -> Result<Option<IpAddr>> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "0.0.0.0" || raw == "::" {
        return Ok(None);
    }
    Ok(Some(raw.parse().with_context(|| {
        format!("parse Binance FIX MD local IP {raw}")
    })?))
}

async fn connect_tcp(host: &str, port: u16, source_ip: Option<IpAddr>) -> Result<TcpStream> {
    let addrs = lookup_host((host, port))
        .await
        .with_context(|| format!("resolve Binance FIX MD {host}:{port}"))?;
    let mut last_error = None;
    for remote in addrs {
        if source_ip.is_some_and(|source| source.is_ipv4() != remote.is_ipv4()) {
            continue;
        }
        let result = if let Some(source) = source_ip {
            let socket = if source.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            socket.bind(SocketAddr::new(source, 0))?;
            socket.connect(remote).await
        } else {
            TcpStream::connect(remote).await
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("no usable address for Binance FIX MD {host}:{port}")))
}

fn current_fix_time() -> String {
    chrono::Utc::now().format("%Y%m%d-%H:%M:%S%.6f").to_string()
}

fn generate_sender_comp_id() -> String {
    let id = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
    format!("M{}", &id[..7])
}

fn read_var_string8<'a>(frame: &'a [u8], offset: &mut usize) -> Result<&'a str> {
    require_range(frame, *offset, 1, "varString8 length")?;
    let len = frame[*offset] as usize;
    *offset += 1;
    require_range(frame, *offset, len, "varString8 data")?;
    let value = std::str::from_utf8(&frame[*offset..*offset + len])?;
    *offset += len;
    Ok(value)
}

fn read_u16(raw: &[u8], offset: usize) -> Result<u16> {
    require_range(raw, offset, 2, "u16")?;
    Ok(u16::from_le_bytes([raw[offset], raw[offset + 1]]))
}

fn read_u32(raw: &[u8], offset: usize) -> Result<u32> {
    require_range(raw, offset, 4, "u32")?;
    Ok(u32::from_le_bytes(raw[offset..offset + 4].try_into()?))
}

fn read_i64(raw: &[u8], offset: usize) -> Result<i64> {
    require_range(raw, offset, 8, "i64")?;
    Ok(i64::from_le_bytes(raw[offset..offset + 8].try_into()?))
}

fn require_len(raw: &[u8], len: usize, label: &str) -> Result<()> {
    if raw.len() < len {
        bail!("{label} truncated: need={len} actual={}", raw.len());
    }
    Ok(())
}

fn require_range(raw: &[u8], offset: usize, len: usize, label: &str) -> Result<()> {
    if offset > raw.len() || len > raw.len().saturating_sub(offset) {
        bail!(
            "{label} truncated: offset={offset} len={len} actual={}",
            raw.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(template_id: u16, root: &[u8], tail: &[u8]) -> Vec<u8> {
        let len = SOFH_LEN + SBE_HEADER_LEN + root.len() + tail.len();
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(&(len as u32).to_le_bytes());
        out.extend_from_slice(&FIX_SBE_ENCODING_LE.to_le_bytes());
        out.extend_from_slice(&(root.len() as u16).to_le_bytes());
        out.extend_from_slice(&template_id.to_le_bytes());
        out.extend_from_slice(&FIX_SBE_SCHEMA_ID.to_le_bytes());
        out.extend_from_slice(&FIX_SBE_SCHEMA_VERSION.to_le_bytes());
        out.extend_from_slice(&7u32.to_le_bytes());
        out.extend_from_slice(&123_456i64.to_le_bytes());
        out.extend_from_slice(root);
        out.extend_from_slice(tail);
        out
    }

    fn group8(levels: &[(i64, i64)]) -> Vec<u8> {
        let mut out = vec![16, levels.len() as u8];
        for (price, qty) in levels {
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        out
    }

    fn group16(levels: &[(i64, i64)]) -> Vec<u8> {
        let mut out = vec![16];
        out.extend_from_slice(&(levels.len() as u16).to_le_bytes());
        for (price, qty) in levels {
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        out
    }

    #[test]
    fn transport_is_explicit_and_defaults_to_ws() {
        std::env::remove_var(ENV_BINANCE_SPOT_TRANSPORT);
        assert_eq!(
            BinanceSpotTransport::from_env().unwrap(),
            BinanceSpotTransport::WsSbe
        );
        std::env::set_var(ENV_BINANCE_SPOT_TRANSPORT, "fix_sbe");
        assert_eq!(
            BinanceSpotTransport::from_env().unwrap(),
            BinanceSpotTransport::FixSbe
        );
        std::env::remove_var(ENV_BINANCE_SPOT_TRANSPORT);
    }

    #[test]
    fn decodes_incremental_book_ticker() {
        let mut root = Vec::new();
        root.extend_from_slice(&42i64.to_le_bytes());
        root.push((-2i8) as u8);
        root.push((-3i8) as u8);
        let mut tail = group8(&[(12_345, 2_000)]);
        tail.extend_from_slice(&group8(&[(12_355, 3_000)]));
        tail.push(7);
        tail.extend_from_slice(b"BTCUSDT");
        let frame = frame(206, &root, &tail);
        let mut state = new_bbo_state();
        let mut events = Vec::new();
        decode_market_frame(&frame, FixMdStreamKind::Bbo, &mut state, &mut |event| {
            if let FixSbeMarketEvent::Bbo {
                seq_id,
                bid_price,
                ask_price,
                ..
            } = event
            {
                events.push((seq_id, bid_price, ask_price));
            }
        })
        .unwrap();
        assert_eq!(events, vec![(42, 123.45, 123.55)]);
    }

    #[test]
    fn decodes_incremental_trade() {
        let mut root = Vec::new();
        root.extend_from_slice(&777i64.to_le_bytes());
        root.push((-2i8) as u8);
        root.push((-3i8) as u8);
        let mut tail = Vec::new();
        tail.extend_from_slice(&25u16.to_le_bytes());
        tail.extend_from_slice(&1u32.to_le_bytes());
        tail.extend_from_slice(&99i64.to_le_bytes());
        tail.extend_from_slice(&12_345i64.to_le_bytes());
        tail.extend_from_slice(&2_000i64.to_le_bytes());
        tail.push(b'1');
        tail.push(7);
        tail.extend_from_slice(b"BTCUSDT");
        let frame = frame(205, &root, &tail);
        let mut events = Vec::new();

        decode_market_frame(
            &frame,
            FixMdStreamKind::Trade,
            &mut new_bbo_state(),
            &mut |event| {
                if let FixSbeMarketEvent::Trade {
                    timestamp_us,
                    trade_id,
                    side,
                    price,
                    amount,
                    ..
                } = event
                {
                    events.push((timestamp_us, trade_id, side, price, amount));
                }
            },
        )
        .unwrap();

        assert_eq!(events, vec![(777, 99, 'B', 123.45, 2.0)]);
    }

    #[test]
    fn decodes_incremental_depth() {
        let mut root = Vec::new();
        root.extend_from_slice(&40i64.to_le_bytes());
        root.extend_from_slice(&42i64.to_le_bytes());
        root.push((-2i8) as u8);
        root.push((-3i8) as u8);
        let mut tail = group16(&[(12_345, 2_000)]);
        tail.extend_from_slice(&group16(&[(12_355, 3_000)]));
        tail.push(7);
        tail.extend_from_slice(b"BTCUSDT");
        let frame = frame(207, &root, &tail);
        let mut events = Vec::new();

        decode_market_frame(
            &frame,
            FixMdStreamKind::Depth,
            &mut new_bbo_state(),
            &mut |event| {
                if let FixSbeMarketEvent::Book {
                    first_update_id,
                    final_update_id,
                    is_snapshot,
                    bids,
                    asks,
                    ..
                } = event
                {
                    events.push((
                        first_update_id,
                        final_update_id,
                        is_snapshot,
                        bids[0],
                        asks[0],
                    ));
                }
            },
        )
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, 40);
        assert_eq!(events[0].1, 42);
        assert!(!events[0].2);
        assert_eq!(events[0].3.price, 123.45);
        assert_eq!(events[0].3.amount, 2.0);
        assert_eq!(events[0].4.price, 123.55);
        assert_eq!(events[0].4.amount, 3.0);
    }

    #[test]
    fn market_data_request_contains_all_symbols() {
        let msg = build_market_data_request(
            "M1234567",
            2,
            FixMdStreamKind::Bbo,
            20,
            &["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            "primary",
        );
        assert!(msg.contains("146=2\x01"));
        assert!(msg.contains("55=BTCUSDT\x01"));
        assert!(msg.contains("55=ETHUSDT\x01"));
        assert!(msg.contains("267=2\x01"));
        assert!(msg.find("266=Y\x01").unwrap() < msg.find("146=2\x01").unwrap());
        assert!(msg.find("146=2\x01").unwrap() < msg.find("267=2\x01").unwrap());
    }
}
