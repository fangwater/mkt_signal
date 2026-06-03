//! OKex spread 适配器（SBE）。
//!
//! - URL:     `wss://ws.okx.com:8443/ws/v5/public-sbe`
//! - 鉴权:    handshake 时带 REST 风格 `OK-ACCESS-*` 头（不是连上后发 login）
//!            prehash = `<unix_seconds>GET/users/self/verify`
//! - 订阅:    JSON 文本，args 用 `instIdCode` (int) **而不是** `instId` (string)
//!            `instIdCode` 由 REST `GET /api/v5/public/instruments` 一次性拉到映射
//! - 数据帧:  WebSocket 二进制帧 (opcode 2), littleEndian
//!            header 8B (blockLength u16, templateId u16, schemaId u16, version u16)
//!            bbo-tbt body 74B = 8*i64 + 2*i32 + 2*i8
//!            trades body 62B = 7*i64 + i16 + 4*i8
//! - 心跳:    SBE 端不接受 text "ping" (返 60012)；返 None 走 ws 协议级 Ping/Pong
//! - 时间戳:  使用 SBE `outTime` 填 `BboFrame.ts_us`，与现行口径 (JSON `ts`) 同语义
//!            (= binance `E`)；SBE 多出来的 `tsUs` (撮合时刻, = binance `T`) 暂未用到

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use mkt_parsers::okex as okex_codec;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::mkt_msg::Level;
use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};

const OKEX_PUBLIC_SBE_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public-sbe";
const OKEX_INSTRUMENTS_URL: &str = "https://www.okx.com/api/v5/public/instruments";
const OKEX_BOOKS_SBE_URL: &str = "https://openapi.okx.com/api/v5/market/books-sbe";
const OKEX_SUBSCRIBE_CHUNK: usize = 240;

/// 把 OKex `BTC-USDT-SWAP` / `BTC-USDT` 归一化成 `BTCUSDT`。
pub fn normalize_okex_symbol(symbol: &str) -> String {
    okex_codec::normalize_okex_symbol(symbol)
}

pub struct OkexAdapter {
    venue: TradingVenue,
    /// instId (e.g. `BTC-USDT-SWAP`) → instIdCode (e.g. 10459)
    sym_to_code: HashMap<String, i64>,
    /// instIdCode → 归一化后的 symbol（与 JSON 时代输出格式一致）
    code_to_norm: HashMap<i64, String>,
}

impl OkexAdapter {
    /// 启动时 REST 拉一次 instruments，建立双向映射。失败直接返 Err，由上层重试。
    pub async fn new(venue: TradingVenue) -> Result<Self> {
        let inst_type = match venue {
            TradingVenue::OkexFutures => "SWAP",
            TradingVenue::OkexMargin => "SPOT",
            other => {
                bail!("OkexAdapter created with non-okex venue: {:?}", other);
            }
        };
        let raw = fetch_inst_id_codes(inst_type).await.with_context(|| {
            format!(
                "fetch OKEx instIdCode mapping for {:?} (instType={})",
                venue, inst_type
            )
        })?;
        let mut sym_to_code = HashMap::with_capacity(raw.len());
        let mut code_to_norm = HashMap::with_capacity(raw.len());
        for (inst_id, code) in raw {
            let norm = normalize_okex_symbol(&inst_id);
            sym_to_code.insert(inst_id, code);
            code_to_norm.insert(code, norm);
        }
        log::info!(
            "OkexAdapter[{:?}] loaded {} instIdCode entries (instType={})",
            venue,
            sym_to_code.len(),
            inst_type
        );
        Ok(Self {
            venue,
            sym_to_code,
            code_to_norm,
        })
    }
}

/// REST：`GET /api/v5/market/books-sbe?instIdCode=...&source=0` → SBE binary snapshot。
pub async fn fetch_books_sbe_snapshot_bytes(inst_id_code: i64) -> Result<Bytes> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("build reqwest client for OKEx books-sbe snapshot")?;
    let resp = client
        .get(OKEX_BOOKS_SBE_URL)
        .query(&[
            ("instIdCode", inst_id_code.to_string()),
            ("source", "0".to_string()),
        ])
        .send()
        .await
        .with_context(|| format!("OKEx books-sbe GET instIdCode={}", inst_id_code))?;
    let status = resp.status();
    let body = resp
        .bytes()
        .await
        .context("OKEx books-sbe read response body")?;
    if !status.is_success() {
        bail!(
            "OKEx books-sbe returned status={} body={}",
            status,
            String::from_utf8_lossy(&body[..body.len().min(256)])
        );
    }
    if body.first() == Some(&b'{') {
        bail!(
            "OKEx books-sbe returned JSON error body={}",
            String::from_utf8_lossy(&body[..body.len().min(512)])
        );
    }
    Ok(body)
}

/// REST：`GET /api/v5/public/instruments?instType={SWAP|SPOT}` → `{instId → instIdCode}`。
/// 我们不在这里做 USDT/state=live 过滤——`cfg::wait_for_symbols` 已经过滤了；多余的
/// inst 留在映射里不会被订阅（subscribe 只发 wait_for_symbols 返回的那批）。
async fn fetch_inst_id_codes(inst_type: &str) -> Result<HashMap<String, i64>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("build reqwest client for OKEx instruments")?;
    let resp: Value = client
        .get(OKEX_INSTRUMENTS_URL)
        .query(&[("instType", inst_type)])
        .send()
        .await
        .context("OKEx instruments GET")?
        .json()
        .await
        .context("OKEx instruments JSON")?;
    if resp.get("code").and_then(|v| v.as_str()) != Some("0") {
        bail!(
            "OKEx instruments returned non-zero code: {}",
            resp.get("msg").and_then(|v| v.as_str()).unwrap_or("?")
        );
    }
    let data = resp
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("OKEx instruments: missing data array"))?;
    let mut out = HashMap::with_capacity(data.len());
    for row in data {
        let inst_id = match row.get("instId").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let code = match row.get("instIdCode").and_then(|v| v.as_i64()) {
            Some(c) => c,
            None => {
                log::warn!("OKEx instrument {} missing instIdCode, skipped", inst_id);
                continue;
            }
        };
        out.insert(inst_id, code);
    }
    Ok(out)
}

/// SBE 端 handshake auth headers：REST 风格 OK-ACCESS-* 签名。
/// prehash 用 unix seconds (整数, 字符串化), 不是 REST 那种 ISO8601 ms。
fn build_sbe_handshake_headers() -> Result<Vec<(String, String)>> {
    let key = std::env::var("OKX_API_KEY").context("OKX_API_KEY not set")?;
    let secret = std::env::var("OKX_API_SECRET").context("OKX_API_SECRET not set")?;
    let passphrase = std::env::var("OKX_PASSPHRASE").context("OKX_PASSPHRASE not set")?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?
        .as_secs()
        .to_string();
    let prehash = format!("{}GET/users/self/verify", ts);
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .context("hmac key invalid length")?;
    mac.update(prehash.as_bytes());
    let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    Ok(vec![
        ("OK-ACCESS-KEY".to_string(), key),
        ("OK-ACCESS-SIGN".to_string(), sig),
        ("OK-ACCESS-TIMESTAMP".to_string(), ts),
        ("OK-ACCESS-PASSPHRASE".to_string(), passphrase),
    ])
}

/// 把 instId 列表切片成多条 OKX SBE subscribe 消息（240 args/帧软上限）。
/// 用 `instIdCode` 订阅（SBE 端要求；用 `instId` 字符串会返 60018）。
fn build_sbe_subscribe_messages(
    sym_to_code: &HashMap<String, i64>,
    symbols: &[String],
    chunk_size: usize,
) -> Vec<Value> {
    build_sbe_channel_subscribe_messages(sym_to_code, symbols, chunk_size, &["bbo-tbt", "trades"])
}

fn build_sbe_incremental_subscribe_messages(
    sym_to_code: &HashMap<String, i64>,
    symbols: &[String],
    chunk_size: usize,
) -> Vec<Value> {
    build_sbe_channel_subscribe_messages(sym_to_code, symbols, chunk_size, &["books-l2-tbt"])
}

fn build_sbe_channel_subscribe_messages(
    sym_to_code: &HashMap<String, i64>,
    symbols: &[String],
    chunk_size: usize,
    channels: &[&str],
) -> Vec<Value> {
    let chunk_size = chunk_size.max(1);
    // 先把 symbol → code 解析掉，命中后才入下游 subscribe；漏映射的告警一次。
    let mut codes: Vec<i64> = Vec::with_capacity(symbols.len());
    let mut missing: Vec<&String> = Vec::new();
    for sym in symbols {
        match sym_to_code.get(sym) {
            Some(c) => codes.push(*c),
            None => missing.push(sym),
        }
    }
    if !missing.is_empty() {
        log::warn!(
            "OkexAdapter: {} symbol(s) absent from instIdCode map, skipped (sample: {:?})",
            missing.len(),
            &missing[..missing.len().min(8)]
        );
    }
    let mut out = Vec::new();
    for chunk in codes.chunks(chunk_size) {
        for channel in channels {
            let args: Vec<Value> = chunk
                .iter()
                .map(|code| {
                    serde_json::json!({
                        "channel": channel,
                        "instIdCode": code,
                    })
                })
                .collect();
            out.push(serde_json::json!({
                "op": "subscribe",
                "args": args,
            }));
        }
    }
    out
}

/// 解一帧 SBE 二进制 bbo-tbt (templateId=1000)。其他 template 返回空 Vec。
fn parse_sbe_bbo_tbt(raw: &[u8], code_to_norm: &HashMap<i64, String>) -> Result<Vec<BboFrame>> {
    let Some(bbo) = okex_codec::parse_sbe_bbo_tbt(raw)? else {
        return Ok(Vec::new());
    };
    let Some(symbol) = code_to_norm.get(&bbo.inst_id_code).cloned() else {
        log::warn!("OKEx SBE: unknown instIdCode={}; dropped", bbo.inst_id_code);
        return Ok(Vec::new());
    };
    Ok(vec![BboFrame {
        symbol,
        ts_us: bbo.timestamp_us,
        seq_id: bbo.seq_id,
        reset_seq: false,
        bid_price: bbo.bid_price,
        bid_amount: bbo.bid_amount,
        ask_price: bbo.ask_price,
        ask_amount: bbo.ask_amount,
    }])
}

fn parse_sbe_books(
    raw: &[u8],
    code_to_norm: &HashMap<i64, String>,
) -> Result<Vec<IncrementalFrame>> {
    let mut out = Vec::new();
    for book in okex_codec::parse_sbe_books(raw)? {
        if let Some(frame) = sbe_book_to_incremental(book, code_to_norm) {
            out.push(frame);
        }
    }
    Ok(out)
}

/// 解一帧 SBE 二进制 trades (templateId=1005)。其他 template 返回空 Vec。
fn parse_sbe_trades(raw: &[u8], code_to_norm: &HashMap<i64, String>) -> Result<Vec<TradeFrame>> {
    let mut out = Vec::new();
    for trade in okex_codec::parse_sbe_trades(raw)? {
        let Some(symbol) = code_to_norm.get(&trade.inst_id_code).cloned() else {
            log::warn!(
                "OKEx SBE trade: unknown instIdCode={}; dropped",
                trade.inst_id_code
            );
            continue;
        };
        out.push(TradeFrame {
            symbol,
            timestamp_us: trade.timestamp_us,
            seq_id: trade.seq_id,
            trade_id: trade.trade_id,
            side: trade.side,
            price: trade.price,
            amount: trade.amount,
        });
    }
    Ok(out)
}

fn sbe_book_to_incremental(
    book: okex_codec::SbeBook,
    code_to_norm: &HashMap<i64, String>,
) -> Option<IncrementalFrame> {
    match book {
        okex_codec::SbeBook::Book {
            inst_id_code,
            timestamp_us,
            seq_id,
            prev_seq_id,
            first_update_id,
            final_update_id,
            gap_check,
            is_snapshot,
            bids,
            asks,
        } => {
            let Some(symbol) = code_to_norm.get(&inst_id_code).cloned() else {
                log::warn!(
                    "OKEx SBE books: unknown instIdCode={}; dropped",
                    inst_id_code
                );
                return None;
            };
            Some(IncrementalFrame::Book {
                symbol,
                timestamp: timestamp_us,
                seq_id,
                prev_seq_id,
                first_update_id,
                final_update_id,
                gap_check,
                is_snapshot,
                bids: book_levels_to_msg(bids),
                asks: book_levels_to_msg(asks),
            })
        }
        okex_codec::SbeBook::SequenceOnly {
            inst_id_code,
            timestamp_us,
            seq_id,
            prev_seq_id,
        } => {
            let Some(symbol) = code_to_norm.get(&inst_id_code).cloned() else {
                log::warn!(
                    "OKEx SBE books exponent: unknown instIdCode={}; dropped",
                    inst_id_code
                );
                return None;
            };
            Some(IncrementalFrame::SequenceOnly {
                symbol,
                timestamp: timestamp_us,
                seq_id,
                prev_seq_id,
            })
        }
    }
}

fn book_levels_to_msg(levels: Vec<okex_codec::Level>) -> Vec<Level> {
    levels
        .into_iter()
        .map(|level| Level::from_values(level.price, level.amount))
        .collect()
}

impl VenueAdapter for OkexAdapter {
    fn name(&self) -> &'static str {
        "okex"
    }

    fn ws_url(&self) -> String {
        OKEX_PUBLIC_SBE_WS_URL.to_string()
    }

    fn ws_headers(&self) -> Vec<(String, String)> {
        match build_sbe_handshake_headers() {
            Ok(h) => h,
            Err(e) => {
                // env 缺失或系统时钟异常；handshake 必 401，由 ws.rs 主循环重试
                log::error!(
                    "OkexAdapter[{:?}] SBE handshake header build failed: {:#}",
                    self.venue,
                    e
                );
                Vec::new()
            }
        }
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_sbe_subscribe_messages(&self.sym_to_code, symbols, OKEX_SUBSCRIBE_CHUNK)
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_sbe_incremental_subscribe_messages(&self.sym_to_code, symbols, OKEX_SUBSCRIBE_CHUNK)
    }

    fn inst_id_code(&self, symbol: &str) -> Option<i64> {
        self.sym_to_code.get(symbol).copied()
    }

    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>> {
        if let Some(event) = value.get("event").and_then(|v| v.as_str()) {
            if event == "error" || value.get("code").is_some() {
                log::error!("OKEx SBE control event: {}", value);
            }
        }
        Ok(Vec::new())
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        parse_sbe_bbo_tbt(raw, &self.code_to_norm)
    }

    fn parse_trade_binary_frame(&self, raw: &[u8]) -> Result<Vec<TradeFrame>> {
        parse_sbe_trades(raw, &self.code_to_norm)
    }

    fn parse_incremental_binary_frame(&self, raw: &[u8]) -> Result<Vec<IncrementalFrame>> {
        parse_sbe_books(raw, &self.code_to_norm)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        // SBE 端不接受 text "ping"（返 60012 Illegal request）。
        // 依赖 tokio-tungstenite 默认行为 + 服务端 ws Ping → 我们 Pong 回复 (ws.rs run_session)。
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okex_codec::{
        SBE_BBO_TBT_BLOCK_LENGTH, SBE_BOOKS_EXPONENT_BLOCK_LENGTH, SBE_BOOKS_L2_TBT_BLOCK_LENGTH,
        SBE_BOOKS_SNAPSHOT_BLOCK_LENGTH, SBE_BOOK_LEVEL_MIN_BLOCK_LENGTH, SBE_HEADER_SIZE,
        SBE_SCHEMA_ID, SBE_TEMPLATE_BBO_TBT, SBE_TEMPLATE_BOOKS_L2_TBT,
        SBE_TEMPLATE_BOOKS_L2_TBT_EXPONENT, SBE_TEMPLATE_BOOKS_SNAPSHOT, SBE_TEMPLATE_TRADES,
        SBE_TRADES_BLOCK_LENGTH,
    };

    fn make_adapter() -> OkexAdapter {
        let mut sym_to_code = HashMap::new();
        sym_to_code.insert("BTC-USDT-SWAP".to_string(), 10459);
        sym_to_code.insert("ETH-USDT-SWAP".to_string(), 10461);
        let mut code_to_norm = HashMap::new();
        code_to_norm.insert(10459i64, "BTCUSDT".to_string());
        code_to_norm.insert(10461i64, "ETHUSDT".to_string());
        OkexAdapter {
            venue: TradingVenue::OkexFutures,
            sym_to_code,
            code_to_norm,
        }
    }

    fn build_sbe_bbo_frame(
        inst_id_code: i64,
        ts_us: i64,
        out_time_us: i64,
        seq_id: i64,
        ask_px_m: i64,
        ask_sz_m: i64,
        bid_px_m: i64,
        bid_sz_m: i64,
        px_exp: i8,
        sz_exp: i8,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(82);
        buf.extend_from_slice(&(SBE_BBO_TBT_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BBO_TBT.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // version
        buf.extend_from_slice(&inst_id_code.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&out_time_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&ask_px_m.to_le_bytes());
        buf.extend_from_slice(&ask_sz_m.to_le_bytes());
        buf.extend_from_slice(&bid_px_m.to_le_bytes());
        buf.extend_from_slice(&bid_sz_m.to_le_bytes());
        buf.extend_from_slice(&1i32.to_le_bytes()); // askOrdCount
        buf.extend_from_slice(&1i32.to_le_bytes()); // bidOrdCount
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf
    }

    fn build_sbe_trade_frame(
        inst_id_code: i64,
        ts_us: i64,
        out_time_us: i64,
        seq_id: i64,
        px_m: i64,
        sz_m: i64,
        trade_id: i64,
        count: i16,
        side: i8,
        px_exp: i8,
        sz_exp: i8,
        source: i8,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SBE_HEADER_SIZE + SBE_TRADES_BLOCK_LENGTH);
        buf.extend_from_slice(&(SBE_TRADES_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_TRADES.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&inst_id_code.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&out_time_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&px_m.to_le_bytes());
        buf.extend_from_slice(&sz_m.to_le_bytes());
        buf.extend_from_slice(&trade_id.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.push(side as u8);
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.push(source as u8);
        buf
    }

    fn append_sbe_level_group(buf: &mut Vec<u8>, levels: &[(i64, i64, i32)]) {
        buf.extend_from_slice(&(SBE_BOOK_LEVEL_MIN_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&(levels.len() as u16).to_le_bytes());
        for (px_m, sz_m, ord_count) in levels {
            buf.extend_from_slice(&px_m.to_le_bytes());
            buf.extend_from_slice(&sz_m.to_le_bytes());
            buf.extend_from_slice(&ord_count.to_le_bytes());
        }
    }

    fn build_sbe_books_frame(
        inst_id_code: i64,
        ts_us: i64,
        out_time_us: i64,
        seq_id: i64,
        prev_seq_id: i64,
        px_exp: i8,
        sz_exp: i8,
        asks: &[(i64, i64, i32)],
        bids: &[(i64, i64, i32)],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(SBE_BOOKS_L2_TBT_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BOOKS_L2_TBT.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&inst_id_code.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&out_time_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&prev_seq_id.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        append_sbe_level_group(&mut buf, asks);
        append_sbe_level_group(&mut buf, bids);
        buf
    }

    fn build_sbe_books_snapshot_frame(
        inst_id_code: i64,
        ts_us: i64,
        seq_id: i64,
        px_exp: i8,
        sz_exp: i8,
        asks: &[(i64, i64, i32)],
        bids: &[(i64, i64, i32)],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(SBE_BOOKS_SNAPSHOT_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BOOKS_SNAPSHOT.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&inst_id_code.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        append_sbe_level_group(&mut buf, asks);
        append_sbe_level_group(&mut buf, bids);
        buf
    }

    fn build_sbe_books_exponent_frame(
        inst_id_code: i64,
        ts_us: i64,
        out_time_us: i64,
        seq_id: i64,
        prev_seq_id: i64,
        px_exp: i8,
        sz_exp: i8,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(SBE_BOOKS_EXPONENT_BLOCK_LENGTH as u16).to_le_bytes());
        buf.extend_from_slice(&SBE_TEMPLATE_BOOKS_L2_TBT_EXPONENT.to_le_bytes());
        buf.extend_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&inst_id_code.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&out_time_us.to_le_bytes());
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&prev_seq_id.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf
    }

    #[test]
    fn normalize_swap_and_spot() {
        assert_eq!(normalize_okex_symbol("BTC-USDT-SWAP"), "BTCUSDT");
        assert_eq!(normalize_okex_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(normalize_okex_symbol("btc-usdt-swap"), "BTCUSDT");
    }

    #[test]
    fn subscribe_uses_instidcode_and_chunks() {
        let mut sym_to_code = HashMap::new();
        for i in 0..500 {
            sym_to_code.insert(format!("INST{}-USDT-SWAP", i), i as i64);
        }
        let symbols: Vec<String> = (0..500).map(|i| format!("INST{}-USDT-SWAP", i)).collect();
        let msgs = build_sbe_subscribe_messages(&sym_to_code, &symbols, 240);
        assert_eq!(msgs.len(), 6);
        let first_args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(first_args.len(), 240);
        assert_eq!(first_args[0]["channel"], "bbo-tbt");
        assert!(first_args[0]["instIdCode"].is_number());
        assert!(first_args[0].get("instId").is_none());
        let second_args = msgs[1]["args"].as_array().unwrap();
        assert_eq!(second_args.len(), 240);
        assert_eq!(second_args[0]["channel"], "trades");
        assert!(second_args[0]["instIdCode"].is_number());
        assert!(second_args[0].get("instId").is_none());
    }

    #[test]
    fn incremental_subscribe_uses_books_l2_tbt() {
        let mut sym_to_code = HashMap::new();
        sym_to_code.insert("BTC-USDT-SWAP".to_string(), 10459);
        let symbols = vec!["BTC-USDT-SWAP".to_string()];
        let msgs = build_sbe_incremental_subscribe_messages(&sym_to_code, &symbols, 240);
        assert_eq!(msgs.len(), 1);
        let args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(args[0]["channel"], "books-l2-tbt");
        assert_eq!(args[0]["instIdCode"].as_i64(), Some(10459));
    }

    #[test]
    fn subscribe_skips_unknown_symbols() {
        let mut sym_to_code = HashMap::new();
        sym_to_code.insert("BTC-USDT-SWAP".to_string(), 10459);
        let symbols = vec!["BTC-USDT-SWAP".to_string(), "MISSING-USDT-SWAP".to_string()];
        let msgs = build_sbe_subscribe_messages(&sym_to_code, &symbols, 240);
        assert_eq!(msgs.len(), 2);
        let args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0]["channel"], "bbo-tbt");
        assert_eq!(args[0]["instIdCode"].as_i64(), Some(10459));
        let args = msgs[1]["args"].as_array().unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0]["channel"], "trades");
        assert_eq!(args[0]["instIdCode"].as_i64(), Some(10459));
    }

    #[test]
    fn decodes_bbo_tbt_with_negative_exponents() {
        let adapter = make_adapter();
        // BTC: 77623.5 / 77623.6  size 5.12 / 7.89   (px_exp=-1, sz_exp=-2)
        let raw = build_sbe_bbo_frame(
            10459,
            1_779_419_555_777_000,
            1_779_419_555_777_996,
            317_862_000_001,
            776_236, // ask_px_m → 77623.6
            789,     // ask_sz_m → 7.89
            776_235, // bid_px_m → 77623.5
            512,     // bid_sz_m → 5.12
            -1,
            -2,
        );
        let frames = adapter.parse_binary_frame(&raw).expect("decode ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 317_862_000_001);
        assert_eq!(f.ts_us, 1_779_419_555_777_996); // 取 outTime, 不取 tsUs
        assert!((f.bid_price - 77623.5).abs() < 1e-9);
        assert!((f.ask_price - 77623.6).abs() < 1e-9);
        assert!((f.bid_amount - 5.12).abs() < 1e-9);
        assert!((f.ask_amount - 7.89).abs() < 1e-9);
    }

    #[test]
    fn decodes_trade_with_microsecond_out_time() {
        let adapter = make_adapter();
        let raw = build_sbe_trade_frame(
            10459,
            1_779_419_555_777_000,
            1_779_419_555_777_996,
            317_862_000_123,
            776_235,
            512,
            9_876_543_210,
            1,
            1,
            -1,
            -2,
            0,
        );
        let trades = adapter
            .parse_trade_binary_frame(&raw)
            .expect("decode trade ok");
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        assert_eq!(t.symbol, "BTCUSDT");
        assert_eq!(t.timestamp_us, 1_779_419_555_777_996);
        assert_eq!(t.seq_id, 317_862_000_123);
        assert_eq!(t.trade_id, 9_876_543_210);
        assert_eq!(t.side, 'B');
        assert!((t.price - 77623.5).abs() < 1e-9);
        assert!((t.amount - 5.12).abs() < 1e-9);
    }

    #[test]
    fn decodes_books_l2_tbt_groups_in_dat_pbs_order() {
        let adapter = make_adapter();
        let raw = build_sbe_books_frame(
            10459,
            1_779_419_555_777_000,
            1_779_419_555_777_996,
            200,
            150,
            -1,
            -2,
            &[(776_236, 789, 3), (776_240, 100, 1)],
            &[(776_235, 512, 2)],
        );
        let frames = adapter
            .parse_incremental_binary_frame(&raw)
            .expect("decode books ok");
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            IncrementalFrame::Book {
                symbol,
                timestamp,
                seq_id,
                prev_seq_id,
                is_snapshot,
                bids,
                asks,
                ..
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(*timestamp, 1_779_419_555_777_000);
                assert_eq!(*seq_id, 200);
                assert_eq!(*prev_seq_id, 150);
                assert!(!is_snapshot);
                assert_eq!(bids.len(), 1);
                assert_eq!(asks.len(), 2);
                assert!((bids[0].price - 77623.5).abs() < 1e-9);
                assert!((bids[0].amount - 5.12).abs() < 1e-9);
                assert!((asks[0].price - 77623.6).abs() < 1e-9);
                assert!((asks[0].amount - 7.89).abs() < 1e-9);
            }
            _ => panic!("expected book frame"),
        }
    }

    #[test]
    fn decodes_books_sbe_snapshot() {
        let adapter = make_adapter();
        let raw = build_sbe_books_snapshot_frame(
            10459,
            1_779_419_555_777_000,
            200,
            -1,
            -2,
            &[(776_236, 789, 3)],
            &[(776_235, 512, 2)],
        );
        let frames = adapter
            .parse_incremental_binary_frame(&raw)
            .expect("decode snapshot ok");
        match &frames[0] {
            IncrementalFrame::Book {
                symbol,
                seq_id,
                prev_seq_id,
                is_snapshot,
                bids,
                asks,
                ..
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(*seq_id, 200);
                assert_eq!(*prev_seq_id, -1);
                assert!(is_snapshot);
                assert_eq!(bids.len(), 1);
                assert_eq!(asks.len(), 1);
            }
            _ => panic!("expected snapshot book frame"),
        }
    }

    #[test]
    fn decodes_books_exponent_as_sequence_only() {
        let adapter = make_adapter();
        let raw = build_sbe_books_exponent_frame(
            10459,
            1_779_419_555_777_000,
            1_779_419_555_777_996,
            201,
            200,
            -2,
            -4,
        );
        let frames = adapter
            .parse_incremental_binary_frame(&raw)
            .expect("decode exponent ok");
        match &frames[0] {
            IncrementalFrame::SequenceOnly {
                symbol,
                timestamp,
                seq_id,
                prev_seq_id,
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(*timestamp, 1_779_419_555_777_000);
                assert_eq!(*seq_id, 201);
                assert_eq!(*prev_seq_id, 200);
            }
            _ => panic!("expected sequence-only frame"),
        }
    }

    #[test]
    fn rejects_unknown_schema() {
        let adapter = make_adapter();
        let mut raw = build_sbe_bbo_frame(10459, 0, 0, 1, 100, 1, 100, 1, 0, 0);
        // overwrite schemaId (bytes [4..6])
        raw[4] = 9;
        raw[5] = 0;
        let err = adapter.parse_binary_frame(&raw).unwrap_err();
        assert!(err.to_string().contains("schemaId"));
    }

    #[test]
    fn unknown_template_returns_empty() {
        let adapter = make_adapter();
        let mut raw = build_sbe_bbo_frame(10459, 0, 0, 1, 100, 1, 100, 1, 0, 0);
        // template 1001 (books-l2-tbt) — 不在我们订阅范围, drop
        raw[2] = (1001u16 & 0xff) as u8;
        raw[3] = (1001u16 >> 8) as u8;
        assert!(adapter.parse_binary_frame(&raw).unwrap().is_empty());
    }

    #[test]
    fn unknown_instid_code_is_dropped_not_errored() {
        let adapter = make_adapter();
        let raw = build_sbe_bbo_frame(99999, 0, 1, 1, 100, 1, 100, 1, 0, 0);
        assert!(adapter.parse_binary_frame(&raw).unwrap().is_empty());
    }
}
