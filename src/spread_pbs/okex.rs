//! OKex spread 适配器（SBE）。
//!
//! - URL:     `wss://ws.okx.com:8443/ws/v5/public-sbe`
//! - 鉴权:    handshake 时带 REST 风格 `OK-ACCESS-*` 头（不是连上后发 login）
//!            prehash = `<unix_seconds>GET/users/self/verify`
//! - 订阅:    JSON 文本，args 用 `instIdCode` (int) **而不是** `instId` (string)
//!            `instIdCode` 由 REST `GET /api/v5/public/instruments` 一次性拉到映射
//! - 数据帧:  WebSocket 二进制帧 (opcode 2)，82B/帧, littleEndian
//!            header 8B (blockLength u16, templateId u16, schemaId u16, version u16)
//!            body 74B = 8*i64 + 2*i32 + 2*i8
//! - 心跳:    SBE 端不接受 text "ping" (返 60012)；返 None 走 ws 协议级 Ping/Pong
//! - 时间戳:  使用 SBE `outTime` 填 `BboFrame.ts_us`，与现行口径 (JSON `ts`) 同语义
//!            (= binance `E`)；SBE 多出来的 `tsUs` (撮合时刻, = binance `T`) 暂未用到

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};

const OKEX_PUBLIC_SBE_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public-sbe";
const OKEX_INSTRUMENTS_URL: &str = "https://www.okx.com/api/v5/public/instruments";
const OKEX_SUBSCRIBE_CHUNK: usize = 240;

// SBE schema constants (okx_sbe_1_0.xml)
const SBE_HEADER_SIZE: usize = 8;
const SBE_SCHEMA_ID: u16 = 1;
const SBE_TEMPLATE_BBO_TBT: u16 = 1000;
/// `BboTbtChannelEvent` root size = 8*i64 + 2*i32 + 2*i8 = 74。
const SBE_BBO_TBT_BLOCK_LENGTH: usize = 74;

/// 把 OKex `BTC-USDT-SWAP` / `BTC-USDT` 归一化成 `BTCUSDT`。
pub fn normalize_okex_symbol(symbol: &str) -> String {
    let mut upper = symbol.to_ascii_uppercase();
    if upper.ends_with("-SWAP") && upper.len() > 5 {
        upper.truncate(upper.len() - 5);
    }
    upper.retain(|ch| ch != '-');
    upper
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

/// 把 instId 列表切片成多条 SBE bbo-tbt subscribe 消息（240 args/帧软上限）。
/// 用 `instIdCode` 订阅（SBE 端要求；用 `instId` 字符串会返 60018）。
fn build_sbe_bbo_subscribe_messages(
    sym_to_code: &HashMap<String, i64>,
    symbols: &[String],
    chunk_size: usize,
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
        let args: Vec<Value> = chunk
            .iter()
            .map(|code| {
                serde_json::json!({
                    "channel": "bbo-tbt",
                    "instIdCode": code,
                })
            })
            .collect();
        out.push(serde_json::json!({
            "op": "subscribe",
            "args": args,
        }));
    }
    out
}

/// 解一帧 SBE 二进制 bbo-tbt (templateId=1000)。其他 template 返回空 Vec。
fn parse_sbe_bbo_tbt(raw: &[u8], code_to_norm: &HashMap<i64, String>) -> Result<Vec<BboFrame>> {
    if raw.len() < SBE_HEADER_SIZE {
        bail!("OKEx SBE frame too short: {} bytes", raw.len());
    }
    let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let template_id = u16::from_le_bytes([raw[2], raw[3]]);
    let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
    // version 在 raw[6..8]，跨小版本兼容性靠 blockLength 而不是 version，不必校验

    if schema_id != SBE_SCHEMA_ID {
        bail!(
            "OKEx SBE unexpected schemaId={} (want {})",
            schema_id,
            SBE_SCHEMA_ID
        );
    }
    if template_id != SBE_TEMPLATE_BBO_TBT {
        // 当前只订阅 bbo-tbt；预防服务端将来推额外 template (1002 exponent update 等)
        return Ok(Vec::new());
    }
    let body_off = SBE_HEADER_SIZE;
    if raw.len() < body_off + block_length {
        bail!(
            "OKEx SBE frame truncated: have {} bytes, header says blockLength={}",
            raw.len(),
            block_length
        );
    }
    if block_length < SBE_BBO_TBT_BLOCK_LENGTH {
        bail!(
            "OKEx SBE bbo blockLength {} < expected {}",
            block_length,
            SBE_BBO_TBT_BLOCK_LENGTH
        );
    }

    // 按 schema 顺序读，offset 基于 body_off
    let inst_id_code = read_i64_le(raw, body_off)?;
    // tsUs 是撮合时刻 (= binance T)，暂时不用
    let _ts_us = read_i64_le(raw, body_off + 8)?;
    // outTime 是 gateway 发出时刻 (= binance E, = JSON 时代 ts*1000), 走 BboFrame.ts_us
    let out_time_us = read_i64_le(raw, body_off + 16)?;
    let seq_id = read_i64_le(raw, body_off + 24)?;
    let ask_px_m = read_i64_le(raw, body_off + 32)?;
    let ask_sz_m = read_i64_le(raw, body_off + 40)?;
    let bid_px_m = read_i64_le(raw, body_off + 48)?;
    let bid_sz_m = read_i64_le(raw, body_off + 56)?;
    // ask/bidOrdCount @ +64/+68, 单档不用
    let px_exp = raw[body_off + 72] as i8;
    let sz_exp = raw[body_off + 73] as i8;

    let symbol = match code_to_norm.get(&inst_id_code) {
        Some(s) => s.clone(),
        None => {
            // 映射缺失：该 code 没在启动期拉到（新上市 inst？mkt cache 没刷新？）
            // 不构造帧 — 让上层 dedup 不受影响
            log::warn!("OKEx SBE: unknown instIdCode={}; dropped", inst_id_code);
            return Ok(Vec::new());
        }
    };

    let bid_price = mantissa_to_f64(bid_px_m, px_exp);
    let ask_price = mantissa_to_f64(ask_px_m, px_exp);
    let bid_amount = mantissa_to_f64(bid_sz_m, sz_exp);
    let ask_amount = mantissa_to_f64(ask_sz_m, sz_exp);
    if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
        return Ok(Vec::new());
    }

    Ok(vec![BboFrame {
        symbol,
        ts_us: out_time_us,
        seq_id,
        reset_seq: false,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    }])
}

fn read_i64_le(buf: &[u8], off: usize) -> Result<i64> {
    if buf.len() < off + 8 {
        bail!("OKEx SBE OOB read at offset {}", off);
    }
    Ok(i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ]))
}

fn mantissa_to_f64(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10_f64.powi(exponent as i32)
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
        build_sbe_bbo_subscribe_messages(&self.sym_to_code, symbols, OKEX_SUBSCRIBE_CHUNK)
    }

    fn parse_frame(&self, _value: &Value) -> Result<Vec<BboFrame>> {
        // SBE 端的 JSON 文本帧只有 subscribe ack / error event，无业务数据，静默忽略。
        // 错误 event 由 ws.rs `is_keepalive_response` 不识别 → 这里也只是 empty Vec，
        // 不会产生 BboFrame；如需告警可在此 log，但与 binance 等保持静默一致。
        Ok(Vec::new())
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        parse_sbe_bbo_tbt(raw, &self.code_to_norm)
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
        let msgs = build_sbe_bbo_subscribe_messages(&sym_to_code, &symbols, 240);
        assert_eq!(msgs.len(), 3);
        let first_args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(first_args.len(), 240);
        assert_eq!(first_args[0]["channel"], "bbo-tbt");
        assert!(first_args[0]["instIdCode"].is_number());
        assert!(first_args[0].get("instId").is_none());
    }

    #[test]
    fn subscribe_skips_unknown_symbols() {
        let mut sym_to_code = HashMap::new();
        sym_to_code.insert("BTC-USDT-SWAP".to_string(), 10459);
        let symbols = vec!["BTC-USDT-SWAP".to_string(), "MISSING-USDT-SWAP".to_string()];
        let msgs = build_sbe_bbo_subscribe_messages(&sym_to_code, &symbols, 240);
        assert_eq!(msgs.len(), 1);
        let args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(args.len(), 1);
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
