use anyhow::{anyhow, Result};
use serde_json::Value;
use std::cell::RefCell;
use std::time::Duration;

use crate::common::time_util::get_timestamp_us;
use crate::signal::common::TradingVenue;
use crate::spread_pbs::adapter::{BboFrame, KeepaliveSpec, VenueAdapter};
use crate::spread_pbs::latency::LatencyKll;

const OKEX_PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const OKEX_SUBSCRIBE_CHUNK: usize = 240;
/// `tickers` 频道仅用于 RTT 监控（不发布到 IceOryx）。OKex 文档：tickers 单
/// connection 50 subs 上限。我们取至多 N 个 inst_id 跑 RTT 采样，并强制
/// 把 BTC/ETH/SOL 三个高频 base asset 优先纳入（确保延迟样本里有这三个的数据）。
const OKEX_TICKERS_SAMPLE_LIMIT: usize = 50;
/// tickers 采样必须包含的 base asset（按 base asset 名称匹配 inst_id 前缀）。
const OKEX_TICKERS_PRIORITY_BASES: &[&str] = &["BTC", "ETH", "SOL"];

/// 把 OKex `BTC-USDT-SWAP` / `BTC-USDT` 归一化成 `BTCUSDT`。
pub fn normalize_okex_symbol(symbol: &str) -> String {
    let mut upper = symbol.to_ascii_uppercase();
    if upper.ends_with("-SWAP") && upper.len() > 5 {
        upper.truncate(upper.len() - 5);
    }
    upper.retain(|ch| ch != '-');
    upper
}

/// OKex spot/swap 共用 bbo-tbt frame 解析。
pub fn parse_bbo_tbt(json_str: &str) -> Result<Vec<BboFrame>> {
    let value: Value =
        serde_json::from_str(json_str).map_err(|e| anyhow!("bbo-tbt json parse failed: {}", e))?;

    let arg = value
        .get("arg")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("bbo-tbt missing arg object"))?;

    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    if channel != "bbo-tbt" {
        return Ok(Vec::new());
    }
    let inst_id = arg
        .get("instId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("bbo-tbt missing arg.instId"))?;

    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let symbol = normalize_okex_symbol(inst_id);
    let mut out = Vec::with_capacity(data.len());

    for entry in data {
        let ts_ms = entry
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing ts", inst_id))?;

        let seq_id = entry
            .get("seqId")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing seqId", inst_id))?;

        let bid_arr = entry
            .get("bids")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing bids[0]", inst_id))?;
        let ask_arr = entry
            .get("asks")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("bbo-tbt {} missing asks[0]", inst_id))?;

        let bid_price = parse_string_f64(bid_arr.first(), "bid price", inst_id)?;
        let bid_amount = parse_string_f64(bid_arr.get(1), "bid amount", inst_id)?;
        let ask_price = parse_string_f64(ask_arr.first(), "ask price", inst_id)?;
        let ask_amount = parse_string_f64(ask_arr.get(1), "ask amount", inst_id)?;

        if bid_price <= 0.0 || ask_price <= 0.0 || bid_amount <= 0.0 || ask_amount <= 0.0 {
            continue;
        }

        out.push(BboFrame {
            symbol: symbol.clone(),
            ts_ms,
            seq_id,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        });
    }

    Ok(out)
}

fn parse_string_f64(value: Option<&Value>, field: &str, inst_id: &str) -> Result<f64> {
    value
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("bbo-tbt {} missing/invalid {}", inst_id, field))
}

/// 通用 channel 订阅消息构造（chunk 切批 + 单 inst_id 一条 args）。
fn build_subscribe_messages(inst_ids: &[String], channel: &str, chunk_size: usize) -> Vec<Value> {
    let chunk_size = chunk_size.max(1);
    let mut out = Vec::new();
    for chunk in inst_ids.chunks(chunk_size) {
        let args: Vec<Value> = chunk
            .iter()
            .map(|inst| {
                serde_json::json!({
                    "channel": channel,
                    "instId": inst,
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

/// 把 inst_id 列表切片成多条 bbo-tbt subscribe 消息（OKex 软上限 240 args/帧）。
pub fn build_bbo_tbt_subscribe_messages(inst_ids: &[String], chunk_size: usize) -> Vec<Value> {
    build_subscribe_messages(inst_ids, "bbo-tbt", chunk_size)
}

/// 优先把 BTC/ETH/SOL 取出来，再用其余 inst_id 补到 `sample_limit` 个，
/// 最终用于 `tickers` 频道订阅（仅 RTT 监控用）。
fn select_tickers_sample(inst_ids: &[String], sample_limit: usize) -> Vec<String> {
    let limit = sample_limit.min(inst_ids.len());
    let mut picked: Vec<String> = Vec::with_capacity(limit);
    let mut picked_set: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for &base in OKEX_TICKERS_PRIORITY_BASES {
        let prefix = format!("{}-", base);
        if let Some(inst) = inst_ids
            .iter()
            .find(|s| s.eq_ignore_ascii_case(&format!("{}USDT", base)) || s.starts_with(&prefix))
        {
            if !picked_set.contains(inst.as_str()) {
                picked_set.insert(inst.as_str());
                picked.push(inst.clone());
                if picked.len() >= limit {
                    return picked;
                }
            }
        }
    }
    for inst in inst_ids {
        if picked.len() >= limit {
            break;
        }
        if !picked_set.contains(inst.as_str()) {
            picked_set.insert(inst.as_str());
            picked.push(inst.clone());
        }
    }
    picked
}

/// 取至多 `sample_limit` 个 inst_id 订阅 `tickers` 频道（强制包含 BTC/ETH/SOL）。
pub fn build_tickers_subscribe_messages(
    inst_ids: &[String],
    sample_limit: usize,
    chunk_size: usize,
) -> (Vec<Value>, Vec<String>) {
    let sampled = select_tickers_sample(inst_ids, sample_limit);
    let msgs = build_subscribe_messages(&sampled, "tickers", chunk_size);
    (msgs, sampled)
}

/// 解析 OKex `tickers` 帧的 `data[].ts`（毫秒）。tickers 没 seqId，仅做延迟统计。
fn extract_tickers_timestamps(json_str: &str) -> Result<Vec<i64>> {
    let value: Value =
        serde_json::from_str(json_str).map_err(|e| anyhow!("tickers json parse failed: {}", e))?;
    let channel = value
        .get("arg")
        .and_then(|v| v.get("channel"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if channel != "tickers" {
        return Ok(Vec::new());
    }
    let data = match value.get("data").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::with_capacity(data.len());
    for entry in data {
        let ts = entry
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| anyhow!("tickers entry missing ts"))?;
        out.push(ts);
    }
    Ok(out)
}

pub struct OkexAdapter {
    _venue: TradingVenue,
    /// `tickers` 频道独立 RTT 统计；与主链路（基于 bbo-tbt 的 latency）完全分开
    /// 输出，label 形如 `okex-futures-tickers-rtt`。
    rtt_kll: RefCell<LatencyKll>,
}

impl OkexAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        let rtt_label = format!("{}-tickers-rtt", venue.data_pub_slug());
        Self {
            _venue: venue,
            rtt_kll: RefCell::new(LatencyKll::new(rtt_label)),
        }
    }

    fn record_tickers_rtt(&self, raw: &str) {
        match extract_tickers_timestamps(raw) {
            Ok(ts_list) => {
                if ts_list.is_empty() {
                    return;
                }
                let local_us = get_timestamp_us();
                let mut kll = self.rtt_kll.borrow_mut();
                for ts_ms in ts_list {
                    let okex_us = ts_ms.saturating_mul(1000);
                    kll.push((local_us - okex_us) as f64);
                }
            }
            Err(e) => {
                log::error!("okex tickers parse failed: {:#} payload={}", e, raw);
            }
        }
    }
}

impl VenueAdapter for OkexAdapter {
    fn name(&self) -> &'static str {
        "okex"
    }

    fn ws_url(&self) -> String {
        OKEX_PUBLIC_WS_URL.to_string()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        let mut out = build_bbo_tbt_subscribe_messages(symbols, OKEX_SUBSCRIBE_CHUNK);
        let (tickers_msgs, sampled) = build_tickers_subscribe_messages(
            symbols,
            OKEX_TICKERS_SAMPLE_LIMIT,
            OKEX_SUBSCRIBE_CHUNK,
        );
        if !sampled.is_empty() {
            log::info!(
                "spread_pbs[okex] tickers RTT sample {} insts (priority BTC/ETH/SOL): {:?}",
                sampled.len(),
                &sampled[..sampled.len().min(8)]
            );
        }
        out.extend(tickers_msgs);
        out
    }

    fn parse_frame(&self, raw: &str) -> Result<Vec<BboFrame>> {
        // bbo-tbt 是主链路，命中后直接走原解析。
        if raw.contains("\"channel\":\"bbo-tbt\"") {
            return parse_bbo_tbt(raw);
        }
        // tickers 仅用作 RTT 监控：内部统计、不返回 BboFrame、不发布到 IceOryx。
        if raw.contains("\"channel\":\"tickers\"") {
            self.record_tickers_rtt(raw);
            return Ok(Vec::new());
        }
        // 其他控制帧（subscribe ack / pong）忽略。
        Ok(Vec::new())
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::text(Duration::from_secs(20), "ping"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_bbo_tbt_frame() {
        let raw = r#"{
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "asks": [["91234.5", "0.5", "0", "1"]],
                "bids": [["91234.4", "0.3", "0", "1"]],
                "ts": "1700000000123",
                "seqId": 12345678,
                "checksum": -1
            }]
        }"#;
        let frames = parse_bbo_tbt(raw).expect("parse ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.ts_ms, 1700000000123);
        assert_eq!(f.seq_id, 12345678);
        assert!((f.bid_price - 91234.4).abs() < 1e-9);
        assert!((f.ask_price - 91234.5).abs() < 1e-9);
        assert!((f.bid_amount - 0.3).abs() < 1e-9);
        assert!((f.ask_amount - 0.5).abs() < 1e-9);
    }

    #[test]
    fn missing_seq_id_is_an_error() {
        let raw = r#"{
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "asks": [["91234.5", "0.5", "0", "1"]],
                "bids": [["91234.4", "0.3", "0", "1"]],
                "ts": "1700000000123"
            }]
        }"#;
        let err = parse_bbo_tbt(raw).unwrap_err();
        assert!(err.to_string().contains("seqId"));
    }

    #[test]
    fn ignores_other_channels() {
        let raw = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "data": []
        }"#;
        let frames = parse_bbo_tbt(raw).expect("parse ok");
        assert!(frames.is_empty());
    }

    #[test]
    fn chunks_subscribe_into_240_per_batch() {
        let inst_ids: Vec<String> = (0..500).map(|i| format!("INST-{}", i)).collect();
        let msgs = build_bbo_tbt_subscribe_messages(&inst_ids, 240);
        assert_eq!(msgs.len(), 3);
        let first_args = msgs[0]["args"].as_array().unwrap();
        assert_eq!(first_args.len(), 240);
        let last_args = msgs[2]["args"].as_array().unwrap();
        assert_eq!(last_args.len(), 20);
        assert_eq!(msgs[0]["op"], "subscribe");
    }

    #[test]
    fn tickers_sample_forces_btc_eth_sol() {
        // 列表把 BTC/ETH/SOL 故意放后面，验证 priority 选择把它们提到前面
        let mut inst_ids: Vec<String> = (0..47).map(|i| format!("FAKE{}-USDT-SWAP", i)).collect();
        inst_ids.push("BTC-USDT-SWAP".to_string());
        inst_ids.push("ETH-USDT-SWAP".to_string());
        inst_ids.push("SOL-USDT-SWAP".to_string());

        let sampled = select_tickers_sample(&inst_ids, 50);
        assert_eq!(sampled.len(), 50);
        assert!(sampled.contains(&"BTC-USDT-SWAP".to_string()));
        assert!(sampled.contains(&"ETH-USDT-SWAP".to_string()));
        assert!(sampled.contains(&"SOL-USDT-SWAP".to_string()));
        // 而且 BTC/ETH/SOL 排在前 3
        assert_eq!(sampled[0], "BTC-USDT-SWAP");
        assert_eq!(sampled[1], "ETH-USDT-SWAP");
        assert_eq!(sampled[2], "SOL-USDT-SWAP");
    }

    #[test]
    fn tickers_sample_works_for_spot_inst_id() {
        let inst_ids: Vec<String> = vec![
            "FOO-USDT".to_string(),
            "BAR-USDT".to_string(),
            "BTC-USDT".to_string(),
            "ETH-USDT".to_string(),
            "SOL-USDT".to_string(),
        ];
        let sampled = select_tickers_sample(&inst_ids, 50);
        assert_eq!(sampled[0], "BTC-USDT");
        assert_eq!(sampled[1], "ETH-USDT");
        assert_eq!(sampled[2], "SOL-USDT");
        assert_eq!(sampled.len(), 5);
    }

    #[test]
    fn extract_tickers_ts_pulls_ms() {
        let raw = r#"{
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId":"BTC-USDT-SWAP","last":"100","askPx":"101","askSz":"1",
                      "bidPx":"99","bidSz":"2","ts":"1700000000123"}]
        }"#;
        let ts = extract_tickers_timestamps(raw).unwrap();
        assert_eq!(ts, vec![1700000000123_i64]);
    }

    #[test]
    fn extract_tickers_ts_ignores_other_channels() {
        let raw = r#"{"arg":{"channel":"bbo-tbt","instId":"X"},"data":[]}"#;
        assert!(extract_tickers_timestamps(raw).unwrap().is_empty());
    }
}
