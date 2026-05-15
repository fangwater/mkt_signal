use crate::common::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BybitPositionListResponse {
    #[serde(default, rename = "retCode")]
    ret_code: i32,
    #[serde(default)]
    result: Option<BybitPositionListResult>,
}

#[derive(Debug, Deserialize)]
struct BybitPositionListResult {
    #[serde(default)]
    list: Vec<BybitPositionRow>,
}

#[derive(Debug, Deserialize)]
struct BybitPositionRow {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    size: String,
    #[serde(default, rename = "updatedTime")]
    updated_time: String,
    #[serde(default, rename = "unrealisedPnl")]
    unrealised_pnl: String,
    #[serde(default, rename = "unrealizedPnl")]
    unrealized_pnl: String,
}

fn parse_i64(v: &str) -> Option<i64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

fn parse_f32(v: &str) -> Option<f32> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f32>().ok()
}

fn parse_f64(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

fn side_char(raw: &str) -> char {
    match raw.to_ascii_lowercase().as_str() {
        "buy" | "long" => 'L',
        "sell" | "short" => 'S',
        _ => 'N',
    }
}

pub fn parse_bybit_positions_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: BybitPositionListResponse = serde_json::from_str(json).ok()?;
    if resp.ret_code != 0 {
        return None;
    }

    let now_ts = chrono::Utc::now().timestamp_millis();
    let mut out = Vec::new();
    for row in resp.result?.list {
        if row.symbol.is_empty() {
            continue;
        }
        let ts = parse_i64(&row.updated_time).unwrap_or(now_ts);
        let size = parse_f32(&row.size).unwrap_or(0.0);
        let side = side_char(&row.side);

        out.push(BasicPositionMsg::create(ts, row.symbol.clone(), side, size).to_bytes());

        let pnl = parse_f64(&row.unrealised_pnl)
            .or_else(|| parse_f64(&row.unrealized_pnl))
            .unwrap_or(0.0);
        out.push(BasicUmUnrealizedMsg::create(ts, row.symbol, side, pnl).to_bytes());
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        BasicAccountEventType, BasicPositionMsg, BasicUmUnrealizedMsg,
    };

    #[test]
    fn parses_bybit_positions_snapshot() {
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"symbol":"BTCUSDT","side":"Buy","size":"2","updatedTime":"1724742632153","unrealisedPnl":"-12.25"}]}
        }"#;
        let msgs = parse_bybit_positions_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);
        let pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos ok");
        assert_eq!(
            pos.msg_type as u32,
            BasicAccountEventType::PositionUpdate as u32
        );
        let pnl = BasicUmUnrealizedMsg::from_bytes(&msgs[1]).expect("pnl ok");
        assert_eq!(
            pnl.msg_type as u32,
            BasicAccountEventType::UnrealizedPnlUpdate as u32
        );
    }

    #[test]
    fn parses_bybit_linear_usdt_rest_positions_snapshot() {
        let json = r#"{
            "retCode":0,
            "retMsg":"OK",
            "result":{
                "list":[
                    {
                        "symbol":"DOGEUSDT",
                        "side":"Sell",
                        "size":"156378",
                        "updatedTime":"1778823972407",
                        "unrealisedPnl":"-504.631209",
                        "positionIdx":0
                    },
                    {
                        "symbol":"ETHUSDT",
                        "side":"Buy",
                        "size":"40.33",
                        "updatedTime":"1778823748109",
                        "unrealisedPnl":"-2358.72308332",
                        "positionIdx":0
                    }
                ],
                "nextPageCursor":"cursor-value"
            }
        }"#;

        let msgs = parse_bybit_positions_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 4);

        let short_pos = BasicPositionMsg::from_bytes(&msgs[0]).expect("short pos");
        assert_eq!(short_pos.inst_id, "DOGEUSDT");
        assert_eq!(short_pos.position_side, 'S');
        assert!((short_pos.position_amount - 156378.0).abs() < 1e-3);

        let short_pnl = BasicUmUnrealizedMsg::from_bytes(&msgs[1]).expect("short pnl");
        assert_eq!(short_pnl.inst_id, "DOGEUSDT");
        assert_eq!(short_pnl.position_side, 'S');
        assert!((short_pnl.unrealized_pnl + 504.631209).abs() < 1e-6);

        let long_pos = BasicPositionMsg::from_bytes(&msgs[2]).expect("long pos");
        assert_eq!(long_pos.inst_id, "ETHUSDT");
        assert_eq!(long_pos.position_side, 'L');
        assert!((long_pos.position_amount - 40.33).abs() < 1e-5);
    }
}
