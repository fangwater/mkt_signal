use bytes::Bytes;
use mkt_parsers::msg::basic_account_msg::{
    BasicAccountRiskMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawUmAccountResponse {
    #[serde(default, rename = "totalMarginBalance")]
    total_margin_balance: String,
    #[serde(default, rename = "totalMaintMargin")]
    total_maint_margin: String,
    #[serde(default, rename = "totalInitialMargin")]
    total_initial_margin: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
    #[serde(default)]
    positions: Vec<RawUmPosition>,
}

#[derive(Debug, Deserialize)]
struct RawUmPosition {
    #[serde(default)]
    symbol: String,
    #[serde(default, rename = "positionSide")]
    position_side: String,
    #[serde(default, rename = "positionAmt")]
    position_amt: String,
    #[serde(default, rename = "unrealizedProfit", alias = "unRealizedProfit")]
    unrealized_profit: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
}

fn parse_f32(v: &str) -> f32 {
    v.parse::<f32>().unwrap_or(0.0)
}

fn side_to_char(side: &str) -> char {
    match side {
        "LONG" => 'L',
        "SHORT" => 'S',
        _ => 'N',
    }
}

fn parse_positions(positions: Vec<RawUmPosition>) -> Vec<Bytes> {
    let mut out = Vec::new();
    for pos in positions {
        if pos.symbol.is_empty() {
            continue;
        }
        let amount = parse_f32(&pos.position_amt);
        let inst_id = pos.symbol.to_ascii_uppercase();
        let side = side_to_char(&pos.position_side);
        if amount != 0.0 {
            out.push(
                BasicPositionMsg::create(pos.update_time, inst_id.clone(), side, amount).to_bytes(),
            );
        }
        if !pos.unrealized_profit.trim().is_empty() {
            if let Ok(pnl) = pos.unrealized_profit.parse::<f64>() {
                if pnl.abs() > 0.0 {
                    out.push(
                        BasicUmUnrealizedMsg::create(pos.update_time, inst_id, side, pnl)
                            .to_bytes(),
                    );
                }
            }
        }
    }
    out
}

fn parse_standard_account_risk(raw: &RawUmAccountResponse) -> Option<Bytes> {
    let equity = raw.total_margin_balance.trim().parse::<f64>().ok()?;
    let maintenance_margin = raw.total_maint_margin.trim().parse::<f64>().ok()?;
    let initial_margin = raw.total_initial_margin.trim().parse::<f64>().ok()?;
    if !equity.is_finite() || !maintenance_margin.is_finite() || !initial_margin.is_finite() {
        return None;
    }
    let timestamp = if raw.update_time > 0 {
        raw.update_time
    } else {
        chrono::Utc::now().timestamp_millis()
    };
    let margin_ratio = if maintenance_margin > 0.0 {
        equity / maintenance_margin
    } else {
        0.0
    };
    Some(
        BasicAccountRiskMsg::create(
            timestamp,
            equity,
            equity,
            maintenance_margin,
            initial_margin,
            margin_ratio,
            0.0,
            0.0,
        )
        .to_bytes(),
    )
}

pub fn parse_binance_um_account_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let raw: RawUmAccountResponse = serde_json::from_str(json).ok()?;
    Some(parse_positions(raw.positions))
}

/// Parse a Standard USD-M account snapshot, including Binance's account-level USD totals.
/// In Multi-Assets Mode these totals already include BFUSD and the venue's collateral valuation.
pub fn parse_binance_um_account_snapshot_std(json: &str) -> Option<Vec<Bytes>> {
    let raw: RawUmAccountResponse = serde_json::from_str(json).ok()?;
    let risk = parse_standard_account_risk(&raw);
    let mut out = parse_positions(raw.positions);
    out.extend(risk);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{
        get_basic_event_type, BasicAccountEventType, BasicAccountRiskMsg, BasicPositionMsg,
        BasicUmUnrealizedMsg,
    };

    #[test]
    fn parse_um_snapshot_positions() {
        let json = r#"{
            "positions": [{
                "symbol": "BTCUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.5",
                "unRealizedProfit": "12.5",
                "updateTime": 1700000000001
            }]
        }"#;
        let msgs = parse_binance_um_account_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 2);
        let p = BasicPositionMsg::from_bytes(&msgs[0]).expect("pos ok");
        assert_eq!(p.inst_id, "BTCUSDT");
        assert_eq!(p.position_side, 'L');
        assert!((p.position_amount - 0.5).abs() < 1e-6);
        let pnl = BasicUmUnrealizedMsg::from_bytes(&msgs[1]).expect("pnl ok");
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert_eq!(pnl.position_side, 'L');
        assert!((pnl.unrealized_pnl - 12.5).abs() < 1e-9);
    }

    #[test]
    fn standard_snapshot_emits_exchange_valued_multi_asset_equity() {
        let json = r#"{
            "multiAssetsMargin": true,
            "totalMarginBalance": "98765.43",
            "totalMaintMargin": "2500",
            "totalInitialMargin": "12000",
            "updateTime": 1700000000002,
            "assets": [
                {"asset":"USDT","walletBalance":"10000"},
                {"asset":"BFUSD","walletBalance":"90000","marginAvailable":true}
            ],
            "positions": []
        }"#;

        let msgs = parse_binance_um_account_snapshot_std(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            get_basic_event_type(&msgs[0]),
            BasicAccountEventType::AccountRisk
        );
        let risk = BasicAccountRiskMsg::from_bytes(&msgs[0]).expect("risk ok");
        assert_eq!(risk.timestamp, 1_700_000_000_002);
        assert!((risk.actual_equity_usd - 98_765.43).abs() < 1e-9);
        assert!((risk.adj_equity_usd - 98_765.43).abs() < 1e-9);
        assert!((risk.maintenance_margin_usd - 2_500.0).abs() < 1e-12);
        assert!((risk.initial_margin_usd - 12_000.0).abs() < 1e-12);
        assert!((risk.margin_ratio - 39.506172).abs() < 1e-6);
    }

    #[test]
    fn standard_snapshot_keeps_positions_when_totals_are_missing() {
        let json = r#"{
            "positions": [{
                "symbol": "ETHUSDT",
                "positionSide": "BOTH",
                "positionAmt": "2",
                "unrealizedProfit": "0",
                "updateTime": 1700000000003
            }]
        }"#;

        let msgs = parse_binance_um_account_snapshot_std(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            get_basic_event_type(&msgs[0]),
            BasicAccountEventType::PositionUpdate
        );
    }
}
