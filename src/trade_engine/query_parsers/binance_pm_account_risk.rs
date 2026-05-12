use crate::common::basic_account_msg::BasicAccountRiskMsg;
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BinancePmAccountResponse {
    #[serde(default, rename = "accountEquity")]
    account_equity: String,
    #[serde(default, rename = "actualEquity")]
    actual_equity: String,
    #[serde(default, rename = "accountMaintMargin")]
    account_maint_margin: String,
    #[serde(default, rename = "accountInitialMargin")]
    account_initial_margin: String,
    #[serde(default, rename = "uniMMR")]
    uni_mmr: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
}

fn parse_f64(value: &str) -> f64 {
    value.trim().parse::<f64>().unwrap_or(0.0)
}

pub fn parse_binance_pm_account_risk(json: &str) -> Option<Bytes> {
    let resp: BinancePmAccountResponse = serde_json::from_str(json).ok()?;
    let timestamp = if resp.update_time > 0 {
        resp.update_time
    } else {
        chrono::Utc::now().timestamp_millis()
    };

    Some(
        BasicAccountRiskMsg::create(
            timestamp,
            parse_f64(&resp.account_equity),
            parse_f64(&resp.actual_equity),
            parse_f64(&resp.account_maint_margin),
            parse_f64(&resp.account_initial_margin),
            parse_f64(&resp.uni_mmr),
            0.0,
            0.0,
        )
        .to_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_pm_account_risk_parses_account_information() {
        let json = r#"{
            "accountEquity": "100123.45",
            "actualEquity": "99876.50",
            "accountMaintMargin": "1500.00",
            "accountInitialMargin": "8000.00",
            "uniMMR": "5.23",
            "updateTime": 1700000000000
        }"#;

        let bytes = parse_binance_pm_account_risk(json).expect("parse ok");
        let msg = BasicAccountRiskMsg::from_bytes(&bytes).expect("decode ok");

        assert_eq!(msg.timestamp, 1_700_000_000_000);
        assert!((msg.adj_equity_usd - 100_123.45).abs() < 1e-9);
        assert!((msg.actual_equity_usd - 99_876.50).abs() < 1e-9);
        assert!((msg.maintenance_margin_usd - 1_500.0).abs() < 1e-9);
        assert!((msg.initial_margin_usd - 8_000.0).abs() < 1e-9);
        assert!((msg.margin_ratio - 5.23).abs() < 1e-12);
    }
}
