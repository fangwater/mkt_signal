use bytes::Bytes;
use mkt_parsers::msg::basic_account_msg::{
    BasicAccountRiskMsg, BasicBalanceMsg, BasicBorrowInterestMsg,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceResponse {
    #[serde(default, rename = "retCode")]
    ret_code: i32,
    #[serde(default)]
    result: Option<BybitWalletBalanceResult>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceResult {
    #[serde(default)]
    list: Vec<BybitWalletBalanceAccount>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceAccount {
    #[serde(default, rename = "totalAvailableBalance")]
    total_available_balance: String,
    #[serde(default, rename = "totalEquity")]
    total_equity: String,
    #[serde(default, rename = "totalInitialMargin")]
    total_initial_margin: String,
    #[serde(default, rename = "totalMaintenanceMargin")]
    total_maintenance_margin: String,
    #[serde(default, rename = "accountMMRate")]
    account_mm_rate: String,
    #[serde(default)]
    coin: Vec<BybitWalletBalanceCoin>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletBalanceCoin {
    #[serde(default)]
    coin: String,
    #[serde(default, rename = "walletBalance")]
    wallet_balance: String,
    #[serde(default, rename = "borrowAmount")]
    borrow_amount: String,
    #[serde(default, rename = "spotBorrow")]
    spot_borrow: String,
    #[serde(default, rename = "accruedInterest")]
    accrued_interest: String,
    #[serde(default, rename = "borrowInterest")]
    borrow_interest: String,
}

fn parse_f64(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

/// Maps Bybit UTA wallet totals into the common account-risk representation.
///
/// `totalAvailableBalance` is the exchange-calculated capacity for a new UTA
/// order. The common capacity gate derives headroom as `adj_equity - initial`;
/// storing `available + initial` therefore preserves Bybit exact available
/// balance rather than reconstructing it from local positions.
fn parse_bybit_unified_account_risk(account: &BybitWalletBalanceAccount, ts: i64) -> Option<Bytes> {
    let available = parse_f64(&account.total_available_balance)?;
    let actual_equity = parse_f64(&account.total_equity)?;
    let initial_margin = parse_f64(&account.total_initial_margin)?;
    let maintenance_margin = parse_f64(&account.total_maintenance_margin)?;
    if !(available.is_finite()
        && actual_equity.is_finite()
        && initial_margin.is_finite()
        && maintenance_margin.is_finite())
    {
        return None;
    }

    // Bybit reports accountMMRate as maintenance margin / equity. Prefer its
    // account-level value and use the wallet totals as a compatibility fallback.
    let margin_ratio = parse_f64(&account.account_mm_rate)
        .filter(|rate| rate.is_finite() && *rate > f64::EPSILON)
        .map(|rate| 1.0 / rate)
        .unwrap_or_else(|| {
            if maintenance_margin > f64::EPSILON {
                actual_equity / maintenance_margin
            } else {
                f64::INFINITY
            }
        });

    Some(
        BasicAccountRiskMsg::create(
            ts,
            available + initial_margin,
            actual_equity,
            maintenance_margin,
            initial_margin,
            margin_ratio,
            0.0,
            0.0,
        )
        .to_bytes(),
    )
}

pub fn parse_bybit_account_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: BybitWalletBalanceResponse = serde_json::from_str(json).ok()?;
    if resp.ret_code != 0 {
        return None;
    }
    let account = resp.result?.list.into_iter().next()?;
    let ts = chrono::Utc::now().timestamp_millis();

    let mut out = Vec::new();
    if let Some(risk) = parse_bybit_unified_account_risk(&account, ts) {
        out.push(risk);
    }
    for coin in account.coin {
        let symbol = coin.coin.to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }

        let wallet_balance = parse_f64(&coin.wallet_balance).unwrap_or(0.0);
        let has_liability_fields = !coin.borrow_amount.trim().is_empty()
            || !coin.spot_borrow.trim().is_empty()
            || !coin.accrued_interest.trim().is_empty()
            || !coin.borrow_interest.trim().is_empty();
        let borrowed = parse_f64(&coin.spot_borrow)
            .or_else(|| parse_f64(&coin.borrow_amount))
            .unwrap_or(0.0);
        let interest = parse_f64(&coin.accrued_interest)
            .or_else(|| parse_f64(&coin.borrow_interest))
            .unwrap_or(0.0);
        out.push(BasicBalanceMsg::create(ts, symbol.clone(), wallet_balance).to_bytes());

        if borrowed > 0.0 || interest > 0.0 || has_liability_fields {
            out.push(BasicBorrowInterestMsg::create(ts, symbol, borrowed, interest).to_bytes());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{
        get_basic_event_type, BasicAccountEventType, BasicAccountRiskMsg, BasicBalanceMsg,
        BasicBorrowInterestMsg,
    };

    #[test]
    fn parses_bybit_wallet_balance_snapshot() {
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"USDT","walletBalance":"1000","borrowAmount":"50","accruedInterest":"2"},
                {"coin":"BTC","walletBalance":"1.25","borrowAmount":"0","accruedInterest":"0"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 4);
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert_eq!(
            bal.msg_type as u32,
            BasicAccountEventType::BalanceUpdate as u32
        );
        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow ok");
        assert_eq!(
            borrow.msg_type as u32,
            BasicAccountEventType::BorrowInterest as u32
        );
        let clear = BasicBorrowInterestMsg::from_bytes(&msgs[3]).expect("clear ok");
        assert_eq!(clear.symbol, "BTC");
        assert_eq!(clear.borrowed, 0.0);
        assert_eq!(clear.interest, 0.0);
    }

    #[test]
    fn wallet_totals_emit_exact_unified_capacity_and_unimmr() {
        let json = r#"{
            "retCode":0,
            "result":{"list":[{
                "totalAvailableBalance":"7000",
                "totalEquity":"10000",
                "totalInitialMargin":"3000",
                "totalMaintenanceMargin":"500",
                "accountMMRate":"0.05",
                "coin":[]
            }]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        assert_eq!(msgs.len(), 1);
        let risk = BasicAccountRiskMsg::from_bytes(&msgs[0]).expect("risk ok");
        assert!((risk.adj_equity_usd - 10_000.0).abs() < 1e-12);
        assert!((risk.actual_equity_usd - 10_000.0).abs() < 1e-12);
        assert!((risk.initial_margin_usd - 3_000.0).abs() < 1e-12);
        assert!((risk.maintenance_margin_usd - 500.0).abs() < 1e-12);
        assert!((risk.margin_ratio - 20.0).abs() < 1e-12);
        assert!((risk.adj_equity_usd - risk.initial_margin_usd - 7_000.0).abs() < 1e-12);
    }

    #[test]
    fn balance_field_is_gross_wallet_not_net() {
        // Bybit UNIFIED: BasicBalanceMsg.wallet 直接输出 walletBalance，净额由 manager 读取时计算。
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"BNB","walletBalance":"-0.00035791","spotBorrow":"24.474621056148061433","accruedInterest":"0.0000466"},
                {"coin":"DOGE","walletBalance":"9272.16931417","spotBorrow":"101920.083364849093576718","accruedInterest":"0.24072979"},
                {"coin":"USDT","walletBalance":"124649.74470369","borrowAmount":"0","accruedInterest":"0"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        let mut balances: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for m in &msgs {
            if get_basic_event_type(m) == BasicAccountEventType::BalanceUpdate {
                let b = BasicBalanceMsg::from_bytes(m).expect("bal ok");
                balances.insert(b.symbol.clone(), b.wallet);
            }
        }
        assert!((balances["BNB"] + 0.00035791).abs() < 1e-9);
        assert!((balances["DOGE"] - 9272.16931417).abs() < 1e-6);
        assert!((balances["USDT"] - 124649.74470369).abs() < 1e-6);
    }

    #[test]
    fn balance_field_is_net_when_only_walletbalance_present() {
        // 没借币的币种，balance 必须等于 walletBalance（避免误减）
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"BTC","walletBalance":"1.25"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        assert!((bal.wallet - 1.25).abs() < 1e-12);
    }

    #[test]
    fn borrow_prefers_spot_borrow_field() {
        // spotBorrow 更贴近现货借币；borrowAmount 可能包含衍生品负债。
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"ETH","walletBalance":"-0.00011795","spotBorrow":"11.82734984","borrowAmount":"99.0","borrowInterest":"0.00001258"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        let borrow = BasicBorrowInterestMsg::from_bytes(&msgs[1]).expect("borrow ok");
        assert!((borrow.borrowed - 11.82734984).abs() < 1e-12);
        assert!((borrow.interest - 0.00001258).abs() < 1e-12);
    }
}
