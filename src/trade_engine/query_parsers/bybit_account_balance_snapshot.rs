use crate::common::basic_account_msg::{BasicBalanceMsg, BasicBorrowInterestMsg};
use bytes::Bytes;
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

pub fn parse_bybit_account_balance_snapshot(json: &str) -> Option<Vec<Bytes>> {
    let resp: BybitWalletBalanceResponse = serde_json::from_str(json).ok()?;
    if resp.ret_code != 0 {
        return None;
    }
    let account = resp.result?.list.into_iter().next()?;
    let ts = chrono::Utc::now().timestamp_millis();

    let mut out = Vec::new();
    for coin in account.coin {
        let symbol = coin.coin.to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }

        let wallet_balance = parse_f64(&coin.wallet_balance).unwrap_or(0.0);
        let borrowed = parse_f64(&coin.borrow_amount)
            .or_else(|| parse_f64(&coin.spot_borrow))
            .unwrap_or(0.0);
        let interest = parse_f64(&coin.accrued_interest)
            .or_else(|| parse_f64(&coin.borrow_interest))
            .unwrap_or(0.0);
        // 与 OKX/Gate/Binance 对齐：BasicBalanceMsg.balance 写入 net 现货头寸 (= 物理持仓 - 借币本金 - 利息)。
        // Bybit V5 walletBalance 是物理持仓 (借币卖出后会变负)，自身不含负债，必须显式扣掉。
        // 不直接用 Bybit 的 equity 字段，因为 USDT 的 equity 会折进账户级 UPL，下游会与 um_unrealized 双计。
        // borrow=interest=0 的币种 (例如 USDT) 自然回退为 walletBalance。
        let net_balance = wallet_balance - borrowed - interest;
        out.push(BasicBalanceMsg::create(ts, symbol.clone(), net_balance).to_bytes());

        if borrowed > 0.0 || interest > 0.0 {
            out.push(BasicBorrowInterestMsg::create(ts, symbol, borrowed, interest).to_bytes());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        get_basic_event_type, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
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
        assert_eq!(msgs.len(), 3);
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
    }

    #[test]
    fn balance_field_is_net_of_borrow_and_interest() {
        // Bybit UNIFIED: walletBalance 是物理持仓，借币卖出后变负；balance 必须扣掉借币本金+利息后输出 net
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"BNB","walletBalance":"-0.00035791","borrowAmount":"24.474621056148061433","accruedInterest":"0.0000466"},
                {"coin":"DOGE","walletBalance":"9272.16931417","borrowAmount":"101920.083364849093576718","accruedInterest":"0.24072979"},
                {"coin":"USDT","walletBalance":"124649.74470369","borrowAmount":"0","accruedInterest":"0"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        let mut balances: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for m in &msgs {
            if get_basic_event_type(m) == BasicAccountEventType::BalanceUpdate {
                let b = BasicBalanceMsg::from_bytes(m).expect("bal ok");
                balances.insert(b.symbol.clone(), b.balance);
            }
        }
        // 公式：net = walletBalance - borrowAmount - accruedInterest
        // 与 Bybit 自身的 equity 字段不要求 bit-exact，因为 Bybit equity 用 spotBorrow 不扣 interest，
        // 这里采用与 OKX 现行约定一致的更保守做法 (扣本金+利息)；和 equity 仅有 ~interest 量级偏差。
        let bnb = balances["BNB"];
        let bnb_expected = -0.00035791_f64 - 24.474621056148061433_f64 - 0.0000466_f64;
        assert!(
            (bnb - bnb_expected).abs() < 1e-9,
            "BNB net={} 应≈{}",
            bnb,
            bnb_expected
        );
        let doge = balances["DOGE"];
        let doge_expected = 9272.16931417_f64 - 101920.083364849093576718_f64 - 0.24072979_f64;
        assert!(
            (doge - doge_expected).abs() < 1e-6,
            "DOGE net={} 应≈{}",
            doge,
            doge_expected
        );
        // USDT borrow=0, net 应该回退为 walletBalance（避免和下游 um_unrealized 双计 UPL）
        let usdt = balances["USDT"];
        assert!(
            (usdt - 124649.74470369).abs() < 1e-6,
            "USDT net={} 应=walletBalance",
            usdt
        );
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
        assert!((bal.balance - 1.25).abs() < 1e-12);
    }

    #[test]
    fn fallback_to_spot_borrow_field() {
        // 部分响应只带 spotBorrow，balance 也应正确扣减
        let json = r#"{
            "retCode":0,
            "result":{"list":[{"coin":[
                {"coin":"ETH","walletBalance":"-0.00011795","spotBorrow":"11.82734984","borrowInterest":"0.00001258"}
            ]}]}
        }"#;
        let msgs = parse_bybit_account_balance_snapshot(json).expect("parse ok");
        let bal = BasicBalanceMsg::from_bytes(&msgs[0]).expect("bal ok");
        let expected = -0.00011795_f64 - 11.82734984_f64 - 0.00001258_f64;
        assert!(
            (bal.balance - expected).abs() < 1e-9,
            "ETH net={} 应≈{}",
            bal.balance,
            expected
        );
    }
}
