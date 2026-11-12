//! Observe the opening gate already published by native Binance PM FR.
//! This monitor never changes the gate or submits an order.

use serde::Serialize;
use serde_json::Value;
use viz_common::resample::BinancePmUsdtCapacity;

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub state: String,
    pub problem: Option<bool>,
    pub detail: String,
}

impl Status {
    pub fn unknown(&mut self) {
        self.problem = None;
        self.detail = "开仓状态数据不可用，等待新快照确认；不确认恢复".into();
    }
}

pub fn binance_pm_usdt(risk: Option<&Value>, now: i64, observed: bool) -> Option<Status> {
    let value = risk
        .and_then(|r| r.get("binance_pm_usdt"))
        .filter(|v| !v.is_null());
    if value.is_none() && !observed {
        // Standard accounts and other venues do not publish this PM gate.
        return None;
    }
    let mut status = Status {
        state: "unknown".into(),
        problem: None,
        detail: "开仓状态数据缺失或无效，不确认恢复".into(),
    };
    let Some(capacity) =
        value.and_then(|v| serde_json::from_value::<BinancePmUsdtCapacity>(v.clone()).ok())
    else {
        return Some(status);
    };
    let (problem, label) = match capacity.state.as_str() {
        "binance_margin_frequency_cooldown" => (true, "交易所频控冷却，暂停新开仓"),
        "binance_usdt_capacity_not_ready" => (true, "资金数据缺失或过期，暂停新买单"),
        "binance_usdt_buy_capacity_insufficient" => (true, "USDT 购买能力不足，暂停新买单"),
        "binance_usdt_borrow_below_minimum" => (
            true,
            "买单借款差额低于最低借款额或仍在拒单冷却，已在本地拦截",
        ),
        "binance_usdt_borrow_rules_not_ready" => {
            (true, "最低借款规则缺失或过期，暂停需要借款的新买单")
        }
        "ready" => (false, "现金与可借额度已就绪，开仓资金/频控门禁已解除"),
        _ => return Some(status),
    };
    status.state = capacity.state;
    if !problem {
        // A fresh account envelope alone cannot validate an old capacity query.
        let fresh = capacity
            .snapshot_ts_ms
            .is_some_and(|ts| ts > 0 && ts <= now + 5_000 && now.saturating_sub(ts) <= 180_000);
        let valid = [
            capacity.cash_available,
            capacity.max_borrowable,
            capacity.buy_capacity,
        ]
        .iter()
        .all(|v| v.is_some_and(|n| n.is_finite() && n >= 0.0))
            && capacity.buy_capacity.is_some_and(|n| n > 0.0)
            && capacity.reserved_usdt.is_finite()
            && capacity.reserved_usdt >= 0.0;
        if !fresh || !valid {
            status.unknown();
            return Some(status);
        }
    }
    status.problem = Some(problem);
    status.detail = label.into();
    if status.state == "binance_margin_frequency_cooldown" && capacity.retry_after_ms > 0 {
        status.detail.push_str(&format!(
            "；本地冷却剩余约 {} 秒，届时仍需交易所接受请求",
            capacity.retry_after_ms.saturating_add(999) / 1000
        ));
    }
    let amount = |v: Option<f64>| {
        v.filter(|n| n.is_finite() && *n >= 0.0)
            .map(|n| format!("{n:.2} U"))
            .unwrap_or_else(|| "未知".into())
    };
    status.detail.push_str(&format!(
        "；现金 {}，可借 {}，购买能力 {}",
        amount(capacity.cash_available),
        amount(capacity.max_borrowable),
        amount(capacity.buy_capacity)
    ));
    Some(status)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use serde_json::json;
    const NOW: i64 = 1_790_000_000_000;

    pub fn risk(state: &str) -> Value {
        json!({"binance_pm_usdt": {
            "state": state, "cash_available": 95.0, "max_borrowable": 900000.0,
            "buy_capacity": 900095.0, "reserved_usdt": 0.0,
            "snapshot_ts_ms": NOW, "retry_after_ms": 600000
        }})
    }

    #[test]
    fn blocked_states_are_reported_without_an_exposure_breach() {
        for state in [
            "binance_margin_frequency_cooldown",
            "binance_usdt_capacity_not_ready",
            "binance_usdt_buy_capacity_insufficient",
            "binance_usdt_borrow_below_minimum",
            "binance_usdt_borrow_rules_not_ready",
        ] {
            let mut value = risk(state);
            value["binance_pm_usdt"]["snapshot_ts_ms"] = Value::Null;
            assert_eq!(
                binance_pm_usdt(Some(&value), NOW, false).unwrap().problem,
                Some(true)
            );
        }
        assert!(
            binance_pm_usdt(Some(&risk("binance_margin_frequency_cooldown")), NOW, false)
                .unwrap()
                .detail
                .contains("600 秒")
        );
    }

    #[test]
    fn only_valid_ready_capacity_can_confirm_recovery() {
        assert_eq!(
            binance_pm_usdt(Some(&risk("ready")), NOW, true)
                .unwrap()
                .problem,
            Some(false)
        );
        for value in [Value::Null, json!({}), risk("new_unknown_state")] {
            assert_eq!(
                binance_pm_usdt(Some(&value), NOW, true).unwrap().problem,
                None
            );
        }
        for ts in [NOW - 180001, NOW + 5001, 0] {
            let mut value = risk("ready");
            value["binance_pm_usdt"]["snapshot_ts_ms"] = json!(ts);
            assert_eq!(
                binance_pm_usdt(Some(&value), NOW, true).unwrap().problem,
                None
            );
        }
        for value in [Value::Null, json!(-1), json!(0)] {
            let mut r = risk("ready");
            r["binance_pm_usdt"]["buy_capacity"] = value;
            assert_eq!(binance_pm_usdt(Some(&r), NOW, true).unwrap().problem, None);
        }
        assert!(binance_pm_usdt(Some(&json!({})), NOW, false).is_none());
        assert!(binance_pm_usdt(None, NOW, false).is_none());
    }
}
