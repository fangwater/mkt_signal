use anyhow::{anyhow, Result};
use bytes::Bytes;
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope,
    BasicBalanceMsg, BASIC_ACCOUNT_EVENT_HEADER_LEN,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct RecoveryReadiness {
    complete: HashSet<String>,
}

impl RecoveryReadiness {
    pub fn mark_complete(&mut self, channel: &str) {
        self.complete.insert(channel.into());
    }
    pub fn is_ready(&self) -> bool {
        [
            "Assets",
            "Positions",
            "Accounts",
            "LoanInfo",
            "LoanCapacity",
            "Trades",
            "Statements",
        ]
        .iter()
        .all(|channel| self.complete.contains(*channel))
    }
}

/// Tracks balance identities across Assets delta and complete-snapshot updates.
#[derive(Debug, Default)]
pub struct AccountSnapshotState {
    balances: HashMap<(BasicAccountScope, String), i64>,
}

impl AccountSnapshotState {
    pub fn observe(&mut self, event: &[u8]) -> Result<()> {
        let (scope, balance) = decode_balance_event(event)?;
        let timestamp = self
            .balances
            .entry((scope, balance.symbol))
            .or_insert(balance.timestamp);
        *timestamp = (*timestamp).max(balance.timestamp);
        Ok(())
    }

    /// Records every supplied Assets event. A complete snapshot additionally
    /// emits zero balances for identities previously known in this scope but
    /// absent from that snapshot. Delta updates never clear identities.
    pub fn reconcile_assets_snapshot(
        &mut self,
        scope: BasicAccountScope,
        complete_snapshot: bool,
        events: &[Bytes],
        observed_ms: i64,
    ) -> Result<Vec<Bytes>> {
        if scope == BasicAccountScope::Unknown {
            return Err(anyhow!("Assets snapshot has unknown account scope"));
        }
        let prior: Vec<_> = self
            .balances
            .iter()
            .filter(|((known_scope, _), _)| *known_scope == scope)
            .map(|((_, coin), timestamp)| (coin.clone(), *timestamp))
            .collect();
        let mut present = HashSet::new();
        let mut updates = Vec::new();
        for event in events {
            let (event_scope, balance) = decode_balance_event(event)?;
            if event_scope != scope {
                return Err(anyhow!("Assets event scope does not match snapshot scope"));
            }
            if !present.insert(balance.symbol.clone()) {
                return Err(anyhow!("duplicate asset in snapshot"));
            }
            updates.push(balance);
        }
        let mut accepted = Vec::new();
        for (event, balance) in events.iter().zip(updates) {
            let key = (scope, balance.symbol);
            if self
                .balances
                .get(&key)
                .is_some_and(|timestamp| *timestamp > balance.timestamp)
            {
                continue;
            }
            self.balances.insert(key, balance.timestamp);
            accepted.push(event.clone());
        }
        if !complete_snapshot {
            return Ok(accepted);
        }
        for (coin, timestamp) in prior {
            if present.contains(&coin) || timestamp > observed_ms {
                continue;
            }
            let zero = BasicBalanceMsg::create(observed_ms, coin.clone(), 0.0).to_bytes();
            let event =
                BasicAccountEventMsg::create(BasicAccountEventType::BalanceUpdate, scope, zero)
                    .to_bytes();
            self.balances.insert((scope, coin), observed_ms);
            accepted.push(event);
        }
        Ok(accepted)
    }
}

fn decode_balance_event(event: &[u8]) -> Result<(BasicAccountScope, BasicBalanceMsg)> {
    let (kind, scope, payload) = split_basic_account_event(event)
        .ok_or_else(|| anyhow!("malformed BasicAccountEventMsg"))?;
    if event.len() != BASIC_ACCOUNT_EVENT_HEADER_LEN + payload.len() {
        return Err(anyhow!("BasicAccountEventMsg has trailing bytes"));
    }
    if kind != BasicAccountEventType::BalanceUpdate {
        return Err(anyhow!("expected BalanceUpdate, got {:?}", kind));
    }
    if scope == BasicAccountScope::Unknown {
        return Err(anyhow!("BalanceUpdate has unknown account scope"));
    }
    let balance = BasicBalanceMsg::from_bytes(payload)
        .map_err(|err| anyhow!("malformed balance payload: {err}"))?;
    if balance.symbol.is_empty()
        || !balance.wallet.is_finite()
        || payload.len() != 24 + balance.symbol.len()
    {
        return Err(anyhow!("BalanceUpdate has invalid identity or wallet"));
    }
    Ok((scope, balance))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slow_snapshot_does_not_erase_or_rewind_a_newer_delta() {
        let scope = BasicAccountScope::BinanceUnified;
        let mut state = AccountSnapshotState::default();
        state.observe(&balance(scope, "BTC", 2.0, 30)).unwrap();
        assert!(state
            .reconcile_assets_snapshot(scope, true, &[], 20)
            .unwrap()
            .is_empty());
        assert!(state
            .reconcile_assets_snapshot(scope, true, &[balance(scope, "BTC", 1.0, 10)], 20)
            .unwrap()
            .is_empty());
        assert_eq!(
            state
                .reconcile_assets_snapshot(scope, true, &[], 40)
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn risk_cannot_be_ready_before_all_recovery_sources_complete() {
        let mut readiness = RecoveryReadiness::default();
        for channel in [
            "Assets",
            "Positions",
            "Accounts",
            "LoanInfo",
            "LoanCapacity",
        ] {
            readiness.mark_complete(channel);
            assert!(!readiness.is_ready());
        }
        readiness.mark_complete("Trades");
        assert!(!readiness.is_ready());
        readiness.mark_complete("Statements");
        assert!(readiness.is_ready());
    }

    fn balance(scope: BasicAccountScope, coin: &str, wallet: f64, ts: i64) -> Bytes {
        BasicAccountEventMsg::create(
            BasicAccountEventType::BalanceUpdate,
            scope,
            BasicBalanceMsg::create(ts, coin.to_string(), wallet).to_bytes(),
        )
        .to_bytes()
    }

    #[test]
    fn delta_does_not_clear_known_balance() {
        let scope = BasicAccountScope::BinanceUnified;
        let mut state = AccountSnapshotState::default();
        state.observe(&balance(scope, "USDT", 1.0, 10)).unwrap();
        assert!(state
            .reconcile_assets_snapshot(scope, false, &[], 20)
            .unwrap()
            .is_empty());
        assert_eq!(state.balances.len(), 1);
    }

    #[test]
    fn complete_empty_snapshot_emits_zero_for_known_coin() {
        let scope = BasicAccountScope::BinanceUnified;
        let mut state = AccountSnapshotState::default();
        state.observe(&balance(scope, "USDT", 1.0, 10)).unwrap();
        let zeroes = state
            .reconcile_assets_snapshot(scope, true, &[], 20)
            .unwrap();
        let (_, _, payload) = split_basic_account_event(&zeroes[0]).unwrap();
        assert_eq!(BasicBalanceMsg::from_bytes(payload).unwrap().wallet, 0.0);
        assert_eq!(BasicBalanceMsg::from_bytes(payload).unwrap().timestamp, 20);
    }

    #[test]
    fn scope_isolation_is_preserved() {
        let mut state = AccountSnapshotState::default();
        state
            .observe(&balance(BasicAccountScope::BinanceUnified, "USDT", 1.0, 10))
            .unwrap();
        assert!(state
            .reconcile_assets_snapshot(BasicAccountScope::OkexUnified, true, &[], 20)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn malformed_or_non_balance_event_is_rejected() {
        let mut state = AccountSnapshotState::default();
        assert!(state.observe(&[1, 2]).is_err());
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::AccountRisk,
            BasicAccountScope::BinanceUnified,
            Bytes::new(),
        )
        .to_bytes();
        assert!(state.observe(&event).is_err());
    }
}
