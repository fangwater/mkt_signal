//! Exec-owned strategy attribution for one account, aligned with crypto
//! `pre_trade` position allocations. Only the split of the factual account
//! position between strategies lives here; exchange evidence never does.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Account position no named strategy explains: account net minus the named
/// strategies' sum. Its target is always zero, like crypto `SYSTEM_POSITION_CLOSE`.
pub const UNALLOCATED: &str = "__unallocated__";

/// Strategy name to signed lots for one contract; zero holdings are omitted.
pub type Row = BTreeMap<String, i64>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    markets: BTreeMap<String, Row>,
}

/// Target of one strategy in a name-sorted contract target list.
fn target_of(targets: &[(String, i64)], name: &str) -> i64 {
    targets
        .binary_search_by(|(n, _)| n.as_str().cmp(name))
        .map_or(0, |i| targets[i].1)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transfer {
    pub buyer: String,
    pub seller: String,
    pub lots: i64,
}

impl Ledger {
    pub fn from_rows(rows: impl IntoIterator<Item = (String, Row)>) -> Result<Self> {
        let mut markets = BTreeMap::new();
        for (market, row) in rows {
            ensure!(
                row.iter()
                    .all(|(name, lots)| crate::valid_id(name) && name != UNALLOCATED && *lots != 0),
                "invalid strategy ledger row for {market}"
            );
            if !row.is_empty() {
                ensure!(
                    markets.insert(market, row).is_none(),
                    "duplicate strategy ledger market"
                );
            }
        }
        Ok(Self { markets })
    }

    pub fn row(&self, market: &str) -> Row {
        self.markets.get(market).cloned().unwrap_or_default()
    }

    pub fn lots(&self, market: &str, strategy: &str) -> i64 {
        self.markets
            .get(market)
            .and_then(|row| row.get(strategy))
            .copied()
            .unwrap_or(0)
    }

    pub fn allocated(&self, market: &str) -> i64 {
        self.markets.get(market).map_or(0, |row| row.values().sum())
    }

    fn add(&mut self, market: &str, strategy: &str, lots: i64) {
        if strategy == UNALLOCATED || lots == 0 {
            return;
        }
        let row = self.markets.entry(market.to_owned()).or_default();
        let next = row.get(strategy).copied().unwrap_or(0) + lots;
        if next == 0 {
            row.remove(strategy);
        } else {
            row.insert(strategy.to_owned(), next);
        }
        if row.is_empty() {
            self.markets.remove(market);
        }
    }

    /// Named strategies by name: every target plus any strategy still holding,
    /// whose missing target is zero. Allocates; used only when lots move.
    fn strategy_gaps(&self, market: &str, targets: &[(String, i64)]) -> Vec<(String, i64)> {
        let mut names: BTreeSet<&str> = targets.iter().map(|(name, _)| name.as_str()).collect();
        if let Some(row) = self.markets.get(market) {
            names.extend(row.keys().map(String::as_str));
        }
        names
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    target_of(targets, name) - self.lots(market, name),
                )
            })
            .collect()
    }

    /// Unallocated first, then named strategies.
    fn gaps(
        &self,
        market: &str,
        targets: &[(String, i64)],
        account_net: i64,
    ) -> Vec<(String, i64)> {
        let mut gaps = vec![(UNALLOCATED.to_owned(), self.allocated(market) - account_net)];
        gaps.extend(self.strategy_gaps(market, targets));
        gaps
    }

    /// Hot-path check on every fresh quote: no allocation.
    pub fn needs_cross(&self, market: &str, targets: &[(String, i64)], account_net: i64) -> bool {
        let row = self.markets.get(market);
        let lots = |name: &str| row.and_then(|r| r.get(name)).copied().unwrap_or(0);
        let (mut buy, mut sell) = (false, false);
        let mut mark = |gap: i64| {
            buy |= gap > 0;
            sell |= gap < 0;
        };
        mark(row.map_or(0, |r| r.values().sum::<i64>()) - account_net);
        for (name, target) in targets {
            mark(target - lots(name));
        }
        if let Some(row) = row {
            for (name, held) in row {
                if targets
                    .binary_search_by(|(n, _)| n.as_str().cmp(name))
                    .is_err()
                {
                    mark(-held);
                }
            }
        }
        buy && sell
    }

    /// Pairs opposite gaps in deterministic order. Afterwards every remaining gap
    /// has the sign of `Σtargets - account_net`, the lots Exec must trade.
    pub fn cross(
        &mut self,
        market: &str,
        targets: &[(String, i64)],
        account_net: i64,
    ) -> Vec<Transfer> {
        let gaps = self.gaps(market, targets, account_net);
        let mut buyers: VecDeque<_> = gaps.iter().filter(|(_, g)| *g > 0).cloned().collect();
        let mut sellers: VecDeque<_> = gaps
            .iter()
            .filter(|(_, g)| *g < 0)
            .map(|(name, g)| (name.clone(), -g))
            .collect();
        let mut transfers = Vec::new();
        while let (Some(buyer), Some(seller)) = (buyers.front_mut(), sellers.front_mut()) {
            let lots = buyer.1.min(seller.1);
            transfers.push(Transfer {
                buyer: buyer.0.clone(),
                seller: seller.0.clone(),
                lots,
            });
            buyer.1 -= lots;
            seller.1 -= lots;
            if buyer.1 == 0 {
                buyers.pop_front();
            }
            if seller.1 == 0 {
                sellers.pop_front();
            }
        }
        for transfer in &transfers {
            self.add(market, &transfer.buyer, transfer.lots);
            self.add(market, &transfer.seller, -transfer.lots);
        }
        transfers
    }

    /// Operator cutover: moves `lots` (signed; positive makes `strategy`
    /// longer) of the unallocated account remainder to `strategy`. The
    /// remainder must already hold that position; nothing else changes.
    pub fn assign(
        &mut self,
        market: &str,
        strategy: &str,
        lots: i64,
        account_net: i64,
    ) -> Result<Transfer> {
        ensure!(
            crate::valid_id(strategy) && strategy != UNALLOCATED,
            "invalid strategy for position assignment"
        );
        ensure!(lots != 0, "position assignment needs nonzero lots");
        let remainder = account_net - self.allocated(market);
        ensure!(
            remainder.signum() == lots.signum() && remainder.abs() >= lots.abs(),
            "unallocated position of {market} is {remainder} lots; cannot assign {lots}"
        );
        self.add(market, strategy, lots);
        let (buyer, seller) = if lots > 0 {
            (strategy.to_owned(), UNALLOCATED.to_owned())
        } else {
            (UNALLOCATED.to_owned(), strategy.to_owned())
        };
        Ok(Transfer {
            buyer,
            seller,
            lots: lots.abs(),
        })
    }

    /// Attributes one factual fill (signed lots) of this Exec's own order to the
    /// strategies whose gap has the fill's direction, by name. Any remainder stays
    /// unallocated and is returned under [`UNALLOCATED`].
    pub fn allocate_fill(&mut self, market: &str, targets: &[(String, i64)], lots: i64) -> Row {
        let sign = lots.signum();
        let mut remaining = lots.abs();
        let mut allocations = Row::new();
        for (name, gap) in self.strategy_gaps(market, targets) {
            if remaining == 0 {
                break;
            }
            if gap.signum() != sign || gap == 0 {
                continue;
            }
            let take = gap.abs().min(remaining);
            remaining -= take;
            self.add(market, &name, take * sign);
            allocations.insert(name, take * sign);
        }
        if remaining > 0 {
            allocations.insert(UNALLOCATED.to_owned(), remaining * sign);
        }
        allocations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const M: &str = "SHFE:rb2701";

    fn targets(rows: &[(&str, i64)]) -> Vec<(String, i64)> {
        let mut rows: Vec<_> = rows.iter().map(|(n, l)| (n.to_string(), *l)).collect();
        rows.sort();
        rows
    }

    #[test]
    fn assignment_moves_only_the_existing_unallocated_remainder() {
        let mut ledger = Ledger::from_rows([(M.into(), Row::from([("a".into(), 1)]))]).unwrap();
        // Account short 3 with `a` long 1: the remainder is short 4.
        let transfer = ledger.assign(M, "b", -3, -3).unwrap();
        assert_eq!(
            transfer,
            Transfer {
                buyer: UNALLOCATED.into(),
                seller: "b".into(),
                lots: 3
            }
        );
        assert_eq!(ledger.lots(M, "b"), -3);
        assert!(
            ledger.assign(M, "b", -2, -3).is_err(),
            "only 1 lot short remains"
        );
        assert!(ledger.assign(M, "c", 1, -3).is_err(), "wrong direction");
        assert!(ledger.assign(M, UNALLOCATED, -1, -3).is_err());
        assert!(ledger.assign(M, "c", -1, -3).is_ok());
    }

    #[test]
    fn opposite_strategy_targets_cross_internally_without_trading() {
        let mut ledger = Ledger::from_rows([(M.into(), Row::from([("a".into(), 1)]))]).unwrap();
        let t = targets(&[("a", 0), ("b", 1)]);
        assert!(ledger.needs_cross(M, &t, 1));
        let transfers = ledger.cross(M, &t, 1);
        assert_eq!(
            transfers,
            vec![Transfer {
                buyer: "b".into(),
                seller: "a".into(),
                lots: 1
            }]
        );
        assert_eq!(ledger.row(M), Row::from([("b".into(), 1)]));
        assert_eq!(ledger.allocated(M), 1);
        assert!(!ledger.needs_cross(M, &t, 1));
    }

    #[test]
    fn unallocated_account_position_moves_to_matching_strategies_first() {
        // Cutover: account long 3 that no strategy owns yet.
        let mut ledger = Ledger::default();
        let t = targets(&[("a", 2), ("b", 1)]);
        let transfers = ledger.cross(M, &t, 3);
        assert_eq!(transfers.len(), 2);
        assert!(transfers.iter().all(|t| t.seller == UNALLOCATED));
        assert_eq!(ledger.lots(M, "a"), 2);
        assert_eq!(ledger.lots(M, "b"), 1);
        assert_eq!(ledger.allocated(M), 3);
    }

    #[test]
    fn remaining_gaps_share_the_traded_direction() {
        let mut ledger = Ledger::from_rows([(M.into(), Row::from([("a".into(), 2)]))]).unwrap();
        // a -> 0 (sell 2), b -> 3 (buy 3), account 2: trade +1 after crossing.
        let t = targets(&[("a", 0), ("b", 3)]);
        ledger.cross(M, &t, 2);
        let gaps = ledger.gaps(M, &t, 2);
        assert!(gaps.iter().all(|(_, g)| *g >= 0));
        assert_eq!(gaps.iter().map(|(_, g)| g).sum::<i64>(), 3 - 2);
    }

    #[test]
    fn partial_fill_is_attributed_by_name_and_excess_stays_unallocated() {
        let mut ledger = Ledger::default();
        let t = targets(&[("a", 2), ("b", 1)]);
        assert_eq!(ledger.allocate_fill(M, &t, 1), Row::from([("a".into(), 1)]));
        assert_eq!(
            ledger.allocate_fill(M, &t, 3),
            Row::from([("a".into(), 1), ("b".into(), 1), (UNALLOCATED.into(), 1)])
        );
        assert_eq!(ledger.row(M), Row::from([("a".into(), 2), ("b".into(), 1)]));
    }

    #[test]
    fn sell_fill_reduces_long_strategy_and_drops_zero_rows() {
        let mut ledger = Ledger::from_rows([(M.into(), Row::from([("a".into(), 1)]))]).unwrap();
        let t = targets(&[("a", 0)]);
        assert_eq!(
            ledger.allocate_fill(M, &t, -1),
            Row::from([("a".into(), -1)])
        );
        assert_eq!(ledger.row(M), Row::new());
    }

    #[test]
    fn strategy_missing_from_targets_is_flattened() {
        let mut ledger =
            Ledger::from_rows([(M.into(), Row::from([("retired".into(), -2)]))]).unwrap();
        let t = targets(&[("a", -2)]);
        ledger.cross(M, &t, -2);
        assert_eq!(ledger.row(M), Row::from([("a".into(), -2)]));
    }

    #[test]
    fn rows_reject_reserved_names_and_zero_lots() {
        assert!(Ledger::from_rows([(M.into(), Row::from([(UNALLOCATED.into(), 1)]))]).is_err());
        assert!(Ledger::from_rows([(M.into(), Row::from([("a".into(), 0)]))]).is_err());
    }
}
