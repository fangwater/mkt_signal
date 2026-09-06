use crate::rapidx_reconcile::decimal;
use anyhow::{ensure, Result};
use persist_common::rapidx_statement::StatementRecord;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct StatementTotals {
    pub portfolio: String,
    pub exchange: String,
    pub coin: String,
    pub symbol: String,
    pub business_type: String,
    pub statement_type: String,
    pub statement_count: usize,
    pub reported_settlement_amount: String,
    pub available_change: String,
    pub overdraw_change: String,
    pub borrow_change: String,
}

#[derive(Debug, Serialize)]
pub struct StatementReport {
    pub coverage: &'static str,
    pub strategy_attribution: &'static str,
    pub start_us: i64,
    pub end_us: i64,
    pub statement_count: usize,
    pub statements: Vec<StatementRecord>,
    pub totals: Vec<StatementTotals>,
}

pub struct StatementLedger {
    start_us: i64,
    end_us: i64,
    max_statements: usize,
    records: BTreeMap<[u8; 32], StatementRecord>,
}

impl StatementLedger {
    pub fn new(start_us: i64, end_us: i64, max_statements: usize) -> Result<Self> {
        ensure!(
            start_us >= 0 && end_us > start_us && max_statements > 0,
            "invalid ledger window or limit"
        );
        Ok(Self {
            start_us,
            end_us,
            max_statements,
            records: BTreeMap::new(),
        })
    }

    pub fn insert(&mut self, record: StatementRecord) -> Result<()> {
        record.validate()?;
        let key = record.stable_key()?;
        if let Some(previous) = self.records.get(&key) {
            ensure!(*previous == record, "conflicting statement identity");
            return Ok(());
        }
        if record.timestamp_us < self.start_us || record.timestamp_us >= self.end_us {
            return Ok(());
        }
        ensure!(
            self.records.len() < self.max_statements,
            "statement report exceeds --max-statements"
        );
        self.records.insert(key, record);
        Ok(())
    }

    pub fn finish(self) -> Result<StatementReport> {
        let mut totals: BTreeMap<[String; 6], StatementTotals> = BTreeMap::new();
        for row in self.records.values() {
            let key = [
                row.portfolio.clone(),
                row.exchange.clone(),
                row.coin.clone(),
                row.symbol.clone(),
                row.business_type.clone(),
                row.statement_type.clone(),
            ];
            let total = totals.entry(key).or_insert_with(|| StatementTotals {
                portfolio: row.portfolio.clone(),
                exchange: row.exchange.clone(),
                coin: row.coin.clone(),
                symbol: row.symbol.clone(),
                business_type: row.business_type.clone(),
                statement_type: row.statement_type.clone(),
                statement_count: 0,
                reported_settlement_amount: "0".into(),
                available_change: "0".into(),
                overdraw_change: "0".into(),
                borrow_change: "0".into(),
            });
            total.statement_count += 1;
            total.reported_settlement_amount = (decimal(&total.reported_settlement_amount)?
                + decimal(&row.delta_amount)?)
            .normalized()
            .to_string();
            for (accumulated, before, after) in [
                (
                    &mut total.available_change,
                    &row.before_available,
                    &row.after_available,
                ),
                (
                    &mut total.overdraw_change,
                    &row.before_overdraw,
                    &row.after_overdraw,
                ),
                (
                    &mut total.borrow_change,
                    &row.before_borrow,
                    &row.after_borrow,
                ),
            ] {
                *accumulated = (decimal(accumulated)? + decimal(after)? - decimal(before)?)
                    .normalized()
                    .to_string();
            }
        }
        Ok(StatementReport {
            coverage: "persisted_statements_in_window",
            strategy_attribution: "unassigned",
            start_us: self.start_us,
            end_us: self.end_us,
            statement_count: self.records.len(),
            statements: self.records.into_values().collect(),
            totals: totals.into_values().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn statement() -> StatementRecord {
        StatementRecord::parse(&serde_json::json!({"portfolioId":123,"exchangeType":"OKX",
            "statementId":"s1","requestId":"r1","coin":"ETH","sym":"","businessType":"SPOT",
            "statementType":"DEDUCT_INTEREST","createAt":1000,"beforeAvailable":"0","afterAvailable":"0",
            "beforeOverdraw":"0.000000017314874246","afterOverdraw":"0.000000023086525446",
            "beforeBorrow":"0","afterBorrow":"0","deltaAmount":"0"}), "123", "OKX").unwrap()
    }
    #[test]
    fn interest_overdraw_is_not_lost_or_counted_as_zero_cost() {
        let mut ledger = StatementLedger::new(1_000_000, 1_001_000, 4).unwrap();
        ledger.insert(statement()).unwrap();
        ledger.insert(statement()).unwrap();
        let report = ledger.finish().unwrap();
        assert_eq!(report.statement_count, 1);
        assert_eq!(report.totals[0].reported_settlement_amount, "0");
        assert_eq!(
            decimal(&report.totals[0].overdraw_change).unwrap(),
            decimal("0.000000005771651200").unwrap()
        );
        assert_eq!(report.strategy_attribution, "unassigned");
    }
    #[test]
    fn scope_currency_and_statement_type_totals_stay_separate() {
        let mut ledger = StatementLedger::new(0, 2_000_000, 10).unwrap();
        for (id, coin, kind, delta) in [
            ("1", "ETH", "FUNDING_FEE", "-0.1"),
            ("2", "ETH", "FUNDING_FEE", "-0.2"),
            ("3", "USDT", "LIQUIDATION_FEE", "0.5"),
        ] {
            let mut row = statement();
            row.statement_id = id.into();
            row.coin = coin.into();
            row.statement_type = kind.into();
            row.delta_amount = delta.into();
            ledger.insert(row).unwrap();
        }
        let report = ledger.finish().unwrap();
        assert_eq!(report.totals.len(), 2);
        assert_eq!(report.totals[0].reported_settlement_amount, "-0.3");
        assert_eq!(report.totals[1].reported_settlement_amount, "0.5");
    }
    #[test]
    fn window_limit_and_conflicts_are_explicit() {
        let mut ledger = StatementLedger::new(1_000_000, 1_001_000, 1).unwrap();
        let mut outside = statement();
        outside.timestamp_us = 1_001_000;
        ledger.insert(outside).unwrap();
        ledger.insert(statement()).unwrap();
        let mut conflict = statement();
        conflict.delta_amount = "1".into();
        assert!(ledger.insert(conflict).is_err());
        let mut another = statement();
        another.statement_id = "s2".into();
        assert!(ledger.insert(another).is_err());
        assert_eq!(ledger.finish().unwrap().statement_count, 1);
    }
}
