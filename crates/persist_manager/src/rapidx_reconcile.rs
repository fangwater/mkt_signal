//! Read-only reconciliation of execution evidence. No order lifecycle, strategy
//! attribution, settlement currency or cumulative rebate allocation is invented.
use anyhow::{ensure, Context, Result};
use bigdecimal::BigDecimal;
use persist_common::rapidx_execution::{ExecutionEvidence, ExecutionRecord};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

type Identity = (String, String, String);

#[derive(Default)]
struct Observations {
    ws: Option<ExecutionEvidence>,
    rest: Option<ExecutionEvidence>,
}

pub struct Reconciler {
    records: BTreeMap<Identity, Observations>,
    max_executions: usize,
    liquidations: BTreeMap<Identity, LiquidationEvidence>,
    liquidation_observations: usize,
    other_scope_liquidation_observations: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LiquidationEvidence {
    pub liquidation_event_id: String,
    pub position_id: String,
    pub symbol: String,
    pub position_side: String,
    pub reduce_quantity: String,
    pub closed_pnl_usdt: String,
    /// A separate account-ledger penalty, never a fill fee.
    pub liquidation_fee_usdt: String,
    pub reported_total_trading_fee_usdt: Option<String>,
    pub create_us: i64,
    pub update_us: i64,
}

#[derive(Debug, Serialize)]
pub struct FeeTotals {
    pub charged: String,
    pub rebated: String,
    /// Absent for an order containing an unresolved REST-only cumulative rebate.
    pub net_charge: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReconciledExecution {
    pub portfolio: String,
    pub exchange: String,
    pub transaction_id: String,
    pub execution: ExecutionEvidence,
    pub ws_observed: bool,
    pub rest_observed: bool,
    pub fees_complete: bool,
    pub fees: BTreeMap<String, FeeTotals>,
    /// Raw cumulative evidence is retained for audit, never summed as a fill fee.
    pub rest_reported_rebate: Option<String>,
    pub rest_rebate_currency: Option<String>,
    pub forced_close_reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct OrderTotals {
    pub portfolio: String,
    pub exchange: String,
    pub order_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    pub execution_count: usize,
    pub quantity: String,
    /// Venue-reported realized PNL only, not strategy net PNL or settlement cash.
    pub realized_pnl_reported: String,
    pub fees_complete: bool,
    pub fees: BTreeMap<String, FeeTotals>,
    pub forced_close_reason: Option<&'static str>,
    pub liquidation: Option<LiquidationEvidence>,
}

#[derive(Debug, Serialize)]
pub struct ReconciliationReport {
    /// This report checks stored evidence, not exchange history completeness.
    pub coverage: &'static str,
    pub strategy_attribution: &'static str,
    pub liquidation_observations: usize,
    pub other_scope_liquidation_observations: usize,
    /// Snapshot totals are retained as evidence, not independently settled here.
    pub liquidation_totals_verified: bool,
    pub start_us: i64,
    pub end_us: i64,
    pub execution_count: usize,
    pub unresolved_fee_executions: usize,
    pub executions: Vec<ReconciledExecution>,
    pub orders: Vec<OrderTotals>,
    /// Whole liquidation order lifetimes inside the window without a stored fill.
    pub unmatched_liquidations: Vec<UnmatchedLiquidation>,
}

#[derive(Debug, Serialize)]
pub struct UnmatchedLiquidation {
    pub portfolio: String,
    pub exchange: String,
    pub order_id: String,
    pub evidence: LiquidationEvidence,
}

impl Reconciler {
    pub fn new(max_executions: usize) -> Result<Self> {
        ensure!(max_executions > 0, "max executions must be positive");
        Ok(Self {
            records: BTreeMap::new(),
            max_executions,
            liquidations: BTreeMap::new(),
            liquidation_observations: 0,
            other_scope_liquidation_observations: 0,
        })
    }

    /// Only the documented per-liquidation-order channels establish ownership.
    /// Cancellation snapshots and arbitrary tradeSource strings are not proof.
    pub fn observe_liquidation(
        &mut self,
        message: &Value,
        portfolio: &str,
        exchange: &str,
    ) -> Result<()> {
        if !matches!(
            message["channel"].as_str(),
            Some("LiquidationPosition" | "LiquidationPositionByUser")
        ) {
            return Ok(());
        }
        let row = &message["data"];
        let text = |key: &str| -> Result<String> {
            Ok(row[key]
                .as_str()
                .filter(|s| !s.is_empty())
                .with_context(|| format!("liquidation missing {key}"))?
                .to_owned())
        };
        let reported_portfolio = text("portfolioId")?;
        let reported_exchange = text("exchangeType")?;
        if reported_portfolio != portfolio || reported_exchange != exchange {
            self.other_scope_liquidation_observations += 1;
            return Ok(());
        }
        let symbol = text("sym")?;
        let parts: Vec<_> = symbol.split('_').collect();
        ensure!(
            parts.len() == 4
                && parts[0] == exchange
                && parts[1] == "PERP"
                && !parts[2].is_empty()
                && !parts[3].is_empty(),
            "invalid liquidation symbol scope"
        );
        let side = text("positionSide")?;
        ensure!(
            matches!(side.as_str(), "LONG" | "SHORT" | "NONE"),
            "invalid liquidation position side"
        );
        let quantity = text("reduceQty")?;
        ensure!(
            decimal(&quantity)? > BigDecimal::from(0),
            "nonpositive liquidation reduction"
        );
        let pnl = text("closedPnl")?;
        decimal(&pnl)?;
        let penalty = text("liqFee")?;
        ensure!(
            decimal(&penalty)? >= BigDecimal::from(0),
            "negative liquidation penalty"
        );
        let trading_fee = match &row["totalTradingFee"] {
            Value::Null => None,
            Value::String(fee) => {
                decimal(fee)?;
                Some(fee.clone())
            }
            _ => anyhow::bail!("invalid liquidation trading fee"),
        };
        let time = |key: &str| -> Result<i64> {
            row[key]
                .as_i64()
                .filter(|t| *t > 0)
                .and_then(|t| t.checked_mul(1_000))
                .with_context(|| format!("invalid liquidation {key}"))
        };
        let evidence = LiquidationEvidence {
            liquidation_event_id: text("liquidationEventId")?,
            position_id: text("positionId")?,
            symbol,
            position_side: side,
            reduce_quantity: quantity,
            closed_pnl_usdt: pnl,
            liquidation_fee_usdt: penalty,
            reported_total_trading_fee_usdt: trading_fee,
            create_us: time("createAt")?,
            update_us: time("updateAt")?,
        };
        ensure!(
            evidence.update_us >= evidence.create_us,
            "liquidation update predates creation"
        );
        let key = (reported_portfolio, reported_exchange, text("orderId")?);
        if let Some(old) = self.liquidations.get(&key) {
            ensure!(*old == evidence, "conflicting liquidation order evidence");
        } else {
            ensure!(
                self.liquidations.len() < self.max_executions,
                "liquidation evidence limit exceeded"
            );
            self.liquidations.insert(key, evidence);
        }
        self.liquidation_observations += 1;
        Ok(())
    }

    pub fn insert(&mut self, record: ExecutionRecord) -> Result<()> {
        record.validate()?;
        let key = (
            record.portfolio,
            record.exchange,
            record.execution.transaction_id.clone(),
        );
        ensure!(
            self.records.contains_key(&key) || self.records.len() < self.max_executions,
            "reconciliation execution limit exceeded; increase --max-executions"
        );
        let pair = self.records.entry(key).or_default();
        let slot = if record.execution.rest {
            &mut pair.rest
        } else {
            &mut pair.ws
        };
        if let Some(previous) = slot {
            ensure!(
                *previous == record.execution,
                "conflicting same-source execution observation"
            );
        } else {
            *slot = Some(record.execution);
        }
        Ok(())
    }

    pub fn finish(self, start_us: i64, end_us: i64) -> Result<ReconciliationReport> {
        ensure!(
            start_us >= 0 && end_us > start_us,
            "invalid reconciliation time window"
        );
        let mut executions = Vec::new();
        let mut orders: BTreeMap<Identity, OrderTotals> = BTreeMap::new();
        for ((portfolio, exchange, transaction_id), pair) in self.records {
            // Select both observations if either falls in the window, so a
            // conflicting timestamp cannot hide its counterpart at the boundary.
            if !pair.ws.iter().chain(pair.rest.iter()).any(|e| {
                let us = e.timestamp_ms * 1_000; // checked during insert
                us >= start_us && us < end_us
            }) {
                continue;
            }
            let result = reconcile(&pair).with_context(|| {
                format!(
                "reconcile portfolio={portfolio} exchange={exchange} transaction={transaction_id}"
            )
            })?;
            let (execution, fees, fees_complete) = result;
            let key = (
                portfolio.clone(),
                exchange.clone(),
                execution.order_id.clone(),
            );
            let liquidation = self.liquidations.get(&key).cloned();
            if let Some(evidence) = &liquidation {
                ensure!(
                    evidence.symbol == execution.symbol,
                    "liquidation/execution symbol conflict"
                );
                ensure!(
                    !matches!(
                        (evidence.position_side.as_str(), execution.side.as_str()),
                        ("LONG", "BUY") | ("SHORT", "SELL")
                    ),
                    "liquidation/execution side conflict"
                );
                ensure!(
                    execution.timestamp_ms * 1_000 >= evidence.create_us
                        && execution.timestamp_ms * 1_000 <= evidence.update_us,
                    "execution outside liquidation order lifetime"
                );
            }
            let forced_close_reason = liquidation
                .as_ref()
                .map(|_| "exchange_forced_close:liquidation");
            let order = orders.entry(key).or_insert_with(|| OrderTotals {
                portfolio: portfolio.clone(),
                exchange: exchange.clone(),
                order_id: execution.order_id.clone(),
                client_order_id: execution.client_order_id.clone(),
                symbol: execution.symbol.clone(),
                side: execution.side.clone(),
                execution_count: 0,
                quantity: "0".into(),
                realized_pnl_reported: "0".into(),
                fees_complete: true,
                fees: BTreeMap::new(),
                forced_close_reason,
                liquidation,
            });
            ensure!(
                order.client_order_id == execution.client_order_id
                    && order.symbol == execution.symbol
                    && order.side == execution.side,
                "inconsistent identity within order {}",
                execution.order_id
            );
            order.execution_count += 1;
            order.quantity = sum(&order.quantity, &execution.quantity)?;
            order.realized_pnl_reported =
                sum(&order.realized_pnl_reported, &execution.realized_pnl)?;
            order.fees_complete &= fees_complete;
            for (coin, fee) in &fees {
                let total = order.fees.entry(coin.clone()).or_insert_with(zero_fees);
                total.charged = sum(&total.charged, &fee.charged)?;
                total.rebated = sum(&total.rebated, &fee.rebated)?;
            }
            executions.push(ReconciledExecution {
                portfolio,
                exchange,
                transaction_id,
                execution,
                ws_observed: pair.ws.is_some(),
                rest_observed: pair.rest.is_some(),
                fees_complete,
                fees,
                rest_reported_rebate: pair.rest.as_ref().and_then(|r| r.reported_rebate.clone()),
                rest_rebate_currency: pair.rest.as_ref().and_then(|r| r.rebate_currency.clone()),
                forced_close_reason,
            });
        }
        for order in orders.values_mut() {
            if let Some(evidence) = &order.liquidation {
                ensure!(
                    decimal(&order.quantity)? <= decimal(&evidence.reduce_quantity)?,
                    "execution quantity exceeds liquidation order reduction"
                );
            }
            for fee in order.fees.values_mut() {
                fee.net_charge = if order.fees_complete {
                    Some(
                        (decimal(&fee.charged)? - decimal(&fee.rebated)?)
                            .normalized()
                            .to_string(),
                    )
                } else {
                    None
                };
            }
        }
        let unmatched_liquidations = self
            .liquidations
            .into_iter()
            .filter(|(key, evidence)| {
                evidence.create_us >= start_us
                    && evidence.update_us < end_us
                    && !orders.contains_key(key)
            })
            .map(
                |((portfolio, exchange, order_id), evidence)| UnmatchedLiquidation {
                    portfolio,
                    exchange,
                    order_id,
                    evidence,
                },
            )
            .collect();
        Ok(ReconciliationReport {
            coverage: "persisted_executions_in_window",
            strategy_attribution: "unassigned",
            liquidation_observations: self.liquidation_observations,
            other_scope_liquidation_observations: self.other_scope_liquidation_observations,
            liquidation_totals_verified: false,
            start_us,
            end_us,
            execution_count: executions.len(),
            unresolved_fee_executions: executions.iter().filter(|e| !e.fees_complete).count(),
            executions,
            orders: orders.into_values().collect(),
            unmatched_liquidations,
        })
    }
}

fn reconcile(
    pair: &Observations,
) -> Result<(ExecutionEvidence, BTreeMap<String, FeeTotals>, bool)> {
    if let (Some(ws), Some(rest)) = (&pair.ws, &pair.rest) {
        ensure!(
            ws.order_id == rest.order_id
                && ws.client_order_id == rest.client_order_id
                && ws.symbol == rest.symbol
                && ws.side == rest.side
                && ws.timestamp_ms == rest.timestamp_ms,
            "WS/REST execution identity or timestamp conflict"
        );
        for (name, a, b) in [
            ("quantity", &ws.quantity, &rest.quantity),
            ("price", &ws.price, &rest.price),
            ("rpnl", &ws.realized_pnl, &rest.realized_pnl),
        ] {
            ensure!(decimal(a)? == decimal(b)?, "WS/REST {name} conflict");
        }
        let signed = decimal(ws.signed_fee.as_deref().context("WS fee missing")?)?;
        let charge = decimal(rest.fee.as_deref().context("REST fee missing")?)?;
        let expected = signed.max(BigDecimal::from(0));
        ensure!(charge == expected, "WS/REST charged fee conflict");
        if charge != BigDecimal::from(0) {
            ensure!(
                ws.signed_fee_currency == rest.fee_currency,
                "WS/REST fee currency conflict"
            );
        }
    }
    let execution = pair
        .ws
        .as_ref()
        .or(pair.rest.as_ref())
        .context("empty execution observations")?
        .clone();
    let mut fees = BTreeMap::new();
    let complete = if let Some(ws) = &pair.ws {
        let signed = decimal(ws.signed_fee.as_deref().context("WS fee missing")?)?;
        let coin = ws
            .signed_fee_currency
            .as_ref()
            .context("WS fee currency missing")?;
        if !coin.is_empty() {
            let mut fee = zero_fees();
            if signed < BigDecimal::from(0) {
                fee.rebated = (-&signed).normalized().to_string();
            } else {
                fee.charged = signed.normalized().to_string();
            }
            fee.net_charge = Some(signed.normalized().to_string());
            fees.insert(coin.clone(), fee);
        }
        true
    } else {
        let rest = pair.rest.as_ref().context("REST observation missing")?;
        let charge = decimal(rest.fee.as_deref().context("REST fee missing")?)?;
        let rebate = decimal(
            rest.reported_rebate
                .as_deref()
                .context("REST rebate missing")?,
        )?;
        let complete = rebate == BigDecimal::from(0);
        let coin = rest
            .fee_currency
            .as_ref()
            .context("REST fee currency missing")?;
        if !coin.is_empty() {
            let mut fee = zero_fees();
            fee.charged = charge.normalized().to_string();
            fee.net_charge = complete.then(|| fee.charged.clone());
            fees.insert(coin.clone(), fee);
        }
        complete
    };
    // Validate exact numeric limits even for single-source evidence.
    for value in [
        &execution.quantity,
        &execution.price,
        &execution.realized_pnl,
    ] {
        decimal(value)?;
    }
    Ok((execution, fees, complete))
}

fn zero_fees() -> FeeTotals {
    FeeTotals {
        charged: "0".into(),
        rebated: "0".into(),
        net_charge: Some("0".into()),
    }
}

fn decimal(raw: &str) -> Result<BigDecimal> {
    ensure!(
        raw.len() <= 256,
        "reconciliation decimal exceeds 256 characters"
    );
    let value = BigDecimal::from_str(raw).context("invalid exact decimal")?;
    let (_, scale) = value.as_bigint_and_exponent();
    ensure!(
        (-256..=256).contains(&scale),
        "reconciliation decimal scale out of bounds"
    );
    Ok(value)
}

fn sum(a: &str, b: &str) -> Result<String> {
    Ok((decimal(a)? + decimal(b)?).normalized().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn liquidation() -> Value {
        json!({"channel":"LiquidationPosition", "data":{
            "portfolioId":"123", "exchangeType":"OKX", "orderId":"order-1", "liquidationEventId":"liq-1",
            "positionId":"pos-1", "sym":"OKX_PERP_BTC_USDT", "positionSide":"SHORT", "reduceQty":"1",
            "closedPnl":"-3", "liqFee":"0.5", "totalTradingFee":null, "createAt":900, "updateAt":1100
        }})
    }

    fn liquidation_fill() -> ExecutionRecord {
        let mut fill = record(
            "123", "tx-1", "order-1", false, "1", "10", 1000, "0.1", "USDT", "0",
        );
        fill.execution.symbol = "OKX_PERP_BTC_USDT".into();
        fill
    }

    #[test]
    fn liquidation_order_evidence_attributes_fills_but_never_adds_penalty_to_fees() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .observe_liquidation(&liquidation(), "123", "OKX")
            .unwrap();
        let mut duplicate = liquidation();
        duplicate["channel"] = json!("LiquidationPositionByUser");
        reconciler
            .observe_liquidation(&duplicate, "123", "OKX")
            .unwrap();
        reconciler.insert(liquidation_fill()).unwrap();
        let report = finish(reconciler);
        assert_eq!(
            report.executions[0].forced_close_reason,
            Some("exchange_forced_close:liquidation")
        );
        assert_eq!(report.orders[0].fees["USDT"].charged, "0.1");
        assert_eq!(
            report.orders[0]
                .liquidation
                .as_ref()
                .unwrap()
                .liquidation_fee_usdt,
            "0.5"
        );
        assert_eq!(report.strategy_attribution, "unassigned");
    }

    #[test]
    fn cancellation_and_other_portfolio_are_not_liquidation_fill_proof() {
        let mut reconciler = Reconciler::new(8).unwrap();
        let mut other = liquidation();
        other["data"]["portfolioId"] = json!("456");
        reconciler
            .observe_liquidation(&other, "123", "OKX")
            .unwrap();
        let mut cancellation = liquidation();
        cancellation["channel"] = json!("LiquidationEvent");
        reconciler
            .observe_liquidation(&cancellation, "123", "OKX")
            .unwrap();
        reconciler.insert(liquidation_fill()).unwrap();
        assert!(finish(reconciler).executions[0]
            .forced_close_reason
            .is_none());
    }

    #[test]
    fn conflicting_liquidation_evidence_or_fill_identity_is_rejected() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .observe_liquidation(&liquidation(), "123", "OKX")
            .unwrap();
        let mut conflict = liquidation();
        conflict["data"]["liqFee"] = json!("9");
        assert!(reconciler
            .observe_liquidation(&conflict, "123", "OKX")
            .is_err());
        let mut fill = liquidation_fill();
        fill.execution.side = "SELL".into();
        reconciler.insert(fill).unwrap();
        assert!(reconciler.finish(0, 10_000_000).is_err());
    }

    #[test]
    fn liquidation_snapshot_without_execution_never_invents_a_fill() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .observe_liquidation(&liquidation(), "123", "OKX")
            .unwrap();
        let report = finish(reconciler);
        assert_eq!(report.execution_count, 0);
        assert!(report.orders.is_empty());
        assert_eq!(report.unmatched_liquidations.len(), 1);
    }

    fn record(
        portfolio: &str,
        transaction_id: &str,
        order_id: &str,
        rest: bool,
        quantity: &str,
        price: &str,
        timestamp_ms: i64,
        fee: &str,
        fee_currency: &str,
        rebate: &str,
    ) -> ExecutionRecord {
        let row = json!({
            "portfolioId": portfolio,
            "exchangeType": "OKX",
            "businessType": "SPOT",
            "sym": "OKX_SPOT_BTC_USDT",
            "transactionId": transaction_id,
            "orderId": order_id,
            "clientOrderId": "external",
            "side": "BUY",
            "quantity": quantity,
            "price": price,
            "createAt": timestamp_ms.to_string(),
            "rpnl": "0",
            "tradingFee": fee,
            "tradingFeeCoin": fee_currency,
            "fee": fee,
            "feeCoin": fee_currency,
            "rebate": rebate,
            "rebateCoin": fee_currency,
        });
        ExecutionRecord {
            portfolio: portfolio.into(),
            exchange: "OKX".into(),
            execution: ExecutionEvidence::parse(&row, portfolio, "OKX", rest).unwrap(),
        }
    }

    fn finish(reconciler: Reconciler) -> ReconciliationReport {
        reconciler.finish(0, 10_000_000).unwrap()
    }

    #[test]
    fn ws_and_rest_same_execution_merge_without_doubling_quantity() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1.0", "10", 1000, "0.1", "USDT", "0",
            ))
            .unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", true, "1", "10", 1000, "0.1", "USDT", "0",
            ))
            .unwrap();

        let report = finish(reconciler);
        assert_eq!(report.execution_count, 1);
        assert_eq!(report.executions[0].execution.quantity, "1.0");
        assert!(report.executions[0].ws_observed && report.executions[0].rest_observed);
        assert_eq!(report.orders[0].quantity, "1");
        assert_eq!(report.orders[0].execution_count, 1);
    }

    #[test]
    fn same_source_duplicate_is_idempotent_but_conflict_errors() {
        let source = record(
            "123", "tx-1", "order-1", false, "1", "10", 1000, "0", "USDT", "0",
        );
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler.insert(source.clone()).unwrap();
        reconciler.insert(source.clone()).unwrap();
        assert_eq!(finish(reconciler).execution_count, 1);

        let mut conflicting = source;
        conflicting.execution.price = "11".into();
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "0", "USDT", "0",
            ))
            .unwrap();
        assert!(reconciler.insert(conflicting).is_err());
    }

    #[test]
    fn ws_negative_fee_is_a_rebate() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "-0.25", "USDT", "0",
            ))
            .unwrap();
        let report = finish(reconciler);
        let fee = &report.executions[0].fees["USDT"];
        assert_eq!(fee.charged, "0");
        assert_eq!(fee.rebated, "0.25");
        assert_eq!(fee.net_charge.as_deref(), Some("-0.25"));
    }

    #[test]
    fn rest_only_cumulative_rebate_remains_unresolved_and_is_not_summed() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", true, "1", "10", 1000, "1", "USDT", "0.5",
            ))
            .unwrap();
        let report = finish(reconciler);
        let execution = &report.executions[0];
        assert!(!execution.fees_complete);
        assert_eq!(execution.rest_reported_rebate.as_deref(), Some("0.5"));
        let fee = &execution.fees["USDT"];
        assert_eq!(fee.charged, "1");
        assert_eq!(fee.rebated, "0");
        assert!(fee.net_charge.is_none());
        assert!(report.orders[0].fees["USDT"].net_charge.is_none());
    }

    #[test]
    fn ws_rest_fee_and_currency_conflicts_are_rejected() {
        let mut fee_conflict = Reconciler::new(8).unwrap();
        fee_conflict
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "0.1", "USDT", "0",
            ))
            .unwrap();
        fee_conflict
            .insert(record(
                "123", "tx-1", "order-1", true, "1", "10", 1000, "0.2", "USDT", "0",
            ))
            .unwrap();
        assert!(fee_conflict.finish(0, 10_000_000).is_err());

        let mut currency_conflict = Reconciler::new(8).unwrap();
        currency_conflict
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "0.1", "USDT", "0",
            ))
            .unwrap();
        currency_conflict
            .insert(record(
                "123", "tx-1", "order-1", true, "1", "10", 1000, "0.1", "BTC", "0",
            ))
            .unwrap();
        assert!(currency_conflict.finish(0, 10_000_000).is_err());
    }

    #[test]
    fn order_totals_use_exact_decimal_quantity_and_fee_currency_buckets() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "0.1", "10", 1000, "0.01", "USDT", "0",
            ))
            .unwrap();
        reconciler
            .insert(record(
                "123", "tx-2", "order-1", false, "0.2", "10", 1001, "0.02", "BTC", "0",
            ))
            .unwrap();
        let report = finish(reconciler);
        let order = &report.orders[0];
        assert_eq!(order.execution_count, 2);
        assert_eq!(order.quantity, "0.3");
        assert_eq!(order.fees["USDT"].charged, "0.01");
        assert_eq!(order.fees["BTC"].charged, "0.02");
    }

    #[test]
    fn identical_transaction_ids_in_different_portfolios_do_not_merge() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "0", "USDT", "0",
            ))
            .unwrap();
        reconciler
            .insert(record(
                "456", "tx-1", "order-1", false, "1", "10", 1000, "0", "USDT", "0",
            ))
            .unwrap();
        let report = finish(reconciler);
        assert_eq!(report.execution_count, 2);
        assert_eq!(report.orders.len(), 2);
    }

    #[test]
    fn counterpart_outside_window_still_exposes_timestamp_conflict() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "0", "USDT", "0",
            ))
            .unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", true, "1", "10", 2000, "0", "USDT", "0",
            ))
            .unwrap();
        assert!(reconciler.finish(1_000_000, 1_500_000).is_err());
    }

    #[test]
    fn execution_capacity_limit_is_enforced() {
        let mut reconciler = Reconciler::new(1).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1", "10", 1000, "0", "USDT", "0",
            ))
            .unwrap();
        assert!(reconciler
            .insert(record(
                "123", "tx-2", "order-2", false, "1", "10", 1000, "0", "USDT", "0"
            ))
            .is_err());
    }

    #[test]
    fn extreme_decimal_exponent_is_rejected_during_reconciliation() {
        let mut reconciler = Reconciler::new(8).unwrap();
        reconciler
            .insert(record(
                "123", "tx-1", "order-1", false, "1e257", "10", 1000, "0", "USDT", "0",
            ))
            .unwrap();
        assert!(reconciler.finish(0, 10_000_000).is_err());
    }
}
