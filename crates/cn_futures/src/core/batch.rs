use super::*;
use std::collections::HashSet;

#[derive(Debug, Serialize)]
pub struct CancelReceipt {
    pub key: OrderKey,
    pub outcome: ChildOutcome,
    pub error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct BatchCancelReceipt {
    pub children: Vec<CancelReceipt>,
    pub all_submitted_locally: bool,
}

impl<T: Trader> SingleLoop<T> {
    /// Native batches are independent orders. Reserve every child before any send;
    /// a false/unknown native batch send retains every possibly sent reservation.
    pub fn submit_batch_with(
        &mut self,
        intents: Vec<Intent>,
        mut after_native: impl FnMut(&PreparedOrder) -> Result<()>,
    ) -> Result<SubmitReceipt> {
        ensure!(self.execute, "read-only runtime: submissions disabled");
        ensure!(
            !intents.is_empty() && intents.len() <= 16,
            "native batch requires 1..16 orders"
        );
        let mut prepared: Vec<(usize, PreparedOrder)> = vec![];
        let preparation = (|| -> Result<()> {
            for (parent, intent) in intents.into_iter().enumerate() {
                for child in self.core.plan(intent)? {
                    ensure!(
                        prepared.len() < 16,
                        "planned batch exceeds 16 native children"
                    );
                    let reference = self.trader.allocate_reference(self.core.next_ref)?;
                    ensure!(
                        reference >= self.core.next_ref
                            && reference <= self.core.scope.reference_limit,
                        "invalid SDK allocated reference"
                    );
                    self.core.next_ref = reference;
                    prepared.push((parent, self.core.prepare(child)?));
                }
            }
            Ok(())
        })();
        if let Err(error) = preparation {
            for (_, order) in &prepared {
                self.core.reject(&order.key)?;
            }
            return Err(error.context("no batch child was submitted"));
        }
        let requests: Vec<_> = prepared.iter().map(|(_, order)| *order).collect();
        // Evidence follows the SDK call so publishing never delays the batch.
        let result = self.trader.insert_batch(&requests);
        // Record every child even after one failure; any failure blocks trading.
        let mut evidence_failed = false;
        for order in &requests {
            evidence_failed |= after_native(order).is_err();
        }
        if evidence_failed {
            self.core
                .block("batch submission evidence unavailable after SDK call");
        }
        let all_submitted_locally = matches!(result, Ok(BatchSend::SubmittedLocally));
        if result.is_err() {
            self.core
                .block("native batch submission uncertain; retain all reservations and reconcile");
        }
        let mut children = vec![];
        for (index, (parent_index, order)) in prepared.into_iter().enumerate() {
            let (outcome, error) = match &result {
                Ok(BatchSend::SubmittedLocally) => (ChildOutcome::SubmittedLocally, None),
                Ok(BatchSend::RejectedBeforeSend {
                    index: failed,
                    code,
                }) => {
                    self.core.reject(&order.key)?;
                    if index == *failed {
                        (
                            ChildOutcome::RejectedLocally,
                            Some(format!("native local rejection {code}")),
                        )
                    } else {
                        (ChildOutcome::NotSent, None)
                    }
                }
                Err(error) => (ChildOutcome::Uncertain, Some(error.to_string())),
            };
            children.push(ChildReceipt {
                parent_index,
                key: order.key,
                offset: order.intent.offset,
                lots: order.intent.lots,
                outcome,
                error,
            });
        }
        Ok(SubmitReceipt {
            children: children.into(),
            all_submitted_locally,
        })
    }

    pub fn cancel_batch_with(
        &mut self,
        keys: Vec<OrderKey>,
        after_native: impl FnMut(&PreparedCancel) -> Result<()>,
    ) -> Result<BatchCancelReceipt> {
        self.cancel_batch_with_route(keys, None, after_native)
    }
    pub fn cancel_batch_with_route(
        &mut self,
        keys: Vec<OrderKey>,
        route: Option<ExecutionRoute>,
        mut after_native: impl FnMut(&PreparedCancel) -> Result<()>,
    ) -> Result<BatchCancelReceipt> {
        ensure!(self.execute, "read-only runtime: cancellations disabled");
        if let Some(route) = route {
            route.validate()?;
        }
        let unique: HashSet<_> = keys.iter().collect();
        ensure!(
            !keys.is_empty() && keys.len() <= 16 && unique.len() == keys.len(),
            "native cancel batch requires 1..16 unique keys"
        );
        let mut requests = vec![];
        for key in &keys {
            match self.core.prepare_cancel(key) {
                Ok(mut order) => {
                    if route.is_some() {
                        order.execution_route = route;
                    }
                    requests.push(order);
                }
                Err(error) => {
                    for order in &requests {
                        self.core.cancel_failed(&order.key);
                    }
                    return Err(error.context("no batch cancellation was submitted"));
                }
            }
        }
        let result = self.trader.cancel_batch(&requests);
        // Record every child even after one failure; any failure blocks trading.
        let mut evidence_failed = false;
        for order in &requests {
            evidence_failed |= after_native(order).is_err();
        }
        if evidence_failed {
            self.core
                .block("batch cancellation evidence unavailable after SDK call");
        }
        let all_submitted_locally = matches!(result, Ok(BatchSend::SubmittedLocally));
        if result.is_err() {
            self.core
                .block("native batch cancellation uncertain; reconcile");
        }
        let mut children = vec![];
        for (index, key) in keys.into_iter().enumerate() {
            let (outcome, error) = match &result {
                Ok(BatchSend::SubmittedLocally) => (ChildOutcome::SubmittedLocally, None),
                Ok(BatchSend::RejectedBeforeSend {
                    index: failed,
                    code,
                }) => {
                    self.core.cancel_failed(&key);
                    if index == *failed {
                        (
                            ChildOutcome::RejectedLocally,
                            Some(format!("native local cancel rejection {code}")),
                        )
                    } else {
                        (ChildOutcome::NotSent, None)
                    }
                }
                Err(error) => (ChildOutcome::Uncertain, Some(error.to_string())),
            };
            children.push(CancelReceipt {
                key,
                outcome,
                error,
            });
        }
        Ok(BatchCancelReceipt {
            children,
            all_submitted_locally,
        })
    }
}
