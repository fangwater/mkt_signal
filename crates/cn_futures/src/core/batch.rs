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
        now: Instant,
        mut audit: impl FnMut(&PreparedOrder) -> Result<()>,
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
                    prepared.push((parent, self.core.prepare(child, now)?));
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
        let requests: Vec<_> = prepared.iter().map(|(_, order)| order.clone()).collect();
        let mut audit_error = None;
        for order in &requests {
            if let Err(error) = audit(order) {
                audit_error = Some(error.to_string());
                break;
            }
        }
        let result = if audit_error.is_none() {
            Some(self.trader.insert_batch(&requests))
        } else {
            self.core.block("batch audit failed before SDK call");
            None
        };
        let all_submitted_locally = matches!(result, Some(Ok(BatchSend::SubmittedLocally)));
        if matches!(result, Some(Err(_))) {
            self.core
                .block("native batch submission uncertain; retain all reservations and reconcile");
        }
        let mut children = vec![];
        for (index, (parent_index, order)) in prepared.into_iter().enumerate() {
            let (outcome, error) = match &result {
                Some(Ok(BatchSend::SubmittedLocally)) => (ChildOutcome::SubmittedLocally, None),
                Some(Ok(BatchSend::RejectedBeforeSend {
                    index: failed,
                    code,
                })) => {
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
                Some(Err(error)) => (ChildOutcome::Uncertain, Some(error.to_string())),
                None => {
                    self.core.reject(&order.key)?;
                    (ChildOutcome::NotSent, audit_error.clone())
                }
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
            children,
            all_submitted_locally,
        })
    }

    pub fn cancel_batch_with(
        &mut self,
        keys: Vec<OrderKey>,
        now: Instant,
        audit: impl FnMut(&PreparedCancel) -> Result<()>,
    ) -> Result<BatchCancelReceipt> {
        self.cancel_batch_with_route(keys, None, now, audit)
    }
    pub fn cancel_batch_with_route(
        &mut self,
        keys: Vec<OrderKey>,
        route: Option<ExecutionRoute>,
        now: Instant,
        mut audit: impl FnMut(&PreparedCancel) -> Result<()>,
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
            match self.core.prepare_cancel(key, now) {
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
        let mut audit_error = None;
        for order in &requests {
            if let Err(error) = audit(order) {
                audit_error = Some(error.to_string());
                break;
            }
        }
        let result = if audit_error.is_none() {
            Some(self.trader.cancel_batch(&requests))
        } else {
            self.core
                .block("batch cancellation audit failed before SDK call");
            None
        };
        let all_submitted_locally = matches!(result, Some(Ok(BatchSend::SubmittedLocally)));
        if matches!(result, Some(Err(_))) {
            self.core
                .block("native batch cancellation uncertain; reconcile");
        }
        let mut children = vec![];
        for (index, key) in keys.into_iter().enumerate() {
            let (outcome, error) = match &result {
                Some(Ok(BatchSend::SubmittedLocally)) => (ChildOutcome::SubmittedLocally, None),
                Some(Ok(BatchSend::RejectedBeforeSend {
                    index: failed,
                    code,
                })) => {
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
                Some(Err(error)) => (ChildOutcome::Uncertain, Some(error.to_string())),
                None => {
                    self.core.cancel_failed(&key);
                    (ChildOutcome::NotSent, audit_error.clone())
                }
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
