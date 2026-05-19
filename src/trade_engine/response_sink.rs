use crate::trade_engine::query_response_handle::QueryExecOutcome;
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use log::warn;
use rtrb::{Producer, PushError};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use tokio_util::sync::CancellationToken;

const RESPONSE_SPSC_FULL_WARN_INTERVAL: u64 = 100_000;

#[derive(Clone)]
pub(crate) struct TradeResponseSink {
    producer: Rc<RefCell<Producer<TradeExecOutcome>>>,
    full_count: Rc<Cell<u64>>,
    shutdown: CancellationToken,
}

impl TradeResponseSink {
    pub(crate) fn new(producer: Producer<TradeExecOutcome>, shutdown: CancellationToken) -> Self {
        Self {
            producer: Rc::new(RefCell::new(producer)),
            full_count: Rc::new(Cell::new(0)),
            shutdown,
        }
    }

    pub(crate) fn send(&self, mut out: TradeExecOutcome) -> Result<(), ()> {
        loop {
            if self.shutdown.is_cancelled() {
                return Err(());
            }
            let push_result = { self.producer.borrow_mut().push(out) };
            match push_result {
                Ok(()) => return Ok(()),
                Err(PushError::Full(returned)) => {
                    out = returned;
                    let full_count = self.full_count.get().saturating_add(1);
                    self.full_count.set(full_count);
                    if full_count % RESPONSE_SPSC_FULL_WARN_INTERVAL == 1 {
                        warn!(
                            "TE async trade_resp SPSC full; busy retrying client_order_id={} full_count={}",
                            out.client_order_id, full_count
                        );
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct QueryResponseSink {
    producer: Rc<RefCell<Producer<QueryExecOutcome>>>,
    full_count: Rc<Cell<u64>>,
    shutdown: CancellationToken,
}

impl QueryResponseSink {
    pub(crate) fn new(producer: Producer<QueryExecOutcome>, shutdown: CancellationToken) -> Self {
        Self {
            producer: Rc::new(RefCell::new(producer)),
            full_count: Rc::new(Cell::new(0)),
            shutdown,
        }
    }

    pub(crate) fn send(&self, mut out: QueryExecOutcome) -> Result<(), ()> {
        loop {
            if self.shutdown.is_cancelled() {
                return Err(());
            }
            let push_result = { self.producer.borrow_mut().push(out) };
            match push_result {
                Ok(()) => return Ok(()),
                Err(PushError::Full(returned)) => {
                    out = returned;
                    let full_count = self.full_count.get().saturating_add(1);
                    self.full_count.set(full_count);
                    if full_count % RESPONSE_SPSC_FULL_WARN_INTERVAL == 1 {
                        warn!(
                            "TE async query_resp SPSC full; busy retrying client_query_id={} full_count={}",
                            out.client_query_id, full_count
                        );
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }
}
