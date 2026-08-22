use crate::query_response_handle::{publish_query_response, QueryExecOutcome};
use crate::trade_response_handle::{publish_trade_response, TradeExecOutcome};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use ipc_common::iceoryx_publisher::QUERY_RESP_PAYLOAD;
use std::rc::Rc;

#[derive(Clone)]
pub(crate) struct TradeResponseSink {
    publisher: Rc<Publisher<ipc::Service, [u8; 64], ()>>,
}

impl TradeResponseSink {
    pub(crate) fn new(publisher: Publisher<ipc::Service, [u8; 64], ()>) -> Self {
        Self {
            publisher: Rc::new(publisher),
        }
    }

    pub(crate) fn send(&self, out: TradeExecOutcome) -> Result<(), ()> {
        publish_trade_response(&self.publisher, out);
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct QueryResponseSink {
    publisher: Rc<Publisher<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()>>,
}

impl QueryResponseSink {
    pub(crate) fn new(publisher: Publisher<ipc::Service, [u8; QUERY_RESP_PAYLOAD], ()>) -> Self {
        Self {
            publisher: Rc::new(publisher),
        }
    }

    pub(crate) fn send(&self, out: QueryExecOutcome) -> Result<(), ()> {
        publish_query_response(&self.publisher, out);
        Ok(())
    }
}
