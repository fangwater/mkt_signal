use bytes::Bytes;

pub mod binance_basic_account_event_parser;
pub mod bitget_account_event_parser;
pub mod bybit_account_event_parser;
pub mod gate_account_event_parser;
pub mod okex_account_event_parser;

pub trait AccountEventSink {
    fn emit(&self, msg: Bytes) -> bool;
}

pub trait Parser: Send {
    fn parse<S: AccountEventSink>(&self, msg: Bytes, sink: &S) -> usize;
}

#[cfg(test)]
pub(crate) mod test_sink {
    use super::AccountEventSink;
    use bytes::Bytes;
    use std::cell::RefCell;

    pub(crate) struct TestAccountEventSink {
        msgs: RefCell<Vec<Bytes>>,
    }

    impl TestAccountEventSink {
        pub(crate) fn new() -> Self {
            Self {
                msgs: RefCell::new(Vec::new()),
            }
        }

        pub(crate) fn recv(&self) -> Option<Bytes> {
            let mut msgs = self.msgs.borrow_mut();
            if msgs.is_empty() {
                None
            } else {
                Some(msgs.remove(0))
            }
        }
    }

    impl AccountEventSink for TestAccountEventSink {
        fn emit(&self, msg: Bytes) -> bool {
            self.msgs.borrow_mut().push(msg);
            true
        }
    }
}
