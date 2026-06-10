use bytes::Bytes;
use tokio::sync::mpsc;

pub mod binance_basic_account_event_parser;
pub mod bitget_account_event_parser;
pub mod bybit_account_event_parser;
pub mod gate_account_event_parser;
pub mod okex_account_event_parser;

pub trait Parser: Send {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize;
}
