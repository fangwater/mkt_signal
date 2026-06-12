use bytes::Bytes;
use log::error;
use tokio::sync::broadcast;

pub type RawAccountMessageHandler = Box<dyn FnMut(Bytes) + Send + 'static>;

#[inline]
pub fn forward_raw_account_message(
    tx: &broadcast::Sender<Bytes>,
    handler: Option<&mut RawAccountMessageHandler>,
    bytes: Bytes,
    error_context: &str,
) -> bool {
    if let Some(handler) = handler {
        handler(bytes);
        return true;
    }

    if let Err(err) = tx.send(bytes) {
        error!("{}: {}", error_context, err);
        return false;
    }

    true
}
