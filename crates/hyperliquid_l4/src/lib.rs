//! Node-side Hyperliquid L4 -> exact, order-level binary batches.
//! No public L2 feed can synthesize these events. All timestamps here are microseconds.
pub mod book;
pub mod decode;
#[cfg(feature = "ipc")]
pub mod ipc;
#[cfg(feature = "runtime")]
pub mod runtime;
pub mod wire;

pub use decode::{Batch, Decoder};
pub use wire::{Coin, Event, Header, Kind, Op, Side, DECIMAL_SCALE};
