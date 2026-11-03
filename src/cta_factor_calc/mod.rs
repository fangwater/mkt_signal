//! Named, on-demand factor calculations shared by independent Perp/Spread workers.
pub mod client;
pub mod config;
pub mod engine;
pub(crate) mod positions;
pub mod runtime;

mod catalog;
mod normalize;
mod thresholds;
