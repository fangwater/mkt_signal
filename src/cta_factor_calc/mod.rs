//! Named, on-demand factor calculations shared by independent Perp/Spread workers.
pub mod client;
pub mod config;
pub mod engine;
pub mod hourly_history;
pub(crate) mod positions;
pub use positions::migration;
pub mod runtime;
pub mod symbol_filter;

mod catalog;
pub use catalog::requires_amount_thresholds;
mod hourly_nq_seed;
mod normalize;
mod thresholds;
