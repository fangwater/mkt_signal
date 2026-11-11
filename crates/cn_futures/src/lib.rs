//! Single-owner domestic-futures execution. The CTP SDK owns its transport threads;
//! strategy, risk, order reservations and position changes stay on the calling thread.
mod compact;
pub mod config;
pub mod core;
pub mod ctp;
pub mod deadline;
pub mod model;
pub mod persistence;
pub mod runtime;
pub mod yida;
