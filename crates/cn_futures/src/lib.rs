//! Single-owner domestic-futures execution. The CTP SDK owns its transport threads;
//! strategy, risk, order reservations and position changes stay on the calling thread.
pub mod config;
pub mod core;
pub mod ctp;
pub mod deadline;
pub mod model;
pub mod persist;
pub mod persistence;
pub mod placement;
pub mod poll;
#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
mod position_ipc;
pub mod runtime;
pub mod yida;
