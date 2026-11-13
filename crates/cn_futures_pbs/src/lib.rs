//! One native L2 message and IPC publisher, with source-specific ingress only.
pub mod config;
pub mod ipc;
pub mod msg;
pub mod sources;

pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
