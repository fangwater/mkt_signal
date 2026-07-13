use log::warn;
use std::sync::OnceLock;

pub(crate) fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

pub fn enable_ipc_fast_poll() -> bool {
    for name in ["enable_ipc_fast_poll", "ENABLE_IPC_FAST_POLL"] {
        if let Ok(value) = std::env::var(name) {
            if let Some(enabled) = parse_bool_env(&value) {
                return enabled;
            }
            warn!(
                "invalid {}='{}', treating enable_ipc_fast_poll as disabled",
                name, value
            );
            return false;
        }
    }
    false
}

pub(crate) fn fast_poll_hot_path_mode() -> bool {
    static FAST_POLL: OnceLock<bool> = OnceLock::new();
    *FAST_POLL.get_or_init(enable_ipc_fast_poll)
}

pub(crate) fn suppress_pre_submit_hot_path_logs() -> bool {
    fast_poll_hot_path_mode()
}

#[cfg(test)]
mod tests {
    use super::{enable_ipc_fast_poll, parse_bool_env};
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn parse_bool_env_accepts_common_values() {
        assert_eq!(parse_bool_env("1"), Some(true));
        assert_eq!(parse_bool_env("true"), Some(true));
        assert_eq!(parse_bool_env("on"), Some(true));
        assert_eq!(parse_bool_env("0"), Some(false));
        assert_eq!(parse_bool_env("false"), Some(false));
        assert_eq!(parse_bool_env("off"), Some(false));
        assert_eq!(parse_bool_env("maybe"), None);
    }

    #[test]
    fn enable_ipc_fast_poll_defaults_off() {
        let _guard = env_test_lock();
        std::env::remove_var("ENABLE_IPC_FAST_POLL");
        std::env::remove_var("enable_ipc_fast_poll");
        assert!(!enable_ipc_fast_poll());
    }

    #[test]
    fn enable_ipc_fast_poll_honors_upper_and_lower_case_env_names() {
        let _guard = env_test_lock();
        std::env::set_var("ENABLE_IPC_FAST_POLL", "0");
        assert!(!enable_ipc_fast_poll());
        std::env::remove_var("ENABLE_IPC_FAST_POLL");

        std::env::set_var("enable_ipc_fast_poll", "yes");
        assert!(enable_ipc_fast_poll());
        std::env::remove_var("enable_ipc_fast_poll");
    }
}
