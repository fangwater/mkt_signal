use std::time::Duration;

pub const NON_FAST_POLL_IDLE_SLEEP: Duration = Duration::from_millis(1);

pub fn enable_ipc_fast_poll() -> bool {
    runtime_common::socket_tuning::ipc_fast_poll_enabled()
}

fn idle_sleep_duration(fast_poll: bool) -> Option<Duration> {
    (!fast_poll).then_some(NON_FAST_POLL_IDLE_SLEEP)
}

pub async fn idle_poll_wait(fast_poll: bool) {
    if let Some(duration) = idle_sleep_duration(fast_poll) {
        tokio::time::sleep(duration).await;
    } else {
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{idle_sleep_duration, NON_FAST_POLL_IDLE_SLEEP};

    #[test]
    fn idle_poll_sleeps_only_when_fast_poll_is_off() {
        assert_eq!(idle_sleep_duration(false), Some(NON_FAST_POLL_IDLE_SLEEP));
        assert_eq!(idle_sleep_duration(true), None);
    }
}
