use std::time::Duration;

pub(crate) const MAX_DRAIN_PER_CHANNEL: usize = 256;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PollStats {
    pub(crate) received: usize,
    pub(crate) receive_error: bool,
}

impl PollStats {
    pub(crate) fn record_received(&mut self) {
        self.received += 1;
    }

    pub(crate) fn record_error(&mut self) {
        self.receive_error = true;
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.received += other.received;
        self.receive_error |= other.receive_error;
    }
}

pub(crate) fn idle_sleep() -> Duration {
    Duration::from_millis(1)
}

pub(crate) fn receive_error_sleep() -> Duration {
    Duration::from_millis(200)
}
