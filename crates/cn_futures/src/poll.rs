//! Business-loop idle policy, matching crypto pre_trade's fast-poll split:
//! `busy` for an isolated core (spin, then yield the slice; never sleep) and
//! `sleep` for a shared host. Sleep parks on the SDK callback queues, so market
//! data, order and trade callbacks wake the loop at once; `idle_us` only bounds
//! inputs that cannot wake it (network tasks, stdin commands, deadlines).
use anyhow::{ensure, Result};
use crossbeam_channel::Select;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const MAX_SLEEP_US: u64 = 100_000;
const DEFAULT_SPIN: u32 = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum PollMode {
    /// Spin `spin` idle iterations, then `sched_yield` once.
    Busy {
        #[serde(default = "default_spin")]
        spin: u32,
    },
    /// Park after an iteration without work until a registered SDK queue has an
    /// event, or at most `idle_us`.
    Sleep { idle_us: u64 },
}
fn default_spin() -> u32 {
    DEFAULT_SPIN
}
impl Default for PollMode {
    fn default() -> Self {
        Self::Busy { spin: DEFAULT_SPIN }
    }
}
impl PollMode {
    pub fn validate(self, max_sleep_us: u64) -> Result<()> {
        match self {
            Self::Busy { spin } => ensure!(spin > 0, "busy poll requires spin > 0"),
            Self::Sleep { idle_us } => ensure!(
                (1..=max_sleep_us).contains(&idle_us),
                "sleep poll requires idle_us in 1..={max_sleep_us}"
            ),
        }
        Ok(())
    }
}

pub struct Idle {
    mode: PollMode,
    spins: u32,
}
impl Idle {
    pub fn new(mode: PollMode) -> Self {
        Self { mode, spins: 0 }
    }
    /// The iteration did work: restart the spin budget.
    pub fn busy(&mut self) {
        self.spins = 0;
    }
    /// The iteration found nothing to do.
    /// Called after an iteration without work. `wake_on` registers the queues
    /// whose events end a sleep; it runs only in sleep mode, so busy polling
    /// pays nothing. Nothing is consumed: the loop drains as usual.
    pub fn wait<'a>(&mut self, wake_on: impl FnOnce(&mut Select<'a>)) {
        match self.mode {
            PollMode::Busy { spin } => {
                if self.spins < spin {
                    self.spins += 1;
                    std::hint::spin_loop();
                } else {
                    self.spins = 0;
                    std::thread::yield_now();
                }
            }
            PollMode::Sleep { idle_us } => {
                let mut select = Select::new();
                wake_on(&mut select);
                // Ready (or disconnected) queues return at once; none means a plain sleep.
                let _ = select.ready_timeout(Duration::from_micros(idle_us));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_with_defaults_and_bounds() {
        #[derive(Deserialize)]
        struct Wrapper {
            poll: PollMode,
        }
        let busy: Wrapper = toml::from_str("poll = { mode = \"busy\" }").unwrap();
        assert_eq!(busy.poll, PollMode::Busy { spin: 2048 });
        let sleep: Wrapper =
            toml::from_str("poll = { mode = \"sleep\", idle_us = 64000 }").unwrap();
        assert_eq!(sleep.poll, PollMode::Sleep { idle_us: 64000 });
        sleep.poll.validate(MAX_SLEEP_US).unwrap();
        assert!(PollMode::Sleep { idle_us: 0 }
            .validate(MAX_SLEEP_US)
            .is_err());
        assert!(PollMode::Sleep { idle_us: 2000 }.validate(1000).is_err());
        assert!(PollMode::Busy { spin: 0 }.validate(MAX_SLEEP_US).is_err());
        assert!(toml::from_str::<Wrapper>("poll = { mode = \"busy\", idle_us = 1 }").is_err());
    }

    #[test]
    fn sleep_wakes_on_a_queued_event_and_otherwise_waits_out_idle_us() {
        let (sender, events) = crossbeam_channel::bounded::<u8>(4);
        let (_quiet_sender, quiet) = crossbeam_channel::bounded::<u64>(4);
        let mut idle = Idle::new(PollMode::Sleep { idle_us: 100_000 });
        let waker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            sender.send(7).unwrap();
            sender
        });
        let started = std::time::Instant::now();
        idle.wait(|select| {
            select.recv(&quiet);
            select.recv(&events);
        });
        assert!(
            started.elapsed() < Duration::from_millis(80),
            "woken by the event"
        );
        assert_eq!(events.try_recv(), Ok(7), "waiting consumes nothing");
        let _sender = waker.join().unwrap();

        let started = std::time::Instant::now();
        let mut idle = Idle::new(PollMode::Sleep { idle_us: 20_000 });
        idle.wait(|select| {
            select.recv(&events);
        });
        assert!(
            started.elapsed() >= Duration::from_millis(20),
            "no event: full idle_us"
        );
    }

    #[test]
    fn busy_never_registers_queues() {
        let mut idle = Idle::new(PollMode::Busy { spin: 1 });
        idle.wait(|_| panic!("busy polling must not build a wait set"));
        idle.wait(|_| panic!("busy polling must not build a wait set"));
    }
}
