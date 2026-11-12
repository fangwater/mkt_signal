//! MdApi ingress consumed directly by the trader's single business loop.
use super::{native::Native, Config, Session, TickRecord};
use crate::placement::Placement;
use anyhow::{bail, ensure, Context, Result};
use std::ffi::CString;
use std::fs::File;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[allow(clippy::large_enum_variant)] // Tick delivery stays inline without a per-tick allocation.
pub enum FeedEvent {
    Connected { generation: u64 },
    Disconnected { reason: i32 },
    Ready { generation: u64 },
    Tick(TickRecord),
}

/// One owner, no output writer and no inter-process JSON hop. Create/Init happen
/// before polling starts. The vendor handles reconnects; a lifecycle timeout
/// fails closed instead of Release/Init on the running business thread.
pub struct Feed {
    native: Native,
    session: Session,
    broker: CString,
    user: CString,
    password: CString,
    next_request: i32,
    progress: Instant,
    timeout: Duration,
    _flow: File,
}
impl Feed {
    pub fn new(config: &Config, placement: &Placement) -> Result<Self> {
        config.validate()?;
        let flow = crate::runtime::lock_flow(&config.flow_directory)?;
        let user = super::native::credential(&config.user_id_env, 16)?;
        let password = super::native::credential(&config.password_env, 41)?;
        let native = placement.on_market_data_sdk(|| {
            let native = Native::new(config, placement.market_data_callback()?)?;
            native.start(config)?;
            Ok(native)
        })?;
        Ok(Self {
            native,
            session: Session::new(config),
            broker: CString::new(config.broker_id.as_str())?,
            user,
            password,
            next_request: 0,
            progress: Instant::now(),
            timeout: Duration::from_millis(config.response_timeout_ms),
            _flow: flow,
        })
    }
    pub fn ready(&self) -> bool {
        self.session.ready()
            && !self.native.failed.load(Ordering::Acquire)
            && !self.native.placement_fault.load(Ordering::Acquire)
    }
    pub fn has_backlog(&self) -> bool {
        !self.native.events.is_empty()
    }
    pub fn generation(&self) -> u64 {
        self.session.generation
    }
    pub fn poll(&mut self, now: Instant) -> Result<Option<FeedEvent>> {
        ensure!(
            !self.native.placement_fault.load(Ordering::Acquire),
            "MD SDK CPU/NUMA binding failed"
        );
        ensure!(
            !self.native.failed.load(Ordering::Acquire),
            "MD ingress overflow; stream incomplete"
        );
        let Ok((event, received_us)) = self.native.events.try_recv() else {
            ensure!(
                self.ready() || now.saturating_duration_since(self.progress) < self.timeout,
                "MD lifecycle timeout; supervisor restart required"
            );
            return Ok(None);
        };
        let output = match event.kind {
            1 => {
                self.next_request = self
                    .next_request
                    .checked_add(1)
                    .context("MD request IDs exhausted")?;
                self.session.connected(self.next_request)?;
                self.progress = now;
                self.native
                    .login(self.next_request, &self.broker, &self.user, &self.password)?;
                FeedEvent::Connected {
                    generation: self.session.generation,
                }
            }
            2 => {
                self.session.disconnect();
                self.progress = now;
                FeedEvent::Disconnected {
                    reason: event.error,
                }
            }
            4 => {
                if self.session.login(&event)? {
                    self.progress = now;
                    self.native.subscribe()?;
                }
                return Ok(None);
            }
            21 => {
                self.session.subscribed(&event)?;
                self.progress = now;
                if !self.ready() {
                    return Ok(None);
                }
                FeedEvent::Ready {
                    generation: self.session.generation,
                }
            }
            22 => {
                let venue = self.session.tick_venue(&event)?;
                FeedEvent::Tick(TickRecord::capture(
                    &event,
                    received_us,
                    self.session.generation,
                    venue,
                ))
            }
            16 => bail!("MD error {}; stop before retrying credentials", event.error),
            _ => return Ok(None),
        };
        Ok(Some(output))
    }
}
