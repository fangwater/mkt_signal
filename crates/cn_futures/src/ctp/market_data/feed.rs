//! MdApi ingress consumed directly by the trader's single business loop.
use super::{dominant, native::Native, Config, Session, Subscription, TickRecord};
use crate::placement::Placement;
use anyhow::{bail, ensure, Context, Result};
use std::ffi::CString;
use std::fs::File;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[allow(clippy::large_enum_variant)] // Tick delivery stays inline without a per-tick allocation.
pub enum FeedEvent {
    Connected {
        generation: u64,
    },
    Disconnected {
        reason: i32,
    },
    Ready {
        generation: u64,
    },
    DominantSelected(dominant::List),
    DominantUnavailable {
        error: anyhow::Error,
        trading_day: String,
    },
    Tick(TickRecord),
}

/// One owner, no output writer and no inter-process JSON hop. Create/Init happen
/// before polling starts. Combined runtimes let the vendor reconnect an offline
/// front. A timeout during login or subscription still fails closed.
pub struct Feed {
    native: Native,
    session: Session,
    broker: CString,
    user: CString,
    password: CString,
    next_request: i32,
    progress: Instant,
    timeout: Duration,
    wait_for_front: bool,
    dominant: Option<dominant::Lookup>,
    baseline: Vec<Subscription>,
    /// Bucket and sequence of the tick most recently returned by `poll`.
    last_quote: Option<(usize, u64)>,
    _flow: File,
}
impl Feed {
    pub fn new(config: &Config, placement: &Placement) -> Result<Self> {
        Self::create(config, placement, true, || {
            placement.on_market_data_sdk(|| {
                let native = Native::new(config, placement.market_data_callback()?)?;
                native.start(config)?;
                Ok(native)
            })
        })
    }
    /// Independent IPC publisher: the SDK uses the standalone MD SDK domain.
    pub fn standalone(config: &Config, placement: &Placement) -> Result<Self> {
        Self::create(config, placement, false, || {
            placement.on_sdk(|| {
                let native = Native::new(config, placement.callback())?;
                native.start(config)?;
                Ok(native)
            })
        })
    }
    fn create(
        config: &Config,
        placement: &Placement,
        wait_for_front: bool,
        create_native: impl FnOnce() -> Result<Native>,
    ) -> Result<Self> {
        config.validate()?;
        let flow = crate::runtime::lock_flow(&config.flow_directory)?;
        let user = super::native::credential(&config.user_id_env, 16)?;
        let password = super::native::credential(&config.password_env, 41)?;
        let dominant = config
            .shfe_dominant_url
            .as_deref()
            .map(|url| {
                placement.on_background(|| {
                    dominant::Lookup::new(
                        url,
                        Duration::from_millis(config.reconnect_backoff_ms.max(15_000)),
                    )
                })
            })
            .transpose()?;
        let native = create_native()?;
        Ok(Self {
            native,
            session: Session::new(config),
            broker: CString::new(config.broker_id.as_str())?,
            user,
            password,
            next_request: 0,
            progress: Instant::now(),
            timeout: Duration::from_millis(config.response_timeout_ms),
            wait_for_front,
            dominant,
            baseline: config.symbols.clone(),
            last_quote: None,
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
    pub(crate) fn waiting_for_front(&self) -> bool {
        self.wait_for_front && !self.session.connected
    }
    /// Queued MD events, for latency statistics only.
    pub fn queued(&self) -> usize {
        self.native.events.len()
    }
    /// True when a newer quote for the contract of the tick just returned by
    /// `poll` is already queued. Other contracts' quotes are unrelated to a
    /// decision on this one, so they do not close its dispatch gate.
    pub fn newer_contract_quote_queued(&self) -> bool {
        self.last_quote.is_some_and(|(bucket, sequence)| {
            self.native.latest[bucket].load(std::sync::atomic::Ordering::Acquire) > sequence
        })
    }
    pub fn generation(&self) -> u64 {
        self.session.generation
    }
    /// A ready connection can extend its subscriptions without resetting its generation.
    pub(crate) fn subscribe_contracts(
        &mut self,
        contracts: &[(crate::model::Venue, &str)],
        now: Instant,
    ) -> Result<bool> {
        let missing = self.session.add_contracts(contracts)?;
        if !missing.is_empty() {
            self.native.set_symbols(&missing)?;
            self.native.subscribe()?;
            self.progress = now;
        }
        Ok(self.ready())
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
        self.last_quote = None;
        let Ok((event, received_us, sequence)) = self.native.events.try_recv() else {
            if let Some(lookup) = &mut self.dominant {
                if let Some(result) = lookup.poll(now)? {
                    return match result {
                        Ok(list) => {
                            self.session
                                .install_dominant(&list.trading_day, &list.symbols)?;
                            self.native.set_symbols(&list.symbols)?;
                            self.native.subscribe()?;
                            self.progress = now;
                            Ok(Some(FeedEvent::DominantSelected(list)))
                        }
                        Err(error) => Ok(Some(FeedEvent::DominantUnavailable {
                            error,
                            trading_day: self
                                .session
                                .trading_day()
                                .context("missing MD day during dominant lookup")?
                                .to_owned(),
                        })),
                    };
                }
                if lookup.pending() {
                    return Ok(None);
                }
            }
            ensure!(
                self.ready()
                    || self.waiting_for_front()
                    || now.saturating_duration_since(self.progress) < self.timeout,
                "MD lifecycle timeout; supervisor restart required"
            );
            return Ok(None);
        };
        let output = match event.kind {
            1 => {
                if let Some(lookup) = &mut self.dominant {
                    lookup.reset();
                }
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
                if let Some(lookup) = &mut self.dominant {
                    lookup.reset();
                }
                self.session.disconnect();
                self.progress = now;
                FeedEvent::Disconnected {
                    reason: event.error,
                }
            }
            4 => {
                if self.session.login(&event)? {
                    self.progress = now;
                    if let Some(lookup) = &mut self.dominant {
                        let day = self.session.trading_day().context("missing MD login day")?;
                        lookup.begin(self.session.generation, day, now);
                    } else if self.baseline.is_empty() {
                        return Ok(Some(FeedEvent::Ready {
                            generation: self.session.generation,
                        }));
                    } else {
                        self.native.set_symbols(&self.baseline)?;
                        self.native.subscribe()?;
                    }
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
                if !self.session.ready()
                    || Some(super::super::text_ref(&event.trading_day)?)
                        != self.session.trading_day()
                {
                    return Ok(None);
                }
                let venue = self.session.tick_venue(&event)?;
                self.last_quote = Some((super::native::quote_bucket(&event.symbol), sequence));
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
