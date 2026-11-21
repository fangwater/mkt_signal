//! Single business loop. Evidence goes to persist_manager through
//! `PersistChannel`; no automatic order cancellation or cross-account routing is
//! performed on startup/recovery. Strategy attribution is restored from
//! persist_manager; fills recovered by query stay unallocated.
use crate::model::{Intent, OrderKey};
pub use crate::persist::PersistChannel;
#[cfg(feature = "ctp-sdk")]
use crate::restart::ColdRestartRequired;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::path::Path;
#[cfg(feature = "ctp-sdk")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "ctp-sdk")]
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    ExecTarget {
        target: crate::ctp::exec::Target,
    },
    Submit {
        intent: Intent,
    },
    Cancel {
        key: OrderKey,
        #[serde(default)]
        execution_route: Option<crate::model::ExecutionRoute>,
    },
    SubmitBatch {
        intents: Vec<Intent>,
    },
    CancelBatch {
        keys: Vec<OrderKey>,
        #[serde(default)]
        execution_route: Option<crate::model::ExecutionRoute>,
    },
    /// Cutover only: credit `lots` (signed) of the unallocated account
    /// position in `market` to `strategy` at cost `price`.
    AssignPosition {
        strategy: String,
        market: String,
        lots: i64,
        price: f64,
    },
    Connections {},
    State {},
    Reconcile {},
    Shutdown {},
}

#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
pub(crate) fn command_order_keys(command: &Command) -> Vec<OrderKey> {
    match command {
        Command::Cancel { key, .. } => vec![*key],
        Command::CancelBatch { keys, .. } => keys.clone(),
        _ => vec![],
    }
}

#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
pub(crate) fn input_commands() -> Result<CommandInput> {
    CommandInput::stdin()
}

/// Operator JSON-line commands read from non-blocking stdin inside the business
/// loop. Polled at a bounded rate so idle spinning issues no per-iteration syscall.
#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
pub(crate) struct CommandInput {
    pending: Vec<u8>,
    oversized: bool,
    closed: bool,
    next_read: std::time::Instant,
}
#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
impl CommandInput {
    const MAX_LINE: usize = 16384;
    const READ_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

    fn stdin() -> Result<Self> {
        // SAFETY: fcntl on the process's own stdin descriptor.
        let flags = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_GETFL) };
        anyhow::ensure!(flags >= 0, "read stdin flags");
        let set =
            unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        anyhow::ensure!(set >= 0, "make stdin non-blocking");
        Ok(Self {
            pending: Vec::with_capacity(Self::MAX_LINE),
            oversized: false,
            closed: false,
            next_read: std::time::Instant::now(),
        })
    }

    /// Same contract as a channel: `Empty` when no complete line is buffered,
    /// `Disconnected` once stdin has closed and every line was delivered.
    pub(crate) fn try_recv(
        &mut self,
    ) -> std::result::Result<Result<Command>, crossbeam_channel::TryRecvError> {
        if let Some(command) = self.next_line() {
            return Ok(command);
        }
        if self.closed {
            return Err(crossbeam_channel::TryRecvError::Disconnected);
        }
        let now = std::time::Instant::now();
        if now < self.next_read {
            return Err(crossbeam_channel::TryRecvError::Empty);
        }
        self.next_read = now + Self::READ_INTERVAL;
        let mut chunk = [0u8; 4096];
        // SAFETY: reads into a local buffer of the given length.
        let n = unsafe { libc::read(libc::STDIN_FILENO, chunk.as_mut_ptr().cast(), chunk.len()) };
        match n {
            0 => self.closed = true,
            n if n > 0 => self.pending.extend_from_slice(&chunk[..n as usize]),
            _ => {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::WouldBlock
                    && error.kind() != std::io::ErrorKind::Interrupted
                {
                    self.closed = true;
                    return Ok(Err(anyhow::anyhow!("command input error")));
                }
            }
        }
        self.next_line()
            .ok_or(crossbeam_channel::TryRecvError::Empty)
    }

    fn next_line(&mut self) -> Option<Result<Command>> {
        loop {
            let Some(end) = self.pending.iter().position(|b| *b == b'\n') else {
                if self.pending.len() > Self::MAX_LINE {
                    self.pending.clear();
                    self.oversized = true;
                }
                return None;
            };
            let line: Vec<u8> = self.pending.drain(..=end).collect();
            if std::mem::take(&mut self.oversized) || line.len() > Self::MAX_LINE + 1 {
                return Some(Err(anyhow::anyhow!("command exceeds 16 KiB")));
            }
            let line = &line[..line.len() - 1];
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            return Some(
                serde_json::from_slice(line).map_err(|_| anyhow::anyhow!("invalid command schema")),
            );
        }
    }
}

pub fn lock_flow(path: &Path) -> Result<File> {
    std::fs::create_dir_all(path).context("create CN futures flow directory")?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path.join(".cn_futures.lock"))
        .context("open CN futures flow lock")?;
    file.try_lock()
        .context("CN futures flow directory already in use")?;
    Ok(file)
}

#[cfg(feature = "ctp-sdk")]
pub fn run(config: crate::config::Config, execute: bool) -> Result<()> {
    anyhow::ensure!(
        config.exec.is_none(),
        "exec requires a combined MD/Trader runtime"
    );
    run_with_strategy(
        config,
        None,
        execute,
        crate::ctp::strategy::NoStrategy,
        &AtomicBool::new(false),
    )
}

/// Whether a factual fill belongs to an order this process submitted.
#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk"))]
pub(crate) fn own_order(core: &crate::core::TradingCore, fill: &crate::model::Fill) -> bool {
    core.fill_order_key(fill)
        .and_then(|key| core.order(key))
        .is_some_and(|order| order.owned)
}

#[cfg(feature = "ctp-sdk")]
fn prepare_ctp_contracts(
    session: &mut crate::ctp::session::Session,
    core: &crate::core::TradingCore,
    market_data: Option<&mut crate::ctp::market_data::Feed>,
    contracts: &[(crate::model::Venue, &str)],
    now: std::time::Instant,
) -> Result<()> {
    anyhow::ensure!(
        core.is_ready(),
        "CTP account recovery or settlement pending"
    );
    for &(venue, symbol) in contracts {
        session.require_active_future(venue, symbol)?;
    }
    for &(venue, symbol) in contracts {
        session.request_rates(venue, symbol, now)?;
    }
    if let Some(feed) = market_data {
        anyhow::ensure!(
            feed.subscribe_contracts(contracts, now)?,
            "CTP market data subscription acknowledgement pending"
        );
    }
    anyhow::ensure!(
        contracts
            .iter()
            .all(|&(venue, symbol)| core.instrument(venue, symbol).is_some()),
        "CTP account commission and margin query pending"
    );
    Ok(())
}

#[cfg(feature = "ctp-sdk")]
fn prepare_ctp_target(
    settings: &crate::ctp::exec::Settings,
    target: &crate::ctp::exec::Target,
    attributes: impl Fn(&str) -> bool,
    session: &mut crate::ctp::session::Session,
    core: &crate::core::TradingCore,
    feed: Option<&mut crate::ctp::market_data::Feed>,
    now: std::time::Instant,
) -> Result<()> {
    let now_us = crate::persistence::now_us();
    let contracts = crate::ctp::exec::validate_target(settings, target, core, now_us, false)?;
    let required: Vec<_> = contracts
        .into_iter()
        .filter(|&(venue, symbol, required)| {
            required || attributes(&format!("{}:{symbol}", venue.as_str()))
        })
        .map(|(venue, symbol, _)| (venue, symbol))
        .collect();
    prepare_ctp_contracts(session, core, feed, &required, now)
}

/// Strategy hooks, feed consumption, risk and order submission share this caller thread.
#[cfg(feature = "ctp-sdk")]
pub fn run_with_strategy<S: crate::ctp::strategy::Strategy>(
    config: crate::config::Config,
    market_data: Option<crate::ctp::market_data::Config>,
    execute: bool,
    mut strategy: S,
    stop: &AtomicBool,
) -> Result<()> {
    use crate::core::{SingleLoop, TradingCore};
    use crate::ctp::strategy::{Actions, StrategyContext};
    use crate::ctp::{self, sdk::CtpTrader, session::Session};
    use serde_json::json;
    const WAITING_FOR_TRADER: &str = "CTP trader front offline; waiting for reconnection";
    const WAITING_FOR_MD: &str = "CTP market-data front offline; waiting for reconnection";
    const ACCOUNT_RECONCILIATION: &str = "CTP account query unavailable; reconciliation pending";
    let wait_for_front = market_data.is_some();
    config.validate()?;
    if let Some(ingress) = &config.position_target_ingress {
        if execute {
            anyhow::ensure!(
                config.exec.is_some() && market_data.is_some(),
                "executable target ingress requires exec and combined market data"
            );
        } else {
            ingress.validate_observer()?;
        }
    }
    if let Some(md) = &market_data {
        config.validate_market_data(md)?;
    }
    let placement = if let Some(md) = &market_data {
        crate::placement::Placement::enter_combined(&config.placement, &md.placement)?
    } else {
        crate::placement::Placement::enter(&config.placement)?
    };
    let _flow_lock = lock_flow(&config.flow_directory)?;
    let credentials = config.credentials()?;
    let redactions = [
        &credentials.password,
        &credentials.auth_code,
        &credentials.app_id,
    ]
    .into_iter()
    .map(|s| s.to_string_lossy().into_owned())
    .filter(|s| !s.is_empty())
    .collect();
    let database = config
        .persistence_directory
        .clone()
        .unwrap_or_else(|| config.flow_directory.join("persist_manager"));
    let account_hash = crate::persistence::account_hash(&[
        config.front.as_bytes(),
        credentials.broker.as_bytes(),
        credentials.investor.as_bytes(),
    ]);

    // Both publish inline from this thread; neither creates a worker.
    let mut position_ipc = config
        .position_ipc
        .as_ref()
        .map(|cfg| {
            cn_futures_state::publisher::Producer::start(
                cfg,
                cn_futures_state::Adapter::Ctp,
                &account_hash,
            )
        })
        .transpose()?;
    // Startup recovery opens RocksDB, whose flush/compaction pools outlive the
    // read; spawn them on the background CPUs, not the business CPU.
    let journal = placement.on_background(|| {
        PersistChannel::start(
            &database,
            redactions,
            PersistChannel::persist_manager_sink()?,
        )
    })?;
    let restored = journal.take_previous_ledger();
    let restored_markets = restored.len();
    strategy.restore_ledger(restored)?;
    journal.record(
        json!({"type":"strategy_ledger_restored","markets":restored_markets}),
        true,
    )?;
    let trader = placement.on_sdk(|| CtpTrader::new(&config, credentials, placement.callback()))?;
    let investor = trader
        .investor()
        .to_str()
        .context("invalid InvestorID")?
        .to_owned();
    let mut session = Session::new(config.clone(), investor.clone(), Instant::now());
    let mut core = TradingCore::new(config.risk.clone(), ctp::scope())?;
    core.reserve_orders(config.preallocated_orders)?;
    if wait_for_front {
        core.block(WAITING_FOR_TRADER);
    }
    let mut runtime = SingleLoop {
        core,
        trader,
        execute,
    };
    let mut input = input_commands()?;
    // Network I/O (Manager subscription, target listeners, dominant lookup) runs
    // as local tasks driven from this loop; no I/O thread exists.
    let mut io = cn_futures_target::IoLoop::new(placement.background_hook())?;
    let mut idle = crate::poll::Idle::new(config.poll);
    let target_ingress = if let Some(cfg) = &config.position_target_ingress {
        Some(if execute {
            cn_futures_target::Ingress::start(cfg, &io)?
        } else {
            cn_futures_target::Ingress::start_observer(cfg, &io)?
        })
    } else {
        None
    };
    // Only the business thread may run on the business CPU; the logger joins
    // the background CPUs before trading.
    placement.pin_background_thread(async_log::thread_id())?;
    let sdk_version = placement.on_sdk(|| ctp::sdk::version(&config.sdk_library))?;
    journal.record(json!({"type":"runtime_start","execute":execute,"exchanges":crate::model::Venue::ALL.map(|v|v.as_str()),"production_mode":config.production_mode,"sdk":sdk_version}), true)?;
    crate::position_ipc::publish(
        &mut position_ipc,
        &runtime.core,
        false,
        false,
        cn_futures_state::RuntimeStatus::Running,
        true,
        crate::position_ipc::Observation::default(),
    );
    placement.on_sdk(|| {
        runtime.trader.start(
            &std::iter::once(config.front.as_str())
                .chain(config.fallback_fronts.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(","),
        )
    })?;
    let mut feed = market_data
        .as_ref()
        .map(|md| ctp::market_data::Feed::new(md, &placement, io.spawner()))
        .transpose()?;
    let intruders = placement.business_intruders()?;
    if !intruders.is_empty() {
        log::warn!("threads may run on the business CPU: {intruders:?}");
    }
    journal.record(
        json!({"type":"placement_audit","business_cpu":config.placement.business_cpu,"intruders":intruders}),
        true,
    )?;
    let mut feed_failed = false;
    let mut actions = Actions::default();
    let mut md_latency = ctp::market_data::latency::LatencyStats::new(Instant::now());
    let mut stopping = false;
    let mut commands_open = true;
    let mut transport_online = false;
    let mut transport_progress = Instant::now();
    let mut reconcile_after_account_retry = false;
    let outcome = (|| -> Result<()> {
        while !stopping && !stop.load(Ordering::Acquire) {
            if runtime.trader.placement_fault.load(Ordering::Acquire) {
                runtime.core.block("CTP SDK CPU/NUMA binding failed");
                anyhow::bail!("CTP SDK CPU/NUMA binding failed; no fallback is permitted");
            }
            if runtime.trader.fault.load(Ordering::Acquire) || journal.failed() {
                runtime.core.block(
                    "reliable event ingress/journal failed; stop and reconcile before resubmitting",
                );
                anyhow::bail!(
                "CTP reliable event pipeline failed; existing venue orders have not been cancelled"
            );
            }
            let mut work = false;
            let batch_now = Instant::now();
            io.drive(batch_now);
            for _ in 0..256 {
                let Ok(received) = runtime.trader.events.try_recv() else {
                    break;
                };
                work = true;
                let event = received.event;
                // Persist factual evidence before attempting any parser or strategy association.
                journal.ctp_event(&event, received.received_us, false)?;
                let owns = matches!(
                    event.kind,
                    ctp::ORDER | ctp::TRADE | ctp::QUERY_ORDER | ctp::QUERY_TRADE | ctp::POSITION
                ) && event.present != 0
                    && ctp::text_ref(&event.investor)? == investor
                    && crate::model::Venue::parse(ctp::text_ref(&event.exchange)?).is_ok();
                let decoded = if owns {
                    ctp::decode_event(&event)
                } else {
                    Ok(crate::model::DecodedUpdate::None)
                };
                if event.kind == ctp::CONNECTED {
                    transport_online = true;
                    transport_progress = Instant::now();
                    reconcile_after_account_retry = false;
                    runtime.core.connected();
                }
                if event.kind == ctp::DISCONNECTED {
                    transport_online = false;
                    transport_progress = Instant::now();
                    reconcile_after_account_retry = false;
                    runtime.core.disconnect();
                    if wait_for_front {
                        runtime.core.block(WAITING_FOR_TRADER);
                    }
                }
                if event.kind == ctp::INSTRUMENT_STATUS {
                    runtime.core.set_trading_status(
                        crate::model::Venue::parse(ctp::text_ref(&event.exchange)?)?,
                        ctp::text_ref(&event.symbol)?,
                        event.status == i32::from(b'2'),
                    );
                }
                let account_retry = event.kind == ctp::ERROR
                    && session.retries_account_query(event.request, event.error);
                if account_retry && runtime.core.is_ready() {
                    runtime.core.block(ACCOUNT_RECONCILIATION);
                    reconcile_after_account_retry = true;
                    journal.record(json!({"type":"account_query_recovery_pending"}), true)?;
                }
                if let Some(snapshot) = session.on_event(event.as_ref(), batch_now, &decoded)? {
                    reconcile_after_account_retry = false;
                    let trades = snapshot.trades.clone();
                    runtime.core.restore(snapshot)?;
                    if !session.settlement_confirmed() {
                        runtime
                            .core
                            .block("settlement not confirmed; read-only queries remain available");
                    }
                    if let Err(error) = journal.reconciled(&mut runtime.core, trades) {
                        runtime.core.block("durable recovery failed");
                        journal.record(
                            json!({"type":"trading_blocked","error":error.to_string()}),
                            true,
                        )?;
                    }
                }
                if let Some(available) = session.take_cash_update() {
                    if reconcile_after_account_retry {
                        session.restart_queries(batch_now)?;
                        reconcile_after_account_retry = false;
                    } else {
                        runtime.core.constrain_cash(available)?;
                    }
                }
                if let Some(rules) = session.take_rule_update() {
                    for rule in &rules {
                        runtime.trader.prepare_contract(rule.venue, &rule.symbol)?;
                    }
                    runtime.core.replace_instruments(rules)?;
                }
                if session.ready() {
                    let mut new_fill = false;
                    let outcome = match event.kind {
                        ctp::ORDER | ctp::TRADE if owns => decoded
                            .as_ref()
                            .map_err(|error| anyhow::anyhow!(error.to_string()))
                            .and_then(|parsed| match parsed {
                                crate::model::DecodedUpdate::Order(order) => {
                                    runtime.core.on_order(order)
                                }
                                crate::model::DecodedUpdate::Trade(fill) => {
                                    runtime.core.on_fill(fill).map(|new| new_fill = new)
                                }
                                _ => Ok(()),
                            }),
                        ctp::INSERT_RESPONSE if event.error != 0 => runtime
                            .trader
                            .response_key(&event)
                            .and_then(|key| runtime.core.reject(&key)),
                        ctp::CANCEL_RESPONSE if event.error != 0 => runtime
                            .trader
                            .response_key(&event)
                            .map(|key| runtime.core.cancel_failed(&key)),
                        ctp::ERROR if event.error != 0 && !account_retry => Err(anyhow::anyhow!(
                            "uncorrelated CTP error response {}; query before resubmitting",
                            event.error
                        )),
                        _ => Ok(()),
                    };
                    // A fill the OMS could not apply is still a venue fact.
                    if let (true, Ok(crate::model::DecodedUpdate::Trade(fill))) =
                        (event.kind == ctp::TRADE && outcome.is_err(), &decoded)
                    {
                        journal.fill(&runtime.core, fill, None, received.received_us, None)?;
                    }
                    if outcome.is_ok()
                        && matches!(
                            event.kind,
                            ctp::ORDER | ctp::INSERT_RESPONSE | ctp::CANCEL_RESPONSE
                        )
                    {
                        let key = decoded
                            .as_ref()
                            .ok()
                            .and_then(|p| p.order())
                            .map(|o| o.key)
                            .or_else(|| runtime.trader.response_key(&event).ok());
                        if let Some(key) = key {
                            journal.orders(&runtime.core, &[key])?;
                        }
                    }
                    if let Err(error) = outcome {
                        runtime.core.block(
                        "native event could not be reconciled; factual event retained in journal",
                    );
                        journal.record(
                            json!({"type":"trading_blocked","error":error.to_string()}),
                            true,
                        )?;
                    } else if let Ok(update) = &decoded {
                        strategy.on_update(
                            update,
                            StrategyContext {
                                core: &runtime.core,
                                can_trade: execute
                                    && runtime.core.is_ready()
                                    && feed.as_ref().is_some_and(|md| md.ready())
                                    && runtime.trader.events.is_empty(),
                            },
                        )?;
                        if let (true, crate::model::DecodedUpdate::Trade(fill)) = (new_fill, update)
                        {
                            strategy.on_fill(fill, own_order(&runtime.core, fill));
                            journal.ledger(
                                &runtime.core,
                                &strategy.take_ledger_events(),
                                Some((fill, None, received.received_us)),
                            )?;
                        }
                    }
                }
            }
            let now = Instant::now();
            if runtime.trader.events.is_empty() {
                let control_timeout = session.timed_out(now);
                let front_timeout = !wait_for_front
                    && !transport_online
                    && now.saturating_duration_since(transport_progress)
                        >= Duration::from_millis(config.response_timeout_ms);
                if control_timeout || front_timeout {
                    let restart = if control_timeout && session.account_query_in_flight() {
                        ColdRestartRequired::AccountQuery("CTP account query timed out")
                    } else {
                        ColdRestartRequired::TransportOrControlTimeout
                    };
                    return Err(journal.restart_required(
                        &mut runtime.core,
                        restart,
                        json!({"transport_online":transport_online,"front_timeout":front_timeout,"control_timeout":control_timeout}),
                    ));
                }
                if let Some(kind) = session.request_due(now)? {
                    let id = runtime.trader.next_id()?;
                    let code = runtime.trader.control(kind, id, session.query_target())?;
                    session.sent(kind, id, code, Instant::now())?;
                    work = true;
                }
            }
            // Tick consumption, strategy, risk, OMS and the SDK call all use this thread.
            if let Some(md) = &mut feed {
                for _ in 0..256 {
                    if feed_failed {
                        break;
                    }
                    let queued = md.has_backlog();
                    let event = match md.poll(Instant::now()) {
                        Ok(event) => event,
                        Err(error) if !execute && !error.is::<ColdRestartRequired>() => {
                            // Keep the SDK owner until cold shutdown. Release/Init must not
                            // run on this business loop, and an MD failure must not hide funds.
                            feed_failed = true;
                            strategy.on_market_state(false, md.generation())?;
                            journal.record(
                                json!({"type":"md_unavailable","error":error.to_string(),"account_observation_continues":true}),
                                true,
                            )?;
                            break;
                        }
                        Err(error) => return Err(error),
                    };
                    work |= queued;
                    let Some(event) = event else {
                        if queued {
                            continue;
                        }
                        break;
                    };
                    match event {
                        ctp::market_data::FeedEvent::Tick(tick) => {
                            md_latency.observe(&tick, crate::persistence::now_us(), md.queued());
                            // Only a newer quote for this same contract supersedes this
                            // decision; other contracts' queued quotes are unrelated.
                            let can_trade = execute
                                && runtime.core.is_ready()
                                && md.ready()
                                && !md.newer_contract_quote_queued()
                                && runtime.trader.events.is_empty()
                                && ctp::text_ref(&tick.trading_day)? == runtime.core.trading_day();
                            strategy.on_tick(
                                &tick,
                                StrategyContext {
                                    core: &runtime.core,
                                    can_trade,
                                },
                                &mut actions,
                            )?;
                            journal.ledger(&runtime.core, &strategy.take_ledger_events(), None)?;
                            if let Some(note) = strategy.take_wait_note() {
                                journal.quote_decision(
                                    &note,
                                    tick.received_us,
                                    tick.connection_generation,
                                )?;
                            }
                            for action in actions.drain() {
                                use ctp::strategy::{ActionOutcome, CtpDispatchChecks};
                                let checks = CtpDispatchChecks {
                                    strategy_can_trade: can_trade,
                                    execution_enabled: runtime.execute,
                                    account_ready: runtime.core.is_ready(),
                                    market_data_ready: md.ready(),
                                    newer_contract_quote_queued: md.newer_contract_quote_queued(),
                                    trade_event_backlog: !runtime.trader.events.is_empty(),
                                    journal_failed: journal.failed(),
                                    trade_ingress_failed: runtime
                                        .trader
                                        .fault
                                        .load(Ordering::Acquire),
                                };
                                let allowed = checks.can_dispatch();
                                let outcome = if allowed {
                                    ctp::strategy::dispatch(
                                        &mut runtime,
                                        &journal,
                                        action,
                                        true,
                                        Instant::now(),
                                    )?
                                } else {
                                    ActionOutcome::CtpDispatchBlocked {
                                        action,
                                        checks,
                                        quote_received_us: tick.received_us,
                                        quote_generation: tick.connection_generation,
                                    }
                                };
                                strategy.on_result(
                                    &outcome,
                                    StrategyContext {
                                        core: &runtime.core,
                                        can_trade: allowed && runtime.core.is_ready(),
                                    },
                                )?;
                                // Every CTP order decision outcome belongs in the operator log.
                                journal.strategy_result(&outcome, true)?;
                            }
                        }
                        ctp::market_data::FeedEvent::Connected { generation } => {
                            strategy.on_market_state(false, generation)?;
                            journal.record(
                                json!({"type":"md_connected","generation":generation}),
                                true,
                            )?;
                        }
                        ctp::market_data::FeedEvent::Disconnected { reason } => {
                            strategy.on_market_state(false, md.generation())?;
                            journal.record(
                                json!({"type":"md_disconnected","reason":reason,"gap":true}),
                                true,
                            )?;
                        }
                        ctp::market_data::FeedEvent::Ready { generation } => {
                            strategy.on_market_state(true, generation)?;
                            journal
                                .record(json!({"type":"md_ready","generation":generation}), true)?;
                        }
                        ctp::market_data::FeedEvent::DominantSelected(list) => {
                            journal.record(
                                json!({"type":"md_dominant_selected","trading_day":list.trading_day,"latest_trading_day":list.latest_trading_day,"source":list.source,"calendar_verified":list.calendar_verified,"subscriptions":list.symbols,"generation":md.generation()}),
                                true,
                            )?;
                        }
                        ctp::market_data::FeedEvent::DominantUnavailable { error, trading_day } => {
                            journal.record(
                                json!({"type":"md_dominant_unavailable","trading_day":trading_day,"error":error.to_string(),"retrying":true}),
                                true,
                            )?;
                        }
                    }
                }
                if let Some(stats) = md_latency.take_due(Instant::now()) {
                    journal.record(stats, true)?;
                }
            }
            // Never submit an opening decision ahead of a known backlog of venue events.
            if runtime.trader.events.is_empty() {
                if let Some(ingress) = &target_ingress {
                    anyhow::ensure!(!ingress.is_finished(), "position target ingress stopped");
                    let can_trade = !feed_failed
                        && execute
                        && runtime.core.is_ready()
                        && feed.as_ref().is_some_and(|md| md.ready());
                    let mut audit_error = None;
                    work |= ingress.poll(|target| {
                        let recorded = if execute {
                            anyhow::ensure!(
                                can_trade,
                                "exec target admission requires recovered account and ready MD"
                            );
                            let now_us = crate::persistence::now_us();
                            prepare_ctp_target(
                                config
                                    .exec
                                    .as_ref()
                                    .context("target execution is not configured")?,
                                &target,
                                |market| strategy.attributes(market),
                                &mut session,
                                &runtime.core,
                                feed.as_mut(),
                                now,
                            )?;
                            let admitted = strategy.on_target(
                                target,
                                StrategyContext {
                                    core: &runtime.core,
                                    can_trade,
                                },
                                now_us,
                            )?;
                            journal.target_accepted(&admitted)
                        } else {
                            journal.target_observed(&target)
                        };
                        if let Err(error) = recorded {
                            audit_error = Some(error);
                            anyhow::bail!("target audit unavailable; Exec is stopping");
                        }
                        Ok(())
                    });
                    if let Some(error) = audit_error {
                        return Err(error);
                    }
                }
            }
            if commands_open && runtime.trader.events.is_empty() {
                match input.try_recv() {
                    Ok(command) => {
                        work = true;
                        let day = runtime.core.trading_day().to_owned();
                        let cancel_keys = command
                            .as_ref()
                            .ok()
                            .map(command_order_keys)
                            .unwrap_or_default();
                        let result = match command {
                            Ok(Command::ExecTarget { target }) => {
                                let now_us = crate::persistence::now_us();
                                let can_trade = execute
                                    && runtime.core.is_ready()
                                    && feed.as_ref().is_some_and(|md| md.ready());
                                let admit = |settings| {
                                    prepare_ctp_target(
                                        settings,
                                        &target,
                                        |market| strategy.attributes(market),
                                        &mut session,
                                        &runtime.core,
                                        feed.as_mut(),
                                        now,
                                    )
                                };
                                let prepared = if can_trade {
                                    config
                                        .exec
                                        .as_ref()
                                        .context("target execution is not configured")
                                        .and_then(admit)
                                } else {
                                    Err(anyhow::anyhow!("exec target admission requires recovered account and ready MD"))
                                };
                                prepared.and_then(|()| {
                                    strategy.on_target(
                                        target,
                                        StrategyContext { core: &runtime.core, can_trade },
                                        now_us,
                                    )
                                }).map(|target| json!({"type":"exec_target_accepted","target":target,"venue_accepted":false}))
                            }
                            Ok(Command::AssignPosition {
                                strategy: name,
                                market,
                                lots,
                                price,
                            }) => strategy
                                .assign_position(
                                    &runtime.core,
                                    &name,
                                    &market,
                                    lots,
                                    price,
                                    crate::persistence::now_us(),
                                )
                                .and_then(|()| {
                                    journal.ledger(&runtime.core, &strategy.take_ledger_events(), None)
                                })
                                .map(|()| json!({"type":"position_assigned","strategy":name,"market":market,"lots":lots,"price":price})),
                            Ok(Command::State {}) => Ok(
                                json!({"type":"state","state":runtime.core.view(),"account":session.account(),"settlement_confirmed":session.settlement_confirmed()}),
                            ),
                            Ok(Command::Shutdown {}) => {
                                stopping = true;
                                Ok(json!({"type":"shutdown","venue_orders_cancelled":false}))
                            }
                            Ok(Command::Reconcile {}) => {
                                session.restart_queries(Instant::now()).map(|_| {
                                    reconcile_after_account_retry = false;
                                    runtime.core.block("operator-requested reconciliation");
                                    json!({"type":"reconciling"})
                                })
                            }
                            Ok(Command::Submit { .. }) if config.exec.is_some() => {
                                Err(anyhow::anyhow!(
                                    "exec owns account targets; manual submits are disabled"
                                ))
                            }
                            Ok(Command::Submit { .. })
                                if market_data.is_some()
                                    && !feed.as_ref().is_some_and(|md| md.ready()) =>
                            {
                                Err(anyhow::anyhow!(
                                    "MD is not ready; opening decisions are blocked"
                                ))
                            }
                            Ok(Command::Submit { intent }) => (|| {
                                anyhow::ensure!(execute, "execution is disabled");
                                prepare_ctp_contracts(
                                    &mut session,
                                    &runtime.core,
                                    feed.as_mut(),
                                    &[(intent.venue, &intent.symbol)],
                                    now,
                                )?;
                                runtime.submit_planned_with(intent, Instant::now(), |prepared| {
                                    journal.prepared(day, prepared, None)
                                }).map(|receipt| json!({"type":"submit_receipt","receipt":receipt}))
                            })(),
                            Ok(Command::Cancel {
                                key,
                                execution_route: None,
                            }) => runtime
                                .cancel_with(&key, Instant::now(), |prepared| {
                                    journal.prepared_cancel(prepared)
                                })
                                .map(|_| json!({"type":"cancel_submitted_locally","key":key})),
                            Ok(
                                Command::SubmitBatch { .. }
                                | Command::CancelBatch { .. }
                                | Command::Connections {}
                                | Command::Cancel {
                                    execution_route: Some(_),
                                    ..
                                },
                            ) => Err(anyhow::anyhow!(
                                "native batch/seat commands are available on the Yida runtime only"
                            )),
                            Err(error) => Err(error),
                        };
                        journal.command_state(&runtime.core, &result, cancel_keys)?;
                        journal.record(
                        result.unwrap_or_else(
                            |error| json!({"type":"command_rejected","error":error.to_string()}),
                        ),
                        true,
                    )?;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        commands_open = false;
                        stopping = market_data.is_none();
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                }
            }
            let md_ready = !feed_failed && feed.as_ref().is_none_or(|md| md.ready());
            let can_trade = runtime.execute
                && runtime.core.is_ready()
                && transport_online
                && md_ready
                && runtime.trader.events.is_empty();
            crate::position_ipc::publish(
                &mut position_ipc,
                &runtime.core,
                transport_online,
                can_trade,
                cn_futures_state::RuntimeStatus::Running,
                false,
                crate::position_ipc::Observation {
                    account: session.account_funds(),
                    reason: (runtime.core.is_ready()
                        && feed.as_ref().is_some_and(|md| md.waiting_for_front()))
                    .then_some(WAITING_FOR_MD),
                    ..Default::default()
                },
            );
            if work {
                idle.busy();
            } else if wait_for_front && !transport_online {
                std::thread::sleep(Duration::from_millis(10));
            } else {
                idle.wait(|select| {
                    select.recv(&runtime.trader.events);
                    if let Some(md) = &feed {
                        md.wake_on(select);
                    }
                });
            }
        }
        Ok(())
    })();
    let restart = outcome
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<ColdRestartRequired>());
    if let Some(restart) = restart {
        runtime.core.disconnect();
        runtime.core.block(restart.observation_reason());
    } else if outcome.is_err() {
        runtime
            .core
            .block("runtime failed; complete recovery required before trading");
    }
    // The active business loop has ended. SDK lifecycle calls below are cold shutdown work.
    drop(feed.take());
    // SDK Release joins callbacks; a cloned receiver retains the final delivered evidence.
    let tail = runtime.trader.events.clone();
    let fault = runtime.trader.fault.clone();
    let mut core = runtime.core;
    drop(runtime.trader);
    for received in tail.try_iter() {
        let event = received.event;
        journal.ctp_event(&event, received.received_us, true)?;
        if ctp::text_ref(&event.investor).is_ok_and(|v| v == investor)
            && ctp::text_ref(&event.exchange).is_ok_and(|v| crate::model::Venue::parse(v).is_ok())
        {
            if event.kind == ctp::TRADE {
                if let Ok(fill) = event.to_fill() {
                    match core.on_fill(&fill) {
                        Ok(false) => {}
                        Ok(true) => journal.fill(&core, &fill, None, received.received_us, None)?,
                        Err(_) => {
                            core.block("shutdown fill requires recovery");
                            journal.fill(&core, &fill, None, received.received_us, None)?;
                        }
                    }
                }
            }
            if event.kind == ctp::ORDER {
                if let Ok(update) = event.to_order() {
                    let key = update.key;
                    if core.on_order(&update).is_ok() {
                        journal.orders(&core, &[key])?;
                    } else {
                        core.block("shutdown order requires recovery");
                    }
                }
            }
        }
    }
    journal.state(&core)?;
    let shutdown_ok = !fault.load(Ordering::Acquire);
    crate::position_ipc::publish(
        &mut position_ipc,
        &core,
        false,
        false,
        if (outcome.is_ok() || restart.is_some()) && shutdown_ok {
            cn_futures_state::RuntimeStatus::Stopped
        } else {
            cn_futures_state::RuntimeStatus::Failed
        },
        true,
        crate::position_ipc::Observation::default(),
    );
    journal.record(
        serde_json::json!({"type":"runtime_stopped","venue_orders_cancelled":false}),
        true,
    )?;
    journal.finish()?;
    if let Some(publisher) = position_ipc {
        publisher.finish()?;
    }
    anyhow::ensure!(
        shutdown_ok,
        "CTP callback pipeline failed on shutdown; evidence retained"
    );
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_and_malformed_commands_fail() {
        assert!(serde_json::from_str::<Command>(r#"{"op":"flatten"}"#).is_err());
        assert!(serde_json::from_str::<Command>(r#"{"op":"state","password":"x"}"#).is_err());
    }
    #[test]
    fn flow_directory_cannot_be_shared_concurrently() {
        let path =
            std::env::temp_dir().join(format!("cn-futures-flow-lock-{}", std::process::id()));
        let lock = lock_flow(&path).unwrap();
        assert!(lock_flow(&path).is_err());
        drop(lock);
        let lock = lock_flow(&path).unwrap();
        drop(lock);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
#[path = "runtime/projection_tests.rs"]
mod projection_tests;
