use super::{config::Config, sdk::YidaTrader, session::Session, *};
use crate::{
    core::{SingleLoop, TradingCore},
    ctp::{
        exec::Exec,
        strategy::{self, Actions, Strategy, StrategyContext},
    },
    restart::ColdRestartRequired,
    runtime::{command_order_keys, input_commands, lock_flow, Command, PersistChannel},
};
use anyhow::Result;
use serde_json::json;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

pub fn run(config: Config, execute: bool) -> Result<()> {
    config.validate()?;
    config.validate_sdk_config()?;
    anyhow::ensure!(
        config.exec.is_none() || execute,
        "Yida exec requires --execute"
    );
    // Bind before queues, core and database allocations, and before SDK creation.
    let placement = config
        .placement
        .as_ref()
        .map(crate::placement::Placement::enter)
        .transpose()?;
    let _lock = lock_flow(&config.flow_directory)?;
    let credentials = config.credentials()?;
    let redactions = [
        &credentials.password,
        &credentials.app_id,
        &credentials.auth_code,
    ]
    .into_iter()
    .map(|v| v.to_string_lossy().into_owned())
    .collect();
    let database = config
        .persistence_directory
        .clone()
        .unwrap_or_else(|| config.flow_directory.join("persist_manager"));
    let native_config = std::fs::read_to_string(&config.sdk_config)?;
    let endpoint = native_config
        .lines()
        .filter_map(|line| line.split('#').next()?.split_once('='))
        .filter(|(k, _)| matches!(k.trim(), "TradingServerIP" | "TradingServerPort"))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let endpoint = serde_json::to_vec(&endpoint)?;
    let account_hash = crate::persistence::account_hash(&[&endpoint, credentials.user.as_bytes()]);
    // Both publish inline from this thread; neither creates a worker.
    let mut position_ipc = config
        .position_ipc
        .as_ref()
        .map(|cfg| {
            cn_futures_state::publisher::Producer::start(
                cfg,
                cn_futures_state::Adapter::Yida,
                &account_hash,
            )
        })
        .transpose()?;
    // Startup recovery opens RocksDB, whose flush/compaction pools outlive the
    // read; spawn them on the background CPUs, not the business CPU.
    let journal = on_background(placement.as_ref(), || {
        PersistChannel::start(
            &database,
            redactions,
            PersistChannel::persist_manager_sink()?,
        )
    })?;
    let trader = on_sdk(placement.as_ref(), || {
        YidaTrader::new_placed(
            &config,
            credentials,
            placement.as_ref().map(|p| p.callback()),
        )
    })?;
    let mut core = TradingCore::new(config.risk.clone(), scope())?;
    core.reserve_orders(config.preallocated_orders)?;
    let mut runtime = SingleLoop {
        core,
        trader,
        execute,
    };
    let mut session = Session::default();
    let mut market = observation::MarketBook::default();
    let mut input = input_commands()?;
    let mut exec = config
        .exec
        .clone()
        .map(|settings| {
            Exec::with_scope(
                settings,
                &config.risk,
                config.symbols.iter().map(|s| (s.venue, s.symbol.as_str())),
            )
        })
        .transpose()?;
    if let Some(taker) = &mut exec {
        let restored = journal.take_previous_ledger();
        let markets = restored.len();
        taker.restore_ledger(restored)?;
        journal.record(
            json!({"type":"strategy_ledger_restored","markets":markets}),
            true,
        )?;
    }
    // Network I/O runs as local tasks driven from this loop; no I/O thread exists.
    let mut io = match &placement {
        Some(placement) => cn_futures_target::IoLoop::new(placement.background_hook())?,
        None => cn_futures_target::IoLoop::new(|| {})?,
    };
    let mut idle = crate::poll::Idle::new(config.poll);
    let target_ingress = config
        .position_target_ingress
        .as_ref()
        .map(|cfg| cn_futures_target::Ingress::start(cfg, &io))
        .transpose()?;
    let mut actions = Actions::default();
    let mut l1_generation = None;
    let mut latest_l1_generation = 0;
    let mut checked_l1_drops = 0;
    let mut commands_open = true;
    journal.record(json!({"type":"runtime_start","adapter":"yida","sdk_reference":SDK_REFERENCE,"trading_protocol":config.trading_protocol,"public_simulation":config.public_simulation,"placement":config.placement,"poll":config.poll,"execute":execute,"venues":["SHFE","INE"]}),true)?;
    crate::position_ipc::publish(
        &mut position_ipc,
        &runtime.core,
        false,
        false,
        cn_futures_state::RuntimeStatus::Running,
        true,
        crate::position_ipc::Observation {
            account: None,
            marks: market.marks(),
            connected: runtime.trader.l1_connected.load(Ordering::Acquire),
            dropped: runtime.trader.l1_dropped.load(Ordering::Relaxed),
            reason: None,
        },
    );
    on_sdk(placement.as_ref(), || runtime.trader.start())?;
    if let Some(placement) = &placement {
        // Only the business thread may run on the business CPU.
        placement.pin_background_thread(async_log::thread_id())?;
        let intruders = placement.business_intruders()?;
        if !intruders.is_empty() {
            log::warn!("threads may run on the business CPU: {intruders:?}");
        }
        journal.record(
            json!({"type":"placement_audit","intruders":intruders}),
            true,
        )?;
    }
    let mut stopping = false;
    let mut next_l1_warning = Instant::now();
    let mut transport_seen_ready = false;
    let mut recovery_since = Instant::now();
    // A latched native inconsistency needs an operator; a later stall must not
    // turn it into an automatic cold restart.
    let mut operator_required = false;
    let outcome = (|| -> Result<()> {
        while !stopping {
            anyhow::ensure!(
                !runtime.trader.placement_fault.load(Ordering::Acquire),
                "Yida SDK CPU/NUMA binding failed; no fallback is permitted"
            );
            anyhow::ensure!(!runtime.trader.fault.load(Ordering::Acquire) && !journal.failed(),"reliable Yida ingress/journal failed; stop and reconcile before retry; venue orders not cancelled");
            let mut work = false;
            io.drive(Instant::now());
            for _ in 0..256 {
                let Ok(received) = runtime.trader.events.try_recv() else {
                    break;
                };
                work = true;
                let e = received.event;
                journal.yida_event(&e, received.received_us, false)?;
                if e.kind == CONNECTED {
                    if let Some(taker) = &mut exec {
                        taker.on_market_state(false, 0)?;
                    }
                    l1_generation = None;
                    transport_seen_ready = false;
                    runtime.core.connected();
                    recovery_since = Instant::now();
                }
                if e.kind == DISCONNECTED {
                    if let Some(taker) = &mut exec {
                        taker.on_market_state(false, 0)?;
                    }
                    l1_generation = None;
                    transport_seen_ready = false;
                    runtime.core.disconnect();
                    recovery_since = Instant::now();
                }
                if e.kind == SERVER_RESTARTED {
                    if let Some(taker) = &mut exec {
                        taker.on_market_state(false, 0)?;
                    }
                    return Err(journal.restart_required(
                        &mut runtime.core,
                        ColdRestartRequired::ServerRestarted,
                        json!({"native_event":e.status}),
                    ));
                }
                if e.kind == TRANSPORT {
                    if e.flag == 1 {
                        transport_seen_ready = true;
                    } else if transport_seen_ready
                        && config.trading_protocol == config::TradingProtocol::Xtcp
                    {
                        runtime
                            .core
                            .block("XTCP unavailable; reconcile after transport recovers");
                    }
                    journal.record(json!({"type":"transport_status","trading_protocol":config.trading_protocol,"ready":e.flag==1,"advertised_port":e.minimum}),true)?;
                }
                if e.kind == READY_LOGIN {
                    anyhow::ensure!(
                        e.error == 0,
                        "previous native login failed; no automatic credential retry"
                    );
                    on_sdk(placement.as_ref(), || runtime.trader.login())?;
                }
                let was_ready = session.ready;
                let historical = session.is_pre_snapshot_rejection(&e);
                let decoded = decode_event(&e, historical);
                let mut new_fill = false;
                let outcome = (|| -> Result<()> {
                    if let Some(snapshot) = session.on_event(&e, &config, &decoded)? {
                        let trades = snapshot.trades.clone();
                        runtime.core.restore(snapshot)?;
                        if let Err(error) = journal.reconciled(&mut runtime.core, trades) {
                            runtime.core.block("durable recovery failed");
                            return Err(error);
                        }
                        runtime.trader.recovered();
                        return Ok(());
                    }
                    let parsed = decoded
                        .as_ref()
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    if historical {
                        return Ok(());
                    }
                    if session.ready && e.replay == 0 {
                        new_fill = apply_live_update(&mut runtime, &e, parsed)?;
                    }
                    Ok(())
                })();
                if let (Ok(()), Some(taker), Ok(DecodedUpdate::Trade(fill))) =
                    (&outcome, &mut exec, &decoded)
                {
                    if new_fill {
                        taker.on_fill(fill, crate::runtime::own_order(&runtime.core, fill));
                        journal.ledger(
                            &runtime.core,
                            &taker.take_ledger_events(),
                            Some((fill, Some(e.commission), received.received_us)),
                        )?;
                    }
                }
                if let Ok(parsed) = &decoded {
                    journal_update(
                        &journal,
                        &runtime.core,
                        &e,
                        parsed,
                        received.received_us,
                        was_ready && outcome.is_ok(),
                    )?;
                }
                if was_ready && !session.ready {
                    recovery_since = Instant::now();
                }
                if let Err(e) = outcome {
                    operator_required = true;
                    runtime.core.block(
                    "native event inconsistent; evidence retained; reconnect/reconcile required",
                );
                    journal.record(
                        json!({"type":"trading_blocked","error":e.to_string()}),
                        true,
                    )?;
                }
            }
            if !session.ready
                && runtime.trader.events.is_empty()
                && recovery_since.elapsed() >= Duration::from_millis(config.response_timeout_ms)
            {
                anyhow::ensure!(
                    !operator_required,
                    "Yida login/recovery timed out after a native inconsistency; operator recovery required; venue orders not cancelled"
                );
                return Err(journal.restart_required(
                    &mut runtime.core,
                    ColdRestartRequired::TransportOrControlTimeout,
                    json!({"stage":"login_recovery","transport_ready":runtime.trader.transport_ready(),"timeout_ms":config.response_timeout_ms}),
                ));
            }
            if let Some(request) = runtime
                .trader
                .next_response_deadline()
                .and_then(|_| runtime.trader.expired_response(Instant::now()))
            {
                runtime.core.block("native order/cancel response timed out; retain reservation and reconcile; no automatic retry");
                journal.record(json!({"type":"trading_blocked","request":request,"error":"native order/cancel response timed out; reservation retained; no automatic retry"}),true)?;
            }
            if let Some(taker) = &mut exec {
                let drops = runtime.trader.l1_dropped.load(Ordering::Acquire);
                if drops != checked_l1_drops
                    || !runtime.core.is_ready()
                    || !runtime.trader.transport_ready()
                    || !runtime.trader.l1_connected.load(Ordering::Acquire)
                {
                    taker.on_market_state(false, 0)?;
                    l1_generation = None;
                    checked_l1_drops = drops;
                }
                if let Some(ingress) = &target_ingress {
                    anyhow::ensure!(!ingress.is_finished(), "position target ingress stopped");
                    if runtime.trader.events.is_empty() {
                        let mut audit_error = None;
                        let can_trade = l1_generation.is_some()
                            && runtime.trader.l1_events.is_empty()
                            && runtime.trader.transport_ready()
                            && runtime.trader.l1_connected.load(Ordering::Acquire);
                        work |= ingress.poll(|target| {
                            let admitted = taker.accept(
                                target,
                                StrategyContext {
                                    core: &runtime.core,
                                    can_trade,
                                },
                            )?;
                            if let Err(error) = journal.target_accepted(&admitted) {
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
                        let result = (|| -> Result<serde_json::Value> {
                            match command? {
                                Command::ExecTarget { target } => {
                                    let taker = exec
                                        .as_mut()
                                        .ok_or_else(|| anyhow::anyhow!("exec is not configured"))?;
                                    let can_trade = l1_generation.is_some()
                                        && runtime.trader.l1_events.is_empty()
                                        && runtime.trader.transport_ready()
                                        && runtime.trader.l1_connected.load(Ordering::Acquire);
                                    let target = taker.accept(
                                        target,
                                        StrategyContext {
                                            core: &runtime.core,
                                            can_trade,
                                        },
                                    )?;
                                    Ok(
                                        json!({"type":"exec_target_accepted","target":target,"venue_accepted":false}),
                                    )
                                }
                                Command::AssignPosition {
                                    strategy,
                                    market,
                                    lots,
                                    price,
                                } => {
                                    let taker = exec
                                        .as_mut()
                                        .ok_or_else(|| anyhow::anyhow!("exec is not configured"))?;
                                    taker.assign_position(
                                        &runtime.core,
                                        &strategy,
                                        &market,
                                        lots,
                                        price,
                                        crate::persistence::now_us(),
                                    )?;
                                    journal.ledger(
                                        &runtime.core,
                                        &taker.take_ledger_events(),
                                        None,
                                    )?;
                                    Ok(
                                        json!({"type":"position_assigned","strategy":strategy,"market":market,"lots":lots,"price":price}),
                                    )
                                }
                                Command::State {} => {
                                    let transport_ready = runtime.trader.transport_ready();
                                    Ok(
                                        json!({"type":"state","state":runtime.core.view(),"order_group":config.order_group,"connections":session.connections.values().collect::<Vec<_>>(),"trading_protocol":config.trading_protocol,"transport_ready":transport_ready,"can_trade":runtime.execute && runtime.core.is_ready() && transport_ready}),
                                    )
                                }
                                Command::Shutdown {} => {
                                    stopping = true;
                                    Ok(json!({"type":"shutdown","venue_orders_cancelled":false}))
                                }
                                Command::Reconcile {} => {
                                    if let Some(taker) = &mut exec {
                                        taker.on_market_state(false, 0)?;
                                    }
                                    l1_generation = None;
                                    runtime.core.disconnect();
                                    session.ready = false;
                                    on_sdk(placement.as_ref(), || {
                                        runtime.trader.reconnect();
                                        Ok(())
                                    })?;
                                    recovery_since = Instant::now();
                                    Ok(json!({"type":"reconciling"}))
                                }
                                Command::Submit { .. } | Command::SubmitBatch { .. }
                                    if exec.is_some() =>
                                {
                                    anyhow::bail!(
                                        "exec owns account targets; manual submits are disabled"
                                    )
                                }
                                Command::Submit { intent } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    let receipt = runtime.submit_planned_with(intent, |p| {
                                        journal.prepared(day, p, None)
                                    })?;
                                    Ok(json!({"type":"submit_receipt","receipt":receipt}))
                                }
                                Command::Cancel {
                                    key,
                                    execution_route,
                                } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    runtime.cancel_with_route(&key, execution_route, |p| {
                                        journal.prepared_cancel(p)
                                    })?;
                                    Ok(json!({"type":"cancel_submitted_locally","key":key}))
                                }
                                Command::Connections {} => Ok(
                                    json!({"type":"connections","connections":session.connections.values().collect::<Vec<_>>()}),
                                ),
                                Command::SubmitBatch { intents } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    let receipt = runtime.submit_batch_with(intents, |p| {
                                        journal.prepared(day, p, None)
                                    })?;
                                    Ok(json!({"type":"submit_receipt","receipt":receipt}))
                                }
                                Command::CancelBatch {
                                    keys,
                                    execution_route,
                                } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    let receipt = runtime.cancel_batch_with_route(
                                        keys,
                                        execution_route,
                                        |p| journal.prepared_cancel(p),
                                    )?;
                                    Ok(json!({"type":"cancel_batch_receipt","receipt":receipt}))
                                }
                            }
                        })();
                        journal.command_state(&runtime.core, &result, cancel_keys)?;
                        journal.record(
                            result.unwrap_or_else(
                                |e| json!({"type":"command_rejected","error":e.to_string()}),
                            ),
                            true,
                        )?;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        commands_open = false;
                        stopping = target_ingress.is_none();
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                }
            }
            // L1 has its own bounded lossy queue; never shares reliable trade ingress.
            for _ in 0..256 {
                let Ok(quote) = runtime.trader.l1_events.try_recv() else {
                    break;
                };
                work = true;
                if let Err(error) = market.apply(quote) {
                    runtime.trader.l1_dropped.fetch_add(1, Ordering::Relaxed);
                    if Instant::now() >= next_l1_warning {
                        log::warn!("Yida L1 observation skipped: {error:#}");
                        next_l1_warning = Instant::now() + Duration::from_secs(5);
                    }
                    continue;
                }
                if let Some(taker) = &mut exec {
                    let can_trade = execute
                        && runtime.core.is_ready()
                        && runtime.trader.transport_ready()
                        && runtime.trader.l1_connected.load(Ordering::Acquire)
                        && runtime.trader.events.is_empty()
                        && runtime.trader.l1_events.is_empty()
                        && runtime.trader.l1_dropped.load(Ordering::Acquire) == checked_l1_drops;
                    let day = runtime.core.trading_day();
                    let Some(bbo) = quote.execution_quote(&day) else {
                        continue;
                    };
                    if !can_trade || bbo.generation < latest_l1_generation {
                        continue;
                    }
                    if l1_generation != Some(bbo.generation) {
                        taker.on_market_state(true, bbo.generation)?;
                        latest_l1_generation = bbo.generation;
                        l1_generation = Some(bbo.generation);
                    }
                    taker.on_quote(
                        bbo,
                        StrategyContext {
                            core: &runtime.core,
                            can_trade,
                        },
                        &mut actions,
                        crate::persistence::now_us(),
                    )?;
                    journal.ledger(&runtime.core, &taker.take_ledger_events(), None)?;
                    for action in actions.drain() {
                        let allowed = runtime.trader.transport_ready()
                            && runtime.trader.events.is_empty()
                            && runtime.trader.l1_events.is_empty()
                            && runtime.trader.l1_connected.load(Ordering::Acquire)
                            && runtime.trader.l1_dropped.load(Ordering::Acquire)
                                == checked_l1_drops
                            && !journal.failed()
                            && !runtime.trader.fault.load(Ordering::Acquire);
                        let result = strategy::dispatch(&mut runtime, &journal, action, allowed)?;
                        journal.strategy_result(&result, false)?;
                    }
                }
            }
            let transport_ready = runtime.trader.transport_ready();
            crate::position_ipc::publish(
                &mut position_ipc,
                &runtime.core,
                transport_ready,
                runtime.execute
                    && runtime.core.is_ready()
                    && transport_ready
                    && runtime.trader.events.is_empty(),
                cn_futures_state::RuntimeStatus::Running,
                false,
                crate::position_ipc::Observation {
                    account: None,
                    marks: market.marks(),
                    connected: runtime.trader.l1_connected.load(Ordering::Acquire),
                    dropped: runtime.trader.l1_dropped.load(Ordering::Relaxed),
                    reason: None,
                },
            );
            if work {
                idle.busy();
            } else {
                idle.wait(|select| {
                    select.recv(&runtime.trader.events);
                    select.recv(&runtime.trader.l1_events);
                });
            }
        }
        Ok(())
    })();
    if let Some(restart) = outcome
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<ColdRestartRequired>())
    {
        runtime.core.block(restart.observation_reason());
    }
    // Stop callback producers first, then retain every already-delivered tail event.
    let tail = runtime.trader.events.clone();
    let fault = runtime.trader.fault.clone();
    let mut core = runtime.core;
    on_sdk(placement.as_ref(), || {
        drop(runtime.trader);
        Ok(())
    })?;
    for received in tail.try_iter() {
        let e = received.event;
        journal.yida_event(&e, received.received_us, true)?;
        if e.kind == TRADE {
            if let Ok(fill) = e.to_fill() {
                if core.on_fill(&fill).is_err() {
                    core.block("shutdown fill requires recovery");
                }
                journal.fill(&core, &fill, Some(e.commission), received.received_us, None)?;
            }
        }
        if e.kind == ORDER {
            if let Ok(update) = e.recovered_key().and_then(|key| e.to_order(key)) {
                let key = update.key;
                if core.on_order(&update).is_ok() {
                    journal.orders(&core, &[key])?;
                } else {
                    core.block("shutdown order requires recovery");
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
        if outcome.is_ok() && shutdown_ok {
            cn_futures_state::RuntimeStatus::Stopped
        } else {
            cn_futures_state::RuntimeStatus::Failed
        },
        true,
        crate::position_ipc::Observation {
            account: None,
            marks: market.marks(),
            connected: false,
            dropped: 0,
            reason: None,
        },
    );
    journal.record(
        json!({"type":"runtime_stopped","venue_orders_cancelled":false}),
        true,
    )?;
    journal.finish()?;
    if let Some(publisher) = position_ipc {
        publisher.finish()?;
    }
    anyhow::ensure!(
        shutdown_ok,
        "Yida callback pipeline/destruction failed on shutdown; evidence retained"
    );
    outcome
}

fn on_background<T>(
    placement: Option<&crate::placement::Placement>,
    call: impl FnOnce() -> Result<T>,
) -> Result<T> {
    match placement {
        Some(p) => p.on_background(call),
        None => call(),
    }
}

fn on_sdk<T>(
    placement: Option<&crate::placement::Placement>,
    call: impl FnOnce() -> Result<T>,
) -> Result<T> {
    match placement {
        Some(p) => p.on_sdk(call),
        None => call(),
    }
}

/// Shared production/benchmark entry for already validated live factual updates.
/// Returns whether a new factual fill was applied.
pub fn apply_live_update(
    runtime: &mut SingleLoop<YidaTrader>,
    e: &RawEvent,
    parsed: &DecodedUpdate,
) -> Result<bool> {
    match parsed {
        DecodedUpdate::Order(order) => {
            runtime.trader.observe_order(e, order)?;
            runtime.core.on_order(order)?;
        }
        DecodedUpdate::Trade(fill) => return runtime.core.on_fill(fill),
        _ if e.kind == ACCOUNT => runtime.core.constrain_cash(e.available)?,
        _ => {}
    }
    Ok(false)
}
pub fn journal_update(
    journal: &PersistChannel,
    core: &TradingCore,
    e: &RawEvent,
    parsed: &DecodedUpdate,
    received_us: u64,
    applied: bool,
) -> Result<()> {
    match parsed {
        // A live fill the Exec attributed is already recorded; replayed and
        // unattributed fills are recorded here once, unallocated.
        DecodedUpdate::Trade(fill) => {
            journal.fill(core, fill, Some(e.commission), received_us, None)?;
        }
        DecodedUpdate::Order(order) if applied => {
            journal.orders(core, std::slice::from_ref(&order.key))?
        }
        _ => {}
    }
    Ok(())
}
