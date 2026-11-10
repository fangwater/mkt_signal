use super::{config::Config, sdk::YidaTrader, session::Session, *};
use crate::{
    core::{SingleLoop, TradingCore},
    runtime::{command_order_keys, input_commands, lock_flow, Command, Journal},
};
use anyhow::Result;
use serde_json::json;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

pub fn run(config: Config, execute: bool) -> Result<()> {
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
    let identity =
        crate::persistence::store_identity("yida", &[&endpoint, credentials.user.as_bytes()]);
    let journal = Journal::start(
        &config.journal,
        config.event_capacity,
        redactions,
        &database,
        identity,
        config.sync_before_send,
    )?;
    let trader = YidaTrader::new(&config, credentials)?;
    let mut runtime = SingleLoop {
        core: TradingCore::new(config.risk.clone(), scope())?,
        trader,
        execute,
    };
    let mut session = Session::default();
    let input = input_commands()?;
    journal.record(json!({"type":"runtime_start","adapter":"yida","sdk_reference":SDK_REFERENCE,"trading_protocol":config.trading_protocol,"public_simulation":config.public_simulation,"business_core":config.business_core,"idle_sleep_us":config.idle_sleep_us,"execute":execute,"venues":["SHFE","INE"]}),true)?;
    runtime.trader.start()?;
    // Create input, journal and SDK workers before pinning this business thread.
    // SDK workers have their own explicit CPUIDs when business_core is configured.
    if let Some(core) = config.business_core {
        pin_business_thread(core)?;
    }
    let mut stopping = false;
    let mut transport_seen_ready = false;
    let mut recovery_since = Instant::now();
    let outcome = (|| -> Result<()> {
        while !stopping {
            anyhow::ensure!(!runtime.trader.fault.load(Ordering::Acquire) && !journal.failed.load(Ordering::Acquire),"reliable Yida ingress/journal failed; stop and reconcile before retry; venue orders not cancelled");
            let mut work = false;
            for _ in 0..256 {
                let Ok(received) = runtime.trader.events.try_recv() else {
                    break;
                };
                work = true;
                let e = received.event;
                journal.record(
                    json!({"type":"yida_event","received_us":received.received_us,"event":e}),
                    false,
                )?;
                if e.kind == CONNECTED {
                    transport_seen_ready = false;
                    runtime.core.connected();
                    recovery_since = Instant::now();
                }
                if e.kind == DISCONNECTED {
                    transport_seen_ready = false;
                    runtime.core.disconnect();
                    recovery_since = Instant::now();
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
                    runtime.trader.login()?;
                }
                let was_ready = session.ready;
                let outcome = (|| -> Result<()> {
                    if session.is_pre_snapshot_rejection(&e) {
                        // Validate and retain the raw login replay without assigning ownership.
                        // No core reservations are released; unresolved local orders must still
                        // reconcile against the complete caught-up snapshot.
                        e.to_order(OrderKey {
                            session: group_session_key(e.day, e.order_group),
                            order_ref: e.reference.to_string(),
                        })?;
                        return Ok(());
                    }
                    let key = if e.kind == ORDER {
                        Some(runtime.trader.key(&e)?)
                    } else {
                        None
                    };
                    if e.kind == ORDER {
                        e.to_order(
                            key.clone()
                                .ok_or_else(|| anyhow::anyhow!("missing native order key"))?,
                        )?;
                        runtime.trader.observe_order(&e)?;
                    }

                    if let Some(s) = session.on_event(&e, &config, key.clone())? {
                        let trades = s.trades.clone();
                        runtime.core.restore(s)?;
                        if let Err(error) = journal.reconciled(&mut runtime.core, trades) {
                            runtime.core.block("durable recovery failed");
                            return Err(error);
                        }
                        runtime.trader.recovered();
                        return Ok(());
                    }
                    if session.ready && e.replay == 0 {
                        match e.kind {
                            ORDER => runtime.core.on_order(e.to_order(key.unwrap())?)?,
                            TRADE => {
                                runtime.core.on_fill(e.to_fill()?)?;
                            }
                            // The SDK explicitly does not promise one-to-one cancel-error
                            // correlation. Preserve this evidence without changing OMS state.
                            FAILED_CANCEL => {}
                            ACCOUNT => runtime.core.constrain_cash(e.available)?,
                            _ => {}
                        }
                    }
                    Ok(())
                })();
                if e.kind == TRADE {
                    if let Ok(fill) = e.to_fill() {
                        let mutation = if was_ready && outcome.is_ok() {
                            crate::persistence::projection_for(
                                &runtime.core,
                                &runtime
                                    .core
                                    .fill_order_key(&fill)
                                    .into_iter()
                                    .collect::<Vec<_>>(),
                                &[],
                            )
                        } else {
                            Default::default()
                        };
                        journal.trade_with_projection(
                            fill,
                            Some(e.commission),
                            Some(e.native_time),
                            received.received_us,
                            mutation,
                        )?;
                    }
                }
                if was_ready && outcome.is_ok() && matches!(e.kind, ORDER | FAILED_CANCEL) {
                    let observed = if e.kind == ORDER {
                        runtime.trader.key(&e).ok().into_iter().collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    journal.orders(&runtime.core, &observed, &observed)?;
                }
                if was_ready && !session.ready {
                    recovery_since = Instant::now();
                }
                if let Err(e) = outcome {
                    runtime.core.block(
                    "native event inconsistent; evidence retained; reconnect/reconcile required",
                );
                    journal.record(
                        json!({"type":"trading_blocked","error":e.to_string()}),
                        true,
                    )?;
                }
            }
            anyhow::ensure!(
                session.ready
                    || recovery_since.elapsed() < Duration::from_millis(config.response_timeout_ms),
                "Yida login/recovery timed out; existing venue orders not cancelled"
            );
            if let Some(request) = runtime.trader.expired_response(
                Instant::now(),
                Duration::from_millis(config.response_timeout_ms),
            ) {
                runtime.core.block("native order/cancel response timed out; retain reservation and reconcile; no automatic retry");
                journal.record(json!({"type":"trading_blocked","request":request,"error":"native order/cancel response timed out; reservation retained; no automatic retry"}),true)?;
            }
            if runtime.trader.events.is_empty() {
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
                                    runtime.core.disconnect();
                                    session.ready = false;
                                    runtime.trader.reconnect();
                                    recovery_since = Instant::now();
                                    Ok(json!({"type":"reconciling"}))
                                }
                                Command::Submit { intent } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    let receipt = runtime.submit_planned_with(
                                        intent,
                                        Instant::now(),
                                        |p| journal.prepared_submit(&day, p),
                                    )?;
                                    Ok(json!({"type":"submit_receipt","receipt":receipt}))
                                }
                                Command::Cancel {
                                    key,
                                    execution_route,
                                } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    runtime.cancel_with_route(
                                        &key,
                                        execution_route,
                                        Instant::now(),
                                        |p| journal.prepared_cancel(p),
                                    )?;
                                    Ok(json!({"type":"cancel_submitted_locally","key":key}))
                                }
                                Command::Connections {} => Ok(
                                    json!({"type":"connections","connections":session.connections.values().collect::<Vec<_>>()}),
                                ),
                                Command::SubmitBatch { intents } => {
                                    runtime.trader.ensure_transport_ready()?;
                                    let receipt = runtime.submit_batch_with(
                                        intents,
                                        Instant::now(),
                                        |p| journal.prepared_submit(&day, p),
                                    )?;
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
                                        Instant::now(),
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
                    Err(crossbeam_channel::TryRecvError::Disconnected) => stopping = true,
                    Err(crossbeam_channel::TryRecvError::Empty) => {}
                }
            }
            if !work {
                if config.idle_sleep_us == 0 {
                    std::hint::spin_loop();
                } else {
                    std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
                }
            }
        }
        Ok(())
    })();
    // Stop callback producers first, then retain every already-delivered tail event.
    let tail = runtime.trader.events.clone();
    let fault = runtime.trader.fault.clone();
    let mut core = runtime.core;
    drop(runtime.trader);
    for received in tail.try_iter() {
        let e = received.event;
        journal.record(json!({"type":"yida_event","received_us":received.received_us,"event":e,"shutdown_tail":true}), false)?;
        if e.kind == TRADE {
            if let Ok(fill) = e.to_fill() {
                journal.trade(
                    fill.clone(),
                    Some(e.commission),
                    Some(e.native_time),
                    received.received_us,
                )?;
                if core.on_fill(fill).is_err() {
                    core.block("shutdown fill requires recovery");
                }
            }
        }
        if e.kind == ORDER {
            if let Ok(update) = e.recovered_key().and_then(|key| e.to_order(key)) {
                let key = update.key.clone();
                if core.on_order(update).is_ok() {
                    journal.state(&core, &[key])?;
                } else {
                    core.block("shutdown order requires recovery");
                }
            }
        }
    }
    journal.state(&core, &[])?;
    let shutdown_ok = !fault.load(Ordering::Acquire);
    journal.record(
        json!({"type":"runtime_stopped","venue_orders_cancelled":false}),
        true,
    )?;
    journal.finish()?;
    anyhow::ensure!(
        shutdown_ok,
        "Yida callback pipeline/destruction failed on shutdown; evidence retained"
    );
    outcome
}

#[cfg(target_os = "linux")]
fn pin_business_thread(core: usize) -> Result<()> {
    anyhow::ensure!(
        core < libc::CPU_SETSIZE as usize,
        "business_core exceeds CPU_SETSIZE"
    );
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
            anyhow::bail!(
                "pin Yida business thread to CPU {core}: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_business_thread(_core: usize) -> Result<()> {
    anyhow::bail!("business_core pinning requires Linux")
}
