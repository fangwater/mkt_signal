pub mod config;
pub mod market;
pub mod positions;
pub mod reader;
pub mod state;

use crate::server::{serve_dashboard, WsHub};
use anyhow::Result;
use config::Config;
use serde_json::{json, Value};
use std::{cell::RefCell, rc::Rc, time::Duration};
use tokio::sync::watch;

const DASHBOARD: &str = include_str!("../../../../web/cn_futures_viz/index.html");

fn now_us() -> u64 {
    runtime_common::time_util::get_timestamp_us().max(0) as u64
}

/// Dedicated secondary readers keep RocksDB I/O off both the WS and trading loops.
fn spawn_counter(
    source: config::Counter,
    limit: usize,
    poll_ms: u64,
    stale_ms: u64,
) -> (
    watch::Receiver<Value>,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let initial = json!({"id":source.id,"label":source.label,"account":source.account,"adapter":source.adapter,
        "read_at_us":null,"read_error":null,"lifecycle":{"status":"waiting"},"orders":[],"fills":[]});
    let (tx, rx) = watch::channel(initial);
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut reader = None;
        let mut last_error = None;
        loop {
            let result = (|| -> Result<Value> {
                if reader.is_none() {
                    reader = Some(reader::Reader::open(&source)?);
                }
                reader.as_mut().expect("reader opened above").read(
                    &source,
                    limit,
                    now_us(),
                    stale_ms,
                )
            })();
            let value = match result {
                Ok(v) => {
                    last_error = None;
                    v
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    if last_error.as_ref() != Some(&message) {
                        log::warn!("native viz source {}: {error:#}", source.id);
                    }
                    last_error = Some(message);
                    reader = None;
                    json!({"id":source.id,"label":source.label,"account":source.account,"adapter":source.adapter,
                        "read_at_us":now_us(),"read_error":"execution store unavailable; check source config and server logs",
                        "lifecycle":{"status":"unavailable"},"orders":[],"fills":[]})
                }
            };
            if tx.send(value).is_err() {
                break;
            }
            match stop_rx.recv_timeout(Duration::from_millis(poll_ms)) {
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                _ => break,
            }
        }
    });
    (rx, stop_tx, handle)
}

pub async fn run(cfg: Config) -> Result<()> {
    cfg.validate()?;
    let local = tokio::task::LocalSet::new();
    local.run_until(async move {
        let hub = WsHub::new(128);
        let mut counters = Vec::new();
        let mut workers = Vec::new();
        let mut positions = Vec::new();
        let mut listeners = Vec::new();
        for source in cfg.counters {
            let (state,listener)=positions::spawn(source.clone());
            positions.push(state);
            listeners.push(listener);
            let (rx, stop, handle) = spawn_counter(source, cfg.row_limit, cfg.poll_interval_ms, cfg.stale_after_ms);
            counters.push(rx);
            workers.push((stop, handle));
        }
        let markets: Vec<_> = cfg.market_data.into_iter().map(|md| Rc::new(RefCell::new(market::Market::new(md)))).collect();
        for market in &markets {
            for venue in market.borrow().config.venues.clone() {
                let market = market.clone();
                listeners.push(tokio::task::spawn_local(async move {
                    let mut subscriber = None;
                    let mut retry = tokio::time::Instant::now();
                    let mut interval = tokio::time::interval(Duration::from_millis(10));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        interval.tick().await;
                        if subscriber.is_none() && tokio::time::Instant::now() >= retry {
                            let root = market.borrow().config.service_root.clone();
                            match market::Subscription::open(&root, &venue) {
                                Ok(s) => { subscriber = Some(s); market.borrow_mut().attached(&venue); }
                                Err(_) => { market.borrow_mut().unavailable(&venue, "waiting for market IPC"); }
                            }
                            retry = tokio::time::Instant::now() + Duration::from_secs(1);
                        }
                        if let Some(s) = &subscriber {
                            let result = s.drain(&mut market.borrow_mut(), &venue);
                            if let Err(error) = result {
                                log::warn!("native market {}: {error:#}", venue);
                                market.borrow_mut().unavailable(&venue, "market IPC read failed");
                                subscriber = None;
                            }
                        }
                    }
                }));
            }
        }
        let ws_path = cfg.http.ws_path.clone();
        let http_hub = hub.clone();
        let observer_stale_ms = cfg.stale_after_ms;
        let pump = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(cfg.poll_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now = now_us();
                let sources: Vec<_> = counters.iter().zip(&positions).map(|(rx,positions)| {
                    let mut value = rx.borrow().clone();
                    value["observer_stale"] = json!(state::stale(now, value["read_at_us"].as_u64(), observer_stale_ms));
                    value["position_state"] = positions.borrow().view(now,observer_stale_ms);
                    value
                }).collect();
                hub.broadcast(json!({"type":"cn_execution","ts_us":now,"ws_path":ws_path,"update_interval_ms":cfg.poll_interval_ms,"sources":sources}).to_string());
                let sources: Vec<_> = markets.iter().map(|m| m.borrow().snapshot(now)).collect();
                hub.broadcast(json!({"type":"cn_market","ts_us":now,"sources":sources}).to_string());
            }
        });
        // serve_dashboard shares crypto Viz's snapshot cache, WS replay, bounded
        // broadcast queue, 1s coalescing and slow-client send timeout.
        let outcome = tokio::select! {
            result = serve_dashboard(cfg.http, http_hub, Some(DASHBOARD), None) => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        };
        pump.abort();
        for listener in listeners { listener.abort(); }
        for (stop, _) in &workers { let _ = stop.send(()); }
        for (_, handle) in workers { let _ = handle.join(); }
        outcome
    }).await
}

#[cfg(test)]
mod tests;
