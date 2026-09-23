use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::OnceLock;

use serde::Serialize;

use crate::model_output_hub::ModelOutputScoreLookupResult;

const STATUS_PATH: &str = "run/cta_signal_status.json";

#[derive(Serialize)]
struct SymbolStatus {
    model_service: String,
    model_ts_ms: i64,
    score: Option<f64>,
    quantile: Option<f64>,
    long_threshold: Option<f64>,
    short_threshold: Option<f64>,
    nq_long_value: Option<f64>,
    nq_long_quantile: Option<f64>,
    nq_long_threshold: Option<f64>,
    nq_short_value: Option<f64>,
    nq_short_quantile: Option<f64>,
    nq_short_threshold: Option<f64>,
    score_ready: bool,
    filter_ready: bool,
    decision: &'static str,
}

#[derive(Default, Serialize)]
struct RuntimeStatus {
    updated_ts_us: i64,
    rules: HashMap<String, HashMap<String, SymbolStatus>>,
}

struct Update {
    rule_id: String,
    symbol: String,
    status: SymbolStatus,
}

static STATUS_SENDER: OnceLock<Option<SyncSender<Update>>> = OnceLock::new();

pub fn record(rule_id: &str, symbol: &str, lookup: &ModelOutputScoreLookupResult, vote: i8) {
    let sender = STATUS_SENDER.get_or_init(|| {
        let (sender, receiver) = sync_channel::<Update>(256);
        match std::thread::Builder::new()
            .name("cta_signal_status".to_owned())
            .spawn(move || {
                let mut status = RuntimeStatus::default();
                for update in receiver {
                    let symbols = status.rules.entry(update.rule_id).or_default();
                    if symbols.get(&update.symbol).is_some_and(|previous| {
                        previous.model_ts_ms == update.status.model_ts_ms
                            && previous.model_service == update.status.model_service
                    }) {
                        continue;
                    }
                    symbols.insert(update.symbol, update.status);
                    status.updated_ts_us = runtime_common::time_util::get_timestamp_us();
                    write_status(&status);
                }
            }) {
            Ok(_) => Some(sender),
            Err(error) => {
                log::warn!("CTA signal status worker: {error}");
                None
            }
        }
    });
    if let Some(sender) = sender {
        let _ = sender.try_send(Update {
            rule_id: rule_id.to_owned(),
            symbol: symbol.to_owned(),
            status: SymbolStatus {
                model_service: lookup.service_name.clone(),
                model_ts_ms: lookup.score_ts_ms,
                score: lookup.score.filter(|value| value.is_finite()),
                quantile: lookup.score_quantile.filter(|value| value.is_finite()),
                long_threshold: lookup
                    .score_long_threshold
                    .filter(|value| value.is_finite()),
                short_threshold: lookup
                    .score_short_threshold
                    .filter(|value| value.is_finite()),
                nq_long_value: lookup.filter_long_value.filter(|value| value.is_finite()),
                nq_long_quantile: lookup
                    .filter_long_quantile
                    .filter(|value| value.is_finite()),
                nq_long_threshold: lookup
                    .filter_long_threshold
                    .filter(|value| value.is_finite()),
                nq_short_value: lookup.filter_short_value.filter(|value| value.is_finite()),
                nq_short_quantile: lookup
                    .filter_short_quantile
                    .filter(|value| value.is_finite()),
                nq_short_threshold: lookup
                    .filter_short_threshold
                    .filter(|value| value.is_finite()),
                score_ready: lookup.score_ready,
                filter_ready: lookup.filter_ready,
                decision: match vote {
                    1 => "long",
                    -1 => "short",
                    _ => "flat",
                },
            },
        });
    }
}

fn write_status(status: &RuntimeStatus) {
    let path = Path::new(STATUS_PATH);
    if let Err(error) = fs::create_dir_all(path.parent().unwrap_or(Path::new("."))) {
        log::warn!("CTA signal status directory: {error}");
        return;
    }
    let result = serde_json::to_vec(status)
        .map_err(std::io::Error::other)
        .and_then(|payload| fs::write(path.with_extension("json.tmp"), payload))
        .and_then(|_| fs::rename(path.with_extension("json.tmp"), path));
    if let Err(error) = result {
        log::warn!("CTA signal status write: {error}");
    }
}
