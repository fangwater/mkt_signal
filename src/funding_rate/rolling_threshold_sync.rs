use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::spread_factor::SpreadFactor;
use super::xarb_funding_threshold_loader::XarbFundingThresholdsResolved;

const XARB_SPREAD_REDIS_SYNC_SECS: u64 = 1800;

thread_local! {
    static XARB_SPREAD_REDIS_SYNC_CACHE: RefCell<HashMap<String, Instant>> =
        RefCell::new(HashMap::new());
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StoredXarbMappingConfig {
    pub(crate) rolling_key: Option<String>,
    pub(crate) mapping: HashMap<String, String>,
}

pub(crate) fn format_quantile_field_ref(prefix: &str, percentile: f64) -> String {
    let suffix = if (percentile - percentile.round()).abs() < 1e-9 {
        format!("{}", percentile.round() as i64)
    } else {
        let mut text = percentile.to_string();
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    };
    format!("{prefix}_{suffix}")
}

pub(crate) fn resolve_symbol_single_quantile_thresholds(
    rolling_payloads: &HashMap<String, serde_json::Value>,
    field_ref: &str,
) -> (HashMap<String, f64>, usize) {
    let mut resolved = HashMap::new();
    let mut missing_refs = 0usize;

    for (symbol, payload) in rolling_payloads {
        match extract_quantile_value(payload, field_ref) {
            Some(value) => {
                resolved.insert(symbol.clone(), value);
            }
            None => {
                missing_refs += 1;
            }
        }
    }

    (resolved, missing_refs)
}

pub(crate) fn default_xarb_spread_mapping() -> HashMap<String, String> {
    HashMap::from([
        ("forward_open_mm".to_string(), "spread_5".to_string()),
        ("forward_open_mt".to_string(), "bidask_10".to_string()),
        ("forward_cancel_mm".to_string(), "spread_10".to_string()),
        ("forward_cancel_mt".to_string(), "bidask_15".to_string()),
        ("backward_open_mm".to_string(), "spread_95".to_string()),
        ("backward_open_mt".to_string(), "askbid_90".to_string()),
        ("backward_cancel_mm".to_string(), "spread_90".to_string()),
        ("backward_cancel_mt".to_string(), "askbid_85".to_string()),
    ])
}

fn default_xarb_spread_threshold_order() -> [&'static str; 8] {
    [
        "forward_open_mm",
        "forward_open_mt",
        "forward_cancel_mm",
        "forward_cancel_mt",
        "backward_open_mm",
        "backward_open_mt",
        "backward_cancel_mm",
        "backward_cancel_mt",
    ]
}

pub(crate) fn default_xarb_funding_mapping(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> HashMap<String, String> {
    if open_venue.is_futures() && hedge_venue.is_futures() {
        HashMap::from([
            ("forward_open_mm".to_string(), "spread_fr_80".to_string()),
            ("backward_open_mm".to_string(), "spread_fr_20".to_string()),
        ])
    } else {
        HashMap::from([
            (
                "forward_open_mm".to_string(),
                "hedge_premium_rate_50".to_string(),
            ),
            (
                "backward_open_mm".to_string(),
                "hedge_premium_rate_50".to_string(),
            ),
        ])
    }
}

pub(crate) fn xarb_spread_mapping_key(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    format!(
        "xarb_spread_thresholds_config_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

pub(crate) fn xarb_funding_mapping_key(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> String {
    format!(
        "xarb_funding_thresholds_config_{}_{}",
        open_venue.data_pub_slug(),
        hedge_venue.data_pub_slug()
    )
}

fn normalize_threshold_mapping(mapping: HashMap<String, String>) -> HashMap<String, String> {
    mapping
        .into_iter()
        .filter_map(|(key, value)| {
            let k = key.trim().to_string();
            let v = value.trim().to_string();
            if k.is_empty() || v.is_empty() {
                None
            } else {
                Some((k, v))
            }
        })
        .collect()
}

pub(crate) fn parse_xarb_mapping_config(
    raw: Option<String>,
    default_mapping: HashMap<String, String>,
) -> StoredXarbMappingConfig {
    let Some(text) = raw else {
        return StoredXarbMappingConfig {
            rolling_key: None,
            mapping: default_mapping,
        };
    };

    let parsed: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => {
            warn!("解析 xarb mapping config 失败，回退默认 mapping: {err}");
            return StoredXarbMappingConfig {
                rolling_key: None,
                mapping: default_mapping,
            };
        }
    };

    let rolling_key = parsed
        .get("rolling_key")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let mapping = parsed
        .get("mapping")
        .and_then(|value| value.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(key, value)| {
                    let raw = value.as_str()?.trim();
                    if raw.is_empty() {
                        None
                    } else {
                        Some((key.trim().to_string(), raw.to_string()))
                    }
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_else(|| default_mapping.clone());

    let mapping = normalize_threshold_mapping(mapping);
    StoredXarbMappingConfig {
        rolling_key,
        mapping: if mapping.is_empty() {
            default_mapping
        } else {
            mapping
        },
    }
}

pub(crate) fn normalize_xarb_symbol(symbol: &str) -> String {
    normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures).to_ascii_uppercase()
}

fn normalize_quantile(raw: &serde_json::Value) -> Option<f64> {
    let value = match raw {
        serde_json::Value::Number(number) => number.as_f64()?,
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };

    let quantile = if value > 1.0 { value / 100.0 } else { value };
    if (0.0..=1.0).contains(&quantile) {
        Some(quantile)
    } else {
        None
    }
}

fn normalize_quantile_value(raw: &serde_json::Value) -> Option<f64> {
    let value = match raw {
        serde_json::Value::Number(number) => number.as_f64()?,
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

pub(crate) fn extract_quantile_value(payload: &serde_json::Value, field_ref: &str) -> Option<f64> {
    let (factor, percentile_text) = field_ref.rsplit_once('_')?;
    let percentile = percentile_text.trim().parse::<f64>().ok()? / 100.0;
    let quantiles_key = format!("{}_quantiles", factor.trim());
    let quantiles = payload.get(&quantiles_key)?.as_array()?;

    for item in quantiles {
        let obj = item.as_object()?;
        let q_raw = obj.get("q").or_else(|| obj.get("quantile"))?;
        let q = normalize_quantile(q_raw)?;
        if (q - percentile).abs() >= 1e-9 {
            continue;
        }
        for value_key in ["v", "threshold", "value"] {
            if let Some(raw) = obj.get(value_key) {
                return normalize_quantile_value(raw);
            }
        }
    }

    None
}

pub(crate) fn parse_xarb_rolling_payloads(
    rolling_map: HashMap<String, String>,
    active_symbols: &HashSet<String>,
) -> HashMap<String, serde_json::Value> {
    let mut payloads = HashMap::new();

    for (field, raw_json) in rolling_map {
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&raw_json) else {
            continue;
        };
        let symbol = payload
            .get("base_symbol")
            .and_then(|value| value.as_str())
            .or_else(|| payload.get("symbol").and_then(|value| value.as_str()))
            .unwrap_or_else(|| field.split("::").last().unwrap_or(field.as_str()));
        let symbol_key = normalize_xarb_symbol(symbol);
        if symbol_key.is_empty() {
            continue;
        }
        if !active_symbols.is_empty() && !active_symbols.contains(&symbol_key) {
            continue;
        }
        payloads.insert(symbol_key, payload);
    }

    payloads
}

pub(crate) fn resolve_symbol_quantile_thresholds(
    rolling_payloads: &HashMap<String, serde_json::Value>,
    mapping: &HashMap<String, String>,
) -> (HashMap<String, HashMap<String, f64>>, usize, Vec<String>) {
    let mut resolved = HashMap::new();
    let mut missing_refs = 0usize;
    let mut skipped_symbols = Vec::new();

    for (symbol, payload) in rolling_payloads {
        let mut values = HashMap::new();
        let mut missing = false;
        for (dest_field, field_ref) in mapping {
            match extract_quantile_value(payload, field_ref) {
                Some(value) => {
                    values.insert(dest_field.clone(), value);
                }
                None => {
                    missing_refs += 1;
                    missing = true;
                    break;
                }
            }
        }

        if missing {
            skipped_symbols.push(symbol.clone());
            continue;
        }

        if !values.is_empty() {
            resolved.insert(symbol.clone(), values);
        }
    }

    (resolved, missing_refs, skipped_symbols)
}

pub(crate) fn apply_xarb_spread_thresholds(
    resolved: &HashMap<String, HashMap<String, f64>>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> usize {
    let spread_factor = SpreadFactor::instance();
    spread_factor.clear_thresholds();

    let mut applied = 0usize;
    for (symbol, values) in resolved {
        if let (Some(mm), Some(mt)) = (values.get("forward_open_mm"), values.get("forward_open_mt"))
        {
            spread_factor.set_forward_open_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
        if let (Some(mm), Some(mt)) = (
            values.get("forward_cancel_mm"),
            values.get("forward_cancel_mt"),
        ) {
            spread_factor.set_forward_open_cancel_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
        if let (Some(mm), Some(mt)) = (
            values.get("backward_open_mm"),
            values.get("backward_open_mt"),
        ) {
            spread_factor.set_backward_open_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
        if let (Some(mm), Some(mt)) = (
            values.get("backward_cancel_mm"),
            values.get("backward_cancel_mt"),
        ) {
            spread_factor.set_backward_cancel_threshold(
                open_venue,
                symbol,
                hedge_venue,
                symbol,
                *mm,
                *mt,
            );
            applied += 1;
        }
    }

    applied
}

pub(crate) fn resolve_xarb_funding_thresholds(
    resolved: &HashMap<String, HashMap<String, f64>>,
) -> HashMap<String, XarbFundingThresholdsResolved> {
    resolved
        .iter()
        .filter_map(|(symbol, values)| {
            Some((
                symbol.clone(),
                XarbFundingThresholdsResolved {
                    forward_open: *values.get("forward_open_mm")?,
                    backward_open: *values.get("backward_open_mm")?,
                },
            ))
        })
        .collect()
}

pub(crate) fn xarb_spread_threshold_order(mapping: &HashMap<String, String>) -> Vec<String> {
    let mut ordered = Vec::new();
    let mut seen = HashSet::new();

    for key in default_xarb_spread_threshold_order() {
        if mapping.contains_key(key) {
            ordered.push(key.to_string());
            seen.insert(key.to_string());
        }
    }

    let mut extra_keys: Vec<String> = mapping
        .keys()
        .filter(|key| !seen.contains(*key))
        .cloned()
        .collect();
    extra_keys.sort();
    ordered.extend(extra_keys);
    ordered
}

pub(crate) fn build_xarb_spread_sync_entries(
    resolved: &HashMap<String, HashMap<String, f64>>,
    threshold_order: &[String],
) -> Vec<(String, String)> {
    let mut symbols: Vec<&String> = resolved.keys().collect();
    symbols.sort();

    let mut entries = Vec::new();
    for symbol in symbols {
        let Some(values) = resolved.get(symbol) else {
            continue;
        };
        for suffix in threshold_order {
            let Some(value) = values.get(suffix) else {
                continue;
            };
            let rendered = format!("{value:.8}")
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string();
            entries.push((format!("{symbol}_{suffix}"), rendered));
        }
    }

    entries
}

pub(crate) async fn sync_xarb_spread_thresholds_to_redis(
    redis: &RedisSettings,
    sync_key: &str,
    config_key: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    rolling_key: &str,
    mapping: &HashMap<String, String>,
    resolved: &HashMap<String, HashMap<String, f64>>,
) -> Result<()> {
    let refresh_interval = Duration::from_secs(XARB_SPREAD_REDIS_SYNC_SECS);

    let mut cache_fresh = false;
    XARB_SPREAD_REDIS_SYNC_CACHE.with(|cell| {
        let cache = cell.borrow();
        if let Some(last_sync_at) = cache.get(sync_key) {
            cache_fresh = last_sync_at.elapsed() < refresh_interval;
        }
    });
    if cache_fresh {
        debug!(
            "xarb spread 阈值 Redis sync 命中缓存 (key={} refresh={}s)",
            sync_key, XARB_SPREAD_REDIS_SYNC_SECS
        );
        return Ok(());
    }

    let threshold_order = xarb_spread_threshold_order(mapping);
    let entries = build_xarb_spread_sync_entries(resolved, &threshold_order);
    if entries.is_empty() {
        warn!(
            "xarb spread 阈值 Redis sync 跳过：无可写入字段 (key={} rolling_key={} mapping_fields={})",
            sync_key,
            rolling_key,
            mapping.len()
        );
        XARB_SPREAD_REDIS_SYNC_CACHE.with(|cell| {
            cell.borrow_mut()
                .insert(sync_key.to_string(), Instant::now());
        });
        return Ok(());
    }

    let entry_fields: HashSet<String> = entries.iter().map(|(field, _)| field.clone()).collect();
    let mut client = RedisClient::connect(redis.clone()).await?;
    let stale_fields = match client.hgetall_map(sync_key).await {
        Ok(existing) => existing
            .keys()
            .filter(|field| !entry_fields.contains(*field))
            .cloned()
            .collect::<Vec<_>>(),
        Err(err) => {
            warn!(
                "读取旧 xarb spread 阈值失败，继续覆盖写入 (key={}): {:?}",
                sync_key, err
            );
            Vec::new()
        }
    };

    client
        .hset_multiple_str(sync_key, &entries)
        .await
        .with_context(|| format!("写入 xarb spread 阈值失败 (key={sync_key})"))?;
    if !stale_fields.is_empty() {
        client
            .hdel_fields(sync_key, &stale_fields)
            .await
            .with_context(|| format!("清理旧 xarb spread 阈值失败 (key={sync_key})"))?;
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "namespace": "xarb",
        "open_venue": open_venue.data_pub_slug(),
        "hedge_venue": hedge_venue.data_pub_slug(),
        "rolling_key": rolling_key,
        "mapping": mapping,
        "threshold_order": threshold_order,
        "generated_at": Utc::now().to_rfc3339(),
    });
    client
        .set_json(config_key, &payload)
        .await
        .with_context(|| format!("写入 xarb spread sync 配置失败 (key={config_key})"))?;

    XARB_SPREAD_REDIS_SYNC_CACHE.with(|cell| {
        cell.borrow_mut()
            .insert(sync_key.to_string(), Instant::now());
    });
    info!(
        "xarb spread 阈值已同步到 Redis key={} config_key={} fields={} stale_removed={} interval={}s",
        sync_key,
        config_key,
        entries.len(),
        stale_fields.len(),
        XARB_SPREAD_REDIS_SYNC_SECS
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_xarb_spread_sync_entries, default_xarb_funding_mapping, extract_quantile_value,
        normalize_xarb_symbol, parse_xarb_mapping_config, xarb_spread_threshold_order,
    };
    use crate::signal::common::TradingVenue;
    use std::collections::HashMap;

    #[test]
    fn xarb_extract_quantile_value_supports_factor_refs_with_underscores() {
        let payload = serde_json::json!({
            "spread_fr_quantiles": [
                {"q": 0.2, "v": -0.001},
                {"q": 0.8, "v": 0.003}
            ]
        });
        let value = extract_quantile_value(&payload, "spread_fr_80").expect("spread_fr_80");
        assert!((value - 0.003).abs() < 1e-12);
    }

    #[test]
    fn xarb_default_funding_mapping_depends_on_venues() {
        let fut_fut =
            default_xarb_funding_mapping(TradingVenue::BinanceFutures, TradingVenue::OkexFutures);
        assert_eq!(
            fut_fut.get("forward_open_mm").map(String::as_str),
            Some("spread_fr_80")
        );
        assert_eq!(
            fut_fut.get("backward_open_mm").map(String::as_str),
            Some("spread_fr_20")
        );

        let margin_fut =
            default_xarb_funding_mapping(TradingVenue::BinanceMargin, TradingVenue::BinanceFutures);
        assert_eq!(
            margin_fut.get("forward_open_mm").map(String::as_str),
            Some("hedge_premium_rate_50")
        );
        assert_eq!(
            margin_fut.get("backward_open_mm").map(String::as_str),
            Some("hedge_premium_rate_50")
        );
    }

    #[test]
    fn xarb_mapping_config_falls_back_to_default_when_empty() {
        let defaults = HashMap::from([("forward_open_mm".to_string(), "spread_5".to_string())]);
        let parsed = parse_xarb_mapping_config(
            Some(r#"{"schema_version":1,"mapping":{}}"#.to_string()),
            defaults.clone(),
        );
        assert_eq!(parsed.mapping, defaults);
    }

    #[test]
    fn xarb_symbol_normalization_matches_runtime() {
        assert_eq!(normalize_xarb_symbol("BTC-USDT-SWAP"), "BTCUSDT");
    }

    #[test]
    fn xarb_spread_sync_entries_follow_expected_field_format() {
        let mapping = HashMap::from([
            ("forward_open_mm".to_string(), "spread_5".to_string()),
            ("backward_open_mt".to_string(), "askbid_90".to_string()),
        ]);
        let resolved = HashMap::from([(
            "BTCUSDT".to_string(),
            HashMap::from([
                ("forward_open_mm".to_string(), 0.12345678),
                ("backward_open_mt".to_string(), 0.9),
            ]),
        )]);

        let order = xarb_spread_threshold_order(&mapping);
        let entries = build_xarb_spread_sync_entries(&resolved, &order);

        assert_eq!(
            entries,
            vec![
                (
                    "BTCUSDT_forward_open_mm".to_string(),
                    "0.12345678".to_string()
                ),
                ("BTCUSDT_backward_open_mt".to_string(), "0.9".to_string()),
            ]
        );
    }
}
