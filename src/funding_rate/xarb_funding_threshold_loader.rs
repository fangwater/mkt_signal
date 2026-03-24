use std::collections::HashMap;

use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

const FORWARD_OPEN_MM: &str = "forward_open_mm";
const BACKWARD_OPEN_MM: &str = "backward_open_mm";
const OPS: [&str; 2] = [FORWARD_OPEN_MM, BACKWARD_OPEN_MM];

#[derive(Debug, Clone, Copy)]
pub struct XarbFundingThresholdsResolved {
    pub forward_open: f64,
    pub backward_open: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct XarbFundingThresholdLoadStats {
    pub loaded_symbols: usize,
    pub incomplete_symbols: usize,
    pub ignored_fields: usize,
    pub bad_value_fields: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct XarbFundingThresholdsPartial {
    forward_open: Option<f64>,
    backward_open: Option<f64>,
}

impl XarbFundingThresholdsPartial {
    fn to_resolved(self) -> Option<XarbFundingThresholdsResolved> {
        Some(XarbFundingThresholdsResolved {
            forward_open: self.forward_open?,
            backward_open: self.backward_open?,
        })
    }
}

pub fn resolve_from_redis_map(
    hash_map: HashMap<String, String>,
) -> (
    HashMap<String, XarbFundingThresholdsResolved>,
    XarbFundingThresholdLoadStats,
) {
    let mut grouped: HashMap<String, XarbFundingThresholdsPartial> = HashMap::new();
    let mut stats = XarbFundingThresholdLoadStats::default();

    for (field, raw_value) in hash_map {
        let mut matched = false;
        for op in OPS {
            let suffix = format!("_{op}");
            let Some(symbol_raw) = field.strip_suffix(&suffix) else {
                continue;
            };
            matched = true;

            let value = match raw_value.trim().parse::<f64>() {
                Ok(v) if v.is_finite() => v,
                _ => {
                    stats.bad_value_fields += 1;
                    break;
                }
            };

            let symbol_key = normalize_symbol_for_whitelist(symbol_raw, TradingVenue::OkexFutures)
                .to_ascii_uppercase();
            if symbol_key.is_empty() {
                stats.ignored_fields += 1;
                break;
            }

            let entry = grouped.entry(symbol_key).or_default();
            match op {
                FORWARD_OPEN_MM => entry.forward_open = Some(value),
                BACKWARD_OPEN_MM => entry.backward_open = Some(value),
                _ => {}
            }
            break;
        }

        if !matched {
            stats.ignored_fields += 1;
        }
    }

    let mut resolved = HashMap::new();
    for (symbol, partial) in grouped {
        if let Some(full) = partial.to_resolved() {
            resolved.insert(symbol, full);
        } else {
            stats.incomplete_symbols += 1;
        }
    }

    stats.loaded_symbols = resolved.len();
    (resolved, stats)
}

#[cfg(test)]
mod tests {
    use super::resolve_from_redis_map;

    #[test]
    fn resolves_complete_symbol_thresholds() {
        let mut hash_map = std::collections::HashMap::new();
        hash_map.insert("BTCUSDT_forward_open_mm".to_string(), "0.01".to_string());
        hash_map.insert("BTCUSDT_backward_open_mm".to_string(), "-0.02".to_string());

        let (resolved, stats) = resolve_from_redis_map(hash_map);
        let btc = resolved.get("BTCUSDT").expect("BTCUSDT");
        assert!((btc.forward_open - 0.01).abs() < 1e-12);
        assert!((btc.backward_open + 0.02).abs() < 1e-12);
        assert_eq!(stats.loaded_symbols, 1);
        assert_eq!(stats.incomplete_symbols, 0);
    }

    #[test]
    fn drops_incomplete_symbol_thresholds() {
        let mut hash_map = std::collections::HashMap::new();
        hash_map.insert("BTCUSDT_forward_open_mm".to_string(), "0.01".to_string());

        let (resolved, stats) = resolve_from_redis_map(hash_map);
        assert!(resolved.is_empty());
        assert_eq!(stats.incomplete_symbols, 1);
    }
}
