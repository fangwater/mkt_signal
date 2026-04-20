use anyhow::Result;
use std::collections::HashMap;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::signal::common::TradingVenue;

fn venue_key_part(venue: TradingVenue) -> String {
    venue.data_pub_slug().replace('-', "_")
}

pub fn tlen_threshold_key(open_venue: TradingVenue) -> String {
    format!("{}:tlen_threshold", venue_key_part(open_venue))
}

fn normalize_threshold_symbol(raw_symbol: &str) -> String {
    normalize_symbol_for_internal(raw_symbol)
}

pub async fn load_from_redis(
    redis: &RedisSettings,
    open_venue: TradingVenue,
) -> Result<(String, HashMap<String, f64>, usize)> {
    let redis_key = tlen_threshold_key(open_venue);
    let mut settings = redis.clone();
    settings.prefix = None;
    let mut client = RedisClient::connect(settings).await?;
    let hash_map = client.hgetall_map(&redis_key).await?;
    let mut thresholds = HashMap::new();
    let mut bad_fields = 0usize;
    for (raw_symbol, raw_value) in hash_map {
        let symbol = normalize_threshold_symbol(&raw_symbol);
        if symbol.is_empty() {
            continue;
        }
        match raw_value.trim().parse::<f64>() {
            Ok(value) if value.is_finite() && value >= 0.0 => {
                thresholds.insert(symbol, value);
            }
            _ => bad_fields += 1,
        }
    }
    Ok((redis_key, thresholds, bad_fields))
}

#[cfg(test)]
mod tests {
    use super::{normalize_threshold_symbol, tlen_threshold_key};
    use crate::signal::common::TradingVenue;

    #[test]
    fn builds_tlen_key_from_open_venue() {
        let key = tlen_threshold_key(TradingVenue::BinanceMargin);
        assert_eq!(key, "binance_margin:tlen_threshold");
    }

    #[test]
    fn normalizes_okex_inst_id_to_internal_symbol_key() {
        assert_eq!(normalize_threshold_symbol("BTC-USDT-SWAP"), "BTCUSDT");
        assert_eq!(normalize_threshold_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(normalize_threshold_symbol("BTCUSDT"), "BTCUSDT");
    }
}
