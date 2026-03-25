use anyhow::Result;
use std::collections::HashMap;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;

fn venue_key_part(venue: TradingVenue) -> String {
    venue.data_pub_slug().replace('-', "_")
}

pub fn mm_tlen_threshold_key(open_venue: TradingVenue, hedge_venue: TradingVenue) -> String {
    format!(
        "{}_{}:mm:tlen_threshold",
        venue_key_part(hedge_venue),
        venue_key_part(open_venue)
    )
}

pub async fn load_from_redis(
    redis: &RedisSettings,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<(String, HashMap<String, f64>, usize)> {
    let redis_key = mm_tlen_threshold_key(open_venue, hedge_venue);
    let mut settings = redis.clone();
    settings.prefix = None;
    let mut client = RedisClient::connect(settings).await?;
    let hash_map = client.hgetall_map(&redis_key).await?;
    let mut thresholds = HashMap::new();
    let mut bad_fields = 0usize;
    for (raw_symbol, raw_value) in hash_map {
        let symbol = raw_symbol.trim().to_ascii_uppercase();
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
    use super::mm_tlen_threshold_key;
    use crate::signal::common::TradingVenue;

    #[test]
    fn builds_mm_tlen_key_from_hedge_then_open_venues() {
        let key = mm_tlen_threshold_key(TradingVenue::BinanceMargin, TradingVenue::GateFutures);
        assert_eq!(key, "gate_futures_binance_margin:mm:tlen_threshold");
    }
}
