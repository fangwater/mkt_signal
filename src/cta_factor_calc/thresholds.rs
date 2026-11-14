use super::{
    catalog::requires_amount_thresholds,
    config::{Market, Plan},
};
use crate::common::amount_threshold::AmountThreshold;
use anyhow::{ensure, Context, Result};
use chrono::{NaiveDate, Utc};
use runtime_common::redis_client::RedisSettings;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

type Hash = BTreeMap<String, String>;

pub fn load(plan: &Plan) -> Result<BTreeMap<String, [f64; 2]>> {
    let markets = plan
        .calculations
        .values()
        .filter(|c| c.id.is_formula() && requires_amount_thresholds(&c.id.factor))
        .map(|c| c.id.market)
        .collect::<BTreeSet<_>>();
    if markets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let client = redis::Client::open(RedisSettings::default().connection_url())?;
    let mut connection = client
        .get_connection_with_timeout(Duration::from_secs(1))
        .context("connect shared amount thresholds")?;
    connection.set_read_timeout(Some(Duration::from_secs(1)))?;
    connection.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    for market in &markets {
        let key = format!("{}:amount-thresholds", market.name());
        pipeline
            .cmd("HGETALL")
            .arg(&key)
            .cmd("HGETALL")
            .arg(format!("{key}:meta"));
    }
    let hashes: Vec<Hash> = pipeline
        .query(&mut connection)
        .context("read shared monthly amount thresholds")?;
    ensure!(
        hashes.len() == markets.len() * 2,
        "incomplete amount-threshold snapshot"
    );
    let month = Utc::now().format("%Y-%m").to_string();
    let mut result = BTreeMap::new();
    for (market, hashes) in markets.into_iter().zip(hashes.chunks_exact(2)) {
        result.extend(parse(market, &hashes[0], &hashes[1], &month)?);
    }
    Ok(result)
}

fn parse(
    market: Market,
    values: &Hash,
    metadata: &Hash,
    month: &str,
) -> Result<BTreeMap<String, [f64; 2]>> {
    let date = NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d")?;
    let previous = date
        .pred_opt()
        .context("invalid threshold month")?
        .format("%Y-%m")
        .to_string();
    let expected_unit = if market == Market::OkexFutures {
        "price_x_contracts"
    } else {
        "USDT"
    };
    ensure!(
        !values.is_empty(),
        "{} amount thresholds are missing",
        market.name()
    );
    ensure!(
        metadata.get("apply_month").map(String::as_str) == Some(month)
            && metadata.get("source_month") == Some(&previous),
        "{} threshold month does not match {month}",
        market.name()
    );
    ensure!(
        metadata.get("unit").map(String::as_str) == Some(expected_unit),
        "{} threshold quantity unit mismatch",
        market.name()
    );
    let mut out = BTreeMap::new();
    for (source_symbol, raw) in values {
        // The shared OKX hash uses venue-native instrument IDs. Formula IDs
        // use base-asset symbols, while price * contracts remains unchanged.
        let symbol = if market == Market::OkexFutures {
            let base = source_symbol
                .strip_suffix("-USDT-SWAP")
                .with_context(|| format!("invalid OKX threshold instrument {source_symbol}"))?;
            format!("{base}USDT")
        } else {
            source_symbol.clone()
        };
        ensure!(
            symbol.len() > 4
                && symbol.len() <= 32
                && symbol.ends_with("USDT")
                && symbol
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()),
            "invalid threshold symbol {symbol}"
        );
        let threshold: AmountThreshold = serde_json::from_str(raw)
            .with_context(|| format!("parse {} {symbol} amount thresholds", market.name()))?;
        ensure!(
            threshold.is_online(),
            "invalid {} {symbol} amount thresholds",
            market.name()
        );
        out.insert(
            format!("{}:{symbol}", market.name()),
            [
                threshold.medium_notional_threshold,
                threshold.large_notional_threshold,
            ],
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn data() -> (Hash, Hash) {
        (
            BTreeMap::from([(
                "BTCUSDT".into(),
                r#"{"medium_notional_threshold":10.0,"large_notional_threshold":100.0}"#.into(),
            )]),
            BTreeMap::from([
                ("source_month".into(), "2026-10".into()),
                ("apply_month".into(), "2026-11".into()),
                ("unit".into(), "USDT".into()),
            ]),
        )
    }
    #[test]
    fn shared_thresholds_are_market_scoped_and_validate_month_and_units() {
        let (values, mut meta) = data();
        assert_eq!(
            parse(Market::BinanceFutures, &values, &meta, "2026-11").unwrap()
                ["binance-futures:BTCUSDT"],
            [10.0, 100.0]
        );
        assert!(parse(Market::BinanceFutures, &values, &meta, "2026-12").is_err());
        assert!(parse(Market::OkexFutures, &values, &meta, "2026-11").is_err());
        meta.insert("unit".into(), "price_x_contracts".into());
        let okx_values = BTreeMap::from([("BTC-USDT-SWAP".into(), values["BTCUSDT"].clone())]);
        assert_eq!(
            parse(Market::OkexFutures, &okx_values, &meta, "2026-11").unwrap()
                ["okex-futures:BTCUSDT"],
            [10.0, 100.0]
        );
        assert!(parse(Market::OkexFutures, &values, &meta, "2026-11").is_err());
        for invalid in ["BTC-USD-SWAP", "BTC-USDT", "-USDT-SWAP", "btc-USDT-SWAP"] {
            assert!(parse(
                Market::OkexFutures,
                &BTreeMap::from([(invalid.into(), values["BTCUSDT"].clone())]),
                &meta,
                "2026-11"
            )
            .is_err());
        }
        meta.insert("source_month".into(), "2026-09".into());
        assert!(parse(Market::OkexFutures, &values, &meta, "2026-11").is_err());
    }
    #[test]
    fn missing_or_invalid_thresholds_never_get_defaults() {
        let (mut values, meta) = data();
        assert!(parse(Market::BinanceFutures, &Hash::new(), &meta, "2026-11").is_err());
        values.insert(
            "BTCUSDT".into(),
            r#"{"medium_notional_threshold":100.0,"large_notional_threshold":10.0}"#.into(),
        );
        assert!(parse(Market::BinanceFutures, &values, &meta, "2026-11").is_err());
        values = BTreeMap::from([(
            "USDT".into(),
            r#"{"medium_notional_threshold":10.0,"large_notional_threshold":100.0}"#.into(),
        )]);
        assert!(parse(Market::BinanceFutures, &values, &meta, "2026-11").is_err());
    }
}
