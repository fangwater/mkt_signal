//! Read-only FR configuration and cached public venue order rules for Delist.
use anyhow::{ensure, Context, Result};
use futures_util::{stream, StreamExt};
use mkt_signal::common::delist_accounts::AccountSpec;
use mkt_signal::strategy::arb_hedge_strategy::ARB_HEDGE_PENDING_QUERY_MIN_USDT;
use order_common::TradingVenue;
use quote_plan::hedge_split::MIN_TAIL_NOTIONAL_USDT;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use serde::Serialize;
use signal_common::venue_min_qty_table::VenueMinQtyTable;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct Limits {
    /// Zero means the configured limit is disabled.
    pub symbol_limit_usdt: f64,
    pub order_amount_usdt: f64,
    /// None means the direction has no finite maker cap.
    pub maker_buy_orders: Option<u32>,
    pub maker_sell_orders: Option<u32>,
    pub min_hedge_usdt: Option<f64>,
}

pub struct Parameters {
    max_pos_u: f64,
    symbol_ratio: f64,
    total_ratio: f64,
    order_amount: f64,
    buy_orders: Option<u32>,
    sell_orders: Option<u32>,
    max_pos_overrides: BTreeMap<String, f64>,
    amount_overrides: BTreeMap<String, f64>,
}

type Hash = BTreeMap<String, String>;

fn field(hash: &Hash, name: &str) -> Result<f64> {
    hash.get(name)
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .with_context(|| format!("Redis 参数缺少或无效: {name}"))
}

fn directional_maker_capacity(total: u32, open: u32, close: u32) -> Option<u32> {
    let open = match (total > 0, open > 0) {
        (true, true) => total.min(open),
        (true, false) => total,
        (false, true) => open,
        (false, false) => return None,
    };
    (close > 0).then_some(open.max(close))
}

fn exposure_limit(base: f64, ratio: f64) -> Option<f64> {
    if ratio <= 0.0 {
        return Some(0.0);
    }
    let value = base * ratio;
    (base.is_finite() && base > 0.0 && value.is_finite() && value > 0.0).then_some(value)
}

fn overrides(raw: Option<&str>) -> Result<BTreeMap<String, f64>> {
    let Some(raw) = raw else {
        return Ok(BTreeMap::new());
    };
    let parsed: BTreeMap<String, f64> =
        serde_json::from_str(raw).context("Redis 单币覆盖参数无效")?;
    let mut result = BTreeMap::new();
    for (symbol, value) in parsed {
        ensure!(
            !symbol.trim().is_empty() && value.is_finite() && value > 0.0,
            "Redis 单币覆盖参数必须为有效币种及正数"
        );
        let key = normalize_symbol_for_internal(symbol.trim());
        ensure!(
            result.insert(key, value).is_none(),
            "Redis 单币覆盖参数有重复归一化币种"
        );
    }
    Ok(result)
}

impl Parameters {
    fn parse(
        risk: Hash,
        strategy: Hash,
        max_pos: [Option<String>; 3],
        amount: Option<String>,
    ) -> Result<Self> {
        let count = |name| -> Result<u32> {
            let value = field(&risk, name)?;
            ensure!(
                value.fract() == 0.0 && value <= i32::MAX as f64,
                "Redis maker 数量无效: {name}"
            );
            Ok(value.max(0.0) as u32)
        };
        let total = count("max_pending_limit_orders")?;
        let buy_orders = directional_maker_capacity(
            total,
            count("arb_max_pending_limit_buy_orders")?,
            count("arb_close_max_pending_limit_buy_orders")?,
        );
        let sell_orders = directional_maker_capacity(
            total,
            count("arb_max_pending_limit_sell_orders")?,
            count("arb_close_max_pending_limit_sell_orders")?,
        );
        // Same open-venue precedence as PreTradeParamsLoader: MM -> Exec -> FR.
        // The hedge venue's Exec map cannot override an open-venue position cap.
        let mut max_pos_overrides = BTreeMap::new();
        for raw in max_pos {
            max_pos_overrides.extend(overrides(raw.as_deref())?);
        }
        let max_pos_u = field(&risk, "max_pos_u")?;
        let order_amount = field(&strategy, "order_amount")?;
        ensure!(
            max_pos_u > 0.0 && order_amount > 0.0,
            "Redis 仓位/单手金额必须为正数"
        );
        Ok(Self {
            max_pos_u,
            symbol_ratio: field(&risk, "max_symbol_exposure_ratio")?,
            total_ratio: field(&risk, "max_total_exposure_ratio")?,
            order_amount,
            buy_orders,
            sell_orders,
            max_pos_overrides,
            amount_overrides: overrides(amount.as_deref())?,
        })
    }

    pub fn account_limit(&self, equity: f64) -> Option<f64> {
        exposure_limit(equity, self.total_ratio)
    }

    pub fn limits(
        &self,
        asset: &str,
        mark: Option<f64>,
        rules: Option<&VenueMinQtyTable>,
    ) -> Option<Limits> {
        let symbol = normalize_symbol_for_internal(&format!("{asset}USDT"));
        let order_amount = *self
            .amount_overrides
            .get(&symbol)
            .unwrap_or(&self.order_amount);
        Some(Limits {
            symbol_limit_usdt: exposure_limit(
                *self
                    .max_pos_overrides
                    .get(&symbol)
                    .unwrap_or(&self.max_pos_u),
                self.symbol_ratio,
            )?,
            order_amount_usdt: order_amount,
            maker_buy_orders: self.buy_orders,
            maker_sell_orders: self.sell_orders,
            min_hedge_usdt: rules
                .and_then(|table| min_hedge_usdt(table, &symbol, mark?, order_amount)),
        })
    }
}

pub async fn read(url: &str, spec: &AccountSpec) -> Result<Parameters> {
    let operation = async {
        // Do not include credential-bearing connection URLs in errors.
        let mut conn = redis::Client::open(url)
            .map_err(|_| anyhow::anyhow!("Redis 地址无效"))?
            .get_multiplexed_async_connection()
            .await
            .map_err(|_| anyhow::anyhow!("Redis 参数连接失败"))?;
        let env = &spec.slug;
        let open = format!("{}-margin", spec.exchange);
        let hedge = format!("{}-futures", spec.exchange);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HGETALL")
            .arg(format!("{env}:{open}:{hedge}:pre_trade_risk_params"))
            .cmd("HGETALL")
            .arg(format!("fr_strategy_params_{open}_{hedge}"));
        for key in [
            format!("{env}:{open}:mm:max_pos_u"),
            format!("{env}:{open}:exec:max_pos_u"),
            format!("{env}:{open}:{hedge}:max_pos_u_overrides"),
            format!("{env}:{open}:{hedge}:amount_u_overrides"),
        ] {
            pipe.cmd("GET").arg(key);
        }
        let (risk, strategy, mm, exec, arb, amount): (
            Hash,
            Hash,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = pipe
            .query_async(&mut conn)
            .await
            .map_err(|_| anyhow::anyhow!("Redis 参数读取失败"))?;
        Parameters::parse(risk, strategy, [mm, exec, arb], amount)
    };
    tokio::time::timeout(Duration::from_secs(5), operation)
        .await
        .context("Redis 参数读取超时")?
}

fn hedge_venue(exchange: &str) -> Result<TradingVenue> {
    match exchange {
        "binance" => Ok(TradingVenue::BinanceFutures),
        "gate" => Ok(TradingVenue::GateFutures),
        "bitget" => Ok(TradingVenue::BitgetFutures),
        "bybit" => Ok(TradingVenue::BybitFutures),
        "okex" => Ok(TradingVenue::OkexFutures),
        _ => anyhow::bail!("不支持的 FR 对冲市场"),
    }
}

fn min_hedge_usdt(
    table: &VenueMinQtyTable,
    symbol: &str,
    mark: f64,
    order_amount: f64,
) -> Option<f64> {
    if !mark.is_finite() || mark <= 0.0 || !table.is_tradable_symbol(symbol) {
        return None;
    }
    let multiplier = match table.venue() {
        TradingVenue::GateFutures | TradingVenue::OkexFutures => {
            table.contract_multiplier_opt(symbol)?
        }
        TradingVenue::BinanceFutures | TradingVenue::BitgetFutures | TradingVenue::BybitFutures => {
            1.0
        }
        _ => return None,
    };
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return None;
    }
    let step = table.step_size(symbol)?;
    // The ordinary hedge route waits below its query floor, then the maker
    // splitter retains sub-25U tails. A smaller full hand can still execute.
    let strategy_minimum = order_amount
        .min(MIN_TAIL_NOTIONAL_USDT)
        .max(ARB_HEDGE_PENDING_QUERY_MIN_USDT);
    let qty = table.min_qty(symbol)?.max(step).max(
        table
            .min_notional(symbol)
            .unwrap_or(0.0)
            .max(strategy_minimum)
            / (multiplier * mark),
    );
    let value = (qty / step).ceil() * step * multiplier * mark;
    (value.is_finite() && value > 0.0).then_some(value)
}

struct CachedRules {
    table: VenueMinQtyTable,
    attempted: Option<Instant>,
    refreshed: Option<Instant>,
}

#[derive(Default)]
pub struct RuleCache(BTreeMap<String, CachedRules>);

impl RuleCache {
    pub async fn refresh(&mut self, specs: &[AccountSpec]) {
        for spec in specs.iter().filter(|s| s.kind == "funding_rate") {
            if let Ok(venue) = hedge_venue(&spec.exchange) {
                self.0
                    .entry(spec.exchange.clone())
                    .or_insert_with(|| CachedRules {
                        table: VenueMinQtyTable::new(venue),
                        attempted: None,
                        refreshed: None,
                    });
            }
        }
        // One catalog per exchange, at most once every five minutes (not per account).
        stream::iter(self.0.iter_mut())
            .for_each_concurrent(5, |(exchange, cached)| async move {
                if cached
                    .attempted
                    .is_some_and(|at| at.elapsed() < Duration::from_secs(300))
                {
                    return;
                }
                cached.attempted = Some(Instant::now());
                match tokio::time::timeout(Duration::from_secs(5), cached.table.refresh()).await {
                    Ok(Ok(())) => cached.refreshed = Some(Instant::now()),
                    _ => {
                        log::warn!("Delist exposure order-rule refresh failed exchange={exchange}")
                    }
                }
            })
            .await;
    }

    pub fn get(&self, exchange: &str) -> Option<&VenueMinQtyTable> {
        let cached = self.0.get(exchange)?;
        (cached.refreshed?.elapsed() <= Duration::from_secs(900)).then_some(&cached.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_common::min_qty_table::MinQtyEntry;

    fn risk() -> Hash {
        [
            ("max_pos_u", "100000"),
            ("max_symbol_exposure_ratio", "0.008"),
            ("max_total_exposure_ratio", "0.005"),
            ("max_pending_limit_orders", "20"),
            ("arb_max_pending_limit_buy_orders", "10"),
            ("arb_max_pending_limit_sell_orders", "4"),
            ("arb_close_max_pending_limit_buy_orders", "10"),
            ("arb_close_max_pending_limit_sell_orders", "6"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }

    fn strategy() -> Hash {
        [("order_amount".into(), "100".into())].into()
    }

    #[test]
    fn redis_caps_follow_account_symbol_precedence_and_directional_sizing() {
        let params = Parameters::parse(
            risk(),
            strategy(),
            [
                Some(r#"{"ON_USDT":9000,"AINUSDT":8000}"#.into()),
                Some(r#"{"ON-USDT":7000}"#.into()),
                Some(r#"{"ONUSDT":5000}"#.into()),
            ],
            Some(r#"{"ON_USDT":30}"#.into()),
        )
        .unwrap();
        assert_eq!(params.account_limit(750000.0), Some(3750.0));
        let on = params.limits("ON", Some(1.0), None).unwrap();
        assert_eq!(on.symbol_limit_usdt, 40.0);
        assert_eq!(on.order_amount_usdt, 30.0);
        assert_eq!(
            (on.maker_buy_orders, on.maker_sell_orders),
            (Some(10), Some(6))
        );
        assert_eq!(
            params.limits("AIN", None, None).unwrap().symbol_limit_usdt,
            64.0
        );
        assert_eq!(
            params.limits("BTC", None, None).unwrap().symbol_limit_usdt,
            800.0
        );
        assert_eq!(on.min_hedge_usdt, None); // Missing rules do not hide a known hard cap.
    }

    #[test]
    fn invalid_or_missing_redis_parameters_never_invent_defaults() {
        for name in risk().keys() {
            let mut bad = risk();
            bad.remove(name);
            assert!(Parameters::parse(bad, strategy(), [None, None, None], None).is_err());
        }
        assert!(Parameters::parse(risk(), Hash::new(), [None, None, None], None).is_err());
        for raw in [
            "bad",
            r#"{"BTCUSDT":0}"#,
            r#"{"BTCUSDT":-1}"#,
            r#"{"":10}"#,
            r#"{"ON_USDT":5,"ONUSDT":10}"#,
        ] {
            assert!(
                Parameters::parse(risk(), strategy(), [None, None, Some(raw.into())], None)
                    .is_err()
            );
        }
    }

    #[test]
    fn unlimited_direction_and_disabled_caps_keep_their_configured_meaning() {
        assert_eq!(directional_maker_capacity(20, 10, 10), Some(10));
        assert_eq!(directional_maker_capacity(4, 10, 3), Some(4));
        assert_eq!(directional_maker_capacity(0, 2, 5), Some(5));
        assert_eq!(directional_maker_capacity(0, 0, 10), None);
        assert_eq!(directional_maker_capacity(20, 10, 0), None);
        assert_eq!(exposure_limit(0.0, 0.0), Some(0.0));
        assert_eq!(exposure_limit(0.0, 0.01), None);
        assert_eq!(exposure_limit(f64::MAX, 2.0), None);
        let mut risk = risk();
        risk.insert("max_total_exposure_ratio".into(), "0".into());
        risk.insert("max_symbol_exposure_ratio".into(), "0".into());
        let params = Parameters::parse(risk, strategy(), [None, None, None], None).unwrap();
        assert_eq!(params.account_limit(f64::NAN), Some(0.0));
        assert_eq!(
            params.limits("ON", None, None).unwrap().symbol_limit_usdt,
            0.0
        );
    }

    fn table(venue: TradingVenue, multiplier: Option<f64>) -> VenueMinQtyTable {
        let mut table = VenueMinQtyTable::new(venue);
        table.replace_snapshot(
            [(
                "ONUSDT".into(),
                MinQtyEntry {
                    symbol: "ONUSDT".into(),
                    base_asset: "ON".into(),
                    quote_asset: "USDT".into(),
                    min_qty: 0.1,
                    step_size: 0.1,
                    price_tick: None,
                    min_notional: Some(5.0),
                },
            )]
            .into(),
            multiplier
                .map(|m| [("ONUSDT".into(), m)].into())
                .unwrap_or_default(),
            ["ONUSDT".into()].into(),
        );
        table
    }

    #[test]
    fn decimal_contract_minimum_uses_multiplier_and_rounds_up_to_step() {
        let gate = table(TradingVenue::GateFutures, Some(10.0));
        assert_eq!(min_hedge_usdt(&gate, "ONUSDT", 3.0, 100.0), Some(27.0));
        assert_eq!(
            min_hedge_usdt(
                &table(TradingVenue::GateFutures, None),
                "ONUSDT",
                3.0,
                100.0
            ),
            None
        );
        let bitget = table(TradingVenue::BitgetFutures, Some(100.0));
        assert!((min_hedge_usdt(&bitget, "ONUSDT", 3.0, 100.0).unwrap() - 25.2).abs() < 1e-10);
        assert_eq!(min_hedge_usdt(&gate, "MISSINGUSDT", 3.0, 100.0), None);
        assert_eq!(min_hedge_usdt(&gate, "ONUSDT", 0.0, 100.0), None);
    }

    #[test]
    fn expired_order_rules_cannot_confirm_recovery() {
        let mut cache = RuleCache::default();
        cache.0.insert(
            "gate".into(),
            CachedRules {
                table: table(TradingVenue::GateFutures, Some(10.0)),
                attempted: Some(Instant::now()),
                refreshed: Some(Instant::now() - Duration::from_secs(901)),
            },
        );
        assert!(cache.get("gate").is_none());
        cache.0.get_mut("gate").unwrap().refreshed = Some(Instant::now());
        assert!(cache.get("gate").is_some());
    }

    #[test]
    fn smaller_full_hands_and_query_floor_are_not_confused_with_25u_tails() {
        let table = table(TradingVenue::BitgetFutures, None);
        for (amount, expected) in [(100.0, 25.2), (20.0, 20.1), (1.0, 10.2)] {
            assert!(
                (min_hedge_usdt(&table, "ONUSDT", 3.0, amount).unwrap() - expected).abs() < 1e-10
            );
        }
    }
}
