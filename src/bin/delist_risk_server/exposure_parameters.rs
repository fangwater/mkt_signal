//! Read-only FR limits and directional order sizing from Redis for Delist.
use anyhow::{ensure, Context, Result};
use mkt_signal::common::delist_accounts::AccountSpec;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct Limits {
    /// Zero means the configured limit is disabled.
    pub symbol_limit_usdt: f64,
    pub order_amount_usdt: f64,
    /// None means the direction has no finite maker cap.
    pub maker_buy_orders: Option<u32>,
    pub maker_sell_orders: Option<u32>,
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

    pub fn limits(&self, asset: &str) -> Option<Limits> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let on = params.limits("ON").unwrap();
        assert_eq!(on.symbol_limit_usdt, 40.0);
        assert_eq!(on.order_amount_usdt, 30.0);
        assert_eq!(
            (on.maker_buy_orders, on.maker_sell_orders),
            (Some(10), Some(6))
        );
        assert_eq!(params.limits("AIN").unwrap().symbol_limit_usdt, 64.0);
        assert_eq!(params.limits("BTC").unwrap().symbol_limit_usdt, 800.0);
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
        assert_eq!(params.limits("ON").unwrap().symbol_limit_usdt, 0.0);
    }
}
