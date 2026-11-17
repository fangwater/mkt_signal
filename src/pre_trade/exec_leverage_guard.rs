//! Opt-in account leverage for Exec; separate from exchange contract leverage.
use anyhow::{bail, Context, Result};
use log::{info, warn};
use runtime_common::redis_client::{RedisClient, RedisSettings};
use std::cell::Cell;
use std::time::Duration;

const REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const REDIS_TIMEOUT: Duration = Duration::from_secs(2);

thread_local! {
    static LIMIT: Cell<Option<f64>> = const { Cell::new(None) };
}

pub fn limit() -> Option<f64> {
    LIMIT.with(Cell::get)
}

fn parse_limit(value: Option<&str>) -> Result<Option<f64>> {
    let Some(value) = value else { return Ok(None) };
    let value: f64 = value.parse().context("max_leverage must be a number")?;
    if !value.is_finite() || value < 0.0 {
        bail!("Exec max_leverage must be finite and >= 0 (0 disables the limit)");
    }
    Ok((value > 0.0).then_some(value))
}

fn apply_limit(value: Option<&str>) -> Result<()> {
    let next = parse_limit(value)?;
    LIMIT.with(|current| {
        if current.replace(next) != next {
            info!(
                "Exec account max_leverage applied: {:?} (None=disabled)",
                next
            );
        }
    });
    Ok(())
}

/// One persistent async connection, on the same LocalSet as the order checks.
/// Failed refreshes retain the last valid limit; no Redis I/O on order submission.
pub async fn start(settings: RedisSettings) -> Result<()> {
    let mut client = tokio::time::timeout(REDIS_TIMEOUT, RedisClient::connect(settings))
        .await
        .context("Exec leverage Redis connection timed out")??;
    let values = tokio::time::timeout(REDIS_TIMEOUT, client.hgetall_map("pre_trade_risk_params"))
        .await
        .context("Exec leverage initial read timed out")??;
    if values.is_empty() {
        bail!("Exec leverage risk params hash is missing or empty");
    }
    apply_limit(values.get("max_leverage").map(String::as_str))?;
    info!(
        "Exec account leverage guard started: max_leverage={:?} refresh_ms=250",
        limit()
    );
    tokio::task::spawn_local(async move {
        let mut previous_error = None;
        loop {
            tokio::time::sleep(REFRESH_INTERVAL).await;
            let result = async {
                let values = tokio::time::timeout(
                    REDIS_TIMEOUT,
                    client.hgetall_map("pre_trade_risk_params"),
                )
                .await
                .context("Exec leverage refresh timed out")??;
                if values.is_empty() {
                    bail!("Exec leverage risk params hash is missing or empty");
                }
                apply_limit(values.get("max_leverage").map(String::as_str))
            }
            .await;
            let error = result.err().map(|err| format!("{err:#}"));
            if error != previous_error {
                if let Some(error) = &error {
                    warn!("Exec account leverage refresh failed; retaining max_leverage={:?}: {error}", limit());
                } else {
                    info!("Exec account leverage refresh recovered");
                }
                previous_error = error;
            }
        }
    });
    Ok(())
}

/// Quantities are kept in venue units, so inverse contracts retain their face value.
#[derive(Default, Clone, Copy)]
pub(crate) struct PositionRisk {
    pub qty: f64,
    pub buys: f64,
    pub sells: f64,
    pub unit_notional: f64,
}

impl PositionRisk {
    pub fn worst_notional(self) -> f64 {
        (self.qty + self.buys)
            .abs()
            .max((self.qty - self.sells).abs())
            * self.unit_notional
    }

    pub fn with_order(mut self, signed_qty: f64) -> Self {
        if signed_qty > 0.0 {
            self.buys += signed_qty;
        } else {
            self.sells -= signed_qty;
        }
        self
    }

    /// A reduction must stay reducing even after every other same-side order fills.
    pub fn reduces_position(self, signed_qty: f64) -> bool {
        if self.qty > 0.0 && signed_qty < 0.0 {
            self.sells - signed_qty <= self.qty + 1e-12
        } else if self.qty < 0.0 && signed_qty > 0.0 {
            self.buys + signed_qty <= -self.qty + 1e-12
        } else {
            false
        }
    }
}

pub(crate) fn check_projection(
    position: PositionRisk,
    signed_qty: f64,
    current_worst: f64,
    equity: f64,
    max_leverage: f64,
) -> Result<(), String> {
    if !signed_qty.is_finite() || signed_qty == 0.0 {
        return Err("Exec leverage: invalid order quantity".into());
    }
    if position.reduces_position(signed_qty) {
        return Ok(());
    }
    if !equity.is_finite() || equity <= 0.0 {
        return Err("Exec leverage: account equity is missing, invalid or non-positive".into());
    }
    let projected = current_worst - position.worst_notional()
        + position.with_order(signed_qty).worst_notional();
    let cap = equity * max_leverage;
    if !projected.is_finite() || !cap.is_finite() || projected > cap + 1e-9 {
        return Err(format!(
            "Exec account max_leverage exceeded: projected_notional={projected:.4} equity={equity:.4} max_leverage={max_leverage:.4} cap={cap:.4}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_leverage_is_opt_in_and_hot_updates_without_changing_other_limits() {
        assert_eq!(parse_limit(None).unwrap(), None);
        apply_limit(Some("1")).unwrap();
        assert_eq!(limit(), Some(1.0));
        apply_limit(Some("1.5")).unwrap();
        assert_eq!(limit(), Some(1.5));
        for invalid in ["NaN", "inf", "-1", "", "oops"] {
            assert!(apply_limit(Some(invalid)).is_err());
            assert_eq!(limit(), Some(1.5));
        }
        apply_limit(Some("0")).unwrap();
        assert_eq!(limit(), None);
        apply_limit(Some("1")).unwrap();
        apply_limit(None).unwrap();
        assert_eq!(limit(), None);
    }

    #[test]
    fn exec_leverage_reserves_both_sides_without_netting_live_orders() {
        let position = PositionRisk {
            qty: 0.0,
            buys: 7.0,
            sells: 8.0,
            unit_notional: 100.0,
        };
        assert_eq!(position.worst_notional(), 800.0);
        assert!(check_projection(position, 3.0, 800.0, 1000.0, 1.0).is_ok());
        assert!(check_projection(position, 4.0, 800.0, 1000.0, 1.0).is_err());
        assert!(check_projection(position, -3.0, 800.0, 1000.0, 1.0).is_err());
    }

    #[test]
    fn exec_leverage_allows_reductions_over_cap_but_reserves_the_close_quantity() {
        let position = PositionRisk {
            qty: 20.0,
            sells: 5.0,
            unit_notional: 100.0,
            ..Default::default()
        };
        assert!(check_projection(position, -15.0, 2000.0, 1000.0, 1.0).is_ok());
        assert!(check_projection(position, -16.0, 2000.0, 1000.0, 1.0).is_err());
        assert!(check_projection(position, 1.0, 2000.0, 1000.0, 1.0).is_err());
        assert!(check_projection(position, -1.0, 2000.0, 0.0, 1.0).is_ok());
        assert!(check_projection(position, 1.0, 2000.0, f64::NAN, 1.0).is_err());
    }
}
