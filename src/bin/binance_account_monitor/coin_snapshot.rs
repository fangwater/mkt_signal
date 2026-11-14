use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use viz_common::coin_account::{CoinAccountSnapshot, CoinAsset, CoinPosition};

#[derive(Deserialize)]
struct Account {
    assets: Vec<Asset>,
    positions: Vec<Position>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Asset {
    asset: String,
    wallet_balance: String,
    unrealized_profit: String,
    margin_balance: String,
    available_balance: String,
    initial_margin: String,
    maint_margin: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Position {
    symbol: String,
    position_side: String,
    position_amt: String,
    entry_price: String,
    notional_value: String,
    unrealized_profit: String,
    isolated: bool,
}
fn number(raw: &str) -> Result<f64> {
    let value: f64 = raw.parse().context("invalid account decimal")?;
    ensure!(value.is_finite(), "non-finite account decimal");
    Ok(value)
}
pub fn parse(body: &str, ts_ms: i64) -> Result<CoinAccountSnapshot> {
    let raw: Account = serde_json::from_str(body).context("decode COIN-M account snapshot")?;
    let mut assets = Vec::new();
    for row in raw.assets {
        ensure!(!row.asset.is_empty(), "missing settlement asset");
        assets.push(CoinAsset {
            asset: row.asset,
            wallet_balance: number(&row.wallet_balance)?,
            unrealized_pnl: number(&row.unrealized_profit)?,
            equity: number(&row.margin_balance)?,
            available_balance: number(&row.available_balance)?,
            initial_margin: number(&row.initial_margin)?,
            maintenance_margin: number(&row.maint_margin)?,
        });
    }
    let mut positions = Vec::new();
    for row in raw.positions {
        let contracts = number(&row.position_amt)?;
        if contracts == 0.0 {
            continue;
        }
        // Show delivery contracts too if the actual account holds them; do not
        // hide factual exposure merely because Exec trades perpetuals only.
        let (asset, suffix) = row
            .symbol
            .split_once("USD_")
            .context("invalid COIN-M symbol")?;
        ensure!(
            !asset.is_empty() && !suffix.is_empty(),
            "invalid COIN-M symbol"
        );
        ensure!(
            matches!(row.position_side.as_str(), "BOTH" | "LONG" | "SHORT"),
            "invalid position side"
        );
        ensure!(contracts.fract() == 0.0, "fractional COIN-M contract count");
        positions.push(CoinPosition {
            settlement_asset: asset.into(),
            symbol: row.symbol,
            side: row.position_side,
            contracts,
            entry_price: number(&row.entry_price)?,
            notional_coin: number(&row.notional_value)?,
            unrealized_pnl: number(&row.unrealized_profit)?,
            isolated: row.isolated,
        });
    }
    assets.sort_by(|a, b| a.asset.cmp(&b.asset));
    positions.sort_by(|a, b| (&a.symbol, &a.side).cmp(&(&b.symbol, &b.side)));
    Ok(CoinAccountSnapshot {
        ts_ms,
        venue: "binance-coin-futures".into(),
        account_mode: "STANDARD".into(),
        assets,
        positions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn account() -> serde_json::Value {
        serde_json::json!({"assets":[{"asset":"BTC","walletBalance":"1","unrealizedProfit":"0.02","marginBalance":"1.02","availableBalance":"0.8","initialMargin":"0.2","maintMargin":"0.01"}],"positions":[{"symbol":"BTCUSD_PERP","positionSide":"BOTH","positionAmt":"-10","entryPrice":"50000","notionalValue":"-0.02","unrealizedProfit":"0.02","isolated":false}]})
    }
    #[test]
    fn preserves_native_equity_signed_contracts_and_coin_notional() {
        let s = parse(&account().to_string(), 123).unwrap();
        assert_eq!(s.assets[0].equity, 1.02);
        assert_eq!(s.positions[0].contracts, -10.0);
        assert_eq!(s.positions[0].notional_coin, -0.02);
        assert_eq!(s.positions[0].settlement_asset, "BTC");
        let decoded: CoinAccountSnapshot =
            bincode::deserialize(&bincode::serialize(&s).unwrap()).unwrap();
        assert_eq!(decoded.ts_ms, 123);
    }
    #[test]
    fn complete_empty_snapshot_clears_previous_positions() {
        let s = parse(r#"{"assets":[],"positions":[]}"#, 124).unwrap();
        assert!(s.positions.is_empty());
        assert!(parse(r#"{"code":-2015,"msg":"denied"}"#, 124).is_err());
    }
    #[test]
    fn rejects_invalid_values_instead_of_fabricating_zero() {
        for invalid in ["NaN", "inf", "bad"] {
            let mut a = account();
            a["assets"][0]["walletBalance"] = invalid.into();
            assert!(parse(&a.to_string(), 123).is_err());
        }
        let mut a = account();
        a["positions"][0]["positionAmt"] = "0.5".into();
        assert!(parse(&a.to_string(), 123).is_err());
    }
    #[test]
    fn retains_delivery_and_hedged_positions_separately() {
        let mut a = account();
        let mut p = a["positions"][0].clone();
        p["symbol"] = "BTCUSD_261225".into();
        p["positionSide"] = "LONG".into();
        p["positionAmt"] = "10".into();
        a["positions"].as_array_mut().unwrap().push(p);
        let s = parse(&a.to_string(), 123).unwrap();
        assert_eq!(s.positions.len(), 2);
        assert!(s.positions.iter().any(|p| p.symbol == "BTCUSD_261225"));
    }
}
