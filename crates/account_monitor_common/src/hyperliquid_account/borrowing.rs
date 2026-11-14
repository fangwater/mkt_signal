use super::*;

#[derive(Debug, Clone)]
pub(super) struct BorrowSnapshot {
    pub observed_at_ms: i64,
    pub by_asset: HashMap<String, (f64, f64)>,
    pub borrowed_usd: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::split_basic_account_event;

    fn processor() -> HyperliquidAccountProcessor {
        HyperliquidAccountProcessor::new(
            "0x1111111111111111111111111111111111111111",
            HyperliquidAssetCatalog::from_meta(
                &json!({"universe":[]}),
                &json!({
                    "tokens":[{"name":"USDC","index":0}], "universe":[]
                }),
            )
            .unwrap(),
            HyperliquidAccountMode::PortfolioMargin,
            FillSnapshotPolicy::Ignore,
        )
        .unwrap()
    }

    fn borrowing(basis: &str, value: &str) -> Value {
        json!({"tokenToState":[[0,{
            "borrow":{"basis":basis,"value":value},
            "supply":{"basis":"0","value":"0"}
        }]],"health":"healthy","healthFactor":null})
    }

    fn spot(total: &str) -> Value {
        json!({"portfolioMarginRatio":"0.25", "balances":[{
            "coin":"USDC","token":0,"total":total,"hold":"0","entryNtl":"0"
        }]})
    }

    fn financial(events: &[Bytes]) -> (BasicBalanceMsg, BasicBorrowInterestMsg) {
        let wallet = events
            .iter()
            .find_map(|event| {
                let (kind, _, body) = split_basic_account_event(event)?;
                (kind == BasicAccountEventType::BalanceUpdate)
                    .then(|| BasicBalanceMsg::from_bytes(body).unwrap())
            })
            .unwrap();
        let borrow = events
            .iter()
            .find_map(|event| {
                let (kind, _, body) = split_basic_account_event(event)?;
                (kind == BasicAccountEventType::BorrowInterest)
                    .then(|| BasicBorrowInterestMsg::from_bytes(body).unwrap())
            })
            .unwrap();
        (wallet, borrow)
    }

    #[test]
    fn pm_normalizes_gross_and_debt_without_changing_net() {
        let mut processor = processor();
        processor
            .seed_borrow_lend_user_state(&borrowing("20", "20.25"), 1000)
            .unwrap();
        for total in ["-20.25", "100"] {
            let events = processor.apply_spot_snapshot(&spot(total), 1010).unwrap();
            let (wallet, borrow) = financial(&events);
            assert_eq!(borrow.borrowed, 20.0);
            assert_eq!(borrow.interest, 0.25);
            assert_eq!(borrow.timestamp, 1000);
            assert_eq!(
                wallet.wallet - borrow.borrowed - borrow.interest,
                total.parse::<f64>().unwrap()
            );
            let risk = events
                .iter()
                .find_map(|event| {
                    let (kind, _, body) = split_basic_account_event(event)?;
                    (kind == BasicAccountEventType::AccountRisk)
                        .then(|| BasicAccountRiskMsg::from_bytes(body).unwrap())
                })
                .unwrap();
            assert!(risk.actual_equity_usd.is_nan());
            assert_eq!(risk.margin_ratio, 3.8);
        }
    }

    #[test]
    fn pm_missing_stale_or_invalid_borrowing_fails_closed_atomically() {
        let mut processor = processor();
        assert!(processor.apply_spot_snapshot(&spot("100"), 1000).is_err());
        processor
            .seed_borrow_lend_user_state(&borrowing("20", "20.25"), 1000)
            .unwrap();
        for (basis, value) in [("NaN", "20"), ("-1", "20"), ("21", "20")] {
            assert!(processor
                .seed_borrow_lend_user_state(&borrowing(basis, value), 1001)
                .is_err());
        }
        assert_eq!(
            processor.borrow_snapshot.as_ref().unwrap().observed_at_ms,
            1000
        );
        assert!(processor.apply_spot_snapshot(&spot("100"), 999).is_err());
        assert!(processor.apply_spot_snapshot(&spot("100"), 61_000).is_err());
        assert!(processor.balances.is_empty());
        assert!(!processor.spot_snapshot_seen);
        assert!(processor
            .seed_borrow_lend_user_state(&borrowing("0", "0"), 999)
            .is_err());
        let mut bad_identity = spot("100");
        bad_identity["balances"][0]["token"] = json!(42);
        assert!(processor.apply_spot_snapshot(&bad_identity, 1001).is_err());
    }

    #[test]
    fn pm_borrowed_usd_uses_reserve_oracles_and_rejects_overflow() {
        let mut processor = processor();
        let reserves = json!([[0,{
            "borrowYearlyRate":"0.05", "supplyYearlyRate":"0.01",
            "balance":"100", "utilization":"0.1", "oraclePx":"0.99",
            "ltv":"0", "totalSupplied":"1000", "totalBorrowed":"100"
        }]]);
        processor
            .process_borrow_lend_snapshot(&borrowing("20", "20.25"), &reserves, 1000)
            .unwrap();
        let events = processor
            .apply_spot_snapshot(&spot("-20.25"), 1001)
            .unwrap();
        let risk = events
            .iter()
            .find_map(|event| {
                let (kind, _, body) = split_basic_account_event(event)?;
                (kind == BasicAccountEventType::AccountRisk)
                    .then(|| BasicAccountRiskMsg::from_bytes(body).unwrap())
            })
            .unwrap();
        assert_eq!(risk.borrowed_usd, 20.25 * 0.99);
        assert!(risk.actual_equity_usd.is_nan());
        let mut invalid = reserves;
        invalid[0][1]["oraclePx"] = json!("1e308");
        assert!(processor
            .process_borrow_lend_snapshot(&borrowing("20", "20.25"), &invalid, 1002)
            .is_err());
        assert_eq!(
            processor.borrow_snapshot.as_ref().unwrap().observed_at_ms,
            1000
        );
    }

    #[test]
    fn pm_repayment_clears_previously_published_debt_even_without_a_spot_row() {
        let mut processor = processor();
        processor
            .seed_borrow_lend_user_state(&borrowing("20", "20.25"), 1000)
            .unwrap();
        let empty_spot = json!({"balances":[],"portfolioMarginRatio":"0"});
        let (wallet, borrow) =
            financial(&processor.apply_spot_snapshot(&empty_spot, 1001).unwrap());
        assert_eq!(wallet.wallet - borrow.borrowed - borrow.interest, 0.0);
        processor
            .seed_borrow_lend_user_state(
                &json!({"tokenToState":[],"health":"healthy","healthFactor":null}),
                1002,
            )
            .unwrap();
        let (wallet, borrow) =
            financial(&processor.apply_spot_snapshot(&empty_spot, 1003).unwrap());
        assert_eq!(
            (wallet.wallet, borrow.borrowed, borrow.interest),
            (0.0, 0.0, 0.0)
        );
    }
}

impl BorrowSnapshot {
    pub fn validate_freshness(&self, now_ms: i64) -> Result<()> {
        let age = now_ms
            .checked_sub(self.observed_at_ms)
            .context("Hyperliquid borrow snapshot age overflow")?;
        if !(0..HYPERLIQUID_BORROW_SNAPSHOT_TTL_MS).contains(&age) {
            anyhow::bail!("Hyperliquid borrow snapshot is stale or future-dated: age_ms={age}");
        }
        Ok(())
    }
}

impl HyperliquidAccountProcessor {
    /// Receipt time is local, not a claim that HTTP borrowing and WS balances
    /// form an atomic venue snapshot. The paired normalization preserves net.
    pub fn seed_borrow_lend_user_state(
        &mut self,
        state: &Value,
        observed_at_ms: i64,
    ) -> Result<()> {
        if observed_at_ms < 0
            || self
                .borrow_snapshot
                .as_ref()
                .is_some_and(|previous| observed_at_ms < previous.observed_at_ms)
        {
            anyhow::bail!("Hyperliquid borrow snapshot timestamp regressed");
        }
        let mut by_asset = HashMap::new();
        for row in state
            .get("tokenToState")
            .and_then(Value::as_array)
            .context("missing Hyperliquid borrow/lend tokenToState")?
        {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .context("invalid Hyperliquid borrow/lend user pair")?;
            let token = pair[0].as_i64().context("invalid borrow/lend user token")?;
            let asset = self
                .catalog
                .spot_assets_by_token
                .get(&token)
                .with_context(|| format!("unknown Hyperliquid borrow/lend token: {token}"))?;
            let mut debt = (0.0, 0.0);
            for leg in ["borrow", "supply"] {
                let values = pair[1]
                    .get(leg)
                    .context("missing Hyperliquid borrow/lend leg")?;
                let basis = required_f64(values, "basis")?;
                let value = required_f64(values, "value")?;
                validate_nonnegative_finite("basis", basis)?;
                validate_nonnegative_finite("value", value)?;
                if value < basis {
                    anyhow::bail!(
                        "Hyperliquid {leg} value is smaller than its basis for token {token}"
                    );
                }
                if leg == "borrow" {
                    debt = (basis, value - basis);
                }
            }
            if by_asset.insert(asset.clone(), debt).is_some() {
                anyhow::bail!("duplicate Hyperliquid borrow/lend asset: {asset}");
            }
        }
        required_nonempty_string(state, "health")?;
        let health_factor = state
            .get("healthFactor")
            .context("missing Hyperliquid borrow/lend healthFactor")?;
        if !health_factor.is_null() {
            validate_nonnegative_finite("healthFactor", required_f64(state, "healthFactor")?)?;
        }
        self.borrow_snapshot = Some(BorrowSnapshot {
            observed_at_ms,
            by_asset,
            borrowed_usd: None,
        });
        Ok(())
    }
}
