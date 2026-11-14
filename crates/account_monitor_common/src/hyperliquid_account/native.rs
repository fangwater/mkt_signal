use super::*;
use mkt_parsers::msg::hyperliquid_native_msg::{
    HyperliquidNativeEventMsg, HyperliquidNativeSource as Source,
};

#[derive(Debug, Clone, Default)]
pub(super) struct NativeDedup {
    events: HashMap<(Source, String), String>,
    age: VecDeque<(Source, String)>,
    snapshots: HashMap<(Source, String), String>,
}

impl HyperliquidAccountProcessor {
    pub(super) fn process_order_updates(&mut self, root: &Value) -> Result<Vec<Bytes>> {
        let mut candidate = self.clone();
        let mut output = candidate.process_order_updates_inner(root)?;
        let now_ms = Utc::now().timestamp_millis();
        for row in root
            .get("data")
            .and_then(Value::as_array)
            .context("missing Hyperliquid order rows")?
        {
            let Some((oid, identity)) = candidate.parse_order_identity(row)? else {
                continue;
            };
            if identity.ipc_intent().is_some() {
                continue;
            }
            let mut canonical = row.clone();
            canonical.sort_all_objects();
            let digest = hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?));
            output.extend(candidate.native_event(
                Source::OrderLifecycle,
                format!("oid:{oid}:{digest}"),
                row,
                now_ms,
                false,
            )?);
        }
        *self = candidate;
        Ok(output)
    }

    pub fn process_borrow_lend_snapshot(
        &mut self,
        user_state: &Value,
        reserves: &Value,
        now_ms: i64,
    ) -> Result<Vec<Bytes>> {
        let mut candidate = self.clone();
        let reserve_rows = reserves
            .as_array()
            .context("Hyperliquid borrow/lend reserves must be an array")?;
        let mut tokens = HashSet::new();
        let mut oracles = HashMap::new();
        for row in reserve_rows {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .context("invalid Hyperliquid reserve pair")?;
            let token = pair[0]
                .as_i64()
                .filter(|value| *value >= 0)
                .context("invalid reserve token")?;
            if !tokens.insert(token) {
                anyhow::bail!("duplicate Hyperliquid reserve token");
            }
            for field in [
                "borrowYearlyRate",
                "supplyYearlyRate",
                "balance",
                "totalSupplied",
                "totalBorrowed",
            ] {
                validate_nonnegative_finite(field, required_f64(&pair[1], field)?)?;
            }
            validate_positive_finite("borrow oracle", required_f64(&pair[1], "oraclePx")?)?;
            oracles.insert(token, required_f64(&pair[1], "oraclePx")?);
            for field in ["utilization", "ltv"] {
                let value = required_f64(&pair[1], field)?;
                if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                    anyhow::bail!("invalid Hyperliquid reserve {field}");
                }
            }
        }
        let mut seen = HashSet::new();
        let mut borrowed_usd = 0.0;
        for row in user_state
            .get("tokenToState")
            .and_then(Value::as_array)
            .context("missing Hyperliquid borrow/lend tokenToState")?
        {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .context("invalid Hyperliquid borrow/lend user pair")?;
            let token = pair[0].as_i64().context("invalid borrow/lend user token")?;
            if !tokens.contains(&token) || !seen.insert(token) {
                anyhow::bail!("unknown/duplicate Hyperliquid borrow/lend user token");
            }
            for leg in ["borrow", "supply"] {
                let leg = pair[1]
                    .get(leg)
                    .context("missing Hyperliquid borrow/lend leg")?;
                for field in ["basis", "value"] {
                    validate_nonnegative_finite(field, required_f64(leg, field)?)?;
                }
            }
            borrowed_usd += required_f64(&pair[1]["borrow"], "value")? * oracles[&token];
        }
        validate_nonnegative_finite("oracle-valued borrowed USD", borrowed_usd)?;
        required_nonempty_string(user_state, "health")?;
        let health_factor = user_state
            .get("healthFactor")
            .context("missing Hyperliquid borrow/lend healthFactor")?;
        if !health_factor.is_null() {
            validate_nonnegative_finite("healthFactor", required_f64(user_state, "healthFactor")?)?;
        }
        candidate.seed_borrow_lend_user_state(user_state, now_ms)?;
        if let Some(snapshot) = candidate.borrow_snapshot.as_mut() {
            snapshot.borrowed_usd = Some(borrowed_usd);
        }
        let mut output = Vec::new();
        output.extend(candidate.native_event(
            Source::BorrowLendReserves,
            "reserves".into(),
            reserves,
            now_ms,
            true,
        )?);
        output.extend(candidate.native_event(
            Source::BorrowLendUser,
            "user".into(),
            user_state,
            now_ms,
            true,
        )?);
        *self = candidate;
        Ok(output)
    }
    fn native_event(
        &mut self,
        source: Source,
        key: String,
        payload: &Value,
        now_ms: i64,
        snapshot: bool,
    ) -> Result<Option<Bytes>> {
        let observed_at_us = now_ms
            .checked_mul(1_000)
            .context("Hyperliquid receipt timestamp overflow")?;
        let mut message =
            HyperliquidNativeEventMsg::create(observed_at_us, source, key.clone(), payload)?;
        let cache_key = (source, key);
        let cache = if snapshot {
            &self.native_events.snapshots
        } else {
            &self.native_events.events
        };
        if let Some(previous) = cache.get(&cache_key) {
            if previous == &message.payload_json {
                return Ok(None);
            }
            if !snapshot {
                anyhow::bail!(
                    "conflicting Hyperliquid native event identity {:?}",
                    cache_key
                );
            }
        }
        if snapshot {
            let digest = hex::encode(Sha256::digest(message.payload_json.as_bytes()));
            message
                .event_key
                .push_str(&format!(":observed:{observed_at_us}:{digest}"));
        }
        let event = self.wrap_venue(
            TradingVenue::HyperliquidFutures,
            BasicAccountEventType::HyperliquidNativeEvent,
            message.to_bytes(),
        );
        validate_pm_event_size("Hyperliquid native event", &event)?;
        if snapshot {
            self.native_events
                .snapshots
                .insert(cache_key, message.payload_json);
        } else {
            self.native_events
                .events
                .insert(cache_key.clone(), message.payload_json);
            self.native_events.age.push_back(cache_key);
            while self.native_events.age.len() > DEFAULT_DEDUP_CAPACITY {
                if let Some(oldest) = self.native_events.age.pop_front() {
                    self.native_events.events.remove(&oldest);
                }
            }
        }
        Ok(Some(event))
    }

    pub(super) fn process_native_frame(
        &mut self,
        root: &Value,
        now_ms: i64,
        fill_context: FillSnapshotContext,
    ) -> Result<Vec<Bytes>> {
        let mut candidate = self.clone();
        let events = candidate.process_native_frame_inner(root, now_ms, fill_context)?;
        *self = candidate;
        Ok(events)
    }

    fn process_native_frame_inner(
        &mut self,
        root: &Value,
        now_ms: i64,
        fill_context: FillSnapshotContext,
    ) -> Result<Vec<Bytes>> {
        let channel = required_str(root, "channel")?;
        let data = root
            .get("data")
            .filter(|data| data.is_object())
            .context("Hyperliquid native data must be an object")?;
        let mut output = Vec::new();
        match channel {
            "user" => {
                // This venue channel omits user identity: the exact per-socket
                // userEvents ACK binds it to the account. Validate any supplied identity too.
                if data.get("user").is_some() {
                    self.validate_user(data)?;
                }
                let kinds = ["fills", "funding", "liquidation", "nonUserCancel"];
                if kinds
                    .iter()
                    .filter(|kind| data.get(**kind).is_some())
                    .count()
                    != 1
                {
                    anyhow::bail!(
                        "Hyperliquid user event must contain one documented event variant"
                    );
                }
                if let Some(fills) = data.get("fills") {
                    return self.process_user_fills(
                        &json!({"channel":"userFills","data":{"user":self.user,"fills":fills}}),
                        now_ms,
                        fill_context,
                    );
                }
                if let Some(funding) = data.get("funding") {
                    return self.process_user_fundings(&json!({"channel":"userFundings","data":{"user":self.user,"fundings":[funding]}}));
                }
                if let Some(liquidation) = data.get("liquidation") {
                    let lid = required_i64(liquidation, "lid")?;
                    if lid < 0 {
                        anyhow::bail!("negative Hyperliquid liquidation id");
                    }
                    let liquidator =
                        normalize_hyperliquid_address(required_str(liquidation, "liquidator")?)?;
                    let liquidated = normalize_hyperliquid_address(required_str(
                        liquidation,
                        "liquidated_user",
                    )?)?;
                    if liquidator != self.user && liquidated != self.user {
                        anyhow::bail!("Hyperliquid liquidation belongs to another account");
                    }
                    required_nonnegative_decimal_string(liquidation, "liquidated_ntl_pos")?;
                    required_finite_decimal_string(liquidation, "liquidated_account_value")?;
                    output.extend(self.native_event(
                        Source::Liquidation,
                        format!("lid:{lid}"),
                        liquidation,
                        now_ms,
                        false,
                    )?);
                }
                if let Some(cancels) = data.get("nonUserCancel") {
                    for cancel in cancels
                        .as_array()
                        .context("Hyperliquid nonUserCancel must be an array")?
                    {
                        let coin = required_str(cancel, "coin")?;
                        self.catalog
                            .resolve(coin)
                            .context("unknown Hyperliquid system-cancel coin")?;
                        let oid = required_i64(cancel, "oid")?;
                        if oid <= 0 {
                            anyhow::bail!("invalid Hyperliquid system-cancel oid");
                        }
                        output.extend(self.native_event(
                            Source::NonUserCancel,
                            format!("{coin}:{oid}"),
                            cancel,
                            now_ms,
                            false,
                        )?);
                    }
                }
            }
            "twapStates" => {
                self.validate_user(data)?;
                let dex = required_str(data, "dex")?;
                if !self.catalog.dex_collateral_tokens.contains_key(dex) {
                    anyhow::bail!("unknown Hyperliquid TWAP DEX {dex:?}");
                }
                let states = data
                    .get("states")
                    .and_then(Value::as_array)
                    .context("Hyperliquid twapStates missing states")?;
                let mut ids = HashSet::new();
                for pair in states {
                    let pair = pair
                        .as_array()
                        .filter(|pair| pair.len() == 2)
                        .context("invalid Hyperliquid TWAP state pair")?;
                    let id = pair[0]
                        .as_i64()
                        .filter(|id| *id >= 0)
                        .context("invalid Hyperliquid TWAP state id")?;
                    if !ids.insert(id) {
                        anyhow::bail!("duplicate Hyperliquid TWAP state id");
                    }
                    let state = &pair[1];
                    self.validate_user(state)?;
                    let coin = required_str(state, "coin")?;
                    self.catalog
                        .resolve(coin)
                        .context("unknown Hyperliquid TWAP state coin")?;
                    if coin.split_once(':').map_or("", |(prefix, _)| prefix) != dex {
                        anyhow::bail!("Hyperliquid TWAP coin/DEX mismatch");
                    }
                    parse_side(required_str(state, "side")?)?;
                    for field in ["sz", "executedSz", "executedNtl"] {
                        validate_nonnegative_finite(field, required_f64(state, field)?)?;
                    }
                    if required_i64(state, "minutes")? <= 0 || required_i64(state, "timestamp")? < 0
                    {
                        anyhow::bail!("invalid Hyperliquid TWAP state time");
                    }
                    required_bool(state, "reduceOnly")?;
                    required_bool(state, "randomize")?;
                }
                output.extend(self.native_event(
                    Source::TwapStates,
                    format!("dex:{dex}"),
                    data,
                    now_ms,
                    true,
                )?);
            }
            "activeAssetData" => {
                self.validate_user(data)?;
                let coin = required_str(data, "coin")?;
                if !self
                    .catalog
                    .active_perp_coins
                    .iter()
                    .any(|active| active == coin)
                {
                    anyhow::bail!("unknown/inactive Hyperliquid activeAssetData coin");
                }
                let leverage = data
                    .get("leverage")
                    .context("missing Hyperliquid asset leverage")?;
                validate_positive_finite("Hyperliquid leverage", required_f64(leverage, "value")?)?;
                if !matches!(required_str(leverage, "type")?, "cross" | "isolated") {
                    anyhow::bail!("invalid Hyperliquid leverage");
                }
                for field in ["maxTradeSzs", "availableToTrade"] {
                    let values = data
                        .get(field)
                        .and_then(Value::as_array)
                        .filter(|values| values.len() == 2)
                        .context("Hyperliquid asset trading capacity must have buy/sell values")?;
                    for value in values {
                        let number = value
                            .as_f64()
                            .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
                            .context("invalid Hyperliquid asset trading capacity")?;
                        validate_nonnegative_finite(field, number)?;
                    }
                }
                output.extend(self.native_event(
                    Source::ActiveAssetData,
                    coin.to_string(),
                    data,
                    now_ms,
                    true,
                )?);
            }
            "notification" => {
                if data.get("user").is_some() {
                    self.validate_user(data)?;
                }
                required_str(data, "notification")?;
                output.extend(self.native_event(
                    Source::Notification,
                    "notification".into(),
                    data,
                    now_ms,
                    true,
                )?);
            }
            "webData3" => {
                let user_state = data
                    .get("userState")
                    .context("Hyperliquid webData3 missing userState")?;
                self.validate_user(user_state)?;
                // UI aggregates are evidence only, never a replacement for canonical state streams.
                output.extend(self.native_event(
                    Source::WebData,
                    "webData3".into(),
                    data,
                    now_ms,
                    true,
                )?);
            }
            _ => anyhow::bail!("unsupported Hyperliquid native channel {channel}"),
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const USER: &str = "0x1111111111111111111111111111111111111111";
    const OTHER: &str = "0x2222222222222222222222222222222222222222";

    fn processor() -> HyperliquidAccountProcessor {
        let catalog = HyperliquidAssetCatalog::from_meta(
            &json!({"universe":[{"name":"BTC"},{"name":"ETH"},{"name":"OLD","isDelisted":true}]}),
            &json!({"tokens":[{"name":"USDC","index":0}],"universe":[]}),
        )
        .unwrap();
        HyperliquidAccountProcessor::new(
            USER,
            catalog,
            HyperliquidAccountMode::Unified,
            FillSnapshotPolicy::Process,
        )
        .unwrap()
    }

    fn decode(event: &Bytes) -> HyperliquidNativeEventMsg {
        let (kind, _, body) =
            mkt_parsers::msg::basic_account_msg::split_basic_account_event(event).unwrap();
        assert_eq!(kind, BasicAccountEventType::HyperliquidNativeEvent);
        HyperliquidNativeEventMsg::from_bytes(body).unwrap()
    }

    #[test]
    fn hyperliquid_native_liquidation_is_identity_bound_audit_only_and_conflict_atomic() {
        let mut processor = processor();
        let mut frame = json!({"channel":"user","data":{"liquidation":{
            "lid":7,"liquidator":OTHER,"liquidated_user":USER,"liquidated_ntl_pos":"12.00","liquidated_account_value":"-0.100","extra":true
        }}});
        let events = processor.process_value_at(&frame, 100).unwrap();
        assert_eq!(events.len(), 1);
        let msg = decode(&events[0]);
        assert_eq!(msg.source, Source::Liquidation);
        assert!(msg.payload_json.contains("-0.100"));
        assert!(processor.positions.is_empty());
        assert!(processor.process_value_at(&frame, 101).unwrap().is_empty());
        frame["data"]["liquidation"]["liquidated_account_value"] = json!("1");
        assert!(processor.process_value_at(&frame, 102).is_err());
        assert_eq!(processor.native_events.events.len(), 1);
        frame["data"]["liquidation"]["liquidated_user"] = json!(OTHER);
        assert!(processor.process_value_at(&frame, 103).is_err());
    }

    #[test]
    fn hyperliquid_native_cancel_batch_is_atomic_and_does_not_create_orders() {
        let mut processor = processor();
        let mut frame = json!({"channel":"user","data":{"nonUserCancel":[{"coin":"BTC","oid":9},{"coin":"ETH","oid":0}]}});
        assert!(processor.process_value_at(&frame, 100).is_err());
        assert!(processor.native_events.events.is_empty());
        frame["data"]["nonUserCancel"][1]["oid"] = json!(10);
        let events = processor.process_value_at(&frame, 101).unwrap();
        assert_eq!(events.len(), 2);
        assert!(processor.orders.is_empty());
        assert_eq!(decode(&events[0]).source, Source::NonUserCancel);
    }

    #[test]
    fn hyperliquid_native_ack_correlates_same_type_by_coin_and_dex() {
        let processor = processor();
        let requests = subscription_messages_for_catalog(
            USER,
            HyperliquidAccountMode::Unified,
            &processor.catalog,
        )
        .unwrap();
        assert!(!requests
            .iter()
            .any(|request| request["subscription"]["coin"] == "OLD"));
        let mut acks = HyperliquidSubscriptionAcks::from_requests(&requests).unwrap();
        let btc = requests
            .iter()
            .find(|request| request["subscription"]["coin"] == "BTC")
            .unwrap();
        acks.observe(&json!({"channel":"subscriptionResponse","data":btc}))
            .unwrap();
        assert!(acks
            .has_acknowledged_frame(&json!({"channel":"activeAssetData","data":{"coin":"BTC"}})));
        assert!(!acks
            .has_acknowledged_frame(&json!({"channel":"activeAssetData","data":{"coin":"ETH"}})));
        assert!(!acks.is_complete());
        let user_events = requests
            .iter()
            .find(|request| request["subscription"]["type"] == "userEvents")
            .unwrap();
        acks.observe(&json!({"channel":"subscriptionResponse","data":user_events}))
            .unwrap();
        assert!(acks.has_acknowledged_frame(&json!({"channel":"user","data":{}})));
    }

    #[test]
    fn hyperliquid_native_asset_and_empty_twap_snapshots_preserve_changes() {
        let mut processor = processor();
        let mut frame = json!({"channel":"activeAssetData","data":{"user":USER,"coin":"BTC","leverage":{"type":"cross","value":3},"maxTradeSzs":["1.0","2.0"],"availableToTrade":["10.00","20.00"]}});
        let first = processor.process_value_at(&frame, 100).unwrap();
        assert!(processor.process_value_at(&frame, 101).unwrap().is_empty());
        frame["data"]["availableToTrade"][0] = json!("11.00");
        let changed = processor.process_value_at(&frame, 100).unwrap();
        assert_ne!(
            decode(&first[0]).stable_venue_key(),
            decode(&changed[0]).stable_venue_key()
        );
        let empty = json!({"channel":"twapStates","data":{"user":USER,"dex":"","states":[]}});
        assert_eq!(processor.process_value_at(&empty, 102).unwrap().len(), 1);
        frame["data"]["availableToTrade"] = json!([1]);
        assert!(processor.process_value_at(&frame, 103).is_err());
        frame["data"]["availableToTrade"] = json!(["10", "20"]);
        frame["data"]["leverage"]["value"] = json!("NaN");
        assert!(processor.process_value_at(&frame, 104).is_err());
    }

    #[test]
    fn hyperliquid_native_oversized_snapshot_rejects_without_dedup_progress() {
        let mut processor = processor();
        let frame = json!({"channel":"webData3","data":{"userState":{"user":USER},"extra":"x".repeat(20_000)}});
        assert!(processor.process_value_at(&frame, 100).is_err());
        assert!(processor.native_events.snapshots.is_empty());
        let small =
            json!({"channel":"webData3","data":{"userState":{"user":USER},"extra":"retained"}});
        let events = processor.process_value_at(&small, 101).unwrap();
        assert_eq!(events.len(), 1);
        assert!(decode(&events[0]).payload_json.contains("retained"));
    }

    #[test]
    fn hyperliquid_native_borrow_state_retains_oracle_ltv_without_invented_risk() {
        let mut processor = processor();
        let reserves = json!([[0,{"borrowYearlyRate":"0.05","supplyYearlyRate":"0.01","balance":"9","utilization":"0.1","oraclePx":"1.0","ltv":"0.0","totalSupplied":"10","totalBorrowed":"1"}]]);
        let user_state = json!({"tokenToState":[[0,{"borrow":{"basis":"0","value":"0"},"supply":{"basis":"1.0","value":"1.01"}}]],"health":"healthy","healthFactor":null});
        let events = processor
            .process_borrow_lend_snapshot(&user_state, &reserves, 100)
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(decode(&events[1]).source, Source::BorrowLendUser);
        let mut invalid = reserves;
        invalid[0][1]["ltv"] = json!("2.0");
        assert!(processor
            .process_borrow_lend_snapshot(&user_state, &invalid, 101)
            .is_err());
        assert_eq!(processor.native_events.snapshots.len(), 2);
    }
}
