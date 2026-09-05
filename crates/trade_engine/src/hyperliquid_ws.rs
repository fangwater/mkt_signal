use crate::query_request::{HyperliquidQueryParams, QueryRequestMsg, QueryRequestType};
use crate::trade_request::{
    HyperliquidCancelOrderParams, HyperliquidNewOrderParamsRef, TradeRequestMsg, TradeRequestType,
};
use account_monitor_common::hyperliquid_account::{
    parse_user_role, resolve_user_abstraction, FillSnapshotPolicy, HyperliquidAccountMode,
    HyperliquidAccountProcessor, HyperliquidAssetCatalog as HyperliquidAccountAssetCatalog,
    HyperliquidUserRole,
};
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use k256::ecdsa::SigningKey;
use mkt_parsers::msg::basic_account_msg::{split_basic_account_event, BasicAccountEventType};
use mkt_parsers::msg::hyperliquid_account_msg::hyperliquid_account_identity_hash;
use order_common::{hyperliquid_cloid_from_client_order_id, OrderStatus, OrderType, Side};
use runtime_common::symbol_util::{hyperliquid_internal_symbol, HyperliquidSpotBaseResolver};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
#[cfg(test)]
use signal_common::hyperliquid::DEFAULT_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS;
use signal_common::hyperliquid::{
    hyperliquid_action_expires_after_ms, HyperliquidEndpoints, HYPERLIQUID_MAINNET_WS_URL,
    HYPERLIQUID_TESTNET_WS_URL,
};
use signal_common::tick_math::QuantizedValue;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAINNET_WS_URL: &str = HYPERLIQUID_MAINNET_WS_URL;
pub const TESTNET_WS_URL: &str = HYPERLIQUID_TESTNET_WS_URL;
static HYPERLIQUID_LAST_NONCE: AtomicU64 = AtomicU64::new(0);
const HYPERLIQUID_FIRST_HIP3_ASSET_ID: u32 = 110_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidAsset {
    pub asset_id: u32,
    pub sz_decimals: u32,
    pub is_spot: bool,
}

#[derive(Debug)]
struct HyperliquidAssetCatalog {
    spot: HashMap<String, HyperliquidAsset>,
    perp: HashMap<String, HyperliquidAsset>,
}

impl HyperliquidAssetCatalog {
    #[cfg(test)]
    fn from_values(perp: Value, spot: Value) -> Result<Self> {
        let mut default_all_meta = perp.clone();
        default_all_meta
            .as_object_mut()
            .context("Hyperliquid meta must be an object")?
            .entry("collateralToken")
            .or_insert_with(|| Value::from(0));
        Self::from_all_values(&perp, &spot, &json!([null]), &json!([default_all_meta]))
    }

    fn from_all_values(
        default_perp: &Value,
        spot: &Value,
        perp_dexs: &Value,
        all_perp_metas: &Value,
    ) -> Result<Self> {
        let default_perp: PerpMeta = serde_json::from_value(default_perp.clone())
            .context("decode Hyperliquid default meta")?;
        let spot: SpotMeta =
            serde_json::from_value(spot.clone()).context("decode Hyperliquid spotMeta")?;
        let dex_rows = perp_dexs
            .as_array()
            .context("Hyperliquid perpDexs response must be an array")?;
        let meta_rows = all_perp_metas
            .as_array()
            .context("Hyperliquid allPerpMetas response must be an array")?;
        if dex_rows.is_empty() || dex_rows.len() != meta_rows.len() {
            bail!(
                "Hyperliquid perpDexs/allPerpMetas length mismatch or empty: {} != {}",
                dex_rows.len(),
                meta_rows.len()
            );
        }
        if !dex_rows[0].is_null() {
            bail!("Hyperliquid perpDexs index 0 must be the null default DEX");
        }
        let all_metas = meta_rows
            .iter()
            .enumerate()
            .map(|(index, value)| {
                serde_json::from_value::<PerpMeta>(value.clone())
                    .with_context(|| format!("decode Hyperliquid allPerpMetas entry {index}"))
            })
            .collect::<Result<Vec<_>>>()?;
        if all_metas[0].universe != default_perp.universe {
            bail!("Hyperliquid meta and allPerpMetas default-Dex universe disagree");
        }

        let mut tokens = HashMap::with_capacity(spot.tokens.len());
        for token in &spot.tokens {
            if tokens.insert(token.index, token).is_some() {
                bail!("duplicate Hyperliquid spot token index {}", token.index);
            }
        }

        let mut perp_assets = HashMap::new();
        let mut wire_coins = HashSet::new();
        let mut dex_names = HashSet::new();
        for (dex_index, (dex_row, meta)) in dex_rows.iter().zip(&all_metas).enumerate() {
            let dex_name = if dex_index == 0 {
                String::new()
            } else {
                let name = dex_row
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .with_context(|| {
                        format!("Hyperliquid perpDexs entry {dex_index} missing name")
                    })?;
                if name.trim() != name || !name.is_ascii() || name.contains(':') {
                    bail!("invalid Hyperliquid perp DEX name {name:?}");
                }
                name.to_string()
            };
            if !dex_names.insert(dex_name.to_ascii_lowercase()) {
                bail!("duplicate Hyperliquid perp DEX name {dex_name:?}");
            }
            let collateral_token = meta.collateral_token.with_context(|| {
                format!("Hyperliquid allPerpMetas entry {dex_index} missing collateralToken")
            })?;
            let collateral = tokens.get(&collateral_token).with_context(|| {
                format!(
                    "Hyperliquid perp DEX {dex_name:?} references unknown collateral token {}",
                    collateral_token
                )
            })?;
            for (asset_index, asset) in meta.universe.iter().enumerate() {
                if asset.is_delisted {
                    continue;
                }
                if dex_index > 0 {
                    let expected_prefix = format!("{dex_name}:");
                    if !asset.name.starts_with(&expected_prefix)
                        || asset.name.len() == expected_prefix.len()
                    {
                        bail!(
                            "Hyperliquid HIP-3 coin {:?} does not match DEX prefix {:?}",
                            asset.name,
                            expected_prefix
                        );
                    }
                } else if asset.name.contains(':') {
                    bail!(
                        "Hyperliquid default-Dex coin must not contain a DEX prefix: {:?}",
                        asset.name
                    );
                }
                if !wire_coins.insert(asset.name.to_ascii_lowercase()) {
                    bail!("duplicate Hyperliquid perp wire coin {:?}", asset.name);
                }
                let symbol = hyperliquid_internal_symbol(&asset.name, &collateral.name)?;
                let asset_index =
                    u32::try_from(asset_index).context("Hyperliquid perp asset index overflow")?;
                let asset_id = if dex_index == 0 {
                    asset_index
                } else {
                    let dex_index =
                        u32::try_from(dex_index).context("Hyperliquid perp DEX index overflow")?;
                    100_000_u32
                        .checked_add(
                            dex_index
                                .checked_mul(10_000)
                                .context("Hyperliquid HIP-3 DEX asset-id overflow")?,
                        )
                        .and_then(|base| base.checked_add(asset_index))
                        .context("Hyperliquid HIP-3 asset-id overflow")?
                };
                if perp_assets
                    .insert(
                        symbol.clone(),
                        HyperliquidAsset {
                            asset_id,
                            sz_decimals: asset.sz_decimals,
                            is_spot: false,
                        },
                    )
                    .is_some()
                {
                    bail!("duplicate Hyperliquid perp internal symbol {symbol}");
                }
            }
        }

        let base_resolver =
            HyperliquidSpotBaseResolver::new(tokens.values().map(|token| token.name.as_str()));
        let mut spot_assets = HashMap::with_capacity(spot.universe.len());
        for market in &spot.universe {
            let base = tokens.get(&market.tokens[0]).ok_or_else(|| {
                anyhow!(
                    "Hyperliquid spot market index={} references unknown base token={}",
                    market.index,
                    market.tokens[0]
                )
            })?;
            let quote = tokens.get(&market.tokens[1]).ok_or_else(|| {
                anyhow!(
                    "Hyperliquid spot market index={} references unknown quote token={}",
                    market.index,
                    market.tokens[1]
                )
            })?;
            if !quote.name.eq_ignore_ascii_case("USDC") {
                continue;
            }
            let canonical_base = base_resolver.canonical_base(&base.name);
            let symbol = hyperliquid_internal_symbol(&canonical_base, &quote.name)?;
            let asset_id = 10_000u32
                .checked_add(market.index)
                .context("Hyperliquid spot asset id overflow")?;
            if spot_assets
                .insert(
                    symbol.clone(),
                    HyperliquidAsset {
                        asset_id,
                        sz_decimals: base.sz_decimals,
                        is_spot: true,
                    },
                )
                .is_some()
            {
                bail!("duplicate Hyperliquid USDC spot symbol {symbol}");
            }
        }
        Ok(Self {
            spot: spot_assets,
            perp: perp_assets,
        })
    }

    fn get(&self, req_type: TradeRequestType, symbol: &str) -> Option<HyperliquidAsset> {
        let symbol = canonical_symbol(symbol);
        match req_type {
            TradeRequestType::HyperliquidNewMarginOrder
            | TradeRequestType::HyperliquidCancelMarginOrder => self.spot.get(&symbol).copied(),
            TradeRequestType::HyperliquidNewUMOrder
            | TradeRequestType::HyperliquidCancelUMOrder => self.perp.get(&symbol).copied(),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PerpMeta {
    universe: Vec<PerpAsset>,
    #[serde(rename = "collateralToken")]
    collateral_token: Option<u32>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct PerpAsset {
    name: String,
    sz_decimals: u32,
    #[serde(default)]
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
struct SpotMeta {
    tokens: Vec<SpotToken>,
    universe: Vec<SpotMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotToken {
    name: String,
    index: u32,
    sz_decimals: u32,
}

#[derive(Debug, Deserialize)]
struct SpotMarket {
    index: u32,
    tokens: [u32; 2],
}

pub struct HyperliquidTradingClient {
    signing_key: SigningKey,
    vault_address: Option<String>,
    vault_address_bytes: Option<[u8; 20]>,
    mainnet: bool,
    catalog: HyperliquidAssetCatalog,
    account_address: String,
    account_hash: [u8; 32],
    account_mode: HyperliquidAccountMode,
    account_role: HyperliquidUserRole,
    account_mode_valid: AtomicBool,
    account_catalog: HyperliquidAccountAssetCatalog,
    ws_url: String,
    action_expires_after_ms: u64,
}

impl HyperliquidTradingClient {
    pub async fn from_env(local_ip: Option<IpAddr>) -> Result<Self> {
        let private_key = std::env::var("HYPERLIQUID_PRIVATE_KEY")
            .ok()
            .or_else(|| std::env::var("HYPERLIQUID_API_SECRET").ok())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!("Hyperliquid requires HYPERLIQUID_PRIVATE_KEY (or HYPERLIQUID_API_SECRET)")
            })?;
        let signing_key = parse_private_key(&private_key)?;
        let signer_address = signing_key_address(&signing_key);
        let endpoints = HyperliquidEndpoints::from_env()?;
        let mainnet = !endpoints.testnet;
        let ws_url = endpoints.ws_url;
        let info_url = endpoints.info_url;

        let (vault_address, vault_address_bytes) = match nonempty_env("HYPERLIQUID_VAULT_ADDRESS") {
            Some(address) => {
                let bytes = parse_address(&address).context("invalid HYPERLIQUID_VAULT_ADDRESS")?;
                (Some(format_address(bytes)), Some(bytes))
            }
            None => (None, None),
        };
        let account_address = nonempty_env("HYPERLIQUID_ACCOUNT_ADDRESS")
            .ok_or_else(|| anyhow!("Hyperliquid requires HYPERLIQUID_ACCOUNT_ADDRESS"))?;
        let account_address = format_address(
            parse_address(&account_address).context("invalid HYPERLIQUID_ACCOUNT_ADDRESS")?,
        );
        let account_hash = hyperliquid_account_identity_hash(&account_address, endpoints.testnet)?;
        if vault_address
            .as_deref()
            .is_some_and(|vault| vault != account_address)
        {
            bail!(
                "HYPERLIQUID_ACCOUNT_ADDRESS must equal HYPERLIQUID_VAULT_ADDRESS so order target, queries, and private streams observe the same account"
            );
        }
        let mut builder = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10));
        if let Some(ip) = local_ip {
            builder = builder.local_address(ip);
        }
        let http = builder
            .build()
            .context("build Hyperliquid metadata client")?;
        let (perp, spot, perp_dexs, all_perp_metas, abstraction, target_role, signer_role) = tokio::try_join!(
            fetch_info(&http, &info_url, json!({"type":"meta"})),
            fetch_info(&http, &info_url, json!({"type":"spotMeta"})),
            fetch_info(&http, &info_url, json!({"type":"perpDexs"})),
            fetch_info(&http, &info_url, json!({"type":"allPerpMetas"})),
            fetch_info(
                &http,
                &info_url,
                json!({"type":"userAbstraction", "user": account_address}),
            ),
            fetch_info(
                &http,
                &info_url,
                json!({"type":"userRole", "user": account_address}),
            ),
            fetch_info(
                &http,
                &info_url,
                json!({"type":"userRole", "user": signer_address}),
            ),
        )?;
        let account_role =
            parse_user_role(&target_role).context("decode order-target Hyperliquid userRole")?;
        let vault_details = if account_role == HyperliquidUserRole::Vault {
            Some(
                fetch_info(
                    &http,
                    &info_url,
                    json!({"type":"vaultDetails", "vaultAddress": account_address}),
                )
                .await
                .context("fetch Hyperliquid vault leader for order-target validation")?,
            )
        } else {
            None
        };
        validate_order_target_role(
            &target_role,
            &signer_role,
            &account_address,
            &signer_address,
            vault_address.as_deref(),
            vault_details.as_ref(),
        )?;
        let account_mode = resolve_user_abstraction(&abstraction, account_role)
            .context("resolve Hyperliquid userAbstraction")?;
        let account_catalog = HyperliquidAccountAssetCatalog::from_all_meta(
            &perp,
            &spot,
            &perp_dexs,
            &all_perp_metas,
        )?;
        let catalog =
            HyperliquidAssetCatalog::from_all_values(&perp, &spot, &perp_dexs, &all_perp_metas)?;
        if catalog.spot.is_empty() || catalog.perp.is_empty() {
            bail!("Hyperliquid metadata returned an empty spot or perp catalog");
        }
        let action_expires_after_ms = hyperliquid_action_expires_after_ms()?;
        Ok(Self {
            signing_key,
            vault_address,
            vault_address_bytes,
            mainnet,
            catalog,
            account_address,
            account_hash,
            account_mode,
            account_role,
            account_mode_valid: AtomicBool::new(true),
            account_catalog,
            ws_url,
            action_expires_after_ms,
        })
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    pub fn account_address(&self) -> &str {
        &self.account_address
    }

    pub fn account_hash(&self) -> [u8; 32] {
        self.account_hash
    }

    pub fn account_mode(&self) -> HyperliquidAccountMode {
        self.account_mode
    }

    pub fn build_info_payload(&self, msg: &QueryRequestMsg, transport_id: i64) -> Result<String> {
        if transport_id <= 0 {
            bail!("Hyperliquid websocket post id must be positive");
        }
        let params = HyperliquidQueryParams::from_bytes(&msg.params)
            .context("decode account-bound Hyperliquid query params")?;
        self.validate_account_hash(params.account_hash)?;
        let is_order_query = matches!(
            msg.req_type,
            QueryRequestType::HyperliquidMarginQuery | QueryRequestType::HyperliquidUMQuery
        );
        if !is_order_query && !params.body.is_empty() {
            bail!(
                "Hyperliquid account query {:?} must have an empty request body",
                msg.req_type
            );
        }
        let payload = match msg.req_type {
            QueryRequestType::HyperliquidMarginQuery | QueryRequestType::HyperliquidUMQuery => {
                let params: Value = serde_json::from_slice(&params.body)
                    .context("decode Hyperliquid orderStatus query params")?;
                let oid = params
                    .get("oid")
                    .context("Hyperliquid orderStatus query missing oid")?;
                validate_order_lookup_id(oid)?;
                json!({
                    "type": "orderStatus",
                    "user": self.account_address,
                    "oid": oid,
                })
            }
            QueryRequestType::HyperliquidClearinghouseSnapshot => {
                if self.account_mode == HyperliquidAccountMode::PortfolioMargin {
                    bail!(
                        "Hyperliquid clearinghouseState is a single default-DEX view and cannot reconstruct a portfolio-margin/all-DEX account snapshot"
                    );
                }
                json!({
                    "type": "clearinghouseState",
                    "user": self.account_address,
                })
            }
            QueryRequestType::HyperliquidSpotStateSnapshot => json!({
                "type": "spotClearinghouseState",
                "user": self.account_address,
            }),
            QueryRequestType::HyperliquidUserAbstraction => json!({
                "type": "userAbstraction",
                "user": self.account_address,
            }),
            other => bail!("unsupported Hyperliquid info request type {other:?}"),
        };
        serde_json::to_string(&json!({
            "method": "post",
            "id": transport_id,
            "request": {
                "type": "info",
                "payload": payload,
            }
        }))
        .context("encode Hyperliquid websocket info request")
    }

    pub fn process_snapshot(
        &self,
        req_type: QueryRequestType,
        payload: &Value,
        now_ms: i64,
    ) -> Result<Vec<Bytes>> {
        if req_type == QueryRequestType::HyperliquidClearinghouseSnapshot
            && self.account_mode == HyperliquidAccountMode::PortfolioMargin
        {
            bail!(
                "Hyperliquid clearinghouseState is a single default-DEX view and cannot reconstruct a portfolio-margin/all-DEX account snapshot"
            );
        }
        // Query snapshots must be independently reconstructible by a freshly
        // restarted consumer. A new processor emits every current row instead
        // of suppressing values that matched an earlier query.
        let mut processor = HyperliquidAccountProcessor::new(
            self.account_address.clone(),
            self.account_catalog.clone(),
            self.account_mode,
            FillSnapshotPolicy::Ignore,
        )?;
        let mut messages = match req_type {
            QueryRequestType::HyperliquidClearinghouseSnapshot => {
                match self.account_mode {
                    HyperliquidAccountMode::Standard => {
                        processor.apply_clearinghouse_snapshot(payload, now_ms)
                    }
                    HyperliquidAccountMode::Unified => {
                        // allDexsClearinghouseState is subscription-only. The
                        // Info query can safely refresh default-dex rows, but is
                        // not a complete cross-dex replacement snapshot.
                        let states = serde_json::Map::from_iter([(String::new(), payload.clone())]);
                        processor
                            .apply_all_dexs_clearinghouse_snapshot(&Value::Object(states), now_ms)
                    }
                    HyperliquidAccountMode::PortfolioMargin => bail!(
                        "Hyperliquid clearinghouseState cannot reconstruct a portfolio-margin/all-DEX account snapshot"
                    ),
                }
            }
            QueryRequestType::HyperliquidSpotStateSnapshot => {
                processor.apply_spot_snapshot(payload, now_ms)
            }
            other => bail!("not a Hyperliquid account snapshot request: {other:?}"),
        }?;
        // The account-stream completion event has no query correlation. Query
        // recovery publishes its own correlated BEGIN/COMPLETE transaction.
        messages.retain(|message| {
            !matches!(
                split_basic_account_event(message),
                Some((BasicAccountEventType::HyperliquidSnapshotComplete, _, _))
            )
        });
        Ok(messages)
    }

    pub fn query_snapshot_is_complete(&self, req_type: QueryRequestType) -> bool {
        req_type == QueryRequestType::HyperliquidSpotStateSnapshot
            && self.account_mode != HyperliquidAccountMode::PortfolioMargin
    }

    pub fn snapshot_scope(&self, req_type: QueryRequestType) -> Result<u32> {
        let scope = match req_type {
            QueryRequestType::HyperliquidClearinghouseSnapshot => self.account_mode.perp_scope(),
            QueryRequestType::HyperliquidSpotStateSnapshot => self.account_mode.spot_scope(),
            other => bail!("not a Hyperliquid account snapshot request: {other:?}"),
        };
        Ok(scope as u32)
    }

    pub fn validate_account_mode_response(
        &self,
        payload: &Value,
    ) -> Result<HyperliquidAccountMode> {
        let mode = match resolve_user_abstraction(payload, self.account_role) {
            Ok(mode) => mode,
            Err(err) => {
                self.account_mode_valid.store(false, Ordering::Release);
                return Err(err).context(
                    "Hyperliquid account mode validation failed; trading is latched off until process restart",
                );
            }
        };
        if mode != self.account_mode {
            self.account_mode_valid.store(false, Ordering::Release);
            bail!(
                "Hyperliquid account mode changed after startup: startup={} current={}; trading is latched off until process restart",
                self.account_mode.as_str(),
                mode.as_str()
            );
        }
        if !self.account_mode_valid.load(Ordering::Acquire) {
            bail!("Hyperliquid trading remains latched off after an account mode change; restart is required");
        }
        Ok(mode)
    }

    pub fn build_payload(&self, msg: &TradeRequestMsg, transport_id: i64) -> Result<String> {
        if !self.account_mode_valid.load(Ordering::Acquire) {
            bail!(
                "Hyperliquid trading is latched off after an account mode change; restart is required"
            );
        }
        if transport_id <= 0 {
            bail!("Hyperliquid websocket post id must be positive");
        }
        match msg.req_type {
            TradeRequestType::HyperliquidNewMarginOrder
            | TradeRequestType::HyperliquidNewUMOrder => {
                let params = HyperliquidNewOrderParamsRef::from_bytes(&msg.params)
                    .context("decode Hyperliquid new-order params")?;
                self.validate_account_hash(params.account_hash)?;
                let asset = self.resolve_asset(msg.req_type, params.symbol)?;
                if msg.req_type == TradeRequestType::HyperliquidNewUMOrder
                    && self.account_mode == HyperliquidAccountMode::Standard
                    && asset.asset_id >= HYPERLIQUID_FIRST_HIP3_ASSET_ID
                    && !params.reduce_only
                {
                    bail!(
                        "opening Hyperliquid HIP-3 orders in standard account mode is blocked because per-DEX collateral risk is not represented by the generic pre-trade risk gate; use unified account mode or reduceOnly"
                    );
                }
                let price = validated_price(params.price_qv, asset, params.side)?;
                let size = validated_size(params.quantity_qv, asset)?;
                let tif = match params.order_type {
                    OrderType::Limit => "Alo",
                    OrderType::Market => "Ioc",
                    other => bail!("unsupported Hyperliquid order type {other:?}"),
                };
                let action = OrderAction {
                    action_type: "order",
                    orders: vec![OrderWire {
                        asset: asset.asset_id,
                        is_buy: params.side == Side::Buy,
                        price,
                        size,
                        // Hyperliquid spot has no reduce-only order primitive;
                        // spot balance/margin mode determines whether a sell may borrow.
                        reduce_only: !asset.is_spot && params.reduce_only,
                        order_type: LimitOrderType {
                            limit: LimitTif { tif },
                        },
                        cloid: hyperliquid_cloid(msg.client_order_id)?,
                    }],
                    grouping: "na",
                };
                let expires_after = self.action_expiry_for_request(msg)?;
                self.wrap_action(transport_id, action, Some(expires_after))
            }
            TradeRequestType::HyperliquidCancelMarginOrder
            | TradeRequestType::HyperliquidCancelUMOrder => {
                let params = HyperliquidCancelOrderParams::from_bytes(&msg.params)
                    .context("decode Hyperliquid cancel params")?;
                self.validate_account_hash(params.account_hash)?;
                let asset = self.resolve_asset(msg.req_type, &params.symbol)?;
                let action = CancelByCloidAction {
                    action_type: "cancelByCloid",
                    cancels: vec![CancelByCloidWire {
                        asset: asset.asset_id,
                        cloid: hyperliquid_cloid(msg.client_order_id)?,
                    }],
                };
                self.wrap_action(transport_id, action, None)
            }
            other => bail!("unsupported Hyperliquid request type {other:?}"),
        }
    }

    fn resolve_asset(&self, req_type: TradeRequestType, symbol: &str) -> Result<HyperliquidAsset> {
        self.catalog.get(req_type, symbol).ok_or_else(|| {
            anyhow!(
                "Hyperliquid asset not found for request={req_type:?} symbol={}",
                canonical_symbol(symbol)
            )
        })
    }

    fn validate_account_hash(&self, account_hash: [u8; 32]) -> Result<()> {
        if account_hash != self.account_hash {
            bail!("Hyperliquid IPC account identity does not match this trade_engine target");
        }
        Ok(())
    }

    fn action_expiry_for_request(&self, msg: &TradeRequestMsg) -> Result<u64> {
        if msg.create_time <= 0 {
            bail!(
                "Hyperliquid new order has invalid create_time={}",
                msg.create_time
            );
        }
        let created_ms = u64::try_from(msg.create_time.div_euclid(1_000))
            .context("Hyperliquid new-order create_time overflow")?;
        created_ms
            .checked_add(self.action_expires_after_ms)
            .context("Hyperliquid action expiresAfter overflow")
    }

    fn wrap_action<A: Serialize>(
        &self,
        transport_id: i64,
        action: A,
        expires_after: Option<u64>,
    ) -> Result<String> {
        let nonce = self.next_nonce()?;
        let signature = sign_l1_action(
            &self.signing_key,
            &action,
            self.vault_address_bytes,
            nonce,
            expires_after,
            self.mainnet,
        )?;
        let payload = ActionPayload {
            action,
            nonce,
            signature,
            vault_address: self.vault_address.as_deref(),
            expires_after,
        };
        serde_json::to_string(&WsPostRequest {
            method: "post",
            id: transport_id,
            request: WsActionRequest {
                request_type: "action",
                payload,
            },
        })
        .context("encode Hyperliquid websocket action")
    }

    fn next_nonce(&self) -> Result<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_millis();
        let now = u64::try_from(now).context("Hyperliquid nonce timestamp overflow")?;
        let mut previous = HYPERLIQUID_LAST_NONCE.load(Ordering::Relaxed);
        loop {
            let next = now.max(previous.saturating_add(1));
            match HYPERLIQUID_LAST_NONCE.compare_exchange_weak(
                previous,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(next),
                Err(actual) => previous = actual,
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct OrderAction<'a> {
    #[serde(rename = "type")]
    action_type: &'static str,
    orders: Vec<OrderWire<'a>>,
    grouping: &'static str,
}

#[derive(Debug, Serialize)]
struct OrderWire<'a> {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "s")]
    size: String,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: LimitOrderType<'a>,
    #[serde(rename = "c")]
    cloid: String,
}

#[derive(Debug, Serialize)]
struct LimitOrderType<'a> {
    limit: LimitTif<'a>,
}

#[derive(Debug, Serialize)]
struct LimitTif<'a> {
    tif: &'a str,
}

#[derive(Debug, Serialize)]
struct CancelByCloidAction {
    #[serde(rename = "type")]
    action_type: &'static str,
    cancels: Vec<CancelByCloidWire>,
}

#[derive(Debug, Serialize)]
struct CancelByCloidWire {
    asset: u32,
    cloid: String,
}

#[derive(Debug, Serialize)]
struct ActionSignature {
    r: String,
    s: String,
    v: u8,
}

#[derive(Debug, Serialize)]
struct ActionPayload<'a, A> {
    action: A,
    nonce: u64,
    signature: ActionSignature,
    #[serde(rename = "vaultAddress", skip_serializing_if = "Option::is_none")]
    vault_address: Option<&'a str>,
    #[serde(rename = "expiresAfter", skip_serializing_if = "Option::is_none")]
    expires_after: Option<u64>,
}

#[derive(Debug, Serialize)]
struct WsActionRequest<'a, A> {
    #[serde(rename = "type")]
    request_type: &'static str,
    payload: ActionPayload<'a, A>,
}

#[derive(Debug, Serialize)]
struct WsPostRequest<'a, A> {
    method: &'static str,
    id: i64,
    request: WsActionRequest<'a, A>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HyperliquidActionOutcome {
    pub transport_id: i64,
    pub status: u16,
    pub code: i32,
    pub message: String,
    pub order_id: i64,
    pub order_status_u8: u8,
    pub executed_qty: f64,
    pub response_price: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HyperliquidInfoOutcome {
    pub transport_id: i64,
    pub status: u16,
    pub payload: Option<Value>,
    pub message: String,
}

pub fn parse_info_response(payload: &str) -> Option<HyperliquidInfoOutcome> {
    let root: Value = serde_json::from_str(payload).ok()?;
    if root.get("channel").and_then(Value::as_str) != Some("post") {
        return None;
    }
    let data = root.get("data")?;
    let transport_id = parse_i64(data.get("id"))?;
    let response = data.get("response")?;
    match response.get("type").and_then(Value::as_str)? {
        "info" => {
            let payload = response.get("payload")?;
            let payload = payload.get("data").unwrap_or(payload).clone();
            Some(HyperliquidInfoOutcome {
                transport_id,
                status: 200,
                payload: Some(payload),
                message: String::new(),
            })
        }
        "error" => Some(HyperliquidInfoOutcome {
            transport_id,
            status: 400,
            payload: None,
            message: value_text(response.get("payload")),
        }),
        other => Some(HyperliquidInfoOutcome {
            transport_id,
            status: 400,
            payload: None,
            message: format!("unexpected Hyperliquid post response type {other}"),
        }),
    }
}

pub fn parse_action_response(
    payload: &str,
    req_type: TradeRequestType,
) -> Option<HyperliquidActionOutcome> {
    let root: Value = serde_json::from_str(payload).ok()?;
    if root.get("channel").and_then(Value::as_str) != Some("post") {
        return None;
    }
    let data = root.get("data")?;
    let transport_id = parse_i64(data.get("id"))?;
    let response = data.get("response")?;
    let response_type = response.get("type").and_then(Value::as_str)?;
    if response_type == "error" {
        return Some(error_outcome(
            transport_id,
            value_text(response.get("payload")),
        ));
    }
    if response_type != "action" {
        return Some(error_outcome(
            transport_id,
            format!("unexpected Hyperliquid post response type {response_type}"),
        ));
    }
    parse_exchange_action(transport_id, response.get("payload")?, req_type)
}

pub fn post_response_id(payload: &str) -> Option<i64> {
    let root: Value = serde_json::from_str(payload).ok()?;
    (root.get("channel").and_then(Value::as_str) == Some("post"))
        .then(|| parse_i64(root.pointer("/data/id")))
        .flatten()
}

pub fn is_pong(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|root| {
            root.get("channel")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some("pong")
}

fn parse_exchange_action(
    transport_id: i64,
    payload: &Value,
    req_type: TradeRequestType,
) -> Option<HyperliquidActionOutcome> {
    if payload.get("status").and_then(Value::as_str) != Some("ok") {
        return Some(error_outcome(
            transport_id,
            value_text(payload.get("response")),
        ));
    }
    let response = payload.get("response")?;
    let response_type = response.get("type").and_then(Value::as_str)?;
    let first_status = response
        .pointer("/data/statuses")
        .and_then(Value::as_array)
        .and_then(|statuses| statuses.first());

    if matches!(
        req_type,
        TradeRequestType::HyperliquidCancelMarginOrder | TradeRequestType::HyperliquidCancelUMOrder
    ) {
        if response_type == "cancel" && first_status.and_then(Value::as_str) == Some("success") {
            return Some(success_outcome(
                transport_id,
                0,
                OrderStatus::Canceled,
                0.0,
                0.0,
            ));
        }
        return Some(error_outcome(
            transport_id,
            first_status
                .map(|status| value_text_direct(status.get("error").unwrap_or(status)))
                .unwrap_or_else(|| format!("unexpected Hyperliquid cancel response {response}")),
        ));
    }

    if response_type != "order" {
        return Some(error_outcome(
            transport_id,
            format!("unexpected Hyperliquid order response {response}"),
        ));
    }
    let status = first_status?;
    if let Some(resting) = status.get("resting") {
        return Some(success_outcome(
            transport_id,
            parse_order_id(resting.get("oid")),
            OrderStatus::New,
            0.0,
            0.0,
        ));
    }
    if let Some(filled) = status.get("filled") {
        return Some(success_outcome(
            transport_id,
            parse_order_id(filled.get("oid")),
            OrderStatus::Filled,
            parse_f64(filled.get("totalSz")).unwrap_or(0.0),
            parse_f64(filled.get("avgPx")).unwrap_or(0.0),
        ));
    }
    Some(error_outcome(
        transport_id,
        value_text_direct(status.get("error").unwrap_or(status)),
    ))
}

fn success_outcome(
    transport_id: i64,
    order_id: i64,
    order_status: OrderStatus,
    executed_qty: f64,
    response_price: f64,
) -> HyperliquidActionOutcome {
    HyperliquidActionOutcome {
        transport_id,
        status: 206,
        code: 0,
        message: String::new(),
        order_id,
        order_status_u8: order_status.to_u8(),
        executed_qty,
        response_price,
    }
}

fn error_outcome(transport_id: i64, message: String) -> HyperliquidActionOutcome {
    HyperliquidActionOutcome {
        transport_id,
        status: 400,
        code: classify_error(&message),
        message,
        order_id: 0,
        order_status_u8: 0,
        executed_qty: 0.0,
        response_price: 0.0,
    }
}

fn classify_error(message: &str) -> i32 {
    use order_common::trade_error_code::hyperliquid::*;
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("post only")
        || normalized.contains("would have immediately matched")
        || normalized.contains("would immediately match")
    {
        POST_ONLY_REJECTED
    } else if normalized.contains("insufficient spot balance") {
        INSUFFICIENT_SPOT_BALANCE
    } else if normalized.contains("insufficient margin")
        || normalized.contains("insufficient balance")
    {
        INSUFFICIENT_MARGIN
    } else if normalized.contains("price more aggressive than oracle")
        || normalized.contains("price too far from oracle")
        || normalized.contains("order too far from reference price")
    {
        PRICE_LIMIT_REJECTED
    } else if normalized.contains("would increase open interest")
        || normalized.contains("exceed margin tier limit at current leverage")
    {
        POSITION_LIMIT_EXCEEDED
    } else if normalized.contains("price must be divisible by tick size") {
        INVALID_TICK
    } else if normalized.contains("order must have minimum value of") {
        MIN_NOTIONAL
    } else if normalized.contains("reduce only order would increase position") {
        REDUCE_ONLY_REJECTED
    } else if normalized.contains("invalid tp/sl price") {
        INVALID_TRIGGER_PRICE
    } else if normalized.contains("could not immediately match against any resting orders")
        || normalized.contains("no liquidity available for market order")
    {
        NO_LIQUIDITY
    } else if normalized.contains("order was never placed")
        || normalized.contains("unknown oid")
        || normalized.contains("not found")
        || normalized.contains("already canceled")
        || normalized.contains("already cancelled")
    {
        ORDER_NOT_FOUND
    } else {
        ACTION_REJECTED
    }
}

fn sign_l1_action<A: Serialize>(
    signing_key: &SigningKey,
    action: &A,
    vault_address: Option<[u8; 20]>,
    nonce: u64,
    expires_after: Option<u64>,
    mainnet: bool,
) -> Result<ActionSignature> {
    let mut action_bytes = rmp_serde::to_vec_named(action).context("msgpack Hyperliquid action")?;
    action_bytes.extend_from_slice(&nonce.to_be_bytes());
    match vault_address {
        Some(address) => {
            action_bytes.push(1);
            action_bytes.extend_from_slice(&address);
        }
        None => action_bytes.push(0),
    }
    if let Some(expires_after) = expires_after {
        action_bytes.push(0);
        action_bytes.extend_from_slice(&expires_after.to_be_bytes());
    }
    let connection_id: [u8; 32] = Keccak256::digest(&action_bytes).into();
    let digest = agent_eip712_digest(if mainnet { "a" } else { "b" }, connection_id);
    let (signature, recovery_id) = signing_key
        .sign_prehash_recoverable(&digest)
        .map_err(|error| anyhow!("sign Hyperliquid action: {error}"))?;
    Ok(ActionSignature {
        r: ethereum_quantity_hex(&signature.r().to_bytes()),
        s: ethereum_quantity_hex(&signature.s().to_bytes()),
        v: recovery_id.to_byte().saturating_add(27),
    })
}

fn ethereum_quantity_hex(bytes: &[u8]) -> String {
    let encoded = hex::encode(bytes);
    let digits = encoded.trim_start_matches('0');
    format!("0x{}", if digits.is_empty() { "0" } else { digits })
}

fn agent_eip712_digest(source: &str, connection_id: [u8; 32]) -> [u8; 32] {
    let domain_type_hash = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let agent_type_hash = keccak(b"Agent(string source,bytes32 connectionId)");

    let mut domain = Vec::with_capacity(32 * 5);
    domain.extend_from_slice(&domain_type_hash);
    domain.extend_from_slice(&keccak(b"Exchange"));
    domain.extend_from_slice(&keccak(b"1"));
    let mut chain_id = [0u8; 32];
    chain_id[30..].copy_from_slice(&1337u16.to_be_bytes());
    domain.extend_from_slice(&chain_id);
    domain.extend_from_slice(&[0u8; 32]);
    let domain_separator = keccak(&domain);

    let mut agent = Vec::with_capacity(32 * 3);
    agent.extend_from_slice(&agent_type_hash);
    agent.extend_from_slice(&keccak(source.as_bytes()));
    agent.extend_from_slice(&connection_id);
    let agent_hash = keccak(&agent);

    let mut input = Vec::with_capacity(66);
    input.extend_from_slice(b"\x19\x01");
    input.extend_from_slice(&domain_separator);
    input.extend_from_slice(&agent_hash);
    keccak(&input)
}

fn keccak(input: &[u8]) -> [u8; 32] {
    Keccak256::digest(input).into()
}

fn validated_size(value: QuantizedValue, asset: HyperliquidAsset) -> Result<String> {
    let value = canonical_decimal(value)?;
    let decimal_places = decimal_places(&value);
    if decimal_places > asset.sz_decimals as usize {
        bail!(
            "Hyperliquid size {} has {} decimals; asset {} allows {}",
            value,
            decimal_places,
            asset.asset_id,
            asset.sz_decimals
        );
    }
    Ok(value)
}

fn validated_price(value: QuantizedValue, asset: HyperliquidAsset, side: Side) -> Result<String> {
    let value = canonical_decimal(value)
        .context("Hyperliquid market orders require a positive client-side IOC protection price")?;
    let max_decimals = if asset.is_spot { 8u32 } else { 6u32 }
        .checked_sub(asset.sz_decimals)
        .context("Hyperliquid szDecimals exceeds price precision base")?;
    normalize_hyperliquid_price(&value, max_decimals as usize, side).with_context(|| {
        format!(
            "normalize Hyperliquid price for asset {} (szDecimals={})",
            asset.asset_id, asset.sz_decimals
        )
    })
}

fn normalize_hyperliquid_price(value: &str, max_decimals: usize, side: Side) -> Result<String> {
    let Some((integer, fraction)) = value.split_once('.') else {
        return Ok(value.to_string());
    };

    let mut seen_nonzero = false;
    let mut significant = 0usize;
    for byte in integer.bytes() {
        if byte != b'0' {
            seen_nonzero = true;
        }
        if seen_nonzero {
            significant += 1;
        }
    }

    let mut keep = 0usize;
    for byte in fraction.bytes() {
        if keep >= max_decimals {
            break;
        }
        let contributes = seen_nonzero || byte != b'0';
        if contributes && significant >= 5 {
            break;
        }
        keep += 1;
        if contributes {
            seen_nonzero = true;
            significant += 1;
        }
    }

    let discarded_nonzero = fraction.as_bytes()[keep..].iter().any(|byte| *byte != b'0');
    let mut rounded = if keep == 0 {
        integer.to_string()
    } else {
        format!("{integer}.{}", &fraction[..keep])
    };
    if side == Side::Sell && discarded_nonzero {
        increment_last_decimal_unit(&mut rounded);
    }
    trim_decimal_zeroes(&mut rounded);
    if rounded.bytes().all(|byte| byte == b'0') {
        bail!("price rounds to zero at Hyperliquid precision");
    }
    Ok(rounded)
}

fn increment_last_decimal_unit(value: &mut String) {
    let mut bytes = value.as_bytes().to_vec();
    for index in (0..bytes.len()).rev() {
        match bytes[index] {
            b'.' => continue,
            b'0'..=b'8' => {
                bytes[index] += 1;
                *value = String::from_utf8(bytes).expect("decimal text is ASCII");
                return;
            }
            b'9' => bytes[index] = b'0',
            _ => unreachable!("decimal text contains only digits and a dot"),
        }
    }
    bytes.insert(0, b'1');
    *value = String::from_utf8(bytes).expect("decimal text is ASCII");
}

fn trim_decimal_zeroes(value: &mut String) {
    if !value.contains('.') {
        return;
    }
    while value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
}

fn canonical_decimal(value: QuantizedValue) -> Result<String> {
    if value.is_zero() || !value.get_val().is_finite() || value.get_val() <= 0.0 {
        bail!("value must be positive");
    }
    let mut text = value.decimal_string();
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    Ok(text)
}

fn decimal_places(value: &str) -> usize {
    value
        .split_once('.')
        .map(|(_, fraction)| fraction.len())
        .unwrap_or(0)
}

fn hyperliquid_cloid(client_order_id: i64) -> Result<String> {
    hyperliquid_cloid_from_client_order_id(client_order_id)
        .context("Hyperliquid client_order_id must be a positive i64")
}

fn validate_order_lookup_id(value: &Value) -> Result<()> {
    if value.as_u64().is_some_and(|value| value > 0) {
        return Ok(());
    }
    let Some(value) = value.as_str() else {
        bail!("Hyperliquid orderStatus oid must be a positive u64 or cloid");
    };
    let raw = value.strip_prefix("0x").unwrap_or_default();
    if raw.len() != 32 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("Hyperliquid orderStatus cloid must be a 16-byte hex string");
    }
    Ok(())
}

fn parse_private_key(raw: &str) -> Result<SigningKey> {
    let raw = raw.strip_prefix("0x").unwrap_or(raw);
    let bytes = hex::decode(raw).context("HYPERLIQUID_PRIVATE_KEY must be hex")?;
    if bytes.len() != 32 {
        bail!("HYPERLIQUID_PRIVATE_KEY must contain exactly 32 bytes");
    }
    SigningKey::from_slice(&bytes)
        .map_err(|error| anyhow!("invalid Hyperliquid private key: {error}"))
}

fn parse_address(raw: &str) -> Result<[u8; 20]> {
    let raw = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    let bytes = hex::decode(raw).context("address must be hex")?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("address must contain exactly 20 bytes"))
}

fn format_address(address: [u8; 20]) -> String {
    format!("0x{}", hex::encode(address))
}

fn signing_key_address(signing_key: &SigningKey) -> String {
    let encoded = signing_key.verifying_key().to_encoded_point(false);
    let digest = Keccak256::digest(&encoded.as_bytes()[1..]);
    format!("0x{}", hex::encode(&digest[12..]))
}

fn validate_order_target_role(
    target_role_payload: &Value,
    signer_role_payload: &Value,
    account_address: &str,
    signer_address: &str,
    vault_address: Option<&str>,
    vault_details: Option<&Value>,
) -> Result<()> {
    let role = parse_user_role(target_role_payload)?;
    let controlling_user = match role {
        HyperliquidUserRole::User => {
            if vault_address.is_some() {
                bail!(
                    "HYPERLIQUID_VAULT_ADDRESS must be unset when HYPERLIQUID_ACCOUNT_ADDRESS is a master user account"
                );
            }
            account_address.to_string()
        }
        HyperliquidUserRole::SubAccount => {
            if vault_address != Some(account_address) {
                bail!(
                    "Hyperliquid subAccount order target requires HYPERLIQUID_VAULT_ADDRESS to equal HYPERLIQUID_ACCOUNT_ADDRESS"
                );
            }
            normalized_role_data_address(target_role_payload, "master", "subAccount master")?
        }
        HyperliquidUserRole::Vault => {
            if vault_address != Some(account_address) {
                bail!(
                    "Hyperliquid vault order target requires HYPERLIQUID_VAULT_ADDRESS to equal HYPERLIQUID_ACCOUNT_ADDRESS"
                );
            }
            let details = vault_details.context("Hyperliquid vaultDetails response is required")?;
            let returned_vault = details
                .get("vaultAddress")
                .and_then(Value::as_str)
                .context("Hyperliquid vaultDetails missing vaultAddress")?;
            let returned_vault = format_address(
                parse_address(returned_vault).context("invalid vaultDetails.vaultAddress")?,
            );
            if returned_vault != account_address {
                bail!(
                    "Hyperliquid vaultDetails target mismatch: expected={account_address} received={returned_vault}"
                );
            }
            let leader = details
                .get("leader")
                .and_then(Value::as_str)
                .context("Hyperliquid vaultDetails missing leader")?;
            format_address(parse_address(leader).context("invalid vaultDetails.leader")?)
        }
        HyperliquidUserRole::Agent => bail!(
            "HYPERLIQUID_ACCOUNT_ADDRESS identifies an API agent wallet; set it to the master user, subaccount, or vault whose state should be traded"
        ),
        HyperliquidUserRole::Missing => bail!(
            "HYPERLIQUID_ACCOUNT_ADDRESS has no Hyperliquid user role and cannot be used as an order target"
        ),
    };

    if signer_address == controlling_user {
        return Ok(());
    }
    let signer_role = parse_user_role(signer_role_payload)
        .context("decode Hyperliquid signer userRole response")?;
    if signer_role != HyperliquidUserRole::Agent {
        bail!(
            "Hyperliquid signer {signer_address} is not the controlling user {controlling_user} or one of its API agents (signer role={})",
            signer_role.as_str()
        );
    }
    let agent_user = normalized_role_data_address(signer_role_payload, "user", "agent user")?;
    if agent_user != controlling_user {
        bail!(
            "Hyperliquid API agent owner mismatch: target controller={controlling_user} agent_user={agent_user}"
        );
    }
    Ok(())
}

fn normalized_role_data_address(payload: &Value, field: &str, label: &str) -> Result<String> {
    let raw = payload
        .get("data")
        .and_then(|data| data.get(field))
        .and_then(Value::as_str)
        .with_context(|| format!("Hyperliquid userRole missing data.{field} for {label}"))?;
    Ok(format_address(parse_address(raw).with_context(|| {
        format!("invalid Hyperliquid {label} address")
    })?))
}

fn canonical_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn fetch_info(client: &reqwest::Client, url: &str, request: Value) -> Result<Value> {
    let response = client
        .post(url)
        .json(&request)
        .send()
        .await
        .with_context(|| format!("request Hyperliquid metadata type={}", request["type"]))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read Hyperliquid metadata response")?;
    if !status.is_success() {
        bail!("Hyperliquid metadata HTTP status={} body={body}", status);
    }
    serde_json::from_str(&body).context("decode Hyperliquid metadata JSON")
}

fn parse_i64(value: Option<&Value>) -> Option<i64> {
    value?
        .as_i64()
        .or_else(|| value?.as_str().and_then(|text| text.parse::<i64>().ok()))
}

fn parse_order_id(value: Option<&Value>) -> i64 {
    value
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|id| i64::try_from(id).ok()))
                .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        })
        .unwrap_or(0)
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    value?
        .as_f64()
        .or_else(|| value?.as_str().and_then(|text| text.parse::<f64>().ok()))
}

fn value_text(value: Option<&Value>) -> String {
    value.map(value_text_direct).unwrap_or_default()
}

fn value_text_direct(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_request::GenericQueryRequest;
    use crate::trade_request::HyperliquidNewOrderParams;
    use order_common::trade_error_code::hyperliquid::POST_ONLY_REJECTED;

    fn signing_key() -> SigningKey {
        parse_private_key("0x0123456789012345678901234567890123456789012345678901234567890123")
            .unwrap()
    }

    fn test_client() -> HyperliquidTradingClient {
        test_client_with_mode(HyperliquidAccountMode::Standard)
    }

    fn test_client_with_mode(account_mode: HyperliquidAccountMode) -> HyperliquidTradingClient {
        let perp = json!({"universe":[{"name":"BTC","szDecimals":5}]});
        let spot = json!({
            "tokens":[
                {"name":"USDC","index":0,"szDecimals":8},
                {"name":"HYPE","index":150,"szDecimals":3}
            ],
            "universe":[{"name":"@107","index":107,"tokens":[150,0]}]
        });
        let account_address = "0x1111111111111111111111111111111111111111".to_string();
        let account_hash = hyperliquid_account_identity_hash(&account_address, false).unwrap();
        let account_catalog = HyperliquidAccountAssetCatalog::from_meta(&perp, &spot).unwrap();
        HyperliquidTradingClient {
            signing_key: signing_key(),
            vault_address: None,
            vault_address_bytes: None,
            mainnet: true,
            catalog: HyperliquidAssetCatalog::from_values(perp, spot).unwrap(),
            account_address,
            account_hash,
            account_mode,
            account_role: HyperliquidUserRole::User,
            account_mode_valid: AtomicBool::new(true),
            account_catalog,
            ws_url: MAINNET_WS_URL.to_string(),
            action_expires_after_ms: DEFAULT_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS,
        }
    }

    #[derive(Serialize)]
    struct DummyAction {
        #[serde(rename = "type")]
        action_type: &'static str,
        num: u64,
    }

    #[test]
    fn matches_official_dummy_action_signature_vector() {
        let signature = sign_l1_action(
            &signing_key(),
            &DummyAction {
                action_type: "dummy",
                num: 100_000_000_000,
            },
            None,
            0,
            None,
            true,
        )
        .unwrap();
        assert_eq!(
            signature.r,
            "0x53749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298"
        );
        assert_eq!(
            signature.s,
            "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8"
        );
        assert_eq!(signature.v, 27);
    }

    #[test]
    fn matches_official_order_with_cloid_signature_vector() {
        let action = OrderAction {
            action_type: "order",
            orders: vec![OrderWire {
                asset: 1,
                is_buy: true,
                price: "100".to_string(),
                size: "100".to_string(),
                reduce_only: false,
                order_type: LimitOrderType {
                    limit: LimitTif { tif: "Gtc" },
                },
                cloid: "0x00000000000000000000000000000001".to_string(),
            }],
            grouping: "na",
        };
        let signature = sign_l1_action(&signing_key(), &action, None, 0, None, true).unwrap();
        assert_eq!(
            signature.r,
            "0x41ae18e8239a56cacbc5dad94d45d0b747e5da11ad564077fcac71277a946e3"
        );
        assert_eq!(
            signature.s,
            "0x3c61f667e747404fe7eea8f90ab0e76cc12ce60270438b2058324681a00116da"
        );
        assert_eq!(signature.v, 27);
    }

    #[test]
    fn new_order_payload_signs_and_carries_create_time_expiry() {
        let client = test_client();
        let create_time_us = 1_700_000_000_123_456_i64;
        let request = HyperliquidNewOrderParams::request_bytes_from_parts(
            TradeRequestType::HyperliquidNewUMOrder,
            create_time_us,
            42,
            client.account_hash,
            "BTCUSDC",
            Side::Buy,
            OrderType::Market,
            QuantizedValue::from_parts(1, -3, 100),
            QuantizedValue::from_parts(1, 0, 60_000),
            false,
        )
        .unwrap();
        let request = TradeRequestMsg::parse(&request).unwrap();
        let payload: Value =
            serde_json::from_str(&client.build_payload(&request, 7).unwrap()).unwrap();
        assert_eq!(
            payload["request"]["payload"]["expiresAfter"],
            create_time_us.div_euclid(1_000) as u64 + DEFAULT_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
        );
        assert_eq!(
            payload["request"]["payload"]["action"]["orders"][0]["t"]["limit"]["tif"],
            "Ioc"
        );
    }

    #[test]
    fn portfolio_margin_startup_and_signed_trade_protocol_are_enabled() {
        assert_eq!(
            resolve_user_abstraction(&json!("portfolioMargin"), HyperliquidUserRole::User).unwrap(),
            HyperliquidAccountMode::PortfolioMargin
        );

        let client = test_client_with_mode(HyperliquidAccountMode::PortfolioMargin);
        let order = HyperliquidNewOrderParams::request_bytes_from_parts(
            TradeRequestType::HyperliquidNewUMOrder,
            1_700_000_000_123_456,
            42,
            client.account_hash,
            "BTCUSDC",
            Side::Buy,
            OrderType::Limit,
            QuantizedValue::from_parts(1, -3, 100),
            QuantizedValue::from_parts(1, 0, 60_000),
            false,
        )
        .unwrap();
        let order = TradeRequestMsg::parse(&order).unwrap();
        let order_payload: Value =
            serde_json::from_str(&client.build_payload(&order, 7).unwrap()).unwrap();
        assert_eq!(
            order_payload["request"]["payload"]["action"]["type"],
            "order"
        );
        assert!(order_payload["request"]["payload"]["signature"]["r"]
            .as_str()
            .is_some_and(|value| value.starts_with("0x")));
        assert!(order_payload["request"]["payload"]["signature"]["s"]
            .as_str()
            .is_some_and(|value| value.starts_with("0x")));

        let spot_order = HyperliquidNewOrderParams::request_bytes_from_parts(
            TradeRequestType::HyperliquidNewMarginOrder,
            1_700_000_000_123_456,
            43,
            client.account_hash,
            "HYPEUSDC",
            Side::Sell,
            OrderType::Limit,
            QuantizedValue::from_parts(1, -3, 100),
            QuantizedValue::from_parts(1, 0, 60_000),
            false,
        )
        .unwrap();
        let spot_order = TradeRequestMsg::parse(&spot_order).unwrap();
        let spot_order_payload: Value =
            serde_json::from_str(&client.build_payload(&spot_order, 8).unwrap()).unwrap();
        assert_eq!(
            spot_order_payload["request"]["payload"]["action"]["type"],
            "order"
        );

        let cancel = HyperliquidCancelOrderParams::request_bytes_from_parts(
            TradeRequestType::HyperliquidCancelUMOrder,
            1_700_000_000_123_457,
            42,
            client.account_hash,
            "BTCUSDC",
        )
        .unwrap();
        let cancel = TradeRequestMsg::parse(&cancel).unwrap();
        let cancel_payload: Value =
            serde_json::from_str(&client.build_payload(&cancel, 9).unwrap()).unwrap();
        assert_eq!(
            cancel_payload["request"]["payload"]["action"]["type"],
            "cancelByCloid"
        );

        let spot_cancel = HyperliquidCancelOrderParams::request_bytes_from_parts(
            TradeRequestType::HyperliquidCancelMarginOrder,
            1_700_000_000_123_457,
            43,
            client.account_hash,
            "HYPEUSDC",
        )
        .unwrap();
        let spot_cancel = TradeRequestMsg::parse(&spot_cancel).unwrap();
        let spot_cancel_payload: Value =
            serde_json::from_str(&client.build_payload(&spot_cancel, 10).unwrap()).unwrap();
        assert_eq!(
            spot_cancel_payload["request"]["payload"]["action"]["type"],
            "cancelByCloid"
        );

        for (index, req_type) in [
            QueryRequestType::HyperliquidUMQuery,
            QueryRequestType::HyperliquidMarginQuery,
        ]
        .into_iter()
        .enumerate()
        {
            let query = GenericQueryRequest::create(
                req_type,
                1,
                2,
                HyperliquidQueryParams::create(
                    client.account_hash,
                    Bytes::from_static(b"{\"oid\":123}"),
                )
                .to_bytes(),
            );
            let query = QueryRequestMsg::parse(&query.to_bytes()).unwrap();
            let query_payload: Value = serde_json::from_str(
                &client
                    .build_info_payload(&query, 11 + index as i64)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(query_payload["request"]["payload"]["type"], "orderStatus");
            assert_eq!(query_payload["request"]["payload"]["oid"], 123);
        }

        assert_eq!(
            client
                .validate_account_mode_response(&json!("portfolioMargin"))
                .unwrap(),
            HyperliquidAccountMode::PortfolioMargin
        );
        assert!(client
            .validate_account_mode_response(&json!("unifiedAccount"))
            .is_err());
        assert!(client
            .build_payload(&order, 13)
            .unwrap_err()
            .to_string()
            .contains("latched off"));
    }

    #[test]
    fn rejects_trade_and_query_ipc_for_a_different_account_or_network() {
        let client = test_client();
        let wrong_hash =
            hyperliquid_account_identity_hash("0x1111111111111111111111111111111111111111", true)
                .unwrap();
        let order = HyperliquidNewOrderParams::request_bytes_from_parts(
            TradeRequestType::HyperliquidNewUMOrder,
            1_700_000_000_123_456,
            42,
            wrong_hash,
            "BTCUSDC",
            Side::Buy,
            OrderType::Limit,
            QuantizedValue::from_parts(1, -3, 100),
            QuantizedValue::from_parts(1, 0, 60_000),
            false,
        )
        .unwrap();
        let order = TradeRequestMsg::parse(&order).unwrap();
        assert!(client
            .build_payload(&order, 7)
            .unwrap_err()
            .to_string()
            .contains("account identity"));

        let query = GenericQueryRequest::create(
            QueryRequestType::HyperliquidUMQuery,
            1,
            2,
            HyperliquidQueryParams::create(wrong_hash, Bytes::from_static(b"{\"oid\":123}"))
                .to_bytes(),
        );
        let query = QueryRequestMsg::parse(&query.to_bytes()).unwrap();
        assert!(client
            .build_info_payload(&query, 8)
            .unwrap_err()
            .to_string()
            .contains("account identity"));
    }

    #[test]
    fn catalog_uses_perp_position_spot_pair_index_and_shared_aliases() {
        let catalog = HyperliquidAssetCatalog::from_values(
            json!({"universe":[
                {"name":"BTC","szDecimals":5},
                {"name":"HYPE","szDecimals":2}
            ]}),
            json!({
                "tokens":[
                    {"name":"USDC","index":0,"szDecimals":8},
                    {"name":"HYPE","index":150,"szDecimals":3},
                    {"name":"UBTC","index":151,"szDecimals":5}
                ],
                "universe":[
                    {"name":"@107","index":107,"tokens":[150,0]},
                    {"name":"@108","index":108,"tokens":[151,0]}
                ]
            }),
        )
        .unwrap();
        assert_eq!(
            catalog
                .get(TradeRequestType::HyperliquidNewUMOrder, "HYPEUSDC")
                .unwrap()
                .asset_id,
            1
        );
        assert_eq!(
            catalog
                .get(TradeRequestType::HyperliquidNewMarginOrder, "HYPEUSDC")
                .unwrap(),
            HyperliquidAsset {
                asset_id: 10_107,
                sz_decimals: 3,
                is_spot: true,
            }
        );
        assert_eq!(
            catalog
                .get(TradeRequestType::HyperliquidNewMarginOrder, "BTCUSDC")
                .unwrap(),
            HyperliquidAsset {
                asset_id: 10_108,
                sz_decimals: 5,
                is_spot: true,
            }
        );
    }

    #[test]
    fn catalog_uses_official_hip3_asset_ids_and_collateral_symbols() {
        let default_perp = json!({"universe":[
            {"name":"BTC","szDecimals":5},
            {"name":"HYPE","szDecimals":2}
        ]});
        let spot = json!({
            "tokens":[
                {"name":"USDC","index":0,"szDecimals":8},
                {"name":"USDH","index":7,"szDecimals":6}
            ],
            "universe":[]
        });
        let catalog = HyperliquidAssetCatalog::from_all_values(
            &default_perp,
            &spot,
            &json!([null, {"name":"xyz"}]),
            &json!([
                {"collateralToken":0,"universe":[
                    {"name":"BTC","szDecimals":5},
                    {"name":"HYPE","szDecimals":2}
                ]},
                {"collateralToken":7,"universe":[
                    {"name":"xyz:FOO","szDecimals":3},
                    {"name":"xyz:OLD","szDecimals":4,"isDelisted":true}
                ]}
            ]),
        )
        .unwrap();
        assert_eq!(
            catalog
                .get(TradeRequestType::HyperliquidNewUMOrder, "BTCUSDC")
                .unwrap()
                .asset_id,
            0
        );
        assert_eq!(
            catalog
                .get(TradeRequestType::HyperliquidNewUMOrder, "XYZFOOUSDH")
                .unwrap(),
            HyperliquidAsset {
                asset_id: 110_000,
                sz_decimals: 3,
                is_spot: false,
            }
        );
        assert!(catalog
            .get(TradeRequestType::HyperliquidNewUMOrder, "XYZOLDUSDH")
            .is_none());
    }

    #[test]
    fn catalog_rejects_hip3_internal_symbol_collisions() {
        let error = HyperliquidAssetCatalog::from_all_values(
            &json!({"universe":[{"name":"BTC","szDecimals":5}]}),
            &json!({
                "tokens":[{"name":"USDC","index":0,"szDecimals":8}],
                "universe":[]
            }),
            &json!([null, {"name":"xy-z"}, {"name":"xyz"}]),
            &json!([
                {"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]},
                {"collateralToken":0,"universe":[{"name":"xy-z:FOO","szDecimals":3}]},
                {"collateralToken":0,"universe":[{"name":"xyz:FOO","szDecimals":3}]}
            ]),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate Hyperliquid perp internal symbol XYZFOOUSDC"));
    }

    #[test]
    fn hip3_order_uses_official_asset_id_and_standard_open_fails_closed() {
        let default_perp = json!({"universe":[{"name":"BTC","szDecimals":5}]});
        let spot = json!({
            "tokens":[
                {"name":"USDC","index":0,"szDecimals":8},
                {"name":"USDH","index":7,"szDecimals":6}
            ],
            "universe":[]
        });
        let make_catalog = || {
            HyperliquidAssetCatalog::from_all_values(
                &default_perp,
                &spot,
                &json!([null, {"name":"xyz"}]),
                &json!([
                    {"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]},
                    {"collateralToken":7,"universe":[{"name":"xyz:FOO","szDecimals":3}]}
                ]),
            )
            .unwrap()
        };
        let request = |account_hash, reduce_only| {
            let bytes = HyperliquidNewOrderParams::request_bytes_from_parts(
                TradeRequestType::HyperliquidNewUMOrder,
                1_700_000_000_123_456,
                42,
                account_hash,
                "XYZFOOUSDH",
                Side::Buy,
                OrderType::Limit,
                QuantizedValue::from_parts(1, -3, 1),
                QuantizedValue::from_parts(1, -1, 25),
                reduce_only,
            )
            .unwrap();
            TradeRequestMsg::parse(&bytes).unwrap()
        };

        let mut client = test_client_with_mode(HyperliquidAccountMode::Standard);
        client.catalog = make_catalog();
        assert!(client
            .build_payload(&request(client.account_hash, false), 7)
            .unwrap_err()
            .to_string()
            .contains("standard account mode"));

        let reduced: Value = serde_json::from_str(
            &client
                .build_payload(&request(client.account_hash, true), 8)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            reduced["request"]["payload"]["action"]["orders"][0]["a"],
            110_000
        );

        client.account_mode = HyperliquidAccountMode::Unified;
        client.catalog = make_catalog();
        let opened: Value = serde_json::from_str(
            &client
                .build_payload(&request(client.account_hash, false), 9)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            opened["request"]["payload"]["action"]["orders"][0]["a"],
            110_000
        );

        let mut portfolio = test_client_with_mode(HyperliquidAccountMode::PortfolioMargin);
        portfolio.catalog = make_catalog();
        let opened: Value = serde_json::from_str(
            &portfolio
                .build_payload(&request(portfolio.account_hash, false), 10)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            opened["request"]["payload"]["action"]["orders"][0]["a"],
            110_000
        );
    }

    #[test]
    fn parses_resting_filled_cancel_and_text_errors() {
        let resting = r#"{"channel":"post","data":{"id":7,"response":{"type":"action","payload":{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":123}}]}}}}}}"#;
        let outcome =
            parse_action_response(resting, TradeRequestType::HyperliquidNewUMOrder).unwrap();
        assert_eq!(outcome.transport_id, 7);
        assert_eq!(outcome.order_id, 123);
        assert_eq!(outcome.order_status_u8, OrderStatus::New.to_u8());

        let filled = r#"{"channel":"post","data":{"id":8,"response":{"type":"action","payload":{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"oid":"124","totalSz":"1.25","avgPx":"100.5"}}]}}}}}}"#;
        let outcome =
            parse_action_response(filled, TradeRequestType::HyperliquidNewMarginOrder).unwrap();
        assert_eq!(outcome.order_status_u8, OrderStatus::Filled.to_u8());
        assert_eq!(outcome.executed_qty, 1.25);
        assert_eq!(outcome.response_price, 100.5);

        let cancel = r#"{"channel":"post","data":{"id":9,"response":{"type":"action","payload":{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}}}}"#;
        let outcome =
            parse_action_response(cancel, TradeRequestType::HyperliquidCancelUMOrder).unwrap();
        assert_eq!(outcome.order_status_u8, OrderStatus::Canceled.to_u8());

        let rejected = r#"{"channel":"post","data":{"id":10,"response":{"type":"action","payload":{"status":"ok","response":{"type":"order","data":{"statuses":["Post only order would have immediately matched"]}}}}}}"#;
        let outcome =
            parse_action_response(rejected, TradeRequestType::HyperliquidNewUMOrder).unwrap();
        assert_eq!(outcome.status, 400);
        assert_eq!(outcome.code, POST_ONLY_REJECTED);
    }

    #[test]
    fn documented_hyperliquid_errors_reach_shared_response_categories() {
        use order_common::trade_error_code::hyperliquid::*;
        use order_common::{TradeEngineResponse, TradeEngineResponseMessage};

        let cases = [
            ("Price must be divisible by tick size.", INVALID_TICK),
            ("Order must have minimum value of $10.", MIN_NOTIONAL),
            ("Order must have minimum value of 10 USDC.", MIN_NOTIONAL),
            ("Insufficient margin to place order.", INSUFFICIENT_MARGIN),
            ("Reduce only order would increase position.", REDUCE_ONLY_REJECTED),
            ("Post only order would have immediately matched, bbo was 100/101.", POST_ONLY_REJECTED),
            ("Order could not immediately match against any resting orders.", NO_LIQUIDITY),
            ("No liquidity available for market order.", NO_LIQUIDITY),
            ("Invalid TP/SL price.", INVALID_TRIGGER_PRICE),
            ("Order would increase open interest while open interest is capped", POSITION_LIMIT_EXCEEDED),
            ("Order would increase open interest too quickly", POSITION_LIMIT_EXCEEDED),
            ("Order would cause position to exceed margin tier limit at current leverage", POSITION_LIMIT_EXCEEDED),
            ("Order rejected due to price more aggressive than oracle while at open interest cap", PRICE_LIMIT_REJECTED),
            ("Order price too far from oracle", PRICE_LIMIT_REJECTED),
            ("Order has insufficient spot balance to trade", INSUFFICIENT_SPOT_BALANCE),
            ("Order was never placed, already canceled, or filled.", ORDER_NOT_FOUND),
            ("Unknown new exchange rejection", ACTION_REJECTED),
        ];
        for (message, code) in cases {
            // Cover both per-order rejects and whole-action prevalidation errors.
            for payload in [
                json!({"status":"ok","response":{"type":"order","data":{"statuses":[{"error":message}]}}}),
                json!({"status":"err","response":message}),
            ] {
                let wire = json!({"channel":"post","data":{"id":42,"response":{"type":"action","payload":payload}}});
                let outcome = parse_action_response(
                    &wire.to_string(),
                    TradeRequestType::HyperliquidNewUMOrder,
                )
                .unwrap();
                assert_eq!(outcome.code, code, "{message}");
                assert_eq!(outcome.transport_id, 42);
                assert_eq!(outcome.message, message);
                let response = TradeEngineResponseMessage::new(
                    outcome.status,
                    TradeRequestType::HyperliquidNewUMOrder as u32,
                    symbol_utils::Exchange::Hyperliquid as u32,
                    9001,
                    outcome.code,
                );
                assert!(response.is_open_rejected());
                assert_eq!(
                    response.is_insufficient_margin(),
                    matches!(code, INSUFFICIENT_MARGIN | INSUFFICIENT_SPOT_BALANCE)
                );
                assert_eq!(
                    response.is_price_limit_rejected(),
                    code == PRICE_LIMIT_REJECTED
                );
                assert_eq!(
                    response.is_hyperliquid_position_limit_exceeded(),
                    code == POSITION_LIMIT_EXCEEDED
                );
                assert!(!response.is_hyperliquid_action_ambiguous());
            }
        }
    }

    #[test]
    fn builds_info_queries_with_actual_account_address() {
        let client = test_client();
        let request = GenericQueryRequest::create(
            QueryRequestType::HyperliquidUMQuery,
            1,
            2,
            HyperliquidQueryParams::create(
                client.account_hash,
                Bytes::from_static(b"{\"oid\":\"0x6d6b745f73696731000000000000002a\"}"),
            )
            .to_bytes(),
        );
        let parsed = QueryRequestMsg::parse(&request.to_bytes()).unwrap();
        let payload: Value =
            serde_json::from_str(&client.build_info_payload(&parsed, 77).unwrap()).unwrap();
        assert_eq!(payload["method"], "post");
        assert_eq!(payload["id"], 77);
        assert_eq!(payload["request"]["type"], "info");
        assert_eq!(payload["request"]["payload"]["type"], "orderStatus");
        assert_eq!(
            payload["request"]["payload"]["user"],
            "0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            payload["request"]["payload"]["oid"],
            "0x6d6b745f73696731000000000000002a"
        );
    }

    #[test]
    fn query_snapshots_are_full_and_independently_reconstructible() {
        let client = test_client();
        let state = json!({
            "balances": [
                {"coin":"USDC", "token":0, "total":"100", "hold":"0", "entryNtl":"0"},
                {"coin":"HYPE", "token":150, "total":"2.5", "hold":"0.5", "entryNtl":"90"}
            ]
        });
        let first = client
            .process_snapshot(QueryRequestType::HyperliquidSpotStateSnapshot, &state, 10)
            .unwrap();
        let second = client
            .process_snapshot(QueryRequestType::HyperliquidSpotStateSnapshot, &state, 20)
            .unwrap();
        for snapshot in [&first, &second] {
            assert_eq!(snapshot.len(), 4);
            let event_types = snapshot
                .iter()
                .map(|event| split_basic_account_event(event).unwrap().0)
                .collect::<Vec<_>>();
            assert_eq!(
                event_types,
                vec![
                    BasicAccountEventType::BalanceUpdate,
                    BasicAccountEventType::BalanceUpdate,
                    BasicAccountEventType::HyperliquidSpotBalance,
                    BasicAccountEventType::HyperliquidSpotBalance,
                ]
            );
        }
        assert_eq!(
            client
                .snapshot_scope(QueryRequestType::HyperliquidSpotStateSnapshot)
                .unwrap(),
            HyperliquidAccountMode::Standard.spot_scope() as u32
        );
    }

    #[test]
    fn unified_default_dex_query_is_incremental_not_a_complete_snapshot() {
        let client = test_client_with_mode(HyperliquidAccountMode::Unified);
        let state = json!({
            "assetPositions": [{
                "type": "oneWay",
                "position": {
                    "coin": "BTC",
                    "szi": "0.25",
                    "unrealizedPnl": "3.5",
                    "leverage": {"type": "cross", "value": 10}
                }
            }],
            "marginSummary": {"accountValue": "100", "totalNtlPos": "25", "totalRawUsd": "75", "totalMarginUsed": "2.5"},
            "crossMarginSummary": {"accountValue": "95", "totalNtlPos": "25", "totalRawUsd": "70", "totalMarginUsed": "2.5"},
            "crossMaintenanceMarginUsed": "1.0",
            "withdrawable": "72.5"
        });
        let messages = client
            .process_snapshot(
                QueryRequestType::HyperliquidClearinghouseSnapshot,
                &state,
                10,
            )
            .unwrap();
        assert_eq!(messages.len(), 3);
        assert!(
            !client.query_snapshot_is_complete(QueryRequestType::HyperliquidClearinghouseSnapshot)
        );
        assert!(client.query_snapshot_is_complete(QueryRequestType::HyperliquidSpotStateSnapshot));
    }

    #[test]
    fn portfolio_margin_queries_cannot_replace_combined_account_stream_state() {
        let client = test_client_with_mode(HyperliquidAccountMode::PortfolioMargin);
        let query = |req_type| {
            let request = GenericQueryRequest::create(
                req_type,
                1,
                2,
                HyperliquidQueryParams::create(client.account_hash, Bytes::new()).to_bytes(),
            );
            QueryRequestMsg::parse(&request.to_bytes()).unwrap()
        };

        let error = client
            .build_info_payload(
                &query(QueryRequestType::HyperliquidClearinghouseSnapshot),
                10,
            )
            .unwrap_err();
        assert!(error.to_string().contains("single default-DEX view"));
        assert!(client
            .process_snapshot(
                QueryRequestType::HyperliquidClearinghouseSnapshot,
                &json!({}),
                10,
            )
            .unwrap_err()
            .to_string()
            .contains("single default-DEX view"));
        assert!(
            !client.query_snapshot_is_complete(QueryRequestType::HyperliquidClearinghouseSnapshot)
        );

        let request: Value = serde_json::from_str(
            &client
                .build_info_payload(&query(QueryRequestType::HyperliquidSpotStateSnapshot), 11)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            request["request"]["payload"]["type"],
            "spotClearinghouseState"
        );
        assert!(!client.query_snapshot_is_complete(QueryRequestType::HyperliquidSpotStateSnapshot));

        let error = client
            .process_snapshot(
                QueryRequestType::HyperliquidSpotStateSnapshot,
                &json!({
                    "portfolioMarginRatio":"0.25",
                    "balances": [{
                        "coin":"USDC",
                        "token":0,
                        "total":"100",
                        "hold":"0",
                        "entryNtl":"0"
                    }]
                }),
                12,
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("requires a borrow/lend snapshot"));
    }

    #[test]
    fn standard_default_dex_query_is_incremental_not_an_all_dex_snapshot() {
        let client = test_client_with_mode(HyperliquidAccountMode::Standard);
        assert!(
            !client.query_snapshot_is_complete(QueryRequestType::HyperliquidClearinghouseSnapshot)
        );
        assert!(client.query_snapshot_is_complete(QueryRequestType::HyperliquidSpotStateSnapshot));
    }

    #[test]
    fn parses_wrapped_info_and_error_responses() {
        let success = r#"{"channel":"post","data":{"id":7,"response":{"type":"info","payload":{"type":"orderStatus","data":{"status":"unknownOid"}}}}}"#;
        let parsed = parse_info_response(success).unwrap();
        assert_eq!(parsed.transport_id, 7);
        assert_eq!(parsed.status, 200);
        assert_eq!(parsed.payload.unwrap()["status"], "unknownOid");

        let error = r#"{"channel":"post","data":{"id":8,"response":{"type":"error","payload":"429 Too Many Requests"}}}"#;
        let parsed = parse_info_response(error).unwrap();
        assert_eq!(parsed.transport_id, 8);
        assert_eq!(parsed.status, 400);
        assert_eq!(parsed.message, "429 Too Many Requests");
        assert!(parsed.payload.is_none());
    }

    #[test]
    fn rejects_agent_style_or_malformed_order_lookup_ids() {
        assert!(validate_order_lookup_id(&json!(0)).is_err());
        assert!(validate_order_lookup_id(&json!("0x01")).is_err());
        assert!(validate_order_lookup_id(&json!(1)).is_ok());
        assert!(validate_order_lookup_id(&json!("0x00000000000000000000000000000001")).is_ok());
    }

    #[test]
    fn validates_master_subaccount_and_vault_order_targets() {
        let account = "0x1111111111111111111111111111111111111111";
        let master = "0x2222222222222222222222222222222222222222";
        let agent = "0x3333333333333333333333333333333333333333";
        let master_role = json!({"role":"user"});
        let agent_role = json!({"role":"agent", "data":{"user": master}});
        assert!(validate_order_target_role(
            &json!({"role":"user"}),
            &master_role,
            account,
            account,
            None,
            None,
        )
        .is_ok());
        assert!(validate_order_target_role(
            &json!({"role":"user"}),
            &master_role,
            account,
            account,
            Some(account),
            None,
        )
        .is_err());
        assert!(validate_order_target_role(
            &json!({"role":"subAccount", "data":{"master": master}}),
            &agent_role,
            account,
            agent,
            Some(account),
            None,
        )
        .is_ok());
        let vault_details = json!({"vaultAddress": account, "leader": master});
        assert!(validate_order_target_role(
            &json!({"role":"vault"}),
            &agent_role,
            account,
            agent,
            Some(account),
            Some(&vault_details),
        )
        .is_ok());
        assert!(validate_order_target_role(
            &json!({"role":"agent"}),
            &agent_role,
            account,
            agent,
            None,
            None,
        )
        .is_err());
        assert!(validate_order_target_role(
            &json!({"role":"missing"}),
            &agent_role,
            account,
            agent,
            None,
            None,
        )
        .is_err());
        let wrong_agent_role = json!({
            "role":"agent",
            "data":{"user":"0x4444444444444444444444444444444444444444"}
        });
        assert!(validate_order_target_role(
            &json!({"role":"user"}),
            &wrong_agent_role,
            account,
            agent,
            None,
            None,
        )
        .is_err());
    }

    #[test]
    fn derives_expected_evm_address_from_signing_key() {
        assert_eq!(
            signing_key_address(&signing_key()),
            "0x14791697260e4c9a71f18484c9f997b308e59325"
        );
    }

    #[test]
    fn account_mode_change_latches_trading_off_until_restart() {
        let client = test_client();
        assert!(client
            .validate_account_mode_response(&json!("disabled"))
            .is_ok());
        assert!(client
            .validate_account_mode_response(&json!("unifiedAccount"))
            .is_err());
        assert!(!client.account_mode_valid.load(Ordering::Acquire));
        assert!(client
            .validate_account_mode_response(&json!("disabled"))
            .is_err());
    }

    #[test]
    fn runtime_ambiguous_default_is_rejected_for_users_but_resolves_for_vaults() {
        let user = test_client();
        assert!(user
            .validate_account_mode_response(&json!("default"))
            .is_err());
        assert!(!user.account_mode_valid.load(Ordering::Acquire));

        let mut vault = test_client();
        vault.account_role = HyperliquidUserRole::Vault;
        assert_eq!(
            vault
                .validate_account_mode_response(&json!("default"))
                .unwrap(),
            HyperliquidAccountMode::Standard
        );
        assert!(vault.account_mode_valid.load(Ordering::Acquire));
    }

    #[test]
    fn canonical_decimal_trims_trailing_zeroes_and_checks_precision() {
        assert_eq!(
            canonical_decimal(QuantizedValue::from_parts(1, -2, 300)).unwrap(),
            "3"
        );
        let perp = HyperliquidAsset {
            asset_id: 1,
            sz_decimals: 2,
            is_spot: false,
        };
        assert_eq!(
            validated_price(
                QuantizedValue::from_decimal(12.345).unwrap(),
                perp,
                Side::Buy,
            )
            .unwrap(),
            "12.345"
        );
        assert_eq!(
            validated_price(
                QuantizedValue::from_decimal(123.4567).unwrap(),
                perp,
                Side::Buy,
            )
            .unwrap(),
            "123.45"
        );
        assert_eq!(
            validated_price(
                QuantizedValue::from_decimal(123.4567).unwrap(),
                perp,
                Side::Sell,
            )
            .unwrap(),
            "123.46"
        );
        assert_eq!(
            validated_price(
                QuantizedValue::from_decimal(0.0123456).unwrap(),
                perp,
                Side::Buy,
            )
            .unwrap(),
            "0.0123"
        );
        assert_eq!(
            validated_price(
                QuantizedValue::from_decimal(0.0123456).unwrap(),
                perp,
                Side::Sell,
            )
            .unwrap(),
            "0.0124"
        );
        assert_eq!(
            validated_price(
                QuantizedValue::from_decimal(123_456.0).unwrap(),
                perp,
                Side::Buy,
            )
            .unwrap(),
            "123456"
        );
        assert!(validated_size(QuantizedValue::from_decimal(1.234).unwrap(), perp).is_err());
        assert_eq!(
            hyperliquid_cloid(1).unwrap(),
            "0x6d6b745f736967310000000000000001"
        );
    }
}
