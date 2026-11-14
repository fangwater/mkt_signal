use account_common::pm_ipc::PM_MAX_BYTES;
use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::Utc;
use mkt_parsers::msg::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use mkt_parsers::msg::hyperliquid_account_msg::{
    HyperliquidBasicFillMsg, HyperliquidBasicOrderMsg, HyperliquidFundingMsg, HyperliquidLedgerMsg,
    HyperliquidPerpDexStateMsg, HyperliquidSnapshotCompleteMsg, HyperliquidSpotBalanceMsg,
    HyperliquidTwapHistoryMsg, HyperliquidTwapSliceFillMsg,
};
use order_common::trade_request_type::hyperliquid_client_order_id_from_cloid;
use order_common::{ExecutionType, OrderStatus, OrderType, Side, TimeInForce, TradingVenue};
use runtime_common::symbol_util::{hyperliquid_internal_symbol, HyperliquidSpotBaseResolver};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};

mod borrowing;
mod native;

const DEFAULT_DEDUP_CAPACITY: usize = 32_768;
const DEFAULT_ORDER_CACHE_CAPACITY: usize = 16_384;
const DEFAULT_PENDING_FILL_CAPACITY: usize = 2_048;
const DEFAULT_LATE_ATTRIBUTION_CAPACITY: usize = 2_048;
const HYPERLIQUID_HISTORICAL_ORDERS_CAPACITY: usize = 2_000;
pub const HYPERLIQUID_BORROW_SNAPSHOT_TTL_MS: i64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidAccountMode {
    Standard,
    Unified,
    PortfolioMargin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidUserRole {
    User,
    Agent,
    Vault,
    SubAccount,
    Missing,
}

impl HyperliquidUserRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Vault => "vault",
            Self::SubAccount => "subAccount",
            Self::Missing => "missing",
        }
    }
}

impl HyperliquidAccountMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Unified => "unified",
            Self::PortfolioMargin => "portfolio_margin",
        }
    }

    pub fn spot_scope(self) -> BasicAccountScope {
        match self {
            Self::Standard => BasicAccountScope::HyperliquidStdSpot,
            Self::Unified => BasicAccountScope::HyperliquidUnified,
            Self::PortfolioMargin => BasicAccountScope::HyperliquidPortfolioMargin,
        }
    }

    pub fn perp_scope(self) -> BasicAccountScope {
        match self {
            Self::Standard => BasicAccountScope::HyperliquidStdPerp,
            Self::Unified => BasicAccountScope::HyperliquidUnified,
            Self::PortfolioMargin => BasicAccountScope::HyperliquidPortfolioMargin,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidInstrument {
    pub symbol: String,
    pub venue: TradingVenue,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HyperliquidAssetCatalog {
    by_coin: HashMap<String, HyperliquidInstrument>,
    active_perp_coins: Vec<String>,
    dex_collateral_tokens: HashMap<String, i64>,
    spot_balance_assets: HashMap<String, String>,
    spot_assets_by_token: HashMap<i64, String>,
}

impl HyperliquidAssetCatalog {
    pub fn from_meta(meta: &Value, spot_meta: &Value) -> Result<Self> {
        let mut catalog = Self::default();
        let universe = meta
            .get("universe")
            .and_then(Value::as_array)
            .context("Hyperliquid meta missing universe")?;
        for asset in universe {
            let coin = required_str(asset, "name")?;
            if !asset
                .get("isDelisted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                catalog.active_perp_coins.push(coin.to_string());
            }
            catalog.insert(
                coin,
                HyperliquidInstrument {
                    symbol: hyperliquid_internal_symbol(coin, "USDC")?,
                    venue: TradingVenue::HyperliquidFutures,
                },
            )?;
        }

        let token_names = spot_token_names(spot_meta)?;
        let spot_base_resolver =
            HyperliquidSpotBaseResolver::new(token_names.values().map(String::as_str));
        for (token, name) in &token_names {
            let raw_asset = sanitize_asset(name);
            if raw_asset.is_empty() {
                anyhow::bail!("Hyperliquid spot token name has no alphanumeric asset: {name:?}");
            }
            let canonical_asset = spot_base_resolver.canonical_base(&raw_asset);
            catalog
                .spot_assets_by_token
                .insert(*token, canonical_asset.clone());
            if catalog
                .spot_balance_assets
                .insert(name.to_ascii_lowercase(), canonical_asset)
                .is_some()
            {
                anyhow::bail!("duplicate Hyperliquid spot token name: {name}");
            }
        }

        let spot_universe = spot_meta
            .get("universe")
            .and_then(Value::as_array)
            .context("Hyperliquid spotMeta missing universe")?;
        for pair in spot_universe {
            let pair_name = required_str(pair, "name")?;
            let index = required_i64(pair, "index")?;
            let pair_tokens = pair
                .get("tokens")
                .and_then(Value::as_array)
                .filter(|tokens| tokens.len() == 2)
                .context("Hyperliquid spot pair must contain two token indexes")?;
            let base_index = value_i64(&pair_tokens[0]).context("invalid spot base token index")?;
            let quote_index =
                value_i64(&pair_tokens[1]).context("invalid spot quote token index")?;
            let base = token_names.get(&base_index).with_context(|| {
                format!("spot base token index {base_index} missing from tokens")
            })?;
            let quote = token_names.get(&quote_index).with_context(|| {
                format!("spot quote token index {quote_index} missing from tokens")
            })?;
            let canonical_base = spot_base_resolver.canonical_base(&sanitize_asset(base));
            let instrument = HyperliquidInstrument {
                symbol: hyperliquid_internal_symbol(&canonical_base, quote)?,
                venue: TradingVenue::HyperliquidMargin,
            };
            catalog.insert(pair_name, instrument.clone())?;
            catalog.insert(&format!("@{index}"), instrument)?;
        }
        let default_collateral_token = token_names
            .iter()
            .find_map(|(index, name)| name.eq_ignore_ascii_case("USDC").then_some(*index))
            .context("Hyperliquid spotMeta missing USDC collateral token")?;
        catalog
            .dex_collateral_tokens
            .insert(String::new(), default_collateral_token);
        Ok(catalog)
    }

    pub fn from_all_meta(
        meta: &Value,
        spot_meta: &Value,
        perp_dexs: &Value,
        all_perp_metas: &Value,
    ) -> Result<Self> {
        let mut catalog = Self::from_meta(meta, spot_meta)?;
        catalog.active_perp_coins.clear();
        let dex_rows = perp_dexs
            .as_array()
            .context("Hyperliquid perpDexs response must be an array")?;
        let meta_rows = all_perp_metas
            .as_array()
            .context("Hyperliquid allPerpMetas response must be an array")?;
        if dex_rows.is_empty() || dex_rows.len() != meta_rows.len() {
            anyhow::bail!(
                "Hyperliquid perpDexs/allPerpMetas length mismatch or empty: {} != {}",
                dex_rows.len(),
                meta_rows.len()
            );
        }
        if !dex_rows[0].is_null() {
            anyhow::bail!("Hyperliquid perpDexs missing null default-dex entry at index 0");
        }

        let token_names = spot_token_names(spot_meta)?;
        let mut dex_collateral_tokens = HashMap::with_capacity(dex_rows.len());
        let mut dex_names = HashSet::with_capacity(dex_rows.len());
        let mut perp_internal_owners = HashMap::<String, String>::new();
        for (index, (dex_row, dex_meta)) in dex_rows.iter().zip(meta_rows).enumerate() {
            let dex = if index == 0 {
                String::new()
            } else {
                let dex = required_str(dex_row, "name")?;
                if dex.is_empty() || dex.trim() != dex || !dex.is_ascii() || dex.contains(':') {
                    anyhow::bail!("invalid Hyperliquid perp dex name {dex:?}");
                }
                dex.to_string()
            };
            if !dex_names.insert(dex.to_ascii_lowercase()) {
                anyhow::bail!("duplicate Hyperliquid perp dex name {dex:?}");
            }
            let collateral_token = required_i64(dex_meta, "collateralToken")?;
            let collateral_name = token_names.get(&collateral_token).with_context(|| {
                format!(
                    "Hyperliquid dex {dex:?} collateral token {collateral_token} missing from spotMeta"
                )
            })?;
            if dex_collateral_tokens
                .insert(dex.clone(), collateral_token)
                .is_some()
            {
                anyhow::bail!("duplicate Hyperliquid perp dex name {dex:?}");
            }

            let universe = dex_meta
                .get("universe")
                .and_then(Value::as_array)
                .with_context(|| {
                    format!("Hyperliquid allPerpMetas entry {index} missing universe")
                })?;
            if index == 0 {
                let default_universe = meta
                    .get("universe")
                    .and_then(Value::as_array)
                    .context("Hyperliquid meta missing universe")?;
                let default_names = default_universe
                    .iter()
                    .map(|asset| required_str(asset, "name"))
                    .collect::<Result<Vec<_>>>()?;
                let all_meta_names = universe
                    .iter()
                    .map(|asset| required_str(asset, "name"))
                    .collect::<Result<Vec<_>>>()?;
                if default_names != all_meta_names {
                    anyhow::bail!(
                        "Hyperliquid meta and allPerpMetas default-Dex universe disagree"
                    );
                }
                if !collateral_name.eq_ignore_ascii_case("USDC") {
                    anyhow::bail!(
                        "Hyperliquid default DEX collateral must be USDC, got {collateral_name:?}"
                    );
                }
            }
            for asset in universe {
                let coin = required_str(asset, "name")?;
                if !asset
                    .get("isDelisted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    catalog.active_perp_coins.push(coin.to_string());
                }
                if index > 0 {
                    let expected_prefix = format!("{dex}:");
                    if !coin.starts_with(&expected_prefix) || coin.len() == expected_prefix.len() {
                        anyhow::bail!(
                            "Hyperliquid HIP-3 coin {coin:?} does not match DEX prefix {expected_prefix:?}"
                        );
                    }
                } else if coin.contains(':') {
                    anyhow::bail!(
                        "Hyperliquid default-Dex coin must not contain a DEX prefix: {coin:?}"
                    );
                }
                let symbol = hyperliquid_internal_symbol(coin, collateral_name)?;
                if let Some(owner) = perp_internal_owners.insert(symbol.clone(), coin.to_string()) {
                    if !owner.eq_ignore_ascii_case(coin) {
                        anyhow::bail!(
                            "Hyperliquid perp internal symbol collision {symbol}: {owner:?} vs {coin:?}"
                        );
                    }
                }
                catalog.insert(
                    coin,
                    HyperliquidInstrument {
                        symbol,
                        venue: TradingVenue::HyperliquidFutures,
                    },
                )?;
            }
        }
        catalog.dex_collateral_tokens = dex_collateral_tokens;
        Ok(catalog)
    }

    pub async fn fetch(client: &reqwest::Client, info_url: &str) -> Result<Self> {
        let meta_fut = fetch_info(client, info_url, json!({"type": "meta"}));
        let spot_meta_fut = fetch_info(client, info_url, json!({"type": "spotMeta"}));
        let perp_dexs_fut = fetch_info(client, info_url, json!({"type": "perpDexs"}));
        let all_perp_metas_fut = fetch_info(client, info_url, json!({"type": "allPerpMetas"}));
        let (meta, spot_meta, perp_dexs, all_perp_metas) =
            tokio::try_join!(meta_fut, spot_meta_fut, perp_dexs_fut, all_perp_metas_fut)?;
        Self::from_all_meta(&meta, &spot_meta, &perp_dexs, &all_perp_metas)
    }

    pub fn resolve(&self, coin: &str) -> Option<&HyperliquidInstrument> {
        self.by_coin.get(&coin.to_ascii_lowercase())
    }

    pub fn len(&self) -> usize {
        self.by_coin.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_coin.is_empty()
    }

    pub fn collateral_token_for_dex(&self, dex: &str) -> Option<i64> {
        self.dex_collateral_tokens.get(dex).copied()
    }

    pub fn perp_dexes(&self) -> Vec<String> {
        let mut dexs = self
            .dex_collateral_tokens
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        dexs.sort_unstable();
        dexs
    }

    fn spot_balance_asset(&self, coin: &str) -> Option<&str> {
        self.spot_balance_assets
            .get(&coin.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn insert(&mut self, coin: &str, instrument: HyperliquidInstrument) -> Result<()> {
        let key = coin.to_ascii_lowercase();
        if let Some(existing) = self.by_coin.get(&key) {
            if existing != &instrument {
                anyhow::bail!(
                    "conflicting Hyperliquid instrument mapping for wire coin {coin:?}: {existing:?} vs {instrument:?}"
                );
            }
            return Ok(());
        }
        self.by_coin.insert(key, instrument);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSnapshotPolicy {
    /// Explicit baseline-only mode: build identity/cumulative baselines from
    /// initial-session snapshots without emitting their rows. Reconnect
    /// snapshots still recover gaps.
    Ignore,
    /// Emit the first snapshot as well as later reconnect snapshots, with
    /// in-process identity deduplication.
    Process,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSnapshotContext {
    Initial,
    Reconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HyperliquidSubscriptionControl {
    NotControl,
    Acknowledged {
        subscription_type: String,
        completed_now: bool,
    },
}

/// Validates private subscription acknowledgements against one websocket
/// path's exact request set. A tracker is intentionally not shared between the
/// primary and secondary paths.
#[derive(Debug)]
pub struct HyperliquidSubscriptionAcks {
    user: String,
    expected_subscriptions: HashMap<String, Value>,
    acknowledged_types: HashSet<String>,
}

impl HyperliquidSubscriptionAcks {
    pub fn from_requests(requests: &[Value]) -> Result<Self> {
        let mut user = None;
        let mut expected_subscriptions = HashMap::with_capacity(requests.len());
        for request in requests {
            if request.get("method").and_then(Value::as_str) != Some("subscribe") {
                anyhow::bail!("Hyperliquid private subscription request must use method=subscribe");
            }
            let subscription = request
                .get("subscription")
                .context("Hyperliquid private subscription request missing subscription")?;
            let subscription_type = required_str(subscription, "type")?;
            let request_user = normalize_hyperliquid_address(required_str(subscription, "user")?)?;
            if user
                .as_ref()
                .is_some_and(|expected| expected != &request_user)
            {
                anyhow::bail!("Hyperliquid private subscription requests contain different users");
            }
            user.get_or_insert(request_user);
            let normalized_subscription = normalize_private_subscription(subscription)?;
            if expected_subscriptions
                .insert(normalized_subscription.to_string(), normalized_subscription)
                .is_some()
            {
                anyhow::bail!(
                    "duplicate Hyperliquid private subscription type {subscription_type}"
                );
            }
        }
        let user = user.context("Hyperliquid private subscription request set is empty")?;
        Ok(Self {
            user,
            expected_subscriptions,
            acknowledged_types: HashSet::with_capacity(requests.len()),
        })
    }

    pub fn observe(&mut self, root: &Value) -> Result<HyperliquidSubscriptionControl> {
        match root.get("channel").and_then(Value::as_str) {
            Some("error") => {
                let detail = root
                    .get("data")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "<missing data>".to_string());
                anyhow::bail!("Hyperliquid subscription error: {detail}");
            }
            Some("subscriptionResponse") => {}
            _ => return Ok(HyperliquidSubscriptionControl::NotControl),
        }

        let data = root
            .get("data")
            .context("Hyperliquid subscriptionResponse missing data")?;
        if data.get("method").and_then(Value::as_str) != Some("subscribe") {
            anyhow::bail!("Hyperliquid subscriptionResponse must acknowledge method=subscribe");
        }
        let subscription = data
            .get("subscription")
            .context("Hyperliquid subscriptionResponse missing subscription")?;
        let subscription_type = required_str(subscription, "type")?;
        let normalized_subscription = normalize_private_subscription(subscription)?;
        let subscription_key = normalized_subscription.to_string();
        let Some(expected_subscription) = self.expected_subscriptions.get(&subscription_key) else {
            anyhow::bail!(
                "unexpected Hyperliquid private subscription acknowledgement type {subscription_type}"
            );
        };
        let response_user = normalize_hyperliquid_address(required_str(subscription, "user")?)?;
        if response_user != self.user {
            anyhow::bail!(
                "Hyperliquid subscriptionResponse user mismatch: expected={} received={}",
                self.user,
                response_user
            );
        }
        if &normalized_subscription != expected_subscription {
            anyhow::bail!(
                "Hyperliquid subscriptionResponse parameters do not match the requested {subscription_type} subscription"
            );
        }

        let was_complete = self.is_complete();
        self.acknowledged_types.insert(subscription_key);
        Ok(HyperliquidSubscriptionControl::Acknowledged {
            subscription_type: subscription_type.to_string(),
            completed_now: !was_complete && self.is_complete(),
        })
    }

    pub fn is_complete(&self) -> bool {
        self.acknowledged_types.len() == self.expected_subscriptions.len()
    }

    pub fn has_acknowledged(&self, subscription_type: &str) -> bool {
        self.expected_subscriptions
            .iter()
            .any(|(key, subscription)| {
                subscription.get("type").and_then(Value::as_str) == Some(subscription_type)
                    && self.acknowledged_types.contains(key)
            })
    }

    pub fn has_acknowledged_frame(&self, root: &Value) -> bool {
        let Some(channel) = root.get("channel").and_then(Value::as_str) else {
            return false;
        };
        let kind = if channel == "user" {
            "userEvents"
        } else {
            channel
        };
        self.expected_subscriptions
            .iter()
            .any(|(key, subscription)| {
                subscription.get("type").and_then(Value::as_str) == Some(kind)
                    && self.acknowledged_types.contains(key)
                    && ["coin", "dex"].iter().all(|field| {
                        subscription.get(*field).is_none_or(|expected| {
                            root.get("data").and_then(|data| data.get(*field)) == Some(expected)
                        })
                    })
            })
    }

    pub fn reset(&mut self) {
        self.acknowledged_types.clear();
    }
}

fn normalize_private_subscription(subscription: &Value) -> Result<Value> {
    let mut normalized = subscription.clone();
    normalized["user"] = Value::String(normalize_hyperliquid_address(required_str(
        subscription,
        "user",
    )?)?);
    if required_str(subscription, "type")? == "spotState" {
        let object = normalized
            .as_object_mut()
            .context("Hyperliquid private subscription must be an object")?;
        let is_portfolio_margin = object
            .remove("isPortfolioMargin")
            .map(|value| {
                value
                    .as_bool()
                    .with_context(|| "Hyperliquid spotState isPortfolioMargin must be a boolean")
            })
            .transpose()?;
        let ignore_portfolio_margin = object
            .remove("ignorePortfolioMargin")
            .map(|value| {
                value.as_bool().with_context(|| {
                    "Hyperliquid spotState ignorePortfolioMargin must be a boolean"
                })
            })
            .transpose()?;
        let legacy_ignore_portfolio_margin = is_portfolio_margin.map(|value| !value);
        if let (Some(legacy), Some(canonical)) =
            (legacy_ignore_portfolio_margin, ignore_portfolio_margin)
        {
            if legacy != canonical {
                anyhow::bail!(
                    "Hyperliquid spotState subscription has conflicting portfolio-margin flags"
                );
            }
        }
        object.insert(
            "ignorePortfolioMargin".to_string(),
            Value::Bool(
                ignore_portfolio_margin
                    .or(legacy_ignore_portfolio_margin)
                    .unwrap_or(false),
            ),
        );
    }
    normalized.sort_all_objects();
    Ok(normalized)
}

#[derive(Debug, Clone)]
struct OrderIdentity {
    client_order_id: i64,
    cloid: String,
    instrument: HyperliquidInstrument,
    orig_size: f64,
    order_type: Option<OrderType>,
    time_in_force: Option<TimeInForce>,
    intent_unrepresentable: bool,
}

impl OrderIdentity {
    fn ipc_intent(&self) -> Option<(OrderType, TimeInForce)> {
        if self.intent_unrepresentable {
            return None;
        }
        if let (Some(order_type), Some(time_in_force)) = (self.order_type, self.time_in_force) {
            return Some((order_type, time_in_force));
        }

        let market_tif = |order_type| {
            matches!(
                order_type,
                OrderType::Market | OrderType::StopMarket | OrderType::TakeProfitMarket
            )
            .then_some((order_type, TimeInForce::IOC))
        };
        if let Some(order_type) = self.order_type {
            if let Some(intent) = market_tif(order_type) {
                return Some(intent);
            }
        }

        // Internal Hyperliquid requests currently support post-only Limit/Alo
        // and protected Market/Ioc. The downstream local order, when present,
        // replaces this fallback with the exact local intent. External orders
        // without a venue intent fact are left unpublished instead of being
        // mislabeled as GTC by the fixed IPC enum contract.
        if self.client_order_id > 0 {
            return match (self.order_type, self.time_in_force) {
                (Some(OrderType::Limit), None) | (None, None) => {
                    Some((OrderType::Limit, TimeInForce::GTX))
                }
                (None, Some(TimeInForce::GTX)) => Some((OrderType::Limit, TimeInForce::GTX)),
                (None, Some(TimeInForce::IOC)) => Some((OrderType::Market, TimeInForce::IOC)),
                _ => None,
            };
        }
        None
    }
}

#[derive(Debug, Clone)]
struct ParsedFill {
    coin: String,
    price: f64,
    quantity: f64,
    side: Side,
    time: i64,
    order_id: i64,
    crossed: bool,
    tid: i64,
    transaction_hash: String,
    liquidation_method: String,
    start_position: Option<String>,
    dir: Option<String>,
    closed_pnl: Option<String>,
    fee: Option<String>,
    fee_token: Option<String>,
    builder_fee: Option<String>,
    twap_id: Option<i64>,
    liquidated_user: Option<String>,
    liquidation_mark_price: Option<String>,
    received_at: i64,
    cumulative_filled_quantity: f64,
}

#[derive(Debug, Clone)]
struct ParsedFunding {
    time: i64,
    coin: String,
    usdc: String,
    szi: String,
    funding_rate: String,
    transaction_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HyperliquidFactWatermarks {
    pub fill_time_ms: Option<i64>,
    pub funding_time_ms: Option<i64>,
    pub ledger_time_ms: Option<i64>,
    pub twap_slice_time_ms: Option<i64>,
    pub twap_history_time_s: Option<i64>,
}

#[derive(Debug)]
pub struct HyperliquidOrderCutRecovery {
    pub events: Vec<Bytes>,
    pub historical_seed_count: usize,
    pub frontend_seed_count: usize,
}

#[derive(Debug, Clone)]
struct ParsedLedgerUpdate {
    time: i64,
    transaction_hash: String,
    delta_type: String,
    delta_json: String,
}

#[derive(Debug, Clone)]
struct ParsedSpotBalance {
    token: i64,
    wire_coin: String,
    internal_coin: String,
    total: String,
    total_value: f64,
    hold: String,
    entry_ntl: String,
}

#[derive(Debug, Clone)]
struct ParsedPerpDexState {
    message: HyperliquidPerpDexStateMsg,
    account_value: f64,
    total_raw_usd: f64,
    cross_account_value: f64,
    cross_total_ntl_pos: f64,
    cross_total_margin_used: f64,
    cross_maintenance_margin_used: f64,
}

#[derive(Debug, Clone)]
struct ParsedOrderUpdate {
    order_id: i64,
    identity: Option<OrderIdentity>,
    status: String,
    status_timestamp: i64,
    remaining: f64,
    quantity: f64,
    price: f64,
    side: Side,
    order_status: OrderStatus,
    execution_type: ExecutionType,
    dedup_key: String,
    is_active: bool,
}

#[derive(Debug, Clone, Copy)]
struct OrderLifecycleWatermark {
    status_timestamp: i64,
    terminal: bool,
}

#[derive(Debug, Clone)]
struct BoundedSet<T> {
    values: HashSet<T>,
    order: VecDeque<T>,
    capacity: usize,
}

impl<T> BoundedSet<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    fn new(capacity: usize) -> Self {
        Self {
            values: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn insert(&mut self, value: T) -> bool {
        if !self.values.insert(value.clone()) {
            return false;
        }
        self.order.push_back(value);
        if self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
        true
    }

    fn contains(&self, value: &T) -> bool {
        self.values.contains(value)
    }
}

#[derive(Debug, Clone)]
pub struct HyperliquidAccountProcessor {
    user: String,
    catalog: HyperliquidAssetCatalog,
    mode: HyperliquidAccountMode,
    fill_snapshot_policy: FillSnapshotPolicy,
    orders: HashMap<i64, OrderIdentity>,
    order_age: VecDeque<i64>,
    active_order_ids: HashSet<i64>,
    seen_order_updates: BoundedSet<String>,
    seen_fills: BoundedSet<String>,
    seen_fundings: HashMap<[u8; 32], Option<String>>,
    seen_funding_age: VecDeque<[u8; 32]>,
    seen_ledger_updates: BoundedSet<[u8; 32]>,
    seen_twap_slice_fills: HashMap<[u8; 32], [u8; 32]>,
    seen_twap_slice_fill_age: VecDeque<[u8; 32]>,
    seen_twap_history: HashMap<[u8; 32], [u8; 32]>,
    seen_twap_history_age: VecDeque<[u8; 32]>,
    native_events: native::NativeDedup,
    fact_watermarks: HyperliquidFactWatermarks,
    historical_fill_anchor_by_oid: HashMap<i64, f64>,
    expected_fill_cumulative_by_oid: HashMap<i64, f64>,
    fill_cumulative_by_oid: HashMap<i64, f64>,
    attributed_fill_quantity_by_oid: HashMap<i64, f64>,
    fill_oid_age: VecDeque<i64>,
    pending_fills: HashMap<i64, VecDeque<ParsedFill>>,
    pending_fill_count: usize,
    late_attribution_fills: HashMap<i64, VecDeque<ParsedFill>>,
    late_attribution_fill_count: usize,
    unrecoverable_unattributed_oids: HashSet<i64>,
    pending_terminal_by_oid: HashMap<i64, ParsedOrderUpdate>,
    order_lifecycle_by_oid: HashMap<i64, OrderLifecycleWatermark>,
    balances: HashMap<String, f64>,
    spot_balances_by_token: HashMap<i64, f64>,
    spot_snapshot_seen: bool,
    borrow_snapshot: Option<borrowing::BorrowSnapshot>,
    positions: HashMap<String, (f32, f64)>,
    unified_margin_by_token: Option<(HashMap<i64, f64>, HashMap<i64, f64>)>,
}

impl HyperliquidAccountProcessor {
    pub fn new(
        user: impl Into<String>,
        catalog: HyperliquidAssetCatalog,
        mode: HyperliquidAccountMode,
        fill_snapshot_policy: FillSnapshotPolicy,
    ) -> Result<Self> {
        let user = normalize_hyperliquid_address(&user.into())?;
        Ok(Self {
            user,
            catalog,
            mode,
            fill_snapshot_policy,
            orders: HashMap::with_capacity(DEFAULT_ORDER_CACHE_CAPACITY),
            order_age: VecDeque::with_capacity(DEFAULT_ORDER_CACHE_CAPACITY),
            active_order_ids: HashSet::new(),
            seen_order_updates: BoundedSet::new(DEFAULT_DEDUP_CAPACITY),
            seen_fills: BoundedSet::new(DEFAULT_DEDUP_CAPACITY),
            seen_fundings: HashMap::with_capacity(DEFAULT_DEDUP_CAPACITY),
            seen_funding_age: VecDeque::with_capacity(DEFAULT_DEDUP_CAPACITY),
            seen_ledger_updates: BoundedSet::new(DEFAULT_DEDUP_CAPACITY),
            seen_twap_slice_fills: HashMap::with_capacity(DEFAULT_DEDUP_CAPACITY),
            seen_twap_slice_fill_age: VecDeque::with_capacity(DEFAULT_DEDUP_CAPACITY),
            seen_twap_history: HashMap::with_capacity(DEFAULT_DEDUP_CAPACITY),
            seen_twap_history_age: VecDeque::with_capacity(DEFAULT_DEDUP_CAPACITY),
            native_events: native::NativeDedup::default(),
            fact_watermarks: HyperliquidFactWatermarks::default(),
            historical_fill_anchor_by_oid: HashMap::with_capacity(DEFAULT_ORDER_CACHE_CAPACITY),
            expected_fill_cumulative_by_oid: HashMap::with_capacity(DEFAULT_ORDER_CACHE_CAPACITY),
            fill_cumulative_by_oid: HashMap::with_capacity(DEFAULT_DEDUP_CAPACITY),
            attributed_fill_quantity_by_oid: HashMap::with_capacity(DEFAULT_DEDUP_CAPACITY),
            fill_oid_age: VecDeque::with_capacity(DEFAULT_DEDUP_CAPACITY),
            pending_fills: HashMap::new(),
            pending_fill_count: 0,
            late_attribution_fills: HashMap::new(),
            late_attribution_fill_count: 0,
            unrecoverable_unattributed_oids: HashSet::new(),
            pending_terminal_by_oid: HashMap::new(),
            order_lifecycle_by_oid: HashMap::new(),
            balances: HashMap::new(),
            spot_balances_by_token: HashMap::new(),
            spot_snapshot_seen: false,
            borrow_snapshot: None,
            positions: HashMap::new(),
            unified_margin_by_token: None,
        })
    }

    pub fn process_json(&mut self, payload: &[u8]) -> Result<Vec<Bytes>> {
        self.process_json_at(payload, Utc::now().timestamp_millis())
    }

    pub fn process_json_at(&mut self, payload: &[u8], now_ms: i64) -> Result<Vec<Bytes>> {
        let root: Value = serde_json::from_slice(payload).context("decode Hyperliquid WS JSON")?;
        self.process_value_at(&root, now_ms)
    }

    pub fn process_value_at(&mut self, root: &Value, now_ms: i64) -> Result<Vec<Bytes>> {
        self.process_value_at_with_fill_snapshot_context(root, now_ms, FillSnapshotContext::Initial)
    }

    pub fn process_value_at_with_fill_snapshot_context(
        &mut self,
        root: &Value,
        now_ms: i64,
        fill_snapshot_context: FillSnapshotContext,
    ) -> Result<Vec<Bytes>> {
        match root.get("channel").and_then(Value::as_str) {
            Some("orderUpdates") => self.process_order_updates(root),
            Some("userFills") => self.process_user_fills(root, now_ms, fill_snapshot_context),
            Some("spotState") => self.process_spot_state(root, now_ms),
            Some("clearinghouseState") => self.process_clearinghouse_state(root, now_ms),
            Some("allDexsClearinghouseState") => {
                self.process_all_dexs_clearinghouse_state(root, now_ms)
            }
            Some("userFundings") => self.process_user_fundings(root),
            Some("userNonFundingLedgerUpdates") => {
                self.process_user_non_funding_ledger_updates(root)
            }
            Some("userTwapSliceFills") => {
                self.process_user_twap_slice_fills(root, now_ms, fill_snapshot_context)
            }
            Some("userTwapHistory") => self.process_user_twap_history(root),
            Some("user" | "twapStates" | "activeAssetData" | "notification" | "webData3") => {
                self.process_native_frame(root, now_ms, fill_snapshot_context)
            }
            Some("subscriptionResponse") | Some("pong") => Ok(Vec::new()),
            Some(_) | None => Ok(Vec::new()),
        }
    }

    pub fn fact_watermarks(&self) -> HyperliquidFactWatermarks {
        self.fact_watermarks
    }

    pub fn active_order_ids_snapshot(&self) -> HashSet<i64> {
        self.active_order_ids.clone()
    }

    /// Atomically apply one HTTP order cut and recover its factual lifecycle.
    /// `required_active_order_ids` must be captured immediately before the cut
    /// starts; omitting any such pin makes the venue's bounded history
    /// insufficient and rejects the complete cut without changing this state.
    pub fn recover_order_lifecycle_cut(
        &mut self,
        historical_orders: &Value,
        frontend_open_orders: &[(String, Value)],
        required_active_order_ids: &HashSet<i64>,
    ) -> Result<HyperliquidOrderCutRecovery> {
        let mut candidate = self.clone();
        let recovered = candidate.recover_order_lifecycle_cut_inner(
            historical_orders,
            frontend_open_orders,
            required_active_order_ids,
        )?;
        *self = candidate;
        Ok(recovered)
    }

    fn recover_order_lifecycle_cut_inner(
        &mut self,
        historical_orders: &Value,
        frontend_open_orders: &[(String, Value)],
        required_active_order_ids: &HashSet<i64>,
    ) -> Result<HyperliquidOrderCutRecovery> {
        let historical_rows = historical_orders
            .as_array()
            .context("Hyperliquid historicalOrders response must be an array")?;
        if historical_rows.len() > HYPERLIQUID_HISTORICAL_ORDERS_CAPACITY {
            anyhow::bail!(
                "Hyperliquid historicalOrders exceeded its documented {}-order capacity: {}",
                HYPERLIQUID_HISTORICAL_ORDERS_CAPACITY,
                historical_rows.len()
            );
        }

        let mut historical_oids = HashSet::with_capacity(historical_rows.len());
        let mut lifecycle_rows = Vec::with_capacity(historical_rows.len());
        for row in historical_rows {
            let order = row
                .get("order")
                .context("historicalOrders row missing order object")?;
            let order_id = required_i64(order, "oid")?;
            if order_id <= 0 {
                anyhow::bail!("Hyperliquid oid must be positive");
            }
            if !historical_oids.insert(order_id) {
                anyhow::bail!("duplicate oid in Hyperliquid historicalOrders: {order_id}");
            }
            let status_timestamp = required_i64(row, "statusTimestamp")?;
            if status_timestamp <= 0 {
                anyhow::bail!("Hyperliquid order statusTimestamp must be positive");
            }
            lifecycle_rows.push((status_timestamp, order_id, row.clone()));
        }

        let mut frontend_oids = HashSet::new();
        let mut frontend_rows = Vec::new();
        for (dex, payload) in frontend_open_orders {
            let rows = payload.as_array().with_context(|| {
                format!("Hyperliquid frontendOpenOrders dex {dex:?} response must be an array")
            })?;
            for order in rows {
                let order_id = required_i64(order, "oid")?;
                if order_id <= 0 {
                    anyhow::bail!("Hyperliquid oid must be positive");
                }
                if !frontend_oids.insert(order_id) {
                    anyhow::bail!(
                        "duplicate oid across Hyperliquid frontendOpenOrders cuts: {order_id}"
                    );
                }
                frontend_rows.push((dex, order_id, order));
            }
        }

        for order_id in required_active_order_ids {
            if historical_oids.contains(order_id) || frontend_oids.contains(order_id) {
                continue;
            }
            if historical_rows.len() == HYPERLIQUID_HISTORICAL_ORDERS_CAPACITY {
                anyhow::bail!(
                    "Hyperliquid historicalOrders reached its {}-order retention boundary without active pinned oid {}; frontendOpenOrders also omitted it, so lifecycle coverage is unprovable",
                    HYPERLIQUID_HISTORICAL_ORDERS_CAPACITY,
                    order_id
                );
            }
            anyhow::bail!(
                "authoritative Hyperliquid order cut omitted active pinned oid {order_id}; refusing to clear it without a factual lifecycle"
            );
        }

        // frontendOpenOrders carries the richer orderType/tif facts. Seed it
        // first so a historical lifecycle row that omits those fields reuses
        // the factual intent instead of replacing it with a default.
        let mut frontend_seed_count = 0_usize;
        for (dex, payload) in frontend_open_orders {
            frontend_seed_count = frontend_seed_count
                .checked_add(self.seed_frontend_open_orders(payload).with_context(|| {
                    format!("seed authoritative Hyperliquid frontendOpenOrders dex {dex:?}")
                })?)
                .context("Hyperliquid frontendOpenOrders seed count overflow")?;
        }
        let historical_seed_count = self
            .seed_historical_orders(historical_orders)
            .context("seed authoritative Hyperliquid historicalOrders cut")?;

        // A frontend-only order was created after historicalOrders answered.
        // Its active state and creation timestamp are both venue facts; the
        // live WS buffer will replay any later transition after this cut.
        for (_, order_id, order) in frontend_rows {
            if historical_oids.contains(&order_id) {
                continue;
            }
            let timestamp = required_i64(order, "timestamp")?;
            if timestamp <= 0 {
                anyhow::bail!("Hyperliquid frontend open order timestamp must be positive");
            }
            lifecycle_rows.push((
                timestamp,
                order_id,
                json!({
                    "order": order.clone(),
                    "status": "open",
                    "statusTimestamp": timestamp,
                }),
            ));
        }
        lifecycle_rows.sort_by_key(|(timestamp, order_id, _)| (*timestamp, *order_id));
        let events = self.process_order_updates(&json!({
            "channel": "orderUpdates",
            "data": lifecycle_rows
                .into_iter()
                .map(|(_, _, row)| row)
                .collect::<Vec<_>>(),
        }))?;
        // A stale frontend response must not resurrect an oid whose equal/newer
        // terminal lifecycle was already accepted from the other redundant
        // path. A deferred internal terminal remains pinned until factual fill
        // coverage releases it.
        for order_id in frontend_oids {
            if self
                .order_lifecycle_by_oid
                .get(&order_id)
                .is_some_and(|watermark| watermark.terminal)
                && !self.pending_terminal_by_oid.contains_key(&order_id)
            {
                self.active_order_ids.remove(&order_id);
            }
        }
        self.evict_inactive_orders_to_capacity();
        Ok(HyperliquidOrderCutRecovery {
            events,
            historical_seed_count,
            frontend_seed_count,
        })
    }

    /// Seed `oid -> cloid` attribution and the absolute exchange cumulative
    /// fill anchor from Info `historicalOrders`, without replaying lifecycle
    /// events or inventing fill prices.
    pub fn seed_historical_orders(&mut self, payload: &Value) -> Result<usize> {
        let rows = payload
            .as_array()
            .context("Hyperliquid historicalOrders response must be an array")?;
        let mut parsed = Vec::with_capacity(rows.len());
        let mut seen_oids = HashSet::with_capacity(rows.len());
        for row in rows {
            if let Some((order_id, identity)) = self.parse_order_identity(row)? {
                if !seen_oids.insert(order_id) {
                    anyhow::bail!("duplicate oid in Hyperliquid historicalOrders: {order_id}");
                }
                let order = row
                    .get("order")
                    .context("historicalOrders row missing order object")?;
                let remaining = required_f64(order, "sz")?;
                validate_nonnegative_finite("historical order remaining size", remaining)?;
                let epsilon = (identity.orig_size.abs() * 1.0e-9).max(1.0e-12);
                if remaining > identity.orig_size + epsilon {
                    anyhow::bail!(
                        "Hyperliquid historical order remaining size exceeds origSz: oid={} remaining={} origSz={}",
                        order_id,
                        remaining,
                        identity.orig_size
                    );
                }
                let status = required_str(row, "status")?;
                if status.eq_ignore_ascii_case("filled") && remaining > epsilon {
                    anyhow::bail!(
                        "Hyperliquid historical filled order has nonzero remaining size: oid={} remaining={}",
                        order_id,
                        remaining
                    );
                }
                let anchor = (identity.orig_size - remaining)
                    .max(0.0)
                    .min(identity.orig_size);
                parsed.push((order_id, identity, anchor));
            }
        }
        let inserted = parsed.len();
        for (order_id, identity, anchor) in parsed {
            self.historical_fill_anchor_by_oid
                .entry(order_id)
                .and_modify(|current| *current = current.max(anchor))
                .or_insert(anchor);
            self.expected_fill_cumulative_by_oid
                .entry(order_id)
                .and_modify(|current| *current = current.max(anchor))
                .or_insert(anchor);
            self.cache_order(order_id, identity);
        }
        Ok(inserted)
    }

    /// Seed active order attribution from Info `frontendOpenOrders`. Unlike
    /// `historicalOrders`, this endpoint returns each order as a flat row and
    /// is the authoritative startup source for orders that must stay pinned.
    pub fn seed_frontend_open_orders(&mut self, payload: &Value) -> Result<usize> {
        let rows = payload
            .as_array()
            .context("Hyperliquid frontendOpenOrders response must be an array")?;
        let mut parsed = Vec::with_capacity(rows.len());
        let mut seen_oids = HashSet::with_capacity(rows.len());
        for order in rows {
            let Some((order_id, identity)) = self.parse_order_identity_from_order(order)? else {
                continue;
            };
            if !seen_oids.insert(order_id) {
                anyhow::bail!("duplicate oid in Hyperliquid frontendOpenOrders: {order_id}");
            }
            let remaining = required_f64(order, "sz")?;
            validate_positive_finite("frontend open order remaining size", remaining)?;
            let epsilon = (identity.orig_size.abs() * 1.0e-9).max(1.0e-12);
            if remaining > identity.orig_size + epsilon {
                anyhow::bail!(
                    "Hyperliquid frontend open order remaining size exceeds origSz: oid={} remaining={} origSz={}",
                    order_id,
                    remaining,
                    identity.orig_size
                );
            }
            let anchor = (identity.orig_size - remaining)
                .max(0.0)
                .min(identity.orig_size);
            parsed.push((order_id, identity, anchor));
        }

        let inserted = parsed.len();
        for (order_id, identity, anchor) in parsed {
            self.active_order_ids.insert(order_id);
            self.historical_fill_anchor_by_oid
                .entry(order_id)
                .and_modify(|current| *current = current.max(anchor))
                .or_insert(anchor);
            self.expected_fill_cumulative_by_oid
                .entry(order_id)
                .and_modify(|current| *current = current.max(anchor))
                .or_insert(anchor);
            self.cache_order(order_id, identity);
        }
        Ok(inserted)
    }

    /// Verify that every active internal order's exchange cumulative is fully
    /// backed by individual factual fills produced by this processor lifetime.
    /// Exchange order rows can prove a cumulative quantity, but cannot provide
    /// the missing trade price or trade id, so they must never advance this
    /// barrier by themselves.
    pub fn validate_active_internal_fill_coverage(&self) -> Result<()> {
        for order_id in &self.active_order_ids {
            let Some(identity) = self.orders.get(order_id) else {
                anyhow::bail!(
                    "active Hyperliquid oid {order_id} is missing its order identity during fill coverage validation"
                );
            };
            if identity.client_order_id <= 0 {
                continue;
            }
            let expected = self
                .expected_fill_cumulative_by_oid
                .get(order_id)
                .copied()
                .unwrap_or(0.0);
            let attributed = self
                .attributed_fill_quantity_by_oid
                .get(order_id)
                .copied()
                .unwrap_or(0.0);
            let epsilon = (identity.orig_size.abs() * 1.0e-9).max(1.0e-12);
            if expected > attributed + epsilon {
                anyhow::bail!(
                    "Hyperliquid factual fill history does not cover active internal oid {order_id}: expected_cumulative={expected} recovered_quantity={attributed}; the missing fills are outside the recoverable snapshot window"
                );
            }
        }
        Ok(())
    }

    fn process_order_updates_inner(&mut self, root: &Value) -> Result<Vec<Bytes>> {
        let rows = root
            .get("data")
            .and_then(Value::as_array)
            .context("Hyperliquid orderUpdates data must be an array")?;
        let parsed = rows
            .iter()
            .map(|row| self.parse_order_update(row))
            .collect::<Result<Vec<_>>>()?;

        // Build every fallible output before touching cache, dedup, pin, or
        // pending-fill state. A malformed row therefore rejects the whole
        // websocket frame and leaves it safe to replay.
        let mut output = Vec::new();
        let mut frame_dedup = HashSet::with_capacity(parsed.len());
        let mut dedup_to_commit = Vec::with_capacity(parsed.len());
        let mut pending_to_drain = HashSet::new();
        let mut final_order_state = HashMap::new();
        let mut lifecycle_shadow = self.order_lifecycle_by_oid.clone();
        let mut lifecycle_to_commit = HashMap::new();
        let mut attributed_quantity_to_commit = HashMap::new();
        let mut expected_cumulative_to_commit = HashMap::new();
        let mut accepted_indices = Vec::with_capacity(parsed.len());
        for (index, update) in parsed.iter().enumerate() {
            let Some(identity) = update.identity.as_ref() else {
                continue;
            };
            if lifecycle_shadow.get(&update.order_id).is_some_and(|last| {
                update.status_timestamp <= last.status_timestamp
                    || (last.terminal && update.is_active)
            }) {
                continue;
            }
            let watermark = OrderLifecycleWatermark {
                status_timestamp: update.status_timestamp,
                terminal: !update.is_active,
            };
            lifecycle_shadow.insert(update.order_id, watermark);
            lifecycle_to_commit.insert(update.order_id, watermark);
            accepted_indices.push(index);

            if identity.client_order_id > 0
                && self
                    .unrecoverable_unattributed_oids
                    .contains(&update.order_id)
            {
                anyhow::bail!(
                    "Hyperliquid internal oid {} arrived after its unattributed fill journal overflowed; refusing to synthesize or terminalize",
                    update.order_id
                );
            }

            let drain_attribution = pending_to_drain.insert(update.order_id);
            let mut fills_to_attribute = Vec::new();
            if drain_attribution {
                if identity.client_order_id > 0 {
                    if let Some(rows) = self.late_attribution_fills.get(&update.order_id) {
                        fills_to_attribute.extend(rows.iter());
                    }
                }
                if let Some(rows) = self.pending_fills.get(&update.order_id) {
                    fills_to_attribute.extend(rows.iter());
                }
                fills_to_attribute.sort_by(|left, right| {
                    left.time
                        .cmp(&right.time)
                        .then_with(|| left.coin.cmp(&right.coin))
                        .then_with(|| left.tid.cmp(&right.tid))
                        .then_with(|| left.order_id.cmp(&right.order_id))
                        .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
                });
            }

            let mut attributed_fills = Vec::with_capacity(fills_to_attribute.len());
            let mut newly_attributed_quantity = 0.0;
            for fill in fills_to_attribute {
                let msg = self.fill_message(Some(identity), fill)?.with_context(|| {
                    format!(
                        "cannot attribute retained Hyperliquid fill for internal oid {}",
                        update.order_id
                    )
                })?;
                attributed_fills.push(msg);
                if identity.client_order_id > 0 {
                    newly_attributed_quantity += fill.quantity;
                    validate_nonnegative_finite(
                        "attributed Hyperliquid fill quantity",
                        newly_attributed_quantity,
                    )?;
                }
            }
            let previous_attributed = attributed_quantity_to_commit
                .get(&update.order_id)
                .copied()
                .or_else(|| {
                    self.attributed_fill_quantity_by_oid
                        .get(&update.order_id)
                        .copied()
                })
                .unwrap_or(0.0);
            let attributed_quantity = previous_attributed + newly_attributed_quantity;
            validate_nonnegative_finite(
                "total attributed Hyperliquid fill quantity",
                attributed_quantity,
            )?;
            if identity.client_order_id > 0 && newly_attributed_quantity > 0.0 {
                attributed_quantity_to_commit.insert(update.order_id, attributed_quantity);
            }

            let exchange_cumulative = (update.quantity - update.remaining)
                .max(0.0)
                .min(update.quantity);
            expected_cumulative_to_commit
                .entry(update.order_id)
                .and_modify(|current: &mut f64| *current = current.max(exchange_cumulative))
                .or_insert(exchange_cumulative);
            let epsilon = (identity.orig_size.abs() * 1.0e-9).max(1.0e-12);
            let defer_terminal = identity.client_order_id > 0
                && !update.is_active
                && exchange_cumulative > attributed_quantity + epsilon;
            final_order_state.insert(
                update.order_id,
                if update.is_active {
                    None
                } else if defer_terminal {
                    Some(update.clone())
                } else {
                    // A terminal row with no pending value is complete and may
                    // release the active-order pin after this frame commits.
                    Some(ParsedOrderUpdate {
                        identity: None,
                        ..update.clone()
                    })
                },
            );

            let is_new_update = !self.seen_order_updates.contains(&update.dedup_key)
                && frame_dedup.insert(update.dedup_key.clone());
            if is_new_update {
                dedup_to_commit.push(update.dedup_key.clone());
            }
            let deferred_before = self.pending_terminal_by_oid.contains_key(&update.order_id);
            let lifecycle = if (is_new_update || deferred_before)
                && !defer_terminal
                && !(identity.client_order_id > 0 && update.order_status == OrderStatus::Filled)
            {
                // userFills is the only fill-quantity source for internal
                // orders. Exchange-owned orders retain the factual venue
                // cumulative carried by orderUpdates.
                let emitted_cumulative = if identity.client_order_id > 0 {
                    attributed_quantity
                } else {
                    exchange_cumulative
                };
                self.order_update_message(identity, update, emitted_cumulative)
            } else {
                None
            };

            if is_new_update || !attributed_fills.is_empty() || lifecycle.is_some() {
                // A terminal lifecycle must never delete the strategy order
                // before all factual userFills already observed for that oid.
                if update.is_active {
                    if let Some(msg) = lifecycle {
                        output.push(msg);
                    }
                    output.extend(attributed_fills);
                } else {
                    output.extend(attributed_fills);
                    if let Some(msg) = lifecycle {
                        output.push(msg);
                    }
                }
            }
        }

        // No operation below can fail. Commit in frame order so a batch with
        // multiple lifecycle states for one oid leaves the final state pinned
        // exactly when the final update is active.
        for dedup_key in dedup_to_commit {
            self.seen_order_updates.insert(dedup_key);
        }
        let mut drained_pending = HashSet::new();
        for index in accepted_indices {
            let update = &parsed[index];
            let Some(identity) = update.identity.as_ref() else {
                continue;
            };
            if update.is_active {
                self.active_order_ids.insert(update.order_id);
            }
            self.cache_order(update.order_id, identity.clone());
            if drained_pending.insert(update.order_id) {
                if let Some(pending) = self.pending_fills.remove(&update.order_id) {
                    self.pending_fill_count = self.pending_fill_count.saturating_sub(pending.len());
                }
                if let Some(retained) = self.late_attribution_fills.remove(&update.order_id) {
                    self.late_attribution_fill_count = self
                        .late_attribution_fill_count
                        .saturating_sub(retained.len());
                }
                if identity.client_order_id == 0 {
                    self.unrecoverable_unattributed_oids
                        .remove(&update.order_id);
                }
            }
        }
        self.order_lifecycle_by_oid.extend(lifecycle_to_commit);
        self.attributed_fill_quantity_by_oid
            .extend(attributed_quantity_to_commit);
        for (order_id, expected) in expected_cumulative_to_commit {
            self.expected_fill_cumulative_by_oid
                .entry(order_id)
                .and_modify(|current| *current = current.max(expected))
                .or_insert(expected);
        }
        for (order_id, state) in final_order_state {
            match state {
                None => {
                    self.pending_terminal_by_oid.remove(&order_id);
                    self.active_order_ids.insert(order_id);
                }
                Some(update) if update.identity.is_some() => {
                    self.pending_terminal_by_oid.insert(order_id, update);
                    self.active_order_ids.insert(order_id);
                }
                Some(_) => {
                    self.pending_terminal_by_oid.remove(&order_id);
                    self.active_order_ids.remove(&order_id);
                    self.evict_inactive_orders_to_capacity();
                }
            }
        }
        Ok(output)
    }

    fn parse_order_update(&self, row: &Value) -> Result<ParsedOrderUpdate> {
        let identity = self
            .parse_order_identity(row)?
            .map(|(_, identity)| identity);
        let order = row.get("order").context("orderUpdates row missing order")?;
        let order_id = required_i64(order, "oid")?;
        if order_id <= 0 {
            anyhow::bail!("Hyperliquid oid must be positive");
        }
        let status = required_str(row, "status")?;
        let status_timestamp = required_i64(row, "statusTimestamp")?;
        let remaining = required_f64(order, "sz")?;
        let quantity = required_f64(order, "origSz")?;
        let price = required_f64(order, "limitPx")?;
        validate_nonnegative_finite("remaining size", remaining)?;
        validate_positive_finite("original size", quantity)?;
        validate_positive_finite("limit price", price)?;
        let epsilon = (quantity.abs() * 1.0e-9).max(1.0e-12);
        if remaining > quantity + epsilon {
            anyhow::bail!(
                "Hyperliquid order remaining size exceeds origSz: oid={} remaining={} origSz={}",
                order_id,
                remaining,
                quantity
            );
        }
        let cumulative = (quantity - remaining).max(0.0).min(quantity);
        let (order_status, execution_type) = map_order_status(status, cumulative)
            .with_context(|| format!("unsupported Hyperliquid order status {status}"))?;
        let side = parse_side(required_str(order, "side")?)?;
        let dedup_key = format!("{order_id}:{status_timestamp}");
        Ok(ParsedOrderUpdate {
            order_id,
            identity,
            status: status.to_string(),
            status_timestamp,
            remaining,
            quantity,
            price,
            side,
            order_status,
            execution_type,
            dedup_key,
            is_active: is_active_order_status(status),
        })
    }

    fn parse_order_identity(&self, row: &Value) -> Result<Option<(i64, OrderIdentity)>> {
        let order = row.get("order").context("order row missing order object")?;
        self.parse_order_identity_from_order(order)
    }

    fn parse_order_identity_from_order(
        &self,
        order: &Value,
    ) -> Result<Option<(i64, OrderIdentity)>> {
        let order_id = required_i64(order, "oid")?;
        if order_id <= 0 {
            anyhow::bail!("Hyperliquid oid must be positive");
        }
        let cloid = match order.get("cloid") {
            None | Some(Value::Null) => "",
            Some(Value::String(value)) => value,
            Some(_) => anyhow::bail!("Hyperliquid order cloid must be a string or null"),
        };
        let client_order_id = hyperliquid_client_order_id_from_cloid(cloid).unwrap_or(0);
        let reported_order_type_text = optional_order_string(order, "orderType")?;
        let reported_time_in_force_text = optional_order_string(order, "tif")?;
        let reported_order_type = reported_order_type_text.and_then(parse_hyperliquid_order_type);
        let reported_time_in_force =
            reported_time_in_force_text.and_then(parse_hyperliquid_time_in_force);
        let reported_intent_unrepresentable = (reported_order_type_text.is_some()
            && reported_order_type.is_none())
            || (reported_time_in_force_text.is_some() && reported_time_in_force.is_none());
        let cached = self.orders.get(&order_id);
        if let (Some(reported), Some(cached)) = (
            reported_order_type,
            cached.and_then(|value| value.order_type),
        ) {
            if reported != cached {
                anyhow::bail!(
                    "Hyperliquid oid {order_id} changed orderType from {cached:?} to {reported:?}"
                );
            }
        }
        if let (Some(reported), Some(cached)) = (
            reported_time_in_force,
            cached.and_then(|value| value.time_in_force),
        ) {
            if reported != cached {
                anyhow::bail!(
                    "Hyperliquid oid {order_id} changed tif from {cached:?} to {reported:?}"
                );
            }
        }
        let coin = required_str(order, "coin")?;
        let orig_size = required_f64(order, "origSz")?;
        validate_positive_finite("original size", orig_size)?;
        let instrument = self.catalog.resolve(coin).cloned().with_context(|| {
            format!("unknown Hyperliquid order coin {coin:?}; account metadata refresh is required")
        })?;
        Ok(Some((
            order_id,
            OrderIdentity {
                client_order_id,
                cloid: cloid.to_string(),
                instrument,
                orig_size,
                order_type: reported_order_type
                    .or_else(|| cached.and_then(|value| value.order_type)),
                time_in_force: reported_time_in_force
                    .or_else(|| cached.and_then(|value| value.time_in_force)),
                intent_unrepresentable: reported_intent_unrepresentable
                    || cached.is_some_and(|value| value.intent_unrepresentable),
            },
        )))
    }

    fn process_user_fills(
        &mut self,
        root: &Value,
        now_ms: i64,
        snapshot_context: FillSnapshotContext,
    ) -> Result<Vec<Bytes>> {
        let data = root.get("data").context("userFills missing data")?;
        self.validate_user(data)?;
        validate_optional_snapshot_flag(data, "userFills")?;
        let is_snapshot = data
            .get("isSnapshot")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let rows = data
            .get("fills")
            .and_then(Value::as_array)
            .context("userFills fills must be an array")?;
        let mut fills = rows
            .iter()
            .map(|row| parse_fill(row, now_ms))
            .collect::<Result<Vec<_>>>()?;
        fills.sort_by(|left, right| {
            left.time
                .cmp(&right.time)
                .then_with(|| left.coin.cmp(&right.coin))
                .then_with(|| left.tid.cmp(&right.tid))
                .then_with(|| left.order_id.cmp(&right.order_id))
                .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
        });
        let latest_fill_time = fills.last().map(|fill| fill.time);

        // Resolve every instrument before mutating dedup, cumulative-fill, or
        // pending-fill state. A newly listed coin must make the complete frame
        // retryable after metadata refresh/reconnect rather than being consumed.
        for fill in &fills {
            self.catalog.resolve(&fill.coin).with_context(|| {
                format!(
                    "unknown Hyperliquid fill coin {:?}; account metadata refresh is required",
                    fill.coin
                )
            })?;
        }

        let mut snapshot_initial_cumulative = HashMap::new();
        if is_snapshot {
            let mut snapshot_qty_by_oid: HashMap<i64, f64> = HashMap::new();
            for fill in &fills {
                let total = snapshot_qty_by_oid.entry(fill.order_id).or_default();
                *total += fill.quantity;
                validate_nonnegative_finite("snapshot fill quantity total", *total)?;
            }
            for (order_id, snapshot_qty) in snapshot_qty_by_oid {
                if self.fill_cumulative_by_oid.contains_key(&order_id) {
                    continue;
                }
                let anchor = self
                    .historical_fill_anchor_by_oid
                    .get(&order_id)
                    .copied()
                    .unwrap_or(0.0);
                snapshot_initial_cumulative.insert(order_id, (anchor - snapshot_qty).max(0.0));
            }
        }

        let baseline_only = is_snapshot
            && snapshot_context == FillSnapshotContext::Initial
            && self.fill_snapshot_policy == FillSnapshotPolicy::Ignore;

        struct PreparedFill {
            key: String,
            fill: ParsedFill,
            direct_message: Option<Bytes>,
            queue_unattributed: bool,
            attributed_total_after: Option<f64>,
            terminal_message: Option<Bytes>,
            close_terminal: bool,
        }

        // Precompute every fallible operation against frame-local shadows.
        // The commit loop below only mutates maps/queues and cannot reject a
        // later row after consuming an earlier one.
        let mut frame_seen = HashSet::with_capacity(fills.len());
        let mut cumulative_shadow: HashMap<i64, f64> = HashMap::new();
        let mut attributed_shadow: HashMap<i64, f64> = HashMap::new();
        let mut terminals_closed = HashSet::new();
        let mut prepared = Vec::with_capacity(fills.len());
        for mut fill in fills {
            let fill_key = fill_identity(&fill);
            if self.seen_fills.contains(&fill_key) || !frame_seen.insert(fill_key.clone()) {
                continue;
            }

            let previous = cumulative_shadow
                .get(&fill.order_id)
                .copied()
                .or_else(|| self.fill_cumulative_by_oid.get(&fill.order_id).copied())
                .or_else(|| snapshot_initial_cumulative.get(&fill.order_id).copied())
                .or_else(|| {
                    self.historical_fill_anchor_by_oid
                        .get(&fill.order_id)
                        .copied()
                })
                .unwrap_or(0.0);
            let cumulative = previous + fill.quantity;
            validate_positive_finite("cumulative fill size", cumulative)?;
            fill.cumulative_filled_quantity = cumulative;
            cumulative_shadow.insert(fill.order_id, cumulative);

            let identity = self.orders.get(&fill.order_id).cloned();
            let direct_message = if baseline_only {
                None
            } else if let Some(identity) = identity.as_ref() {
                self.fill_message(Some(identity), &fill)?
            } else {
                None
            };
            if let Some(message) = direct_message.as_ref() {
                validate_pm_event_size("userFills", message)?;
            }
            let attributed_total_after = if direct_message.is_some() {
                let previous = attributed_shadow
                    .get(&fill.order_id)
                    .copied()
                    .or_else(|| {
                        self.attributed_fill_quantity_by_oid
                            .get(&fill.order_id)
                            .copied()
                    })
                    .unwrap_or(0.0);
                let total = previous + fill.quantity;
                validate_nonnegative_finite("attributed Hyperliquid fill quantity", total)?;
                attributed_shadow.insert(fill.order_id, total);
                Some(total)
            } else {
                None
            };

            let factual = attributed_shadow
                .get(&fill.order_id)
                .copied()
                .or_else(|| {
                    self.attributed_fill_quantity_by_oid
                        .get(&fill.order_id)
                        .copied()
                })
                .unwrap_or(0.0);
            let mut terminal_message = None;
            let mut close_terminal = false;
            let pending_terminal = if terminals_closed.contains(&fill.order_id) {
                None
            } else {
                self.pending_terminal_by_oid.get(&fill.order_id).cloned()
            };
            if let (Some(identity), Some(terminal)) = (identity.as_ref(), pending_terminal) {
                let expected = (terminal.quantity - terminal.remaining)
                    .max(0.0)
                    .min(terminal.quantity);
                let epsilon = (identity.orig_size.abs() * 1.0e-9).max(1.0e-12);
                if factual + epsilon >= expected {
                    if terminal.order_status != OrderStatus::Filled {
                        if let Some(message) =
                            self.order_update_message(identity, &terminal, factual)
                        {
                            validate_pm_event_size("userFills terminal order", &message)?;
                            terminal_message = Some(message);
                        }
                    }
                    close_terminal = true;
                    terminals_closed.insert(fill.order_id);
                }
            }

            prepared.push(PreparedFill {
                key: fill_key,
                fill,
                direct_message,
                queue_unattributed: !baseline_only && identity.is_none(),
                attributed_total_after,
                terminal_message,
                close_terminal,
            });
        }

        // Queue overflow emits the oldest pending fill as external evidence.
        // Prebuild every message that can be selected before the commit starts.
        let queued_in_frame = prepared
            .iter()
            .filter(|fill| fill.queue_unattributed)
            .count();
        let may_evict_pending = queued_in_frame > 0
            && self.pending_fill_count.saturating_add(queued_in_frame)
                > DEFAULT_PENDING_FILL_CAPACITY;
        let mut external_messages = HashMap::new();
        if may_evict_pending || self.pending_fill_count >= DEFAULT_PENDING_FILL_CAPACITY {
            for fill in self.pending_fills.values().flatten().chain(
                prepared
                    .iter()
                    .filter(|fill| fill.queue_unattributed)
                    .map(|fill| &fill.fill),
            ) {
                if let Some(message) = self.fill_message(None, fill)? {
                    validate_pm_event_size("userFills external overflow", &message)?;
                    external_messages.insert(fill_identity(fill), message);
                }
            }
        }

        // Commit only after all rows and all possible outputs have passed.
        for (order_id, cumulative) in snapshot_initial_cumulative {
            self.initialize_fill_cumulative(order_id, cumulative);
        }
        let mut output = Vec::new();
        for fill in prepared {
            self.seen_fills.insert(fill.key);
            self.commit_fill_cumulative(&fill.fill);

            if let Some(message) = fill.direct_message {
                output.push(message);
            } else if fill.queue_unattributed {
                if let Some(evicted) = self.queue_pending_fill(fill.fill.clone()) {
                    let evicted_key = fill_identity(&evicted);
                    self.journal_unattributed_fill(evicted);
                    output.push(
                        external_messages
                            .remove(&evicted_key)
                            .expect("external overflow message was precomputed"),
                    );
                }
            }
            if let Some(total) = fill.attributed_total_after {
                self.attributed_fill_quantity_by_oid
                    .insert(fill.fill.order_id, total);
            }

            self.expected_fill_cumulative_by_oid
                .entry(fill.fill.order_id)
                .and_modify(|current| *current = current.max(fill.fill.cumulative_filled_quantity))
                .or_insert(fill.fill.cumulative_filled_quantity);

            if fill.close_terminal {
                if let Some(message) = fill.terminal_message {
                    output.push(message);
                }
                self.pending_terminal_by_oid.remove(&fill.fill.order_id);
                self.active_order_ids.remove(&fill.fill.order_id);
                self.evict_inactive_orders_to_capacity();
            }
        }
        if let Some(latest_fill_time) = latest_fill_time {
            self.fact_watermarks.fill_time_ms = Some(
                self.fact_watermarks
                    .fill_time_ms
                    .map_or(latest_fill_time, |current| current.max(latest_fill_time)),
            );
        }
        Ok(output)
    }

    fn process_user_twap_slice_fills(
        &mut self,
        root: &Value,
        now_ms: i64,
        snapshot_context: FillSnapshotContext,
    ) -> Result<Vec<Bytes>> {
        // The slice channel shares fill/cumulative state with userFills. Apply
        // the complete mirror frame on a clone so an association conflict or a
        // malformed later nested fill cannot consume an earlier fill key.
        let mut candidate = self.clone();
        let events =
            candidate.process_user_twap_slice_fills_inner(root, now_ms, snapshot_context)?;
        *self = candidate;
        Ok(events)
    }

    fn process_user_twap_slice_fills_inner(
        &mut self,
        root: &Value,
        now_ms: i64,
        snapshot_context: FillSnapshotContext,
    ) -> Result<Vec<Bytes>> {
        let data = root
            .get("data")
            .context("userTwapSliceFills missing data")?;
        self.validate_user(data)?;
        validate_optional_snapshot_flag(data, "userTwapSliceFills")?;
        let is_snapshot = data
            .get("isSnapshot")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let rows = data
            .get("twapSliceFills")
            .and_then(Value::as_array)
            .context("userTwapSliceFills twapSliceFills must be an array")?;

        struct PreparedSlice {
            fill: ParsedFill,
            fill_value: Value,
            association: HyperliquidTwapSliceFillMsg,
            stable_key: [u8; 32],
            content_digest: [u8; 32],
        }

        let mut parsed = Vec::with_capacity(rows.len());
        for row in rows {
            let twap_id = required_i64(row, "twapId")?;
            if twap_id < 0 {
                anyhow::bail!("Hyperliquid TWAP slice twapId must be nonnegative");
            }
            let mut fill_value = row
                .get("fill")
                .cloned()
                .context("Hyperliquid TWAP slice row missing fill")?;
            let fill_object = fill_value
                .as_object_mut()
                .context("Hyperliquid TWAP slice fill must be an object")?;
            if let Some(inner_twap_id) = optional_i64(fill_object.get("twapId")) {
                if inner_twap_id != twap_id {
                    anyhow::bail!(
                        "Hyperliquid TWAP slice outer/inner twapId conflict: {twap_id} != {inner_twap_id}"
                    );
                }
            } else if fill_object
                .get("twapId")
                .is_some_and(|value| !value.is_null())
            {
                anyhow::bail!("Hyperliquid TWAP slice fill twapId must be an integer or null");
            }
            fill_object.insert("twapId".to_string(), Value::from(twap_id));
            let fill = parse_fill(&fill_value, now_ms)?;
            let instrument = self.catalog.resolve(&fill.coin).cloned().with_context(|| {
                format!(
                    "unknown Hyperliquid TWAP slice coin {:?}; account metadata refresh is required",
                    fill.coin
                )
            })?;
            let association = HyperliquidTwapSliceFillMsg::create(
                instrument.venue.to_u8(),
                fill.time,
                fill.coin.clone(),
                instrument.symbol,
                fill.order_id,
                fill.tid,
                fill.transaction_hash.clone(),
                twap_id,
            );
            let stable_key = association.stable_venue_key();
            let content_digest = twap_slice_row_digest(&association, &fill);
            parsed.push(PreparedSlice {
                fill,
                fill_value,
                association,
                stable_key,
                content_digest,
            });
        }
        parsed.sort_by(|left, right| {
            left.fill
                .time
                .cmp(&right.fill.time)
                .then_with(|| left.fill.coin.cmp(&right.fill.coin))
                .then_with(|| left.fill.tid.cmp(&right.fill.tid))
                .then_with(|| left.association.twap_id.cmp(&right.association.twap_id))
        });

        let mut frame_rows = Vec::with_capacity(parsed.len());
        let mut frame_seen = HashMap::with_capacity(parsed.len());
        for row in parsed {
            if let Some(existing) = frame_seen.get(&row.stable_key) {
                if existing != &row.content_digest {
                    anyhow::bail!(
                        "conflicting Hyperliquid TWAP slice rows for one venue fill identity"
                    );
                }
                continue;
            }
            if let Some(existing) = self.seen_twap_slice_fills.get(&row.stable_key) {
                if existing != &row.content_digest {
                    anyhow::bail!(
                        "conflicting recovered Hyperliquid TWAP slice row for one venue fill identity"
                    );
                }
            }
            frame_seen.insert(row.stable_key, row.content_digest);
            frame_rows.push(row);
        }

        // Only associations not already committed need to re-enter the shared
        // fill path. An association is committed atomically after that path,
        // so this cannot hide an unprocessed fill.
        let fill_rows = frame_rows
            .iter()
            .filter(|row| !self.seen_twap_slice_fills.contains_key(&row.stable_key))
            .map(|row| row.fill_value.clone())
            .collect::<Vec<_>>();
        let mut output = self.process_user_fills(
            &json!({
                "channel": "userFills",
                "data": {
                    "user": self.user.clone(),
                    "isSnapshot": is_snapshot,
                    "fills": fill_rows,
                },
            }),
            now_ms,
            snapshot_context,
        )?;

        let latest_time = frame_rows.iter().map(|row| row.fill.time).max();
        for row in frame_rows {
            if self.seen_twap_slice_fills.contains_key(&row.stable_key) {
                continue;
            }
            let venue = TradingVenue::from_u8(row.association.venue)
                .context("processor produced an invalid Hyperliquid TWAP slice venue")?;
            let event = self.wrap_venue(
                venue,
                BasicAccountEventType::HyperliquidTwapSliceFill,
                row.association.to_bytes(),
            );
            validate_pm_event_size("userTwapSliceFills association", &event)?;
            output.push(event);
            self.seen_twap_slice_fills
                .insert(row.stable_key, row.content_digest);
            self.seen_twap_slice_fill_age.push_back(row.stable_key);
            while self.seen_twap_slice_fills.len() > DEFAULT_DEDUP_CAPACITY {
                if let Some(oldest) = self.seen_twap_slice_fill_age.pop_front() {
                    self.seen_twap_slice_fills.remove(&oldest);
                } else {
                    break;
                }
            }
        }
        if let Some(latest_time) = latest_time {
            self.fact_watermarks.twap_slice_time_ms = Some(
                self.fact_watermarks
                    .twap_slice_time_ms
                    .map_or(latest_time, |current| current.max(latest_time)),
            );
        }
        Ok(output)
    }

    fn process_user_twap_history(&mut self, root: &Value) -> Result<Vec<Bytes>> {
        let mut candidate = self.clone();
        let events = candidate.process_user_twap_history_inner(root)?;
        *self = candidate;
        Ok(events)
    }

    fn process_user_twap_history_inner(&mut self, root: &Value) -> Result<Vec<Bytes>> {
        let data = root.get("data").context("userTwapHistory missing data")?;
        self.validate_user(data)?;
        validate_optional_snapshot_flag(data, "userTwapHistory")?;
        let rows = data
            .get("history")
            .and_then(Value::as_array)
            .context("userTwapHistory history must be an array")?;
        let mut parsed = rows
            .iter()
            .map(|row| parse_twap_history(row, &self.user))
            .collect::<Result<Vec<_>>>()?;
        parsed.sort_by(|left, right| {
            left.event_time
                .cmp(&right.event_time)
                .then_with(|| left.twap_id.cmp(&right.twap_id))
                .then_with(|| left.timestamp.cmp(&right.timestamp))
                .then_with(|| left.status.cmp(&right.status))
        });

        let mut frame_seen = HashMap::with_capacity(parsed.len());
        let mut prepared = Vec::with_capacity(parsed.len());
        for message in parsed {
            let instrument = self.catalog.resolve(&message.coin).cloned().with_context(|| {
                format!(
                    "unknown Hyperliquid TWAP history coin {:?}; account metadata refresh is required",
                    message.coin
                )
            })?;
            let stable_key = message.stable_venue_key();
            let content_digest = message.content_digest();
            if let Some(existing) = frame_seen.get(&stable_key) {
                if existing != &content_digest {
                    anyhow::bail!(
                        "conflicting Hyperliquid TWAP history rows for one lifecycle identity"
                    );
                }
                continue;
            }
            if let Some(existing) = self.seen_twap_history.get(&stable_key) {
                if existing != &content_digest {
                    anyhow::bail!(
                        "conflicting recovered Hyperliquid TWAP history row for one lifecycle identity"
                    );
                }
            }
            frame_seen.insert(stable_key, content_digest);
            let event = self.wrap_venue(
                instrument.venue,
                BasicAccountEventType::HyperliquidTwapHistory,
                message.to_bytes(),
            );
            validate_pm_event_size("userTwapHistory", &event)?;
            prepared.push((message, event, stable_key, content_digest));
        }

        let latest_time = prepared.iter().map(|row| row.0.event_time).max();
        let mut output = Vec::with_capacity(prepared.len());
        for (_, event, stable_key, content_digest) in prepared {
            if self.seen_twap_history.contains_key(&stable_key) {
                continue;
            }
            output.push(event);
            self.seen_twap_history.insert(stable_key, content_digest);
            self.seen_twap_history_age.push_back(stable_key);
            while self.seen_twap_history.len() > DEFAULT_DEDUP_CAPACITY {
                if let Some(oldest) = self.seen_twap_history_age.pop_front() {
                    self.seen_twap_history.remove(&oldest);
                } else {
                    break;
                }
            }
        }
        if let Some(latest_time) = latest_time {
            self.fact_watermarks.twap_history_time_s = Some(
                self.fact_watermarks
                    .twap_history_time_s
                    .map_or(latest_time, |current| current.max(latest_time)),
            );
        }
        Ok(output)
    }

    fn process_user_fundings(&mut self, root: &Value) -> Result<Vec<Bytes>> {
        let data = root.get("data").context("userFundings missing data")?;
        self.validate_user(data)?;
        validate_optional_snapshot_flag(data, "userFundings")?;
        let rows = data
            .get("fundings")
            .and_then(Value::as_array)
            .context("userFundings fundings must be an array")?;
        let mut parsed = rows.iter().map(parse_funding).collect::<Result<Vec<_>>>()?;
        parsed.sort_by(|left, right| {
            left.time
                .cmp(&right.time)
                .then_with(|| left.coin.cmp(&right.coin))
                .then_with(|| left.usdc.cmp(&right.usdc))
                .then_with(|| left.szi.cmp(&right.szi))
                .then_with(|| left.funding_rate.cmp(&right.funding_rate))
                .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
        });
        let latest_funding_time = parsed.last().map(|funding| funding.time);

        // Initial and reconnect snapshots are factual history. Emit every row
        // not already seen on either redundant path; never turn funding into a
        // synthetic balance delta because the complete state already reflects it.
        let mut frame_index: HashMap<[u8; 32], usize> = HashMap::with_capacity(parsed.len());
        let mut candidates: Vec<(HyperliquidFundingMsg, Option<String>)> =
            Vec::with_capacity(parsed.len());
        for funding in parsed {
            let msg = HyperliquidFundingMsg::create(
                funding.time,
                funding.coin,
                funding.usdc,
                funding.szi,
                funding.funding_rate,
            )
            .with_transaction_hash(funding.transaction_hash);
            let identity = msg.stable_venue_key();
            if let Some(index) = frame_index.get(&identity).copied() {
                let (_, existing): &mut (HyperliquidFundingMsg, Option<String>) =
                    &mut candidates[index];
                match (existing.as_deref(), msg.transaction_hash.as_deref()) {
                    (Some(left), Some(right)) if left != right => {
                        anyhow::bail!(
                            "conflicting Hyperliquid funding hashes for one economic fact: {left} != {right}"
                        );
                    }
                    (None, Some(_)) => {
                        *existing = msg.transaction_hash.clone();
                        candidates[index].0 = msg;
                    }
                    _ => {}
                }
            } else {
                frame_index.insert(identity, candidates.len());
                candidates.push((msg.clone(), msg.transaction_hash.clone()));
            }
        }

        let mut output = Vec::with_capacity(candidates.len());
        let mut changes = Vec::with_capacity(candidates.len());
        for (msg, transaction_hash) in candidates {
            let identity = msg.stable_venue_key();
            let should_emit = match self.seen_fundings.get(&identity) {
                None => true,
                Some(None) => transaction_hash.is_some(),
                Some(Some(existing)) => match transaction_hash.as_deref() {
                    Some(current) if current != existing => {
                        anyhow::bail!(
                            "conflicting recovered Hyperliquid funding hash for one economic fact: {existing} != {current}"
                        );
                    }
                    _ => false,
                },
            };
            if !should_emit {
                continue;
            }
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::HyperliquidFunding,
                msg.to_bytes(),
            ));
            changes.push((identity, transaction_hash));
        }
        for (identity, transaction_hash) in changes {
            if !self.seen_fundings.contains_key(&identity) {
                self.seen_funding_age.push_back(identity);
            }
            self.seen_fundings.insert(identity, transaction_hash);
            while self.seen_fundings.len() > DEFAULT_DEDUP_CAPACITY {
                if let Some(oldest) = self.seen_funding_age.pop_front() {
                    self.seen_fundings.remove(&oldest);
                } else {
                    break;
                }
            }
        }
        if let Some(latest_funding_time) = latest_funding_time {
            self.fact_watermarks.funding_time_ms = Some(
                self.fact_watermarks
                    .funding_time_ms
                    .map_or(latest_funding_time, |current| {
                        current.max(latest_funding_time)
                    }),
            );
        }
        Ok(output)
    }

    fn process_user_non_funding_ledger_updates(&mut self, root: &Value) -> Result<Vec<Bytes>> {
        let data = root
            .get("data")
            .context("userNonFundingLedgerUpdates missing data")?;
        self.validate_user(data)?;
        validate_optional_snapshot_flag(data, "userNonFundingLedgerUpdates")?;
        let rows = data
            .get("nonFundingLedgerUpdates")
            .and_then(Value::as_array)
            .context("userNonFundingLedgerUpdates nonFundingLedgerUpdates must be an array")?;
        let mut parsed = rows
            .iter()
            .map(parse_ledger_update)
            .collect::<Result<Vec<_>>>()?;
        parsed.sort_by(|left, right| {
            left.time
                .cmp(&right.time)
                .then_with(|| left.transaction_hash.cmp(&right.transaction_hash))
                .then_with(|| left.delta_type.cmp(&right.delta_type))
                .then_with(|| left.delta_json.cmp(&right.delta_json))
        });
        let latest_ledger_time = parsed.last().map(|update| update.time);

        let mut frame_seen = HashSet::with_capacity(parsed.len());
        let mut output = Vec::with_capacity(parsed.len());
        let mut identities = Vec::with_capacity(parsed.len());
        for update in parsed {
            let msg = HyperliquidLedgerMsg::create(
                update.time,
                update.transaction_hash,
                update.delta_type,
                update.delta_json,
            );
            let identity = msg.stable_venue_key();
            if !frame_seen.insert(identity) || self.seen_ledger_updates.contains(&identity) {
                continue;
            }
            // The ledger is account-wide. The perp scope is also the factual
            // replay control scope for Standard accounts; Unified has one scope.
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::HyperliquidLedger,
                msg.to_bytes(),
            ));
            identities.push(identity);
        }
        for event in &output {
            validate_pm_event_size("userNonFundingLedgerUpdates", event)?;
        }
        for identity in identities {
            self.seen_ledger_updates.insert(identity);
        }
        if let Some(latest_ledger_time) = latest_ledger_time {
            self.fact_watermarks.ledger_time_ms = Some(
                self.fact_watermarks
                    .ledger_time_ms
                    .map_or(latest_ledger_time, |current| {
                        current.max(latest_ledger_time)
                    }),
            );
        }
        Ok(output)
    }

    fn fill_message(
        &self,
        identity: Option<&OrderIdentity>,
        fill: &ParsedFill,
    ) -> Result<Option<Bytes>> {
        let fill_instrument = self.catalog.resolve(&fill.coin).with_context(|| {
            format!(
                "unknown Hyperliquid fill coin {:?}; account metadata refresh is required",
                fill.coin
            )
        })?;
        if let Some(identity) = identity {
            if fill_instrument != &identity.instrument {
                anyhow::bail!(
                    "Hyperliquid fill/order instrument mismatch: oid={} fill={} order={}",
                    fill.order_id,
                    fill_instrument.symbol,
                    identity.instrument.symbol
                );
            }
        }
        let trade_id = compact_trade_id(fill);
        let client_order_id = identity.map(|value| value.client_order_id).unwrap_or(0);
        let cloid = identity
            .map(|value| value.cloid.clone())
            .unwrap_or_default();
        let msg = HyperliquidBasicFillMsg::create(
            fill_instrument.venue.to_u8(),
            fill.time,
            fill.time,
            fill_instrument.symbol.clone(),
            fill.order_id,
            client_order_id,
            cloid,
            &trade_id,
            fill.tid,
            fill.transaction_hash.clone(),
            fill.liquidation_method.clone(),
            fill.side.to_u8(),
            !fill.crossed,
            fill.price,
            fill.quantity,
            fill.cumulative_filled_quantity,
            identity.map(|identity| {
                let epsilon = (identity.orig_size.abs() * 1.0e-9).max(1.0e-12);
                if fill.cumulative_filled_quantity + epsilon >= identity.orig_size {
                    OrderStatus::Filled.to_u8()
                } else {
                    OrderStatus::PartiallyFilled.to_u8()
                }
            }),
        )
        .with_venue_audit_fields(
            Some(fill.coin.clone()),
            fill.start_position.clone(),
            fill.dir.clone(),
            fill.closed_pnl.clone(),
            fill.fee.clone(),
            fill.fee_token.clone(),
            fill.builder_fee.clone(),
            fill.twap_id,
            fill.liquidated_user.clone(),
            fill.liquidation_mark_price.clone(),
        );
        Ok(Some(self.wrap_venue(
            fill_instrument.venue,
            BasicAccountEventType::HyperliquidFill,
            msg.to_bytes(),
        )))
    }

    fn order_update_message(
        &self,
        identity: &OrderIdentity,
        update: &ParsedOrderUpdate,
        cumulative_filled_quantity: f64,
    ) -> Option<Bytes> {
        let (order_type, time_in_force) = identity.ipc_intent()?;
        let msg = HyperliquidBasicOrderMsg::create(
            identity.instrument.venue.to_u8(),
            update.status_timestamp,
            identity.instrument.symbol.clone(),
            update.order_id,
            identity.client_order_id,
            identity.cloid.clone(),
            update.side.to_u8(),
            order_type.to_u8(),
            time_in_force.to_u8(),
            update.execution_type.to_u8(),
            update.order_status.to_u8(),
            update.price,
            update.quantity,
            cumulative_filled_quantity,
            update.status.clone(),
        );
        Some(self.wrap_venue(
            identity.instrument.venue,
            BasicAccountEventType::OrderUpdate,
            msg.to_bytes(),
        ))
    }

    /// Flush fills whose order update did not provide an attributable cloid
    /// within `max_wait_ms`. The binary drives this with a wall-clock tick.
    pub fn flush_pending_fills(&mut self, now_ms: i64, max_wait_ms: i64) -> Result<Vec<Bytes>> {
        let mut ready = Vec::new();
        let mut empty_oids = Vec::new();
        for (order_id, fills) in &mut self.pending_fills {
            while fills
                .front()
                .is_some_and(|fill| now_ms.saturating_sub(fill.received_at) >= max_wait_ms.max(0))
            {
                if let Some(fill) = fills.pop_front() {
                    ready.push(fill);
                    self.pending_fill_count = self.pending_fill_count.saturating_sub(1);
                }
            }
            if fills.is_empty() {
                empty_oids.push(*order_id);
            }
        }
        for order_id in empty_oids {
            self.pending_fills.remove(&order_id);
        }

        let mut output = Vec::with_capacity(ready.len());
        for fill in ready {
            self.journal_unattributed_fill(fill.clone());
            if let Some(msg) = self.fill_message(None, &fill)? {
                output.push(msg);
            }
        }
        Ok(output)
    }

    fn process_spot_state(&mut self, root: &Value, now_ms: i64) -> Result<Vec<Bytes>> {
        let data = root.get("data").context("spotState missing data")?;
        self.validate_user(data)?;
        let state = data
            .get("spotState")
            .context("spotState missing inner state")?;
        self.apply_spot_snapshot(state, now_ms)
    }

    /// Apply an Info `spotClearinghouseState` response or the inner `spotState`
    /// object from the websocket subscription.
    pub fn apply_spot_snapshot(&mut self, state: &Value, now_ms: i64) -> Result<Vec<Bytes>> {
        let borrowing = if self.mode == HyperliquidAccountMode::PortfolioMargin {
            let snapshot = self
                .borrow_snapshot
                .as_ref()
                .context("Hyperliquid PM spot snapshot requires a borrow/lend snapshot")?;
            snapshot.validate_freshness(now_ms)?;
            Some(snapshot)
        } else {
            None
        };
        // The venue already includes collateral haircuts, borrowing and caps
        // in this ratio. Never substitute borrow/lend healthFactor or a
        // single-DEX margin summary for a PM account's portfolio-wide risk.
        let portfolio_risk = if self.mode == HyperliquidAccountMode::PortfolioMargin {
            let ratio = required_f64(state, "portfolioMarginRatio")?;
            validate_nonnegative_finite("portfolio margin ratio", ratio)?;
            let safe_ratio = if ratio > 0.0 {
                (0.95 / ratio).min(1.0e12)
            } else {
                1.0e12
            };
            let mut risk = BasicAccountRiskMsg::ratio_only(now_ms, safe_ratio);
            risk.borrowed_usd = borrowing
                .and_then(|snapshot| snapshot.borrowed_usd)
                .unwrap_or(f64::NAN);
            Some(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::AccountRisk,
                risk.to_bytes(),
            ))
        } else {
            None
        };
        let rows = state
            .get("balances")
            .and_then(Value::as_array)
            .context("spotState balances must be an array")?;
        let mut parsed = rows
            .iter()
            .map(|row| {
                let wire_coin = required_str(row, "coin")?.to_string();
                let internal_coin = self
                    .catalog
                    .spot_balance_asset(&wire_coin)
                    .with_context(|| format!("unknown Hyperliquid spot balance coin: {wire_coin}"))?
                    .to_string();
                let token = required_i64(row, "token")?;
                if token < 0 {
                    anyhow::bail!("Hyperliquid spot balance token must be nonnegative: {token}");
                }
                let total = required_finite_decimal_string(row, "total")?;
                let total_value = required_f64(row, "total")?;
                let hold = required_finite_decimal_string(row, "hold")?;
                let entry_ntl = required_finite_decimal_string(row, "entryNtl")?;
                Ok(ParsedSpotBalance {
                    token,
                    wire_coin,
                    internal_coin,
                    total,
                    total_value,
                    hold,
                    entry_ntl,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        parsed.sort_by(|left, right| {
            left.token
                .cmp(&right.token)
                .then_with(|| left.wire_coin.cmp(&right.wire_coin))
        });
        let mut current = HashMap::with_capacity(rows.len());
        let mut current_by_token = HashMap::with_capacity(rows.len());
        for balance in &parsed {
            if self.catalog.spot_assets_by_token.get(&balance.token) != Some(&balance.internal_coin)
            {
                anyhow::bail!(
                    "Hyperliquid spot token/coin identity mismatch: {}",
                    balance.token
                );
            }
            if current
                .insert(balance.internal_coin.clone(), balance.total_value)
                .is_some()
            {
                anyhow::bail!(
                    "duplicate coin in Hyperliquid spotState: {}",
                    balance.internal_coin
                );
            }
            if current_by_token
                .insert(balance.token, balance.total_value)
                .is_some()
            {
                anyhow::bail!(
                    "duplicate token in Hyperliquid spotState: {}",
                    balance.token
                );
            }
        }

        let mut output = Vec::new();
        let mut symbols: Vec<_> = current
            .keys()
            .chain(self.balances.keys())
            .cloned()
            .collect();
        symbols.sort_unstable();
        symbols.dedup();
        if let Some(borrowing) = borrowing {
            symbols.extend(borrowing.by_asset.keys().cloned());
            for asset in borrowing.by_asset.keys() {
                current.entry(asset.clone()).or_insert(0.0);
            }
            symbols.sort_unstable();
            symbols.dedup();
        }
        for symbol in symbols {
            let next = current.get(&symbol).copied().unwrap_or(0.0);
            // spot total is net. Gross up using exactly the liability emitted
            // in this transaction, so the common wallet-minus-debt model
            // preserves the venue's net balance without double subtraction.
            let (principal, interest) = borrowing
                .and_then(|snapshot| snapshot.by_asset.get(&symbol).copied())
                .unwrap_or_default();
            let gross = next + principal + interest;
            if !gross.is_finite() {
                anyhow::bail!("Hyperliquid normalized wallet overflow for {symbol}");
            }
            let msg = BasicBalanceMsg::create(now_ms, symbol.clone(), gross);
            output.push(self.wrap_scope(
                self.mode.spot_scope(),
                BasicAccountEventType::BalanceUpdate,
                msg.to_bytes(),
            ));
            if let Some(borrowing) = borrowing {
                output.push(
                    self.wrap_scope(
                        self.mode.spot_scope(),
                        BasicAccountEventType::BorrowInterest,
                        BasicBorrowInterestMsg::create(
                            borrowing.observed_at_ms,
                            symbol,
                            principal,
                            interest,
                        )
                        .to_bytes(),
                    ),
                );
            }
        }
        for balance in parsed {
            let msg = HyperliquidSpotBalanceMsg::create(
                now_ms,
                balance.token,
                balance.wire_coin,
                balance.total,
                balance.hold,
                balance.entry_ntl,
            );
            output.push(self.wrap_scope(
                self.mode.spot_scope(),
                BasicAccountEventType::HyperliquidSpotBalance,
                msg.to_bytes(),
            ));
        }
        let complete_timestamp = borrowing.map_or(now_ms, |snapshot| snapshot.observed_at_ms);
        self.balances = current;
        self.spot_balances_by_token = current_by_token;
        self.spot_snapshot_seen = true;
        if self.mode == HyperliquidAccountMode::Unified {
            if let Some(risk) = self.maybe_emit_unified_risk(now_ms)? {
                output.push(risk);
            }
        }
        if let Some(risk) = portfolio_risk {
            output.push(risk);
        }
        output.push(self.snapshot_complete(TradingVenue::HyperliquidMargin, complete_timestamp));
        Ok(output)
    }

    fn process_clearinghouse_state(&mut self, root: &Value, now_ms: i64) -> Result<Vec<Bytes>> {
        let data = root
            .get("data")
            .context("clearinghouseState missing data")?;
        self.validate_user(data)?;
        if data.get("dex").and_then(Value::as_str).unwrap_or("") != "" {
            return Ok(Vec::new());
        }
        let state = data
            .get("clearinghouseState")
            .context("clearinghouseState missing inner state")?;
        self.apply_clearinghouse_snapshot(state, now_ms)
    }

    fn process_all_dexs_clearinghouse_state(
        &mut self,
        root: &Value,
        now_ms: i64,
    ) -> Result<Vec<Bytes>> {
        let data = root
            .get("data")
            .context("allDexsClearinghouseState missing data")?;
        self.validate_user(data)?;
        let states = data
            .get("clearinghouseStates")
            .context("allDexsClearinghouseState missing clearinghouseStates")?;
        self.apply_all_dexs_clearinghouse_snapshot(states, now_ms)
    }

    /// Apply the `clearinghouseStates` payload from an
    /// `allDexsClearinghouseState` response. API servers have emitted both a
    /// JSON record and an array of `[dex, state]` pairs. This is the complete
    /// perpetual state source for both Standard and Unified accounts. Unified
    /// balance truth still comes from `spotState`.
    pub fn apply_all_dexs_clearinghouse_snapshot(
        &mut self,
        clearinghouse_states: &Value,
        now_ms: i64,
    ) -> Result<Vec<Bytes>> {
        let mut states = clearinghouse_state_entries(clearinghouse_states)?;
        states.sort_unstable_by(|left, right| left.0.cmp(right.0));
        let received_dexes = states
            .iter()
            .map(|(dex, _)| (*dex).to_string())
            .collect::<HashSet<_>>();
        let expected_dexes = self.catalog.perp_dexes();
        let missing_dexes = expected_dexes
            .iter()
            .filter(|dex| !received_dexes.contains(*dex))
            .cloned()
            .collect::<Vec<_>>();
        if !missing_dexes.is_empty() || received_dexes.len() != expected_dexes.len() {
            anyhow::bail!(
                "Hyperliquid clearinghouseStates dex set is incomplete: expected={expected_dexes:?} received={received_dexes:?} missing={missing_dexes:?}"
            );
        }

        let mut current = HashMap::new();
        let mut parsed_dex_states = Vec::with_capacity(states.len());
        let mut cross_margin_by_token: HashMap<i64, f64> = HashMap::new();
        let mut isolated_margin_by_token: HashMap<i64, f64> = HashMap::new();
        for (dex, state) in states {
            let collateral_token =
                self.catalog
                    .collateral_token_for_dex(dex)
                    .with_context(|| {
                        format!("unknown Hyperliquid perp dex in account state: {dex:?}")
                    })?;
            let parsed_dex_state = parse_perp_dex_state(dex, collateral_token, state, now_ms)?;
            if self.mode == HyperliquidAccountMode::Unified {
                *cross_margin_by_token.entry(collateral_token).or_default() +=
                    parsed_dex_state.cross_maintenance_margin_used;
            }

            let rows = state
                .get("assetPositions")
                .and_then(Value::as_array)
                .with_context(|| {
                    format!("Hyperliquid dex {dex:?} assetPositions must be an array")
                })?;
            for row in rows {
                let position = row
                    .get("position")
                    .context("assetPositions row missing position")?;
                let coin = required_str(position, "coin")?;
                let instrument = self
                    .catalog
                    .resolve(coin)
                    .filter(|instrument| instrument.venue == TradingVenue::HyperliquidFutures)
                    .with_context(|| {
                        format!("unknown Hyperliquid perp coin in dex {dex:?} position: {coin}")
                    })?;
                let size = required_f64(position, "szi")?;
                let unrealized_pnl = required_f64(position, "unrealizedPnl")?;
                validate_finite("perp position size", size)?;
                validate_finite("perp unrealized pnl", unrealized_pnl)?;
                let size = size as f32;
                if !size.is_finite() {
                    anyhow::bail!("Hyperliquid perp position size exceeds f32 range");
                }
                if current
                    .insert(instrument.symbol.clone(), (size, unrealized_pnl))
                    .is_some()
                {
                    anyhow::bail!(
                        "duplicate internal position symbol across Hyperliquid perp dexs: {}",
                        instrument.symbol
                    );
                }

                let leverage = position
                    .get("leverage")
                    .context("Hyperliquid perp position missing leverage")?;
                match required_str(leverage, "type")? {
                    "cross" => {}
                    "isolated" => {
                        let margin_used = required_f64(position, "marginUsed")?;
                        validate_nonnegative_finite("isolated margin used", margin_used)?;
                        if self.mode == HyperliquidAccountMode::Unified {
                            *isolated_margin_by_token
                                .entry(collateral_token)
                                .or_default() += margin_used;
                        }
                    }
                    other => anyhow::bail!("unknown Hyperliquid leverage type {other}"),
                }
            }
            parsed_dex_states.push(parsed_dex_state);
        }
        for value in cross_margin_by_token
            .values()
            .chain(isolated_margin_by_token.values())
        {
            validate_nonnegative_finite("aggregated unified margin", *value)?;
        }

        let unified_risk = if self.mode == HyperliquidAccountMode::Unified {
            self.build_unified_risk(&cross_margin_by_token, &isolated_margin_by_token, now_ms)?
        } else {
            None
        };
        let standard_default_state = if self.mode == HyperliquidAccountMode::Standard {
            Some(
                parsed_dex_states
                    .iter()
                    .find(|state| state.message.dex.is_empty())
                    .context("Hyperliquid all-DEX state missing default dex")?,
            )
        } else {
            None
        };

        let mut output = Vec::new();
        let mut symbols: Vec<_> = current
            .keys()
            .chain(self.positions.keys())
            .cloned()
            .collect();
        symbols.sort_unstable();
        symbols.dedup();
        for symbol in symbols {
            let next = current.get(&symbol).copied().unwrap_or((0.0, 0.0));
            let msg = BasicPositionMsg::create(now_ms, symbol.clone(), 'N', next.0);
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::PositionUpdate,
                msg.to_bytes(),
            ));
            let msg = BasicUmUnrealizedMsg::create(now_ms, symbol, 'N', next.1);
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::UnrealizedPnlUpdate,
                msg.to_bytes(),
            ));
        }
        if let Some(risk) = unified_risk {
            output.push(risk);
        }
        if let Some(state) = standard_default_state {
            output.extend(self.standard_default_dex_events(state, now_ms));
        }
        for state in &parsed_dex_states {
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::HyperliquidPerpDexState,
                state.message.to_bytes(),
            ));
        }
        output.push(self.snapshot_complete(TradingVenue::HyperliquidFutures, now_ms));

        self.positions = current;
        self.unified_margin_by_token = (self.mode == HyperliquidAccountMode::Unified)
            .then_some((cross_margin_by_token, isolated_margin_by_token));
        Ok(output)
    }

    /// Apply an Info `clearinghouseState` response or the inner state from the
    /// websocket subscription. The response is a complete current snapshot.
    pub fn apply_clearinghouse_snapshot(
        &mut self,
        state: &Value,
        now_ms: i64,
    ) -> Result<Vec<Bytes>> {
        if self.mode != HyperliquidAccountMode::Standard {
            anyhow::bail!(
                "single-dex clearinghouseState is only meaningful for Hyperliquid standard accounts; use apply_all_dexs_clearinghouse_snapshot for unified accounts"
            );
        }
        let rows = state
            .get("assetPositions")
            .and_then(Value::as_array)
            .context("clearinghouseState assetPositions must be an array")?;
        let mut current = HashMap::with_capacity(rows.len());
        for row in rows {
            let position = row
                .get("position")
                .context("assetPositions row missing position")?;
            let coin = required_str(position, "coin")?;
            let instrument = self
                .catalog
                .resolve(coin)
                .filter(|instrument| instrument.venue == TradingVenue::HyperliquidFutures)
                .with_context(|| format!("unknown Hyperliquid perp coin in position: {coin}"))?;
            let size = required_f64(position, "szi")?;
            let unrealized_pnl = required_f64(position, "unrealizedPnl")?;
            validate_finite("perp position size", size)?;
            validate_finite("perp unrealized pnl", unrealized_pnl)?;
            let size = size as f32;
            if !size.is_finite() {
                anyhow::bail!("Hyperliquid perp position size exceeds f32 range");
            }
            if current
                .insert(instrument.symbol.clone(), (size, unrealized_pnl))
                .is_some()
            {
                anyhow::bail!("duplicate position in Hyperliquid clearinghouseState: {coin}");
            }
        }

        // Validate the complete default-DEX state before mutating cached state
        // or returning any partial output.
        let collateral_token = self
            .catalog
            .collateral_token_for_dex("")
            .context("Hyperliquid catalog missing default-dex collateral token")?;
        let parsed_state = parse_perp_dex_state("", collateral_token, state, now_ms)?;

        let mut output = Vec::new();
        let mut symbols: Vec<_> = current
            .keys()
            .chain(self.positions.keys())
            .cloned()
            .collect();
        symbols.sort_unstable();
        symbols.dedup();
        for symbol in symbols {
            let next = current.get(&symbol).copied().unwrap_or((0.0, 0.0));
            let msg = BasicPositionMsg::create(now_ms, symbol.clone(), 'N', next.0);
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::PositionUpdate,
                msg.to_bytes(),
            ));
            let msg = BasicUmUnrealizedMsg::create(now_ms, symbol, 'N', next.1);
            output.push(self.wrap_scope(
                self.mode.perp_scope(),
                BasicAccountEventType::UnrealizedPnlUpdate,
                msg.to_bytes(),
            ));
        }
        output.extend(self.standard_default_dex_events(&parsed_state, now_ms));
        output.push(self.wrap_scope(
            BasicAccountScope::HyperliquidStdPerp,
            BasicAccountEventType::HyperliquidPerpDexState,
            parsed_state.message.to_bytes(),
        ));
        output.push(self.snapshot_complete(TradingVenue::HyperliquidFutures, now_ms));
        self.positions = current;
        Ok(output)
    }

    fn validate_user(&self, data: &Value) -> Result<()> {
        let user = required_str(data, "user")?;
        let normalized = normalize_hyperliquid_address(user)?;
        if normalized != self.user {
            anyhow::bail!(
                "Hyperliquid account update user mismatch: expected={} received={}",
                self.user,
                normalized
            );
        }
        Ok(())
    }

    fn maybe_emit_unified_risk(&self, now_ms: i64) -> Result<Option<Bytes>> {
        if self.mode != HyperliquidAccountMode::Unified || !self.spot_snapshot_seen {
            return Ok(None);
        }
        let Some((cross_margin_by_token, isolated_margin_by_token)) =
            self.unified_margin_by_token.as_ref()
        else {
            return Ok(None);
        };

        self.build_unified_risk(cross_margin_by_token, isolated_margin_by_token, now_ms)
    }

    fn build_unified_risk(
        &self,
        cross_margin_by_token: &HashMap<i64, f64>,
        isolated_margin_by_token: &HashMap<i64, f64>,
        now_ms: i64,
    ) -> Result<Option<Bytes>> {
        if !self.spot_snapshot_seen {
            return Ok(None);
        }

        let mut liquidation_ratio = 0.0_f64;
        let mut unavailable_cross_collateral = false;
        for (token, cross_margin) in cross_margin_by_token {
            let spot_total = self
                .spot_balances_by_token
                .get(token)
                .copied()
                .unwrap_or(0.0);
            let isolated_margin = isolated_margin_by_token.get(token).copied().unwrap_or(0.0);
            let available = spot_total - isolated_margin;
            if available > 0.0 {
                liquidation_ratio = liquidation_ratio.max(*cross_margin / available);
            } else if *cross_margin > 0.0 {
                unavailable_cross_collateral = true;
            }
        }
        let safe_ratio = if unavailable_cross_collateral {
            0.0
        } else if liquidation_ratio > 0.0 {
            (1.0 / liquidation_ratio).min(1.0e12)
        } else {
            1.0e12
        };
        validate_nonnegative_finite("unified account safe margin ratio", safe_ratio)?;
        // Cross-DEX collateral tokens are not necessarily USD stablecoins.
        // The cross-token USD amounts are unavailable, not zero.
        let msg = BasicAccountRiskMsg::ratio_only(now_ms, safe_ratio);
        Ok(Some(self.wrap_scope(
            self.mode.perp_scope(),
            BasicAccountEventType::AccountRisk,
            msg.to_bytes(),
        )))
    }

    fn standard_default_dex_events(&self, state: &ParsedPerpDexState, now_ms: i64) -> Vec<Bytes> {
        let balance = BasicBalanceMsg::create(now_ms, "USDC".to_string(), state.total_raw_usd);
        let margin_ratio = if state.cross_maintenance_margin_used > 0.0 {
            (state.cross_account_value / state.cross_maintenance_margin_used).clamp(-1.0e12, 1.0e12)
        } else {
            1.0e12
        };
        let risk = BasicAccountRiskMsg::create(
            now_ms,
            state.cross_account_value,
            state.account_value,
            state.cross_maintenance_margin_used,
            state.cross_total_margin_used,
            margin_ratio,
            0.0,
            state.cross_total_ntl_pos,
        );
        vec![
            self.wrap_scope(
                BasicAccountScope::HyperliquidStdPerp,
                BasicAccountEventType::BalanceUpdate,
                balance.to_bytes(),
            ),
            self.wrap_scope(
                BasicAccountScope::HyperliquidStdPerp,
                BasicAccountEventType::AccountRisk,
                risk.to_bytes(),
            ),
        ]
    }

    fn snapshot_complete(&self, venue: TradingVenue, now_ms: i64) -> Bytes {
        let msg = HyperliquidSnapshotCompleteMsg::create(venue.to_u8(), now_ms);
        self.wrap_venue(
            venue,
            BasicAccountEventType::HyperliquidSnapshotComplete,
            msg.to_bytes(),
        )
    }

    fn commit_fill_cumulative(&mut self, fill: &ParsedFill) {
        let is_new_oid = !self.fill_cumulative_by_oid.contains_key(&fill.order_id);
        self.fill_cumulative_by_oid
            .insert(fill.order_id, fill.cumulative_filled_quantity);
        self.historical_fill_anchor_by_oid
            .insert(fill.order_id, fill.cumulative_filled_quantity);
        if is_new_oid {
            self.fill_oid_age.push_back(fill.order_id);
        }
        while self.fill_cumulative_by_oid.len() > DEFAULT_DEDUP_CAPACITY {
            let Some(oldest) = self.fill_oid_age.pop_front() else {
                break;
            };
            self.fill_cumulative_by_oid.remove(&oldest);
            self.attributed_fill_quantity_by_oid.remove(&oldest);
        }
    }

    fn initialize_fill_cumulative(&mut self, order_id: i64, cumulative: f64) {
        if self
            .fill_cumulative_by_oid
            .insert(order_id, cumulative)
            .is_none()
        {
            self.fill_oid_age.push_back(order_id);
        }
    }

    fn wrap_venue(
        &self,
        venue: TradingVenue,
        event_type: BasicAccountEventType,
        payload: Bytes,
    ) -> Bytes {
        let scope = match venue {
            TradingVenue::HyperliquidMargin => self.mode.spot_scope(),
            TradingVenue::HyperliquidFutures => self.mode.perp_scope(),
            _ => BasicAccountScope::Unknown,
        };
        self.wrap_scope(scope, event_type, payload)
    }

    fn wrap_scope(
        &self,
        scope: BasicAccountScope,
        event_type: BasicAccountEventType,
        payload: Bytes,
    ) -> Bytes {
        BasicAccountEventMsg::create(event_type, scope, payload).to_bytes()
    }

    fn cache_order(&mut self, order_id: i64, identity: OrderIdentity) {
        let is_new = self.orders.insert(order_id, identity).is_none();
        if is_new {
            self.order_age.push_back(order_id);
        }
        self.evict_inactive_orders_to_capacity();
    }

    fn evict_inactive_orders_to_capacity(&mut self) {
        while self.orders.len() > DEFAULT_ORDER_CACHE_CAPACITY {
            let candidates = self.order_age.len();
            let mut evicted = false;
            for _ in 0..candidates {
                let Some(oldest) = self.order_age.pop_front() else {
                    break;
                };
                if !self.orders.contains_key(&oldest) {
                    continue;
                }
                if self.active_order_ids.contains(&oldest) {
                    self.order_age.push_back(oldest);
                    continue;
                }
                self.orders.remove(&oldest);
                self.historical_fill_anchor_by_oid.remove(&oldest);
                self.expected_fill_cumulative_by_oid.remove(&oldest);
                if let Some(pending) = self.pending_fills.remove(&oldest) {
                    self.pending_fill_count = self.pending_fill_count.saturating_sub(pending.len());
                }
                self.pending_terminal_by_oid.remove(&oldest);
                self.order_lifecycle_by_oid.remove(&oldest);
                self.attributed_fill_quantity_by_oid.remove(&oldest);
                if let Some(retained) = self.late_attribution_fills.remove(&oldest) {
                    self.late_attribution_fill_count = self
                        .late_attribution_fill_count
                        .saturating_sub(retained.len());
                }
                self.unrecoverable_unattributed_oids.remove(&oldest);
                evicted = true;
                break;
            }
            if !evicted {
                // Active orders are intentionally allowed to take the cache
                // above its soft cap. They become eligible after a terminal
                // orderUpdates row and any already-pending fills are drained.
                break;
            }
        }
    }

    fn queue_pending_fill(&mut self, fill: ParsedFill) -> Option<ParsedFill> {
        let evicted = if self.pending_fill_count >= DEFAULT_PENDING_FILL_CAPACITY {
            let oldest_oid = self
                .pending_fills
                .iter()
                .filter_map(|(oid, rows)| rows.front().map(|row| (*oid, row.received_at)))
                .min_by_key(|(_, received_at)| *received_at)
                .map(|(oid, _)| oid);
            oldest_oid.and_then(|oid| {
                let rows = self.pending_fills.get_mut(&oid)?;
                let oldest = rows.pop_front();
                if oldest.is_some() {
                    self.pending_fill_count = self.pending_fill_count.saturating_sub(1);
                }
                if rows.is_empty() {
                    self.pending_fills.remove(&oid);
                }
                oldest
            })
        } else {
            None
        };
        if self.pending_fill_count >= DEFAULT_PENDING_FILL_CAPACITY {
            // Defensive fallback for an inconsistent count. Emit this fill as
            // external rather than dropping factual exchange evidence.
            return Some(fill);
        }
        self.pending_fills
            .entry(fill.order_id)
            .or_default()
            .push_back(fill);
        self.pending_fill_count += 1;
        evicted
    }

    fn journal_unattributed_fill(&mut self, fill: ParsedFill) {
        if self.late_attribution_fill_count >= DEFAULT_LATE_ATTRIBUTION_CAPACITY {
            let oldest_oid = self
                .late_attribution_fills
                .iter()
                .filter_map(|(oid, rows)| rows.front().map(|row| (*oid, row.received_at)))
                .min_by_key(|(_, received_at)| *received_at)
                .map(|(oid, _)| oid);
            if let Some(oldest_oid) = oldest_oid {
                if let Some(rows) = self.late_attribution_fills.get_mut(&oldest_oid) {
                    if rows.pop_front().is_some() {
                        self.late_attribution_fill_count =
                            self.late_attribution_fill_count.saturating_sub(1);
                        self.unrecoverable_unattributed_oids.insert(oldest_oid);
                    }
                    if rows.is_empty() {
                        self.late_attribution_fills.remove(&oldest_oid);
                    }
                }
            }
        }
        if self.late_attribution_fill_count >= DEFAULT_LATE_ATTRIBUTION_CAPACITY {
            self.unrecoverable_unattributed_oids.insert(fill.order_id);
            return;
        }
        self.late_attribution_fills
            .entry(fill.order_id)
            .or_default()
            .push_back(fill);
        self.late_attribution_fill_count += 1;
    }
}

fn parse_perp_dex_state(
    dex: &str,
    collateral_token: i64,
    state: &Value,
    now_ms: i64,
) -> Result<ParsedPerpDexState> {
    if collateral_token < 0 {
        anyhow::bail!(
            "Hyperliquid perp dex {dex:?} collateral token must be nonnegative: {collateral_token}"
        );
    }
    let margin = state
        .get("marginSummary")
        .with_context(|| format!("Hyperliquid dex {dex:?} missing marginSummary"))?;
    let cross = state
        .get("crossMarginSummary")
        .with_context(|| format!("Hyperliquid dex {dex:?} missing crossMarginSummary"))?;

    let margin_account_value = required_finite_decimal_string(margin, "accountValue")?;
    let margin_total_ntl_pos = required_finite_decimal_string(margin, "totalNtlPos")?;
    let margin_total_raw_usd = required_finite_decimal_string(margin, "totalRawUsd")?;
    let margin_total_margin_used = required_finite_decimal_string(margin, "totalMarginUsed")?;
    let cross_account_value = required_finite_decimal_string(cross, "accountValue")?;
    let cross_total_ntl_pos = required_finite_decimal_string(cross, "totalNtlPos")?;
    let cross_total_raw_usd = required_finite_decimal_string(cross, "totalRawUsd")?;
    let cross_total_margin_used = required_finite_decimal_string(cross, "totalMarginUsed")?;
    let cross_maintenance_margin_used =
        required_finite_decimal_string(state, "crossMaintenanceMarginUsed")?;
    let withdrawable = required_finite_decimal_string(state, "withdrawable")?;

    let parsed_cross_maintenance = required_f64(state, "crossMaintenanceMarginUsed")?;
    validate_nonnegative_finite("cross maintenance margin used", parsed_cross_maintenance)?;
    Ok(ParsedPerpDexState {
        message: HyperliquidPerpDexStateMsg::create(
            now_ms,
            dex.to_string(),
            collateral_token,
            margin_account_value,
            margin_total_ntl_pos,
            margin_total_raw_usd,
            margin_total_margin_used,
            cross_account_value,
            cross_total_ntl_pos,
            cross_total_raw_usd,
            cross_total_margin_used,
            cross_maintenance_margin_used,
            withdrawable,
        ),
        account_value: required_f64(margin, "accountValue")?,
        total_raw_usd: required_f64(margin, "totalRawUsd")?,
        cross_account_value: required_f64(cross, "accountValue")?,
        cross_total_ntl_pos: required_f64(cross, "totalNtlPos")?,
        cross_total_margin_used: required_f64(cross, "totalMarginUsed")?,
        cross_maintenance_margin_used: parsed_cross_maintenance,
    })
}

fn clearinghouse_state_entries(value: &Value) -> Result<Vec<(&str, &Value)>> {
    match value {
        Value::Object(states) => Ok(states
            .iter()
            .map(|(dex, state)| (dex.as_str(), state))
            .collect()),
        Value::Array(rows) => {
            let mut entries = Vec::with_capacity(rows.len());
            let mut seen = HashSet::with_capacity(rows.len());
            for (index, row) in rows.iter().enumerate() {
                let pair = row.as_array().filter(|pair| pair.len() == 2).with_context(|| {
                    format!(
                        "Hyperliquid clearinghouseStates pair at index {index} must contain dex and state"
                    )
                })?;
                let dex = match &pair[0] {
                    Value::String(dex) => dex.as_str(),
                    Value::Null => "",
                    _ => anyhow::bail!(
                        "Hyperliquid clearinghouseStates dex at index {index} must be a string or null"
                    ),
                };
                if !seen.insert(dex) {
                    anyhow::bail!("duplicate Hyperliquid clearinghouseStates dex {dex:?}");
                }
                entries.push((dex, &pair[1]));
            }
            Ok(entries)
        }
        _ => anyhow::bail!(
            "Hyperliquid clearinghouseStates must be an object or an array of [dex, state] pairs"
        ),
    }
}

pub async fn fetch_historical_orders(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
) -> Result<Value> {
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({"type": "historicalOrders", "user": user}),
    )
    .await
}

pub async fn fetch_frontend_open_orders(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    dex: &str,
) -> Result<Value> {
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({"type": "frontendOpenOrders", "user": user, "dex": dex}),
    )
    .await
}

pub async fn fetch_user_fills_by_time(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Value> {
    if start_time_ms < 0 || end_time_ms < start_time_ms {
        anyhow::bail!(
            "invalid Hyperliquid userFillsByTime range: start={start_time_ms} end={end_time_ms}"
        );
    }
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({
            "type": "userFillsByTime",
            "user": user,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "aggregateByTime": false,
        }),
    )
    .await
}

pub async fn fetch_user_twap_slice_fills_by_time(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Value> {
    validate_history_time_range("userTwapSliceFillsByTime", start_time_ms, end_time_ms)?;
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({
            "type": "userTwapSliceFillsByTime",
            "user": user,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }),
    )
    .await
}

pub async fn fetch_twap_history(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
) -> Result<Value> {
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({"type": "twapHistory", "user": user}),
    )
    .await
}

pub async fn fetch_user_funding_by_time(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Value> {
    validate_history_time_range("userFunding", start_time_ms, end_time_ms)?;
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({
            "type": "userFunding",
            "user": user,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }),
    )
    .await
}

pub async fn fetch_user_non_funding_ledger_updates_by_time(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Value> {
    validate_history_time_range("userNonFundingLedgerUpdates", start_time_ms, end_time_ms)?;
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({
            "type": "userNonFundingLedgerUpdates",
            "user": user,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }),
    )
    .await
}

fn validate_history_time_range(kind: &str, start_time_ms: i64, end_time_ms: i64) -> Result<()> {
    if start_time_ms < 0 || end_time_ms < start_time_ms {
        anyhow::bail!("invalid Hyperliquid {kind} range: start={start_time_ms} end={end_time_ms}");
    }
    Ok(())
}

pub fn parse_user_abstraction(value: &Value) -> Result<HyperliquidAccountMode> {
    match value.as_str() {
        Some("disabled") => Ok(HyperliquidAccountMode::Standard),
        Some("unifiedAccount") => Ok(HyperliquidAccountMode::Unified),
        Some("portfolioMargin") => Ok(HyperliquidAccountMode::PortfolioMargin),
        Some("default") => anyhow::bail!(
            "Hyperliquid userAbstraction returned default; account ownership mode is ambiguous"
        ),
        Some("dexAbstraction") => anyhow::bail!(
            "Hyperliquid dexAbstraction is not supported by the default-dex account monitor"
        ),
        Some(other) => anyhow::bail!("unknown Hyperliquid userAbstraction value {other}"),
        None => anyhow::bail!("Hyperliquid userAbstraction response must be a string"),
    }
}

pub fn parse_user_role(value: &Value) -> Result<HyperliquidUserRole> {
    let role = value
        .get("role")
        .and_then(Value::as_str)
        .context("Hyperliquid userRole response missing role")?;
    match role.to_ascii_lowercase().as_str() {
        "user" => Ok(HyperliquidUserRole::User),
        "agent" => Ok(HyperliquidUserRole::Agent),
        "vault" => Ok(HyperliquidUserRole::Vault),
        "subaccount" => Ok(HyperliquidUserRole::SubAccount),
        "missing" => Ok(HyperliquidUserRole::Missing),
        other => anyhow::bail!("unsupported Hyperliquid user role {other:?}"),
    }
}

pub fn resolve_user_abstraction(
    value: &Value,
    role: HyperliquidUserRole,
) -> Result<HyperliquidAccountMode> {
    match role {
        HyperliquidUserRole::Agent => anyhow::bail!(
            "Hyperliquid account address identifies an API agent wallet; use the user, subaccount, or vault whose state is required"
        ),
        HyperliquidUserRole::Missing => anyhow::bail!(
            "Hyperliquid account address has no user role and cannot be monitored or traded"
        ),
        HyperliquidUserRole::User
        | HyperliquidUserRole::SubAccount
        | HyperliquidUserRole::Vault => {}
    }

    let reported = value
        .as_str()
        .context("Hyperliquid userAbstraction response must be a string")?;
    let mode = match reported {
        "disabled" => HyperliquidAccountMode::Standard,
        "unifiedAccount" => HyperliquidAccountMode::Unified,
        "portfolioMargin" => HyperliquidAccountMode::PortfolioMargin,
        "default" => match role {
            HyperliquidUserRole::Vault => HyperliquidAccountMode::Standard,
            HyperliquidUserRole::User | HyperliquidUserRole::SubAccount => {
                anyhow::bail!(
                    "Hyperliquid userAbstraction returned ambiguous default for role={}; cannot verify exchange account rules",
                    role.as_str()
                )
            }
            HyperliquidUserRole::Agent | HyperliquidUserRole::Missing => unreachable!(),
        },
        "dexAbstraction" => anyhow::bail!(
            "Hyperliquid dexAbstraction is not supported by the default-dex account path"
        ),
        other => anyhow::bail!("unknown Hyperliquid userAbstraction value {other}"),
    };
    Ok(mode)
}

pub async fn fetch_user_role(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
) -> Result<HyperliquidUserRole> {
    let user = normalize_hyperliquid_address(user)?;
    let value = fetch_info(client, info_url, json!({"type": "userRole", "user": user})).await?;
    parse_user_role(&value)
}

pub async fn fetch_user_abstraction(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
    role: HyperliquidUserRole,
) -> Result<HyperliquidAccountMode> {
    let value = fetch_user_abstraction_raw(client, info_url, user).await?;
    resolve_user_abstraction(&value, role)
}

/// Read-only discovery shared by consumers that do not already load userRole.
pub async fn discover_account_mode(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
) -> Result<HyperliquidAccountMode> {
    let (role, abstraction) = tokio::try_join!(
        fetch_user_role(client, info_url, user),
        fetch_user_abstraction_raw(client, info_url, user),
    )?;
    resolve_user_abstraction(&abstraction, role)
}

pub async fn fetch_user_abstraction_raw(
    client: &reqwest::Client,
    info_url: &str,
    user: &str,
) -> Result<Value> {
    let user = normalize_hyperliquid_address(user)?;
    fetch_info(
        client,
        info_url,
        json!({"type": "userAbstraction", "user": user}),
    )
    .await
}

async fn fetch_info(client: &reqwest::Client, info_url: &str, body: Value) -> Result<Value> {
    let response = client
        .post(info_url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("request Hyperliquid info endpoint {info_url}"))?
        .error_for_status()
        .with_context(|| format!("Hyperliquid info endpoint returned error for {info_url}"))?;
    response
        .json::<Value>()
        .await
        .context("decode Hyperliquid info response")
}

pub fn subscription_messages(user: &str, mode: HyperliquidAccountMode) -> Result<Vec<Value>> {
    let user = normalize_hyperliquid_address(user)?;
    let perp_subscription = json!({"method": "subscribe", "subscription": {"type": "allDexsClearinghouseState", "user": user}});
    let spot_subscription = match mode {
        HyperliquidAccountMode::Standard | HyperliquidAccountMode::Unified => {
            json!({"method": "subscribe", "subscription": {"type": "spotState", "user": user}})
        }
        HyperliquidAccountMode::PortfolioMargin => {
            json!({"method": "subscribe", "subscription": {"type": "spotState", "user": user, "isPortfolioMargin": true}})
        }
    };
    // Canonical factual sources:
    // - orderUpdates owns order lifecycle;
    // - userFills and userTwapSliceFills share one fill identity/dedup path;
    //   the latter additionally owns the parent-TWAP association;
    // - userTwapHistory owns parent lifecycle rows;
    // - state streams own balances, positions, PnL, and risk.
    // userEvents remains omitted to avoid its fill/funding/TWAP mirrors. Its
    // liquidation aggregate and nonUserCancel variants are separate optional
    // audit surfaces and are not canonicalized by this channel set.
    // Funding and non-funding ledger streams are factual accounting evidence
    // and never synthesize a fee, fill, or balance delta.
    Ok(vec![
        json!({"method": "subscribe", "subscription": {"type": "orderUpdates", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "userFills", "user": user, "aggregateByTime": false}}),
        perp_subscription,
        spot_subscription,
        json!({"method": "subscribe", "subscription": {"type": "userFundings", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "userNonFundingLedgerUpdates", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "userTwapSliceFills", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "userTwapHistory", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "userEvents", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "notification", "user": user}}),
        json!({"method": "subscribe", "subscription": {"type": "webData3", "user": user}}),
    ])
}

pub fn subscription_messages_for_catalog(
    user: &str,
    mode: HyperliquidAccountMode,
    catalog: &HyperliquidAssetCatalog,
) -> Result<Vec<Value>> {
    let mut subscriptions = subscription_messages(user, mode)?;
    let user = normalize_hyperliquid_address(user)?;
    for dex in catalog.perp_dexes() {
        subscriptions.push(json!({"method":"subscribe","subscription":{"type":"twapStates","user":user,"dex":dex}}));
    }
    for coin in &catalog.active_perp_coins {
        subscriptions.push(json!({"method":"subscribe","subscription":{"type":"activeAssetData","user":user,"coin":coin}}));
    }
    Ok(subscriptions)
}

fn parse_fill(value: &Value, fallback_time: i64) -> Result<ParsedFill> {
    let time = required_i64(value, "time")?;
    let coin = required_str(value, "coin")?.to_string();
    if coin.is_empty() {
        anyhow::bail!("Hyperliquid fill coin must not be empty");
    }
    let start_position = optional_finite_decimal_string(value, "startPosition")?;
    let dir = optional_nonempty_string(value, "dir")?;
    let closed_pnl = optional_finite_decimal_string(value, "closedPnl")?;
    let fee = optional_finite_decimal_string(value, "fee")?;
    let fee_token = optional_nonempty_string(value, "feeToken")?;
    let builder_fee = optional_finite_decimal_string(value, "builderFee")?;
    let twap_id = optional_i64_field(value, "twapId")?;
    if twap_id.is_some_and(|value| value < 0) {
        anyhow::bail!("Hyperliquid fill twapId must be nonnegative");
    }
    let (liquidation_method, liquidated_user, liquidation_mark_price) =
        match value.get("liquidation") {
            None | Some(Value::Null) => (String::new(), None, None),
            Some(liquidation) if liquidation.is_object() => {
                let method = required_str(liquidation, "method")?.to_string();
                if method.is_empty() {
                    anyhow::bail!("Hyperliquid fill liquidation method must not be empty");
                }
                (
                    method,
                    optional_nonempty_string(liquidation, "liquidatedUser")?,
                    Some(required_finite_decimal_string(liquidation, "markPx")?),
                )
            }
            Some(_) => anyhow::bail!("Hyperliquid fill liquidation must be an object or null"),
        };
    let transaction_hash = required_str(value, "hash")?.to_string();
    if transaction_hash.is_empty() {
        anyhow::bail!("Hyperliquid fill hash must not be empty");
    }
    let fill = ParsedFill {
        coin,
        price: required_f64(value, "px")?,
        quantity: required_f64(value, "sz")?,
        side: parse_side(required_str(value, "side")?)?,
        time,
        order_id: required_i64(value, "oid")?,
        crossed: value
            .get("crossed")
            .and_then(Value::as_bool)
            .context("Hyperliquid fill missing crossed")?,
        tid: required_i64(value, "tid")?,
        transaction_hash,
        liquidation_method,
        start_position,
        dir,
        closed_pnl,
        fee,
        fee_token,
        builder_fee,
        twap_id,
        liquidated_user,
        liquidation_mark_price,
        received_at: fallback_time,
        cumulative_filled_quantity: 0.0,
    };
    validate_positive_finite("fill price", fill.price)?;
    validate_positive_finite("fill size", fill.quantity)?;
    if fill.order_id <= 0 {
        anyhow::bail!("Hyperliquid fill oid must be positive");
    }
    Ok(fill)
}

fn twap_slice_row_digest(association: &HyperliquidTwapSliceFillMsg, fill: &ParsedFill) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"mkt_signal/hyperliquid/twap_slice_row/content");
    hasher.update(association.content_digest());
    hasher.update(fill.price.to_bits().to_be_bytes());
    hasher.update(fill.quantity.to_bits().to_be_bytes());
    hasher.update([fill.side.to_u8(), u8::from(fill.crossed)]);
    for value in [
        fill.start_position.as_deref(),
        fill.dir.as_deref(),
        fill.closed_pnl.as_deref(),
        fill.fee.as_deref(),
        fill.fee_token.as_deref(),
        fill.builder_fee.as_deref(),
        fill.liquidated_user.as_deref(),
        fill.liquidation_mark_price.as_deref(),
    ] {
        match value {
            Some(value) => {
                hasher.update([1]);
                hasher.update((value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
            None => hasher.update([0]),
        }
    }
    hasher.update((fill.liquidation_method.len() as u64).to_be_bytes());
    hasher.update(fill.liquidation_method.as_bytes());
    hasher.finalize().into()
}

fn parse_twap_history(value: &Value, expected_user: &str) -> Result<HyperliquidTwapHistoryMsg> {
    let event_time = required_i64(value, "time")?;
    if event_time < 0 {
        anyhow::bail!("Hyperliquid TWAP history time must be nonnegative");
    }
    let twap_id = optional_i64_field(value, "twapId")?;
    if twap_id.is_some_and(|value| value < 0) {
        anyhow::bail!("Hyperliquid TWAP history twapId must be nonnegative");
    }
    let state_value = value
        .get("state")
        .filter(|state| state.is_object())
        .context("Hyperliquid TWAP history state must be an object")?;
    let user = normalize_hyperliquid_address(required_str(state_value, "user")?)?;
    if user != expected_user {
        anyhow::bail!(
            "Hyperliquid TWAP history state user mismatch: expected={expected_user} received={user}"
        );
    }
    let coin = required_nonempty_string(state_value, "coin")?;
    let side = required_str(state_value, "side")?;
    if !matches!(side, "B" | "A") {
        anyhow::bail!("invalid Hyperliquid TWAP history side {side}");
    }
    let size = required_nonnegative_decimal_string(state_value, "sz")?;
    let executed_size = required_nonnegative_decimal_string(state_value, "executedSz")?;
    let executed_notional = required_nonnegative_decimal_string(state_value, "executedNtl")?;
    let minutes = required_i64(state_value, "minutes")?;
    if minutes <= 0 {
        anyhow::bail!("Hyperliquid TWAP history minutes must be positive");
    }
    let reduce_only = required_bool(state_value, "reduceOnly")?;
    let randomize = required_bool(state_value, "randomize")?;
    let timestamp = required_i64(state_value, "timestamp")?;
    if timestamp < 0 {
        anyhow::bail!("Hyperliquid TWAP history timestamp must be nonnegative");
    }
    let stop_price = required_nullable_nonnegative_decimal_string(state_value, "stopPx")?;
    let (trigger_price, trigger_above) = match state_value
        .get("trigger")
        .context("Hyperliquid TWAP history state missing trigger")?
    {
        Value::Null => (None, None),
        Value::Object(_) => (
            Some(required_nonnegative_decimal_string(
                state_value.get("trigger").expect("matched trigger"),
                "px",
            )?),
            Some(required_bool(
                state_value.get("trigger").expect("matched trigger"),
                "above",
            )?),
        ),
        _ => anyhow::bail!("Hyperliquid TWAP history trigger must be an object or null"),
    };
    let status_value = value
        .get("status")
        .filter(|status| status.is_object())
        .context("Hyperliquid TWAP history status must be an object")?;
    let status = required_nonempty_string(status_value, "status")?;
    if !matches!(
        status.as_str(),
        "finished" | "activated" | "terminated" | "waitingForTrigger" | "stopped" | "error"
    ) {
        anyhow::bail!("unknown Hyperliquid TWAP history status {status:?}");
    }
    let description = optional_nonempty_string(status_value, "description")?;
    if status == "error" && description.is_none() {
        anyhow::bail!("Hyperliquid TWAP error history row is missing description");
    }

    Ok(HyperliquidTwapHistoryMsg::create(
        event_time,
        twap_id,
        user,
        coin,
        side.to_string(),
        size,
        executed_size,
        executed_notional,
        minutes,
        reduce_only,
        randomize,
        timestamp,
        stop_price,
        trigger_price,
        trigger_above,
        status,
        description,
    ))
}

fn parse_funding(value: &Value) -> Result<ParsedFunding> {
    let coin = required_str(value, "coin")?.to_string();
    if coin.is_empty() {
        anyhow::bail!("Hyperliquid userFunding coin must not be empty");
    }
    Ok(ParsedFunding {
        time: required_i64(value, "time")?,
        coin,
        usdc: required_finite_decimal_string(value, "usdc")?,
        szi: required_finite_decimal_string(value, "szi")?,
        funding_rate: required_finite_decimal_string(value, "fundingRate")?,
        transaction_hash: optional_nonempty_string(value, "hash")?,
    })
}

fn parse_ledger_update(value: &Value) -> Result<ParsedLedgerUpdate> {
    let transaction_hash = required_str(value, "hash")?.to_string();
    if transaction_hash.is_empty() {
        anyhow::bail!("Hyperliquid ledger update hash must not be empty");
    }
    let delta = value
        .get("delta")
        .and_then(Value::as_object)
        .context("Hyperliquid ledger update delta must be an object")?;
    let delta_type = delta
        .get("type")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("Hyperliquid ledger update delta.type must be a nonempty string")?
        .to_string();
    let delta_json = serde_json::to_string(delta).context("encode Hyperliquid ledger delta")?;
    Ok(ParsedLedgerUpdate {
        time: required_i64(value, "time")?,
        transaction_hash,
        delta_type,
        delta_json,
    })
}

fn fill_identity(fill: &ParsedFill) -> String {
    format!("{}:{}:{}", fill.time, fill.coin, fill.tid)
}

fn compact_trade_id(fill: &ParsedFill) -> String {
    let mut hasher = Sha256::new();
    hasher.update(fill.time.to_be_bytes());
    hasher.update([0]);
    hasher.update(fill.coin.as_bytes());
    hasher.update([0]);
    hasher.update(fill.tid.to_be_bytes());
    hasher.update([0]);
    hasher.update(fill.transaction_hash.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("hl:{}", &digest[..32])
}

fn map_order_status(status: &str, cumulative: f64) -> Option<(OrderStatus, ExecutionType)> {
    let lower = status.to_ascii_lowercase();
    if lower == "open" || lower == "triggered" {
        let status = if cumulative > 0.0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::New
        };
        return Some((status, ExecutionType::New));
    }
    if lower == "filled" {
        // Fills are emitted by userFills, so this is lifecycle-only.
        return Some((OrderStatus::Filled, ExecutionType::New));
    }
    if lower == "canceled" || lower.ends_with("canceled") || lower == "scheduledcancel" {
        return Some((OrderStatus::Canceled, ExecutionType::Canceled));
    }
    if lower == "rejected" || lower.ends_with("rejected") {
        return Some((OrderStatus::Expired, ExecutionType::Rejected));
    }
    None
}

fn is_active_order_status(status: &str) -> bool {
    status.eq_ignore_ascii_case("open") || status.eq_ignore_ascii_case("triggered")
}

fn parse_side(value: &str) -> Result<Side> {
    match value {
        "B" | "b" => Ok(Side::Buy),
        "A" | "a" => Ok(Side::Sell),
        _ => anyhow::bail!("invalid Hyperliquid side {value}"),
    }
}

fn optional_order_string<'a>(order: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match order.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value)),
        Some(Value::String(_)) => {
            anyhow::bail!("Hyperliquid order {field} must not be empty")
        }
        Some(_) => anyhow::bail!("Hyperliquid order {field} must be a string or null"),
    }
}

fn parse_hyperliquid_order_type(value: &str) -> Option<OrderType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "limit" => Some(OrderType::Limit),
        "market" => Some(OrderType::Market),
        "stop" | "stop loss" => Some(OrderType::StopLoss),
        "stop limit" | "stop loss limit" => Some(OrderType::StopLossLimit),
        "take" | "take profit" => Some(OrderType::TakeProfit),
        "take limit" | "take profit limit" => Some(OrderType::TakeProfitLimit),
        "stop market" | "stop loss market" => Some(OrderType::StopMarket),
        "take market" | "take profit market" => Some(OrderType::TakeProfitMarket),
        _ => None,
    }
}

fn parse_hyperliquid_time_in_force(value: &str) -> Option<TimeInForce> {
    match value.trim().to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" | "frontendmarket" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "alo" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn sanitize_asset(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect()
}

fn spot_token_names(spot_meta: &Value) -> Result<HashMap<i64, String>> {
    let tokens = spot_meta
        .get("tokens")
        .and_then(Value::as_array)
        .context("Hyperliquid spotMeta missing tokens")?;
    let mut names = HashMap::with_capacity(tokens.len());
    for token in tokens {
        let index = required_i64(token, "index")?;
        let name = required_str(token, "name")?.to_string();
        if names.insert(index, name).is_some() {
            anyhow::bail!("duplicate token index in Hyperliquid spotMeta: {index}");
        }
    }
    Ok(names)
}

pub fn normalize_hyperliquid_address(value: &str) -> Result<String> {
    let trimmed = value.trim();
    let raw = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .context("Hyperliquid account address must start with 0x")?;
    if raw.len() != 40 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("Hyperliquid account address must contain exactly 40 hex digits");
    }
    Ok(format!("0x{}", raw.to_ascii_lowercase()))
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing or invalid string field {field}"))
}

fn validate_optional_snapshot_flag(data: &Value, channel: &str) -> Result<()> {
    if data
        .get("isSnapshot")
        .is_some_and(|value| !value.is_boolean())
    {
        anyhow::bail!("Hyperliquid {channel} isSnapshot must be a boolean");
    }
    Ok(())
}

fn required_i64(value: &Value, field: &str) -> Result<i64> {
    optional_i64(value.get(field)).with_context(|| format!("missing or invalid integer {field}"))
}

fn optional_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(value_i64)
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
}

fn required_f64(value: &Value, field: &str) -> Result<f64> {
    let field_value = value
        .get(field)
        .with_context(|| format!("missing numeric field {field}"))?;
    field_value
        .as_f64()
        .or_else(|| field_value.as_str().and_then(|value| value.parse().ok()))
        .with_context(|| format!("invalid numeric field {field}"))
}

fn required_finite_decimal_string(value: &Value, field: &str) -> Result<String> {
    let raw = required_str(value, field)?;
    let parsed = raw
        .parse::<f64>()
        .with_context(|| format!("invalid decimal string field {field}"))?;
    validate_finite(field, parsed)?;
    Ok(raw.to_string())
}

fn required_nonnegative_decimal_string(value: &Value, field: &str) -> Result<String> {
    let raw = required_finite_decimal_string(value, field)?;
    let parsed = raw
        .parse::<f64>()
        .with_context(|| format!("invalid decimal string field {field}"))?;
    validate_nonnegative_finite(field, parsed)?;
    Ok(raw)
}

fn required_nullable_nonnegative_decimal_string(
    value: &Value,
    field: &str,
) -> Result<Option<String>> {
    match value.get(field) {
        None => anyhow::bail!("missing nullable decimal string field {field}"),
        Some(Value::Null) => Ok(None),
        Some(_) => required_nonnegative_decimal_string(value, field).map(Some),
    }
}

fn required_bool(value: &Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .with_context(|| format!("missing or invalid boolean field {field}"))
}

fn optional_finite_decimal_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_finite_decimal_string(value, field).map(Some),
    }
}

fn required_nonempty_string(value: &Value, field: &str) -> Result<String> {
    let raw = required_str(value, field)?;
    if raw.is_empty() {
        anyhow::bail!("Hyperliquid {field} must not be empty");
    }
    Ok(raw.to_string())
}

fn optional_nonempty_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_nonempty_string(value, field).map(Some),
    }
}

fn optional_i64_field(value: &Value, field: &str) -> Result<Option<i64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value_i64(value)
            .map(Some)
            .with_context(|| format!("invalid integer {field}")),
    }
}

fn validate_finite(field: &str, value: f64) -> Result<()> {
    if !value.is_finite() {
        anyhow::bail!("Hyperliquid {field} must be finite");
    }
    Ok(())
}

fn validate_nonnegative_finite(field: &str, value: f64) -> Result<()> {
    validate_finite(field, value)?;
    if value < 0.0 {
        anyhow::bail!("Hyperliquid {field} must be nonnegative");
    }
    Ok(())
}

fn validate_positive_finite(field: &str, value: f64) -> Result<()> {
    validate_finite(field, value)?;
    if value <= 0.0 {
        anyhow::bail!("Hyperliquid {field} must be positive");
    }
    Ok(())
}

fn validate_pm_event_size(channel: &str, event: &Bytes) -> Result<()> {
    if event.len() > PM_MAX_BYTES {
        anyhow::bail!(
            "Hyperliquid {channel} event exceeds the PM envelope: {} > {}; rejecting the complete frame without truncating venue audit fields",
            event.len(),
            PM_MAX_BYTES
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{
        split_basic_account_event, BasicAccountEventType, BasicAccountRiskMsg, BasicBalanceMsg,
        BasicPositionMsg, BasicUmUnrealizedMsg,
    };
    use mkt_parsers::msg::hyperliquid_account_msg::{
        HyperliquidBasicFillMsg, HyperliquidBasicOrderMsg, HyperliquidFundingMsg,
        HyperliquidLedgerMsg, HyperliquidPerpDexStateMsg, HyperliquidSnapshotCompleteMsg,
        HyperliquidSpotBalanceMsg, HyperliquidTwapHistoryMsg, HyperliquidTwapSliceFillMsg,
    };

    const USER: &str = "0x1111111111111111111111111111111111111111";
    const CLOID: &str = "0x6d6b745f73696731000000000000002a";

    fn catalog() -> HyperliquidAssetCatalog {
        HyperliquidAssetCatalog::from_meta(
            &json!({"universe": [{"name": "BTC", "szDecimals": 5}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "PURR", "index": 1}
                ],
                "universe": [{"name": "PURR/USDC", "tokens": [1, 0], "index": 7}]
            }),
        )
        .unwrap()
    }

    fn processor(policy: FillSnapshotPolicy) -> HyperliquidAccountProcessor {
        HyperliquidAccountProcessor::new(USER, catalog(), HyperliquidAccountMode::Unified, policy)
            .unwrap()
    }

    fn perp_state(positions: Value, cross_maintenance: &str) -> Value {
        json!({
            "assetPositions": positions,
            "marginSummary": {
                "accountValue": "1000.000",
                "totalNtlPos": "20.500",
                "totalRawUsd": "979.500",
                "totalMarginUsed": "2.0500"
            },
            "crossMarginSummary": {
                "accountValue": "995.000",
                "totalNtlPos": "18.7500",
                "totalRawUsd": "976.250",
                "totalMarginUsed": "1.87500"
            },
            "crossMaintenanceMarginUsed": cross_maintenance,
            "withdrawable": "971.234500"
        })
    }

    fn unwrap_event(bytes: &Bytes) -> (BasicAccountEventType, &[u8]) {
        let (kind, scope, payload) = split_basic_account_event(bytes).unwrap();
        assert_eq!(scope, BasicAccountScope::HyperliquidUnified);
        (kind, payload)
    }

    #[test]
    fn catalog_resolves_perp_and_both_spot_aliases() {
        let catalog = catalog();
        assert_eq!(catalog.resolve("BTC").unwrap().symbol, "BTCUSDC");
        assert_eq!(catalog.resolve("@7").unwrap().symbol, "PURRUSDC");
        assert_eq!(catalog.resolve("PURR/USDC").unwrap().symbol, "PURRUSDC");
        assert_eq!(
            catalog.resolve("@7").unwrap().venue,
            TradingVenue::HyperliquidMargin
        );
        assert_eq!(catalog.collateral_token_for_dex(""), Some(0));
    }

    #[test]
    fn catalog_uses_explicit_collision_safe_spot_base_aliases() {
        let alias_catalog = HyperliquidAssetCatalog::from_meta(
            &json!({"universe": [{"name": "BTC"}, {"name": "ETH"}, {"name": "SOL"}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "UBTC", "index": 1},
                    {"name": "UETH", "index": 2},
                    {"name": "USOL", "index": 3},
                    {"name": "UPUMP", "index": 4}
                ],
                "universe": [
                    {"name": "@1", "tokens": [1, 0], "index": 1},
                    {"name": "@2", "tokens": [2, 0], "index": 2},
                    {"name": "@3", "tokens": [3, 0], "index": 3},
                    {"name": "@4", "tokens": [4, 0], "index": 4}
                ]
            }),
        )
        .unwrap();
        assert_eq!(alias_catalog.resolve("@1").unwrap().symbol, "BTCUSDC");
        assert_eq!(alias_catalog.resolve("@2").unwrap().symbol, "ETHUSDC");
        assert_eq!(alias_catalog.resolve("@3").unwrap().symbol, "SOLUSDC");
        assert_eq!(alias_catalog.resolve("@4").unwrap().symbol, "UPUMPUSDC");

        let collision_catalog = HyperliquidAssetCatalog::from_meta(
            &json!({"universe": [{"name": "BTC"}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "BTC", "index": 1},
                    {"name": "UBTC", "index": 2}
                ],
                "universe": [
                    {"name": "BTC/USDC", "tokens": [1, 0], "index": 1},
                    {"name": "@2", "tokens": [2, 0], "index": 2}
                ]
            }),
        )
        .unwrap();
        assert_eq!(
            collision_catalog.resolve("BTC/USDC").unwrap().symbol,
            "BTCUSDC"
        );
        assert_eq!(collision_catalog.resolve("@2").unwrap().symbol, "UBTCUSDC");
    }

    #[test]
    fn all_meta_catalog_maps_each_dex_to_its_collateral_token() {
        let catalog = HyperliquidAssetCatalog::from_all_meta(
            &json!({"universe": [{"name": "BTC"}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "USDH", "index": 2}
                ],
                "universe": []
            }),
            &json!([null, {"name": "xyz"}]),
            &json!([
                {"collateralToken": 0, "universe": [{"name": "BTC"}]},
                {"collateralToken": 2, "universe": [{"name": "xyz:FOO"}]}
            ]),
        )
        .unwrap();
        assert_eq!(catalog.collateral_token_for_dex(""), Some(0));
        assert_eq!(catalog.collateral_token_for_dex("xyz"), Some(2));
        assert_eq!(catalog.resolve("xyz:FOO").unwrap().symbol, "XYZFOOUSDH");
    }

    #[test]
    fn subscriptions_cover_canonical_private_facts_without_duplicate_fill_views() {
        let standard = subscription_messages(USER, HyperliquidAccountMode::Standard).unwrap();
        let types: Vec<_> = standard
            .iter()
            .map(|value| value["subscription"]["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            [
                "orderUpdates",
                "userFills",
                "allDexsClearinghouseState",
                "spotState",
                "userFundings",
                "userNonFundingLedgerUpdates",
                "userTwapSliceFills",
                "userTwapHistory",
                "userEvents",
                "notification",
                "webData3"
            ]
        );
        assert!(types.contains(&"userEvents"));
        assert!(types.contains(&"userTwapSliceFills"));
        assert!(types.contains(&"userTwapHistory"));
        assert!(standard[2]["subscription"].get("dex").is_none());
        let unified = subscription_messages(USER, HyperliquidAccountMode::Unified).unwrap();
        assert_eq!(
            unified[2]["subscription"]["type"],
            "allDexsClearinghouseState"
        );
        assert!(unified[3]["subscription"]
            .get("ignorePortfolioMargin")
            .is_none());
        assert!(unified[3]["subscription"]
            .get("isPortfolioMargin")
            .is_none());
        let portfolio_margin =
            subscription_messages(USER, HyperliquidAccountMode::PortfolioMargin).unwrap();
        assert_eq!(
            portfolio_margin[2]["subscription"]["type"],
            "allDexsClearinghouseState"
        );
        assert_eq!(
            portfolio_margin[3]["subscription"]["isPortfolioMargin"],
            true
        );
        assert!(HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::PortfolioMargin,
            FillSnapshotPolicy::Process,
        )
        .is_ok());
    }

    #[test]
    fn subscription_responses_require_expected_type_and_normalized_user() {
        let requests = subscription_messages(USER, HyperliquidAccountMode::Unified).unwrap();
        let mut acks = HyperliquidSubscriptionAcks::from_requests(&requests).unwrap();
        for (index, request) in requests.iter().enumerate() {
            let mut echoed = request.clone();
            echoed["subscription"]["user"] =
                Value::String(USER.trim_start_matches("0x").to_ascii_uppercase());
            echoed["subscription"]["user"] = Value::String(format!(
                "0X{}",
                echoed["subscription"]["user"].as_str().unwrap()
            ));
            let result = acks
                .observe(&json!({"channel": "subscriptionResponse", "data": echoed}))
                .unwrap();
            assert!(matches!(
                result,
                HyperliquidSubscriptionControl::Acknowledged {
                    completed_now,
                    ..
                } if completed_now == (index + 1 == requests.len())
            ));
        }
        assert!(acks.is_complete());
        assert!(matches!(
            acks.observe(&json!({
                "channel": "subscriptionResponse",
                "data": requests[0].clone()
            }))
            .unwrap(),
            HyperliquidSubscriptionControl::Acknowledged {
                completed_now: false,
                ..
            }
        ));
        assert!(acks.is_complete());
        acks.reset();
        assert!(!acks.is_complete());
        assert!(matches!(
            acks.observe(&json!({
                "channel": "subscriptionResponse",
                "data": requests[0].clone()
            }))
            .unwrap(),
            HyperliquidSubscriptionControl::Acknowledged {
                completed_now: false,
                ..
            }
        ));
        assert!(!acks.is_complete());
        assert!(acks.has_acknowledged("orderUpdates"));

        let wrong_user = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "userFills",
                "user": "0x2222222222222222222222222222222222222222"
            }}
        });
        assert!(acks.observe(&wrong_user).is_err());
        let unexpected_type = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "unknownPrivateChannel",
                "user": USER
            }}
        });
        assert!(acks.observe(&unexpected_type).is_err());
        let wrong_method = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "unsubscribe", "subscription": {
                "type": "userFills",
                "user": USER
            }}
        });
        assert!(acks.observe(&wrong_method).is_err());

        let wrong_fill_aggregation = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "userFills",
                "user": USER,
                "aggregateByTime": true
            }}
        });
        assert!(acks.observe(&wrong_fill_aggregation).is_err());
        let wrong_spot_scope = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "spotState",
                "user": USER,
                "ignorePortfolioMargin": true
            }}
        });
        assert!(acks.observe(&wrong_spot_scope).is_err());

        let mut legacy_spot_ack_tracker =
            HyperliquidSubscriptionAcks::from_requests(&requests).unwrap();
        let legacy_spot_ack = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "spotState",
                "user": USER,
                "isPortfolioMargin": true
            }}
        });
        assert!(matches!(
            legacy_spot_ack_tracker.observe(&legacy_spot_ack).unwrap(),
            HyperliquidSubscriptionControl::Acknowledged {
                subscription_type,
                completed_now: false,
            } if subscription_type == "spotState"
        ));
        let conflicting_spot_ack = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "spotState",
                "user": USER,
                "isPortfolioMargin": true,
                "ignorePortfolioMargin": true
            }}
        });
        assert!(legacy_spot_ack_tracker
            .observe(&conflicting_spot_ack)
            .is_err());
        let inverse_legacy_spot_ack = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "spotState",
                "user": USER,
                "isPortfolioMargin": false
            }}
        });
        assert!(legacy_spot_ack_tracker
            .observe(&inverse_legacy_spot_ack)
            .is_err());

        let standard_requests =
            subscription_messages(USER, HyperliquidAccountMode::Standard).unwrap();
        let mut standard_acks =
            HyperliquidSubscriptionAcks::from_requests(&standard_requests).unwrap();
        let wrong_dex = json!({
            "channel": "subscriptionResponse",
            "data": {"method": "subscribe", "subscription": {
                "type": "allDexsClearinghouseState",
                "user": USER,
                "dex": "xyz"
            }}
        });
        assert!(standard_acks.observe(&wrong_dex).is_err());
    }

    #[test]
    fn user_role_parser_accepts_only_documented_roles() {
        assert_eq!(
            parse_user_role(&json!({"role": "user"})).unwrap(),
            HyperliquidUserRole::User
        );
        assert_eq!(
            parse_user_role(&json!({"role": "agent"})).unwrap(),
            HyperliquidUserRole::Agent
        );
        assert_eq!(
            parse_user_role(&json!({"role": "vault"})).unwrap(),
            HyperliquidUserRole::Vault
        );
        assert_eq!(
            parse_user_role(&json!({"role": "subAccount"})).unwrap(),
            HyperliquidUserRole::SubAccount
        );
        assert_eq!(
            parse_user_role(&json!({"role": "missing"})).unwrap(),
            HyperliquidUserRole::Missing
        );
        assert!(parse_user_role(&json!({"role": "unknown"})).is_err());
        assert!(parse_user_role(&json!({})).is_err());
    }

    #[test]
    fn user_abstraction_resolver_disambiguates_default_without_guessing() {
        assert_eq!(
            parse_user_abstraction(&json!("disabled")).unwrap(),
            HyperliquidAccountMode::Standard
        );
        assert_eq!(
            parse_user_abstraction(&json!("unifiedAccount")).unwrap(),
            HyperliquidAccountMode::Unified
        );
        assert_eq!(
            parse_user_abstraction(&json!("portfolioMargin")).unwrap(),
            HyperliquidAccountMode::PortfolioMargin
        );
        assert!(parse_user_abstraction(&json!("default")).is_err());
        assert!(parse_user_abstraction(&json!("dexAbstraction")).is_err());

        assert_eq!(
            resolve_user_abstraction(&json!("default"), HyperliquidUserRole::Vault).unwrap(),
            HyperliquidAccountMode::Standard
        );
        assert_eq!(
            resolve_user_abstraction(&json!("unifiedAccount"), HyperliquidUserRole::User,).unwrap(),
            HyperliquidAccountMode::Unified
        );
        assert_eq!(
            resolve_user_abstraction(&json!("portfolioMargin"), HyperliquidUserRole::SubAccount,)
                .unwrap(),
            HyperliquidAccountMode::PortfolioMargin
        );
        assert!(resolve_user_abstraction(&json!("default"), HyperliquidUserRole::User).is_err());
        assert!(
            resolve_user_abstraction(&json!("default"), HyperliquidUserRole::SubAccount).is_err()
        );
        assert_eq!(
            resolve_user_abstraction(&json!("default"), HyperliquidUserRole::Vault,).unwrap(),
            HyperliquidAccountMode::Standard
        );
        assert_eq!(
            resolve_user_abstraction(&json!("disabled"), HyperliquidUserRole::User,).unwrap(),
            HyperliquidAccountMode::Standard
        );
        assert!(resolve_user_abstraction(&json!("disabled"), HyperliquidUserRole::Agent,).is_err());
        assert!(
            resolve_user_abstraction(&json!("disabled"), HyperliquidUserRole::Missing,).is_err()
        );
    }

    #[test]
    fn order_update_does_not_claim_gtc_for_unknown_external_intent() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        let payload = json!({
            "channel": "orderUpdates",
            "data": [
                {"order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.15", "oid": 9, "timestamp": 1000, "origSz": "0.25", "cloid": CLOID}, "status": "open", "statusTimestamp": 1234},
                {"order": {"coin": "BTC", "side": "A", "limitPx": "61000", "sz": "1", "oid": 10, "timestamp": 1000, "origSz": "1", "cloid": "0xffffffffffffffffffffffffffffffff"}, "status": "open", "statusTimestamp": 1234}
            ]
        });
        let output = processor
            .process_json_at(payload.to_string().as_bytes(), 2000)
            .unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::HyperliquidNativeEvent
        );
        let (kind, body) = unwrap_event(&output[0]);
        assert_eq!(kind, BasicAccountEventType::OrderUpdate);
        let msg = HyperliquidBasicOrderMsg::from_bytes(body).unwrap();
        assert_eq!(msg.client_order_id, 42);
        assert_eq!(msg.symbol, "BTCUSDC");
        assert_eq!(msg.cumulative_filled_quantity, 0.0);
        assert_eq!(msg.order_status, OrderStatus::PartiallyFilled.to_u8());
        assert_eq!(msg.order_type, OrderType::Limit.to_u8());
        assert_eq!(msg.time_in_force, TimeInForce::GTX.to_u8());
        assert!(processor.orders.contains_key(&10));
        assert!(processor.active_order_ids.contains(&10));

        let enriched = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "A", "limitPx": "61000", "sz": "1",
                      "oid": 10, "timestamp": 1000, "origSz": "1",
                      "cloid": "0xffffffffffffffffffffffffffffffff",
                      "orderType": "Limit", "tif": "Gtc"},
            "status": "open", "statusTimestamp": 1235
        }]});
        let enriched_output = processor.process_value_at(&enriched, 2001).unwrap();
        assert_eq!(enriched_output.len(), 1);
        let (_, body) = unwrap_event(&enriched_output[0]);
        let external = HyperliquidBasicOrderMsg::from_bytes(body).unwrap();
        assert_eq!(external.order_id, 10);
        assert_eq!(external.client_order_id, 0);
        assert_eq!(external.cloid, "0xffffffffffffffffffffffffffffffff");
        assert_eq!(external.order_type, OrderType::Limit.to_u8());
        assert_eq!(external.time_in_force, TimeInForce::GTC.to_u8());
        assert!(processor
            .process_json_at(payload.to_string().as_bytes(), 2002)
            .unwrap()
            .is_empty());

        let scale = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 11, "timestamp": 1000, "origSz": "1", "cloid": null,
                      "orderType": "Scale", "tif": "Gtc"},
            "status": "open", "statusTimestamp": 1236
        }]});
        let raw_scale = processor.process_value_at(&scale, 2003).unwrap();
        assert_eq!(raw_scale.len(), 1);
        assert_eq!(
            unwrap_event(&raw_scale[0]).0,
            BasicAccountEventType::HyperliquidNativeEvent
        );
        assert!(processor.orders[&11].intent_unrepresentable);
        assert!(processor.active_order_ids.contains(&11));
    }

    #[test]
    fn frontend_open_orders_seed_flat_rows_and_absolute_fill_anchors() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let seeded = processor
            .seed_frontend_open_orders(&json!([
                {
                    "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                    "oid": 9, "timestamp": 1000, "origSz": "6", "cloid": CLOID
                },
                {
                    "coin": "BTC", "side": "A", "limitPx": "61000", "sz": "2",
                    "oid": 10, "timestamp": 1000, "origSz": "2", "cloid": null
                }
            ]))
            .unwrap();
        assert_eq!(seeded, 2);
        assert_eq!(processor.orders[&9].client_order_id, 42);
        assert_eq!(processor.orders[&10].client_order_id, 0);
        assert_eq!(processor.historical_fill_anchor_by_oid[&9], 5.0);
        assert_eq!(processor.historical_fill_anchor_by_oid[&10], 0.0);
        assert!(processor.active_order_ids.contains(&9));
        assert!(processor.active_order_ids.contains(&10));

        let snapshot = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": true,
            "fills": [
                {"coin": "BTC", "px": "60000", "sz": "1", "side": "B", "time": 100,
                 "hash": "0x1", "oid": 9, "crossed": true, "tid": 1},
                {"coin": "BTC", "px": "60001", "sz": "1", "side": "B", "time": 101,
                 "hash": "0x2", "oid": 9, "crossed": true, "tid": 2}
            ]
        }});
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &snapshot,
                1000,
                FillSnapshotContext::Initial,
            )
            .unwrap();
        assert_eq!(output.len(), 2);
        let first = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        let second = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(first.cumulative_filled_quantity, 4.0);
        assert_eq!(second.cumulative_filled_quantity, 5.0);
        let coverage_error = processor
            .validate_active_internal_fill_coverage()
            .unwrap_err()
            .to_string();
        assert!(coverage_error.contains("expected_cumulative=5"));
        assert!(coverage_error.contains("recovered_quantity=2"));

        let mut covered = HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::Unified,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        covered
            .seed_frontend_open_orders(&json!([{
                "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                "oid": 19, "timestamp": 1000, "origSz": "3", "cloid": CLOID
            }]))
            .unwrap();
        let covered_snapshot = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": true,
            "fills": [
                {"coin": "BTC", "px": "60000", "sz": "1", "side": "B", "time": 100,
                 "hash": "0xa", "oid": 19, "crossed": true, "tid": 11},
                {"coin": "BTC", "px": "60001", "sz": "1", "side": "B", "time": 101,
                 "hash": "0xb", "oid": 19, "crossed": true, "tid": 12}
            ]
        }});
        assert_eq!(
            covered
                .process_value_at_with_fill_snapshot_context(
                    &covered_snapshot,
                    1000,
                    FillSnapshotContext::Initial,
                )
                .unwrap()
                .len(),
            2
        );
        covered.validate_active_internal_fill_coverage().unwrap();

        let mut rejected = HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::Unified,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        assert!(rejected
            .seed_frontend_open_orders(&json!([
                {"coin": "BTC", "sz": "1", "origSz": "1", "oid": 20, "cloid": CLOID},
                {"coin": "BTC", "sz": "2", "origSz": "1", "oid": 21, "cloid": null}
            ]))
            .is_err());
        assert!(rejected.orders.is_empty());
        assert!(rejected.active_order_ids.is_empty());
        assert!(rejected.historical_fill_anchor_by_oid.is_empty());
    }

    #[test]
    fn order_cut_recovers_frontend_intent_and_dedups_ws_overlap_by_oid_timestamp() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let historical_order = json!({
            "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
            "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID
        });
        let frontend_order = json!({
            "coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
            "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID,
            "orderType": "Limit", "tif": "Alo"
        });
        let frontend_only_market = json!({
            "coin": "BTC", "side": "A", "limitPx": "59000", "sz": "0.5",
            "oid": 10, "timestamp": 1050, "origSz": "0.5", "cloid": null,
            "orderType": "Market", "tif": "FrontendMarket"
        });
        let historical = json!([{
            "order": historical_order,
            "status": "open",
            "statusTimestamp": 1100
        }]);
        let frontend = vec![(String::new(), json!([frontend_order, frontend_only_market]))];

        let recovered = processor
            .recover_order_lifecycle_cut(&historical, &frontend, &HashSet::new())
            .unwrap();
        assert_eq!(recovered.historical_seed_count, 1);
        assert_eq!(recovered.frontend_seed_count, 2);
        assert_eq!(recovered.events.len(), 2);
        let orders = recovered
            .events
            .iter()
            .map(|event| HyperliquidBasicOrderMsg::from_bytes(unwrap_event(event).1).unwrap())
            .map(|order| (order.order_id, order))
            .collect::<HashMap<_, _>>();
        assert_eq!(orders[&9].order_type, OrderType::Limit.to_u8());
        assert_eq!(orders[&9].time_in_force, TimeInForce::GTX.to_u8());
        assert_eq!(orders[&10].order_type, OrderType::Market.to_u8());
        assert_eq!(orders[&10].time_in_force, TimeInForce::IOC.to_u8());

        let required = processor.active_order_ids_snapshot();
        assert!(processor
            .recover_order_lifecycle_cut(&historical, &frontend, &required)
            .unwrap()
            .events
            .is_empty());
        let same_timestamp_cancel = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID},
            "status": "canceled", "statusTimestamp": 1100
        }]});
        assert!(processor
            .process_value_at(&same_timestamp_cancel, 1200)
            .unwrap()
            .is_empty());
        assert!(processor.active_order_ids.contains(&9));

        let later_cancel = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID},
            "status": "canceled", "statusTimestamp": 1200
        }]});
        let output = processor.process_value_at(&later_cancel, 1300).unwrap();
        assert_eq!(output.len(), 1);
        let terminal = HyperliquidBasicOrderMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(terminal.time_in_force, TimeInForce::GTX.to_u8());
        assert!(!processor.active_order_ids.contains(&9));

        let stale_cut = processor
            .recover_order_lifecycle_cut(&historical, &frontend, &HashSet::new())
            .unwrap();
        assert!(stale_cut.events.is_empty());
        assert!(!processor.active_order_ids.contains(&9));
    }

    #[test]
    fn order_cut_defers_internal_terminal_until_factual_fill_coverage() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let open = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1100
        }]});
        assert_eq!(processor.process_value_at(&open, 1200).unwrap().len(), 1);
        let required = processor.active_order_ids_snapshot();
        let historical = json!([{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.6",
                      "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID,
                      "orderType": "Limit", "tif": "Alo"},
            "status": "canceled", "statusTimestamp": 2000
        }]);
        let recovered = processor
            .recover_order_lifecycle_cut(&historical, &[(String::new(), json!([]))], &required)
            .unwrap();
        assert!(recovered.events.is_empty());
        assert!(processor.pending_terminal_by_oid.contains_key(&9));
        assert!(processor.active_order_ids.contains(&9));
        assert!(processor.validate_active_internal_fill_coverage().is_err());

        let fills = json!({"channel": "userFills", "data": {
            "user": USER, "isSnapshot": true, "fills": [{
                "coin": "BTC", "px": "59950", "sz": "0.4", "side": "B",
                "time": 1900, "hash": "0xcut-fill", "oid": 9,
                "crossed": true, "tid": 501
            }]
        }});
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &fills,
                2100,
                FillSnapshotContext::Reconnect,
            )
            .unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[0]).0,
            BasicAccountEventType::HyperliquidFill
        );
        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::OrderUpdate
        );
        processor.validate_active_internal_fill_coverage().unwrap();
        assert!(!processor.active_order_ids.contains(&9));
    }

    #[test]
    fn full_historical_retention_without_active_pin_rejects_cut_atomically() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let open = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "timestamp": 900, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1000
        }]});
        processor.process_value_at(&open, 1001).unwrap();
        let required = processor.active_order_ids_snapshot();
        let historical = Value::Array(
            (0..HYPERLIQUID_HISTORICAL_ORDERS_CAPACITY)
                .map(|offset| {
                    let oid = 10_000_i64 + offset as i64;
                    json!({
                        "order": {"coin": "BTC", "side": "B", "limitPx": "60000",
                                  "sz": "1", "oid": oid, "timestamp": 1000,
                                  "origSz": "1", "cloid": null},
                        "status": "open", "statusTimestamp": 2000 + offset as i64
                    })
                })
                .collect(),
        );
        let error = processor
            .recover_order_lifecycle_cut(&historical, &[(String::new(), json!([]))], &required)
            .unwrap_err();
        assert!(error.to_string().contains("retention boundary"));
        assert_eq!(processor.orders.len(), 1);
        assert!(processor.orders.contains_key(&9));
        assert!(processor.active_order_ids.contains(&9));
        assert!(processor.seen_order_updates.contains(&"9:1000".to_string()));
    }

    #[test]
    fn malformed_late_order_cut_row_does_not_commit_valid_prefix() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let historical = json!([
            {
                "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                          "oid": 9, "timestamp": 1000, "origSz": "1", "cloid": CLOID},
                "status": "open", "statusTimestamp": 1100
            },
            {
                "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                          "oid": 10, "timestamp": 1000, "origSz": "1", "cloid": null},
                "status": "open"
            }
        ]);
        assert!(processor
            .recover_order_lifecycle_cut(
                &historical,
                &[(String::new(), json!([]))],
                &HashSet::new(),
            )
            .is_err());
        assert!(processor.orders.is_empty());
        assert!(processor.active_order_ids.is_empty());
        assert!(processor.seen_order_updates.values.is_empty());
    }

    #[test]
    fn active_internal_and_external_orders_survive_fifo_pressure_until_terminal() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fill = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": false, "fills": [{
                "coin": "BTC", "px": "60000", "sz": "1", "side": "B", "time": 1500,
                "hash": "0xactive", "oid": 9, "crossed": true, "tid": 900
            }]}
        });
        assert!(processor.process_value_at(&fill, 1600).unwrap().is_empty());
        assert_eq!(processor.pending_fill_count, 1);
        processor
            .seed_frontend_open_orders(&json!([
                {"coin": "BTC", "sz": "1", "origSz": "1", "oid": 9, "cloid": CLOID},
                {"coin": "BTC", "sz": "1", "origSz": "1", "oid": 10, "cloid": null,
                 "orderType": "Limit", "tif": "Gtc"}
            ]))
            .unwrap();
        let inactive_identity = processor.orders[&10].clone();
        for offset in 0..DEFAULT_ORDER_CACHE_CAPACITY {
            processor.cache_order(10_000 + offset as i64, inactive_identity.clone());
        }
        assert_eq!(processor.orders.len(), DEFAULT_ORDER_CACHE_CAPACITY);
        assert!(processor.orders.contains_key(&9));
        assert!(processor.orders.contains_key(&10));
        assert!(processor.active_order_ids.contains(&9));
        assert!(processor.active_order_ids.contains(&10));

        let terminal = json!({"channel": "orderUpdates", "data": [
            {
                "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0",
                          "oid": 9, "origSz": "1", "cloid": CLOID},
                "status": "filled", "statusTimestamp": 2000
            },
            {
                "order": {"coin": "BTC", "side": "A", "limitPx": "61000", "sz": "1",
                          "oid": 10, "origSz": "1", "cloid": null},
                "status": "canceled", "statusTimestamp": 2001
            }
        ]});
        let output = processor.process_value_at(&terminal, 3000).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[0]).0,
            BasicAccountEventType::HyperliquidFill
        );
        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::OrderUpdate
        );
        assert_eq!(processor.pending_fill_count, 0);
        assert!(!processor.active_order_ids.contains(&9));
        assert!(!processor.active_order_ids.contains(&10));
    }

    #[test]
    fn malformed_order_update_frame_is_atomic_and_replayable() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fill = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": false, "fills": [{
                "coin": "BTC", "px": "60100", "sz": "0.1", "side": "B", "time": 1500,
                "hash": "0xabc", "oid": 77, "crossed": true, "tid": 123
            }]}
        });
        assert!(processor.process_value_at(&fill, 2000).unwrap().is_empty());
        assert_eq!(processor.pending_fill_count, 1);

        let valid = json!({
            "order": {"coin": "BTC", "side": "B", "limitPx": "60200", "sz": "1",
                      "oid": 77, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1600
        });
        let malformed = json!({
            "order": {"coin": "BTC", "side": "B", "limitPx": "60200", "sz": "1",
                      "oid": 78, "origSz": "1", "cloid": CLOID},
            "status": "open"
        });
        let frame = json!({"channel": "orderUpdates", "data": [valid.clone(), malformed]});
        assert!(processor.process_value_at(&frame, 2001).is_err());

        let dedup_key = "77:1600".to_string();
        assert!(!processor.orders.contains_key(&77));
        assert!(!processor.active_order_ids.contains(&77));
        assert!(!processor.seen_order_updates.contains(&dedup_key));
        assert_eq!(processor.pending_fill_count, 1);
        assert_eq!(processor.pending_fills.get(&77).unwrap().len(), 1);

        let replay = json!({"channel": "orderUpdates", "data": [valid]});
        let output = processor.process_value_at(&replay, 2002).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[0]).0,
            BasicAccountEventType::OrderUpdate
        );
        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::HyperliquidFill
        );
        assert_eq!(processor.pending_fill_count, 0);
        assert!(!processor.pending_fills.contains_key(&77));
        assert!(processor.active_order_ids.contains(&77));
        assert!(processor.seen_order_updates.contains(&dedup_key));
    }

    #[test]
    fn factual_streams_require_exchange_timestamps_for_cross_path_dedup() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let order_without_status_time = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.1", "oid": 9, "origSz": "0.1", "cloid": CLOID},
            "status": "open"
        }]});
        assert!(processor
            .process_value_at(&order_without_status_time, 1000)
            .is_err());

        let fill_without_trade_time = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": false,
            "fills": [{
                "coin": "BTC", "px": "60000", "sz": "0.1", "side": "B",
                "hash": "0x1", "oid": 9, "crossed": true, "tid": 1
            }]
        }});
        assert!(processor
            .process_value_at(&fill_without_trade_time, 1000)
            .is_err());
    }

    #[test]
    fn fill_accepts_optional_twap_and_liquidated_user_fields() {
        let parsed = parse_fill(
            &json!({
                "coin": "BTC", "px": "60000", "sz": "0.1", "side": "B",
                "time": 1500, "hash": "0xabc", "oid": 9, "crossed": true,
                "tid": 123, "twapId": null,
                "liquidation": {"method": "market", "markPx": "59999"}
            }),
            2000,
        )
        .unwrap();
        assert_eq!(parsed.twap_id, None);
        assert_eq!(parsed.liquidated_user, None);
        assert_eq!(parsed.liquidation_mark_price.as_deref(), Some("59999"));
    }

    #[test]
    fn fill_before_order_mapping_emits_once_without_filled_lifecycle() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fill = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": false, "fills": [{
                "coin": "BTC", "px": "60100", "sz": "0.1", "side": "B", "time": 1500,
                "startPosition": "0", "dir": "Open Long", "closedPnl": "0", "hash": "0xabc",
                "oid": 9, "crossed": true, "fee": "1", "tid": 123, "feeToken": "USDC"
            }]}
        });
        assert!(processor
            .process_json_at(fill.to_string().as_bytes(), 2000)
            .unwrap()
            .is_empty());

        let order = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60200", "sz": "0", "oid": 9, "timestamp": 1000, "origSz": "0.1", "cloid": CLOID},
            "status": "filled", "statusTimestamp": 1600
        }]});
        let output = processor
            .process_json_at(order.to_string().as_bytes(), 2000)
            .unwrap();
        assert_eq!(output.len(), 1);
        let (kind, body) = unwrap_event(&output[0]);
        assert_eq!(kind, BasicAccountEventType::HyperliquidFill);
        let msg = HyperliquidBasicFillMsg::from_bytes(body).unwrap();
        assert_eq!(msg.client_order_id, 42);
        assert_eq!(msg.symbol, "BTCUSDC");
        assert_eq!(msg.trade_time, 1500);
        assert_eq!(msg.is_maker, 0);
        assert_eq!(msg.last_filled_quantity, 0.1);
        assert_eq!(msg.cumulative_filled_quantity, 0.1);
        assert_eq!(msg.order_status, OrderStatus::Filled.to_u8());
        assert_eq!(msg.trade_id_str().len(), 35);
        assert!(processor
            .process_json_at(fill.to_string().as_bytes(), 2000)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn filled_order_update_before_fill_does_not_drop_internal_fill() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let order = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60200", "sz": "0", "oid": 9, "timestamp": 1000, "origSz": "0.1", "cloid": CLOID},
            "status": "filled", "statusTimestamp": 1600
        }]});
        assert!(processor
            .process_json_at(order.to_string().as_bytes(), 2000)
            .unwrap()
            .is_empty());

        let fill = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": false, "fills": [{
                "coin": "BTC", "px": "60100", "sz": "0.1", "side": "B", "time": 1500,
                "hash": "0xabc", "oid": 9, "crossed": true, "tid": 123
            }]}
        });
        let output = processor
            .process_json_at(fill.to_string().as_bytes(), 2001)
            .unwrap();
        assert_eq!(output.len(), 1);
        let (kind, body) = unwrap_event(&output[0]);
        assert_eq!(kind, BasicAccountEventType::HyperliquidFill);
        assert_eq!(
            HyperliquidBasicFillMsg::from_bytes(body)
                .unwrap()
                .client_order_id,
            42
        );
        let msg = HyperliquidBasicFillMsg::from_bytes(body).unwrap();
        assert_eq!(msg.cumulative_filled_quantity, 0.1);
        assert_eq!(msg.order_status, OrderStatus::Filled.to_u8());
    }

    #[test]
    fn internal_cancel_waits_for_factual_fill_then_emits_terminal_in_order() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let open = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1400
        }]});
        assert_eq!(processor.process_value_at(&open, 1500).unwrap().len(), 1);

        let canceled = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.6",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "canceled", "statusTimestamp": 1600
        }]});
        assert!(processor
            .process_value_at(&canceled, 1700)
            .unwrap()
            .is_empty());
        assert!(processor.pending_terminal_by_oid.contains_key(&9));
        assert!(processor.active_order_ids.contains(&9));

        let fill = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": false,
            "fills": [{
                "coin": "BTC", "px": "59950", "sz": "0.4", "side": "B", "time": 1550,
                "hash": "0xpartial", "oid": 9, "crossed": true, "tid": 501
            }]
        }});
        let output = processor.process_value_at(&fill, 1800).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[0]).0,
            BasicAccountEventType::HyperliquidFill
        );
        let factual_fill = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(factual_fill.cumulative_filled_quantity, 0.4);
        assert_eq!(
            factual_fill.order_status,
            OrderStatus::PartiallyFilled.to_u8()
        );

        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::OrderUpdate
        );
        let terminal = HyperliquidBasicOrderMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(terminal.order_status, OrderStatus::Canceled.to_u8());
        assert_eq!(terminal.cumulative_filled_quantity, 0.4);
        assert!(!processor.pending_terminal_by_oid.contains_key(&9));
        assert!(!processor.active_order_ids.contains(&9));
    }

    #[test]
    fn terminal_watermark_rejects_stale_open_from_redundant_path() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let open = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1400
        }]});
        assert_eq!(processor.process_value_at(&open, 1500).unwrap().len(), 1);

        let canceled = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.6",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "canceled", "statusTimestamp": 1600
        }]});
        assert!(processor
            .process_value_at(&canceled, 1700)
            .unwrap()
            .is_empty());

        let stale_open = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1800
        }]});
        assert!(processor
            .process_value_at(&stale_open, 1900)
            .unwrap()
            .is_empty());
        assert_eq!(processor.pending_terminal_by_oid[&9].status_timestamp, 1600);

        let fill = json!({"channel": "userFills", "data": {
            "user": USER, "isSnapshot": false, "fills": [{
                "coin": "BTC", "px": "59950", "sz": "0.4", "side": "B", "time": 1550,
                "hash": "0xstale-open", "oid": 9, "crossed": true, "tid": 502
            }]
        }});
        let output = processor.process_value_at(&fill, 2000).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[0]).0,
            BasicAccountEventType::HyperliquidFill
        );
        let terminal = HyperliquidBasicOrderMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(terminal.order_status, OrderStatus::Canceled.to_u8());
    }

    #[test]
    fn pending_fills_keep_monotonic_cumulative_and_finish_at_orig_size() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fills = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": true, "fills": [
                {
                    "coin": "BTC", "px": "60100", "sz": "0.4", "side": "B",
                    "time": 1500, "hash": "0xabc", "oid": 77, "crossed": true, "tid": 123
                },
                {
                    "coin": "BTC", "px": "60110", "sz": "0.6", "side": "B",
                    "time": 1501, "hash": "0xdef", "oid": 77, "crossed": false, "tid": 124
                }
            ]}
        });
        assert!(processor
            .process_json_at(fills.to_string().as_bytes(), 2000)
            .unwrap()
            .is_empty());

        let order = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60200", "sz": "0", "oid": 77, "timestamp": 1000, "origSz": "1", "cloid": CLOID},
            "status": "filled", "statusTimestamp": 1600
        }]});
        let output = processor
            .process_json_at(order.to_string().as_bytes(), 2001)
            .unwrap();
        assert_eq!(output.len(), 2);
        let first = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        let second = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(first.last_filled_quantity, 0.4);
        assert_eq!(first.cumulative_filled_quantity, 0.4);
        assert_eq!(first.order_status, OrderStatus::PartiallyFilled.to_u8());
        assert_eq!(second.last_filled_quantity, 0.6);
        assert_eq!(second.cumulative_filled_quantity, 1.0);
        assert_eq!(second.order_status, OrderStatus::Filled.to_u8());
        assert!(processor
            .process_json_at(fills.to_string().as_bytes(), 2002)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unknown_fill_flushes_as_factual_external_after_wait() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fill = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": false, "fills": [{
                "coin": "@7", "px": "0.25", "sz": "12", "side": "A", "time": 1500,
                "startPosition": "0", "dir": "Sell", "closedPnl": "0", "hash": "0xabc",
                "oid": 99, "crossed": false, "fee": "0.0100", "tid": 456, "feeToken": "USDC",
                "builderFee": "0.001", "twapId": 987, "liquidation": {
                    "liquidatedUser": "0x2222222222222222222222222222222222222222",
                    "markPx": "0.2490", "method": "market"
                }
            }]}
        });
        assert!(processor
            .process_json_at(fill.to_string().as_bytes(), 2000)
            .unwrap()
            .is_empty());
        assert!(processor
            .flush_pending_fills(6999, 5000)
            .unwrap()
            .is_empty());
        let output = processor.flush_pending_fills(7000, 5000).unwrap();
        assert_eq!(output.len(), 1);
        let (kind, body) = unwrap_event(&output[0]);
        assert_eq!(kind, BasicAccountEventType::HyperliquidFill);
        let msg = HyperliquidBasicFillMsg::from_bytes(body).unwrap();
        assert_eq!(msg.order_id, 99);
        assert_eq!(msg.client_order_id, 0);
        assert_eq!(msg.venue_trade_id, 456);
        assert_eq!(msg.symbol, "PURRUSDC");
        assert_eq!(msg.last_filled_quantity, 12.0);
        assert_eq!(msg.cumulative_filled_quantity, 12.0);
        assert_eq!(msg.order_status, 0);
        assert_eq!(msg.is_maker, 1);
        assert_eq!(msg.wire_coin.as_deref(), Some("@7"));
        assert_eq!(msg.start_position.as_deref(), Some("0"));
        assert_eq!(msg.dir.as_deref(), Some("Sell"));
        assert_eq!(msg.closed_pnl.as_deref(), Some("0"));
        assert_eq!(msg.fee.as_deref(), Some("0.0100"));
        assert_eq!(msg.fee_token.as_deref(), Some("USDC"));
        assert_eq!(msg.builder_fee.as_deref(), Some("0.001"));
        assert_eq!(msg.twap_id, Some(987));
        assert_eq!(msg.liquidation_method, "market");
        assert_eq!(
            msg.liquidated_user.as_deref(),
            Some("0x2222222222222222222222222222222222222222")
        );
        assert_eq!(msg.liquidation_mark_price.as_deref(), Some("0.2490"));
    }

    #[test]
    fn unknown_fill_rejects_the_complete_frame_without_consuming_known_rows() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fills = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": true, "fills": [
                {
                    "coin": "BTC", "px": "60000", "sz": "0.1", "side": "B",
                    "time": 1500, "hash": "0xknown", "oid": 77, "crossed": true,
                    "tid": 700
                },
                {
                    "coin": "NEWCOIN", "px": "1", "sz": "2", "side": "A",
                    "time": 1501, "hash": "0xunknown", "oid": 78, "crossed": false,
                    "tid": 701
                }
            ]}
        });

        for now_ms in [2_000, 2_001] {
            let error = processor
                .process_value_at(&fills, now_ms)
                .expect_err("unknown metadata must fail closed");
            assert!(error.to_string().contains("NEWCOIN"));
            assert!(processor.seen_fills.values.is_empty());
            assert!(processor.fill_cumulative_by_oid.is_empty());
            assert!(processor.expected_fill_cumulative_by_oid.is_empty());
            assert!(processor.pending_fills.is_empty());
            assert_eq!(processor.pending_fill_count, 0);
        }
    }

    #[test]
    fn later_fill_error_does_not_commit_the_valid_prefix_or_terminal_state() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let open = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "1",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "open", "statusTimestamp": 1400
        }]});
        processor.process_value_at(&open, 1500).unwrap();
        let canceled = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.9",
                      "oid": 9, "origSz": "1", "cloid": CLOID},
            "status": "canceled", "statusTimestamp": 1600
        }]});
        assert!(processor
            .process_value_at(&canceled, 1700)
            .unwrap()
            .is_empty());
        assert!(processor.pending_terminal_by_oid.contains_key(&9));
        assert!(processor.active_order_ids.contains(&9));

        let mixed = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": false,
            "fills": [
                {"coin": "BTC", "px": "59950", "sz": "0.1", "side": "B",
                 "time": 1800, "hash": "0xgood", "oid": 9, "crossed": true, "tid": 501},
                {"coin": "@7", "px": "0.25", "sz": "0.1", "side": "B",
                 "time": 1801, "hash": "0xbad", "oid": 9, "crossed": true, "tid": 502}
            ]
        }});
        let error = processor.process_value_at(&mixed, 1900).unwrap_err();
        assert!(error.to_string().contains("instrument mismatch"));
        assert!(processor.seen_fills.values.is_empty());
        assert!(processor.fill_cumulative_by_oid.is_empty());
        assert!(processor.attributed_fill_quantity_by_oid.is_empty());
        assert_eq!(processor.fact_watermarks().fill_time_ms, None);
        assert!(processor.pending_terminal_by_oid.contains_key(&9));
        assert!(processor.active_order_ids.contains(&9));

        let valid = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": false,
            "fills": [{"coin": "BTC", "px": "59950", "sz": "0.1", "side": "B",
                       "time": 1800, "hash": "0xgood", "oid": 9, "crossed": true,
                       "tid": 501}]
        }});
        let output = processor.process_value_at(&valid, 1901).unwrap();
        assert_eq!(output.len(), 2);
        let fill = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(fill.cumulative_filled_quantity, 0.1);
        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::OrderUpdate
        );
        assert!(!processor.pending_terminal_by_oid.contains_key(&9));
        assert!(!processor.active_order_ids.contains(&9));
    }

    #[test]
    fn flushed_external_fill_is_replayed_with_late_internal_attribution_before_cancel() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fill = json!({
            "channel": "userFills",
            "data": {"user": USER, "isSnapshot": false, "fills": [{
                "coin": "BTC", "px": "59950", "sz": "0.4", "side": "B", "time": 1500,
                "hash": "0xlate-map", "oid": 909, "crossed": true, "tid": 700
            }]}
        });
        assert!(processor.process_value_at(&fill, 2000).unwrap().is_empty());
        let external = processor.flush_pending_fills(7000, 5000).unwrap();
        assert_eq!(external.len(), 1);
        assert_eq!(
            HyperliquidBasicFillMsg::from_bytes(unwrap_event(&external[0]).1)
                .unwrap()
                .client_order_id,
            0
        );

        let canceled = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0.6",
                      "oid": 909, "origSz": "1", "cloid": CLOID},
            "status": "canceled", "statusTimestamp": 8000
        }]});
        let output = processor.process_value_at(&canceled, 8001).unwrap();
        assert_eq!(output.len(), 2);
        let attributed = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(attributed.client_order_id, 42);
        assert_eq!(attributed.cumulative_filled_quantity, 0.4);
        assert_eq!(
            attributed.order_status,
            OrderStatus::PartiallyFilled.to_u8()
        );
        let terminal = HyperliquidBasicOrderMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(terminal.order_status, OrderStatus::Canceled.to_u8());
        assert_eq!(terminal.cumulative_filled_quantity, 0.4);
        assert!(!processor.late_attribution_fills.contains_key(&909));
    }

    #[test]
    fn internal_mapping_fails_closed_after_late_attribution_journal_loss() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        processor.unrecoverable_unattributed_oids.insert(909);
        let order = json!({"channel": "orderUpdates", "data": [{
            "order": {"coin": "BTC", "side": "B", "limitPx": "60000", "sz": "0",
                      "oid": 909, "origSz": "0.4", "cloid": CLOID},
            "status": "filled", "statusTimestamp": 8000
        }]});
        assert!(processor.process_value_at(&order, 8001).is_err());
        assert!(!processor.orders.contains_key(&909));
        assert!(!processor
            .seen_order_updates
            .contains(&"909:8000".to_string()));
    }

    #[test]
    fn initial_fill_snapshot_seeds_baseline_and_reconnect_snapshot_recovers_in_order() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        processor
            .seed_historical_orders(&json!([{
                "order": {"coin": "BTC", "origSz": "0.3", "sz": "0.2", "oid": 9, "cloid": CLOID},
                "status": "open"
            }]))
            .unwrap();
        let initial = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": true,
            "fills": [{
                "coin": "BTC", "px": "60000", "sz": "0.1", "side": "B", "time": 100,
                "hash": "0x1", "oid": 9, "crossed": true, "tid": 1
            }]
        }});
        assert!(processor
            .process_value_at_with_fill_snapshot_context(
                &initial,
                1000,
                FillSnapshotContext::Initial,
            )
            .unwrap()
            .is_empty());

        // The server does not promise array ordering. Stable identity ordering
        // at an equal timestamp makes cumulative quantities deterministic.
        let reconnect = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": true,
            "fills": [
                {"coin": "BTC", "px": "60002", "sz": "0.1", "side": "B", "time": 200,
                 "hash": "0x3", "oid": 9, "crossed": true, "tid": 3},
                {"coin": "BTC", "px": "60000", "sz": "0.1", "side": "B", "time": 100,
                 "hash": "0x1", "oid": 9, "crossed": true, "tid": 1},
                {"coin": "BTC", "px": "60001", "sz": "0.1", "side": "B", "time": 200,
                 "hash": "0x2", "oid": 9, "crossed": true, "tid": 2}
            ]
        }});
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &reconnect,
                2000,
                FillSnapshotContext::Reconnect,
            )
            .unwrap();
        assert_eq!(output.len(), 2);
        let (_, first_body) = unwrap_event(&output[0]);
        let (_, second_body) = unwrap_event(&output[1]);
        let first = HyperliquidBasicFillMsg::from_bytes(first_body).unwrap();
        let second = HyperliquidBasicFillMsg::from_bytes(second_body).unwrap();
        assert_eq!(first.venue_trade_id, 2);
        assert!((first.cumulative_filled_quantity - 0.2).abs() < 1e-12);
        assert_eq!(second.venue_trade_id, 3);
        assert!((second.cumulative_filled_quantity - 0.3).abs() < 1e-12);
        assert!(processor
            .process_value_at_with_fill_snapshot_context(
                &reconnect,
                2001,
                FillSnapshotContext::Reconnect,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn process_policy_emits_initial_fill_snapshot_in_stable_order() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        processor
            .seed_historical_orders(&json!([{
                "order": {"coin": "BTC", "origSz": "0.2", "sz": "0", "oid": 9, "cloid": CLOID},
                "status": "filled"
            }]))
            .unwrap();
        let initial = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": true,
            "fills": [
                {"coin": "BTC", "px": "60001", "sz": "0.1", "side": "B", "time": 100,
                 "hash": "0x2", "oid": 9, "crossed": true, "tid": 2},
                {"coin": "BTC", "px": "60000", "sz": "0.1", "side": "B", "time": 100,
                 "hash": "0x1", "oid": 9, "crossed": true, "tid": 1}
            ]
        }});
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &initial,
                1000,
                FillSnapshotContext::Initial,
            )
            .unwrap();
        assert_eq!(output.len(), 2);
        let first = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        let second = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(first.venue_trade_id, 1);
        assert!((first.cumulative_filled_quantity - 0.1).abs() < 1e-12);
        assert_eq!(second.venue_trade_id, 2);
        assert!((second.cumulative_filled_quantity - 0.2).abs() < 1e-12);
    }

    #[test]
    fn truncated_fill_snapshot_starts_from_historical_absolute_anchor() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        processor
            .seed_historical_orders(&json!([{
                "order": {"coin": "BTC", "origSz": "6", "sz": "1", "oid": 9, "cloid": CLOID},
                "status": "open"
            }]))
            .unwrap();
        let snapshot = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": true,
            "fills": [
                {"coin": "BTC", "px": "60000", "sz": "1", "side": "B", "time": 100,
                 "hash": "0x1", "oid": 9, "crossed": true, "tid": 1},
                {"coin": "BTC", "px": "60001", "sz": "1", "side": "B", "time": 101,
                 "hash": "0x2", "oid": 9, "crossed": true, "tid": 2}
            ]
        }});
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &snapshot,
                1000,
                FillSnapshotContext::Initial,
            )
            .unwrap();
        assert_eq!(output.len(), 2);
        let first = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        let second = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(first.cumulative_filled_quantity, 4.0);
        assert_eq!(second.cumulative_filled_quantity, 5.0);

        let live = json!({"channel": "userFills", "data": {
            "user": USER,
            "isSnapshot": false,
            "fills": [{
                "coin": "BTC", "px": "60002", "sz": "1", "side": "B", "time": 102,
                "hash": "0x3", "oid": 9, "crossed": true, "tid": 3
            }]
        }});
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &live,
                1001,
                FillSnapshotContext::Reconnect,
            )
            .unwrap();
        assert_eq!(output.len(), 1);
        let fill = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(fill.cumulative_filled_quantity, 6.0);
        assert_eq!(fill.order_status, OrderStatus::Filled.to_u8());
    }

    #[test]
    fn twap_slice_fill_uses_shared_fill_dedup_and_emits_parent_association() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        processor
            .seed_historical_orders(&json!([{
                "order": {"coin": "BTC", "origSz": "1", "sz": "1", "oid": 9, "cloid": CLOID},
                "status": "open"
            }]))
            .unwrap();
        let fill = json!({
            "coin": "BTC", "px": "60000.00", "sz": "0.25", "side": "B",
            "time": 1_725_000_000_123_i64, "startPosition": "0", "dir": "Open Long",
            "closedPnl": "0.00", "fee": "0.1250", "feeToken": "USDC",
            "hash": "0xabc", "oid": 9, "crossed": true, "tid": 71, "twapId": null
        });
        let slice = json!({"channel": "userTwapSliceFills", "data": {
            "user": USER, "isSnapshot": false,
            "twapSliceFills": [{"fill": fill.clone(), "twapId": 73}]
        }});
        let output = processor
            .process_value_at(&slice, 1_725_000_000_124)
            .unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            unwrap_event(&output[0]).0,
            BasicAccountEventType::HyperliquidFill
        );
        let fill_msg = HyperliquidBasicFillMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(fill_msg.twap_id, Some(73));
        assert_eq!(
            unwrap_event(&output[1]).0,
            BasicAccountEventType::HyperliquidTwapSliceFill
        );
        let association =
            HyperliquidTwapSliceFillMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(association.twap_id, 73);
        assert_eq!(association.venue_trade_id, 71);

        let duplicate_fill = json!({"channel": "userFills", "data": {
            "user": USER, "isSnapshot": false, "fills": [fill]
        }});
        assert!(processor
            .process_value_at(&duplicate_fill, 1_725_000_000_125)
            .unwrap()
            .is_empty());
        assert!(processor
            .process_value_at(&slice, 1_725_000_000_126)
            .unwrap()
            .is_empty());
        assert_eq!(
            processor.fact_watermarks().twap_slice_time_ms,
            Some(1_725_000_000_123)
        );
    }

    #[test]
    fn twap_frames_are_atomic_and_history_preserves_exact_known_fields() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let fill = json!({
            "coin": "BTC", "px": "60000", "sz": "0.1", "side": "A",
            "time": 1000, "hash": "0xfill", "oid": 9, "crossed": false, "tid": 8
        });
        let conflicting = json!({"channel": "userTwapSliceFills", "data": {
            "user": USER, "twapSliceFills": [
                {"fill": fill.clone(), "twapId": 10},
                {"fill": fill.clone(), "twapId": 11}
            ]
        }});
        assert!(processor.process_value_at(&conflicting, 1001).is_err());
        assert!(processor.seen_twap_slice_fills.is_empty());
        assert_eq!(processor.fact_watermarks().twap_slice_time_ms, None);

        let history_row = json!({
            "time": 1_788_587_622_i64,
            "twapId": 2_184_501,
            "state": {
                "user": USER, "coin": "BTC", "side": "B", "sz": "1.2500",
                "executedSz": "0.500", "executedNtl": "32000.1250", "minutes": 120,
                "reduceOnly": true, "randomize": false, "timestamp": 1_788_581_510_182_i64,
                "stopPx": "70000.00", "trigger": {"px": "65000.50", "above": true}
            },
            "status": {"status": "waitingForTrigger"}
        });
        let history = json!({"channel": "userTwapHistory", "data": {
            "user": USER, "isSnapshot": true, "history": [history_row.clone()]
        }});
        let output = processor.process_value_at(&history, 2000).unwrap();
        assert_eq!(output.len(), 1);
        let message = HyperliquidTwapHistoryMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(message.event_time, 1_788_587_622);
        assert_eq!(message.size, "1.2500");
        assert_eq!(message.executed_size, "0.500");
        assert_eq!(message.executed_notional, "32000.1250");
        assert_eq!(message.stop_price.as_deref(), Some("70000.00"));
        assert_eq!(message.trigger_price.as_deref(), Some("65000.50"));
        assert_eq!(message.trigger_above, Some(true));
        assert_eq!(message.status, "waitingForTrigger");
        assert_eq!(
            processor.fact_watermarks().twap_history_time_s,
            Some(1_788_587_622)
        );
        assert!(processor
            .process_value_at(&history, 2001)
            .unwrap()
            .is_empty());

        let mut changed = history_row;
        changed["state"]["executedSz"] = Value::String("0.501".to_string());
        let conflict = json!({"channel": "userTwapHistory", "data": {
            "user": USER, "history": [changed]
        }});
        assert!(processor.process_value_at(&conflict, 2002).is_err());
        assert_eq!(processor.seen_twap_history.len(), 1);
    }

    #[test]
    fn spot_snapshot_emits_changed_rows_and_zeroes_omissions() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        let first = json!({"channel": "spotState", "data": {"user": USER, "spotState": {"balances": [
            {"coin": "USDC", "token": 0, "hold": "10", "total": "100", "entryNtl": "0"},
            {"coin": "PURR", "token": 1, "hold": "0", "total": "2", "entryNtl": "0.5"}
        ]}}});
        let output = processor
            .process_json_at(first.to_string().as_bytes(), 3000)
            .unwrap();
        assert_eq!(output.len(), 5);
        let native = output
            .iter()
            .filter_map(|event| {
                let (kind, body) = unwrap_event(event);
                (kind == BasicAccountEventType::HyperliquidSpotBalance)
                    .then(|| HyperliquidSpotBalanceMsg::from_bytes(body).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(native.len(), 2);
        assert_eq!(native[0].token, 0);
        assert_eq!(native[0].coin, "USDC");
        assert_eq!(native[0].total, "100");
        assert_eq!(native[0].hold, "10");
        assert_eq!(native[0].entry_ntl, "0");
        let (kind, body) = unwrap_event(output.last().unwrap());
        assert_eq!(kind, BasicAccountEventType::HyperliquidSnapshotComplete);
        let complete = HyperliquidSnapshotCompleteMsg::from_bytes(body).unwrap();
        assert_eq!(complete.venue, TradingVenue::HyperliquidMargin.to_u8());
        assert_eq!(complete.timestamp, 3000);

        let second = json!({"channel": "spotState", "data": {"user": USER, "spotState": {"balances": [
            {"coin": "USDC", "token": 0, "hold": "10", "total": "100", "entryNtl": "0"}
        ]}}});
        let output = processor
            .process_json_at(second.to_string().as_bytes(), 4000)
            .unwrap();
        assert_eq!(output.len(), 4);
        let balances = output
            .iter()
            .filter_map(|event| {
                let (kind, body) = unwrap_event(event);
                (kind == BasicAccountEventType::BalanceUpdate)
                    .then(|| BasicBalanceMsg::from_bytes(body).unwrap())
            })
            .collect::<Vec<_>>();
        assert!(balances
            .iter()
            .any(|msg| msg.symbol == "PURR" && msg.wallet == 0.0));
        assert!(balances
            .iter()
            .any(|msg| msg.symbol == "USDC" && msg.wallet == 100.0));
        let repeat = processor
            .process_json_at(second.to_string().as_bytes(), 5000)
            .unwrap();
        assert_eq!(repeat.len(), 3);
        assert_eq!(
            unwrap_event(repeat.last().unwrap()).0,
            BasicAccountEventType::HyperliquidSnapshotComplete
        );
    }

    #[test]
    fn empty_snapshots_complete_without_synthetic_balance_or_position_rows() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        let spot = processor
            .apply_spot_snapshot(&json!({"balances": []}), 1000)
            .unwrap();
        assert_eq!(spot.len(), 1);
        assert_eq!(
            unwrap_event(&spot[0]).0,
            BasicAccountEventType::HyperliquidSnapshotComplete
        );

        let perp = processor
            .apply_all_dexs_clearinghouse_snapshot(&json!({"": perp_state(json!([]), "0")}), 1001)
            .unwrap();
        assert_eq!(perp.len(), 3);
        assert_eq!(unwrap_event(&perp[0]).0, BasicAccountEventType::AccountRisk);
        assert_eq!(
            unwrap_event(&perp[1]).0,
            BasicAccountEventType::HyperliquidPerpDexState
        );
        assert_eq!(
            unwrap_event(&perp[2]).0,
            BasicAccountEventType::HyperliquidSnapshotComplete
        );
    }

    #[test]
    fn portfolio_margin_uses_venue_ratio_and_does_not_invent_per_dex_risk() {
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::PortfolioMargin,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        processor
            .seed_borrow_lend_user_state(
                &json!({"tokenToState":[],"health":"healthy","healthFactor":null}),
                1000,
            )
            .unwrap();
        let spot = processor
            .apply_spot_snapshot(
                &json!({"portfolioMarginRatio":"0.25", "balances": [{
                    "coin": "USDC", "token": 0, "total": "125.500",
                    "hold": "2.0", "entryNtl": "0"
                }]}),
                1000,
            )
            .unwrap();
        assert!(spot.iter().all(|event| {
            let (_, scope, _) = split_basic_account_event(event).unwrap();
            scope == BasicAccountScope::HyperliquidPortfolioMargin
        }));
        let risks = spot
            .iter()
            .filter_map(|event| {
                let (kind, _, body) = split_basic_account_event(event)?;
                (kind == BasicAccountEventType::AccountRisk)
                    .then(|| BasicAccountRiskMsg::from_bytes(body).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(risks.len(), 1);
        assert!((risks[0].margin_ratio - 3.8).abs() < 1e-12);
        assert!(spot.iter().any(|event| {
            matches!(
                split_basic_account_event(event),
                Some((BasicAccountEventType::HyperliquidSpotBalance, _, _))
            )
        }));

        let perp = processor
            .apply_all_dexs_clearinghouse_snapshot(
                &json!({"": perp_state(json!([{"type": "oneWay", "position": {
                    "coin": "BTC", "szi": "0.25", "unrealizedPnl": "3.75",
                    "leverage": {"type": "cross", "value": 4}, "marginUsed": "10"
                }}]), "1.0")}),
                1001,
            )
            .unwrap();
        assert!(perp.iter().all(|event| {
            let (kind, scope, _) = split_basic_account_event(event).unwrap();
            scope == BasicAccountScope::HyperliquidPortfolioMargin
                && kind != BasicAccountEventType::AccountRisk
        }));
        assert!(perp.iter().any(|event| {
            matches!(
                split_basic_account_event(event),
                Some((BasicAccountEventType::PositionUpdate, _, _))
            )
        }));
        assert!(perp.iter().any(|event| {
            matches!(
                split_basic_account_event(event),
                Some((BasicAccountEventType::UnrealizedPnlUpdate, _, _))
            )
        }));
        assert!(perp.iter().any(|event| {
            matches!(
                split_basic_account_event(event),
                Some((BasicAccountEventType::HyperliquidPerpDexState, _, _))
            )
        }));
        assert!(processor.unified_margin_by_token.is_none());
    }

    #[test]
    fn portfolio_margin_ratio_is_required_finite_and_atomic() {
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::PortfolioMargin,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        processor
            .seed_borrow_lend_user_state(
                &json!({"tokenToState":[],"health":"healthy","healthFactor":null}),
                1000,
            )
            .unwrap();
        let mut snapshot =
            json!({"balances":[{"coin":"USDC","token":0,"total":"100","hold":"0","entryNtl":"0"}]});
        assert!(processor.apply_spot_snapshot(&snapshot, 1000).is_err());
        for invalid in [
            Value::Null,
            json!("NaN"),
            json!("inf"),
            json!(-0.1),
            json!(true),
            json!({}),
        ] {
            snapshot["portfolioMarginRatio"] = invalid;
            assert!(processor.apply_spot_snapshot(&snapshot, 1000).is_err());
            assert!(processor.balances.is_empty());
            assert!(!processor.spot_snapshot_seen);
        }
        for (ratio, expected) in [(0.0, 1.0e12), (0.475, 2.0), (0.95, 1.0), (1.9, 0.5)] {
            snapshot["portfolioMarginRatio"] = json!(ratio);
            let output = processor.apply_spot_snapshot(&snapshot, 1001).unwrap();
            let risk = output
                .iter()
                .find_map(|event| {
                    let (kind, _, body) = split_basic_account_event(event)?;
                    (kind == BasicAccountEventType::AccountRisk)
                        .then(|| BasicAccountRiskMsg::from_bytes(body).unwrap())
                })
                .unwrap();
            assert!((risk.margin_ratio - expected).abs() < 1e-12);
        }
        snapshot
            .as_object_mut()
            .unwrap()
            .remove("portfolioMarginRatio");
        snapshot["balances"][0]["total"] = json!("999");
        assert!(processor.apply_spot_snapshot(&snapshot, 1002).is_err());
        assert_eq!(processor.balances["USDC"], 100.0);
    }

    #[test]
    fn unified_risk_fails_closed_when_cross_collateral_is_unavailable() {
        let mut processor = processor(FillSnapshotPolicy::Process);
        processor
            .apply_spot_snapshot(&json!({"balances": []}), 1000)
            .unwrap();
        let events = processor
            .apply_all_dexs_clearinghouse_snapshot(&json!({"": perp_state(json!([]), "1")}), 1001)
            .unwrap();
        let (_, risk_body) = unwrap_event(&events[0]);
        let risk = BasicAccountRiskMsg::from_bytes(risk_body).unwrap();
        assert_eq!(risk.margin_ratio, 0.0);
    }

    #[test]
    fn unified_snapshot_rejects_missing_catalog_dex_before_clearing_positions() {
        let catalog = HyperliquidAssetCatalog::from_all_meta(
            &json!({"universe": [{"name": "BTC"}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "USDH", "index": 2}
                ],
                "universe": []
            }),
            &json!([null, {"name": "xyz"}]),
            &json!([
                {"collateralToken": 0, "universe": [{"name": "BTC"}]},
                {"collateralToken": 2, "universe": [{"name": "xyz:FOO"}]}
            ]),
        )
        .unwrap();
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            catalog,
            HyperliquidAccountMode::Unified,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        processor
            .positions
            .insert("XYZFOOUSDH".to_string(), (2.0, 3.0));

        assert!(processor
            .apply_all_dexs_clearinghouse_snapshot(
                &json!({"": {
                    "assetPositions": [],
                    "crossMaintenanceMarginUsed": "0"
                }}),
                1001,
            )
            .is_err());
        assert_eq!(processor.positions["XYZFOOUSDH"], (2.0, 3.0));
        assert!(processor.unified_margin_by_token.is_none());
    }

    #[test]
    fn unified_all_dex_snapshot_emits_exact_safe_ratio_and_zeroes_positions() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        processor
            .apply_spot_snapshot(
                &json!({"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0", "entryNtl": "0"}]}),
                2000,
            )
            .unwrap();
        let state = |positions: Value| json!({"channel": "allDexsClearinghouseState", "data": {"user": USER, "clearinghouseStates": [["", perp_state(positions, "100")]]}});
        let first = state(json!([{"type": "oneWay", "position": {
            "coin": "BTC", "szi": "0.5", "unrealizedPnl": "25",
            "leverage": {"type": "isolated", "value": 10}, "marginUsed": "200"
        }}]));
        let output = processor
            .process_json_at(first.to_string().as_bytes(), 3000)
            .unwrap();
        assert_eq!(output.len(), 5);
        let (_, position_body) = unwrap_event(&output[0]);
        let position = BasicPositionMsg::from_bytes(position_body).unwrap();
        assert_eq!(position.inst_id, "BTCUSDC");
        assert_eq!(position.position_side, 'N');
        assert_eq!(position.position_amount, 0.5);
        let (_, risk_body) = unwrap_event(&output[2]);
        let risk = BasicAccountRiskMsg::from_bytes(risk_body).unwrap();
        assert!((risk.margin_ratio - 8.0).abs() < 1e-12);
        assert!(risk.actual_equity_usd.is_nan());

        let second = json!({"channel": "allDexsClearinghouseState", "data": {"user": USER, "clearinghouseStates": {"": perp_state(json!([]), "0")}}});
        let output = processor
            .process_json_at(second.to_string().as_bytes(), 4000)
            .unwrap();
        assert_eq!(output.len(), 5);
        let (_, position_body) = unwrap_event(&output[0]);
        assert_eq!(
            BasicPositionMsg::from_bytes(position_body)
                .unwrap()
                .position_amount,
            0.0
        );
        let (_, pnl_body) = unwrap_event(&output[1]);
        assert_eq!(
            BasicUmUnrealizedMsg::from_bytes(pnl_body)
                .unwrap()
                .unrealized_pnl,
            0.0
        );
        let (_, risk_body) = unwrap_event(&output[2]);
        assert_eq!(
            BasicAccountRiskMsg::from_bytes(risk_body)
                .unwrap()
                .margin_ratio,
            1.0e12
        );
    }

    #[test]
    fn standard_all_dex_snapshot_emits_every_position_and_only_default_dex_risk() {
        let catalog = HyperliquidAssetCatalog::from_all_meta(
            &json!({"universe": [{"name": "BTC"}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "USDH", "index": 2}
                ],
                "universe": []
            }),
            &json!([null, {"name": "xyz"}]),
            &json!([
                {"collateralToken": 0, "universe": [{"name": "BTC"}]},
                {"collateralToken": 2, "universe": [{"name": "xyz:FOO"}]}
            ]),
        )
        .unwrap();
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            catalog,
            HyperliquidAccountMode::Standard,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        let default_state = json!({
            "assetPositions": [{"type": "oneWay", "position": {
                "coin": "BTC", "szi": "0.25", "unrealizedPnl": "3.1250",
                "leverage": {"type": "cross", "value": 10}
            }}],
            "marginSummary": {"accountValue": "100.0000", "totalNtlPos": "25.00", "totalRawUsd": "75.000", "totalMarginUsed": "2.500"},
            "crossMarginSummary": {"accountValue": "90.000", "totalNtlPos": "25.00", "totalRawUsd": "65.0000", "totalMarginUsed": "2.500"},
            "crossMaintenanceMarginUsed": "1.2500",
            "withdrawable": "70.12500"
        });
        let xyz_state = json!({
            "assetPositions": [{"type": "oneWay", "position": {
                "coin": "xyz:FOO", "szi": "-2.50", "unrealizedPnl": "7.7500",
                "leverage": {"type": "isolated", "value": 5}, "marginUsed": "4.000"
            }}],
            "marginSummary": {"accountValue": "250.12500", "totalNtlPos": "40.00", "totalRawUsd": "210.125", "totalMarginUsed": "4.000"},
            "crossMarginSummary": {"accountValue": "225.500", "totalNtlPos": "0.00", "totalRawUsd": "225.5000", "totalMarginUsed": "0.000"},
            "crossMaintenanceMarginUsed": "0.0000",
            "withdrawable": "206.12500"
        });
        let events = processor
            .apply_all_dexs_clearinghouse_snapshot(
                &json!([["xyz", xyz_state.clone()], ["", default_state.clone()]]),
                5_000,
            )
            .unwrap();

        let mut positions = HashMap::new();
        let mut unrealized_pnls = HashMap::new();
        let mut dex_states = Vec::new();
        let mut balances = Vec::new();
        let mut risks = Vec::new();
        for event in &events {
            let (kind, scope, body) = split_basic_account_event(event).unwrap();
            assert_eq!(scope, BasicAccountScope::HyperliquidStdPerp);
            match kind {
                BasicAccountEventType::PositionUpdate => {
                    let row = BasicPositionMsg::from_bytes(body).unwrap();
                    positions.insert(row.inst_id, row.position_amount);
                }
                BasicAccountEventType::UnrealizedPnlUpdate => {
                    let row = BasicUmUnrealizedMsg::from_bytes(body).unwrap();
                    unrealized_pnls.insert(row.inst_id, row.unrealized_pnl);
                }
                BasicAccountEventType::HyperliquidPerpDexState => {
                    dex_states.push(HyperliquidPerpDexStateMsg::from_bytes(body).unwrap());
                }
                BasicAccountEventType::BalanceUpdate => {
                    balances.push(BasicBalanceMsg::from_bytes(body).unwrap());
                }
                BasicAccountEventType::AccountRisk => {
                    risks.push(BasicAccountRiskMsg::from_bytes(body).unwrap());
                }
                _ => {}
            }
        }
        assert_eq!(positions["BTCUSDC"], 0.25);
        assert_eq!(positions["XYZFOOUSDH"], -2.5);
        assert_eq!(unrealized_pnls["BTCUSDC"], 3.125);
        assert_eq!(unrealized_pnls["XYZFOOUSDH"], 7.75);
        assert_eq!(dex_states.len(), 2);
        assert_eq!(dex_states[0].dex, "");
        assert_eq!(dex_states[0].collateral_token, 0);
        assert_eq!(dex_states[0].margin_account_value, "100.0000");
        assert_eq!(dex_states[0].cross_maintenance_margin_used, "1.2500");
        assert_eq!(dex_states[1].dex, "xyz");
        assert_eq!(dex_states[1].collateral_token, 2);
        assert_eq!(dex_states[1].margin_account_value, "250.12500");
        assert_eq!(dex_states[1].withdrawable, "206.12500");
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].symbol, "USDC");
        assert_eq!(balances[0].wallet, 75.0);
        assert_eq!(risks.len(), 1);
        assert_eq!(risks[0].adj_equity_usd, 90.0);
        assert_eq!(risks[0].notional_usd, 25.0);

        let mut malformed_xyz = xyz_state;
        malformed_xyz
            .as_object_mut()
            .unwrap()
            .remove("withdrawable");
        assert!(processor
            .apply_all_dexs_clearinghouse_snapshot(
                &json!([["", default_state], ["xyz", malformed_xyz]]),
                6_000,
            )
            .is_err());
        assert_eq!(processor.positions["BTCUSDC"], (0.25, 3.125));
        assert_eq!(processor.positions["XYZFOOUSDH"], (-2.5, 7.75));
    }

    #[test]
    fn standard_mode_separates_spot_and_perp_scopes() {
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::Standard,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        let spot = processor
            .apply_spot_snapshot(
                &json!({"balances": [{"coin": "USDC", "token": 0, "total": "100", "hold": "0", "entryNtl": "0"}]}),
                1000,
            )
            .unwrap();
        let (_, scope, _) = split_basic_account_event(&spot[0]).unwrap();
        assert_eq!(scope, BasicAccountScope::HyperliquidStdSpot);

        let perp = processor
            .apply_clearinghouse_snapshot(
                &json!({
                    "assetPositions": [],
                    "marginSummary": {"accountValue": "80", "totalNtlPos": "0", "totalRawUsd": "80", "totalMarginUsed": "0"},
                    "crossMarginSummary": {"accountValue": "80", "totalNtlPos": "0", "totalRawUsd": "80", "totalMarginUsed": "0"},
                    "crossMaintenanceMarginUsed": "0",
                    "withdrawable": "80"
                }),
                1000,
            )
            .unwrap();
        assert_eq!(perp.len(), 4);
        for event in perp {
            let (_, scope, _) = split_basic_account_event(&event).unwrap();
            assert_eq!(scope, BasicAccountScope::HyperliquidStdPerp);
        }
    }

    #[test]
    fn standard_risk_uses_cross_summary_not_isolated_account_value() {
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            catalog(),
            HyperliquidAccountMode::Standard,
            FillSnapshotPolicy::Process,
        )
        .unwrap();
        let output = processor
            .apply_clearinghouse_snapshot(
                &json!({
                    "assetPositions": [],
                    "marginSummary": {
                        "accountValue": "1000",
                        "totalNtlPos": "900",
                        "totalRawUsd": "100",
                        "totalMarginUsed": "300"
                    },
                    "crossMarginSummary": {
                        "accountValue": "100",
                        "totalNtlPos": "80",
                        "totalRawUsd": "20",
                        "totalMarginUsed": "20"
                    },
                    "crossMaintenanceMarginUsed": "50",
                    "withdrawable": "60"
                }),
                1000,
            )
            .unwrap();
        let risk = output
            .iter()
            .find_map(|event| {
                let (kind, scope, body) = split_basic_account_event(event).unwrap();
                assert_eq!(scope, BasicAccountScope::HyperliquidStdPerp);
                (kind == BasicAccountEventType::AccountRisk)
                    .then(|| BasicAccountRiskMsg::from_bytes(body).unwrap())
            })
            .unwrap();
        assert_eq!(risk.adj_equity_usd, 100.0);
        assert_eq!(risk.actual_equity_usd, 1000.0);
        assert_eq!(risk.maintenance_margin_usd, 50.0);
        assert_eq!(risk.initial_margin_usd, 20.0);
        assert_eq!(risk.margin_ratio, 2.0);
        assert_eq!(risk.notional_usd, 80.0);
    }

    #[test]
    fn funding_and_ledger_snapshots_emit_unseen_facts_in_stable_order() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        let funding = json!({
            "channel": "userFundings",
            "data": {
                "isSnapshot": true,
                "user": USER,
                "fundings": [
                    {"time": 1001, "coin": "xyz:FOO", "usdc": "0.1250", "szi": "-2", "fundingRate": "-0.0002"},
                    {"time": 1000, "coin": "BTC", "usdc": "-0.2500", "szi": "1.5", "fundingRate": "0.0001", "hash": "0xfunding"},
                    {"time": 1000, "coin": "BTC", "usdc": "-0.2500", "szi": "1.5", "fundingRate": "0.0001", "hash": "0xfunding"}
                ]
            }
        });
        let output = processor
            .process_value_at_with_fill_snapshot_context(
                &funding,
                2000,
                FillSnapshotContext::Initial,
            )
            .unwrap();
        assert_eq!(output.len(), 2);
        let first = HyperliquidFundingMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        let second = HyperliquidFundingMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(first.event_time, 1000);
        assert_eq!(first.coin, "BTC");
        assert_eq!(first.usdc, "-0.2500");
        assert_eq!(first.transaction_hash.as_deref(), Some("0xfunding"));
        assert_eq!(processor.fact_watermarks().funding_time_ms, Some(1001));
        assert_eq!(second.event_time, 1001);
        assert_eq!(second.coin, "xyz:FOO");
        assert!(processor
            .process_value_at_with_fill_snapshot_context(
                &funding,
                2001,
                FillSnapshotContext::Reconnect,
            )
            .unwrap()
            .is_empty());

        let funding_hash_enrichment = json!({
            "channel": "userFundings",
            "data": {
                "user": USER,
                "fundings": [{
                    "time": 1001,
                    "coin": "xyz:FOO",
                    "usdc": "0.1250",
                    "szi": "-2",
                    "fundingRate": "-0.0002",
                    "hash": "0xenriched"
                }]
            }
        });
        let output = processor
            .process_value_at(&funding_hash_enrichment, 2002)
            .unwrap();
        assert_eq!(output.len(), 1);
        let enriched = HyperliquidFundingMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(enriched.transaction_hash.as_deref(), Some("0xenriched"));
        assert!(processor
            .process_value_at(&funding_hash_enrichment, 2003)
            .unwrap()
            .is_empty());
        let conflicting_funding_hash = json!({
            "channel": "userFundings",
            "data": {
                "user": USER,
                "fundings": [{
                    "time": 1001,
                    "coin": "xyz:FOO",
                    "usdc": "0.1250",
                    "szi": "-2",
                    "fundingRate": "-0.0002",
                    "hash": "0xconflict"
                }]
            }
        });
        assert!(processor
            .process_value_at(&conflicting_funding_hash, 2004)
            .unwrap_err()
            .to_string()
            .contains("conflicting recovered Hyperliquid funding hash"));

        let ledger = json!({
            "channel": "userNonFundingLedgerUpdates",
            "data": {
                "isSnapshot": true,
                "user": USER,
                "nonFundingLedgerUpdates": [
                    {"time": 1003, "hash": "0xdef", "delta": {
                        "type": "futureLedgerVariant", "nested": {"asset": "HYPE"}, "amount": "1.2500"
                    }},
                    {"time": 1002, "hash": "0xabc", "delta": {"type": "deposit", "usdc": "10.00"}}
                ]
            }
        });
        let output = processor.process_value_at(&ledger, 2002).unwrap();
        assert_eq!(output.len(), 2);
        let first = HyperliquidLedgerMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        let second = HyperliquidLedgerMsg::from_bytes(unwrap_event(&output[1]).1).unwrap();
        assert_eq!(first.transaction_hash, "0xabc");
        assert_eq!(first.delta_type, "deposit");
        assert_eq!(
            serde_json::from_str::<Value>(&first.delta_json).unwrap(),
            json!({"type": "deposit", "usdc": "10.00"})
        );
        assert_eq!(second.transaction_hash, "0xdef");
        assert_eq!(second.delta_type, "futureLedgerVariant");
        assert_eq!(processor.fact_watermarks().ledger_time_ms, Some(1003));
        assert_eq!(
            serde_json::from_str::<Value>(&second.delta_json).unwrap(),
            json!({"type": "futureLedgerVariant", "nested": {"asset": "HYPE"}, "amount": "1.2500"})
        );
        assert!(processor
            .process_value_at(&ledger, 2003)
            .unwrap()
            .is_empty());

        let wrong_user = json!({
            "channel": "userFundings",
            "data": {
                "user": "0x2222222222222222222222222222222222222222",
                "fundings": []
            }
        });
        assert!(processor
            .process_json_at(wrong_user.to_string().as_bytes(), 2000)
            .is_err());
        let malformed_ledger = json!({
            "channel": "userNonFundingLedgerUpdates",
            "data": {"user": USER, "nonFundingLedgerUpdates": [
                {"time": 1004, "hash": "0xnew", "delta": {"type": "withdraw", "usdc": "2"}},
                {"time": 1005, "hash": "0xbad", "delta": {}}
            ]}
        });
        assert!(processor
            .process_json_at(malformed_ledger.to_string().as_bytes(), 2000)
            .is_err());
        assert_eq!(processor.fact_watermarks().ledger_time_ms, Some(1003));
        let valid_after_reject = json!({
            "channel": "userNonFundingLedgerUpdates",
            "data": {"user": USER, "nonFundingLedgerUpdates": [{
                "time": 1004, "hash": "0xnew", "delta": {"type": "withdraw", "usdc": "2"}
            }]}
        });
        assert_eq!(
            processor
                .process_value_at(&valid_after_reject, 2004)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(processor.fact_watermarks().ledger_time_ms, Some(1004));
    }

    #[test]
    fn oversized_ledger_rejects_the_complete_frame_without_consuming_valid_rows() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        let valid_row = json!({
            "time": 1000,
            "hash": "0xvalid",
            "delta": {"type": "deposit", "usdc": "10.00"}
        });
        let oversized = json!({
            "channel": "userNonFundingLedgerUpdates",
            "data": {"user": USER, "isSnapshot": false, "nonFundingLedgerUpdates": [
                valid_row.clone(),
                {"time": 1001, "hash": "0xoversized", "delta": {
                    "type": "futureLedgerVariant", "audit": "x".repeat(PM_MAX_BYTES)
                }}
            ]}
        });

        let error = processor.process_value_at(&oversized, 2000).unwrap_err();
        assert!(error.to_string().contains("exceeds the PM envelope"));
        assert!(processor.seen_ledger_updates.values.is_empty());
        assert_eq!(processor.fact_watermarks().ledger_time_ms, None);

        let valid = json!({
            "channel": "userNonFundingLedgerUpdates",
            "data": {"user": USER, "isSnapshot": false, "nonFundingLedgerUpdates": [valid_row]}
        });
        let output = processor.process_value_at(&valid, 2001).unwrap();
        assert_eq!(output.len(), 1);
        assert!(output[0].len() <= PM_MAX_BYTES);
        let decoded = HyperliquidLedgerMsg::from_bytes(unwrap_event(&output[0]).1).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&decoded.delta_json).unwrap(),
            json!({"type": "deposit", "usdc": "10.00"})
        );
    }

    #[test]
    fn spot_state_uses_the_same_alias_as_spot_fills_and_market_data() {
        let alias_catalog = HyperliquidAssetCatalog::from_meta(
            &json!({"universe": [{"name": "BTC"}]}),
            &json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "UBTC", "index": 1}
                ],
                "universe": [{"name": "@1", "tokens": [1, 0], "index": 1}]
            }),
        )
        .unwrap();
        let mut processor = HyperliquidAccountProcessor::new(
            USER,
            alias_catalog,
            HyperliquidAccountMode::Standard,
            FillSnapshotPolicy::Ignore,
        )
        .unwrap();
        let events = processor
            .apply_spot_snapshot(
                &json!({"balances": [{"coin": "UBTC", "token": 1, "total": "0.5", "hold": "0.1", "entryNtl": "100"}]}),
                1000,
            )
            .unwrap();
        let (_, scope, body) = split_basic_account_event(&events[0]).unwrap();
        assert_eq!(scope, BasicAccountScope::HyperliquidStdSpot);
        let balance = BasicBalanceMsg::from_bytes(body).unwrap();
        assert_eq!(balance.symbol, "BTC");
    }

    #[test]
    fn account_addresses_are_trimmed_and_lowercase_normalized() {
        assert_eq!(
            normalize_hyperliquid_address("  0XABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD  ")
                .unwrap(),
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        assert!(normalize_hyperliquid_address("abcdef").is_err());
        assert!(
            normalize_hyperliquid_address("0xgggggggggggggggggggggggggggggggggggggggg").is_err()
        );
    }

    #[test]
    fn rejects_account_mismatch_without_mutating_snapshot_state() {
        let mut processor = processor(FillSnapshotPolicy::Ignore);
        let wrong = json!({"channel": "spotState", "data": {"user": "0x2222222222222222222222222222222222222222", "spotState": {"balances": []}}});
        assert!(processor
            .process_json_at(wrong.to_string().as_bytes(), 3000)
            .is_err());
        let valid = json!({"channel": "spotState", "data": {"user": USER, "spotState": {"balances": [{"coin": "USDC", "token": 0, "total": "10", "hold": "0", "entryNtl": "0"}]}}});
        assert_eq!(
            processor
                .process_json_at(valid.to_string().as_bytes(), 4000)
                .unwrap()
                .len(),
            3
        );
    }
}
