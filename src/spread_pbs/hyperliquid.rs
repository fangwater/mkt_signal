//! Hyperliquid public BBO, trade, L2 snapshot, and derivatives adapter.
//!
//! Hyperliquid spot and perpetual markets share one WebSocket endpoint, but a
//! spot wire coin is usually `@<spot-index>` while the perpetual wire coin is
//! the exact metadata name. The catalog preserves the existing USDC spot/default
//! perp intersection while naming every perp from its full wire coin and
//! collateral token (for example `xyz:FOO` + `USDH` => `XYZFOOUSDH`).

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use order_common::TradingVenue;
use runtime_common::fast_hash::{fast_hash_map_with_capacity, FastHashMap};
use runtime_common::symbol_util::{hyperliquid_internal_symbol, HyperliquidSpotBaseResolver};
use runtime_common::time_util::get_timestamp_us;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use crate::spread_pbs::adapter::{
    BboDedupPolicy, BboFrame, IncrementalDedupPolicy, IncrementalFrame, KeepaliveSpec,
    SubscriptionAckPolicy, TradeDedupPolicy, TradeFrame, VenueAdapter,
};
use mkt_parsers::msg::mkt_msg::{FundingRateMsg, IndexPriceMsg, Level, MarkPriceMsg};
use mkt_parsers::msg::open_interest_msg::OpenInterestMsg;
use signal_common::hyperliquid::HyperliquidEndpoints;

const CATALOG_COALESCE_WINDOW: Duration = Duration::from_secs(5);
const INTERNAL_QUOTE: &str = "USDC";

#[derive(Clone)]
struct SubscriptionDemand {
    per_leg: usize,
    sources: [Option<std::net::IpAddr>; 2],
}

impl SubscriptionDemand {
    fn new(per_leg: usize, primary: &str, secondary: &str) -> Result<Self> {
        Ok(Self {
            per_leg,
            sources: [Self::parse_source(primary)?, Self::parse_source(secondary)?],
        })
    }

    fn parse_source(raw: &str) -> Result<Option<std::net::IpAddr>> {
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let ip = raw
            .trim()
            .parse::<std::net::IpAddr>()
            .context("invalid Hyperliquid market-data source IP")?;
        Ok((!ip.is_unspecified()).then_some(ip))
    }
}

#[derive(Default)]
struct SubscriptionBudgetState {
    next_id: u64,
    demands: HashMap<u64, SubscriptionDemand>,
}

impl SubscriptionBudgetState {
    fn validate(&self, replacement_id: u64, replacement: &SubscriptionDemand) -> Result<()> {
        const MAX_SUBSCRIPTIONS_PER_IP: usize = 1_000;
        let mut counts = HashMap::<std::net::IpAddr, usize>::new();
        let mut unbound = 0_usize;
        for demand in self
            .demands
            .iter()
            .filter(|(id, _)| **id != replacement_id)
            .map(|(_, demand)| demand)
            .chain(std::iter::once(replacement))
        {
            for source in demand.sources {
                let count = match source {
                    Some(ip) => counts.entry(ip).or_default(),
                    None => &mut unbound,
                };
                *count = count.saturating_add(demand.per_leg);
            }
        }
        // Unbound legs may use any of the explicit source routes. Distinct
        // local IPs must also have distinct public egress, verified by ops.
        let per_ip = counts
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_add(unbound);
        if per_ip > MAX_SUBSCRIPTIONS_PER_IP {
            bail!(
                "Hyperliquid market-data subscription capacity exceeded: \
                 {per_ip} per source IP across this process, limit={MAX_SUBSCRIPTIONS_PER_IP}; \
                 reduce SPREAD_PBS_SYMBOLS or use explicitly partitioned collectors with distinct \
                 public egress IPs; more connections on the same IP do not increase capacity"
            );
        }
        Ok(())
    }

    fn reserve(&mut self, demand: SubscriptionDemand) -> Result<u64> {
        let id = self
            .next_id
            .checked_add(1)
            .context("Hyperliquid subscription reservation id exhausted")?;
        self.validate(id, &demand)?;
        self.demands.insert(id, demand);
        self.next_id = id;
        Ok(id)
    }

    fn grow(&mut self, id: u64, per_leg: usize) -> Result<()> {
        let mut demand = self
            .demands
            .get(&id)
            .context("missing Hyperliquid subscription reservation")?
            .clone();
        // Rolling refresh keeps the other leg alive. Retain the peak budget
        // until shutdown so a smaller first leg cannot release its peer's quota.
        demand.per_leg = demand.per_leg.max(per_leg);
        self.validate(id, &demand)?;
        self.demands.insert(id, demand);
        Ok(())
    }
}

fn subscription_budget() -> &'static std::sync::Mutex<SubscriptionBudgetState> {
    static BUDGET: OnceLock<std::sync::Mutex<SubscriptionBudgetState>> = OnceLock::new();
    BUDGET.get_or_init(Default::default)
}

pub(super) struct HyperliquidSubscriptionBudget(u64);

impl HyperliquidSubscriptionBudget {
    pub(super) fn reserve(per_leg: usize, primary: &str, secondary: &str) -> Result<Self> {
        let demand = SubscriptionDemand::new(per_leg, primary, secondary)?;
        let id = subscription_budget()
            .lock()
            .map_err(|_| anyhow::anyhow!("Hyperliquid subscription budget lock poisoned"))?
            .reserve(demand)?;
        Ok(Self(id))
    }

    pub(super) fn grow(&self, per_leg: usize) -> Result<()> {
        subscription_budget()
            .lock()
            .map_err(|_| anyhow::anyhow!("Hyperliquid subscription budget lock poisoned"))?
            .grow(self.0, per_leg)
    }
}

impl Drop for HyperliquidSubscriptionBudget {
    fn drop(&mut self) {
        if let Ok(mut budget) = subscription_budget().lock() {
            budget.demands.remove(&self.0);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MarketRoute {
    wire_coin: String,
    internal_symbol: String,
}

#[derive(Debug)]
struct HyperliquidCatalog {
    spot_symbols: Vec<String>,
    perp_symbols: Vec<String>,
    spot_by_internal: HashMap<String, MarketRoute>,
    perp_by_internal: HashMap<String, MarketRoute>,
}

impl HyperliquidCatalog {
    fn from_values(perp_dexs: Value, all_perp_metas: Value, spot_meta: Value) -> Result<Self> {
        let perp_dexs: Vec<Option<PerpDex>> =
            serde_json::from_value(perp_dexs).context("decode Hyperliquid perpDexs")?;
        let all_perp_metas: Vec<PerpMeta> =
            serde_json::from_value(all_perp_metas).context("decode Hyperliquid allPerpMetas")?;
        let spot_meta: SpotMeta =
            serde_json::from_value(spot_meta).context("decode Hyperliquid spot meta")?;
        if perp_dexs.len() != all_perp_metas.len() {
            bail!(
                "Hyperliquid perpDexs/allPerpMetas length mismatch: {} != {}",
                perp_dexs.len(),
                all_perp_metas.len()
            );
        }

        let mut token_by_index = HashMap::<u32, String>::new();
        for token in &spot_meta.tokens {
            if token.name.is_empty() {
                bail!(
                    "Hyperliquid spot token index={} has an empty name",
                    token.index
                );
            }
            if token.name.trim() != token.name {
                bail!(
                    "Hyperliquid spot token index={} has surrounding whitespace",
                    token.index
                );
            }
            if !token.name.is_ascii() {
                bail!(
                    "Hyperliquid spot token index={} has a non-ASCII name {:?}",
                    token.index,
                    token.name
                );
            }
            if token_by_index
                .insert(token.index, token.name.clone())
                .is_some()
            {
                bail!("duplicate Hyperliquid spot token index={}", token.index);
            }
        }

        let mut perp_by_internal = HashMap::<String, MarketRoute>::new();
        let mut perp_wire_coins = HashSet::<String>::new();
        let mut default_perp_by_base = HashMap::<String, MarketRoute>::new();
        let mut dex_names = HashSet::<String>::new();
        for (index, (dex, meta)) in perp_dexs
            .into_iter()
            .zip(all_perp_metas.into_iter())
            .enumerate()
        {
            let dex_name = match (index, dex) {
                (0, None) => String::new(),
                (0, Some(_)) => {
                    bail!("Hyperliquid perpDexs index 0 must be the null default DEX")
                }
                (_, None) => bail!("Hyperliquid perpDexs index {index} is unexpectedly null"),
                (_, Some(dex)) => {
                    validate_perp_dex_name(&dex.name)?;
                    dex.name
                }
            };
            if !dex_names.insert(dex_name.to_ascii_lowercase()) {
                bail!("duplicate Hyperliquid perp DEX name {dex_name:?}");
            }
            let collateral = token_by_index
                .get(&meta.collateral_token)
                .with_context(|| {
                    format!(
                        "Hyperliquid DEX {dex_name:?} collateral token {} is missing from spotMeta",
                        meta.collateral_token
                    )
                })?;
            if dex_name.is_empty() && !collateral.eq_ignore_ascii_case(INTERNAL_QUOTE) {
                bail!(
                    "Hyperliquid default DEX collateral must be {INTERNAL_QUOTE}, got {collateral:?}"
                );
            }

            for asset in meta.universe {
                if asset.is_delisted {
                    continue;
                }
                validate_ascii_name(&asset.name, "perp wire coin")?;
                if dex_name.is_empty() {
                    if asset.name.contains(':') {
                        bail!(
                            "Hyperliquid default DEX wire coin unexpectedly has a DEX prefix: {:?}",
                            asset.name
                        );
                    }
                } else {
                    let expected_prefix = format!("{dex_name}:");
                    if !asset.name.starts_with(&expected_prefix)
                        || asset.name.len() == expected_prefix.len()
                    {
                        bail!(
                            "Hyperliquid DEX {dex_name:?} wire coin {:?} must start with {:?}",
                            asset.name,
                            expected_prefix
                        );
                    }
                }
                if !perp_wire_coins.insert(asset.name.to_ascii_lowercase()) {
                    bail!(
                        "duplicate Hyperliquid active perp wire coin {:?}",
                        asset.name
                    );
                }
                let internal_symbol = hyperliquid_internal_symbol(&asset.name, collateral)
                    .with_context(|| {
                        format!(
                            "build Hyperliquid internal symbol coin={:?} collateral={:?}",
                            asset.name, collateral
                        )
                    })?;
                let route = MarketRoute {
                    wire_coin: asset.name.clone(),
                    internal_symbol: internal_symbol.clone(),
                };
                if let Some(previous) =
                    perp_by_internal.insert(internal_symbol.clone(), route.clone())
                {
                    bail!(
                        "Hyperliquid perp internal symbol collision {}: {:?} and {:?}",
                        internal_symbol,
                        previous.wire_coin,
                        asset.name
                    );
                }
                if dex_name.is_empty() {
                    let base = normalize_base_key(&asset.name, false).with_context(|| {
                        format!("invalid Hyperliquid default perp name {:?}", asset.name)
                    })?;
                    if default_perp_by_base.insert(base.clone(), route).is_some() {
                        bail!("duplicate Hyperliquid active default perp base {base}");
                    }
                }
            }
        }
        if !dex_names.contains("") {
            bail!("Hyperliquid perpDexs is missing the null default DEX at index 0");
        }
        let base_resolver =
            HyperliquidSpotBaseResolver::new(token_by_index.values().map(String::as_str));

        let mut spot_by_base = HashMap::<String, MarketRoute>::new();
        let mut spot_market_indices = HashSet::<u32>::new();
        let mut spot_wire_coins = HashSet::<String>::new();
        for market in spot_meta.universe {
            if market.name.trim() != market.name {
                bail!(
                    "Hyperliquid spot market index={} has surrounding whitespace",
                    market.index
                );
            }
            if !spot_market_indices.insert(market.index) {
                bail!("duplicate Hyperliquid spot market index={}", market.index);
            }
            if !spot_wire_coins.insert(market.name.clone()) {
                bail!("duplicate Hyperliquid spot wire coin={}", market.name);
            }
            let base_name = token_by_index.get(&market.tokens[0]).ok_or_else(|| {
                anyhow::anyhow!(
                    "Hyperliquid spot market index={} references missing base token index={}",
                    market.index,
                    market.tokens[0]
                )
            })?;
            let quote_name = token_by_index.get(&market.tokens[1]).ok_or_else(|| {
                anyhow::anyhow!(
                    "Hyperliquid spot market index={} references missing quote token index={}",
                    market.index,
                    market.tokens[1]
                )
            })?;
            if !quote_name.eq_ignore_ascii_case(INTERNAL_QUOTE) {
                continue;
            }
            validate_spot_wire_coin(&market.name, market.index, base_name, quote_name)?;
            let raw_base = normalize_base_key(base_name, false).with_context(|| {
                format!(
                    "invalid Hyperliquid spot base token {:?} for market index={}",
                    base_name, market.index
                )
            })?;
            let base = base_resolver.canonical_base(&raw_base);
            let internal_symbol =
                hyperliquid_internal_symbol(&base, quote_name).with_context(|| {
                    format!(
                        "build Hyperliquid spot internal symbol base={base:?} quote={quote_name:?}"
                    )
                })?;
            let route = MarketRoute {
                wire_coin: market.name,
                internal_symbol,
            };
            if spot_by_base.insert(base.clone(), route).is_some() {
                bail!("duplicate Hyperliquid USDC spot base {base}");
            }
        }

        let mut paired_bases: Vec<String> = default_perp_by_base
            .keys()
            .filter(|base| spot_by_base.contains_key(*base))
            .cloned()
            .collect();
        paired_bases.sort_unstable();

        let mut spot_symbols = Vec::with_capacity(paired_bases.len());
        let mut spot_by_internal = HashMap::with_capacity(paired_bases.len());
        for base in paired_bases {
            let spot = spot_by_base
                .remove(&base)
                .expect("paired spot route must exist");
            let perp = default_perp_by_base
                .remove(&base)
                .expect("paired perp route must exist");
            if spot.internal_symbol != perp.internal_symbol {
                bail!(
                    "Hyperliquid paired symbol mismatch base={} spot={} perp={}",
                    base,
                    spot.internal_symbol,
                    perp.internal_symbol
                );
            }
            spot_symbols.push(spot.internal_symbol.clone());
            spot_by_internal.insert(spot.internal_symbol.clone(), spot);
        }
        let mut perp_symbols = perp_by_internal.keys().cloned().collect::<Vec<_>>();
        perp_symbols.sort_unstable();

        Ok(Self {
            spot_symbols,
            perp_symbols,
            spot_by_internal,
            perp_by_internal,
        })
    }

    fn route(&self, venue: TradingVenue, internal_symbol: &str) -> Option<&MarketRoute> {
        match venue {
            TradingVenue::HyperliquidMargin => self.spot_by_internal.get(internal_symbol),
            TradingVenue::HyperliquidFutures => self.perp_by_internal.get(internal_symbol),
            _ => None,
        }
    }

    fn symbols(&self, venue: TradingVenue) -> Result<Vec<String>> {
        match venue {
            TradingVenue::HyperliquidMargin => Ok(self.spot_symbols.clone()),
            TradingVenue::HyperliquidFutures => Ok(self.perp_symbols.clone()),
            _ => bail!("unsupported Hyperliquid catalog venue {venue:?}"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PerpDex {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpMeta {
    collateral_token: u32,
    universe: Vec<PerpAsset>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpAsset {
    name: String,
    #[serde(default)]
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
struct SpotMeta {
    tokens: Vec<SpotToken>,
    universe: Vec<SpotMarket>,
}

#[derive(Debug, Deserialize)]
struct SpotToken {
    name: String,
    index: u32,
}

#[derive(Debug, Deserialize)]
struct SpotMarket {
    name: String,
    index: u32,
    tokens: [u32; 2],
}

struct CachedCatalog {
    loaded_at: Instant,
    info_url: String,
    catalog: Arc<HyperliquidCatalog>,
}

fn catalog_cache() -> &'static RwLock<Option<CachedCatalog>> {
    static CACHE: OnceLock<RwLock<Option<CachedCatalog>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(None))
}

static CATALOG_REFRESH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn read_cached_catalog() -> Option<(Instant, String, Arc<HyperliquidCatalog>)> {
    let cache = catalog_cache()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.as_ref().map(|cached| {
        (
            cached.loaded_at,
            cached.info_url.clone(),
            cached.catalog.clone(),
        )
    })
}

fn current_catalog(info_url: &str) -> Option<Arc<HyperliquidCatalog>> {
    read_cached_catalog()
        .and_then(|(_, cached_info_url, catalog)| (cached_info_url == info_url).then_some(catalog))
}

fn replace_cached_catalog(info_url: String, catalog: Arc<HyperliquidCatalog>) {
    let mut cache = catalog_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cache = Some(CachedCatalog {
        loaded_at: Instant::now(),
        info_url,
        catalog,
    });
}

fn invalidate_cached_catalog(info_url: &str) {
    let mut cache = catalog_cache()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cache
        .as_ref()
        .is_some_and(|cached| cached.info_url == info_url)
    {
        *cache = None;
    }
}

/// Refresh the complete catalog and return the symbols supported by one venue.
/// Concurrent `hyperliquid-both` legs coalesce their startup requests.
pub async fn refresh_symbols(venue: TradingVenue) -> Result<Vec<String>> {
    if !matches!(
        venue,
        TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
    ) {
        bail!("unsupported Hyperliquid catalog venue {venue:?}");
    }
    let endpoints = HyperliquidEndpoints::from_env()?;
    let _guard = CATALOG_REFRESH_LOCK.lock().await;
    if let Some((loaded_at, info_url, catalog)) = read_cached_catalog() {
        if info_url == endpoints.info_url && loaded_at.elapsed() <= CATALOG_COALESCE_WINDOW {
            return catalog.symbols(venue);
        }
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build Hyperliquid metadata HTTP client")?;
    let (perp_dexs, all_perp_metas, spot_meta) = tokio::try_join!(
        fetch_info(
            &client,
            &endpoints.info_url,
            serde_json::json!({"type": "perpDexs"}),
        ),
        fetch_info(
            &client,
            &endpoints.info_url,
            serde_json::json!({"type": "allPerpMetas"}),
        ),
        fetch_info(
            &client,
            &endpoints.info_url,
            serde_json::json!({"type": "spotMeta"}),
        ),
    )?;
    let catalog = Arc::new(HyperliquidCatalog::from_values(
        perp_dexs,
        all_perp_metas,
        spot_meta,
    )?);
    log::info!(
        "Hyperliquid catalog loaded: paired_usdc_spot={} active_perp={}",
        catalog.spot_symbols.len(),
        catalog.perp_symbols.len(),
    );
    let symbols = catalog.symbols(venue)?;
    if symbols.is_empty() {
        bail!(
            "Hyperliquid metadata has no supported symbols for {}",
            venue.data_pub_slug()
        );
    }
    replace_cached_catalog(endpoints.info_url, catalog);
    Ok(symbols)
}

/// Return the USDC spot/default-perp intersection used by cross-market consumers.
pub async fn refresh_paired_symbols() -> Result<Vec<String>> {
    refresh_symbols(TradingVenue::HyperliquidMargin).await
}

async fn fetch_info(client: &reqwest::Client, info_url: &str, payload: Value) -> Result<Value> {
    let response = client
        .post(info_url)
        .json(&payload)
        .send()
        .await
        .with_context(|| format!("request Hyperliquid info payload={payload}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!(
            "Hyperliquid info request failed status={} payload={}",
            status,
            payload
        );
    }
    response
        .json::<Value>()
        .await
        .with_context(|| format!("decode Hyperliquid info response payload={payload}"))
}

fn validate_ascii_name(raw: &str, label: &str) -> Result<()> {
    if raw.is_empty() {
        bail!("Hyperliquid {label} is empty");
    }
    if raw.trim() != raw {
        bail!("Hyperliquid {label} has surrounding whitespace: {raw:?}");
    }
    if !raw.is_ascii() {
        bail!("Hyperliquid {label} must be ASCII: {raw:?}");
    }
    if !raw.bytes().any(|byte| byte.is_ascii_alphanumeric()) {
        bail!("Hyperliquid {label} has no ASCII alphanumeric characters: {raw:?}");
    }
    Ok(())
}

fn validate_perp_dex_name(raw: &str) -> Result<()> {
    validate_ascii_name(raw, "perp DEX name")?;
    if raw.contains(':') {
        bail!("Hyperliquid perp DEX name must not contain ':': {raw:?}");
    }
    Ok(())
}

fn normalize_base_key(raw: &str, allow_colon: bool) -> Result<String> {
    let normalized = raw.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        bail!("empty asset name");
    }
    if !normalized
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || (allow_colon && byte == b':'))
    {
        bail!("unsupported asset characters in {raw:?}");
    }
    Ok(normalized)
}

fn validate_spot_wire_coin(
    wire_coin: &str,
    market_index: u32,
    base_name: &str,
    quote_name: &str,
) -> Result<()> {
    let wire_coin = wire_coin.trim();
    if wire_coin.is_empty() {
        bail!("Hyperliquid spot market index={market_index} has an empty wire coin");
    }
    if let Some(raw_index) = wire_coin.strip_prefix('@') {
        let wire_index = raw_index.parse::<u32>().with_context(|| {
            format!("invalid Hyperliquid spot wire coin {wire_coin:?} for index={market_index}")
        })?;
        if wire_index != market_index {
            bail!(
                "Hyperliquid spot wire coin/index mismatch wire={} index={}",
                wire_coin,
                market_index
            );
        }
    } else {
        let expected = format!("{base_name}/{quote_name}");
        if wire_coin != expected {
            bail!(
                "Hyperliquid non-indexed spot wire coin mismatch wire={} expected={} index={}",
                wire_coin,
                expected,
                market_index
            );
        }
    }
    Ok(())
}

struct AdapterState {
    slot_by_symbol: FastHashMap<String, usize>,
    wire_by_internal: FastHashMap<String, String>,
    internal_by_wire: FastHashMap<String, String>,
}

impl AdapterState {
    fn new() -> Self {
        Self {
            slot_by_symbol: fast_hash_map_with_capacity(32),
            wire_by_internal: fast_hash_map_with_capacity(32),
            internal_by_wire: fast_hash_map_with_capacity(32),
        }
    }

    fn ensure_slots(&mut self, symbols: &[String]) {
        for symbol in symbols {
            let next_index = self.slot_by_symbol.len();
            self.slot_by_symbol
                .entry(symbol.to_ascii_uppercase())
                .or_insert(next_index);
        }
    }

    fn refresh_routes(
        &mut self,
        venue: TradingVenue,
        symbols: &[String],
        catalog: &HyperliquidCatalog,
    ) -> Result<()> {
        let mut wire_by_internal = fast_hash_map_with_capacity(symbols.len());
        let mut internal_by_wire = fast_hash_map_with_capacity(symbols.len());
        for symbol in symbols {
            let internal = symbol.to_ascii_uppercase();
            let route = catalog.route(venue, &internal).with_context(|| {
                format!(
                    "spread_pbs[{}] missing Hyperliquid catalog route for {}",
                    venue.data_pub_slug(),
                    internal
                )
            })?;
            if wire_by_internal
                .insert(internal.clone(), route.wire_coin.clone())
                .is_some()
            {
                bail!(
                    "spread_pbs[{}] duplicate Hyperliquid subscription symbol {}",
                    venue.data_pub_slug(),
                    internal
                );
            }
            if let Some(previous) =
                internal_by_wire.insert(route.wire_coin.clone(), route.internal_symbol.clone())
            {
                bail!(
                    "spread_pbs[{}] Hyperliquid wire coin {:?} maps to both {} and {}",
                    venue.data_pub_slug(),
                    route.wire_coin,
                    previous,
                    route.internal_symbol
                );
            }
        }
        self.ensure_slots(symbols);
        self.wire_by_internal = wire_by_internal;
        self.internal_by_wire = internal_by_wire;
        Ok(())
    }
}

pub struct HyperliquidAdapter {
    venue: TradingVenue,
    endpoints: HyperliquidEndpoints,
    state: std::cell::RefCell<AdapterState>,
    catalog_override: Option<Arc<HyperliquidCatalog>>,
}

impl HyperliquidAdapter {
    pub fn new(venue: TradingVenue) -> Result<Self> {
        debug_assert!(matches!(
            venue,
            TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
        ));
        Ok(Self {
            venue,
            endpoints: HyperliquidEndpoints::from_env()?,
            state: std::cell::RefCell::new(AdapterState::new()),
            catalog_override: None,
        })
    }

    #[cfg(test)]
    fn with_catalog(venue: TradingVenue, catalog: Arc<HyperliquidCatalog>) -> Self {
        Self {
            venue,
            endpoints: HyperliquidEndpoints::mainnet(),
            state: std::cell::RefCell::new(AdapterState::new()),
            catalog_override: Some(catalog),
        }
    }

    fn catalog(&self) -> Option<Arc<HyperliquidCatalog>> {
        self.catalog_override
            .clone()
            .or_else(|| current_catalog(&self.endpoints.info_url))
    }

    fn refresh_routes(&self, symbols: &[String]) -> Result<()> {
        let Some(catalog) = self.catalog() else {
            bail!(
                "spread_pbs[{}] Hyperliquid catalog is not loaded",
                self.venue.data_pub_slug()
            );
        };
        self.state
            .borrow_mut()
            .refresh_routes(self.venue, symbols, &catalog)
    }

    fn build_channel_subscribe(&self, symbols: &[String], channel: &str) -> Vec<Value> {
        // Planning a refresh must not change the routes used by live sockets
        // before its subscription budget and publisher capacity are accepted.
        let mut state = AdapterState::new();
        let result = self
            .catalog()
            .context("Hyperliquid catalog is not loaded")
            .and_then(|catalog| state.refresh_routes(self.venue, symbols, &catalog));
        if let Err(err) = result {
            log::error!(
                "spread_pbs[{}] cannot build Hyperliquid {} subscriptions: {err:#}",
                self.venue.data_pub_slug(),
                channel
            );
            return Vec::new();
        }
        symbols
            .iter()
            .map(|symbol| {
                let internal = symbol.to_ascii_uppercase();
                let wire_coin = state
                    .wire_by_internal
                    .get(&internal)
                    .expect("validated Hyperliquid route must exist");
                let subscription = serde_json::json!({
                    "type": channel,
                    "coin": wire_coin,
                });
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": subscription,
                })
            })
            .collect()
    }

    fn internal_symbol(&self, wire_coin: &str) -> Option<String> {
        self.state.borrow().internal_by_wire.get(wire_coin).cloned()
    }
}

impl VenueAdapter for HyperliquidAdapter {
    fn name(&self) -> &'static str {
        "hyperliquid"
    }

    fn ws_url(&self) -> String {
        self.endpoints.ws_url.clone()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "bbo")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "trades")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "l2Book")
    }

    fn build_derivatives_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        if self.venue != TradingVenue::HyperliquidFutures {
            return Vec::new();
        }
        self.build_channel_subscribe(symbols, "activeAssetCtx")
    }

    fn bbo_dedup_policy(&self) -> BboDedupPolicy {
        BboDedupPolicy::RecentIdentity
    }

    fn trade_dedup_policy(&self) -> TradeDedupPolicy {
        TradeDedupPolicy::RecentIdentity
    }

    fn incremental_dedup_policy(&self) -> IncrementalDedupPolicy {
        IncrementalDedupPolicy::RecentSnapshotIdentity
    }

    fn subscription_ack_policy(&self) -> SubscriptionAckPolicy {
        SubscriptionAckPolicy::HyperliquidTypeAndCoin
    }

    fn reconnect_on_parse_error(&self) -> bool {
        true
    }

    fn on_fatal_parse_error(&self) {
        invalidate_cached_catalog(&self.endpoints.info_url);
    }

    fn seed_symbols(&self, symbols: &[String]) {
        if let Err(err) = self.refresh_routes(symbols) {
            log::error!(
                "spread_pbs[{}] cannot seed Hyperliquid routes: {err:#}",
                self.venue.data_pub_slug()
            );
        }
    }

    fn symbol_slot_index(&self, symbol: &str) -> Option<usize> {
        self.state
            .borrow()
            .slot_by_symbol
            .get(&symbol.to_ascii_uppercase())
            .copied()
    }

    fn parse_frame(
        &self,
        value: &Value,
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()> {
        if value.get("channel").and_then(Value::as_str) != Some("bbo") {
            return Ok(());
        }
        let data = value
            .get("data")
            .and_then(Value::as_object)
            .context("Hyperliquid bbo missing object data")?;
        let wire_coin = data
            .get("coin")
            .and_then(Value::as_str)
            .context("Hyperliquid bbo missing coin")?;
        let symbol = self.internal_symbol(wire_coin).with_context(|| {
            format!(
                "spread_pbs[{}] received Hyperliquid bbo for unsubscribed coin={wire_coin:?}",
                self.venue.data_pub_slug()
            )
        })?;
        let time_ms = parse_i64(data.get("time")).context("Hyperliquid bbo missing time")?;
        if time_ms <= 0 {
            bail!("Hyperliquid bbo time must be positive coin={wire_coin}");
        }
        let sides = data
            .get("bbo")
            .and_then(Value::as_array)
            .context("Hyperliquid bbo missing sides")?;
        if sides.len() != 2 {
            bail!(
                "Hyperliquid bbo expected two sides, got {} coin={}",
                sides.len(),
                wire_coin
            );
        }
        let bid = parse_bbo_level(sides.first(), wire_coin, "bid")?;
        let ask = parse_bbo_level(sides.get(1), wire_coin, "ask")?;
        // AskBidSpread is a two-sided contract. If Hyperliquid reports either
        // side as null, clear the whole quote so no consumer can retain the
        // previously published tradable pair.
        let (bid_price, bid_amount, ask_price, ask_amount) = match (bid, ask) {
            (Some((bid_price, bid_amount)), Some((ask_price, ask_amount))) => {
                (bid_price, bid_amount, ask_price, ask_amount)
            }
            _ => (0.0, 0.0, 0.0, 0.0),
        };
        // Hyperliquid has no native BBO sequence. The shared layer uses the
        // complete quote identity for this venue; keep event time here only as
        // an ordering diagnostic, never as a same-millisecond arrival ordinal.
        let seq_id = time_ms;
        emit(BboFrame {
            symbol,
            ts_us: timestamp_ms_to_us(time_ms)?,
            seq_id,
            reset_seq: false,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        })?;
        Ok(())
    }

    fn parse_trade_frame(&self, value: &Value) -> Result<Vec<TradeFrame>> {
        if value.get("channel").and_then(Value::as_str) != Some("trades") {
            return Ok(Vec::new());
        }
        let items = value
            .get("data")
            .and_then(Value::as_array)
            .context("Hyperliquid trades missing array data")?;
        if items.is_empty() {
            bail!("Hyperliquid trades data must not be empty");
        }
        let mut out = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            out.push(
                self.parse_trade_item(item)
                    .with_context(|| format!("invalid Hyperliquid trades data[{index}]"))?,
            );
        }
        Ok(out)
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        if value.get("channel").and_then(Value::as_str) != Some("l2Book") {
            return Ok(Vec::new());
        }
        let data = value
            .get("data")
            .and_then(Value::as_object)
            .context("Hyperliquid l2Book missing object data")?;
        let wire_coin = data
            .get("coin")
            .and_then(Value::as_str)
            .context("Hyperliquid l2Book missing coin")?;
        let symbol = self.internal_symbol(wire_coin).with_context(|| {
            format!(
                "spread_pbs[{}] received Hyperliquid l2Book for unsubscribed coin={wire_coin:?}",
                self.venue.data_pub_slug()
            )
        })?;
        let time_ms = parse_i64(data.get("time")).context("Hyperliquid l2Book missing time")?;
        if time_ms <= 0 {
            bail!("Hyperliquid l2Book time must be positive coin={wire_coin}");
        }
        let timestamp_us = timestamp_ms_to_us(time_ms)?;
        let levels = data
            .get("levels")
            .and_then(Value::as_array)
            .context("Hyperliquid l2Book missing levels")?;
        if levels.len() != 2 {
            bail!(
                "Hyperliquid l2Book expected two sides, got {} coin={}",
                levels.len(),
                wire_coin
            );
        }
        let bids = parse_book_side(levels.first(), wire_coin, "bids")?;
        let asks = parse_book_side(levels.get(1), wire_coin, "asks")?;

        Ok(vec![IncrementalFrame::Book {
            symbol,
            timestamp: timestamp_us,
            // Hyperliquid has no book sequence. The shared layer uses event
            // time for ordering and the complete book identity for exact
            // duplicate suppression, so different books in one millisecond
            // remain observable.
            seq_id: time_ms,
            prev_seq_id: 0,
            first_update_id: time_ms,
            final_update_id: time_ms,
            gap_check: false,
            is_snapshot: true,
            bids,
            asks,
        }])
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        if self.venue != TradingVenue::HyperliquidFutures
            || value.get("channel").and_then(Value::as_str) != Some("activeAssetCtx")
        {
            return Ok(Vec::new());
        }
        let data = value
            .get("data")
            .and_then(Value::as_object)
            .context("Hyperliquid activeAssetCtx missing object data")?;
        let wire_coin = data
            .get("coin")
            .and_then(Value::as_str)
            .context("Hyperliquid activeAssetCtx missing coin")?;
        let symbol = self.internal_symbol(wire_coin).with_context(|| {
            format!(
                "spread_pbs[{}] received Hyperliquid activeAssetCtx for unsubscribed coin={wire_coin:?}",
                self.venue.data_pub_slug()
            )
        })?;
        let ctx = data
            .get("ctx")
            .and_then(Value::as_object)
            .context("Hyperliquid activeAssetCtx missing ctx")?;
        // The subscription payload has no exchange event timestamp. Quantizing
        // receipt time to seconds preserves freshness while making identical
        // dual-path frames serialize the same for downstream byte deduplication.
        let timestamp_us = match data.get("time") {
            None | Some(Value::Null) => get_timestamp_us().div_euclid(1_000_000) * 1_000_000,
            Some(value) => {
                let time_ms = parse_i64(Some(value))
                    .context("Hyperliquid activeAssetCtx has invalid time")?;
                if time_ms <= 0 {
                    bail!("Hyperliquid activeAssetCtx time must be positive");
                }
                timestamp_ms_to_us(time_ms)?
            }
        };
        let next_funding_time_us = next_hour_us(timestamp_us);
        let mark_price = parse_positive_f64(ctx.get("markPx"))
            .context("Hyperliquid activeAssetCtx missing/invalid markPx")?;
        let index_price = parse_positive_f64(ctx.get("oraclePx"))
            .context("Hyperliquid activeAssetCtx missing/invalid oraclePx")?;
        let funding_rate = parse_f64(ctx.get("funding"))
            .filter(|value| value.is_finite())
            .context("Hyperliquid activeAssetCtx missing/invalid funding")?;
        let open_interest = parse_f64(ctx.get("openInterest"))
            .filter(|value| value.is_finite() && *value >= 0.0)
            .context("Hyperliquid activeAssetCtx missing/invalid openInterest")?;
        Ok(vec![
            MarkPriceMsg::create(symbol.clone(), mark_price, timestamp_us).to_bytes(),
            IndexPriceMsg::create(symbol.clone(), index_price, timestamp_us).to_bytes(),
            FundingRateMsg::create(
                symbol.clone(),
                funding_rate,
                next_funding_time_us,
                timestamp_us,
            )
            .to_bytes(),
            OpenInterestMsg::create(symbol, open_interest, timestamp_us).to_bytes(),
        ])
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        Some(KeepaliveSpec::text(
            Duration::from_secs(30),
            r#"{"method":"ping"}"#,
        ))
    }
}

impl HyperliquidAdapter {
    fn parse_trade_item(&self, item: &Value) -> Result<TradeFrame> {
        let item = item
            .as_object()
            .context("Hyperliquid trade row must be an object")?;
        let wire_coin = item
            .get("coin")
            .and_then(Value::as_str)
            .context("Hyperliquid trade missing coin")?;
        let symbol = self.internal_symbol(wire_coin).with_context(|| {
            format!(
                "spread_pbs[{}] received Hyperliquid trade for unsubscribed coin={wire_coin:?}",
                self.venue.data_pub_slug()
            )
        })?;
        let time_ms =
            parse_i64(item.get("time")).context("Hyperliquid trade missing/invalid time")?;
        if time_ms <= 0 {
            bail!("Hyperliquid trade time must be positive, got {time_ms}");
        }
        let timestamp_us = timestamp_ms_to_us(time_ms)?;
        let trade_id =
            parse_i64(item.get("tid")).context("Hyperliquid trade missing/invalid tid")?;
        if trade_id < 0 {
            bail!("Hyperliquid trade tid must be nonnegative, got {trade_id}");
        }
        let hash = item
            .get("hash")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("Hyperliquid trade missing/empty hash")?;
        let _ = hash;
        let users = item
            .get("users")
            .and_then(Value::as_array)
            .filter(|users| users.len() == 2)
            .context("Hyperliquid trade users must contain buyer and seller")?;
        if users
            .iter()
            .any(|user| !user.as_str().is_some_and(|address| !address.is_empty()))
        {
            bail!("Hyperliquid trade users contains an invalid address");
        }
        let side = match item
            .get("side")
            .and_then(Value::as_str)
            .context("Hyperliquid trade missing side")?
        {
            "B" => 'B',
            "A" => 'S',
            other => bail!("Hyperliquid trade has invalid side {other:?}"),
        };
        let price = parse_f64(item.get("px")).context("Hyperliquid trade missing/invalid px")?;
        let amount = parse_f64(item.get("sz")).context("Hyperliquid trade missing/invalid sz")?;
        if !price.is_finite() || !amount.is_finite() || price <= 0.0 || amount <= 0.0 {
            bail!("Hyperliquid trade px/sz must be positive finite values");
        }
        Ok(TradeFrame {
            symbol,
            timestamp_us,
            // `tid` is a non-monotonic 50-bit hash. The adapter advertises
            // RecentIdentity so the shared layer never high-water compares it.
            seq_id: trade_id,
            trade_id,
            side,
            price,
            amount,
        })
    }
}

fn parse_book_side(side: Option<&Value>, wire_coin: &str, label: &str) -> Result<Vec<Level>> {
    let levels = side
        .and_then(Value::as_array)
        .with_context(|| format!("Hyperliquid l2Book {label} is not an array coin={wire_coin}"))?;
    let mut out = Vec::with_capacity(levels.len());
    for (index, level) in levels.iter().enumerate() {
        let price = parse_f64(level.get("px")).with_context(|| {
            format!("Hyperliquid l2Book invalid {label}[{index}].px coin={wire_coin}")
        })?;
        let amount = parse_f64(level.get("sz")).with_context(|| {
            format!("Hyperliquid l2Book invalid {label}[{index}].sz coin={wire_coin}")
        })?;
        let count = parse_i64(level.get("n")).with_context(|| {
            format!("Hyperliquid l2Book invalid {label}[{index}].n coin={wire_coin}")
        })?;
        if !price.is_finite() || !amount.is_finite() || price <= 0.0 || amount <= 0.0 {
            bail!(
                "Hyperliquid l2Book invalid {label}[{index}] price={} amount={} coin={}",
                price,
                amount,
                wire_coin
            );
        }
        if count <= 0 {
            bail!(
                "Hyperliquid l2Book invalid {label}[{index}] n={} coin={}",
                count,
                wire_coin
            );
        }
        out.push(Level::from_values(price, amount));
    }
    Ok(out)
}

fn parse_bbo_level(
    value: Option<&Value>,
    wire_coin: &str,
    side: &str,
) -> Result<Option<(f64, f64)>> {
    let Some(value) = value else {
        bail!("Hyperliquid bbo missing {side} value coin={wire_coin}");
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_object()
        .with_context(|| format!("Hyperliquid bbo {side} is not an object coin={wire_coin}"))?;
    let price = parse_positive_f64(value.get("px"))
        .with_context(|| format!("Hyperliquid bbo invalid {side}.px coin={wire_coin}"))?;
    let amount = parse_positive_f64(value.get("sz"))
        .with_context(|| format!("Hyperliquid bbo invalid {side}.sz coin={wire_coin}"))?;
    let count = parse_i64(value.get("n"))
        .filter(|count| *count > 0)
        .with_context(|| format!("Hyperliquid bbo invalid {side}.n coin={wire_coin}"))?;
    let _ = count;
    Ok(Some((price, amount)))
}

fn parse_i64(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
}

fn parse_positive_f64(value: Option<&Value>) -> Option<f64> {
    parse_f64(value).filter(|number| number.is_finite() && *number > 0.0)
}

fn timestamp_ms_to_us(timestamp_ms: i64) -> Result<i64> {
    timestamp_ms
        .checked_mul(1_000)
        .context("Hyperliquid millisecond timestamp overflows microseconds")
}

fn next_hour_us(timestamp_us: i64) -> i64 {
    const HOUR_US: i64 = 3_600_000_000;
    timestamp_us
        .div_euclid(HOUR_US)
        .saturating_add(1)
        .saturating_mul(HOUR_US)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate_subscription_capacity(
        per_leg: usize,
        primary: &str,
        secondary: &str,
    ) -> Result<()> {
        SubscriptionBudgetState::default()
            .reserve(SubscriptionDemand::new(per_leg, primary, secondary)?)
            .map(|_| ())
    }

    #[test]
    fn hyperliquid_subscription_budget_combines_both_and_keeps_peak_during_refresh() {
        let mut budget = SubscriptionBudgetState::default();
        let spot = budget
            .reserve(SubscriptionDemand::new(300, "10.0.0.1", "10.0.0.2").unwrap())
            .unwrap();
        let perp = budget
            .reserve(SubscriptionDemand::new(700, "10.0.0.1", "10.0.0.2").unwrap())
            .unwrap();
        assert!(budget.grow(perp, 701).is_err());
        assert_eq!(budget.demands[&perp].per_leg, 700);
        budget.grow(spot, 100).unwrap();
        assert_eq!(budget.demands[&spot].per_leg, 300);
        assert!(budget.grow(perp, 701).is_err());
        budget.demands.remove(&spot);
        budget.grow(perp, 1_000).unwrap();
    }

    #[test]
    fn hyperliquid_subscription_budget_rejects_combined_startup_without_reserving() {
        let mut budget = SubscriptionBudgetState::default();
        budget
            .reserve(SubscriptionDemand::new(200, "", "").unwrap())
            .unwrap();
        assert!(budget
            .reserve(SubscriptionDemand::new(601, "10.0.0.1", "10.0.0.2").unwrap())
            .is_err());
        assert_eq!(budget.demands.len(), 1);
        assert_eq!(budget.next_id, 1);
    }

    #[test]
    fn hyperliquid_subscription_capacity_counts_both_legs_on_shared_source() {
        for (primary, secondary) in [
            ("", "0.0.0.0"),
            ("::", "0.0.0.0"),
            ("10.0.0.1", "10.0.0.1"),
            ("10.0.0.1", ""),
            ("", "10.0.0.1"),
        ] {
            assert!(validate_subscription_capacity(500, primary, secondary).is_ok());
            assert!(validate_subscription_capacity(501, primary, secondary).is_err());
        }
    }

    #[test]
    fn hyperliquid_subscription_capacity_rejects_oversized_all_market_leg() {
        assert!(validate_subscription_capacity(1_000, "10.0.0.1", "10.0.0.2").is_ok());
        assert!(validate_subscription_capacity(1_001, "10.0.0.1", "10.0.0.2").is_err());
        assert!(validate_subscription_capacity(313 * 4, "10.0.0.1", "10.0.0.2").is_err());
        assert!(validate_subscription_capacity(1, "not-an-ip", "10.0.0.2").is_err());
    }

    fn fixture_perp_metadata() -> (Value, Value) {
        (
            serde_json::json!([null, {"name": "xyz"}]),
            serde_json::json!([
                {
                    "collateralToken": 0,
                    "universe": [
                        {"name": "BTC"},
                        {"name": "ETH"},
                        {"name": "HYPE"},
                        {"name": "PUMP"},
                        {"name": "PURR"},
                        {"name": "SOL"},
                        {"name": "UBTC", "isDelisted": true}
                    ]
                },
                {
                    "collateralToken": 359,
                    "universe": [
                        {"name": "xyz:FOO"}
                    ]
                }
            ]),
        )
    }

    fn default_perp_metadata() -> (Value, Value) {
        (
            serde_json::json!([null]),
            serde_json::json!([{
                "collateralToken": 0,
                "universe": [{"name": "HYPE"}]
            }]),
        )
    }

    fn fixture_catalog() -> Arc<HyperliquidCatalog> {
        let (perp_dexs, all_perp_metas) = fixture_perp_metadata();
        let spot = serde_json::json!({
            "tokens": [
                {"name": "HYPE", "index": 150},
                {"name": "USDC", "index": 0},
                {"name": "PURR", "index": 1},
                {"name": "UBTC", "index": 197},
                {"name": "UETH", "index": 221},
                {"name": "USOL", "index": 254},
                {"name": "PUMP", "index": 298},
                {"name": "UPUMP", "index": 299},
                {"name": "USDH", "index": 359}
            ],
            "universe": [
                {"name": "@107", "index": 107, "tokens": [150, 0]},
                {"name": "PURR/USDC", "index": 0, "tokens": [1, 0]},
                {"name": "@142", "index": 142, "tokens": [197, 0]},
                {"name": "@166", "index": 166, "tokens": [221, 0]},
                {"name": "@199", "index": 199, "tokens": [254, 0]},
                {"name": "@243", "index": 243, "tokens": [298, 0]},
                {"name": "@244", "index": 244, "tokens": [299, 0]},
                {"name": "@207", "index": 207, "tokens": [150, 359]}
            ]
        });
        Arc::new(HyperliquidCatalog::from_values(perp_dexs, all_perp_metas, spot).unwrap())
    }

    fn adapter(venue: TradingVenue) -> HyperliquidAdapter {
        HyperliquidAdapter::with_catalog(venue, fixture_catalog())
    }

    #[test]
    fn catalog_resolves_sparse_indices_and_explicit_spot_aliases() {
        let catalog = fixture_catalog();
        assert_eq!(
            catalog.spot_symbols,
            vec![
                "BTCUSDC".to_string(),
                "ETHUSDC".to_string(),
                "HYPEUSDC".to_string(),
                "PUMPUSDC".to_string(),
                "PURRUSDC".to_string(),
                "SOLUSDC".to_string(),
            ]
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidMargin, "BTCUSDC")
                .unwrap()
                .wire_coin,
            "@142"
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidMargin, "ETHUSDC")
                .unwrap()
                .wire_coin,
            "@166"
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidMargin, "SOLUSDC")
                .unwrap()
                .wire_coin,
            "@199"
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidMargin, "HYPEUSDC")
                .unwrap()
                .wire_coin,
            "@107"
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidMargin, "PURRUSDC")
                .unwrap()
                .wire_coin,
            "PURR/USDC"
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidFutures, "HYPEUSDC")
                .unwrap()
                .wire_coin,
            "HYPE"
        );
        assert_eq!(
            catalog
                .route(TradingVenue::HyperliquidFutures, "XYZFOOUSDH")
                .unwrap()
                .wire_coin,
            "xyz:FOO"
        );
        assert!(catalog.perp_symbols.contains(&"XYZFOOUSDH".to_string()));
        assert!(catalog
            .route(TradingVenue::HyperliquidMargin, "UBTCUSDC")
            .is_none());
        assert!(catalog
            .route(TradingVenue::HyperliquidMargin, "UPUMPUSDC")
            .is_none());
    }

    #[test]
    fn rejects_bad_spot_index_and_missing_token_reference() {
        let (perp_dexs, all_perp_metas) = default_perp_metadata();
        let bad_index = serde_json::json!({
            "tokens": [{"name": "USDC", "index": 0}, {"name": "HYPE", "index": 150}],
            "universe": [{"name": "@106", "index": 107, "tokens": [150, 0]}]
        });
        assert!(HyperliquidCatalog::from_values(
            perp_dexs.clone(),
            all_perp_metas.clone(),
            bad_index
        )
        .unwrap_err()
        .to_string()
        .contains("mismatch"));

        let missing = serde_json::json!({
            "tokens": [{"name": "USDC", "index": 0}],
            "universe": [{"name": "@107", "index": 107, "tokens": [150, 0]}]
        });
        assert!(HyperliquidCatalog::from_values(
            perp_dexs.clone(),
            all_perp_metas.clone(),
            missing
        )
        .unwrap_err()
        .to_string()
        .contains("missing base token"));

        let noncanonical = serde_json::json!({
            "tokens": [{"name": "USDC", "index": 0}, {"name": "HYPE", "index": 150}],
            "universe": [{"name": "HYPE", "index": 107, "tokens": [150, 0]}]
        });
        assert!(
            HyperliquidCatalog::from_values(perp_dexs, all_perp_metas, noncanonical)
                .unwrap_err()
                .to_string()
                .contains("non-indexed spot wire coin mismatch")
        );
    }

    #[test]
    fn rejects_duplicate_spot_market_identity() {
        let (perp_dexs, all_perp_metas) = default_perp_metadata();
        let duplicate_index = serde_json::json!({
            "tokens": [
                {"name": "USDC", "index": 0},
                {"name": "HYPE", "index": 150},
                {"name": "OTHER", "index": 151}
            ],
            "universe": [
                {"name": "@107", "index": 107, "tokens": [150, 0]},
                {"name": "@108", "index": 107, "tokens": [151, 0]}
            ]
        });
        assert!(
            HyperliquidCatalog::from_values(perp_dexs, all_perp_metas, duplicate_index)
                .unwrap_err()
                .to_string()
                .contains("duplicate Hyperliquid spot market index")
        );
    }

    #[test]
    fn rejects_perp_metadata_mismatch_and_internal_symbol_collision() {
        let spot = serde_json::json!({
            "tokens": [
                {"name": "USDC", "index": 0},
                {"name": "USDH", "index": 2}
            ],
            "universe": []
        });
        assert!(HyperliquidCatalog::from_values(
            serde_json::json!([null, {"name": "xyz"}]),
            serde_json::json!([{"collateralToken": 0, "universe": []}]),
            spot.clone(),
        )
        .unwrap_err()
        .to_string()
        .contains("length mismatch"));

        let collision = HyperliquidCatalog::from_values(
            serde_json::json!([null, {"name": "xyz"}, {"name": "xyz_"}]),
            serde_json::json!([
                {"collateralToken": 0, "universe": []},
                {"collateralToken": 2, "universe": [{"name": "xyz:FOO"}]},
                {"collateralToken": 2, "universe": [{"name": "xyz_:FOO"}]}
            ]),
            spot,
        )
        .unwrap_err();
        assert!(collision.to_string().contains("internal symbol collision"));

        let duplicate_dex = HyperliquidCatalog::from_values(
            serde_json::json!([null, {"name": "xyz"}, {"name": "XYZ"}]),
            serde_json::json!([
                {"collateralToken": 0, "universe": []},
                {"collateralToken": 2, "universe": []},
                {"collateralToken": 2, "universe": []}
            ]),
            serde_json::json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "USDH", "index": 2}
                ],
                "universe": []
            }),
        )
        .unwrap_err();
        assert!(duplicate_dex
            .to_string()
            .contains("duplicate Hyperliquid perp DEX name"));

        let invalid_dex = HyperliquidCatalog::from_values(
            serde_json::json!([null, {"name": "bad:dex"}]),
            serde_json::json!([
                {"collateralToken": 0, "universe": []},
                {"collateralToken": 2, "universe": []}
            ]),
            serde_json::json!({
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "USDH", "index": 2}
                ],
                "universe": []
            }),
        )
        .unwrap_err();
        assert!(invalid_dex.to_string().contains("must not contain ':'"));
    }

    #[test]
    fn subscriptions_are_individual_and_spot_uses_wire_index() {
        let spot = adapter(TradingVenue::HyperliquidMargin);
        let symbols = vec!["HYPEUSDC".to_string(), "PURRUSDC".to_string()];
        spot.seed_symbols(&symbols);

        let trades = spot.build_trade_subscribe(&symbols);
        assert_eq!(trades.len(), 2);
        assert!(trades.iter().all(Value::is_object));
        assert_eq!(trades[0]["subscription"]["coin"], "@107");
        assert_eq!(trades[1]["subscription"]["coin"], "PURR/USDC");

        let books = spot.build_incremental_subscribe(&symbols);
        assert_eq!(books.len(), 2);
        assert_eq!(books[0]["subscription"]["type"], "l2Book");
        assert!(books[0]["subscription"].get("fast").is_none());

        let bbo = spot.build_subscribe(&symbols);
        assert_eq!(bbo.len(), 2);
        assert_eq!(bbo[0]["subscription"]["type"], "bbo");
        assert_eq!(bbo[0]["subscription"]["coin"], "@107");
        assert!(spot.build_derivatives_subscribe(&symbols).is_empty());

        let perp = adapter(TradingVenue::HyperliquidFutures);
        perp.seed_symbols(&symbols);
        assert_eq!(
            perp.build_trade_subscribe(&symbols)[0]["subscription"]["coin"],
            "HYPE"
        );
        assert_eq!(
            perp.build_derivatives_subscribe(&symbols)[0]["subscription"]["type"],
            "activeAssetCtx"
        );

        let hip3 = vec!["XYZFOOUSDH".to_string()];
        assert_eq!(
            perp.build_subscribe(&hip3)[0]["subscription"]["coin"],
            "xyz:FOO"
        );
        assert_eq!(
            perp.build_trade_subscribe(&hip3)[0]["subscription"]["coin"],
            "xyz:FOO"
        );
        assert_eq!(
            perp.build_incremental_subscribe(&hip3)[0]["subscription"]["coin"],
            "xyz:FOO"
        );
        assert_eq!(
            perp.build_derivatives_subscribe(&hip3)[0]["subscription"]["coin"],
            "xyz:FOO"
        );
    }

    #[test]
    fn subscription_route_refresh_is_atomic_and_fails_closed() {
        let adapter = adapter(TradingVenue::HyperliquidFutures);
        let known = vec!["HYPEUSDC".to_string()];
        adapter.seed_symbols(&known);
        assert_eq!(adapter.internal_symbol("HYPE").as_deref(), Some("HYPEUSDC"));

        let incomplete = vec!["HYPEUSDC".to_string(), "MISSINGUSDC".to_string()];
        assert!(adapter.build_subscribe(&incomplete).is_empty());
        assert_eq!(adapter.internal_symbol("HYPE").as_deref(), Some("HYPEUSDC"));
        assert!(adapter.internal_symbol("MISSING").is_none());

        let planned = vec!["XYZFOOUSDH".to_string()];
        assert_eq!(adapter.build_subscribe(&planned).len(), 1);
        assert_eq!(adapter.internal_symbol("HYPE").as_deref(), Some("HYPEUSDC"));
        assert!(adapter.internal_symbol("xyz:FOO").is_none());
        adapter.seed_symbols(&planned);
        assert_eq!(
            adapter.internal_symbol("xyz:FOO").as_deref(),
            Some("XYZFOOUSDH")
        );
    }

    #[test]
    fn parses_bbo_and_clears_one_sided_updates() {
        let adapter = adapter(TradingVenue::HyperliquidMargin);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let value = serde_json::json!({
            "channel": "bbo",
            "data": {
                "coin": "@107",
                "time": 1700000000123_i64,
                "bbo": [
                    {"px":"39.0","sz":"10","n":3},
                    {"px":"39.1","sz":"8","n":2}
                ]
            }
        });
        let frames = adapter.collect_frame(&value).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].symbol, "HYPEUSDC");
        assert_eq!(frames[0].ts_us, 1_700_000_000_123_000);
        assert_eq!(frames[0].seq_id, 1_700_000_000_123);
        assert_eq!((frames[0].bid_price, frames[0].bid_amount), (39.0, 10.0));
        assert_eq!((frames[0].ask_price, frames[0].ask_amount), (39.1, 8.0));

        let one_sided = serde_json::json!({
            "channel": "bbo",
            "data": {"coin":"@107","time":1700000000123_i64,"bbo":[null,{"px":"39.1","sz":"8","n":2}]}
        });
        let cleared = adapter.collect_frame(&one_sided).unwrap();
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].ts_us, 1_700_000_000_123_000);
        assert_eq!(cleared[0].seq_id, frames[0].seq_id);
        assert_eq!(cleared[0].bid_price, 0.0);
        assert_eq!(cleared[0].bid_amount, 0.0);
        assert_eq!(cleared[0].ask_price, 0.0);
        assert_eq!(cleared[0].ask_amount, 0.0);

        let malformed = serde_json::json!({
            "channel": "bbo",
            "data": {"coin":"@107","time":1700000000125_i64,"bbo":[{"px":"bad","sz":"1"},null]}
        });
        assert!(adapter.collect_frame(&malformed).is_err());
        assert_eq!(adapter.bbo_dedup_policy(), BboDedupPolicy::RecentIdentity);
    }

    #[test]
    fn parses_non_monotonic_same_millisecond_trades_in_microseconds() {
        let adapter = adapter(TradingVenue::HyperliquidMargin);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let value = serde_json::json!({
            "channel": "trades",
            "data": [
                {"coin":"@107","side":"B","px":"39.1","sz":"2","hash":"0xabc","time":1700000000123_i64,"tid":759070505322107_i64,"users":["0x1","0x2"]},
                {"coin":"@107","side":"A","px":"39.0","sz":"1.5","hash":"0xdef","time":1700000000123_i64,"tid":516854862236860_i64,"users":["0x1","0x2"]}
            ]
        });
        let trades = adapter.parse_trade_frame(&value).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].symbol, "HYPEUSDC");
        assert_eq!(trades[0].timestamp_us, 1_700_000_000_123_000);
        assert_eq!(trades[0].trade_id, 759070505322107);
        assert_eq!(trades[1].trade_id, 516854862236860);
        assert_eq!(trades[1].side, 'S');
        assert_eq!(
            adapter.trade_dedup_policy(),
            TradeDedupPolicy::RecentIdentity
        );
    }

    #[test]
    fn parses_book_as_full_snapshot_without_gap_sequence() {
        let adapter = adapter(TradingVenue::HyperliquidFutures);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let value = serde_json::json!({
            "channel": "l2Book",
            "data": {
                "coin": "HYPE",
                "time": 1700000000456_i64,
                "levels": [
                    [{"px":"39.0","sz":"10","n":3}],
                    [{"px":"39.1","sz":"8","n":2}]
                ]
            }
        });
        let books = adapter.parse_incremental_frame(&value).unwrap();
        assert_eq!(books.len(), 1);
        let IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id,
            gap_check,
            is_snapshot,
            bids,
            asks,
            ..
        } = &books[0]
        else {
            panic!("expected Hyperliquid book");
        };
        assert_eq!(symbol, "HYPEUSDC");
        assert_eq!(*timestamp, 1_700_000_000_456_000);
        assert_eq!(*seq_id, 1_700_000_000_456);
        assert!(!gap_check);
        assert!(is_snapshot);
        assert_eq!(bids[0].price, 39.0);
        assert_eq!(asks[0].amount, 8.0);
        assert_eq!(
            adapter.incremental_dedup_policy(),
            IncrementalDedupPolicy::RecentSnapshotIdentity
        );
    }

    #[test]
    fn emits_empty_book_snapshot_to_clear_previous_liquidity() {
        let adapter = adapter(TradingVenue::HyperliquidFutures);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let value = serde_json::json!({
            "channel": "l2Book",
            "data": {
                "coin": "HYPE",
                "time": 1700000000457_i64,
                "levels": [[], []]
            }
        });

        let books = adapter.parse_incremental_frame(&value).unwrap();
        assert_eq!(books.len(), 1);
        let IncrementalFrame::Book {
            is_snapshot,
            bids,
            asks,
            ..
        } = &books[0]
        else {
            panic!("expected Hyperliquid book");
        };
        assert!(is_snapshot);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn parses_perp_mark_oracle_and_funding_context() {
        let adapter = adapter(TradingVenue::HyperliquidFutures);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let value = serde_json::json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "HYPE",
                "time": 1700000000123_i64,
                "ctx": {
                    "markPx": "39.05",
                    "oraclePx": "39.02",
                    "funding": "-0.0000125",
                    "openInterest": "1234"
                }
            }
        });
        let out = adapter.parse_derivatives_frame(&value).unwrap();
        assert_eq!(out.len(), 4);
        let oi = OpenInterestMsg::from_bytes(&out[3]).unwrap();
        assert_eq!(oi.open_interest, 1234.0);
        assert_eq!(oi.timestamp, 1_700_000_000_123_000);
        assert_eq!(MarkPriceMsg::get_symbol(&out[0]), "HYPEUSDC");
        assert_eq!(MarkPriceMsg::get_mark_price(&out[0]), 39.05);
        assert_eq!(MarkPriceMsg::get_timestamp(&out[0]), 1_700_000_000_123_000);
        assert_eq!(IndexPriceMsg::get_symbol(&out[1]), "HYPEUSDC");
        assert_eq!(IndexPriceMsg::get_index_price(&out[1]), 39.02);
        assert_eq!(FundingRateMsg::get_symbol(&out[2]), "HYPEUSDC");
        assert_eq!(FundingRateMsg::get_funding_rate(&out[2]), -0.0000125);
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&out[2]),
            1_700_002_800_000_000
        );
    }

    #[test]
    fn ignores_control_frames_but_rejects_unknown_wire_coin() {
        let adapter = adapter(TradingVenue::HyperliquidMargin);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        for value in [
            serde_json::json!({"channel":"subscriptionResponse","data":{}}),
            serde_json::json!({"channel":"pong"}),
        ] {
            assert!(adapter.parse_trade_frame(&value).unwrap().is_empty());
            assert!(adapter.parse_incremental_frame(&value).unwrap().is_empty());
        }

        let unknown_trade = serde_json::json!({
            "channel":"trades",
            "data":[{"coin":"HYPE","side":"B","px":"1","sz":"1","hash":"0xabc","time":1,"tid":2,"users":["0x1","0x2"]}]
        });
        assert!(adapter.parse_trade_frame(&unknown_trade).is_err());

        let unknown_book = serde_json::json!({
            "channel": "l2Book",
            "data": {
                "coin": "HYPE",
                "time": 1,
                "levels": [[], []]
            }
        });
        assert!(adapter.parse_incremental_frame(&unknown_book).is_err());
    }

    #[test]
    fn rejects_complete_trade_batch_when_any_row_is_malformed() {
        let adapter = adapter(TradingVenue::HyperliquidMargin);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let mixed = serde_json::json!({
            "channel": "trades",
            "data": [
                {"coin":"@107","side":"B","px":"39.1","sz":"2","hash":"0xabc","time":1700000000123_i64,"tid":1,"users":["0x1","0x2"]},
                {"coin":"@107","side":"A","px":"bad","sz":"1","hash":"0xdef","time":1700000000124_i64,"tid":2,"users":["0x1","0x2"]}
            ]
        });
        assert!(adapter.parse_trade_frame(&mixed).is_err());
        assert!(adapter
            .parse_trade_frame(&serde_json::json!({"channel":"trades","data":{}}))
            .is_err());
        assert!(adapter
            .parse_trade_frame(&serde_json::json!({"channel":"trades"}))
            .is_err());
    }

    #[test]
    fn hip3_market_frames_use_collision_safe_internal_symbol() {
        let adapter = adapter(TradingVenue::HyperliquidFutures);
        adapter.seed_symbols(&["XYZFOOUSDH".to_string()]);

        let bbo = serde_json::json!({
            "channel":"bbo",
            "data":{
                "coin":"xyz:FOO",
                "time":1700000000123_i64,
                "bbo":[{"px":"10","sz":"2","n":1},{"px":"11","sz":"3","n":1}]
            }
        });
        assert_eq!(adapter.collect_frame(&bbo).unwrap()[0].symbol, "XYZFOOUSDH");

        let trades = serde_json::json!({
            "channel":"trades",
            "data":[{
                "coin":"xyz:FOO","side":"B","px":"10","sz":"2","hash":"0xabc",
                "time":1700000000123_i64,"tid":3,"users":["0x1","0x2"]
            }]
        });
        assert_eq!(
            adapter.parse_trade_frame(&trades).unwrap()[0].symbol,
            "XYZFOOUSDH"
        );

        let book = serde_json::json!({
            "channel":"l2Book",
            "data":{
                "coin":"xyz:FOO","time":1700000000123_i64,
                "levels":[[{"px":"10","sz":"2","n":1}],[{"px":"11","sz":"3","n":1}]]
            }
        });
        let IncrementalFrame::Book { symbol, .. } =
            &adapter.parse_incremental_frame(&book).unwrap()[0]
        else {
            panic!("expected HIP-3 book");
        };
        assert_eq!(symbol, "XYZFOOUSDH");

        let ctx = serde_json::json!({
            "channel":"activeAssetCtx",
            "data":{
                "coin":"xyz:FOO",
                "ctx":{"markPx":"10.5","oraclePx":"10.4","funding":"0.0001","openInterest":"25"}
            }
        });
        let derivatives = adapter.parse_derivatives_frame(&ctx).unwrap();
        assert_eq!(MarkPriceMsg::get_symbol(&derivatives[0]), "XYZFOOUSDH");
        assert_eq!(IndexPriceMsg::get_symbol(&derivatives[1]), "XYZFOOUSDH");
        assert_eq!(FundingRateMsg::get_symbol(&derivatives[2]), "XYZFOOUSDH");
    }

    #[test]
    fn rejects_incomplete_or_unknown_perp_context() {
        let adapter = adapter(TradingVenue::HyperliquidFutures);
        adapter.seed_symbols(&["HYPEUSDC".to_string()]);
        let missing_funding = serde_json::json!({
            "channel":"activeAssetCtx",
            "data":{
                "coin":"HYPE",
                "ctx":{"markPx":"39","oraclePx":"38.9","openInterest":"1"}
            }
        });
        assert!(adapter.parse_derivatives_frame(&missing_funding).is_err());

        let unknown = serde_json::json!({
            "channel":"activeAssetCtx",
            "data":{
                "coin":"xyz:FOO",
                "ctx":{"markPx":"10","oraclePx":"10","funding":"0","openInterest":"1"}
            }
        });
        assert!(adapter.parse_derivatives_frame(&unknown).is_err());
    }
}
