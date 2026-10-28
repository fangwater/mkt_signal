//! Offline native ordinary-fill repair. Never publishes IPC or calls an exchange.
//! One synchronous batch owns native rows, fill identities, and rollback bytes.
use crate::parquet::{decode_order_record, decode_trade_record, decode_uniform_order_record};
use anyhow::{bail, ensure, Context, Result};
use bigdecimal::BigDecimal;
use order_common::{OrderStatus, Side, TradeUpdate, TradingVenue};
use persist_common::{native_codec, FillLiquidity, UnifiedOrderRecord};
use rocksdb::{checkpoint::Checkpoint, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

const REPAIRS: &str = "native_fill_repairs";
const NATIVE_CFS: [&str; 5] = [
    "uniform_orders",
    "trade_updates",
    "order_updates",
    "trade_updates_unmatched",
    "order_updates_unmatched",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub source_id: String,
    pub db_path: PathBuf,
    pub db_identity: String,
    pub env_root: PathBuf,
    pub ipc_namespace: String,
    pub venue: u8,
    /// Operator evidence must attest that both live PERSIST_SYNC values are empty.
    pub sync_disabled_evidence_sha256: String,
    pub fee_configuration_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Mutation {
    cf: String,
    key: String,
    value_hex: String,
    value_sha256: String,
    previous_value_hex: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
struct BatchAudit {
    batch_id: String,
    status: String,
    binding: Binding,
    checkpoint: PathBuf,
    received_at_us: i64,
    input_sha256: String,
    mutations: Vec<Mutation>,
}
#[derive(Debug, Serialize, Deserialize)]
struct FillAudit {
    batch_id: String,
    facts: Value,
    rows: Vec<Mutation>,
}

pub struct Inputs {
    binding: Binding,
    manifest: Value,
    checklist: Value,
    input_sha256: String,
    facts: BTreeMap<String, Value>,
}
#[derive(Debug, Serialize)]
pub struct Outcome {
    pub mode: String,
    pub source_id: String,
    pub batch_id: Option<String>,
    pub present_fills: usize,
    pub add_fills: usize,
    pub uniform_rows: usize,
    pub trade_rows: usize,
    pub order_rows: usize,
    pub excluded_fills: usize,
    pub gross_quote_decimal: String,
}
#[derive(Debug)]
struct Plan {
    rows: Vec<Mutation>,
    identities: Vec<(String, Value, Vec<Mutation>)>,
    present: usize,
}

fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .with_context(|| format!("missing string {key}"))
}
fn integer(v: &Value, key: &str) -> Result<i64> {
    v[key]
        .as_i64()
        .with_context(|| format!("missing integer {key}"))
}
fn decimal(v: &Value, key: &str) -> Result<BigDecimal> {
    BigDecimal::from_str(string(v, key)?).with_context(|| format!("invalid decimal {key}"))
}
fn number(v: &Value, key: &str) -> Result<f64> {
    let n: f64 = string(v, key)?.parse()?;
    ensure!(n.is_finite(), "non-finite {key}");
    Ok(n)
}
fn positive(v: &Value, key: &str) -> Result<f64> {
    let n = number(v, key)?;
    ensure!(n > 0.0, "nonpositive {key}");
    Ok(n)
}
// A mobile client ID is not an Exec numeric ID. Match the existing exchange-owned
// event convention (-exchange order ID), retaining the exact original string in
// the audit. Never manufacture a positive Exec/strategy identity.
fn client_identity(order: &Value) -> Result<(i64, bool)> {
    let text = string(order, "clientOrderId")?;
    if let Ok(client) = text.parse::<i64>() {
        ensure!(client > 0, "nonpositive native client ID");
        return Ok((client, false));
    }
    ensure!(
        text.starts_with("ios_")
            && text.len() > 4
            && text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "unsupported external client ID"
    );
    ensure!(
        order["type"] == "MARKET"
            && order["origType"] == "MARKET"
            && order["timeInForce"] == "GTC"
            && order["reduceOnly"] == true
            && order["positionSide"] == "BOTH",
        "only audited mobile reduce-only MARKET fills are supported"
    );
    let id = integer(order, "orderId")?;
    ensure!(id > 0, "invalid external exchange order ID");
    Ok((
        id.checked_neg().context("external order ID overflow")?,
        true,
    ))
}
fn fill_liquidity(market: bool) -> FillLiquidity {
    if market {
        FillLiquidity::Taker
    } else {
        FillLiquidity::Maker
    }
}
fn micros(v: &Value, key: &str) -> Result<i64> {
    let n = integer(v, key)?;
    ensure!(n > 0, "nonpositive {key}");
    n.checked_mul(1000).context("timestamp overflow")
}
fn identity(source: &str, symbol: &str, order: i64, trade: i64) -> String {
    format!(
        "fill:{}",
        json!([source, symbol, 1, order.to_string(), trade.to_string()])
    )
}
fn mutation_bytes(row: &Mutation) -> Result<Vec<u8>> {
    ensure!(
        row.previous_value_hex.is_none(),
        "repair must only insert absent keys"
    );
    let bytes = hex::decode(&row.value_hex)?;
    ensure!(sha(&bytes) == row.value_sha256, "audit value hash mismatch");
    ensure!(
        matches!(
            row.cf.as_str(),
            "uniform_orders" | "trade_updates" | REPAIRS
        ),
        "unexpected repair CF"
    );
    if row.cf != REPAIRS {
        ensure!(
            row.key.len() == 20 && row.key.bytes().all(|b| b.is_ascii_digit()),
            "invalid native audit key"
        );
        let recv: i64 = row.key.parse()?;
        ensure!(
            recv > 0 && bytes.len() >= 8 && bytes[..8] == recv.to_le_bytes(),
            "receive key/value mismatch"
        );
    }
    Ok(bytes)
}
// A reviewed, byte-pinned unmatched WS report is retained in its original CF.
// It proves the missing uniform event without synthesizing another raw report.
fn preserved_raw(facts: &Value) -> Result<BTreeMap<(String, String), Vec<u8>>> {
    let mut result = BTreeMap::new();
    let refs = &facts["manifest_row"]["existing_raw_trade_records"];
    if refs.is_null() {
        return Ok(result);
    }
    let order = &facts["exchange_order"];
    let trade = &facts["exchange_trade"];
    let (client, external) = client_identity(order)?;
    ensure!(
        !external,
        "external reconstruction cannot reuse Exec raw identity"
    );
    for reference in refs.as_array().context("raw references must be an array")? {
        let cf = string(reference, "cf")?;
        let key = string(reference, "key")?;
        ensure!(
            cf == "trade_updates_unmatched"
                && key.len() == 20
                && key.bytes().all(|b| b.is_ascii_digit()),
            "only pinned unmatched trade evidence is supported"
        );
        let bytes = hex::decode(string(reference, "value_hex")?)?;
        ensure!(
            sha(&bytes) == string(reference, "value_sha256")?,
            "raw evidence checksum mismatch"
        );
        let received: i64 = key.parse()?;
        ensure!(
            bytes.len() >= 8 && bytes[..8] == received.to_le_bytes(),
            "raw evidence receive key mismatch"
        );
        let raw = decode_trade_record(&bytes)?;
        ensure!(
            raw.client_order_id == client
                && raw.order_id == integer(order, "orderId")?
                && raw.symbol == string(trade, "symbol")?
                && raw.trading_venue == TradingVenue::BinanceFutures.as_str()
                && raw.side == string(trade, "side")?
                && raw.price == positive(trade, "price")?
                && raw.trade_time == micros(trade, "time")?
                && raw.cumulative_filled_quantity == number(facts, "cumulative_decimal")?
                && raw.is_maker == trade["maker"].as_bool().context("maker flag missing")?,
            "raw evidence does not prove this exact REST fill"
        );
        ensure!(
            result.insert((cf.into(), key.into()), bytes).is_none(),
            "duplicate raw reference"
        );
    }
    Ok(result)
}

fn verify_fill_rows(rows: &[Mutation], facts: &Value) -> Result<()> {
    let preserved = preserved_raw(facts)?;
    let reuse = !preserved.is_empty();
    ensure!(
        rows.len() == if reuse { 1 } else { 2 }
            && rows[0].cf == "uniform_orders"
            && (reuse || rows[1].cf == "trade_updates"),
        "incomplete fill audit mapping"
    );
    let u = decode_uniform_order_record(&mutation_bytes(&rows[0])?)?;
    let t = if reuse {
        decode_trade_record(preserved.values().next().unwrap())?
    } else {
        decode_trade_record(&mutation_bytes(&rows[1])?)?
    };
    let o = &facts["exchange_order"];
    let trade = &facts["exchange_trade"];
    let status = if decimal(facts, "cumulative_decimal")? == decimal(o, "origQty")? {
        OrderStatus::Filled
    } else {
        OrderStatus::PartiallyFilled
    };
    let (client, _external) = client_identity(o)?;
    let market = o["type"] == "MARKET";
    ensure!(
        u.client_order_id == client
            && t.client_order_id == client
            && t.order_id == integer(o, "orderId")?,
        "audit native order identity mismatch"
    );
    ensure!(
        u.symbol == string(trade, "symbol")?
            && t.symbol == u.symbol
            && u.trading_venue == TradingVenue::BinanceFutures.as_str()
            && t.trading_venue == u.trading_venue,
        "audit native market mismatch"
    );
    ensure!(
        u.side == string(trade, "side")?
            && t.side == u.side
            && u.order_type
                == if market {
                    order_common::OrderType::Market.as_str()
                } else {
                    order_common::OrderType::Limit.as_str()
                },
        "audit native side/type mismatch"
    );
    ensure!(
        u.price == positive(trade, "price")?
            && t.price == u.price
            && u.amount_update == positive(trade, "qty")?
            && u.amount_init == positive(o, "origQty")?
            && t.cumulative_filled_quantity == number(facts, "cumulative_decimal")?,
        "audit native price/quantity mismatch"
    );
    ensure!(
        u.create_ts == micros(o, "time")?
            && u.update_ts == micros(trade, "time")?
            && t.trade_time == u.update_ts
            && (reuse || t.event_time == 0),
        "audit native time mismatch"
    );
    ensure!(
        u.signal_ts == 0
            && u.submit_ts == 0
            && u.local_ts == 0
            && u.mkt_ts == 0
            && u.price_offset == 0.0
            && u.signal_bbo.is_none()
            && u.bbo_spread.is_empty(),
        "unknown metadata was fabricated"
    );
    ensure!(
        u.from_key == string(&facts["manifest_row"], "inferred_original_from_key")?
            && u.fill_liquidity == Some(fill_liquidity(market))
            && t.is_maker == !market
            && u.status == status.as_str()
            && (reuse || t.order_status.as_deref() == Some(status.as_str())),
        "audit native attribution/role/status mismatch"
    );
    Ok(())
}
fn valid_hash(v: &str) -> bool {
    v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit())
}

impl Inputs {
    pub fn load(binding: &Path, manifest: &Path, checklist: &Path) -> Result<Self> {
        Self::from_bytes(
            &fs::read(binding)?,
            &fs::read(manifest)?,
            &fs::read(checklist)?,
        )
    }
    fn from_bytes(binding: &[u8], manifest: &[u8], checklist: &[u8]) -> Result<Self> {
        let b: Binding = serde_json::from_slice(binding)?;
        ensure!(
            b.venue == 1 && b.ipc_namespace == b.source_id && !b.source_id.is_empty(),
            "native Binance USD-M source/namespace required"
        );
        ensure!(
            b.env_root.is_absolute()
                && b.db_path.is_absolute()
                && b.db_path == b.env_root.join("data/persist_manager"),
            "absolute deployment DB path mismatch"
        );
        ensure!(
            valid_hash(&b.sync_disabled_evidence_sha256) && valid_hash(&b.fee_configuration_sha256),
            "audit evidence hashes required"
        );
        let source_file_sha256 =
            json!({"binding":sha(binding),"manifest":sha(manifest),"checklist":sha(checklist)});
        let manifest: Value = serde_json::from_slice(manifest)?;
        let checklist: Value = serde_json::from_slice(checklist)?;
        ensure!(
            manifest["source_id"] == b.source_id
                && checklist["source_id"] == b.source_id
                && manifest["venue_code"] == 1,
            "input source/venue mismatch"
        );
        let input_sha256 = sha(&serde_json::to_vec(
            &json!({"binding":b,"manifest":manifest,"checklist":checklist}),
        )?);
        let rows = manifest["rows"]
            .as_array()
            .context("manifest rows missing")?;
        let orders = checklist["orders"]
            .as_array()
            .context("checklist orders missing")?;
        let mut facts = BTreeMap::new();
        let mut ids = BTreeSet::new();
        let mut clients = BTreeSet::new();
        let mut total = BigDecimal::from(0);
        let mut commissions = BTreeMap::<String, BigDecimal>::new();
        let mut attribution_counts = BTreeMap::<String, usize>::new();
        for order in orders {
            let o = &order["exchange_order"];
            let symbol = string(o, "symbol")?;
            let order_id = integer(o, "orderId")?;
            ensure!(
                order_id > 0 && ids.insert((symbol.to_owned(), order_id)),
                "duplicate/invalid order"
            );
            let (client, external) = client_identity(o)?;
            let market = o["type"] == "MARKET";
            ensure!(clients.insert(client), "nonpositive/duplicate client ID");
            ensure!(
                ((market && o["origType"] == "MARKET" && o["timeInForce"] == "GTC")
                    || (!market
                        && o["type"] == "LIMIT"
                        && o["origType"] == "LIMIT"
                        && o["timeInForce"] == "GTX"))
                    && o["positionSide"] == "BOTH",
                "unsupported ordinary order kind"
            );
            ensure!(
                matches!(string(o, "status")?, "FILLED" | "CANCELED"),
                "unsupported final status"
            );
            ensure!(
                symbol.chars().all(|c| {
                    c.is_ascii_uppercase()
                        || c.is_ascii_digit()
                        || (external && !c.is_ascii() && c.is_alphanumeric())
                }) && !symbol.is_empty(),
                "invalid symbol"
            );
            ensure!(o["side"] == "BUY" || o["side"] == "SELL", "invalid side");
            ensure!(
                order["symbol"] == o["symbol"]
                    && integer(order, "exchange_order_id")? == order_id
                    && integer(order, "client_order_id")? == client,
                "checklist order mismatch"
            );
            let orig = decimal(o, "origQty")?;
            positive(o, "origQty")?;
            if market {
                ensure!(
                    decimal(o, "price")? == BigDecimal::from(0),
                    "MARKET order price must be zero"
                );
            } else {
                positive(o, "price")?;
            }
            let create = micros(o, "time")?;
            let final_time = micros(o, "updateTime")?;
            ensure!(final_time >= create, "invalid order times");
            let trades = order["exchange_trades"]
                .as_array()
                .context("trades missing")?;
            let mut ordered = trades.iter().collect::<Vec<_>>();
            ordered.sort_by_key(|t| (t["time"].as_i64(), t["id"].as_i64()));
            let mut cum = BigDecimal::from(0);
            let mut quote = BigDecimal::from(0);
            for t in ordered {
                let trade_id = integer(t, "id")?;
                ensure!(trade_id > 0, "invalid trade ID");
                ensure!(
                    integer(t, "orderId")? == order_id
                        && t["symbol"] == o["symbol"]
                        && t["side"] == o["side"]
                        && t["positionSide"] == "BOTH"
                        && t["maker"] == !market,
                    "trade/order kind or liquidity mismatch"
                );
                ensure!(
                    t["buyer"].as_bool() == Some(o["side"] == "BUY"),
                    "buyer side mismatch"
                );
                let time = micros(t, "time")?;
                ensure!(
                    time >= create && time <= final_time,
                    "trade time outside order"
                );
                positive(t, "price")?;
                positive(t, "qty")?;
                number(t, "commission")?;
                let qty = decimal(t, "qty")?;
                let px = decimal(t, "price")?;
                let q = decimal(t, "quoteQty")?;
                ensure!(&qty * &px == q, "quote not equal price times quantity");
                cum += qty;
                quote += &q;
                total += q;
                let id = identity(&b.source_id, symbol, order_id, trade_id);
                let matches = rows
                    .iter()
                    .filter(|r| {
                        r["identity"]
                            == json!([
                                b.source_id,
                                symbol,
                                1,
                                order_id.to_string(),
                                trade_id.to_string()
                            ])
                    })
                    .collect::<Vec<_>>();
                ensure!(
                    matches.len() == 1,
                    "missing/duplicate manifest identity {id}"
                );
                let r = matches[0];
                for (a, z) in [
                    ("price_decimal", "price"),
                    ("quantity_decimal", "qty"),
                    ("quote_decimal", "quoteQty"),
                    ("commission_decimal", "commission"),
                    ("commission_asset", "commissionAsset"),
                ] {
                    ensure!(r[a] == t[z], "manifest {a} mismatch");
                }
                ensure!(
                    r["client_order_id"] == o["clientOrderId"]
                        && r["side"] == o["side"]
                        && r["maker"] == !market
                        && r["exchange_trade_time_ms"] == t["time"]
                        && r["order_create_time_ms"] == o["time"]
                        && r["order_update_time_ms"] == o["updateTime"]
                        && r["order_final_status"] == o["status"],
                    "manifest order/time mismatch"
                );
                let from = string(r, "inferred_original_from_key")?;
                ensure!(
                    ((!external
                        && ["chase_exec:", "batch_exec:"]
                            .iter()
                            .any(|prefix| from.starts_with(prefix) && from.len() > prefix.len()))
                        || (external
                            && from == "external_account_fill:ios"
                            && r["attribution_kind"] == "external_account"
                            && r["native_client_order_id"] == client))
                        && !string(r, "attribution_basis")?.is_empty(),
                    "missing reviewed Exec strategy attribution"
                );
                *attribution_counts.entry(from.to_owned()).or_default() += 1;
                *commissions
                    .entry(string(t, "commissionAsset")?.to_owned())
                    .or_default() += decimal(t, "commission")?;
                let mut facts_value = json!({"manifest_row":r,"exchange_order":o,"exchange_trade":t,"cumulative_decimal":cum.to_string(),"unknown_fields":{"event_time_us":0,"signal_ts_us":0,"submit_ts_us":0,"local_ts_us":0,"mkt_ts_us":0,"bbo":null,"signal_bbo":null},"provenance":"REST order/trade facts; fill status derived from exact cumulative quantity; no synthetic WS order event","input_sha256":input_sha256,"source_file_sha256":source_file_sha256});
                if external {
                    facts_value["native_client_identity"] = json!({"value":client,
                        "scheme":"negative_exchange_order_id", "original_exchange_client_id":o["clientOrderId"],
                        "strategy_identity":null});
                    facts_value["provenance"] = json!(
                        "REST mobile reduce-only MARKET fill; no Exec strategy identity; native negative order-ID placeholder; original client string preserved in audit; no synthetic WS event"
                    );
                }
                preserved_raw(&facts_value)?;
                ensure!(
                    facts.insert(id, facts_value).is_none(),
                    "duplicate trade identity"
                );
            }
            ensure!(
                cum == decimal(o, "executedQty")? && cum > BigDecimal::from(0) && cum <= orig,
                "incomplete order fills"
            );
            ensure!(quote == decimal(o, "cumQuote")?, "order quote mismatch");
            ensure!(
                o["status"] != "FILLED" || cum == orig,
                "FILLED quantity mismatch"
            );
        }
        ensure!(
            facts.len() == rows.len()
                && manifest["fill_count"].as_u64() == Some(facts.len() as u64)
                && manifest["order_count"].as_u64() == Some(ids.len() as u64),
            "count mismatch"
        );
        ensure!(
            !facts.is_empty() && total == decimal(&manifest, "gross_quote_decimal")?,
            "gross quote mismatch"
        );
        ensure!(
            serde_json::to_value(attribution_counts)? == manifest["from_key_counts"],
            "attribution counts mismatch"
        );
        let stated = manifest["commission_by_asset"]
            .as_object()
            .context("commission totals missing")?;
        ensure!(
            stated.len() == commissions.len(),
            "commission currencies mismatch"
        );
        for (asset, qty) in commissions {
            ensure!(
                BigDecimal::from_str(
                    stated
                        .get(&asset)
                        .and_then(Value::as_str)
                        .context("commission asset missing")?
                )? == qty,
                "commission total mismatch"
            );
        }
        let excluded = &manifest["excluded_hbar"];
        // This exclusion belongs to the reviewed trade03 input, not every
        // independent account. Never require another source's fill identity.
        ensure!(
            b.source_id != "binance_exec_trade03" || !excluded.is_null(),
            "HBAR exclusion evidence missing"
        );
        if !excluded.is_null() {
            ensure!(
                b.source_id == "binance_exec_trade03",
                "foreign HBAR exclusion evidence"
            );
            ensure!(
                excluded["symbol"] == "HBARUSDT"
                    && excluded["exchange_order_id"] == 17467038656_i64
                    && excluded["exchange_trades"][0]["id"] == 812602388_i64,
                "HBAR exclusion evidence missing"
            );
            ensure!(
                !ids.contains(&("HBARUSDT".into(), 17467038656)),
                "HBAR already-present order cannot be imported"
            );
        }
        Ok(Self {
            binding: b,
            manifest,
            checklist,
            input_sha256,
            facts,
        })
    }
    fn open(&self, write: bool) -> Result<DB> {
        let b = &self.binding;
        ensure!(
            fs::canonicalize(&b.db_path)? == b.db_path
                && fs::canonicalize(&b.env_root)? == b.env_root,
            "symlink/noncanonical deployment path"
        );
        ensure!(
            fs::read_to_string(b.db_path.join("IDENTITY"))?.trim() == b.db_identity,
            "RocksDB identity mismatch"
        );
        if write {
            require_offline(&b.env_root)?;
        }
        let mut opts = Options::default();
        opts.set_max_open_files(128);
        let cfs = DB::list_cf(&opts, &b.db_path)?;
        ensure!(
            NATIVE_CFS.iter().all(|n| cfs.iter().any(|c| c == n)),
            "missing native CF"
        );
        ensure!(
            !cfs.iter().any(|c| c.contains("__uniform_orders")),
            "center DB forbidden"
        );
        let db = if write {
            DB::open_cf(&opts, &b.db_path, cfs)?
        } else {
            DB::open_cf_for_read_only(&opts, &b.db_path, cfs, false)?
        };
        // This implementation deliberately refuses any source with replication history.
        // It cannot safely retract an already propagated repair.
        for name in ["sync_meta", "sync_outbox"] {
            if let Some(cf) = db.cf_handle(name) {
                ensure!(
                    db.iterator_cf(cf, IteratorMode::Start)
                        .next()
                        .transpose()?
                        .is_none(),
                    "replication state present: {name}"
                );
            }
        }
        Ok(db)
    }
    fn plan(&self, db: &DB, received: i64) -> Result<Plan> {
        let mut reviewed_raw = BTreeMap::new();
        for facts in self.facts.values() {
            for ((cf, key), bytes) in preserved_raw(facts)? {
                ensure!(
                    db.get_cf(db.cf_handle(&cf).context("raw evidence CF missing")?, &key)?
                        .as_deref()
                        == Some(bytes.as_slice()),
                    "pinned raw evidence changed or disappeared"
                );
                ensure!(
                    reviewed_raw.insert((cf, key), bytes).is_none(),
                    "raw record reused by multiple fills"
                );
            }
        }
        let mut present = 0;
        let mut known = BTreeSet::new();
        let mut missing = Vec::new();
        for (id, facts) in &self.facts {
            let old = if let Some(cf) = db.cf_handle(REPAIRS) {
                db.get_cf(cf, id)?
            } else {
                None
            };
            if let Some(old) = old {
                let audit: FillAudit = serde_json::from_slice(&old)?;
                ensure!(audit.facts == *facts, "identity conflict {id}");
                verify_fill_rows(&audit.rows, facts)?;
                let batch_key = format!("batch:{}", audit.batch_id);
                let batch: BatchAudit = serde_json::from_slice(
                    &db.get_cf(db.cf_handle(REPAIRS).unwrap(), batch_key)?
                        .context("fill batch audit missing")?,
                )?;
                ensure!(
                    batch.status == "committed" && batch.input_sha256 == self.input_sha256,
                    "fill batch not committed for these inputs"
                );
                ensure!(
                    batch.mutations.iter().any(|r| r.cf == REPAIRS
                        && r.key == *id
                        && mutation_bytes(r).ok().as_deref() == Some(old.as_slice())),
                    "fill identity does not match batch audit"
                );
                for r in &audit.rows {
                    ensure!(
                        db.get_cf(db.cf_handle(&r.cf).context("audit CF missing")?, &r.key)?
                            .as_deref()
                            == Some(mutation_bytes(r)?.as_slice()),
                        "repaired bytes changed: {} {}",
                        r.cf,
                        r.key
                    );
                    ensure!(
                        batch.mutations.contains(r),
                        "fill native row absent from batch audit"
                    );
                    ensure!(
                        known.insert((r.cf.clone(), r.key.clone())),
                        "multiple identities own one native row"
                    );
                }
                present += 1;
            } else {
                missing.push((id, facts));
            }
        }
        let targets = self.checklist["orders"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                (
                    o["client_order_id"].as_i64().unwrap(),
                    (
                        o["symbol"].as_str().unwrap(),
                        o["exchange_order_id"].as_i64().unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut attribution = BTreeMap::<(String, i64), BTreeSet<String>>::new();
        let mut hbar_uniform = false;
        let mut hbar_raw = false;
        for name in NATIVE_CFS {
            let cf = db.cf_handle(name).context("native CF missing")?;
            for item in db.iterator_cf(cf, IteratorMode::Start) {
                let (key, value) = item?;
                let key = std::str::from_utf8(&key)?;
                if known.contains(&(name.to_owned(), key.to_owned())) {
                    continue;
                }
                if name == "uniform_orders" {
                    let r = decode_uniform_order_record(&value)
                        .with_context(|| format!("{name}/{key}"))?;
                    if r.trading_venue == TradingVenue::BinanceFutures.as_str() {
                        attribution
                            .entry((r.symbol.clone(), r.client_order_id >> 32))
                            .or_default()
                            .insert(r.from_key.clone());
                    }
                    if targets.contains_key(&r.client_order_id) {
                        ensure!(
                            r.amount_update == 0.0,
                            "native positive fill exists without repair identity: {name}/{key}"
                        );
                    }
                    if r.client_order_id == 7573984675408905011
                        && r.symbol == "HBARUSDT"
                        && r.trading_venue == TradingVenue::BinanceFutures.as_str()
                        && r.side == "SELL"
                        && r.amount_update == 155.0
                        && r.price == 0.08464
                    {
                        hbar_uniform = true;
                    }
                } else {
                    let (client, order, symbol, venue, qty) = if name.starts_with("trade_") {
                        let r =
                            decode_trade_record(&value).with_context(|| format!("{name}/{key}"))?;
                        (
                            r.client_order_id,
                            r.order_id,
                            r.symbol,
                            r.trading_venue,
                            r.cumulative_filled_quantity,
                        )
                    } else {
                        let r =
                            decode_order_record(&value).with_context(|| format!("{name}/{key}"))?;
                        (
                            r.client_order_id,
                            r.order_id,
                            r.symbol,
                            r.trading_venue,
                            r.cumulative_filled_quantity,
                        )
                    };
                    let by_order = targets.values().any(|(s, o)| *s == symbol && *o == order);
                    if targets.contains_key(&client) || by_order {
                        ensure!(
                            qty == 0.0
                                || reviewed_raw.contains_key(&(name.to_owned(), key.to_owned())),
                            "native cumulative fill exists without repair identity: {name}/{key}"
                        );
                        ensure!(
                            targets.get(&client) == Some(&(symbol.as_str(), order))
                                && venue == TradingVenue::BinanceFutures.as_str(),
                            "existing raw identity conflict"
                        );
                    }
                    if name.starts_with("trade_")
                        && client == 7573984675408905011
                        && order == 17467038656
                        && symbol == "HBARUSDT"
                        && venue == TradingVenue::BinanceFutures.as_str()
                        && qty == 155.0
                    {
                        hbar_raw = true;
                    }
                }
            }
        }
        if !self.manifest["excluded_hbar"].is_null() {
            ensure!(
                hbar_uniform && hbar_raw,
                "HBAR exclusion not verified in native CFs"
            );
        }
        let mut rows = Vec::new();
        let mut identities = Vec::new();
        let mut next = received;
        // Explicit exchange order/time/trade-ID order, including same-millisecond fills.
        missing.sort_by_key(|(_, f)| {
            (
                f["exchange_trade"]["time"].as_i64(),
                f["exchange_trade"]["id"].as_i64(),
            )
        });
        for (id, facts) in missing {
            let o = &facts["exchange_order"];
            let t = &facts["exchange_trade"];
            let symbol = string(t, "symbol")?;
            let (client, external) = client_identity(o)?;
            let market = o["type"] == "MARKET";
            let from = string(&facts["manifest_row"], "inferred_original_from_key")?;
            if !external {
                let observed = attribution
                    .get(&(symbol.to_owned(), client >> 32))
                    .context("no persisted attribution evidence")?;
                ensure!(
                    observed.len() == 1 && observed.contains(from),
                    "attribution not unique or changed for {symbol}/{client}"
                );
            }
            let cumulative = number(facts, "cumulative_decimal")?;
            let status = if decimal(facts, "cumulative_decimal")? == decimal(o, "origQty")? {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            let side = if t["side"] == "BUY" {
                Side::Buy
            } else {
                Side::Sell
            };
            let record = UnifiedOrderRecord {
                symbol_len: symbol.len().try_into()?,
                symbol: symbol.as_bytes().to_vec(),
                create_ts: micros(o, "time")?,
                update_ts: micros(t, "time")?,
                signal_ts: 0,
                submit_ts: 0,
                local_ts: 0,
                mkt_ts: 0,
                client_order_id: client,
                venue: 1,
                ttype: if market {
                    order_common::OrderType::Market as u8
                } else {
                    order_common::OrderType::Limit as u8
                },
                side: side as u8,
                price: positive(t, "price")?,
                price_offset: 0.0,
                amount_init: positive(o, "origQty")?,
                amount_update: positive(t, "qty")?,
                status: status.to_u8(),
                from_key_len: from.len().try_into()?,
                from_key: from.as_bytes().to_vec(),
                signal_bbo: None,
                fill_liquidity: fill_liquidity(market),
            };
            let raw = RestTrade {
                symbol: symbol.into(),
                order_id: integer(o, "orderId")?,
                client,
                maker: !market,
                side,
                price: record.price,
                time: record.update_ts,
                cumulative,
                status,
            };
            let mut owned = Vec::new();
            let reuse = !preserved_raw(facts)?.is_empty();
            for name in ["uniform_orders", "trade_updates"] {
                if reuse && name == "trade_updates" {
                    continue;
                }
                let key = format!("{next:020}");
                let payload = if name == "uniform_orders" {
                    native_codec::append_bbo_spread(
                        native_codec::serialize_uniform_order(
                            &record,
                            next,
                            symbol,
                            record.amount_init,
                            record.amount_update,
                        ),
                        "",
                    )
                } else {
                    native_codec::serialize_trade_update(&raw, next, symbol, cumulative)
                };
                ensure!(
                    db.get_cf(db.cf_handle(name).unwrap(), &key)?.is_none(),
                    "physical key collision {name}/{key}"
                );
                if name == "uniform_orders" {
                    let r = decode_uniform_order_record(&payload)?;
                    ensure!(
                        r.amount_update == record.amount_update
                            && r.fill_liquidity == Some(fill_liquidity(market))
                            && r.from_key == from
                            && r.client_order_id == client
                            && r.status == status.as_str(),
                        "uniform codec verification failed"
                    );
                } else {
                    let r = decode_trade_record(&payload)?;
                    ensure!(
                        r.order_id == raw.order_id
                            && r.is_maker == !market
                            && r.cumulative_filled_quantity == cumulative,
                        "trade codec verification failed"
                    );
                }
                owned.push(Mutation {
                    cf: name.into(),
                    key,
                    value_sha256: sha(&payload),
                    previous_value_hex: None,
                    value_hex: hex::encode(payload),
                });
                next = next.checked_add(1).context("receive time overflow")?;
            }
            rows.extend(owned.clone());
            identities.push((id.clone(), facts.clone(), owned));
        }
        Ok(Plan {
            rows,
            identities,
            present,
        })
    }
    pub fn run(&self, apply: bool, backup: Option<&Path>) -> Result<Outcome> {
        let now = chrono::Utc::now().timestamp_micros();
        let mut db = self.open(apply)?;
        let plan = self.plan(&db, now)?;
        let add = plan.identities.len();
        let mut outcome = Outcome {
            mode: if apply { "apply" } else { "dry-run" }.into(),
            source_id: self.binding.source_id.clone(),
            batch_id: None,
            present_fills: plan.present,
            add_fills: add,
            uniform_rows: add,
            trade_rows: plan.rows.iter().filter(|r| r.cf == "trade_updates").count(),
            order_rows: 0,
            excluded_fills: usize::from(!self.manifest["excluded_hbar"].is_null()),
            gross_quote_decimal: string(&self.manifest, "gross_quote_decimal")?.into(),
        };
        if !apply || add == 0 {
            return Ok(outcome);
        }
        let backup = backup.context("--backup required for apply")?;
        ensure!(
            backup.is_absolute() && !backup.exists() && !backup.starts_with(&self.binding.db_path),
            "new absolute checkpoint path outside DB required"
        );
        Checkpoint::new(&db)?.create_checkpoint(backup)?;
        // RocksDB checkpoints need not include the external IDENTITY file.
        // Preserve it explicitly while the primary lock is held, and sync it.
        if !backup.join("IDENTITY").exists() {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(backup.join("IDENTITY"))?;
            file.write_all(fs::read(self.binding.db_path.join("IDENTITY"))?.as_slice())?;
            file.sync_all()?;
            fs::File::open(backup)?.sync_all()?;
        }
        ensure!(
            fs::read_to_string(backup.join("IDENTITY"))?.trim() == self.binding.db_identity,
            "checkpoint identity mismatch"
        );
        // Reopen the full checkpoint and prove the same plan before writing any native row.
        {
            let opts = Options::default();
            let cfs = DB::list_cf(&opts, backup)?;
            let copy = DB::open_cf_for_read_only(&opts, backup, cfs, false)?;
            ensure!(
                self.plan(&copy, now)?.rows == plan.rows,
                "checkpoint plan differs"
            );
        }
        let batch_id = sha(format!("{}:{now}", self.input_sha256).as_bytes());
        if db.cf_handle(REPAIRS).is_none() {
            db.create_cf(REPAIRS, &Options::default())?;
        }
        let mut mutations = plan.rows;
        for (id, facts, rows) in plan.identities {
            let audit = FillAudit {
                batch_id: batch_id.clone(),
                facts,
                rows,
            };
            mutations.push(Mutation {
                cf: REPAIRS.into(),
                key: id,
                value_sha256: sha(&serde_json::to_vec(&audit)?),
                previous_value_hex: None,
                value_hex: hex::encode(serde_json::to_vec(&audit)?),
            });
        }
        let batch_key = format!("batch:{batch_id}");
        ensure!(
            db.get_cf(db.cf_handle(REPAIRS).unwrap(), &batch_key)?
                .is_none(),
            "batch ID collision"
        );
        let audit = BatchAudit {
            batch_id: batch_id.clone(),
            status: "committed".into(),
            binding: self.binding.clone(),
            checkpoint: backup.into(),
            received_at_us: now,
            input_sha256: self.input_sha256.clone(),
            mutations,
        };
        let mut batch = WriteBatch::default();
        for r in &audit.mutations {
            let cf = db.cf_handle(&r.cf).context("write CF missing")?;
            ensure!(db.get_cf(cf, &r.key)?.is_none(), "pre-commit key collision");
            batch.put_cf(cf, &r.key, mutation_bytes(r)?);
        }
        batch.put_cf(
            db.cf_handle(REPAIRS).unwrap(),
            batch_key,
            serde_json::to_vec(&audit)?,
        );
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        db.write_opt(batch, &opts)?;
        let verified = self.plan(&db, now)?;
        ensure!(
            verified.present == self.facts.len() && verified.identities.is_empty(),
            "post-commit verification failed; use durable batch audit to investigate"
        );
        outcome.present_fills = verified.present;
        outcome.batch_id = Some(batch_id);
        Ok(outcome)
    }
    pub fn rollback(&self, batch_id: &str, apply: bool) -> Result<Value> {
        let db = self.open(apply)?;
        let cf = db.cf_handle(REPAIRS).context("repair audit absent")?;
        let key = format!("batch:{batch_id}");
        let bytes = db.get_cf(cf, &key)?.context("batch not found")?;
        let mut audit: BatchAudit = serde_json::from_slice(&bytes)?;
        ensure!(
            audit.binding.source_id == self.binding.source_id
                && audit.binding.db_identity == self.binding.db_identity
                && audit.input_sha256 == self.input_sha256,
            "rollback binding/input mismatch"
        );
        if audit.status == "rolled_back" {
            return Ok(json!({"status":"already_rolled_back","removed_rows":0}));
        }
        ensure!(audit.status == "committed", "unexpected batch status");
        for r in &audit.mutations {
            ensure!(
                db.get_cf(db.cf_handle(&r.cf).context("rollback CF missing")?, &r.key)?
                    .as_deref()
                    == Some(mutation_bytes(r)?.as_slice()),
                "rollback refused: changed or missing {}/{}",
                r.cf,
                r.key
            );
        }
        if apply {
            let mut batch = WriteBatch::default();
            for r in &audit.mutations {
                batch.delete_cf(db.cf_handle(&r.cf).unwrap(), &r.key);
            }
            audit.status = "rolled_back".into();
            batch.put_cf(cf, key, serde_json::to_vec(&audit)?);
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            db.write_opt(batch, &opts)?;
        }
        Ok(
            json!({"status":if apply {"rolled_back"} else {"rollback_dry_run"},"batch_id":batch_id,"rows":audit.mutations.len()}),
        )
    }
}

/// No live producer may run while the maintenance writer is open. The operator
/// must establish lossless draining before stopping them; this check is not a
/// claim that a process stop alone provides that guarantee.
fn require_offline(root: &Path) -> Result<()> {
    for item in fs::read_dir("/proc")? {
        let p = item?.path();
        if !p
            .file_name()
            .unwrap()
            .to_string_lossy()
            .bytes()
            .all(|b| b.is_ascii_digit())
        {
            continue;
        }
        let cwd = match fs::read_link(p.join("cwd")) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => continue,
            Err(e) => return Err(e.into()),
        };
        let exe = fs::read_link(p.join("exe")).unwrap_or_default();
        if cwd == root || exe.starts_with(root) {
            let name = exe.file_name().unwrap_or_default().to_string_lossy();
            if matches!(
                name.as_ref(),
                "persist_manager" | "exec-pre-trade" | "trade_engine" | "pre_trade"
            ) {
                bail!(
                    "offline write refused: {} still running in {}",
                    name,
                    root.display()
                );
            }
        }
    }
    for name in ["PERSIST_SYNC_SOURCE_ID", "PERSIST_SYNC_BIND"] {
        ensure!(
            std::env::var(name).unwrap_or_default().trim().is_empty(),
            "replication enabled in importer environment"
        );
    }
    Ok(())
}
struct RestTrade {
    symbol: String,
    order_id: i64,
    client: i64,
    maker: bool,
    side: Side,
    price: f64,
    time: i64,
    cumulative: f64,
    status: OrderStatus,
}
impl TradeUpdate for RestTrade {
    fn event_time(&self) -> i64 {
        0
    }
    fn trade_time(&self) -> i64 {
        self.time
    }
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn order_id(&self) -> i64 {
        self.order_id
    }
    fn client_order_id(&self) -> i64 {
        self.client
    }
    fn side(&self) -> Side {
        self.side
    }
    fn price(&self) -> f64 {
        self.price
    }
    fn is_maker(&self) -> bool {
        self.maker
    }
    fn factual_is_maker(&self) -> Option<bool> {
        Some(self.maker)
    }
    fn trading_venue(&self) -> TradingVenue {
        TradingVenue::BinanceFutures
    }
    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative
    }
    fn order_status(&self) -> Option<OrderStatus> {
        Some(self.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    const MANIFEST: &str = include_str!("../tests/fixtures/native_fill_import/manifest.json");
    const CHECKLIST: &str = include_str!("../tests/fixtures/native_fill_import/checklist.json");
    struct Fixture {
        root: PathBuf,
        input: Inputs,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
    fn uniform(symbol: &str, client: i64, from: &str, qty: f64, price: f64) -> UnifiedOrderRecord {
        UnifiedOrderRecord {
            symbol_len: symbol.len() as u16,
            symbol: symbol.as_bytes().to_vec(),
            create_ts: 1,
            update_ts: 2,
            signal_ts: 0,
            submit_ts: 0,
            local_ts: 0,
            mkt_ts: 0,
            client_order_id: client,
            venue: 1,
            ttype: 1,
            side: Side::Sell as u8,
            price,
            price_offset: 0.0,
            amount_init: qty,
            amount_update: qty,
            status: OrderStatus::Filled.to_u8(),
            from_key_len: from.len() as u32,
            from_key: from.as_bytes().to_vec(),
            signal_bbo: None,
            fill_liquidity: FillLiquidity::Maker,
        }
    }
    fn fixture() -> Fixture {
        let parent =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/native_fill_import_tests");
        fs::create_dir_all(&parent).unwrap();
        let root = fs::canonicalize(parent).unwrap().join(format!(
            "{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let db_path = root.join("env/data/persist_manager");
        fs::create_dir_all(&db_path).unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf(&opts, &db_path, NATIVE_CFS).unwrap();
        let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
        for (i, r) in manifest["rows"].as_array().unwrap().iter().enumerate() {
            let symbol = r["identity"][1].as_str().unwrap();
            let client = r["client_order_id"]
                .as_str()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            let r = uniform(
                symbol,
                client + 10000,
                r["inferred_original_from_key"].as_str().unwrap(),
                0.0,
                1.0,
            );
            let raw = native_codec::append_bbo_spread(
                native_codec::serialize_uniform_order(&r, i as i64 + 100, symbol, 0.0, 0.0),
                "",
            );
            db.put_cf(
                db.cf_handle("uniform_orders").unwrap(),
                format!("{:020}", i + 100),
                raw,
            )
            .unwrap();
        }
        let r = uniform(
            "HBARUSDT",
            7573984675408905011,
            "chase_exec:rbf_mid_1min",
            155.0,
            0.08464,
        );
        let raw = native_codec::append_bbo_spread(
            native_codec::serialize_uniform_order(&r, 10, "HBARUSDT", 155.0, 155.0),
            "",
        );
        db.put_cf(
            db.cf_handle("uniform_orders").unwrap(),
            "00000000000000000010",
            raw,
        )
        .unwrap();
        let t = RestTrade {
            symbol: "HBARUSDT".into(),
            order_id: 17467038656,
            client: r.client_order_id,
            maker: true,
            side: Side::Sell,
            price: r.price,
            time: 2,
            cumulative: 155.0,
            status: OrderStatus::Filled,
        };
        db.put_cf(
            db.cf_handle("trade_updates").unwrap(),
            "00000000000000000011",
            native_codec::serialize_trade_update(&t, 11, "HBARUSDT", 155.0),
        )
        .unwrap();
        drop(db);
        let binding = Binding {
            source_id: "binance_exec_trade03".into(),
            db_identity: fs::read_to_string(db_path.join("IDENTITY"))
                .unwrap()
                .trim()
                .into(),
            db_path,
            env_root: root.join("env"),
            ipc_namespace: "binance_exec_trade03".into(),
            venue: 1,
            sync_disabled_evidence_sha256: "a".repeat(64),
            fee_configuration_sha256: "b".repeat(64),
        };
        let input = Inputs::from_bytes(
            &serde_json::to_vec(&binding).unwrap(),
            MANIFEST.as_bytes(),
            CHECKLIST.as_bytes(),
        )
        .unwrap();
        Fixture { root, input }
    }
    fn native_rows(db: &DB) -> BTreeMap<(String, Vec<u8>), Vec<u8>> {
        let mut rows = BTreeMap::new();
        for name in NATIVE_CFS {
            for r in db.iterator_cf(db.cf_handle(name).unwrap(), IteratorMode::Start) {
                let (k, v) = r.unwrap();
                rows.insert((name.into(), k.to_vec()), v.to_vec());
            }
        }
        rows
    }
    #[test]
    fn another_source_requires_its_own_identity_without_trade03_hbar() {
        let f = fixture();
        let mut binding = f.input.binding.clone();
        binding.source_id = "binance_exec_trade04".into();
        binding.ipc_namespace = binding.source_id.clone();
        let mut manifest = f.input.manifest.clone();
        let mut checklist = f.input.checklist.clone();
        manifest["source_id"] = json!(binding.source_id);
        checklist["source_id"] = json!(binding.source_id);
        for row in manifest["rows"].as_array_mut().unwrap() {
            row["identity"][0] = json!(binding.source_id);
        }
        // Supplying another account's exclusion is an identity error.
        assert!(Inputs::from_bytes(
            &serde_json::to_vec(&binding).unwrap(),
            &serde_json::to_vec(&manifest).unwrap(),
            &serde_json::to_vec(&checklist).unwrap()
        )
        .is_err());
        manifest.as_object_mut().unwrap().remove("excluded_hbar");
        let input = Inputs::from_bytes(
            &serde_json::to_vec(&binding).unwrap(),
            &serde_json::to_vec(&manifest).unwrap(),
            &serde_json::to_vec(&checklist).unwrap(),
        )
        .unwrap();
        let db = input.open(false).unwrap();
        drop(db);
        let db = DB::open_cf(&Options::default(), &binding.db_path, NATIVE_CFS).unwrap();
        db.delete_cf(
            db.cf_handle("uniform_orders").unwrap(),
            "00000000000000000010",
        )
        .unwrap();
        db.delete_cf(
            db.cf_handle("trade_updates").unwrap(),
            "00000000000000000011",
        )
        .unwrap();
        drop(db);
        let result = input.run(false, None).unwrap();
        assert_eq!(result.source_id, "binance_exec_trade04");
        assert_eq!(result.add_fills, 24);
        assert_eq!(result.excluded_fills, 0);
    }

    #[test]
    fn batch_attribution_requires_same_source_native_evidence() {
        let f = fixture();
        let mut binding = f.input.binding.clone();
        binding.source_id = "binance_exec_trade01".into();
        binding.ipc_namespace = binding.source_id.clone();
        let mut manifest = f.input.manifest.clone();
        let mut checklist = f.input.checklist.clone();
        manifest["source_id"] = json!(binding.source_id);
        checklist["source_id"] = json!(binding.source_id);
        manifest.as_object_mut().unwrap().remove("excluded_hbar");
        let mut counts = serde_json::Map::new();
        for row in manifest["rows"].as_array_mut().unwrap() {
            row["identity"][0] = json!(binding.source_id);
            let from = row["inferred_original_from_key"]
                .as_str()
                .unwrap()
                .replace("chase_exec:", "batch_exec:");
            row["inferred_original_from_key"] = json!(from);
            let n = counts.entry(from).or_insert(json!(0));
            *n = json!(n.as_u64().unwrap() + 1);
        }
        manifest["from_key_counts"] = json!(counts);
        let first_client = checklist["orders"][0]["exchange_order"]["clientOrderId"].clone();
        let order = &mut checklist["orders"][0]["exchange_order"];
        order["type"] = json!("MARKET");
        order["origType"] = json!("MARKET");
        order["timeInForce"] = json!("GTC");
        order["price"] = json!("0");
        for t in checklist["orders"][0]["exchange_trades"]
            .as_array_mut()
            .unwrap()
        {
            t["maker"] = json!(false);
        }
        for row in manifest["rows"].as_array_mut().unwrap() {
            if row["client_order_id"] == first_client {
                row["maker"] = json!(false);
            }
        }
        let input = Inputs::from_bytes(
            &serde_json::to_vec(&binding).unwrap(),
            &serde_json::to_vec(&manifest).unwrap(),
            &serde_json::to_vec(&checklist).unwrap(),
        )
        .unwrap();
        // A valid prefix alone cannot override the historical Chase owner.
        assert!(input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("attribution not unique"));
        let db = DB::open_cf(&Options::default(), &binding.db_path, NATIVE_CFS).unwrap();
        for (i, row) in manifest["rows"].as_array().unwrap().iter().enumerate() {
            let symbol = row["identity"][1].as_str().unwrap();
            let client = row["client_order_id"]
                .as_str()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            let r = uniform(
                symbol,
                client + 10000,
                row["inferred_original_from_key"].as_str().unwrap(),
                0.0,
                1.0,
            );
            db.put_cf(
                db.cf_handle("uniform_orders").unwrap(),
                format!("{:020}", i + 100),
                native_codec::append_bbo_spread(
                    native_codec::serialize_uniform_order(&r, i as i64 + 100, symbol, 0.0, 0.0),
                    "",
                ),
            )
            .unwrap();
        }
        drop(db);
        assert_eq!(input.run(false, None).unwrap().add_fills, 24);
        let checkpoint = f.root.join("batch_checkpoint");
        let result = input.run(true, Some(&checkpoint)).unwrap();
        assert_eq!(result.add_fills, 24);
        assert_eq!(input.run(false, None).unwrap().present_fills, 24);
        let db = input.open(false).unwrap();
        let mut takers = 0;
        for row in db.iterator_cf(db.cf_handle("uniform_orders").unwrap(), IteratorMode::Start) {
            let (_, bytes) = row.unwrap();
            let r = decode_uniform_order_record(&bytes).unwrap();
            if r.client_order_id.to_string() == first_client.as_str().unwrap() {
                assert_eq!(r.order_type, order_common::OrderType::Market.as_str());
                assert_eq!(r.fill_liquidity, Some(FillLiquidity::Taker));
                assert!(r.from_key.starts_with("batch_exec:"));
                takers += 1;
            }
        }
        assert!(takers > 0);
    }

    #[test]
    fn pinned_unmatched_report_adds_only_uniform_and_survives_rollback() {
        let f = fixture();
        let mut manifest = f.input.manifest.clone();
        let row = &manifest["rows"][0];
        let id = identity(
            &f.input.binding.source_id,
            row["identity"][1].as_str().unwrap(),
            row["identity"][3].as_str().unwrap().parse().unwrap(),
            row["identity"][4].as_str().unwrap().parse().unwrap(),
        );
        let fact = &f.input.facts[&id];
        let order = &fact["exchange_order"];
        let trade = &fact["exchange_trade"];
        let raw = RestTrade {
            symbol: string(trade, "symbol").unwrap().into(),
            order_id: integer(order, "orderId").unwrap(),
            client: client_identity(order).unwrap().0,
            maker: true,
            side: if trade["side"] == "BUY" {
                Side::Buy
            } else {
                Side::Sell
            },
            price: number(trade, "price").unwrap(),
            time: micros(trade, "time").unwrap(),
            cumulative: number(fact, "cumulative_decimal").unwrap(),
            status: OrderStatus::Filled,
        };
        let key = "00000000000000000040";
        let bytes = native_codec::serialize_trade_update(&raw, 40, &raw.symbol, raw.cumulative);
        let db = DB::open_cf(&Options::default(), &f.input.binding.db_path, NATIVE_CFS).unwrap();
        db.put_cf(
            db.cf_handle("trade_updates_unmatched").unwrap(),
            key,
            &bytes,
        )
        .unwrap();
        let before = native_rows(&db);
        drop(db);
        assert!(
            f.input.run(false, None).is_err(),
            "unreviewed positive raw must still refuse"
        );
        manifest["rows"][0]["existing_raw_trade_records"] = json!([{
            "cf":"trade_updates_unmatched", "key":key,
            "value_hex":hex::encode(&bytes), "value_sha256":sha(&bytes)}]);
        let input = Inputs::from_bytes(
            &serde_json::to_vec(&f.input.binding).unwrap(),
            &serde_json::to_vec(&manifest).unwrap(),
            CHECKLIST.as_bytes(),
        )
        .unwrap();
        let result = input
            .run(true, Some(&f.root.join("raw_checkpoint")))
            .unwrap();
        assert_eq!(result.uniform_rows, 24);
        assert_eq!(result.trade_rows, 23);
        assert_eq!(input.run(false, None).unwrap().present_fills, 24);
        let db = input.open(false).unwrap();
        assert_eq!(
            db.get_cf(db.cf_handle("trade_updates_unmatched").unwrap(), key)
                .unwrap()
                .unwrap(),
            bytes
        );
        drop(db);
        input
            .rollback(result.batch_id.as_deref().unwrap(), true)
            .unwrap();
        let db = input.open(false).unwrap();
        assert_eq!(native_rows(&db), before);
        drop(db);
        let mut invalid = manifest.clone();
        invalid["rows"][0]["existing_raw_trade_records"][0]["value_sha256"] = json!("0".repeat(64));
        assert!(Inputs::from_bytes(
            &serde_json::to_vec(&f.input.binding).unwrap(),
            &serde_json::to_vec(&invalid).unwrap(),
            CHECKLIST.as_bytes()
        )
        .is_err());
        let families = DB::list_cf(&Options::default(), &f.input.binding.db_path).unwrap();
        let db = DB::open_cf(&Options::default(), &f.input.binding.db_path, families).unwrap();
        db.delete_cf(db.cf_handle("trade_updates_unmatched").unwrap(), key)
            .unwrap();
        drop(db);
        assert!(
            input.run(false, None).is_err(),
            "disappeared evidence must refuse"
        );
    }

    fn mobile_fixture() -> Fixture {
        let mut f = fixture();
        let mut binding = f.input.binding.clone();
        binding.source_id = "binance_exec_trade04".into();
        binding.ipc_namespace = binding.source_id.clone();
        let mut manifest = f.input.manifest.clone();
        let mut checklist = f.input.checklist.clone();
        manifest["source_id"] = json!(binding.source_id);
        checklist["source_id"] = json!(binding.source_id);
        manifest.as_object_mut().unwrap().remove("excluded_hbar");
        let count = manifest["fill_count"].clone();
        manifest["from_key_counts"] = json!({"external_account_fill:ios":count});
        let first_order = checklist["orders"][0]["exchange_order"]["orderId"]
            .as_i64()
            .unwrap();
        for order in checklist["orders"].as_array_mut().unwrap() {
            let old = order["exchange_order"]["clientOrderId"]
                .as_str()
                .unwrap()
                .to_owned();
            let cid = format!("ios_test{old}");
            let id = order["exchange_order"]["orderId"].as_i64().unwrap();
            if id == first_order {
                order["symbol"] = json!("龙虾USDT");
                order["exchange_order"]["symbol"] = json!("龙虾USDT");
                for trade in order["exchange_trades"].as_array_mut().unwrap() {
                    trade["symbol"] = json!("龙虾USDT");
                }
            }
            let o = &mut order["exchange_order"];
            o["clientOrderId"] = json!(cid);
            o["type"] = json!("MARKET");
            o["origType"] = json!("MARKET");
            o["timeInForce"] = json!("GTC");
            o["reduceOnly"] = json!(true);
            o["price"] = json!("0");
            order["client_order_id"] = json!(-id);
            for trade in order["exchange_trades"].as_array_mut().unwrap() {
                trade["maker"] = json!(false);
            }
            for row in manifest["rows"].as_array_mut().unwrap() {
                row["identity"][0] = json!(binding.source_id);
                if row["client_order_id"] == old {
                    if id == first_order {
                        row["identity"][1] = json!("龙虾USDT");
                    }
                    row["client_order_id"] = json!(cid);
                    row["native_client_order_id"] = json!(-id);
                    row["maker"] = json!(false);
                    row["attribution_kind"] = json!("external_account");
                    row["inferred_original_from_key"] = json!("external_account_fill:ios");
                    row["attribution_basis"] = json!(
                        "No strategy identity; original mobile client ID is retained in audit, negative exchange-order-ID placeholder in native codec"
                    );
                }
            }
        }
        f.input = Inputs::from_bytes(
            &serde_json::to_vec(&binding).unwrap(),
            &serde_json::to_vec(&manifest).unwrap(),
            &serde_json::to_vec(&checklist).unwrap(),
        )
        .unwrap();
        f
    }

    #[test]
    fn mobile_fills_keep_original_identity_taker_role_and_no_strategy_through_rollback() {
        let f = mobile_fixture();
        let baseline = native_rows(&f.input.open(false).unwrap());
        assert_eq!(f.input.run(false, None).unwrap().add_fills, 24);
        let result = f
            .input
            .run(true, Some(&f.root.join("mobile_checkpoint")))
            .unwrap();
        assert_eq!(result.present_fills, 24);
        assert_eq!(result.excluded_fills, 0);
        assert_eq!(f.input.run(false, None).unwrap().add_fills, 0);
        assert_eq!(f.input.run(true, None).unwrap().add_fills, 0);
        let db = f.input.open(false).unwrap();
        for (id, fact) in &f.input.facts {
            let audit: FillAudit = serde_json::from_slice(
                &db.get_cf(db.cf_handle(REPAIRS).unwrap(), id)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let u = decode_uniform_order_record(&hex::decode(&audit.rows[0].value_hex).unwrap())
                .unwrap();
            let t = decode_trade_record(&hex::decode(&audit.rows[1].value_hex).unwrap()).unwrap();
            assert_eq!(
                u.client_order_id,
                -integer(&fact["exchange_order"], "orderId").unwrap()
            );
            assert_eq!(t.order_id, -u.client_order_id);
            assert_eq!(t.client_order_id, u.client_order_id);
            assert!(!t.is_maker);
            assert_eq!(u.fill_liquidity, Some(FillLiquidity::Taker));
            assert_eq!(u.order_type, order_common::OrderType::Market.as_str());
            assert_eq!(u.from_key, "external_account_fill:ios");
            assert_eq!(
                audit.facts["native_client_identity"]["original_exchange_client_id"],
                fact["exchange_order"]["clientOrderId"]
            );
            assert!(audit.facts["native_client_identity"]["strategy_identity"].is_null());
        }
        drop(db);
        f.input.rollback(&result.batch_id.unwrap(), true).unwrap();
        assert_eq!(native_rows(&f.input.open(false).unwrap()), baseline);
    }

    #[test]
    fn mobile_fills_reject_strategy_labels_non_reduce_orders_and_existing_account_facts() {
        let f = mobile_fixture();
        let mut m = f.input.manifest.clone();
        m["rows"][0]["inferred_original_from_key"] = json!("chase_exec:rbf_small");
        assert!(Inputs::from_bytes(
            &serde_json::to_vec(&f.input.binding).unwrap(),
            &serde_json::to_vec(&m).unwrap(),
            &serde_json::to_vec(&f.input.checklist).unwrap()
        )
        .is_err());
        let mut c = f.input.checklist.clone();
        c["orders"][0]["exchange_order"]["reduceOnly"] = json!(false);
        assert!(Inputs::from_bytes(
            &serde_json::to_vec(&f.input.binding).unwrap(),
            &serde_json::to_vec(&f.input.manifest).unwrap(),
            &serde_json::to_vec(&c).unwrap()
        )
        .is_err());
        let fact = f.input.facts.values().next().unwrap();
        let db = f.input.open(true).unwrap();
        let t = RestTrade {
            symbol: string(&fact["exchange_trade"], "symbol").unwrap().into(),
            order_id: integer(&fact["exchange_order"], "orderId").unwrap(),
            client: 0,
            maker: false,
            side: Side::Sell,
            price: 1.0,
            time: 1,
            cumulative: 1.0,
            status: OrderStatus::Filled,
        };
        db.put_cf(
            db.cf_handle("trade_updates_unmatched").unwrap(),
            "00000000000000000999",
            native_codec::serialize_trade_update(&t, 999, &t.symbol, 1.0),
        )
        .unwrap();
        let before = native_rows(&db);
        drop(db);
        assert!(f
            .input
            .run(true, Some(&f.root.join("refused_mobile_backup")))
            .unwrap_err()
            .to_string()
            .contains("native cumulative fill exists"));
        assert_eq!(native_rows(&f.input.open(false).unwrap()), before);
        assert!(!f.root.join("refused_mobile_backup").exists());
    }

    #[test]
    fn audited_24_fills_apply_idempotency_native_decoding_and_rollback() {
        let f = fixture();
        let baseline = native_rows(&f.input.open(false).unwrap());
        assert_eq!(f.input.run(false, None).unwrap().add_fills, 24);
        assert!(f.input.open(false).unwrap().cf_handle(REPAIRS).is_none());
        let result = f.input.run(true, Some(&f.root.join("backup"))).unwrap();
        assert_eq!(result.present_fills, 24);
        assert_eq!(result.uniform_rows, 24);
        assert_eq!(result.trade_rows, 24);
        assert_eq!(result.order_rows, 0);
        assert_eq!(f.input.run(false, None).unwrap().add_fills, 0);
        assert_eq!(f.input.run(true, None).unwrap().add_fills, 0);
        let db = f.input.open(false).unwrap();
        let mut quote = 0.0;
        for (id, facts) in &f.input.facts {
            let audit: FillAudit = serde_json::from_slice(
                &db.get_cf(db.cf_handle(REPAIRS).unwrap(), id)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(audit.facts["exchange_trade"]["commissionAsset"], "BNB");
            let r = decode_uniform_order_record(&hex::decode(&audit.rows[0].value_hex).unwrap())
                .unwrap();
            assert!(r.client_order_id > 0);
            assert!(r.from_key.starts_with("chase_exec:"));
            assert_eq!(r.fill_liquidity, Some(FillLiquidity::Maker));
            assert_eq!(r.signal_ts, 0);
            assert!(r.signal_bbo.is_none());
            assert!(r.bbo_spread.is_empty());
            assert_eq!(
                r.update_ts,
                integer(&facts["exchange_trade"], "time").unwrap() * 1000
            );
            quote += r.amount_update * r.price;
            if r.symbol == "SANDUSDT" {
                assert_eq!(r.status, "PARTIALLY_FILLED");
                assert_eq!(audit.facts["exchange_order"]["status"], "CANCELED");
                assert_eq!(r.amount_init, 178.0);
            }
        }
        assert!((quote - 1430.07274340).abs() < 1e-9);
        drop(db);
        let id = result.batch_id.unwrap();
        f.input.rollback(&id, false).unwrap();
        f.input.rollback(&id, true).unwrap();
        assert_eq!(native_rows(&f.input.open(false).unwrap()), baseline);
        assert_eq!(
            f.input.rollback(&id, true).unwrap()["status"],
            "already_rolled_back"
        );
        assert_eq!(f.input.run(false, None).unwrap().add_fills, 24);
    }
    #[test]
    fn identity_conflict_or_changed_bytes_refuse_without_partial_rollback() {
        let f = fixture();
        let result = f.input.run(true, Some(&f.root.join("backup"))).unwrap();
        let db = f.input.open(true).unwrap();
        let id = f.input.facts.keys().next().unwrap();
        let a: FillAudit = serde_json::from_slice(
            &db.get_cf(db.cf_handle(REPAIRS).unwrap(), id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let r = &a.rows[0];
        db.put_cf(db.cf_handle(&r.cf).unwrap(), &r.key, b"changed")
            .unwrap();
        let before = native_rows(&db);
        drop(db);
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("repaired bytes changed"));
        assert!(f
            .input
            .rollback(result.batch_id.as_deref().unwrap(), true)
            .unwrap_err()
            .to_string()
            .contains("rollback refused"));
        assert_eq!(native_rows(&f.input.open(false).unwrap()), before);
    }
    #[test]
    fn collision_and_existing_raw_fills_refuse_before_any_write() {
        let f = fixture();
        let db = f.input.open(true).unwrap();
        let plan = f.input.plan(&db, 900).unwrap();
        let r = &plan.rows[0];
        db.put_cf(
            db.cf_handle(&r.cf).unwrap(),
            &r.key,
            hex::decode(&r.value_hex).unwrap(),
        )
        .unwrap();
        let before = native_rows(&db);
        assert!(f
            .input
            .plan(&db, 900)
            .unwrap_err()
            .to_string()
            .contains("native positive fill exists"));
        assert_eq!(native_rows(&db), before);
    }
    #[test]
    fn sync_state_or_wrong_identity_or_negative_client_refuse() {
        let mut f = fixture();
        let mut db = f.input.open(true).unwrap();
        db.create_cf("sync_meta", &Options::default()).unwrap();
        db.put_cf(db.cf_handle("sync_meta").unwrap(), b"seq", b"1")
            .unwrap();
        drop(db);
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("replication state"));
        f.input.binding.db_identity = "wrong".into();
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("identity mismatch"));
        let mut checklist: Value = serde_json::from_str(CHECKLIST).unwrap();
        checklist["orders"][0]["exchange_order"]["clientOrderId"] = json!("-1");
        assert!(Inputs::from_bytes(
            &serde_json::to_vec(&f.input.binding).unwrap(),
            MANIFEST.as_bytes(),
            &serde_json::to_vec(&checklist).unwrap()
        )
        .is_err());
    }
    #[test]
    fn missing_hbar_or_conflicting_attribution_refuse() {
        let f = fixture();
        let db = f.input.open(true).unwrap();
        db.delete_cf(
            db.cf_handle("trade_updates").unwrap(),
            "00000000000000000011",
        )
        .unwrap();
        drop(db);
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("HBAR exclusion"));
        let f = fixture();
        let db = f.input.open(true).unwrap();
        let row = &f.input.manifest["rows"][0];
        let symbol = row["identity"][1].as_str().unwrap();
        let client = row["client_order_id"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let r = uniform(symbol, client + 20000, "chase_exec:conflict", 0.0, 1.0);
        db.put_cf(
            db.cf_handle("uniform_orders").unwrap(),
            "00000000000000000012",
            native_codec::append_bbo_spread(
                native_codec::serialize_uniform_order(&r, 12, symbol, 0.0, 0.0),
                "",
            ),
        )
        .unwrap();
        drop(db);
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("attribution not unique"));
    }
    #[test]
    fn physical_collision_raw_gap_and_identity_fact_conflict_are_atomic() {
        let f = fixture();
        let db = f.input.open(true).unwrap();
        let r = uniform("OTHERUSDT", 42, "chase_exec:other", 1.0, 2.0);
        db.put_cf(
            db.cf_handle("uniform_orders").unwrap(),
            "00000000000000000900",
            native_codec::append_bbo_spread(
                native_codec::serialize_uniform_order(&r, 900, "OTHERUSDT", 1.0, 1.0),
                "",
            ),
        )
        .unwrap();
        let before = native_rows(&db);
        assert!(f
            .input
            .plan(&db, 900)
            .unwrap_err()
            .to_string()
            .contains("physical key collision"));
        assert_eq!(native_rows(&db), before);
        drop(db);
        let f = fixture();
        let db = f.input.open(true).unwrap();
        let fact = f.input.facts.values().next().unwrap();
        let t = RestTrade {
            symbol: string(&fact["exchange_trade"], "symbol").unwrap().into(),
            order_id: integer(&fact["exchange_order"], "orderId").unwrap(),
            client: string(&fact["exchange_order"], "clientOrderId")
                .unwrap()
                .parse()
                .unwrap(),
            maker: true,
            side: Side::Sell,
            price: 1.0,
            time: 1,
            cumulative: 1.0,
            status: OrderStatus::PartiallyFilled,
        };
        db.put_cf(
            db.cf_handle("trade_updates_unmatched").unwrap(),
            "00000000000000000900",
            native_codec::serialize_trade_update(&t, 900, &t.symbol, 1.0),
        )
        .unwrap();
        drop(db);
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("native cumulative fill exists"));
        let f = fixture();
        f.input.run(true, Some(&f.root.join("backup"))).unwrap();
        let db = f.input.open(true).unwrap();
        let key = f.input.facts.keys().next().unwrap();
        let cf = db.cf_handle(REPAIRS).unwrap();
        let mut a: FillAudit =
            serde_json::from_slice(&db.get_cf(cf, key).unwrap().unwrap()).unwrap();
        a.facts["exchange_trade"]["commission"] = json!("0");
        db.put_cf(cf, key, serde_json::to_vec(&a).unwrap()).unwrap();
        let before = native_rows(&db);
        drop(db);
        assert!(f
            .input
            .run(false, None)
            .unwrap_err()
            .to_string()
            .contains("identity conflict"));
        assert_eq!(native_rows(&f.input.open(false).unwrap()), before);
    }
    #[test]
    fn rollback_keeps_later_native_records_and_checkpoint_contains_originals() {
        let f = fixture();
        let original = native_rows(&f.input.open(false).unwrap());
        let backup = f.root.join("backup");
        let result = f.input.run(true, Some(&backup)).unwrap();
        let opts = Options::default();
        let cfs = DB::list_cf(&opts, &backup).unwrap();
        let copy = DB::open_cf_for_read_only(&opts, &backup, cfs, false).unwrap();
        assert_eq!(native_rows(&copy), original);
        drop(copy);
        let db = f.input.open(true).unwrap();
        let r = uniform("OTHERUSDT", 42, "chase_exec:other", 1.0, 2.0);
        let payload = native_codec::append_bbo_spread(
            native_codec::serialize_uniform_order(&r, 900, "OTHERUSDT", 1.0, 1.0),
            "",
        );
        let key = "00000000000000000900";
        db.put_cf(db.cf_handle("uniform_orders").unwrap(), key, &payload)
            .unwrap();
        drop(db);
        f.input
            .rollback(result.batch_id.as_deref().unwrap(), true)
            .unwrap();
        let mut expected = original;
        expected.insert(
            ("uniform_orders".into(), key.as_bytes().to_vec()),
            payload.to_vec(),
        );
        assert_eq!(native_rows(&f.input.open(false).unwrap()), expected);
    }
    #[test]
    fn offline_gate_refuses_a_running_environment_producer() {
        let f = fixture();
        let exe = f.input.binding.env_root.join("persist_manager");
        fs::copy("/bin/sleep", &exe).unwrap();
        let mut child = std::process::Command::new(&exe)
            .arg("30")
            .current_dir(&f.input.binding.env_root)
            .spawn()
            .unwrap();
        let result = f.input.run(true, Some(&f.root.join("backup")));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("offline write refused"));
        assert!(!f.root.join("backup").exists());
        assert!(f.input.open(false).unwrap().cf_handle(REPAIRS).is_none());
    }
}
