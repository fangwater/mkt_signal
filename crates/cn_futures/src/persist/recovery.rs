//! Startup-only, read-only view of persist_manager's domestic store. Runs once
//! before the business loop; never during trading. Everything Exec needs is
//! derived from the two crypto-format logs, like crypto positions from fills:
//! next OrderRef per session, requests whose outcome was not observed, the
//! strategy attribution ledger and the trade ids already recorded.
use anyhow::{Context, Result};
use cn_futures_target::ledger::{Row, UNALLOCATED};
use order_common::OrderStatus;
use persist_common::cn_futures::{store_cf, Day, NativeId, PersistedOrderKey, Session, Venue};
use persist_common::{native_codec, uniform_fill, UNIFORM_ORDER_TYPE_INTERNAL_CROSS};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

/// `from_key` of lifecycle records for orders this process placed. Orders of
/// an exec target batch append `:batch_id`.
pub const EXEC_FROM_KEY: &str = "exec";
/// `from_key` of lifecycle records for orders placed elsewhere.
pub const OBSERVED_FROM_KEY: &str = "observed";

/// A recorded fill, identified by venue, trading day and exchange trade id.
pub type TradeKey = (Venue, Day, NativeId);

/// Durable state a new Exec process needs before its first venue reconciliation.
#[derive(Debug, Default)]
pub struct DurableRecovery {
    /// Next unused OrderRef per (trading day, native session).
    pub reference_floors: HashMap<(Day, Session), u64>,
    /// Orders this Exec placed whose last recorded state was still open.
    pub unresolved: HashSet<PersistedOrderKey>,
    /// Strategy attribution per contract (`EXCHANGE:symbol`), named strategies only.
    pub ledger: Vec<(String, Row)>,
    /// Fills already in `cn_trade_updates`; recovery never records them twice.
    pub known_trades: HashSet<TradeKey>,
}

fn is_strategy(from_key: &str) -> bool {
    from_key != UNALLOCATED
        && from_key != OBSERVED_FROM_KEY
        && from_key != EXEC_FROM_KEY
        && !from_key.starts_with("exec:")
}

impl DurableRecovery {
    /// Lock-free beside the persist_manager writer. A missing database, or one
    /// without the domestic column families, is an empty account history.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let options = rocksdb::Options::default();
        let existing = rocksdb::DB::list_cf(&options, path)
            .with_context(|| format!("list column families of {}", path.display()))?;
        if !store_cf::ALL
            .iter()
            .all(|cf| existing.iter().any(|name| name == cf))
        {
            return Ok(Self::default());
        }
        let mut options = rocksdb::Options::default();
        // Leave low-numbered descriptors to the SDK's select-based networking.
        options.set_max_open_files(128);
        let db = rocksdb::DB::open_cf_for_read_only(&options, path, store_cf::ALL, false)
            .with_context(|| format!("open native store read-only at {}", path.display()))?;
        let cf = |name| db.cf_handle(name).context("native store column family");
        let mut recovery = Self::default();
        // Last recorded lifecycle of each order this Exec placed.
        let mut open: HashMap<i64, (PersistedOrderKey, bool)> = HashMap::new();
        let mut ledger: BTreeMap<String, Row> = BTreeMap::new();
        for item in db.iterator_cf(cf(store_cf::ORDERS)?, rocksdb::IteratorMode::Start) {
            let (_, value) = item?;
            let stored = uniform_fill::decode(&value).context("decode stored uniform order")?;
            let r = stored.record;
            let from_key = std::str::from_utf8(&r.from_key).context("uniform from_key")?;
            let venue = Venue::from_code(r.venue).context("non-domestic venue in native store")?;
            let symbol = std::str::from_utf8(&r.symbol).context("uniform symbol")?;
            if r.amount_update > 0.0 {
                if is_strategy(from_key) {
                    let lots = r.amount_update.round() as i64;
                    let signed = if r.side == order_common::Side::Buy.to_u8() {
                        lots
                    } else {
                        -lots
                    };
                    let row = ledger.entry(venue.market(symbol)).or_default();
                    *row.entry(from_key.to_owned()).or_default() += signed;
                }
                continue;
            }
            if r.ttype == UNIFORM_ORDER_TYPE_INTERNAL_CROSS {
                continue;
            }
            let ext = r
                .extension
                .context("domestic uniform order without extension")?;
            let day = Day::new(ext.trading_day.as_deref().context("uniform trading day")?)?;
            let key = PersistedOrderKey::from_client_order_text(
                day,
                venue,
                ext.client_order_text
                    .as_deref()
                    .context("uniform client order text")?,
            )?;
            if let Ok(reference) = key.native.order_ref.parse::<u64>() {
                if let Some(next) = reference.checked_add(1) {
                    let floor = recovery
                        .reference_floors
                        .entry((key.trading_day, key.native.session))
                        .or_insert(1);
                    *floor = (*floor).max(next);
                }
            }
            let still_open = !matches!(
                OrderStatus::from_u8(r.status),
                Some(OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Expired)
            );
            let placed_here = from_key == EXEC_FROM_KEY || from_key.starts_with("exec:");
            open.insert(r.client_order_id, (key, placed_here && still_open));
        }
        recovery.unresolved = open
            .into_values()
            .filter_map(|(key, unresolved)| unresolved.then_some(key))
            .collect();
        recovery.ledger = ledger
            .into_iter()
            .filter_map(|(market, mut row)| {
                row.retain(|_, lots| *lots != 0);
                (!row.is_empty()).then_some((market, row))
            })
            .collect();
        for item in db.iterator_cf(cf(store_cf::TRADES)?, rocksdb::IteratorMode::Start) {
            let (_, value) = item?;
            let trade =
                native_codec::decode_trade_update(&value).context("decode stored trade update")?;
            let venue =
                Venue::from_code(trade.venue).context("non-domestic venue in native store")?;
            let ext = trade
                .extension
                .context("domestic trade update without extension")?;
            recovery.known_trades.insert((
                venue,
                Day::new(ext.trading_day.as_deref().context("trade trading day")?)?,
                NativeId::new(ext.trade_id.as_deref().context("trade id")?)?,
            ));
        }
        Ok(recovery)
    }
}
