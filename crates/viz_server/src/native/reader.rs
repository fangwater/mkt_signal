use super::{
    config::{resolved, Counter},
    state::{observe, Lifecycle, ObservedOrder, OrderPhase},
};
use anyhow::{ensure, Context, Result};
use persist_common::cn_futures::{NativeOrderRecord, NativeTradeRecord, StoreIdentity};
use persist_manager::cn_futures::{Evidence, EVENTS, META, ORDERS, TRADES};
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde_json::{json, Value};

pub struct Reader {
    db: DB,
    lifecycle: Lifecycle,
    orders: Vec<ObservedOrder>,
    fills: Vec<NativeTradeRecord>,
    total_orders: usize,
    active_orders: usize,
    total_fills: usize,
    loaded: bool,
}
impl Reader {
    pub fn open(source: &Counter) -> Result<Self> {
        let primary = source
            .rocksdb_path
            .canonicalize()
            .context("resolve native execution store")?;
        let secondary = resolved(&source.secondary_path)?;
        ensure!(
            !secondary.starts_with(&primary) && !primary.starts_with(&secondary),
            "secondary overlaps primary"
        );
        let mut options = Options::default();
        options.set_max_open_files(-1); // RocksDB secondary requirement.
        let cfs =
            DB::list_cf(&options, &primary).context("list native execution column families")?;
        for cf in [META, EVENTS, ORDERS, TRADES] {
            ensure!(
                cfs.iter().any(|c| c == cf),
                "native execution store missing {cf}"
            );
        }
        std::fs::create_dir_all(&secondary).context("create native viz secondary directory")?;
        let db = DB::open_cf_as_secondary(&options, &primary, &secondary, &cfs)
            .context("open native viz secondary")?;
        let reader = Self {
            db,
            lifecycle: Lifecycle::default(),
            orders: vec![],
            fills: vec![],
            total_orders: 0,
            active_orders: 0,
            total_fills: 0,
            loaded: false,
        };
        reader.validate_identity(source)?;
        Ok(reader)
    }
    fn validate_identity(&self, source: &Counter) -> Result<()> {
        let bytes = self
            .db
            .get_cf(
                self.db.cf_handle(META).context("native meta handle")?,
                b"identity",
            )?
            .context("native store missing identity")?;
        let identity: StoreIdentity =
            serde_json::from_slice(&bytes).context("decode native store identity")?;
        ensure!(
            identity.adapter == source.adapter.as_str(),
            "configured adapter does not match execution store"
        );
        ensure!(
            identity.account_hash == source.expected_account_hash,
            "configured account binding does not match execution store"
        );
        Ok(())
    }
    pub fn read(
        &mut self,
        source: &Counter,
        limit: usize,
        now: u64,
        stale_ms: u64,
    ) -> Result<Value> {
        ensure!((1..=1000).contains(&limit), "invalid native row limit");
        // Secondary snapshots are unsupported. Catch up once, then do not advance
        // this handle while scanning any of the four column families.
        self.db
            .try_catch_up_with_primary()
            .context("catch up native viz secondary")?;
        self.validate_identity(source)?;
        let bytes = self.db.get_cf(
            self.db.cf_handle(META).context("native meta handle")?,
            b"sequence",
        )?;
        let sequence: u64 = bytes
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or(0);
        ensure!(
            sequence >= self.lifecycle.sequence,
            "native store sequence went backwards"
        );
        if !self.loaded || sequence != self.lifecycle.sequence {
            let start = self.lifecycle.sequence.to_be_bytes();
            for item in self.db.iterator_cf(
                self.db.cf_handle(EVENTS).context("native events handle")?,
                IteratorMode::From(&start, Direction::Forward),
            ) {
                let (key, bytes) = item.context("scan native evidence")?;
                let event: Evidence =
                    serde_json::from_slice(&bytes).context("decode native evidence")?;
                ensure!(
                    key.as_ref() == event.sequence.to_be_bytes(),
                    "native evidence key mismatch"
                );
                if event.sequence <= self.lifecycle.sequence {
                    continue;
                }
                ensure!(
                    event.sequence == self.lifecycle.sequence + 1 && event.sequence <= sequence,
                    "native evidence sequence gap"
                );
                self.lifecycle.apply(&event)?;
            }
            ensure!(
                self.lifecycle.sequence == sequence,
                "native evidence tail is incomplete"
            );
            self.scan_records(limit)?;
            self.loaded = true;
        }
        Ok(json!({
            "id":source.id, "label":source.label, "account":source.account, "adapter":source.adapter,
            "read_at_us":now, "read_error":null, "sequence":sequence.to_string(),
            "lifecycle":self.lifecycle.view(now, stale_ms),
            "total_orders":self.total_orders, "active_orders":self.active_orders, "total_fills":self.total_fills,
            "orders_omitted":self.total_orders.saturating_sub(self.orders.len()),
            "fills_omitted":self.total_fills.saturating_sub(self.fills.len()),
            "orders":self.orders, "fills":self.fills,
        }))
    }
    fn scan_records(&mut self, limit: usize) -> Result<()> {
        let mut orders = Vec::new();
        let mut fills = Vec::new();
        self.total_orders = 0;
        self.active_orders = 0;
        self.total_fills = 0;
        for item in self.db.iterator_cf(
            self.db.cf_handle(ORDERS).context("native orders handle")?,
            IteratorMode::Start,
        ) {
            let (_, bytes) = item.context("scan native orders")?;
            let record: NativeOrderRecord =
                serde_json::from_slice(&bytes).context("decode native order")?;
            let order = observe(record)?;
            self.total_orders += 1;
            self.active_orders += usize::from(unresolved(&order));
            orders.push(order);
            if orders.len() > limit * 2 {
                trim_orders(&mut orders, limit);
            }
        }
        for item in self.db.iterator_cf(
            self.db.cf_handle(TRADES).context("native fills handle")?,
            IteratorMode::Start,
        ) {
            let (_, bytes) = item.context("scan native fills")?;
            let fill: NativeTradeRecord =
                serde_json::from_slice(&bytes).context("decode native fill")?;
            ensure!(
                fill.fill.lots > 0 && fill.fill.price.is_finite() && fill.fill.price > 0.0,
                "invalid native factual fill"
            );
            ensure!(
                fill.commission.is_none_or(f64::is_finite),
                "invalid reported commission"
            );
            self.total_fills += 1;
            fills.push(fill);
            if fills.len() > limit * 2 {
                trim_fills(&mut fills, limit);
            }
        }
        trim_orders(&mut orders, limit);
        trim_fills(&mut fills, limit);
        self.orders = orders;
        self.fills = fills;
        Ok(())
    }
}
fn unresolved(order: &ObservedOrder) -> bool {
    !matches!(
        order.phase,
        OrderPhase::Filled | OrderPhase::Cancelled | OrderPhase::Rejected
    )
}
fn trim_orders(orders: &mut Vec<ObservedOrder>, limit: usize) {
    orders.sort_by_key(|o| {
        (
            std::cmp::Reverse(unresolved(o)),
            std::cmp::Reverse(o.record.updated_us),
        )
    });
    orders.truncate(limit);
}
fn trim_fills(fills: &mut Vec<NativeTradeRecord>, limit: usize) {
    fills.sort_by_key(|f| std::cmp::Reverse(f.received_us));
    fills.truncate(limit);
}
