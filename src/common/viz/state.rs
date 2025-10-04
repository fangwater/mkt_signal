use std::collections::HashMap;
use std::rc::Rc;

use chrono::Utc;

use crate::common::account_msg::{
    AccountEventType, AccountPositionMsg, AccountUpdateBalanceMsg, AccountUpdatePositionMsg,
};
use crate::common::time_util::get_timestamp_us;
use crate::exchange::Exchange;
use crate::pre_trade::binance_pm_spot_manager::{BinanceSpotBalance, BinanceSpotBalanceSnapshot};
use crate::pre_trade::binance_pm_um_manager::{
    BinanceUmAccountSnapshot, BinanceUmPosition, PositionSide,
};
use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::price_table::{PriceEntry, PriceTable};

#[derive(Clone)]
pub struct SharedState {
    inner: Rc<SharedStateInner>,
}

pub struct SharedStateInner {
    pub price_table: Rc<std::cell::RefCell<PriceTable>>,
    pub exposure_mgr: Rc<std::cell::RefCell<ExposureManager>>,
    // snapshots maintained incrementally
    spot_balances: std::cell::RefCell<BinanceSpotBalanceSnapshot>,
    um_positions: std::cell::RefCell<BinanceUmAccountSnapshot>,
    // freshness
    pub last_account_update: std::cell::Cell<i64>,
    pub last_price_update: std::cell::Cell<i64>,
    // funding stream cache: symbol -> (funding_rate, next_funding_time, ts)
    funding_stream: std::cell::RefCell<HashMap<String, (f64, i64, i64)>>,
}

impl SharedState {
    pub fn new() -> Self {
        let price_table = Rc::new(std::cell::RefCell::new(PriceTable::new()));
        let empty_spot = BinanceSpotBalanceSnapshot {
            balances: vec![],
            fetched_at: Utc::now(),
        };
        let empty_um = BinanceUmAccountSnapshot {
            positions: vec![],
            fetched_at: Utc::now(),
        };
        let exposure_mgr = Rc::new(std::cell::RefCell::new(ExposureManager::new(
            &empty_um,
            &empty_spot,
        )));
        Self {
            inner: Rc::new(SharedStateInner {
                price_table,
                exposure_mgr,
                spot_balances: std::cell::RefCell::new(empty_spot),
                um_positions: std::cell::RefCell::new(empty_um),
                last_account_update: std::cell::Cell::new(0),
                last_price_update: std::cell::Cell::new(0),
                funding_stream: std::cell::RefCell::new(HashMap::new()),
            }),
        }
    }

    pub fn price_table(&self) -> Rc<std::cell::RefCell<PriceTable>> {
        self.inner.price_table.clone()
    }
    pub fn exposure_mgr(&self) -> Rc<std::cell::RefCell<ExposureManager>> {
        self.inner.exposure_mgr.clone()
    }

    pub fn handle_account_event(&self, msg_type: AccountEventType, payload: &[u8]) {
        match msg_type {
            AccountEventType::AccountUpdateBalance => {
                if let Ok(m) = AccountUpdateBalanceMsg::from_bytes(payload) {
                    self.apply_balance_snapshot(&m);
                }
            }
            AccountEventType::AccountPosition => {
                if let Ok(m) = AccountPositionMsg::from_bytes(payload) {
                    self.apply_account_position(&m);
                }
            }
            AccountEventType::AccountUpdatePosition => {
                if let Ok(m) = AccountUpdatePositionMsg::from_bytes(payload) {
                    self.apply_um_position(&m);
                }
            }
            _ => {}
        }
    }

    fn apply_balance_snapshot(&self, msg: &AccountUpdateBalanceMsg) {
        let mut spot = self.inner.spot_balances.borrow_mut();
        let upper = msg.asset.to_uppercase();
        if let Some(bal) = spot
            .balances
            .iter_mut()
            .find(|b| b.asset.eq_ignore_ascii_case(&upper))
        {
            bal.total_wallet_balance = msg.wallet_balance;
            bal.cross_margin_asset = msg.cross_wallet_balance;
            if bal.cross_margin_locked <= msg.cross_wallet_balance {
                bal.cross_margin_free = msg.cross_wallet_balance - bal.cross_margin_locked;
            } else {
                bal.cross_margin_locked = 0.0;
                bal.cross_margin_free = msg.cross_wallet_balance;
            }
            bal.update_time = msg.event_time;
            bal.negative_balance = bal.total_wallet_balance < 0.0 || bal.cross_margin_free < 0.0;
        } else {
            spot.balances.push(BinanceSpotBalance {
                asset: upper,
                total_wallet_balance: msg.wallet_balance,
                cross_margin_asset: msg.cross_wallet_balance,
                cross_margin_borrowed: 0.0,
                cross_margin_free: msg.cross_wallet_balance,
                cross_margin_interest: 0.0,
                cross_margin_locked: 0.0,
                um_wallet_balance: 0.0,
                um_unrealized_pnl: 0.0,
                cm_wallet_balance: 0.0,
                cm_unrealized_pnl: 0.0,
                update_time: msg.event_time,
                negative_balance: msg.wallet_balance < 0.0 || msg.cross_wallet_balance < 0.0,
            });
        }
        drop(spot);
        self.recompute_exposure();
        self.inner.last_account_update.set(get_timestamp_us());
    }

    fn apply_account_position(&self, msg: &AccountPositionMsg) {
        let mut spot = self.inner.spot_balances.borrow_mut();
        let upper = msg.asset.to_uppercase();
        let wallet_balance = msg.free_balance + msg.locked_balance;
        if let Some(bal) = spot
            .balances
            .iter_mut()
            .find(|b| b.asset.eq_ignore_ascii_case(&upper))
        {
            bal.total_wallet_balance = wallet_balance;
            bal.cross_margin_free = msg.free_balance;
            bal.cross_margin_locked = msg.locked_balance;
            bal.cross_margin_asset = wallet_balance;
            bal.update_time = msg.update_time;
            bal.negative_balance = bal.total_wallet_balance < 0.0 || bal.cross_margin_free < 0.0;
        } else {
            spot.balances.push(BinanceSpotBalance {
                asset: upper,
                total_wallet_balance: wallet_balance,
                cross_margin_asset: wallet_balance,
                cross_margin_borrowed: 0.0,
                cross_margin_free: msg.free_balance,
                cross_margin_interest: 0.0,
                cross_margin_locked: msg.locked_balance,
                um_wallet_balance: 0.0,
                um_unrealized_pnl: 0.0,
                cm_wallet_balance: 0.0,
                cm_unrealized_pnl: 0.0,
                update_time: msg.update_time,
                negative_balance: wallet_balance < 0.0,
            });
        }
        drop(spot);
        self.recompute_exposure();
        self.inner.last_account_update.set(get_timestamp_us());
    }

    fn apply_um_position(&self, msg: &AccountUpdatePositionMsg) {
        let mut um = self.inner.um_positions.borrow_mut();
        let side = match msg.position_side {
            'B' | 'b' => PositionSide::Both,
            'L' | 'l' => PositionSide::Long,
            'S' | 's' => PositionSide::Short,
            _ => PositionSide::Both,
        };
        let upper = msg.symbol.to_uppercase();
        if let Some(p) = um
            .positions
            .iter_mut()
            .find(|p| p.symbol.eq_ignore_ascii_case(&upper) && p.position_side == side)
        {
            p.position_amt = msg.position_amount;
            p.entry_price = msg.entry_price;
            p.unrealized_profit = msg.unrealized_pnl;
            p.update_time = msg.event_time;
        } else {
            um.positions.push(BinanceUmPosition {
                symbol: upper,
                position_side: side,
                leverage: 0.0,
                entry_price: msg.entry_price,
                position_amt: msg.position_amount,
                unrealized_profit: msg.unrealized_pnl,
                initial_margin: 0.0,
                maint_margin: 0.0,
                position_initial_margin: 0.0,
                open_order_initial_margin: 0.0,
                max_notional: 0.0,
                update_time: msg.event_time,
            });
        }
        drop(um);
        self.recompute_exposure();
        self.inner.last_account_update.set(get_timestamp_us());
    }

    fn recompute_exposure(&self) {
        let spot = self.inner.spot_balances.borrow();
        let um = self.inner.um_positions.borrow();
        self.inner.exposure_mgr.borrow_mut().recompute(&um, &spot);
    }

    pub fn update_price_mark(&self, symbol: &str, mark: f64, ts: i64) {
        let mut pt = self.inner.price_table.borrow_mut();
        pt.update_mark_price(symbol, mark, ts);
        self.inner.last_price_update.set(get_timestamp_us());
    }
    pub fn update_price_index(&self, symbol: &str, index: f64, ts: i64) {
        let mut pt = self.inner.price_table.borrow_mut();
        pt.update_index_price(symbol, index, ts);
        self.inner.last_price_update.set(get_timestamp_us());
    }

    pub fn snapshot_account(&self) -> (f64, f64, Vec<ExposureEntry>) {
        let mgr = self.inner.exposure_mgr.borrow();
        (
            mgr.total_equity(),
            mgr.total_abs_exposure(),
            mgr.exposures().to_vec(),
        )
    }

    pub fn snapshot_prices(&self, symbols: Option<&[String]>) -> Vec<(String, Option<PriceEntry>)> {
        let pt = self.inner.price_table.borrow();
        let snap = pt.snapshot();
        let mut out = Vec::new();
        match symbols {
            Some(list) => {
                for s in list {
                    let upper = s.to_uppercase();
                    let entry = snap.get(&upper).cloned();
                    out.push((upper, entry));
                }
            }
            None => {
                for (sym, entry) in snap.into_iter() {
                    out.push((sym, Some(entry)));
                }
            }
        }
        out
    }

    pub fn last_account_update_us(&self) -> i64 {
        self.inner.last_account_update.get()
    }
    pub fn last_price_update_us(&self) -> i64 {
        self.inner.last_price_update.get()
    }

    pub fn compute_unrealized_pnl(&self) -> f64 {
        let um = self.inner.um_positions.borrow();
        let pt = self.inner.price_table.borrow();
        let table = pt.snapshot();
        let mut map: HashMap<String, (Option<f64>, Option<f64>)> = HashMap::new();
        for (sym, e) in table {
            map.insert(
                sym.to_uppercase(),
                (Some(e.mark_price), Some(e.index_price)),
            );
        }
        let mut pnl = 0.0;
        for pos in &um.positions {
            let key = pos.symbol.to_uppercase();
            let mark = map
                .get(&key)
                .and_then(|(m, _)| *m)
                .unwrap_or(pos.entry_price);
            let signed_amt = match pos.position_side {
                PositionSide::Long => pos.position_amt,
                PositionSide::Short => -pos.position_amt,
                PositionSide::Both => pos.position_amt,
            };
            pnl += signed_amt * (mark - pos.entry_price);
        }
        pnl
    }

    pub fn funding_for(
        &self,
        symbol: &str,
        _exchange: Exchange,
        _ts_ms: i64,
    ) -> (Option<f64>, f64, Option<i64>, f64) {
        // funding_rate: 首选实时行情缓存，如无则 None
        let fr_cache = self.inner.funding_stream.borrow();
        let key = symbol.to_uppercase();
        let (stream_rate, next_time) = fr_cache.get(&key).map(|(r, n, _)| (*r, *n)).unzip();
        drop(fr_cache);
        // 预测/借贷展示从策略迁移后，这里返回0占位
        (stream_rate, 0.0, next_time, 0.0)
    }

    pub fn set_stream_funding(&self, symbol: &str, funding_rate: f64, next_time: i64, ts: i64) {
        let mut cache = self.inner.funding_stream.borrow_mut();
        cache.insert(symbol.to_uppercase(), (funding_rate, next_time, ts));
    }
}
