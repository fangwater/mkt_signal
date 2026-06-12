use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::msg::basic_account_msg::{
    BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg, BasicUmUnrealizedMsg,
    BinanceBasicOrderMsg, GateBasicOrderMsg, OkexOrderMsg,
};
use crate::msg::bitget_account_msg::BitgetBasicOrderMsg;
use crate::msg::bybit_account_msg::BybitBasicOrderMsg;

pub mod binance_basic_account_event_parser;
pub mod bitget_account_event_parser;
pub mod bybit_account_event_parser;
pub mod gate_account_event_parser;
pub mod okex_account_event_parser;

pub trait AccountEventSink {
    fn emit(&self, msg: Bytes) -> bool;

    fn emit_with_dedup_key(&self, msg: Bytes, dedup_key: u64) -> bool {
        let _ = dedup_key;
        self.emit(msg)
    }
}

pub trait Parser: Send {
    fn parse<S: AccountEventSink>(&self, msg: Bytes, sink: &S) -> usize;
}

#[inline]
pub fn emit_with_key<S: AccountEventSink>(sink: &S, msg: Bytes, key: u64) -> bool {
    sink.emit_with_dedup_key(msg, key)
}

#[inline]
pub fn hash64(parts: &[u64]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for p in parts {
        p.hash(&mut hasher);
    }
    hasher.finish()
}

#[inline]
pub fn hash_str64(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[inline]
pub fn scoped_dedup_key(account_scope: BasicAccountScope, key: u64) -> u64 {
    hash64(&[account_scope as u32 as u64, key])
}

#[inline]
pub fn balance_dedup_key(account_scope: BasicAccountScope, msg: &BasicBalanceMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::BalanceUpdate as u32 as u64,
            msg.timestamp as u64,
            hash_str64(&msg.symbol),
            msg.wallet.to_bits(),
        ]),
    )
}

#[inline]
pub fn borrow_interest_dedup_key(
    account_scope: BasicAccountScope,
    msg: &BasicBorrowInterestMsg,
) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::BorrowInterest as u32 as u64,
            msg.timestamp as u64,
            hash_str64(&msg.symbol),
            msg.borrowed.to_bits(),
            msg.interest.to_bits(),
        ]),
    )
}

#[inline]
pub fn position_dedup_key(account_scope: BasicAccountScope, msg: &BasicPositionMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::PositionUpdate as u32 as u64,
            msg.timestamp as u64,
            hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.position_amount.to_bits() as u64,
        ]),
    )
}

#[inline]
pub fn unrealized_pnl_dedup_key(
    account_scope: BasicAccountScope,
    msg: &BasicUmUnrealizedMsg,
) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::UnrealizedPnlUpdate as u32 as u64,
            msg.timestamp as u64,
            hash_str64(&msg.inst_id),
            msg.position_side as u8 as u64,
            msg.unrealized_pnl.to_bits(),
        ]),
    )
}

#[inline]
pub fn account_risk_dedup_key(account_scope: BasicAccountScope, msg: &BasicAccountRiskMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::AccountRisk as u32 as u64,
            msg.timestamp as u64,
            msg.adj_equity_usd.to_bits(),
            msg.maintenance_margin_usd.to_bits(),
            msg.margin_ratio.to_bits(),
        ]),
    )
}

#[inline]
pub fn binance_order_dedup_key(
    account_scope: BasicAccountScope,
    msg: &BinanceBasicOrderMsg,
) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ]),
    )
}

#[inline]
pub fn okex_order_dedup_key(account_scope: BasicAccountScope, msg: &OkexOrderMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.ord_id as u64,
            msg.cl_ord_id as u64,
            msg.update_time as u64,
            msg.state as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ]),
    )
}

#[inline]
pub fn gate_order_dedup_key(account_scope: BasicAccountScope, msg: &GateBasicOrderMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ]),
    )
}

#[inline]
pub fn bitget_order_dedup_key(account_scope: BasicAccountScope, msg: &BitgetBasicOrderMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ]),
    )
}

#[inline]
pub fn bybit_order_dedup_key(account_scope: BasicAccountScope, msg: &BybitBasicOrderMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::OrderUpdate as u32 as u64,
            msg.order_id as u64,
            msg.client_order_id as u64,
            msg.event_time as u64,
            msg.execution_type as u64,
            msg.order_status as u64,
            msg.cumulative_filled_quantity.to_bits(),
        ]),
    )
}

#[inline]
pub fn trade_lite_dedup_key(account_scope: BasicAccountScope, msg: &BasicTradeLiteMsg) -> u64 {
    scoped_dedup_key(
        account_scope,
        hash64(&[
            BasicAccountEventType::TradeUpdateLite as u32 as u64,
            msg.client_order_id as u64,
            hash_str64(msg.trade_id_str()),
            msg.event_time as u64,
            msg.last_executed_price.to_bits(),
            msg.last_executed_quantity.to_bits(),
        ]),
    )
}

#[cfg(test)]
pub(crate) mod test_sink {
    use super::AccountEventSink;
    use bytes::Bytes;
    use std::cell::RefCell;

    pub(crate) struct TestAccountEventSink {
        msgs: RefCell<Vec<Bytes>>,
    }

    impl TestAccountEventSink {
        pub(crate) fn new() -> Self {
            Self {
                msgs: RefCell::new(Vec::new()),
            }
        }

        pub(crate) fn recv(&self) -> Option<Bytes> {
            let mut msgs = self.msgs.borrow_mut();
            if msgs.is_empty() {
                None
            } else {
                Some(msgs.remove(0))
            }
        }
    }

    impl AccountEventSink for TestAccountEventSink {
        fn emit(&self, msg: Bytes) -> bool {
            self.msgs.borrow_mut().push(msg);
            true
        }
    }
}
