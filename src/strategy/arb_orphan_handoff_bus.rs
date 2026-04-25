use crate::strategy::manager::{ArbOrphanHandoff, ArbOrphanResidualHandoff};
use std::cell::RefCell;
use std::collections::HashSet;

#[derive(Default)]
struct ArbOrphanHandoffBus {
    handoffs: Vec<ArbOrphanHandoff>,
    residuals: Vec<ArbOrphanResidualHandoff>,
    keep_local_on_drop_order_ids: HashSet<i64>,
}

thread_local! {
    static ARB_ORPHAN_HANDOFF_BUS: RefCell<ArbOrphanHandoffBus> =
        RefCell::new(ArbOrphanHandoffBus::default());
}

pub fn queue_arb_orphan_handoff(handoff: ArbOrphanHandoff) -> bool {
    // Bus 层只做跨策略暂存、重复 handoff 过滤，以及 Drop 清理时保留本地订单。
    if handoff.client_order_id <= 0 {
        return false;
    }

    ARB_ORPHAN_HANDOFF_BUS.with(|cell| {
        let mut bus = cell.borrow_mut();
        if bus
            .handoffs
            .iter()
            .any(|pending| pending.client_order_id == handoff.client_order_id)
        {
            return false;
        }
        bus.keep_local_on_drop_order_ids
            .insert(handoff.client_order_id);
        bus.handoffs.push(handoff);
        true
    })
}

pub fn queue_arb_orphan_residual(residual: ArbOrphanResidualHandoff) {
    ARB_ORPHAN_HANDOFF_BUS.with(|cell| {
        cell.borrow_mut().residuals.push(residual);
    });
}

pub fn drain_arb_orphan_handoffs() -> Vec<ArbOrphanHandoff> {
    ARB_ORPHAN_HANDOFF_BUS.with(|cell| std::mem::take(&mut cell.borrow_mut().handoffs))
}

pub fn drain_arb_orphan_residuals() -> Vec<ArbOrphanResidualHandoff> {
    ARB_ORPHAN_HANDOFF_BUS.with(|cell| std::mem::take(&mut cell.borrow_mut().residuals))
}

pub fn take_keep_local_on_drop_order_id(client_order_id: i64) -> bool {
    ARB_ORPHAN_HANDOFF_BUS.with(|cell| {
        cell.borrow_mut()
            .keep_local_on_drop_order_ids
            .remove(&client_order_id)
    })
}
