use crate::common::min_qty_table::MinQtyTable;

/// 净头寸查询 trait
///
/// 用于统一获取不同账户类型的净头寸（标的资产数量）。
/// - 对于 balance：返回币种余额（已考虑借贷影响）
/// - 对于 UM 合约：返回净持仓量 = (long - short) × ctMult
pub trait NetPosition {
    /// 获取某个 symbol 的净头寸（标的资产数量）
    ///
    /// # 参数
    /// - `symbol`: 交易对符号（如 "BTC", "BTCUSDT"）
    /// - `min_qty_table`: 可选的交易对信息表，用于获取合约乘数
    ///
    /// # 返回
    /// - 标的资产数量（正数表示多仓/持有，负数表示空仓/借贷）
    /// - 如果不存在该 symbol，返回 0.0
    fn net_position(&self, symbol: &str, min_qty_table: Option<&MinQtyTable>) -> f64;
}
