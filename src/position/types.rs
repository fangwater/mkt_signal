use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;

/// 仓位类型
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PositionType {
    /// 现货
    Spot,
    /// 永续合约
    Perpetual,
}

impl fmt::Display for PositionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PositionType::Spot => write!(f, "现货"),
            PositionType::Perpetual => write!(f, "永续合约"),
        }
    }
}

/// 仓位方向
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PositionSide {
    /// 多头
    Long,
    /// 空头
    Short,
}

impl fmt::Display for PositionSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PositionSide::Long => write!(f, "多头"),
            PositionSide::Short => write!(f, "空头"),
        }
    }
}

/// 仓位状态
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PositionStatus {
    /// 开仓
    Open,
    /// 平仓
    Closed,
    /// 部分平仓
    Partial,
}

/// 仓位数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// 交易对符号 (例如: BTCUSDT)
    pub symbol: String,
    /// 仓位类型
    pub position_type: PositionType,
    /// 仓位方向 (现货没有方向概念，统一使用Long)
    pub side: PositionSide,
    /// 仓位状态
    pub status: PositionStatus,
    /// 持仓数量
    pub quantity: f64,
    /// 开仓均价
    pub entry_price: f64,
    /// 当前标记价格
    pub mark_price: Option<f64>,
    /// 未实现盈亏
    pub unrealized_pnl: Option<f64>,
    /// 已实现盈亏
    pub realized_pnl: Option<f64>,
    /// 保证金 (仅合约)
    pub margin: Option<f64>,
    /// 杠杆倍数 (仅合约)
    pub leverage: Option<u32>,
    /// 强平价格 (仅合约)
    pub liquidation_price: Option<f64>,
    /// 更新时间
    pub update_time: DateTime<Utc>,
    /// 额外元数据
    pub metadata: HashMap<String, String>,
}

impl Position {
    /// 创建现货仓位
    pub fn new_spot(symbol: String, quantity: f64, entry_price: f64) -> Self {
        Self {
            symbol,
            position_type: PositionType::Spot,
            side: PositionSide::Long,  // 现货默认为多头
            status: PositionStatus::Open,
            quantity,
            entry_price,
            mark_price: None,
            unrealized_pnl: None,
            realized_pnl: None,
            margin: None,
            leverage: None,
            liquidation_price: None,
            update_time: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// 创建永续合约仓位
    pub fn new_perpetual(
        symbol: String,
        side: PositionSide,
        quantity: f64,
        entry_price: f64,
        leverage: u32,
    ) -> Self {
        Self {
            symbol,
            position_type: PositionType::Perpetual,
            side,
            status: PositionStatus::Open,
            quantity,
            entry_price,
            mark_price: None,
            unrealized_pnl: None,
            realized_pnl: None,
            margin: None,
            leverage: Some(leverage),
            liquidation_price: None,
            update_time: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// 更新标记价格并重新计算未实现盈亏
    pub fn update_mark_price(&mut self, mark_price: f64) {
        self.mark_price = Some(mark_price);
        self.calculate_unrealized_pnl();
        self.update_time = Utc::now();
    }

    /// 计算未实现盈亏
    fn calculate_unrealized_pnl(&mut self) {
        if let Some(mark_price) = self.mark_price {
            let pnl = match self.position_type {
                PositionType::Spot => {
                    // 现货的未实现盈亏
                    (mark_price - self.entry_price) * self.quantity
                }
                PositionType::Perpetual => {
                    // 永续合约的未实现盈亏
                    match self.side {
                        PositionSide::Long => (mark_price - self.entry_price) * self.quantity,
                        PositionSide::Short => (self.entry_price - mark_price) * self.quantity,
                    }
                }
            };
            self.unrealized_pnl = Some(pnl);
        }
    }

    /// 获取仓位当前价值
    pub fn value(&self) -> f64 {
        let price = self.mark_price.unwrap_or(self.entry_price);
        price * self.quantity
    }

    /// 判断仓位是否盈利
    pub fn is_profitable(&self) -> bool {
        self.unrealized_pnl.map_or(false, |pnl| pnl > 0.0)
    }

    /// 获取盈亏比例
    pub fn pnl_percentage(&self) -> Option<f64> {
        self.unrealized_pnl.map(|pnl| {
            let cost = self.entry_price * self.quantity;
            (pnl / cost) * 100.0
        })
    }
}

/// 仓位汇总信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummary {
    /// 总仓位数
    pub total_positions: usize,
    /// 开仓数量
    pub open_positions: usize,
    /// 总价值
    pub total_value: f64,
    /// 总未实现盈亏
    pub total_unrealized_pnl: f64,
    /// 总已实现盈亏
    pub total_realized_pnl: f64,
    /// 按类型统计的仓位数量
    pub positions_by_type: HashMap<PositionType, usize>,
    /// 更新时间
    pub update_time: DateTime<Utc>,
}

impl Default for PositionSummary {
    fn default() -> Self {
        Self {
            total_positions: 0,
            open_positions: 0,
            total_value: 0.0,
            total_unrealized_pnl: 0.0,
            total_realized_pnl: 0.0,
            positions_by_type: HashMap::new(),
            update_time: Utc::now(),
        }
    }
}