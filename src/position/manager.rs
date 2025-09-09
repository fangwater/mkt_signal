use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::sync::Arc;
use chrono::Utc;

use crate::common::exchange::Exchange;
use super::types::{Position, PositionSummary, PositionType, PositionSide, PositionStatus};
use super::api::ExchangeApiClient;

/// 统一的仓位管理器
/// 负责管理和维护所有仓位状态
pub struct PositionManager {
    /// 交易所类型
    exchange: Exchange,
    /// 仓位存储: (symbol, position_type) -> Position
    positions: Arc<RwLock<HashMap<(String, PositionType), Position>>>,
    /// API客户端（用于拉取仓位数据）
    api_client: Arc<dyn ExchangeApiClient>,
}

impl PositionManager {
    /// 创建新的仓位管理器
    pub fn new(exchange: Exchange, api_client: Arc<dyn ExchangeApiClient>) -> Self {
        Self {
            exchange,
            positions: Arc::new(RwLock::new(HashMap::new())),
            api_client,
        }
    }

    /// 获取交易所类型
    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    /// 从交易所同步仓位数据
    pub async fn sync_positions(&self) -> Result<()> {
        // 从API获取所有仓位
        let raw_positions = self.api_client.fetch_all_positions().await?;
        
        // 转换并更新仓位
        let mut positions = self.positions.write().await;
        positions.clear(); // 清空旧数据
        
        for raw in raw_positions {
            let position = self.api_client.normalize_position(raw);
            let key = (position.symbol.clone(), position.position_type);
            positions.insert(key, position);
        }
        
        Ok(())
    }

    /// 添加或更新单个仓位
    pub async fn update_position(&self, position: Position) -> Result<()> {
        let key = (position.symbol.clone(), position.position_type);
        let mut positions = self.positions.write().await;
        positions.insert(key, position);
        Ok(())
    }

    /// 获取指定交易对的仓位
    pub async fn get_position(&self, symbol: &str, position_type: PositionType) -> Result<Option<Position>> {
        let positions = self.positions.read().await;
        Ok(positions.get(&(symbol.to_string(), position_type)).cloned())
    }

    /// 获取所有仓位
    pub async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let positions = self.positions.read().await;
        Ok(positions.values().cloned().collect())
    }

    /// 获取仓位汇总信息
    pub async fn get_summary(&self) -> Result<PositionSummary> {
        let positions = self.positions.read().await;
        
        let mut summary = PositionSummary::default();
        summary.total_positions = positions.len();
        
        for position in positions.values() {
            // 统计开仓数量
            if position.status == PositionStatus::Open {
                summary.open_positions += 1;
            }
            
            // 累加价值
            summary.total_value += position.value();
            
            // 累加盈亏
            if let Some(unrealized_pnl) = position.unrealized_pnl {
                summary.total_unrealized_pnl += unrealized_pnl;
            }
            if let Some(realized_pnl) = position.realized_pnl {
                summary.total_realized_pnl += realized_pnl;
            }
            
            // 按类型统计
            *summary.positions_by_type.entry(position.position_type).or_insert(0) += 1;
        }
        
        summary.update_time = Utc::now();
        Ok(summary)
    }

    /// 删除指定仓位
    pub async fn remove_position(&self, symbol: &str, position_type: PositionType) -> Result<()> {
        let mut positions = self.positions.write().await;
        positions.remove(&(symbol.to_string(), position_type));
        Ok(())
    }

    /// 清空所有仓位
    pub async fn clear_all(&self) -> Result<()> {
        let mut positions = self.positions.write().await;
        positions.clear();
        Ok(())
    }

    /// 更新标记价格
    pub async fn update_mark_price(&self, symbol: &str, position_type: PositionType, mark_price: f64) -> Result<()> {
        let mut positions = self.positions.write().await;
        
        if let Some(position) = positions.get_mut(&(symbol.to_string(), position_type)) {
            position.update_mark_price(mark_price);
        }
        
        Ok(())
    }

    /// 批量更新标记价格
    pub async fn batch_update_mark_prices(&self, prices: HashMap<String, f64>) -> Result<()> {
        let mut positions = self.positions.write().await;
        
        for ((symbol, _), position) in positions.iter_mut() {
            if let Some(&mark_price) = prices.get(symbol) {
                position.update_mark_price(mark_price);
            }
        }
        
        Ok(())
    }

    /// 获取按交易对分组的仓位
    pub async fn get_positions_by_symbol(&self) -> Result<HashMap<String, Vec<Position>>> {
        let positions = self.positions.read().await;
        let mut grouped: HashMap<String, Vec<Position>> = HashMap::new();
        
        for position in positions.values() {
            grouped.entry(position.symbol.clone())
                .or_insert_with(Vec::new)
                .push(position.clone());
        }
        
        Ok(grouped)
    }

    /// 获取指定类型的所有仓位
    pub async fn get_positions_by_type(&self, position_type: PositionType) -> Result<Vec<Position>> {
        let positions = self.positions.read().await;
        
        Ok(positions.values()
            .filter(|p| p.position_type == position_type)
            .cloned()
            .collect())
    }

    /// 获取盈利的仓位
    pub async fn get_profitable_positions(&self) -> Result<Vec<Position>> {
        let positions = self.positions.read().await;
        
        Ok(positions.values()
            .filter(|p| p.is_profitable())
            .cloned()
            .collect())
    }

    /// 获取亏损的仓位
    pub async fn get_losing_positions(&self) -> Result<Vec<Position>> {
        let positions = self.positions.read().await;
        
        Ok(positions.values()
            .filter(|p| !p.is_profitable() && p.unrealized_pnl.is_some())
            .cloned()
            .collect())
    }

    /// 计算总资产价值
    pub async fn calculate_total_equity(&self) -> Result<f64> {
        let positions = self.positions.read().await;
        
        let total = positions.values()
            .map(|p| p.value())
            .sum();
        
        Ok(total)
    }

    /// 检查是否有仓位接近强平
    pub async fn check_liquidation_risk(&self, threshold_percentage: f64) -> Result<Vec<Position>> {
        let positions = self.positions.read().await;
        let mut at_risk = Vec::new();
        
        for position in positions.values() {
            // 只检查合约仓位
            if position.position_type == PositionType::Perpetual {
                if let (Some(mark_price), Some(liq_price)) = (position.mark_price, position.liquidation_price) {
                    let distance = match position.side {
                        PositionSide::Long => {
                            // 多头：当前价格距离强平价格的百分比
                            ((mark_price - liq_price) / mark_price) * 100.0
                        }
                        PositionSide::Short => {
                            // 空头：强平价格距离当前价格的百分比
                            ((liq_price - mark_price) / mark_price) * 100.0
                        }
                    };
                    
                    // 如果距离强平价格小于阈值，加入风险列表
                    if distance < threshold_percentage && distance > 0.0 {
                        at_risk.push(position.clone());
                    }
                }
            }
        }
        
        Ok(at_risk)
    }
}