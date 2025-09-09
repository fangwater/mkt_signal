use super::types::{Position, PositionSide, PositionType};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// 账户余额信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    /// 资产名称 (例如: USDT, BTC)
    pub asset: String,
    /// 可用余额
    pub free: f64,
    /// 冻结余额
    pub locked: f64,
    /// 总余额
    pub total: f64,
}

/// 原始仓位数据（从API返回的原始格式）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawPosition {
    /// 交易对
    pub symbol: String,
    /// 仓位类型
    pub position_type: PositionType,
    /// 仓位方向
    pub side: PositionSide,
    /// 持仓数量
    pub quantity: f64,
    /// 开仓均价
    pub entry_price: f64,
    /// 标记价格
    pub mark_price: Option<f64>,
    /// 未实现盈亏
    pub unrealized_pnl: Option<f64>,
    /// 已实现盈亏
    pub realized_pnl: Option<f64>,
    /// 保证金
    pub margin: Option<f64>,
    /// 杠杆倍数
    pub leverage: Option<u32>,
    /// 强平价格
    pub liquidation_price: Option<f64>,
}

/// 组合保证金（Portfolio Margin）账户汇总信息（字段按接口可用性可选）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PmAccountSummary {
    /// 账户总权益
    pub account_equity: Option<f64>,
    /// 总维持保证金
    pub total_maint_margin: Option<f64>,
    /// 总初始保证金
    pub total_initial_margin: Option<f64>,
    /// 总未实现盈亏
    pub total_unrealized_pnl: Option<f64>,
    /// 总保证金余额
    pub total_margin_balance: Option<f64>,
    /// 最大可提取金额
    pub max_withdraw_amount: Option<f64>,
}

/// 交易所API客户端trait
/// 不同交易所实现这个trait来提供统一的接口
#[async_trait]
pub trait ExchangeApiClient: Send + Sync {
    /// 获取现货账户余额
    async fn fetch_spot_balances(&self) -> Result<Vec<Balance>>;

    /// 获取合约账户余额
    async fn fetch_futures_balances(&self) -> Result<Vec<Balance>>;

    /// 获取现货持仓（现货实际上是余额）
    async fn fetch_spot_positions(&self) -> Result<Vec<RawPosition>>;

    /// 获取永续合约持仓
    async fn fetch_perpetual_positions(&self) -> Result<Vec<RawPosition>>;

    /// 获取所有仓位（现货+合约）
    async fn fetch_all_positions(&self) -> Result<Vec<RawPosition>> {
        let mut positions = Vec::new();

        // 获取现货仓位
        if let Ok(spot) = self.fetch_spot_positions().await {
            positions.extend(spot);
        }

        // 获取合约仓位
        if let Ok(perpetual) = self.fetch_perpetual_positions().await {
            positions.extend(perpetual);
        }

        Ok(positions)
    }

    /// 将原始仓位转换为标准仓位格式
    fn normalize_position(&self, raw: RawPosition) -> Position {
        let mut position = match raw.position_type {
            PositionType::Spot => Position::new_spot(raw.symbol, raw.quantity, raw.entry_price),
            PositionType::Perpetual => Position::new_perpetual(
                raw.symbol,
                raw.side,
                raw.quantity,
                raw.entry_price,
                raw.leverage.unwrap_or(1),
            ),
        };

        // 设置可选字段
        if let Some(mark_price) = raw.mark_price {
            position.mark_price = Some(mark_price);
        }
        position.unrealized_pnl = raw.unrealized_pnl;
        position.realized_pnl = raw.realized_pnl;
        position.margin = raw.margin;
        position.liquidation_price = raw.liquidation_price;

        position
    }

    /// 组合保证金账户汇总（默认None，具体交易所可实现）
    async fn fetch_pm_summary(&self) -> Result<Option<PmAccountSummary>> {
        Ok(None)
    }
}

/// API配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// API Key
    pub api_key: String,
    /// API Secret
    pub api_secret: String,
    /// API Passphrase (仅OKX需要)
    pub passphrase: Option<String>,
    /// 是否测试网
    pub testnet: bool,
    /// 基础URL（可选，用于覆盖默认值）
    pub base_url: Option<String>,
}

impl ApiConfig {
    /// 从环境变量创建币安配置
    pub fn from_env_binance() -> Result<Self> {
        Ok(Self {
            api_key: std::env::var("BINANCE_API_KEY")
                .map_err(|_| anyhow::anyhow!("BINANCE_API_KEY 未设置"))?,
            api_secret: std::env::var("BINANCE_API_SECRET")
                .map_err(|_| anyhow::anyhow!("BINANCE_API_SECRET 未设置"))?,
            passphrase: None,
            testnet: std::env::var("BINANCE_TESTNET")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            base_url: std::env::var("BINANCE_BASE_URL").ok(),
        })
    }

    /// 从环境变量创建OKX配置
    pub fn from_env_okx() -> Result<Self> {
        Ok(Self {
            api_key: std::env::var("OKX_API_KEY")
                .map_err(|_| anyhow::anyhow!("OKX_API_KEY 未设置"))?,
            api_secret: std::env::var("OKX_API_SECRET")
                .map_err(|_| anyhow::anyhow!("OKX_API_SECRET 未设置"))?,
            passphrase: Some(
                std::env::var("OKX_API_PASSPHRASE")
                    .map_err(|_| anyhow::anyhow!("OKX_API_PASSPHRASE 未设置"))?,
            ),
            testnet: std::env::var("OKX_TESTNET")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            base_url: std::env::var("OKX_BASE_URL").ok(),
        })
    }

    /// 从环境变量创建Bybit配置
    pub fn from_env_bybit() -> Result<Self> {
        Ok(Self {
            api_key: std::env::var("BYBIT_API_KEY")
                .map_err(|_| anyhow::anyhow!("BYBIT_API_KEY 未设置"))?,
            api_secret: std::env::var("BYBIT_API_SECRET")
                .map_err(|_| anyhow::anyhow!("BYBIT_API_SECRET 未设置"))?,
            passphrase: None,
            testnet: std::env::var("BYBIT_TESTNET")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            base_url: std::env::var("BYBIT_BASE_URL").ok(),
        })
    }
}
