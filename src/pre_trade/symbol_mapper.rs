use std::fmt::Debug;

use crate::common::exchange::Exchange;
use crate::pre_trade::symbol_util::extract_base_asset;

/// Symbol 映射 trait，处理不同交易所的 symbol 格式转换
///
/// 不同交易所有不同的命名规则：
/// - OKX: Balance="BTC", InstId="BTC-USDT-SWAP", PriceSymbol="BTCUSDT"
/// - Binance: Balance="BTC", Symbol="BTCUSDT", PriceSymbol="BTCUSDT"
/// - Gate: 各有不同
pub trait SymbolMapper: Debug {
    /// 从 balance 资产名映射到用于查询 UM 持仓的 symbol
    ///
    /// # 例子
    /// - OKX: "BTC" -> "BTCUSDT"
    /// - Binance: "BTC" -> "BTCUSDT"
    fn balance_asset_to_um_symbol(&self, asset: &str) -> String;

    /// 从 inst_id 提取基础资产名
    ///
    /// # 例子
    /// - OKX: "BTC-USDT-SWAP" -> "BTC"
    /// - Binance: "BTCUSDT" -> "BTC"
    fn inst_id_to_base_asset(&self, inst_id: &str) -> Option<String>;

    /// 从资产名映射到用于查询价格的 symbol
    ///
    /// # 例子
    /// - OKX: "BTC" -> "BTCUSDT"
    /// - Binance: "BTC" -> "BTCUSDT"
    fn asset_to_price_symbol(&self, asset: &str) -> String;
}

/// OKX 的 symbol 映射实现
#[derive(Debug)]
pub struct OkexSymbolMapper;

impl SymbolMapper for OkexSymbolMapper {
    fn balance_asset_to_um_symbol(&self, asset: &str) -> String {
        let upper = asset.to_uppercase();
        if upper == "USDT" {
            "USDT".to_string()
        } else {
            format!("{}USDT", upper)
        }
    }

    fn inst_id_to_base_asset(&self, inst_id: &str) -> Option<String> {
        // OKX: BTC-USDT-SWAP -> BTC
        // 兼容: BTCUSDT -> BTC（某些流程会提前归一化 inst_id）
        extract_base_asset(inst_id)
    }

    fn asset_to_price_symbol(&self, asset: &str) -> String {
        let upper = asset.to_uppercase();
        if upper == "USDT" {
            "USDT".to_string()
        } else {
            format!("{}USDT", upper)
        }
    }
}

/// Binance 的 symbol 映射实现
#[derive(Debug)]
pub struct BinanceSymbolMapper;

impl SymbolMapper for BinanceSymbolMapper {
    fn balance_asset_to_um_symbol(&self, asset: &str) -> String {
        let upper = asset.to_uppercase();
        if upper == "USDT" {
            "USDT".to_string()
        } else {
            format!("{}USDT", upper)
        }
    }

    fn inst_id_to_base_asset(&self, inst_id: &str) -> Option<String> {
        extract_base_asset(inst_id)
    }

    fn asset_to_price_symbol(&self, asset: &str) -> String {
        let upper = asset.to_uppercase();
        if upper == "USDT" {
            "USDT".to_string()
        } else {
            format!("{}USDT", upper)
        }
    }
}

/// Gate 的 symbol 映射实现
#[derive(Debug)]
pub struct GateSymbolMapper;

impl SymbolMapper for GateSymbolMapper {
    fn balance_asset_to_um_symbol(&self, asset: &str) -> String {
        let upper = asset.to_uppercase();
        if upper == "USDT" {
            "USDT".to_string()
        } else {
            format!("{}USDT", upper)
        }
    }

    fn inst_id_to_base_asset(&self, inst_id: &str) -> Option<String> {
        extract_base_asset(inst_id)
    }

    fn asset_to_price_symbol(&self, asset: &str) -> String {
        let upper = asset.to_uppercase();
        if upper == "USDT" {
            "USDT".to_string()
        } else {
            format!("{}USDT", upper)
        }
    }
}

/// 根据交易所创建对应的 SymbolMapper
pub fn create_symbol_mapper(exchange: Exchange) -> Box<dyn SymbolMapper> {
    match exchange {
        Exchange::Okex => Box::new(OkexSymbolMapper),
        Exchange::Binance => Box::new(BinanceSymbolMapper),
        Exchange::Gate => Box::new(GateSymbolMapper),
        Exchange::Hyperliquid => Box::new(BinanceSymbolMapper),
        _ => panic!("SymbolMapper not implemented for {:?}", exchange),
    }
}
