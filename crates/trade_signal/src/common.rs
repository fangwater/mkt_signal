//! 资金费率模块通用定义
//!
//! 包含：
//! - 枚举类型：套利方向、操作类型、因子模式、资金费率周期等
//! - 数据结构：行情报价、资金费率数据
//! - 辅助函数：浮点数比较、数字列表解析

use std::collections::VecDeque;

use depth_pub_common::query_client::DepthQueryClient;
use log::warn;
use order_common::TradingVenue;
use runtime_common::exchange::Exchange;
use signal_common::venue_min_qty_table::VenueMinQtyTable;

pub fn format_tlen_value(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.8}")
    } else {
        "nan".to_string()
    }
}

pub fn append_tlen_to_from_key(base_from_key: &str, level_tlen: f64) -> String {
    format!("{base_from_key}:tlen={}", format_tlen_value(level_tlen))
}

/// Hot-path builder that is byte-for-byte identical to
/// `append_tlen_to_from_key(base_from_key, level_tlen).into_bytes()` but writes
/// straight into a single freshly-sized `Vec<u8>`, skipping the intermediate
/// `format_tlen_value` / `format!` `String` allocations. One allocation per
/// level (the owned output) instead of two, with no format-driven regrowth and
/// no re-formatting of the shared base (it is memcpy'd in as bytes).
pub fn build_tlen_from_key_bytes(base_from_key: &str, level_tlen: f64) -> Vec<u8> {
    use std::io::Write;
    let base = base_from_key.as_bytes();
    // ":tlen=" (6 bytes) + a formatted `{:.8}` value (or "nan"). 32 bytes of
    // headroom covers realistic tlen magnitudes without a reallocation.
    let mut out = Vec::with_capacity(base.len() + 32);
    out.extend_from_slice(base);
    out.extend_from_slice(b":tlen=");
    if level_tlen.is_finite() {
        // `write!` into a `Vec<u8>` (io::Write) drives the same float formatter
        // as `format!("{:.8}")`, so the emitted bytes are identical.
        let _ = write!(out, "{level_tlen:.8}");
    } else {
        out.extend_from_slice(b"nan");
    }
    out
}

pub fn query_batch_tlens_or_zero(
    source: &str,
    depth_query_client: &DepthQueryClient,
    symbol: &str,
    tick_indices: &[i64],
) -> Vec<f64> {
    if tick_indices.is_empty() {
        return Vec::new();
    }

    if let Some(tlens) = super::local_tlen::query_batch_local(
        source,
        depth_query_client.venue_slug(),
        symbol,
        tick_indices,
    ) {
        return tlens;
    }

    match depth_query_client.query_batch_tick_indices(symbol, tick_indices) {
        Ok(tlens) => {
            if tlens.len() == tick_indices.len() {
                tlens
            } else {
                warn!(
                    "{source}: tlen batch query returned mismatched length symbol={} requested={} got={}",
                    symbol,
                    tick_indices.len(),
                    tlens.len()
                );
                let mut out = tlens;
                out.resize(tick_indices.len(), 0.0);
                out
            }
        }
        Err(err) => {
            warn!(
                "{source}: tlen batch query failed symbol={} levels={} err={:#}",
                symbol,
                tick_indices.len(),
                err
            );
            vec![0.0; tick_indices.len()]
        }
    }
}

pub fn normalize_tlen_for_compare(
    _source: &str,
    _table: &VenueMinQtyTable,
    _venue: TradingVenue,
    _symbol: &str,
    level_tlen: f64,
) -> f64 {
    // depth_pub now emits depth/tlen in base-asset qty directly.
    level_tlen
}

pub fn normalize_tlens_for_compare(
    source: &str,
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol: &str,
    tlens: Vec<f64>,
) -> Vec<f64> {
    tlens
        .into_iter()
        .map(|level_tlen| normalize_tlen_for_compare(source, table, venue, symbol, level_tlen))
        .collect()
}

pub fn apply_open_tlen_gate_and_build_from_keys(
    source: &str,
    depth_query_client: &DepthQueryClient,
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol: &str,
    tick_indices: &[i64],
    base_from_key: &str,
    threshold: Option<f64>,
) -> (Vec<Option<Vec<u8>>>, usize) {
    if tick_indices.is_empty() {
        return (Vec::new(), 0);
    }

    let tlens = normalize_tlens_for_compare(
        source,
        table,
        venue,
        symbol,
        query_batch_tlens_or_zero(source, depth_query_client, symbol, tick_indices),
    );
    let mut filtered = 0usize;
    let out = tlens
        .into_iter()
        .map(|level_tlen| {
            if let Some(threshold) = threshold {
                if level_tlen < threshold {
                    filtered += 1;
                    return None;
                }
            }
            Some(build_tlen_from_key_bytes(base_from_key, level_tlen))
        })
        .collect();
    (out, filtered)
}

// ========== 资金费率周期 ==========

/// 资金费率周期类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FundingRatePeriod {
    /// 1小时周期（一天24次）
    Hours1,
    /// 2小时周期（一天12次）
    Hours2,
    /// 4小时周期（一天6次）
    Hours4,
    /// 6小时周期（一天4次）
    Hours6,
    /// 8小时周期（一天3次）
    #[default]
    Hours8,
}

impl FundingRatePeriod {
    /// 返回周期小时数
    pub fn hours(&self) -> u32 {
        match self {
            FundingRatePeriod::Hours1 => 1,
            FundingRatePeriod::Hours2 => 2,
            FundingRatePeriod::Hours4 => 4,
            FundingRatePeriod::Hours6 => 6,
            FundingRatePeriod::Hours8 => 8,
        }
    }

    /// 返回每天收取次数
    pub fn times_per_day(&self) -> u32 {
        match self {
            FundingRatePeriod::Hours1 => 24,
            FundingRatePeriod::Hours2 => 12,
            FundingRatePeriod::Hours4 => 6,
            FundingRatePeriod::Hours6 => 4,
            FundingRatePeriod::Hours8 => 3,
        }
    }

    /// 计算指定天数需要拉取的条数
    pub fn calculate_limit(&self, days: u32) -> usize {
        (days * self.times_per_day()) as usize
    }

    /// 将 24h 借贷利率转换为当前周期利率
    pub fn convert_daily_rate(&self, daily_rate: f64) -> f64 {
        daily_rate / self.times_per_day() as f64
    }

    /// 返回周期字符串表示
    pub fn as_str(&self) -> &'static str {
        match self {
            FundingRatePeriod::Hours1 => "1h",
            FundingRatePeriod::Hours2 => "2h",
            FundingRatePeriod::Hours4 => "4h",
            FundingRatePeriod::Hours6 => "6h",
            FundingRatePeriod::Hours8 => "8h",
        }
    }
}

// ========== RateFetcher Trait ==========

/// 资金费率拉取器 trait
pub trait RateFetcherTrait {
    /// 获取指定 symbol 的资金费率周期
    fn get_period(&self, symbol: &str, venue: TradingVenue) -> FundingRatePeriod;
    /// 获取预测资金费率（返回周期和值）
    fn get_predicted_funding_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)>;
    /// 获取预测借贷利率（返回周期和值）
    fn get_predict_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)>;
    /// 获取当前借贷利率（返回周期和值）
    fn get_current_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)>;
}

// ========== 枚举类型定义 ==========

/// 比较方向枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompareOp {
    /// 大于
    GreaterThan,
    /// 小于
    LessThan,
}

impl CompareOp {
    /// 判断给定值是否满足比较条件
    pub fn check(&self, value: f64, threshold: f64) -> bool {
        match self {
            CompareOp::GreaterThan => value > threshold,
            CompareOp::LessThan => value < threshold,
        }
    }
}

/// 套利方向枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArbDirection {
    /// 正套
    Forward,
    /// 反套
    Backward,
}

/// 操作类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// 开仓
    Open,
    /// 撤单
    Cancel,
    /// 平仓
    Close,
    /// 极端资金费率平仓
    ExtremeClose,
}

/// 交易所对 key: (venue1, venue2)
pub type VenuePair = (TradingVenue, TradingVenue);

/// 交易对 key: (symbol1, symbol2)
pub type SymbolPair = (String, String);

/// 完整的阈值 key: (venue1, symbol1, venue2, symbol2)
pub type ThresholdKey = (TradingVenue, String, TradingVenue, String);

/// 因子模式（MM/MT 模式通用定义）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FactorMode {
    /// Maker-Maker 模式
    #[default]
    MM,
    /// Maker-Taker 模式（对冲是 taker 方）
    MT,
}

/// 根据命令行 exchange 映射现货/期货交易场所
pub fn venue_pair_for_exchange(exchange: Exchange) -> (TradingVenue, TradingVenue) {
    match exchange {
        Exchange::Binance => (TradingVenue::BinanceMargin, TradingVenue::BinanceFutures),
        Exchange::Okex => (TradingVenue::OkexMargin, TradingVenue::OkexFutures),
        Exchange::Bybit => (TradingVenue::BybitMargin, TradingVenue::BybitFutures),
        Exchange::Bitget => (TradingVenue::BitgetMargin, TradingVenue::BitgetFutures),
        Exchange::Gate => (TradingVenue::GateMargin, TradingVenue::GateFutures),
        Exchange::Hyperliquid => (
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ),
        Exchange::Aster => (TradingVenue::AsterMargin, TradingVenue::AsterFutures),
    }
}

// ========== 数据结构定义 ==========

const MAX_SERIES_LEN: usize = 60;

pub use quote_plan::Quote;

/// 计算开仓腿与对冲腿的中间价价差率
/// spread_rate = (mid_open - mid_hedge) / mid_open
pub fn compute_spread_rate(open_quote: &Quote, hedge_quote: &Quote) -> f64 {
    let open_mid = (open_quote.bid + open_quote.ask) / 2.0;
    let hedge_mid = (hedge_quote.bid + hedge_quote.ask) / 2.0;
    if open_mid > 0.0 {
        (open_mid - hedge_mid) / open_mid
    } else {
        0.0
    }
}

pub fn build_decision_from_key_base(
    now_us: i64,
    return_qtl: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
) -> String {
    use std::fmt::Write;
    // now_us (~19 chars) + 5 labeled `{:.8}` fields ≈ 130 bytes; one buffer,
    // no per-field intermediate `String`, no trailing `format!`.
    let mut out = String::with_capacity(160);
    // `write!` into a `String` (fmt::Write) is infallible and byte-identical to
    // the previous `format!` layout.
    let _ = write!(out, "{now_us}");
    let push_field = |out: &mut String, label: &str, value: Option<f64>| {
        out.push(':');
        out.push_str(label);
        out.push('=');
        match value.filter(|v| v.is_finite()) {
            Some(v) => {
                let _ = write!(out, "{v:.8}");
            }
            None => out.push('0'),
        }
    };
    push_field(&mut out, "ret_qtl", return_qtl);
    push_field(&mut out, "ret_thr", return_threshold);
    push_field(&mut out, "vol", volatility);
    push_field(&mut out, "env_score", env_score);
    push_field(&mut out, "env_thr", env_threshold);
    out
}

pub fn format_from_key_optional_value(value: Option<f64>, precision: usize) -> String {
    value
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.precision$}"))
        .unwrap_or_else(|| "NA".to_string())
}

pub fn append_key_value_fields(base: String, fields: &[(&str, String)]) -> String {
    let mut out = base;
    for (key, value) in fields {
        out.push(':');
        out.push_str(key);
        out.push('=');
        out.push_str(value);
    }
    out
}

pub fn build_open_from_key_base(
    now_us: i64,
    return_qtl: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    vol_band_scale: Option<[f64; 2]>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    open_bid: f64,
    open_ask: f64,
    hedge_bid: f64,
    hedge_ask: f64,
) -> String {
    let vol_band_scale_text = vol_band_scale
        .map(|[lo, hi]| format!("{lo:.4},{hi:.4}"))
        .unwrap_or_else(|| "-".to_string());
    // 记录开仓/对冲两腿的原始四档盘口价（最短可往返格式，避免低价币截断）；
    // askbid_sr / bidask_sr / mid spread 均可由这四个价还原，故不再单独记录。
    append_key_value_fields(
        build_decision_from_key_base(
            now_us,
            return_qtl,
            return_threshold,
            volatility,
            env_score,
            env_threshold,
        ),
        &[
            ("vol_band_scale", vol_band_scale_text),
            ("open_bid", format!("{open_bid}")),
            ("open_ask", format!("{open_ask}")),
            ("hedge_bid", format!("{hedge_bid}")),
            ("hedge_ask", format!("{hedge_ask}")),
        ],
    )
}

pub fn append_dump_suffix(base: String) -> String {
    append_suffix_token(base, "dump")
}

pub fn append_suffix_token(base: String, suffix: &str) -> String {
    let mut out = base;
    out.push(':');
    out.push_str(suffix);
    out
}

pub fn append_tlen_suffix(base: String, tlen: f64, threshold: f64) -> String {
    append_key_value_fields(
        base,
        &[
            ("tlen", format!("{tlen:.8}")),
            ("tlen_thr", format!("{threshold:.8}")),
        ],
    )
}

/// Funding Rate 数据（默认维护60条 + rolling sum/mean）
#[derive(Debug, Clone)]
pub struct FundingRateData {
    series: VecDeque<f64>,
    sum: f64,
    mean: Option<f64>,
    max_series_len: usize,
}

impl Default for FundingRateData {
    fn default() -> Self {
        Self::new()
    }
}

impl FundingRateData {
    pub fn new() -> Self {
        Self::with_max_series_len(MAX_SERIES_LEN)
    }

    pub fn with_max_series_len(max_series_len: usize) -> Self {
        let max_series_len = max_series_len.max(1);
        Self {
            series: VecDeque::with_capacity(max_series_len),
            sum: 0.0,
            mean: None,
            max_series_len,
        }
    }

    /// 更新 Funding Rate（立刻重算均值）
    pub fn push(&mut self, funding_rate: f64) {
        // 如果队列满了，移除最旧的值
        if self.series.len() >= self.max_series_len {
            if let Some(oldest) = self.series.pop_front() {
                self.sum -= oldest;
            }
        }

        // 添加新值
        self.series.push_back(funding_rate);
        self.sum += funding_rate;

        // 立刻重算均值
        let count = self.series.len() as f64;
        self.mean = Some(self.sum / count);
    }

    /// 获取均值（O(1) 查询）
    pub fn get_mean(&self) -> Option<f64> {
        self.mean
    }

    /// 获取最新值
    pub fn get_latest(&self) -> Option<f64> {
        self.series.back().copied()
    }
}

// ========== 辅助函数 ==========

/// 浮点数近似相等比较
pub fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-12
}

/// 浮点数数组近似相等比较
pub fn approx_equal_slice(a: &[f64], b: &[f64]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| approx_equal(*x, *y))
}

/// 解析数字列表（支持 JSON 数组、逗号分隔、单个数字）
///
/// # Examples
/// ```
/// use trade_signal::common::parse_numeric_list;
///
/// // JSON 数组
/// assert_eq!(parse_numeric_list("[1.0, 2.0, 3.0]").unwrap(), vec![1.0, 2.0, 3.0]);
///
/// // 逗号分隔
/// assert_eq!(parse_numeric_list("1.0, 2.0, 3.0").unwrap(), vec![1.0, 2.0, 3.0]);
///
/// // 单个数字
/// assert_eq!(parse_numeric_list("1.0").unwrap(), vec![1.0]);
/// ```
pub fn parse_numeric_list(raw: &str) -> Result<Vec<f64>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<f64>>(trimmed)
            .map_err(|err| format!("JSON array parse error: {err}"))
    } else if trimmed.contains(',') {
        let mut out = Vec::new();
        for part in trimmed.split(',') {
            let piece = part.trim();
            if piece.is_empty() {
                continue;
            }
            match piece.parse::<f64>() {
                Ok(v) => out.push(v),
                Err(err) => {
                    return Err(format!("invalid float '{}': {}", piece, err));
                }
            }
        }
        Ok(out)
    } else {
        trimmed
            .parse::<f64>()
            .map(|v| vec![v])
            .map_err(|err| format!("invalid float: {}", err))
    }
}

// ========== 测试 ==========

#[cfg(test)]
mod tests {
    use super::*;
    use order_common::TradingVenue;
    use signal_common::venue_min_qty_table::VenueMinQtyTable;

    #[test]
    fn build_decision_from_key_base_matches_legacy_format() {
        fn legacy(
            now_us: i64,
            return_qtl: Option<f64>,
            return_threshold: Option<f64>,
            volatility: Option<f64>,
            env_score: Option<f64>,
            env_threshold: Option<f64>,
        ) -> String {
            let f = |v: Option<f64>| {
                v.filter(|v| v.is_finite())
                    .map(|v| format!("{v:.8}"))
                    .unwrap_or_else(|| "0".to_string())
            };
            format!(
                "{now_us}:ret_qtl={}:ret_thr={}:vol={}:env_score={}:env_thr={}",
                f(return_qtl),
                f(return_threshold),
                f(volatility),
                f(env_score),
                f(env_threshold),
            )
        }
        let vals = [
            None,
            Some(0.0),
            Some(-0.0),
            Some(1.0),
            Some(-1.234_567_89),
            Some(0.000_000_01),
            Some(1e12),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ];
        for now in [0i64, 1, 1_783_331_226_376_757, -5] {
            for a in vals {
                for b in [None, Some(0.5), Some(f64::NEG_INFINITY)] {
                    assert_eq!(
                        build_decision_from_key_base(now, a, b, a, b, a),
                        legacy(now, a, b, a, b, a),
                        "mismatch now={now} a={a:?} b={b:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn build_tlen_from_key_bytes_matches_legacy_string_path() {
        let bases = [
            "",
            "1783331226376757:ret_qtl=0.00000000:vol=0.12345678:env_score=0",
            "173:ret_qtl=0.99999999:ret_thr=0:vol=0:env_score=0:env_thr=0:vol_band_scale=1.0000,2.0000:open_bid=100:open_ask=100.5:hedge_bid=99.9:hedge_ask=100.1:spread_fr=0.000123",
        ];
        let values = [
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.000_000_01,
            12_345.678_9,
            -9_999.999_999_99,
            f64::MIN_POSITIVE,
            1e12,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ];
        for base in bases {
            for v in values {
                let legacy = append_tlen_to_from_key(base, v).into_bytes();
                let optimized = build_tlen_from_key_bytes(base, v);
                assert_eq!(
                    optimized,
                    legacy,
                    "byte mismatch base={base:?} value={v:?}: {} vs {}",
                    String::from_utf8_lossy(&optimized),
                    String::from_utf8_lossy(&legacy),
                );
            }
        }
    }

    #[test]
    fn test_approx_equal() {
        assert!(approx_equal(1.0, 1.0));
        assert!(approx_equal(1.0, 1.0 + 1e-13));
        assert!(!approx_equal(1.0, 1.0 + 1e-10));
    }

    #[test]
    fn test_approx_equal_slice() {
        assert!(approx_equal_slice(&[1.0, 2.0], &[1.0, 2.0]));
        assert!(approx_equal_slice(&[1.0, 2.0], &[1.0 + 1e-13, 2.0 - 1e-13]));
        assert!(!approx_equal_slice(&[1.0, 2.0], &[1.0, 2.1]));
        assert!(!approx_equal_slice(&[1.0], &[1.0, 2.0]));
    }

    #[test]
    fn funding_rate_data_respects_custom_window_len() {
        let mut data = FundingRateData::with_max_series_len(3);
        data.push(1.0);
        data.push(2.0);
        data.push(3.0);
        assert_eq!(data.get_mean(), Some(2.0));

        data.push(4.0);
        assert_eq!(data.get_mean(), Some(3.0));
        assert_eq!(data.get_latest(), Some(4.0));
    }

    #[test]
    fn test_parse_numeric_list_json() {
        assert_eq!(
            parse_numeric_list("[1.0, 2.0, 3.0]").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn test_parse_numeric_list_comma() {
        assert_eq!(
            parse_numeric_list("1.0, 2.0, 3.0").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn test_parse_numeric_list_single() {
        assert_eq!(parse_numeric_list("1.0").unwrap(), vec![1.0]);
    }

    #[test]
    fn test_parse_numeric_list_empty() {
        assert_eq!(parse_numeric_list("").unwrap(), Vec::<f64>::new());
    }

    #[test]
    fn test_parse_numeric_list_invalid() {
        assert!(parse_numeric_list("invalid").is_err());
        assert!(parse_numeric_list("[1.0, invalid]").is_err());
    }

    #[test]
    fn normalize_tlen_for_okex_futures_keeps_base_qty() {
        let mut table = VenueMinQtyTable::new(TradingVenue::OkexFutures);
        table.set_contract_multiplier_for_test("ETHUSDT", 0.1);

        let normalized =
            normalize_tlen_for_compare("test", &table, TradingVenue::OkexFutures, "ETHUSDT", 2.5);

        assert!((normalized - 2.5).abs() < 1e-12);
    }

    #[test]
    fn normalize_tlen_for_spot_keeps_base_qty() {
        let table = VenueMinQtyTable::new(TradingVenue::OkexMargin);

        let normalized =
            normalize_tlen_for_compare("test", &table, TradingVenue::OkexMargin, "ETHUSDT", 2.5);

        assert!((normalized - 2.5).abs() < 1e-12);
    }

    #[test]
    fn normalize_tlens_for_compare_keeps_order() {
        let mut table = VenueMinQtyTable::new(TradingVenue::OkexFutures);
        table.set_contract_multiplier_for_test("SOLUSDT", 0.1);

        let normalized = normalize_tlens_for_compare(
            "test",
            &table,
            TradingVenue::OkexFutures,
            "SOLUSDT",
            vec![0.1, 0.25, 0.3],
        );

        assert_eq!(normalized.len(), 3);
        assert!((normalized[0] - 0.1).abs() < 1e-12);
        assert!((normalized[1] - 0.25).abs() < 1e-12);
        assert!((normalized[2] - 0.3).abs() < 1e-12);
    }
}
