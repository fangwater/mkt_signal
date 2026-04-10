use factor_engine::math::{
    finite_opt, rolling_mean_last, rolling_mean_last_with_min_periods,
    rolling_sum_last_from_series, sample_std_last,
};
use factor_engine::view::{F64SeriesView, OptF64SeriesView, SymbolSeries};

use super::app::{
    depth_best_ask, depth_best_bid, depth_level_amount, depth_level_price, DepthDerived,
    FusionFactorPubApp,
};
use super::factor_enum::FusionFactorId;
use super::plan::FactorBinding;
use super::window_primitives::rolling_std_last;

macro_rules! dispatch_plain {
    ($binding:expr, $depth:expr, $series:expr, { $($variant:ident => $func:ident($arg:ident),)* }) => {
        match $binding.factor_id {
            $(Some(FusionFactorId::$variant) => {
                Some(FusionFactorPubApp::wrap_factor_value($arg.and_then($func)))
            })*
            _ => None,
        }
    };
}

pub(super) fn compute_supported_plain_factor(
    binding: &FactorBinding,
    depth: Option<&DepthDerived>,
    series: Option<&SymbolSeries<'_>>,
) -> Option<(f64, bool, &'static str)> {
    if let Some(out) = dispatch_plain!(binding, depth, series, {
        Factor005 => compute_factor_005(series),
        Factor007 => compute_factor_007(series),
        Factor013 => compute_factor_013(depth),
        Factor015 => compute_factor_015(depth),
        Factor034 => compute_factor_034(series),
        Factor039 => compute_factor_039(series),
        Factor044 => compute_factor_044(depth),
        Factor050 => compute_factor_050(series),
        Factor071 => compute_factor_071(depth),
        Factor072 => compute_factor_072(depth),
        Factor078 => compute_factor_078(series),
        Factor081 => compute_factor_081(series),
        Factor082 => compute_factor_082(series),
        Factor083 => compute_factor_083(depth),
        Factor084 => compute_factor_084(depth),
        Factor090 => compute_factor_090(depth),
        Factor092 => compute_factor_092(depth),
        Factor098 => compute_factor_098(depth),
        Factor099 => compute_factor_099(depth),
        Factor100 => compute_factor_100(depth),
        Factor101 => compute_factor_101(depth),
        Factor104 => compute_factor_104(depth),
        Factor105 => compute_factor_105(depth),
        Factor106 => compute_factor_106(depth),
        Factor109 => compute_factor_109(depth),
        Factor112 => compute_factor_112(series),
        Factor117 => compute_factor_117(depth),
        Factor127 => compute_factor_127(series),
        Factor131 => compute_factor_131(series),
        Factor132 => compute_factor_132(series),
        Factor135 => compute_factor_135(series),
        Factor136 => compute_factor_136(depth),
        Factor137 => compute_factor_137(series),
        Factor138 => compute_factor_138(depth),
        Factor140 => compute_factor_140(depth),
        Factor141 => compute_factor_141(series),
        Factor142 => compute_factor_142(series),
        Factor143 => compute_factor_143(depth),
        Factor145 => compute_factor_145(depth),
        Factor146 => compute_factor_146(depth),
        Factor147 => compute_factor_147(depth),
        Factor148 => compute_factor_148(depth),
        Factor149 => compute_factor_149(depth),
        Factor150 => compute_factor_150(depth),
        Factor153 => compute_factor_153(depth),
        Factor154 => compute_factor_154(depth),
        Factor155 => compute_factor_155(depth),
        Factor157 => compute_factor_157(series),
        Factor158 => compute_factor_158(depth),
        Factor161 => compute_factor_161(series),
        Factor162 => compute_factor_162(series),
        Factor163 => compute_factor_163(series),
        Factor167 => compute_factor_167(series),
        Factor169 => compute_factor_169(series),
        Factor171 => compute_factor_171(series),
        Factor172 => compute_factor_172(series),
        Factor173 => compute_factor_173(series),
        Factor174 => compute_factor_174(depth),
        Factor177 => compute_factor_177(series),
    }) {
        return Some(out);
    }
    None
}

fn current(values: &(impl F64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        None
    } else {
        finite_opt(Some(values.value_at(values.len() - 1)))
    }
}

fn diff_last(values: &(impl F64SeriesView + ?Sized), periods: usize) -> Option<f64> {
    if values.len() <= periods {
        None
    } else {
        finite_opt(Some(
            values.value_at(values.len() - 1) - values.value_at(values.len() - 1 - periods),
        ))
    }
}

fn current_opt(values: &(impl OptF64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        None
    } else {
        values
            .value_at(values.len() - 1)
            .and_then(|v| finite_opt(Some(v)))
    }
}

fn ratio(num: f64, den: f64) -> Option<f64> {
    if den.abs() <= 1e-12 {
        None
    } else {
        finite_opt(Some(num / den))
    }
}

fn pop_std(values: &[f64]) -> Option<f64> {
    if values.is_empty() || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n;
    finite_opt(Some(var.sqrt()))
}

fn cross_sectional_skew(values: &[f64], bias: bool) -> Option<f64> {
    if values.len() < 3 || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let mut m2 = 0.0;
    let mut m3 = 0.0;
    for value in values {
        let d = *value - mean;
        let d2 = d * d;
        m2 += d2;
        m3 += d2 * d;
    }
    let m2 = m2 / n;
    if m2.abs() <= 1e-12 {
        return Some(0.0);
    }
    let m3 = m3 / n;
    let mut out = m3 / m2.powf(1.5);
    if !bias {
        out *= (n * (n - 1.0)).sqrt() / (n - 2.0);
    }
    finite_opt(Some(out)).or(Some(0.0))
}

fn cross_sectional_kurtosis(values: &[f64], fisher: bool, bias: bool) -> Option<f64> {
    if values.len() < 4 || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let mut m2 = 0.0;
    let mut m4 = 0.0;
    for value in values {
        let d = *value - mean;
        let d2 = d * d;
        m2 += d2;
        m4 += d2 * d2;
    }
    let m2 = m2 / n;
    if m2.abs() <= 1e-12 {
        return Some(0.0);
    }
    let m4 = m4 / n;
    let g2 = m4 / (m2 * m2) - 3.0;
    let mut out = if bias {
        g2
    } else {
        ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0)
    };
    if !fisher {
        out += 3.0;
    }
    finite_opt(Some(out)).or(Some(0.0))
}

fn bid0_price_series(series: &SymbolSeries<'_>) -> Vec<f64> {
    (0..series.mid_price.len())
        .map(|i| series.mid_price[i] - series.spread[i] / 2.0)
        .collect()
}

fn ask0_price_series(series: &SymbolSeries<'_>) -> Vec<f64> {
    (0..series.mid_price.len())
        .map(|i| series.mid_price[i] + series.spread[i] / 2.0)
        .collect()
}

fn compute_factor_005(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.mid_price, 1)
}

fn compute_factor_007(series: &SymbolSeries<'_>) -> Option<f64> {
    match (current(&series.total_bid20), current(&series.total_ask20)) {
        (Some(b), Some(a)) => finite_opt(Some(b - a)),
        _ => None,
    }
}

fn compute_factor_013(depth: &DepthDerived) -> Option<f64> {
    ratio(depth.sum_bid_amount(5), depth.sum_bid_amount(20))
}

fn compute_factor_015(depth: &DepthDerived) -> Option<f64> {
    let total_bid_price = (0..20)
        .map(|i| depth_level_price(&depth.bids, i))
        .filter(|v| v.is_finite())
        .sum::<f64>();
    finite_opt(Some(
        (0..20)
            .map(|i| depth_level_amount(&depth.bids, i) * depth_level_price(&depth.bids, i))
            .filter(|v| v.is_finite())
            .sum::<f64>()
            / (total_bid_price + 1e-6),
    ))
}

fn compute_factor_034(series: &SymbolSeries<'_>) -> Option<f64> {
    let last = current(&series.top10_ask_volume)?;
    let ma = rolling_mean_last(&series.top10_ask_volume, 30)
        .ok()
        .flatten()?;
    ratio(last, ma)
}

fn compute_factor_039(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_factor_007(series)
}

fn compute_factor_044(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..15)
            .map(|i| depth_level_price(&depth.asks, i) - depth_level_price(&depth.bids, i))
            .filter(|v| v.is_finite())
            .sum::<f64>()
            / 15.0,
    ))
}

fn compute_factor_050(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_std_last(&series.avg_ask_price5, 30).ok().flatten()
}

fn compute_factor_071(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..5)
            .map(|i| depth_level_amount(&depth.bids, i).sqrt())
            .filter(|v| v.is_finite())
            .sum::<f64>()
            / 5.0,
    ))
}

fn compute_factor_072(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..5)
            .map(|i| depth_level_amount(&depth.asks, i).sqrt())
            .filter(|v| v.is_finite())
            .sum::<f64>()
            / 5.0,
    ))
}

fn compute_factor_078(series: &SymbolSeries<'_>) -> Option<f64> {
    let bid = current(&series.total_bid20)?;
    let total = bid + current(&series.total_ask20)?;
    ratio(bid, total)
}

fn compute_factor_081(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.combined_level9_volume, 3)
        .ok()
        .flatten()
}

fn compute_factor_082(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_sum_last_from_series(&series.combined_level9_volume, 3, 3)
        .ok()
        .flatten()
}

fn compute_factor_083(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..10)
        .map(|i| depth_level_amount(&depth.bids, i))
        .collect();
    let min = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .min_by(|a, b| a.total_cmp(b))?;
    let max = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .max_by(|a, b| a.total_cmp(b))?;
    finite_opt(Some(max - min))
}

fn compute_factor_084(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..10)
        .map(|i| depth_level_amount(&depth.asks, i))
        .collect();
    let min = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .min_by(|a, b| a.total_cmp(b))?;
    let max = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .max_by(|a, b| a.total_cmp(b))?;
    finite_opt(Some(max - min))
}

fn compute_factor_090(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..10)
            .map(|i| depth_level_price(&depth.asks, i) - depth_level_price(&depth.bids, i))
            .filter(|v| v.is_finite())
            .sum::<f64>()
            / 10.0,
    ))
}

fn compute_factor_092(depth: &DepthDerived) -> Option<f64> {
    let bid: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.bids, i)).collect();
    let ask: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.asks, i)).collect();
    ratio(pop_std(&bid)?, pop_std(&ask)?)
}

fn compute_factor_098(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..15).map(|i| depth_level_price(&depth.bids, i)).collect();
    let min = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .min_by(|a, b| a.total_cmp(b))?;
    let max = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .max_by(|a, b| a.total_cmp(b))?;
    finite_opt(Some(max - min))
}

fn compute_factor_099(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..15).map(|i| depth_level_price(&depth.asks, i)).collect();
    let min = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .min_by(|a, b| a.total_cmp(b))?;
    let max = vals
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .max_by(|a, b| a.total_cmp(b))?;
    finite_opt(Some(max - min))
}

fn compute_factor_100(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.bids, i)).collect();
    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
    finite_opt(Some(vals.iter().map(|v| (v - mean) * (v - mean)).sum()))
}

fn compute_factor_101(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.asks, i)).collect();
    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
    finite_opt(Some(vals.iter().map(|v| (v - mean) * (v - mean)).sum()))
}

fn compute_factor_104(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..5)
            .map(|i| depth_level_price(&depth.asks, i) - depth_level_price(&depth.bids, i))
            .sum::<f64>()
            / 5.0,
    ))
}

fn compute_factor_105(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..20)
            .map(|i| depth_level_price(&depth.asks, i) - depth_level_price(&depth.bids, i))
            .sum::<f64>()
            / 20.0,
    ))
}

fn compute_factor_106(depth: &DepthDerived) -> Option<f64> {
    let max_bid = (0..20)
        .map(|i| depth_level_price(&depth.bids, i))
        .max_by(|a, b| a.total_cmp(b))?;
    let min_ask = (0..20)
        .map(|i| depth_level_price(&depth.asks, i))
        .min_by(|a, b| a.total_cmp(b))?;
    finite_opt(Some(max_bid - min_ask))
}

fn compute_factor_109(depth: &DepthDerived) -> Option<f64> {
    let (bid0p, _) = depth_best_bid(depth);
    let (ask0p, _) = depth_best_ask(depth);
    let mid = (bid0p + ask0p) / 2.0;
    finite_opt(Some(
        (0..5)
            .map(|i| depth_level_price(&depth.bids, i) - mid)
            .sum::<f64>()
            / 5.0,
    ))
}

fn compute_factor_112(series: &SymbolSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.ask9p, 3, 3)?;
    finite_opt(Some(std * std))
}

fn compute_factor_117(depth: &DepthDerived) -> Option<f64> {
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..10 {
        let p = depth_level_price(&depth.asks, i);
        let v = depth_level_amount(&depth.asks, i);
        if !p.is_finite() || !v.is_finite() || v.abs() <= 1e-12 {
            return None;
        }
        num += p / v;
        den += 1.0 / v;
    }
    ratio(num, den)
}

fn compute_factor_127(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.factor_127_bid_price_kurt, 300)
        .ok()
        .flatten()
}

fn compute_factor_131(series: &SymbolSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.factor_131_ask_price_kurt, 200)
        .ok()
        .flatten()
}

fn compute_factor_132(series: &SymbolSeries<'_>) -> Option<f64> {
    ratio(current(&series.total_ask20)?, current(&series.total_bid20)?)
}

fn compute_factor_135(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_factor_005(series)
}

fn compute_factor_136(depth: &DepthDerived) -> Option<f64> {
    let ask_strength = (0..5)
        .map(|i| depth_level_price(&depth.asks, i) * depth_level_amount(&depth.asks, i))
        .sum::<f64>();
    let bid_strength = (0..5)
        .map(|i| depth_level_price(&depth.bids, i) * depth_level_amount(&depth.bids, i))
        .sum::<f64>();
    ratio(bid_strength - ask_strength, bid_strength + ask_strength)
}

fn compute_factor_137(series: &SymbolSeries<'_>) -> Option<f64> {
    compute_factor_007(series)
}

fn compute_factor_138(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..10)
        .map(|i| depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i))
        .collect();
    pop_std(&vals)
}

fn compute_factor_140(depth: &DepthDerived) -> Option<f64> {
    let ratios: Vec<Option<f64>> = (0..15)
        .map(|i| {
            let b = depth_level_amount(&depth.bids, i);
            let a = depth_level_amount(&depth.asks, i);
            ratio(b, a + b)
        })
        .collect();
    let mut sum = 0.0;
    let mut cnt = 0usize;
    for i in 1..ratios.len() {
        let prev = ratios[i - 1]?;
        let curr = ratios[i]?;
        if prev.abs() <= 1e-12 {
            continue;
        }
        let pct = (curr - prev) / prev;
        if pct.is_finite() {
            sum += pct;
            cnt += 1;
        }
    }
    if cnt == 0 {
        None
    } else {
        finite_opt(Some(sum / cnt as f64))
    }
}

fn compute_factor_141(series: &SymbolSeries<'_>) -> Option<f64> {
    current(&series.total_bid20)
}

fn compute_factor_142(series: &SymbolSeries<'_>) -> Option<f64> {
    current(&series.total_ask20)
}

fn compute_factor_143(depth: &DepthDerived) -> Option<f64> {
    compute_factor_138(depth)
}

fn compute_factor_145(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..5)
            .map(|i| depth_level_price(&depth.bids, i) * depth_level_amount(&depth.bids, i))
            .sum(),
    ))
}

fn compute_factor_146(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..5)
            .map(|i| depth_level_price(&depth.asks, i) * depth_level_amount(&depth.asks, i))
            .sum(),
    ))
}

fn compute_factor_147(depth: &DepthDerived) -> Option<f64> {
    (0..10)
        .map(|i| depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i))
        .max_by(|a, b| a.total_cmp(b))
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_factor_148(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..5)
        .map(|i| depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i))
        .collect();
    pop_std(&vals)
}

fn compute_factor_149(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (1..20)
        .map(|i| depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i))
        .collect();
    finite_opt(Some(vals.iter().sum::<f64>() / vals.len() as f64))
}

fn compute_factor_150(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..15)
        .map(|i| depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i))
        .collect();
    cross_sectional_skew(&vals, false)
}

fn compute_factor_153(depth: &DepthDerived) -> Option<f64> {
    let bid: Vec<f64> = (0..20)
        .map(|i| depth_level_amount(&depth.bids, i))
        .collect();
    let ask: Vec<f64> = (0..20)
        .map(|i| depth_level_amount(&depth.asks, i))
        .collect();
    finite_opt(Some(pop_std(&bid)? - pop_std(&ask)?))
}

fn compute_factor_154(depth: &DepthDerived) -> Option<f64> {
    let vals: Vec<f64> = (0..5)
        .map(|i| depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i))
        .collect();
    cross_sectional_kurtosis(&vals, true, false)
}

fn compute_factor_155(depth: &DepthDerived) -> Option<f64> {
    let ratios: Vec<f64> = (0..15)
        .filter_map(|i| {
            let b = depth_level_amount(&depth.bids, i);
            let a = depth_level_amount(&depth.asks, i);
            ratio(b, a + b)
        })
        .collect();
    pop_std(&ratios)
}

fn compute_factor_157(series: &SymbolSeries<'_>) -> Option<f64> {
    current_opt(&series.factor_157_bid_ask_diff_std)
}

fn compute_factor_158(depth: &DepthDerived) -> Option<f64> {
    finite_opt(Some(
        (0..20)
            .map(|i| {
                let b = (depth_level_amount(&depth.bids, i) + 1.0).ln();
                let a = (depth_level_amount(&depth.asks, i) + 1.0).ln();
                b - a
            })
            .sum::<f64>()
            / 20.0,
    ))
}

fn compute_factor_161(series: &SymbolSeries<'_>) -> Option<f64> {
    let bid0 = bid0_price_series(series);
    diff_last(bid0.as_slice(), 1)
}

fn compute_factor_162(series: &SymbolSeries<'_>) -> Option<f64> {
    let ask0 = ask0_price_series(series);
    diff_last(ask0.as_slice(), 1)
}

fn compute_factor_163(series: &SymbolSeries<'_>) -> Option<f64> {
    current(&series.relative_spread)
}

fn compute_factor_167(series: &SymbolSeries<'_>) -> Option<f64> {
    finite_opt(Some(
        (current(&series.bid0v)? + current(&series.ask0v)?) / 2.0,
    ))
}

fn compute_factor_169(series: &SymbolSeries<'_>) -> Option<f64> {
    let totals: Vec<f64> = (0..series.bid0v.len())
        .map(|i| series.bid0v[i] + series.ask0v[i])
        .collect();
    if totals.len() < 2 {
        return None;
    }
    let curr = totals[totals.len() - 1];
    let prev = totals[totals.len() - 2];
    if curr <= 0.0 || prev <= 0.0 {
        return None;
    }
    finite_opt(Some(curr.ln() - prev.ln()))
}

fn compute_factor_171(series: &SymbolSeries<'_>) -> Option<f64> {
    let mid = current(&series.mid_price)?;
    let ma = rolling_mean_last_with_min_periods(&series.mid_price, 10, 1)
        .ok()
        .flatten()?;
    finite_opt(Some(mid - ma))
}

fn compute_factor_172(series: &SymbolSeries<'_>) -> Option<f64> {
    let bid0 = bid0_price_series(series);
    sample_std_last(bid0.as_slice(), 10, 2)
}

fn compute_factor_173(series: &SymbolSeries<'_>) -> Option<f64> {
    current(&series.spread)
}

fn compute_factor_174(depth: &DepthDerived) -> Option<f64> {
    let (bid0p, bid0v) = depth_best_bid(depth);
    let (ask0p, ask0v) = depth_best_ask(depth);
    finite_opt(Some(ask0p * ask0v - bid0p * bid0v))
}

fn compute_factor_177(series: &SymbolSeries<'_>) -> Option<f64> {
    diff_last(&series.mid_price, 3)
}
