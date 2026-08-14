use super::intermediates::{
    build_ad_line_rolling360, build_ht_dc_level, build_ht_dc_phase, build_obv_cumulative,
    build_true_range_series, compute_tp_vpi_006,
};
use super::math::{
    cmo_last_from_series, corr_last_with_min_periods, finite_opt, last_opt,
    linear_regression_intercept, linear_regression_predict_last, linear_regression_slope,
    pct_change_last, rolling_corr_last, rolling_max_last_simple as rolling_max_last,
    rolling_mean_at_from_series, rolling_mean_last, rolling_mean_last_opt_from_series,
    rolling_mean_last_with_min_periods, rolling_mean_series, rolling_mean_series_opt,
    rolling_min_last_simple as rolling_min_last, rolling_position_last, rolling_rank_last,
    rolling_sum_last_opt_from_series, rolling_sum_last_with_min_periods,
    rolling_weighted_mean_last, rsi_last_from_series, sample_cov, sample_std_last,
    tail_quantile_last,
};
use super::view::{CnSeries, F64SeriesView};

pub const SUPPORTED_BASELINES: &[&str] = &[
    "baseline_001",
    "baseline_002",
    "baseline_003",
    "baseline_004",
    "baseline_005",
    "baseline_006",
    "baseline_007",
    "baseline_008",
    "baseline_009",
    "baseline_010",
    "baseline_011",
    "baseline_012",
    "baseline_013",
    "baseline_014",
    "baseline_015",
    "baseline_016",
    "baseline_017",
    "baseline_018",
    "baseline_019",
    "baseline_020",
    "baseline_021",
    "baseline_022",
    "baseline_023",
    "baseline_024",
    "baseline_025",
    "baseline_026",
    "baseline_027",
    "baseline_028",
    "baseline_029",
    "baseline_030",
    "baseline_031",
    "baseline_032",
    "baseline_033",
    "baseline_034",
    "baseline_035",
    "baseline_036",
    "baseline_037",
    "baseline_038",
    "baseline_039",
    "baseline_040",
    "baseline_041",
    "baseline_042",
    "baseline_043",
    "baseline_044",
    "baseline_045",
    "baseline_046",
    "baseline_047",
    "baseline_048",
    "baseline_049",
    "baseline_050",
    "baseline_051",
    "baseline_052",
    "baseline_053",
    "baseline_054",
    "baseline_055",
    "baseline_056",
    "baseline_057",
    "baseline_058",
    "baseline_059",
    "baseline_060",
    "baseline_061",
    "baseline_062",
    "baseline_063",
    "baseline_064",
    "baseline_065",
    "baseline_066",
    "baseline_067",
    "baseline_068",
    "baseline_069",
    "baseline_070",
    "baseline_071",
    "baseline_072",
    "baseline_073",
    "baseline_074",
    "baseline_075",
    "baseline_076",
    "baseline_077",
    "baseline_078",
    "baseline_079",
    "baseline_080",
    "baseline_081",
    "baseline_082",
    "baseline_083",
    "baseline_084",
    "baseline_085",
    "baseline_086",
    "baseline_087",
    "baseline_088",
    "baseline_089",
    "baseline_090",
    "baseline_091",
    "baseline_092",
    "baseline_093",
    "baseline_094",
    "baseline_095",
    "baseline_096",
    "baseline_097",
    "baseline_098",
    "baseline_099",
    "baseline_100",
    "baseline_101",
    "baseline_102",
    "baseline_103",
    "baseline_104",
    "baseline_105",
    "baseline_106",
    "baseline_107",
    "baseline_108",
    "baseline_109",
    "baseline_110",
    "baseline_111",
    "baseline_112",
    "baseline_113",
    "baseline_114",
    "baseline_115",
    "baseline_116",
    "baseline_117",
    "baseline_118",
    "baseline_119",
    "baseline_120",
    "baseline_121",
    "baseline_122",
    "baseline_123",
    "baseline_124",
    "baseline_125",
    "baseline_126",
    "baseline_127",
    "baseline_128",
    "baseline_129",
    "baseline_130",
    "baseline_131",
    "baseline_132",
    "baseline_133",
    "baseline_134",
    "baseline_135",
    "baseline_136",
    "baseline_137",
    "baseline_138",
    "baseline_139",
    "baseline_140",
    "baseline_141",
    "baseline_142",
    "baseline_143",
    "baseline_144",
    "baseline_145",
    "baseline_146",
    "baseline_147",
    "baseline_148",
    "baseline_149",
    "baseline_150",
    "baseline_151",
    "baseline_152",
    "baseline_153",
    "baseline_154",
    "baseline_155",
    "baseline_156",
    "baseline_157",
    "baseline_158",
    "baseline_159",
    "baseline_160",
    "baseline_161",
    "baseline_162",
    "baseline_163",
    "baseline_164",
    "baseline_165",
    "baseline_166",
    "baseline_167",
    "baseline_168",
    "baseline_169",
    "baseline_170",
    "baseline_171",
    "baseline_172",
    "baseline_173",
    "baseline_174",
    "baseline_175",
    "baseline_176",
    "baseline_177",
    "baseline_178",
    "baseline_179",
    "baseline_180",
    "baseline_181",
    "baseline_182",
    "baseline_183",
    "baseline_184",
    "baseline_185",
    "baseline_186",
    "baseline_187",
    "baseline_188",
    "baseline_189",
    "baseline_190",
    "baseline_191",
    "baseline_192",
    "baseline_193",
    "baseline_194",
    "baseline_195",
    "baseline_196",
    "baseline_197",
    "baseline_198",
    "baseline_199",
    "baseline_200",
];

pub fn is_supported_baseline(name: &str) -> bool {
    SUPPORTED_BASELINES.contains(&name)
}

pub fn compute_baseline(name: &str, series: &CnSeries<'_>) -> Option<f64> {
    match name {
        "baseline_001" => compute_baseline_001(series),
        "baseline_002" => compute_baseline_002(series),
        "baseline_003" => compute_baseline_003(series),
        "baseline_004" => compute_baseline_004(series),
        "baseline_005" => compute_baseline_005(series),
        "baseline_006" => compute_baseline_006(series),
        "baseline_007" => compute_baseline_007(series),
        "baseline_008" => compute_baseline_008(series),
        "baseline_009" => compute_baseline_009(series),
        "baseline_010" => compute_baseline_010(series),
        "baseline_011" => compute_baseline_011(series),
        "baseline_012" => compute_baseline_012(series),
        "baseline_013" => compute_baseline_013(series),
        "baseline_014" => compute_baseline_014(series),
        "baseline_015" => compute_baseline_015(series),
        "baseline_016" => compute_baseline_016(series),
        "baseline_017" => compute_baseline_017(series),
        "baseline_018" => compute_baseline_018(series),
        "baseline_019" => compute_baseline_019(series),
        "baseline_020" => compute_baseline_020(series),
        "baseline_021" => compute_baseline_021(series),
        "baseline_022" => compute_baseline_022(series),
        "baseline_023" => compute_baseline_023(series),
        "baseline_024" => compute_baseline_024(series),
        "baseline_025" => compute_baseline_025(series),
        "baseline_026" => compute_baseline_026(series),
        "baseline_027" => compute_baseline_027(series),
        "baseline_028" => compute_baseline_028(series),
        "baseline_029" => compute_baseline_029(series),
        "baseline_030" => compute_baseline_030(series),
        "baseline_031" => compute_baseline_031(series),
        "baseline_032" => compute_baseline_032(series),
        "baseline_033" => compute_baseline_033(series),
        "baseline_034" => compute_baseline_034(series),
        "baseline_035" => compute_baseline_035(series),
        "baseline_036" => compute_baseline_036(series),
        "baseline_037" => compute_baseline_037(series),
        "baseline_038" => compute_baseline_038(series),
        "baseline_039" => compute_baseline_039(series),
        "baseline_040" => compute_baseline_040(series),
        "baseline_041" => compute_baseline_041(series),
        "baseline_042" => compute_baseline_042(series),
        "baseline_043" => compute_baseline_043(series),
        "baseline_044" => compute_baseline_044(series),
        "baseline_045" => compute_baseline_045(series),
        "baseline_046" => compute_baseline_046(series),
        "baseline_047" => compute_baseline_047(series),
        "baseline_048" => compute_baseline_048(series),
        "baseline_049" => compute_baseline_049(series),
        "baseline_050" => compute_baseline_050(series),
        "baseline_051" => compute_baseline_051(series),
        "baseline_052" => compute_baseline_052(series),
        "baseline_053" => compute_baseline_053(series),
        "baseline_054" => compute_baseline_054(series),
        "baseline_055" => compute_baseline_055(series),
        "baseline_056" => compute_baseline_056(series),
        "baseline_057" => compute_baseline_057(series),
        "baseline_058" => compute_baseline_058(series),
        "baseline_059" => compute_baseline_059(series),
        "baseline_060" => compute_baseline_060(series),
        "baseline_061" => compute_baseline_061(series),
        "baseline_062" => compute_baseline_062(series),
        "baseline_063" => compute_baseline_063(series),
        "baseline_064" => compute_baseline_064(series),
        "baseline_065" => compute_baseline_065(series),
        "baseline_066" => compute_baseline_066(series),
        "baseline_067" => compute_baseline_067(series),
        "baseline_068" => compute_baseline_068(series),
        "baseline_069" => compute_baseline_069(series),
        "baseline_070" => compute_baseline_070(series),
        "baseline_071" => compute_baseline_071(series),
        "baseline_072" => compute_baseline_072(series),
        "baseline_073" => compute_baseline_073(series),
        "baseline_074" => compute_baseline_074(series),
        "baseline_075" => compute_baseline_075(series),
        "baseline_076" => compute_baseline_076(series),
        "baseline_077" => compute_baseline_077(series),
        "baseline_078" => compute_baseline_078(series),
        "baseline_079" => compute_baseline_079(series),
        "baseline_080" => compute_baseline_080(series),
        "baseline_081" => compute_baseline_081(series),
        "baseline_082" => compute_baseline_082(series),
        "baseline_083" => compute_baseline_083(series),
        "baseline_084" => compute_baseline_084(series),
        "baseline_085" => compute_baseline_085(series),
        "baseline_086" => compute_baseline_086(series),
        "baseline_087" => compute_baseline_087(series),
        "baseline_088" => compute_baseline_088(series),
        "baseline_089" => compute_baseline_089(series),
        "baseline_090" => compute_baseline_090(series),
        "baseline_091" => compute_baseline_091(series),
        "baseline_092" => compute_baseline_092(series),
        "baseline_093" => compute_baseline_093(series),
        "baseline_094" => compute_baseline_094(series),
        "baseline_095" => compute_baseline_095(series),
        "baseline_096" => compute_baseline_096(series),
        "baseline_097" => compute_baseline_097(series),
        "baseline_098" => compute_baseline_098(series),
        "baseline_099" => compute_baseline_099(series),
        "baseline_100" => compute_baseline_100(series),
        "baseline_101" => compute_baseline_101(series),
        "baseline_102" => compute_baseline_102(series),
        "baseline_103" => compute_baseline_103(series),
        "baseline_104" => compute_baseline_104(series),
        "baseline_105" => compute_baseline_105(series),
        "baseline_106" => compute_baseline_106(series),
        "baseline_107" => compute_baseline_107(series),
        "baseline_108" => compute_baseline_108(series),
        "baseline_109" => compute_baseline_109(series),
        "baseline_110" => compute_baseline_110(series),
        "baseline_111" => compute_baseline_111(series),
        "baseline_112" => compute_baseline_112(series),
        "baseline_113" => compute_baseline_113(series),
        "baseline_114" => compute_baseline_114(series),
        "baseline_115" => compute_baseline_115(series),
        "baseline_116" => compute_baseline_116(series),
        "baseline_117" => compute_baseline_117(series),
        "baseline_118" => compute_baseline_118(series),
        "baseline_119" => compute_baseline_119(series),
        "baseline_120" => compute_baseline_120(series),
        "baseline_121" => compute_baseline_121(series),
        "baseline_122" => compute_baseline_122(series),
        "baseline_123" => compute_baseline_123(series),
        "baseline_124" => compute_baseline_124(series),
        "baseline_125" => compute_baseline_125(series),
        "baseline_126" => compute_baseline_126(series),
        "baseline_127" => compute_baseline_127(series),
        "baseline_128" => compute_baseline_128(series),
        "baseline_129" => compute_baseline_129(series),
        "baseline_130" => compute_baseline_130(series),
        "baseline_131" => compute_baseline_131(series),
        "baseline_132" => compute_baseline_132(series),
        "baseline_133" => compute_baseline_133(series),
        "baseline_134" => compute_baseline_134(series),
        "baseline_135" => compute_baseline_135(series),
        "baseline_136" => compute_baseline_136(series),
        "baseline_137" => compute_baseline_137(series),
        "baseline_138" => compute_baseline_138(series),
        "baseline_139" => compute_baseline_139(series),
        "baseline_140" => compute_baseline_140(series),
        "baseline_141" => compute_baseline_141(series),
        "baseline_142" => compute_baseline_142(series),
        "baseline_143" => compute_baseline_143(series),
        "baseline_144" => compute_baseline_144(series),
        "baseline_145" => compute_baseline_145(series),
        "baseline_146" => compute_baseline_146(series),
        "baseline_147" => compute_baseline_147(series),
        "baseline_148" => compute_baseline_148(series),
        "baseline_149" => compute_baseline_149(series),
        "baseline_150" => compute_baseline_150(series),
        "baseline_151" => compute_baseline_151(series),
        "baseline_152" => compute_baseline_152(series),
        "baseline_153" => compute_baseline_153(series),
        "baseline_154" => compute_baseline_154(series),
        "baseline_155" => compute_baseline_155(series),
        "baseline_156" => compute_baseline_156(series),
        "baseline_157" => compute_baseline_157(series),
        "baseline_158" => compute_baseline_158(series),
        "baseline_159" => compute_baseline_159(series),
        "baseline_160" => compute_baseline_160(series),
        "baseline_161" => compute_baseline_161(series),
        "baseline_162" => compute_baseline_162(series),
        "baseline_163" => compute_baseline_163(series),
        "baseline_164" => compute_baseline_164(series),
        "baseline_165" => compute_baseline_165(series),
        "baseline_166" => compute_baseline_166(series),
        "baseline_167" => compute_baseline_167(series),
        "baseline_168" => compute_baseline_168(series),
        "baseline_169" => compute_baseline_169(series),
        "baseline_170" => compute_baseline_170(series),
        "baseline_171" => compute_baseline_171(series),
        "baseline_172" => compute_baseline_172(series),
        "baseline_173" => compute_baseline_173(series),
        "baseline_174" => compute_baseline_174(series),
        "baseline_175" => compute_baseline_175(series),
        "baseline_176" => compute_baseline_176(series),
        "baseline_177" => compute_baseline_177(series),
        "baseline_178" => compute_baseline_178(series),
        "baseline_179" => compute_baseline_179(series),
        "baseline_180" => compute_baseline_180(series),
        "baseline_181" => compute_baseline_181(series),
        "baseline_182" => compute_baseline_182(series),
        "baseline_183" => compute_baseline_183(series),
        "baseline_184" => compute_baseline_184(series),
        "baseline_185" => compute_baseline_185(series),
        "baseline_186" => compute_baseline_186(series),
        "baseline_187" => compute_baseline_187(series),
        "baseline_188" => compute_baseline_188(series),
        "baseline_189" => compute_baseline_189(series),
        "baseline_190" => compute_baseline_190(series),
        "baseline_191" => compute_baseline_191(series),
        "baseline_192" => compute_baseline_192(series),
        "baseline_193" => compute_baseline_193(series),
        "baseline_194" => compute_baseline_194(series),
        "baseline_195" => compute_baseline_195(series),
        "baseline_196" => compute_baseline_196(series),
        "baseline_197" => compute_baseline_197(series),
        "baseline_198" => compute_baseline_198(series),
        "baseline_199" => compute_baseline_199(series),
        "baseline_200" => compute_baseline_200(series),
        _ => None,
    }
}

fn first_argmax(values: &[f64]) -> Option<usize> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mut best_idx = 0usize;
    let mut best_val = f64::NEG_INFINITY;
    for (idx, value) in values.iter().enumerate() {
        if value.total_cmp(&best_val).is_gt() {
            best_val = *value;
            best_idx = idx;
        }
    }
    Some(best_idx)
}

fn first_argmin(values: &[f64]) -> Option<usize> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mut best_idx = 0usize;
    let mut best_val = f64::INFINITY;
    for (idx, value) in values.iter().enumerate() {
        if value.total_cmp(&best_val).is_lt() {
            best_val = *value;
            best_idx = idx;
        }
    }
    Some(best_idx)
}

fn current_value(values: &(impl F64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        return None;
    }
    finite_opt(Some(values.value_at(values.len() - 1)))
}

fn safe_ratio(numerator: f64, denominator: f64) -> Option<f64> {
    if !numerator.is_finite() || !denominator.is_finite() || denominator.abs() <= 1e-12 {
        return None;
    }
    finite_opt(Some(numerator / denominator))
}

fn rolling_mean_difference(
    values: &(impl F64SeriesView + ?Sized),
    short_window: usize,
    long_window: usize,
) -> Option<f64> {
    let short = rolling_mean_last(values, short_window).ok().flatten()?;
    let long = rolling_mean_last(values, long_window).ok().flatten()?;
    finite_opt(Some(short - long))
}

fn rolling_high_low_range(series: &CnSeries<'_>, window: usize) -> Option<f64> {
    let high = rolling_max_last(&series.high, window)?;
    let low = rolling_min_last(&series.low, window)?;
    finite_opt(Some(high - low))
}

fn rolling_range_mean(series: &CnSeries<'_>, max_window: usize, min_periods: usize) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if max_window == 0 || min_periods == 0 || n < min_periods {
        return None;
    }
    let start = n.saturating_sub(max_window);
    let mut sum = 0.0;
    for i in start..n {
        let high = series.high[i];
        let low = series.low[i];
        if !high.is_finite() || !low.is_finite() {
            return None;
        }
        sum += high - low;
    }
    finite_opt(Some(sum / (n - start) as f64))
}

fn directional_movement_series(series: &CnSeries<'_>) -> Option<(Vec<f64>, Vec<f64>)> {
    let n = series.high.len().min(series.low.len());
    if n == 0 {
        return None;
    }
    let mut plus_dm = vec![0.0; n];
    let mut minus_dm = vec![0.0; n];
    for i in 1..n {
        let high = series.high[i];
        let previous_high = series.high[i - 1];
        let low = series.low[i];
        let previous_low = series.low[i - 1];
        if !high.is_finite()
            || !previous_high.is_finite()
            || !low.is_finite()
            || !previous_low.is_finite()
        {
            plus_dm[i] = f64::NAN;
            minus_dm[i] = f64::NAN;
            continue;
        }
        let high_delta = high - previous_high;
        let low_delta = low - previous_low;
        plus_dm[i] = if high_delta > low_delta {
            high_delta
        } else {
            0.0
        };
        minus_dm[i] = if low_delta > high_delta {
            low_delta
        } else {
            0.0
        };
    }
    Some((plus_dm, minus_dm))
}

fn rolling_ratio_mean(
    numerators: &(impl F64SeriesView + ?Sized),
    denominators: &(impl F64SeriesView + ?Sized),
    window: usize,
) -> Option<f64> {
    let n = numerators.len().min(denominators.len());
    if window == 0 || n < window {
        return None;
    }
    let mut sum = 0.0;
    for i in n - window..n {
        sum += safe_ratio(numerators.value_at(i), denominators.value_at(i))?;
    }
    finite_opt(Some(sum / window as f64))
}

fn activity_ratio(values: &(impl F64SeriesView + ?Sized), window: usize) -> Option<f64> {
    let current = current_value(values)?;
    let mean = rolling_mean_last(values, window).ok().flatten()?;
    safe_ratio(current, mean)
}

fn close_momentum_oscillator(
    series: &CnSeries<'_>,
    periods: usize,
    rsi: bool,
    fill_flat_with_zero: bool,
) -> Option<f64> {
    let n = series.close.len();
    if periods == 0 || n < periods {
        return None;
    }
    let mut gain = 0.0;
    let mut loss = 0.0;
    for i in n - periods..n {
        let current = series.close[i];
        if !current.is_finite() {
            return None;
        }
        let delta = if i == 0 {
            0.0
        } else {
            let previous = series.close[i - 1];
            if !previous.is_finite() {
                return None;
            }
            current - previous
        };
        if delta > 0.0 {
            gain += delta;
        } else {
            loss -= delta;
        }
    }
    let total = gain + loss;
    if total <= 1e-12 {
        return fill_flat_with_zero.then_some(0.0);
    }
    let value = if rsi {
        100.0 * gain / total
    } else {
        100.0 * (gain - loss) / total
    };
    finite_opt(Some(value))
}

fn close_log_return(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 2 {
        return None;
    }
    let current = series.close[n - 1];
    let previous = series.close[n - 2];
    if !current.is_finite() || !previous.is_finite() || current <= 0.0 || previous <= 0.0 {
        return None;
    }
    finite_opt(Some((current / previous).ln()))
}

fn compute_baseline_023(series: &CnSeries<'_>) -> Option<f64> {
    close_momentum_oscillator(series, 14, false, false)
}

fn compute_baseline_030(series: &CnSeries<'_>) -> Option<f64> {
    let short = rolling_mean_last(&series.close, 9).ok().flatten()?;
    let long = rolling_mean_last(&series.close, 150).ok().flatten()?;
    finite_opt(Some(short + long))
}

fn compute_baseline_033(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_difference(&series.close, 12, 26)
}

fn compute_baseline_037(series: &CnSeries<'_>) -> Option<f64> {
    rolling_high_low_range(series, 14)
}

fn compute_baseline_041(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_difference(&series.volume, 12, 26)
}

fn compute_baseline_044(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 14 {
        return None;
    }
    let tail: Vec<f64> = (series.close.len() - 14..series.close.len())
        .map(|i| series.close[i])
        .collect();
    linear_regression_slope(&tail)
}

fn compute_baseline_048(series: &CnSeries<'_>) -> Option<f64> {
    rolling_ratio_mean(&series.buy_volume, &series.volume, 30)
}

fn compute_baseline_050(series: &CnSeries<'_>) -> Option<f64> {
    rolling_range_mean(series, 10, 10)
}

fn compute_baseline_056(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_difference(&series.volume, 12, 26)
}

fn compute_baseline_064(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_difference(&series.close, 12, 26)
}

fn compute_baseline_075(series: &CnSeries<'_>) -> Option<f64> {
    activity_ratio(&series.volume, 300)
}

fn compute_baseline_078(series: &CnSeries<'_>) -> Option<f64> {
    activity_ratio(&series.amount, 300)
}

fn compute_baseline_084(series: &CnSeries<'_>) -> Option<f64> {
    let close = current_value(&series.close)?;
    let open = current_value(&series.open)?;
    safe_ratio(close - open, current_value(&series.sell_vwap)?)
}

fn compute_baseline_089(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_difference(&series.close, 12, 26)
}

fn compute_baseline_094(series: &CnSeries<'_>) -> Option<f64> {
    activity_ratio(&series.volume, 300)
}

fn compute_baseline_095(series: &CnSeries<'_>) -> Option<f64> {
    activity_ratio(&series.amount, 300)
}

fn compute_baseline_097(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.close, 45).ok().flatten()
}

fn compute_baseline_102(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.net_buy_pct, 120).ok().flatten()
}

fn compute_baseline_106(series: &CnSeries<'_>) -> Option<f64> {
    let close = rolling_mean_last(&series.close, 20).ok().flatten()?;
    let high = rolling_mean_last(&series.high, 20).ok().flatten()?;
    let low = rolling_mean_last(&series.low, 20).ok().flatten()?;
    finite_opt(Some(close + high - low))
}

fn compute_baseline_108(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 1)
}

fn compute_baseline_130(series: &CnSeries<'_>) -> Option<f64> {
    rolling_range_mean(series, 14, 1)
}

fn compute_baseline_142(series: &CnSeries<'_>) -> Option<f64> {
    rolling_high_low_range(series, 14)
}

fn compute_baseline_144(series: &CnSeries<'_>) -> Option<f64> {
    finite_opt(Some(
        current_value(&series.close)? - current_value(&series.open)?,
    ))
}

fn compute_baseline_147(series: &CnSeries<'_>) -> Option<f64> {
    close_momentum_oscillator(series, 14, true, false)
}

fn compute_baseline_150(series: &CnSeries<'_>) -> Option<f64> {
    let tr = build_true_range_series(series)?;
    rolling_mean_last_opt_from_series(&tr, 14, 14)
        .ok()
        .flatten()
}

fn compute_baseline_155(series: &CnSeries<'_>) -> Option<f64> {
    rolling_ratio_mean(&series.buy_volume, &series.volume, 150)
}

fn compute_baseline_165(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 1)
}

fn compute_baseline_176(series: &CnSeries<'_>) -> Option<f64> {
    let close = current_value(&series.close)?;
    let open = current_value(&series.open)?;
    safe_ratio(close - open, open)
}

fn compute_baseline_183(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n == 0 {
        return None;
    }
    let current = current_value(&series.close)?;
    if n == 1 {
        return Some(0.0);
    }
    safe_ratio(current - series.close[n - 2], current)
}

fn compute_baseline_186(series: &CnSeries<'_>) -> Option<f64> {
    rolling_range_mean(series, 14, 14)
}

fn compute_baseline_197(series: &CnSeries<'_>) -> Option<f64> {
    close_momentum_oscillator(series, 14, true, true)
}

fn compute_baseline_001(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.close, 30).ok().flatten()
}

fn compute_baseline_002(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.close, 30).ok().flatten()
}

fn compute_baseline_003(series: &CnSeries<'_>) -> Option<f64> {
    rolling_weighted_mean_last(&series.close, 30)
}

fn compute_baseline_004(series: &CnSeries<'_>) -> Option<f64> {
    let s1 = rolling_mean_series(&series.close, 30, 30).ok()?;
    let s2 = rolling_mean_series_opt(&s1, 30, 30).ok()?;
    let s3 = rolling_mean_series_opt(&s2, 30, 30).ok()?;
    let s4 = rolling_mean_series_opt(&s3, 30, 30).ok()?;
    let s5 = rolling_mean_series_opt(&s4, 30, 30).ok()?;
    let s6 = rolling_mean_series_opt(&s5, 30, 30).ok()?;
    let v1 = last_opt(&s1)?;
    let v2 = last_opt(&s2)?;
    let v3 = last_opt(&s3)?;
    let v4 = last_opt(&s4)?;
    let v5 = last_opt(&s5)?;
    let v6 = last_opt(&s6)?;
    finite_opt(Some(
        v1 - 2.0 * v2 + 3.0 * v3 - 4.0 * v4 + 5.0 * v5 - 6.0 * v6,
    ))
}

fn compute_baseline_005(series: &CnSeries<'_>) -> Option<f64> {
    rsi_last_from_series(&series.close, 60)
}

fn compute_baseline_006(series: &CnSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_last(&series.amount, 12).ok().flatten()?;
    let slow = rolling_mean_last(&series.amount, 26).ok().flatten()?;
    finite_opt(Some(fast - slow))
}

fn compute_baseline_007(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.net_buy_amount.len();
    if n < 9 {
        return None;
    }
    let start = n - 9;
    let mut macd_tail = Vec::with_capacity(9);
    for end in start + 1..=n {
        let fast = rolling_mean_at_from_series(&series.net_buy_amount, end, 12, 12)
            .ok()
            .flatten();
        let slow = rolling_mean_at_from_series(&series.net_buy_amount, end, 26, 26)
            .ok()
            .flatten();
        let macd = match (fast, slow) {
            (Some(f), Some(s)) => finite_opt(Some(f - s)),
            _ => Some(0.0),
        };
        macd_tail.push(macd);
    }
    rolling_mean_last_opt_from_series(&macd_tail, 9, 9)
        .ok()
        .flatten()
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_baseline_008(series: &CnSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_series(&series.large_buy, 12, 12).ok()?;
    let slow = rolling_mean_series(&series.large_buy, 26, 26).ok()?;
    let macd: Vec<Option<f64>> = (0..series.large_buy.len())
        .map(|i| match (fast[i], slow[i]) {
            (Some(f), Some(s)) => finite_opt(Some(f - s)),
            _ => Some(0.0),
        })
        .collect();
    let signal = rolling_mean_series_opt(&macd, 9, 9).ok()?;
    let hist: Vec<Option<f64>> = macd
        .iter()
        .zip(signal.iter())
        .map(|(m, s)| {
            let m = finite_opt(*m)?;
            let s = finite_opt(*s)?;
            finite_opt(Some(m - s))
        })
        .collect();
    last_opt(&hist)
}

fn compute_baseline_009(series: &CnSeries<'_>) -> Option<f64> {
    let upper = pct_change_last(&series.close, 10)?;
    let middle = pct_change_last(&series.close, 2)?;
    finite_opt(Some(upper - middle))
}

fn compute_baseline_010(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 3 {
        return None;
    }
    let n = series.close.len();
    let curr_prev = series.close[n - 2];
    let prev_prev = series.close[n - 3];
    if curr_prev.abs() <= 1e-12 || prev_prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    let curr = (series.close[n - 1] - curr_prev) / curr_prev;
    let prev = (curr_prev - prev_prev) / prev_prev;
    if prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((curr - prev) / prev))
}

fn compute_baseline_011(series: &CnSeries<'_>) -> Option<f64> {
    let high_max = rolling_max_last(&series.high, 20)?;
    let low_min = rolling_min_last(&series.low, 20)?;
    finite_opt(Some(high_max - low_min))
}

fn compute_baseline_012(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() <= 10 {
        return None;
    }
    finite_opt(Some(
        series.close[series.close.len() - 1] - series.close[series.close.len() - 11],
    ))
}

fn compute_baseline_013(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| (series.high[i] + series.low[i] + series.close[i]) / 3.0)
        .collect();
    let sma = rolling_mean_last(&tp, 14).ok().flatten()?;
    let start = n - 14;
    let mean_dev = tp[start..n].iter().map(|v| (v - sma).abs()).sum::<f64>() / 14.0;
    if mean_dev.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((tp[n - 1] - sma) / (0.015 * mean_dev)))
}

fn compute_baseline_014(series: &CnSeries<'_>) -> Option<f64> {
    cmo_last_from_series(&series.close, 14)
}

fn compute_baseline_015(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.large_order, 10)
}

fn compute_baseline_016(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let mut plus_dm = vec![0.0; n];
    let mut minus_dm = vec![0.0; n];
    let mut tr = vec![None; n];
    for i in 1..n {
        let high = series.high[i];
        let previous_high = series.high[i - 1];
        let low = series.low[i];
        let previous_low = series.low[i - 1];
        let previous_close = series.close[i - 1];
        if !high.is_finite()
            || !previous_high.is_finite()
            || !low.is_finite()
            || !previous_low.is_finite()
            || !previous_close.is_finite()
        {
            plus_dm[i] = f64::NAN;
            minus_dm[i] = f64::NAN;
            tr[i] = Some(f64::NAN);
            continue;
        }
        let hd = high - previous_high;
        let ld = low - previous_low;
        plus_dm[i] = if hd > ld { hd } else { 0.0 };
        minus_dm[i] = if ld > hd { ld } else { 0.0 };
        tr[i] = Some(
            (high - low)
                .max(high - previous_close)
                .max(low - previous_close),
        );
    }
    let plus_ma = rolling_mean_series(&plus_dm, 14, 14).ok()?;
    let minus_ma = rolling_mean_series(&minus_dm, 14, 14).ok()?;
    let tr_ma = rolling_mean_series_opt(&tr, 14, 14).ok()?;
    let dx: Vec<Option<f64>> = (0..n)
        .map(|i| {
            let (p, m, t) = match (plus_ma[i], minus_ma[i], tr_ma[i]) {
                (Some(p), Some(m), Some(t)) if p.is_finite() && m.is_finite() && t.is_finite() => {
                    (p, m, t)
                }
                (Some(_), Some(_), Some(_)) => return Some(f64::NAN),
                _ => return None,
            };
            if t.abs() <= 1e-12 {
                return Some(0.0);
            }
            let plus_di = 100.0 * p / t;
            let minus_di = 100.0 * m / t;
            let den = plus_di + minus_di;
            if den.abs() <= 1e-12 {
                return Some(0.0);
            }
            finite_opt(Some((plus_di - minus_di).abs() / den * 100.0))
        })
        .collect();
    rolling_mean_last_opt_from_series(&dx, 14, 14)
        .ok()
        .flatten()
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_baseline_017(series: &CnSeries<'_>) -> Option<f64> {
    let tr = build_true_range_series(series)?;
    rolling_mean_last_opt_from_series(&tr, 14, 14)
        .ok()
        .flatten()
}

fn compute_baseline_018(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.buy_volume.len().min(series.sell_volume.len());
    if n <= 60 {
        return None;
    }
    let mut ratio = Vec::with_capacity(n);
    for i in 0..n {
        let buy = series.buy_volume[i];
        let sell = series.sell_volume[i];
        ratio.push(buy / sell);
    }
    let curr = *ratio.last()?;
    let prev = ratio[n - 1 - 60];
    if !curr.is_finite() || !prev.is_finite() {
        return None;
    }
    if prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((curr - prev) / prev))
}

fn compute_baseline_019(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() <= 3 {
        return None;
    }
    let prev = series.close[series.close.len() - 4];
    if prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.close[series.close.len() - 1] / prev))
}

fn compute_baseline_020(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 16 {
        return None;
    }
    let mut fastk = vec![None; n];
    for i in 13..n {
        let mut low_min = f64::INFINITY;
        let mut high_max = f64::NEG_INFINITY;
        let mut missing = !series.close[i].is_finite();
        for j in i + 1 - 14..i + 1 {
            if !series.low[j].is_finite() || !series.high[j].is_finite() {
                missing = true;
                break;
            }
            low_min = low_min.min(series.low[j]);
            high_max = high_max.max(series.high[j]);
        }
        if missing {
            fastk[i] = Some(f64::NAN);
            continue;
        }
        let den = high_max - low_min;
        fastk[i] = if den.abs() > 1e-12 {
            finite_opt(Some((series.close[i] - low_min) / den * 100.0))
        } else {
            Some(0.0)
        };
    }
    rolling_mean_last_opt_from_series(&fastk, 3, 3)
        .ok()
        .flatten()
}

fn compute_baseline_021(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 18 {
        return None;
    }
    let mut fastk = vec![None; n];
    for i in 13..n {
        let mut low_min = f64::INFINITY;
        let mut high_max = f64::NEG_INFINITY;
        let mut missing = !series.close[i].is_finite();
        for j in i + 1 - 14..i + 1 {
            if !series.low[j].is_finite() || !series.high[j].is_finite() {
                missing = true;
                break;
            }
            low_min = low_min.min(series.low[j]);
            high_max = high_max.max(series.high[j]);
        }
        if missing {
            fastk[i] = Some(f64::NAN);
            continue;
        }
        let den = high_max - low_min;
        fastk[i] = if den.abs() > 1e-12 {
            finite_opt(Some((series.close[i] - low_min) / den * 100.0))
        } else {
            Some(0.0)
        };
    }
    let slowk = rolling_mean_series_opt(&fastk, 3, 3).ok()?;
    rolling_mean_last_opt_from_series(&slowk, 3, 3)
        .ok()
        .flatten()
}

fn compute_baseline_022(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let high_max = rolling_max_last(&series.high, 14)?;
    let low_min = rolling_min_last(&series.low, 14)?;
    let den = high_max - low_min;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((high_max - series.close[n - 1]) / den * -100.0))
}

fn compute_baseline_024(series: &CnSeries<'_>) -> Option<f64> {
    let tr = build_true_range_series(series)?;
    last_opt(&tr)
}

fn compute_baseline_025(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.volume.len())
        .min(series.count.len());
    if n < 30 {
        return None;
    }
    let mut cv = Vec::with_capacity(n);
    for i in 0..n {
        cv.push(series.close[i] * series.volume[i]);
    }
    let num = rolling_sum_last_with_min_periods(&cv, 30, 30)
        .ok()
        .flatten()?;
    let den = rolling_sum_last_with_min_periods(&series.count, 30, 30)
        .ok()
        .flatten()?;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(num / den))
}

fn compute_baseline_026(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 7 {
        return None;
    }
    let tp = |i: usize| (series.high[i] + series.low[i] + series.close[i]) / 3.0;
    finite_opt(Some(tp(n - 1) - 2.0 * tp(n - 4) + tp(n - 7)))
}

fn compute_baseline_027(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 4 {
        return None;
    }
    let x0 = series.high[n - 1] - series.low[n - 1];
    let x1 = series.high[n - 2] - series.low[n - 2];
    let x2 = series.high[n - 3] - series.low[n - 3];
    let diff_curr = x0 - x1;
    let diff_prev = x1 - x2;
    if diff_prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((diff_curr - diff_prev) / diff_prev))
}

fn compute_baseline_028(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n <= 30 {
        return None;
    }
    let change = series.close[n - 1] - series.close[n - 1 - 30];
    let mut abs_diff = Vec::with_capacity(n);
    abs_diff.push(f64::NAN);
    for i in 1..n {
        abs_diff.push((series.close[i] - series.close[i - 1]).abs());
    }
    let volatility = tail_quantile_last(&abs_diff, 30, 0.3)?;
    if volatility.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(change / volatility))
}

fn compute_baseline_029(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 34 {
        return None;
    }
    Some(0.0)
}

fn compute_baseline_031(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.close, 20).ok().flatten()?;
    let std = sample_std_last(&series.close, 20, 20)?;
    finite_opt(Some(mean - 2.0 * std))
}

fn compute_baseline_032(series: &CnSeries<'_>) -> Option<f64> {
    if series.open.len() <= 10 {
        return None;
    }
    finite_opt(Some(
        series.open[series.open.len() - 1] - series.open[series.open.len() - 11],
    ))
}

fn compute_baseline_034(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n == 0 {
        return None;
    }
    let fast = rolling_mean_series(&series.close, 12, 12).ok()?;
    let slow = rolling_mean_series(&series.close, 26, 26).ok()?;
    let macd: Vec<Option<f64>> = (0..n)
        .map(|i| match (fast[i], slow[i]) {
            (Some(f), Some(s)) => finite_opt(Some(f - s)),
            _ => Some(0.0),
        })
        .collect();
    let signal = rolling_mean_series_opt(&macd, 9, 9).ok()?;
    let smmao: Vec<Option<f64>> = macd
        .iter()
        .zip(signal.iter())
        .map(|(m, s)| {
            let m = finite_opt(*m)?;
            let s = finite_opt(*s)?;
            finite_opt(Some(m - 2.0 * s))
        })
        .collect();
    last_opt(&smmao)
}

fn compute_baseline_035(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.large_order.len().min(series.small_order.len());
    if n < 59 {
        return None;
    }
    let large_ma = rolling_mean_series(&series.large_order, 30, 30).ok()?;
    let small_ma = rolling_mean_series(&series.small_order, 30, 30).ok()?;
    let diff: Vec<Option<f64>> = (0..n)
        .map(|i| match (large_ma[i], small_ma[i]) {
            (Some(a), Some(b)) => finite_opt(Some(b - a)),
            _ => None,
        })
        .collect();
    rolling_mean_last_opt_from_series(&diff, 30, 30)
        .ok()
        .flatten()
}

fn compute_baseline_036(series: &CnSeries<'_>) -> Option<f64> {
    let fast = rolling_weighted_mean_last(&series.close, 12)?;
    let slow = rolling_weighted_mean_last(&series.close, 26)?;
    finite_opt(Some(fast - slow))
}

fn compute_baseline_038(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 15 {
        return None;
    }
    let mut mom = vec![None; n];
    for i in 10..n {
        mom[i] = finite_opt(Some(series.close[i] - series.close[i - 10]));
    }
    rolling_mean_last_opt_from_series(&mom, 5, 5).ok().flatten()
}

fn compute_baseline_039(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 15 {
        return None;
    }
    let mut mom = vec![0.0; n];
    for i in 10..n {
        mom[i] = series.close[i] - series.close[i - 10];
    }
    rolling_weighted_mean_last(&mom, 5)
}

fn compute_baseline_040(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    if n < 360 {
        return None;
    }
    let mut x = Vec::with_capacity(n);
    x.push(0.0);
    for i in 1..n {
        let prev = series.close[i - 1];
        let delta = series.close[i] - series.close[i - 1];
        let pct = if prev.abs() > 1e-12 {
            delta / prev
        } else {
            0.0
        };
        x.push(pct * series.volume[i]);
    }
    rolling_sum_last_with_min_periods(&x, 360, 360)
        .ok()
        .flatten()
}

fn compute_baseline_042(series: &CnSeries<'_>) -> Option<f64> {
    rsi_last_from_series(&series.volume, 14)
}

fn compute_baseline_043(series: &CnSeries<'_>) -> Option<f64> {
    let tr = build_true_range_series(series)?;
    let atr = rolling_mean_series_opt(&tr, 14, 14).ok()?;
    let curr = last_opt(&atr)?;
    let sma_atr = rolling_mean_last_opt_from_series(&atr, 7, 7)
        .ok()
        .flatten()?;
    finite_opt(Some(curr - sma_atr))
}

fn compute_baseline_045(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.buy_amount.len().min(series.sell_amount.len());
    let window = 90;
    if n < window {
        return None;
    }
    let tail: Vec<f64> = (n - window..n)
        .map(|i| series.buy_amount[i] - series.sell_amount[i])
        .collect();
    linear_regression_intercept(&tail)
}

fn compute_baseline_046(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.large_buy.len().min(series.large_sell.len());
    if n < 30 {
        return None;
    }
    let tail: Vec<f64> = (n - 30..n)
        .map(|i| series.large_buy[i] - series.large_sell[i])
        .collect();
    linear_regression_predict_last(&tail)
}

fn compute_baseline_047(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.small_buy.len().min(series.small_sell.len());
    if n < 300 {
        return None;
    }
    let diff: Vec<f64> = (0..n)
        .map(|i| series.small_buy[i] - series.small_sell[i])
        .collect();
    rolling_mean_last(&diff, 300).ok().flatten()
}

fn compute_baseline_049(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.small_buy.len().min(series.small_sell.len());
    if n < 150 {
        return None;
    }
    let diff: Vec<f64> = (0..n)
        .map(|i| series.small_buy[i] - series.small_sell[i])
        .collect();
    rolling_rank_last(&diff, 150).ok().flatten()
}

fn compute_baseline_051(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.close, 10).ok().flatten()?;
    finite_opt(Some(series.close[series.close.len() - 1] - mean))
}

fn compute_baseline_052(series: &CnSeries<'_>) -> Option<f64> {
    let high_ma = rolling_mean_last(&series.high, 14).ok().flatten()?;
    let low_ma = rolling_mean_last(&series.low, 14).ok().flatten()?;
    finite_opt(Some(high_ma - low_ma))
}

fn compute_baseline_053(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let high_tail: Vec<f64> = (n - 14..n).map(|i| series.high[i]).collect();
    let low_tail: Vec<f64> = (n - 14..n).map(|i| series.low[i]).collect();
    let cov = sample_cov(&high_tail, &low_tail)?;
    let mean_low = low_tail.iter().sum::<f64>() / low_tail.len() as f64;
    let var_low = low_tail
        .iter()
        .map(|v| {
            let d = *v - mean_low;
            d * d
        })
        .sum::<f64>()
        / (low_tail.len() as f64 - 1.0);
    if var_low.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(cov / var_low))
}

fn compute_baseline_054(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len())
        .min(series.volume.len());
    if n < 14 {
        return None;
    }
    let mut avg_price = Vec::with_capacity(n);
    let mut avg_pxv = Vec::with_capacity(n);
    for i in 0..n {
        let price = (series.high[i] + series.low[i] + series.close[i]) / 3.0;
        avg_price.push(price);
        avg_pxv.push(price * series.volume[i]);
    }
    let num = rolling_sum_last_with_min_periods(&avg_pxv, 14, 14)
        .ok()
        .flatten()?;
    let den = rolling_sum_last_with_min_periods(&series.volume, 14, 14)
        .ok()
        .flatten()?;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(avg_price[n - 1] - num / den))
}

fn compute_baseline_055(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_series(&series.close, 14, 14).ok()?;
    let mut sq_dev = Vec::with_capacity(series.close.len());
    for i in 0..series.close.len() {
        match mean[i] {
            Some(m) => {
                let d = series.close[i] - m;
                sq_dev.push(finite_opt(Some(d * d)));
            }
            None => sq_dev.push(None),
        }
    }
    rolling_sum_last_opt_from_series(&sq_dev, 14, 14)
        .ok()
        .flatten()
}

fn compute_baseline_057(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n <= 10 {
        return None;
    }
    let curr = (series.high[n - 1] + series.low[n - 1] + series.close[n - 1]) / 3.0;
    let prev = (series.high[n - 11] + series.low[n - 11] + series.close[n - 11]) / 3.0;
    finite_opt(Some(curr - prev))
}

fn compute_baseline_058(series: &CnSeries<'_>) -> Option<f64> {
    sample_std_last(&series.volume, 14, 14)
}

fn compute_baseline_059(series: &CnSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.close, 20, 20)?;
    finite_opt(Some(4.0 * std))
}

fn compute_baseline_060(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n <= 10 {
        return None;
    }
    let curr = series.high[n - 1] - series.low[n - 1];
    let prev = series.high[n - 11] - series.low[n - 11];
    finite_opt(Some(curr - prev))
}

fn compute_baseline_061(series: &CnSeries<'_>) -> Option<f64> {
    let short = rolling_mean_last(&series.close, 12).ok().flatten()?;
    let long = rolling_mean_last(&series.close, 26).ok().flatten()?;
    finite_opt(Some(short - long))
}

fn compute_baseline_062(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n == 0 {
        return None;
    }
    let low = series.low[n - 1];
    if low.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.high[n - 1] / low))
}

fn compute_baseline_063(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 20 {
        return None;
    }
    let tail: Vec<f64> = (series.close.len() - 20..series.close.len())
        .map(|i| series.close[i])
        .collect();
    linear_regression_slope(&tail)
}

fn compute_baseline_065(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .open
        .len()
        .min(series.high.len())
        .min(series.low.len())
        .min(series.close.len());
    if n == 0 {
        return None;
    }
    let den = series.high[n - 1] - series.low[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.close[n - 1] - series.open[n - 1]) / den))
}

fn compute_baseline_066(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let (plus_dm, minus_dm) = directional_movement_series(series)?;
    let tr = build_true_range_series(series)?;
    let plus = rolling_mean_last(&plus_dm, 14).ok().flatten()?;
    let minus = rolling_mean_last(&minus_dm, 14).ok().flatten()?;
    let tr_ma = rolling_mean_last_opt_from_series(&tr, 14, 14)
        .ok()
        .flatten()?;
    if tr_ma.abs() <= 1e-12 {
        return Some(0.0);
    }
    let plus_di = 100.0 * plus / tr_ma;
    let minus_di = 100.0 * minus / tr_ma;
    let den = plus_di + minus_di;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((plus_di - minus_di).abs() / den * 100.0))
}

fn compute_baseline_067(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let (_, minus_dm) = directional_movement_series(series)?;
    let tr = build_true_range_series(series)?;
    let minus = rolling_mean_last(&minus_dm, 14).ok().flatten()?;
    let tr_ma = rolling_mean_last_opt_from_series(&tr, 14, 14)
        .ok()
        .flatten()?;
    if tr_ma.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * minus / tr_ma))
}

fn compute_baseline_068(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let (plus_dm, _) = directional_movement_series(series)?;
    let tr = build_true_range_series(series)?;
    let plus = rolling_mean_last(&plus_dm, 14).ok().flatten()?;
    let tr_ma = rolling_mean_last_opt_from_series(&tr, 14, 14)
        .ok()
        .flatten()?;
    if tr_ma.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * plus / tr_ma))
}

fn compute_baseline_069(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let (plus_dm, _) = directional_movement_series(series)?;
    rolling_mean_last(&plus_dm, 14).ok().flatten()
}

fn compute_baseline_070(series: &CnSeries<'_>) -> Option<f64> {
    let fast = rolling_mean_last(&series.close, 3).ok().flatten()?;
    let slow = rolling_mean_last(&series.close, 10).ok().flatten()?;
    if slow.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((fast - slow) / slow * 100.0))
}

fn compute_baseline_071(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let (_, minus_dm) = directional_movement_series(series)?;
    rolling_mean_last(&minus_dm, 14).ok().flatten()
}

fn compute_baseline_072(series: &CnSeries<'_>) -> Option<f64> {
    sample_std_last(&series.close, 5, 5)
}

fn compute_baseline_073(series: &CnSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.close, 5, 5)?;
    finite_opt(Some(std * std))
}

fn compute_baseline_074(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 10 {
        return None;
    }
    let median: Vec<f64> = (0..n)
        .map(|i| (series.high[i] + series.low[i]) / 2.0)
        .collect();
    rolling_mean_last(&median, 10).ok().flatten()
}

fn compute_baseline_076(series: &CnSeries<'_>) -> Option<f64> {
    let tr = build_true_range_series(series)?;
    let atr = rolling_mean_last_opt_from_series(&tr, 14, 14)
        .ok()
        .flatten()?;
    let close = series.close[series.close.len() - 1];
    if close.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((atr / close) * 100.0))
}

fn compute_baseline_077(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 29 {
        return None;
    }
    let mut bp = vec![0.0; n];
    let mut tr = vec![0.0; n];
    for i in 1..n {
        let high = series.high[i];
        let low = series.low[i];
        let close = series.close[i];
        let previous_close = series.close[i - 1];
        if !high.is_finite()
            || !low.is_finite()
            || !close.is_finite()
            || !previous_close.is_finite()
        {
            bp[i] = f64::NAN;
            tr[i] = f64::NAN;
            continue;
        }
        bp[i] = close - low.min(previous_close);
        tr[i] = (high - low)
            .max((high - previous_close).abs())
            .max((low - previous_close).abs());
    }
    let avg7_num = rolling_sum_last_with_min_periods(&bp, 7, 7)
        .ok()
        .flatten()?;
    let avg7_den = rolling_sum_last_with_min_periods(&tr, 7, 7)
        .ok()
        .flatten()?;
    let avg14_num = rolling_sum_last_with_min_periods(&bp, 14, 14)
        .ok()
        .flatten()?;
    let avg14_den = rolling_sum_last_with_min_periods(&tr, 14, 14)
        .ok()
        .flatten()?;
    let avg28_num = rolling_sum_last_with_min_periods(&bp, 28, 28)
        .ok()
        .flatten()?;
    let avg28_den = rolling_sum_last_with_min_periods(&tr, 28, 28)
        .ok()
        .flatten()?;
    if avg7_den.abs() <= 1e-12 || avg14_den.abs() <= 1e-12 || avg28_den.abs() <= 1e-12 {
        return Some(0.0);
    }
    let ult =
        (4.0 * (avg7_num / avg7_den) + 2.0 * (avg14_num / avg14_den) + (avg28_num / avg28_den))
            / 7.0
            * 100.0;
    finite_opt(Some(ult))
}

fn compute_baseline_079(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len().min(series.volume.len()) < 14 {
        return None;
    }
    finite_opt(series.corr_close_volume_14_last).or_else(|| {
        rolling_corr_last(&series.close, &series.volume, 14, 1)
            .ok()
            .flatten()
    })
}

fn compute_baseline_080(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    let window = 10usize;
    if n < window {
        return None;
    }
    let start = n - window;
    let mut sum_high = 0.0;
    let mut sum_low = 0.0;
    for i in start..n {
        let h = series.high[i];
        let l = series.low[i];
        if !h.is_finite() || !l.is_finite() {
            return None;
        }
        sum_high += h;
        sum_low += l;
    }

    let mean_high = sum_high / window as f64;
    let mean_low = sum_low / window as f64;
    let mut cov: f64 = 0.0;
    let mut var_low: f64 = 0.0;
    for i in start..n {
        let dh = series.high[i] - mean_high;
        let dl = series.low[i] - mean_low;
        cov += dh * dl;
        var_low += dl * dl;
    }
    if var_low.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(cov / var_low))
}

fn compute_baseline_081(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.close.len())
        .min(series.vwap.len());
    if n < 30 {
        return None;
    }
    let avg_vwap = rolling_mean_last(&series.vwap, 30).ok().flatten()?;
    if avg_vwap.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.high[n - 1] - series.close[n - 1]) / avg_vwap))
}

fn compute_baseline_082(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.low.len())
        .min(series.buy_amount.len())
        .min(series.sell_amount.len());
    if n == 0 {
        return None;
    }
    let den = series.sell_amount[n - 1] - series.buy_amount[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.close[n - 1] - series.low[n - 1]) / den))
}

fn compute_baseline_083(series: &CnSeries<'_>) -> Option<f64> {
    rolling_position_last(&series.close, 14)
}

fn compute_baseline_085(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last_with_min_periods(&series.close, 30, 5)
        .ok()
        .flatten()?;
    let std = sample_std_last(&series.close, 30, 5)?;
    if std.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.close[series.close.len() - 1] - mean) / std))
}

fn compute_baseline_086(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len())
        .min(series.volume.len());
    if n < 14 {
        return None;
    }
    let mut tp = Vec::with_capacity(n);
    for i in 0..n {
        tp.push((series.high[i] + series.low[i] + series.close[i]) / 3.0);
    }
    let mut pos = vec![0.0; n];
    let mut neg = vec![0.0; n];
    for i in 1..n {
        let dt = tp[i] - tp[i - 1];
        if !dt.is_finite() || !series.volume[i].is_finite() {
            pos[i] = f64::NAN;
            neg[i] = f64::NAN;
            continue;
        }
        pos[i] = dt.max(0.0) * series.volume[i];
        neg[i] = (-dt).max(0.0) * series.volume[i];
    }
    let pos_sum = rolling_sum_last_with_min_periods(&pos, 14, 14)
        .ok()
        .flatten()?;
    let neg_sum = rolling_sum_last_with_min_periods(&neg, 14, 14)
        .ok()
        .flatten()?;
    let den = pos_sum + neg_sum;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * pos_sum / den))
}

fn compute_baseline_087(series: &CnSeries<'_>) -> Option<f64> {
    let ad = build_ad_line_rolling360(series)?;
    last_opt(&ad)
}

fn compute_baseline_088(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 42 {
        return None;
    }
    let ema1 = rolling_mean_series(&series.close, 14, 14).ok()?;
    let ema2 = rolling_mean_series_opt(&ema1, 14, 14).ok()?;
    let ema3 = rolling_mean_series_opt(&ema2, 14, 14).ok()?;
    let n = series.close.len();
    let v1 = finite_opt(ema1[n - 1])?;
    let v2 = finite_opt(ema2[n - 1])?;
    let prev_ema3 = finite_opt(ema3[n - 2])?;
    if prev_ema3.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((v1 - v2) / prev_ema3 * 100.0))
}

fn compute_baseline_090(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 5 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| (series.high[i] + series.low[i] + series.close[i]) / 3.0)
        .collect();
    let mean = rolling_mean_last_with_min_periods(&tp, 14, 5)
        .ok()
        .flatten()?;
    let std = sample_std_last(&tp, 14, 5)?;
    if std.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((tp[n - 1] - mean) / std))
}

fn compute_baseline_091(series: &CnSeries<'_>) -> Option<f64> {
    let obv = build_obv_cumulative(series)?;
    finite_opt(Some(*obv.last()?))
}

fn compute_baseline_092(series: &CnSeries<'_>) -> Option<f64> {
    rolling_position_last(&series.close, 10)
}

fn compute_baseline_093(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 10 {
        return None;
    }
    let tp: Vec<f64> = (n - 10..n)
        .map(|i| (series.high[i] + series.low[i] + series.close[i]) / 3.0)
        .collect();
    linear_regression_slope(&tp)
}

fn compute_baseline_096(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.close, 5).ok().flatten()
}

fn compute_baseline_098(series: &CnSeries<'_>) -> Option<f64> {
    let low = rolling_min_last(&series.close, 14)?;
    let high = rolling_max_last(&series.close, 14)?;
    if high.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(low / high))
}

fn compute_baseline_099(series: &CnSeries<'_>) -> Option<f64> {
    rolling_position_last(&series.close, 14)
}

fn compute_baseline_100(series: &CnSeries<'_>) -> Option<f64> {
    rolling_mean_last(&series.close, 180).ok().flatten()
}

fn compute_baseline_101(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.open.len().min(series.close.len());
    if n == 0 {
        return None;
    }
    if !series.open[n - 1].is_finite() || !series.close[n - 1].is_finite() {
        return None;
    }
    Some(if series.open[n - 1] == series.close[n - 1] {
        1.0
    } else {
        0.0
    })
}

fn compute_baseline_103(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 5 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| (series.high[i] + series.low[i] + series.close[i]) / 3.0)
        .collect();
    let std = sample_std_last(&tp, 5, 5)?;
    finite_opt(Some(4.0 * std))
}

fn compute_baseline_104(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 60 {
        return None;
    }
    let ratio: Vec<f64> = (0..n)
        .map(|i| {
            let high = series.high[i];
            let low = series.low[i];
            if !high.is_finite() || !low.is_finite() {
                return f64::NAN;
            }
            if low.abs() > 1e-12 {
                high / low
            } else {
                0.0
            }
        })
        .collect();
    rolling_mean_last(&ratio, 60).ok().flatten()
}

fn compute_baseline_105(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() <= 14 {
        return None;
    }
    finite_opt(Some(
        series.close[series.close.len() - 1] - series.close[series.close.len() - 15],
    ))
}

fn compute_baseline_107(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 10 {
        return None;
    }
    let tp: Vec<f64> = (0..n)
        .map(|i| (series.high[i] + series.low[i] + series.close[i]) / 3.0)
        .collect();
    let sma = rolling_mean_last(&tp, 10).ok().flatten()?;
    if sma.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((tp[n - 1] - sma) / sma))
}

fn compute_baseline_109(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.open.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let mut upper = Vec::with_capacity(n);
    for i in 0..n {
        let high = series.high[i];
        let open = series.open[i];
        let close = series.close[i];
        if !high.is_finite() || !open.is_finite() || !close.is_finite() {
            upper.push(f64::NAN);
        } else {
            upper.push(high - close.max(open));
        }
    }
    rolling_sum_last_with_min_periods(&upper, 14, 14)
        .ok()
        .flatten()
}

fn compute_baseline_110(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .low
        .len()
        .min(series.open.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let mut lower = Vec::with_capacity(n);
    for i in 0..n {
        let low = series.low[i];
        let open = series.open[i];
        let close = series.close[i];
        if !low.is_finite() || !open.is_finite() || !close.is_finite() {
            lower.push(f64::NAN);
        } else {
            lower.push(close.min(open) - low);
        }
    }
    rolling_sum_last_with_min_periods(&lower, 14, 14)
        .ok()
        .flatten()
}

fn compute_baseline_111(series: &CnSeries<'_>) -> Option<f64> {
    rolling_sum_last_with_min_periods(&series.volume, 10, 10)
        .ok()
        .flatten()
}

fn compute_baseline_112(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 14)
}

fn compute_baseline_113(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 90 {
        return None;
    }
    let mid_log: Vec<f64> = (0..n)
        .map(|i| ((series.high[i] + series.low[i]) / 2.0).ln())
        .collect();
    tail_quantile_last(&mid_log, 90, 0.2)
}

fn compute_baseline_114(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() <= 10 {
        return None;
    }
    let curr = series.close[series.close.len() - 1];
    let prev = series.close[series.close.len() - 11];
    if curr <= 0.0 || prev <= 0.0 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * (curr / prev).ln()))
}

fn compute_baseline_115(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.high.len())
        .min(series.low.len());
    if n < 20 {
        return None;
    }
    let mut price_diff = Vec::with_capacity(n);
    let mut range_diff = Vec::with_capacity(n);
    for i in 0..n {
        price_diff.push(series.close[i] - series.open[i]);
        range_diff.push(series.high[i] - series.low[i]);
    }
    let num = rolling_sum_last_with_min_periods(&price_diff, 20, 20)
        .ok()
        .flatten()?;
    let den = rolling_sum_last_with_min_periods(&range_diff, 20, 20)
        .ok()
        .flatten()?;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(100.0 * num / den))
}

fn compute_baseline_116(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len())
        .min(series.volume.len());
    if n < 14 {
        return None;
    }
    let mut tp = Vec::with_capacity(n);
    for i in 0..n {
        tp.push((series.high[i] + series.low[i] + series.close[i]) / 3.0);
    }
    let mut pos = vec![0.0; n];
    let mut neg = vec![0.0; n];
    for i in 1..n {
        let dt = tp[i] - tp[i - 1];
        if !dt.is_finite() || !series.volume[i].is_finite() {
            pos[i] = f64::NAN;
            neg[i] = f64::NAN;
            continue;
        }
        pos[i] = dt.max(0.0) * series.volume[i];
        neg[i] = (-dt).max(0.0) * series.volume[i];
    }
    let pos_sum = rolling_sum_last_with_min_periods(&pos, 14, 14)
        .ok()
        .flatten()?;
    let neg_sum = rolling_sum_last_with_min_periods(&neg, 14, 14)
        .ok()
        .flatten()?;
    finite_opt(Some(pos_sum - neg_sum))
}

fn compute_baseline_117(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 26 {
        return None;
    }
    let mut pos = vec![0.0; n];
    let mut neg = vec![0.0; n];
    for i in 1..n {
        let d = series.close[i] - series.close[i - 1];
        if !d.is_finite() {
            pos[i] = f64::NAN;
            neg[i] = f64::NAN;
            continue;
        }
        if d > 0.0 {
            pos[i] = d;
        } else if d < 0.0 {
            neg[i] = -d;
        }
    }
    let pos_avg = rolling_mean_last(&pos, 12).ok().flatten()?;
    let neg_avg = rolling_mean_last(&neg, 26).ok().flatten()?;
    if neg_avg.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(pos_avg / neg_avg))
}

fn compute_baseline_118(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 143 {
        return None;
    }
    let mut diff3 = vec![f64::NAN; n];
    let mut pct1 = vec![f64::NAN; n];
    for i in 3..n {
        diff3[i] = series.close[i] - series.close[i - 3];
    }
    for i in 1..n {
        let prev = series.close[i - 1];
        if prev.abs() > 1e-12 {
            pct1[i] = (series.close[i] - prev) / prev;
        }
    }
    corr_last_with_min_periods(&diff3, &pct1, 140, 140)
}

fn compute_baseline_119(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 40 {
        return None;
    }
    let ema1 = rolling_mean_series(&series.close, 14, 14).ok()?;
    let ema2 = rolling_mean_series_opt(&ema1, 14, 14).ok()?;
    let ema3 = rolling_mean_series_opt(&ema2, 14, 14).ok()?;
    let v1 = last_opt(&ema1)?;
    let v2 = last_opt(&ema2)?;
    let v3 = last_opt(&ema3)?;
    finite_opt(Some(3.0 * (v1 - v2) + v3))
}

fn compute_baseline_120(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 350 {
        return None;
    }
    let mut wcl = Vec::with_capacity(n);
    for i in 0..n {
        wcl.push((series.high[i] + series.low[i] + 2.0 * series.close[i]) / 4.0);
    }
    let mut pct = vec![f64::NAN; n];
    for i in 50..n {
        let prev = wcl[i - 50];
        if prev.abs() > 1e-12 {
            pct[i] = (wcl[i] - prev) / prev;
        }
    }
    let tail: Vec<f64> = pct[n - 300..n].to_vec();
    if tail.iter().filter(|v| v.is_finite()).count() < 300 {
        return None;
    }
    let mean = tail.iter().sum::<f64>() / 300.0;
    finite_opt(Some(mean))
}

fn compute_baseline_121(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.open.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let mut x = Vec::with_capacity(n);
    for i in 0..n {
        let upper = series.high[i] - series.close[i].max(series.open[i]);
        let lower = series.close[i].min(series.open[i]) - series.low[i];
        let body = (series.close[i] - series.open[i]).abs();
        x.push(upper - lower - body);
    }
    rolling_sum_last_with_min_periods(&x, 14, 14).ok().flatten()
}

fn compute_baseline_122(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 2 {
        return None;
    }
    let value = ((series.high[n - 1] - series.high[n - 2]).abs()
        + (series.low[n - 1] - series.low[n - 2]).abs()
        + (series.close[n - 1] - series.close[n - 2]).abs())
        / 3.0;
    finite_opt(Some(value))
}

fn compute_baseline_123(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 2 {
        return None;
    }
    let value = ((series.high[n - 1] - series.high[n - 2])
        + (series.low[n - 1] - series.low[n - 2])
        + (series.close[n - 1] - series.close[n - 2]))
        / 3.0;
    finite_opt(Some(value))
}

fn compute_baseline_124(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.buy_vwap.len().min(series.sell_vwap.len());
    if n == 0 {
        return None;
    }
    let den = series.sell_vwap[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.buy_vwap[n - 1] / den))
}

fn compute_baseline_125(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 20 {
        return None;
    }
    let mut up = vec![0.0; n];
    let mut down = vec![0.0; n];
    for i in 1..n {
        let diff = series.close[i] - series.close[i - 1];
        if !diff.is_finite() {
            up[i] = f64::NAN;
            down[i] = f64::NAN;
            continue;
        }
        if diff > 0.0 {
            up[i] = diff;
        } else if diff < 0.0 {
            down[i] = -diff;
        }
    }
    let au = rolling_mean_last(&up, 20).ok().flatten()?;
    let ad = rolling_mean_last(&down, 20).ok().flatten()?;
    let den = au + ad;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(au / den))
}

fn compute_baseline_126(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    let timeperiod = 14usize;
    if n < timeperiod + 1 {
        return None;
    }
    let start = n - (timeperiod + 1);
    let high_tail: Vec<f64> = (start..n).map(|i| series.high[i]).collect();
    let low_tail: Vec<f64> = (start..n).map(|i| series.low[i]).collect();
    let aroon_up = 100.0 * first_argmax(&high_tail)? as f64 / timeperiod as f64;
    let aroon_down = 100.0 * first_argmin(&low_tail)? as f64 / timeperiod as f64;
    finite_opt(Some(aroon_up - aroon_down))
}

fn compute_baseline_127(series: &CnSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.close, 14, 14)?;
    finite_opt(Some(-2.0 * std))
}

fn compute_baseline_128(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let mut range = Vec::with_capacity(n);
    for i in 0..n {
        range.push(series.high[i] - series.low[i]);
    }
    rolling_max_last(&range, 14)
}

fn compute_baseline_129(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 28 {
        return None;
    }
    let mut diff = vec![f64::NAN; n];
    for i in 14..n {
        diff[i] = series.close[i] - series.close[i - 14];
    }
    rolling_max_last(&diff, 14)
}

fn compute_baseline_131(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.volume, 14).ok().flatten()?;
    if mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.volume[series.volume.len() - 1] / mean))
}

fn compute_baseline_132(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.close.len());
    if n < 10 {
        return None;
    }
    let mut x = Vec::with_capacity(n);
    for i in 0..n {
        x.push(series.high[i] - series.close[i]);
    }
    rolling_sum_last_with_min_periods(&x, 10, 10).ok().flatten()
}

fn compute_baseline_133(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.low.len());
    if n < 10 {
        return None;
    }
    let mut x = Vec::with_capacity(n);
    for i in 0..n {
        x.push(series.close[i] - series.low[i]);
    }
    rolling_sum_last_with_min_periods(&x, 10, 10).ok().flatten()
}

fn compute_baseline_134(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.open.len())
        .min(series.close.len());
    if n == 0 {
        return None;
    }
    let h = series.high[n - 1];
    let l = series.low[n - 1];
    let o = series.open[n - 1];
    let c = series.close[n - 1];
    if [h, l, o, c].iter().any(|value| !value.is_finite()) {
        return None;
    }
    let range = h - l;
    let is_hammer = (range > 3.0 * (o - c))
        && ((c - l) / (0.001 + range) > 0.6)
        && ((o - l) / (0.001 + range) > 0.6);
    Some(if is_hammer { 1.0 } else { 0.0 })
}

fn compute_baseline_135(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.open.len())
        .min(series.close.len());
    if n == 0 {
        return None;
    }
    let high = series.high[n - 1];
    let low = series.low[n - 1];
    let open = series.open[n - 1];
    let close = series.close[n - 1];
    if [high, low, open, close]
        .iter()
        .any(|value| !value.is_finite())
    {
        return None;
    }
    let range = high - low;
    let is_hangingman =
        (range > 3.0 * (open - close).abs()) && ((close - high).abs() / (0.0001 + range) > 0.6);
    Some(if is_hangingman { 1.0 } else { 0.0 })
}

fn compute_baseline_136(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.open.len())
        .min(series.close.len());
    if n == 0 {
        return None;
    }
    let high = series.high[n - 1];
    let low = series.low[n - 1];
    let open = series.open[n - 1];
    let close = series.close[n - 1];
    if [high, low, open, close]
        .iter()
        .any(|value| !value.is_finite())
    {
        return None;
    }
    let body = (close - open).abs();
    let upper = high - close.max(open);
    let lower = close.min(open) - low;
    Some(if upper > body * 2.0 && lower < body {
        1.0
    } else {
        0.0
    })
}

fn compute_baseline_137(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.volume.len());
    if n == 0 {
        return None;
    }
    let den = series.volume[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.close[n - 1] - series.open[n - 1]) / den))
}

fn compute_baseline_138(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n < 20 {
        return None;
    }
    let mut body = Vec::with_capacity(n);
    for i in 0..n {
        body.push((series.close[i] - series.open[i]).abs());
    }
    rolling_sum_last_with_min_periods(&body, 20, 20)
        .ok()
        .flatten()
}

fn compute_baseline_139(series: &CnSeries<'_>) -> Option<f64> {
    if series.volume.len() < 19 {
        return None;
    }
    let ma10 = rolling_mean_series(&series.volume, 10, 10).ok()?;
    let ratio: Vec<Option<f64>> = (0..series.volume.len())
        .map(|i| {
            let m = finite_opt(ma10[i])?;
            if m.abs() <= 1e-12 {
                return Some(0.0);
            }
            finite_opt(Some(series.volume[i] / m))
        })
        .collect();
    rolling_mean_last_opt_from_series(&ratio, 10, 10)
        .ok()
        .flatten()
}

fn compute_baseline_140(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.low.len().min(series.close.len());
    if n < 60 {
        return None;
    }
    let low_min = rolling_min_last(&series.low, 60)?;
    finite_opt(Some(low_min - series.close[n - 1]))
}

fn compute_baseline_141(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.close.len());
    if n < 60 {
        return None;
    }
    let high_max = rolling_max_last(&series.high, 60)?;
    finite_opt(Some(high_max - series.close[n - 1]))
}

fn compute_baseline_143(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n == 0 {
        return None;
    }
    finite_opt(Some(series.high[n - 1] - series.low[n - 1]))
}

fn compute_baseline_145(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.close, 14).ok().flatten()?;
    if mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.close[series.close.len() - 1] / mean))
}

fn compute_baseline_146(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n < 19 {
        return None;
    }
    let ratio: Vec<f64> = (0..n)
        .map(|i| {
            let den = series.open[i];
            if !series.close[i].is_finite() || !den.is_finite() {
                return f64::NAN;
            }
            if den.abs() > 1e-12 {
                series.close[i] / den
            } else {
                0.0
            }
        })
        .collect();
    rolling_mean_last(&ratio, 19).ok().flatten()
}

fn compute_baseline_148(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n == 0 {
        return None;
    }
    if !series.close[n - 1].is_finite() || !series.open[n - 1].is_finite() {
        return None;
    }
    Some(if series.close[n - 1] > series.open[n - 1] {
        1.0
    } else {
        0.0
    })
}

fn compute_baseline_149(series: &CnSeries<'_>) -> Option<f64> {
    let ad = build_ad_line_rolling360(series)?;
    rolling_mean_last_opt_from_series(&ad, 14, 14)
        .ok()
        .flatten()
}

fn compute_baseline_151(series: &CnSeries<'_>) -> Option<f64> {
    compute_tp_vpi_006(series)
}

fn compute_baseline_152(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 34 {
        return None;
    }
    let fast = rolling_mean_series(&series.close, 12, 12).ok()?;
    let slow = rolling_mean_series(&series.close, 26, 26).ok()?;
    let macd: Vec<Option<f64>> = (0..n)
        .map(|i| match (fast[i], slow[i]) {
            (Some(f), Some(s)) => finite_opt(Some(f - s)),
            _ => None,
        })
        .collect();
    let signal = rolling_mean_series_opt(&macd, 9, 9).ok()?;
    let m = last_opt(&macd)?;
    let s = last_opt(&signal)?;
    finite_opt(Some(m - s))
}

fn compute_baseline_153(series: &CnSeries<'_>) -> Option<f64> {
    let tr = build_true_range_series(series)?;
    let n = tr.len();
    if n < 15 {
        return None;
    }
    let mut tail = Vec::with_capacity(14);
    for item in tr.iter().take(n).skip(n - 14) {
        let value = finite_opt(*item)?;
        tail.push(value);
    }
    sample_std_last(&tail, 14, 14)
}

fn compute_baseline_154(series: &CnSeries<'_>) -> Option<f64> {
    if series.trade_time.len() < 120 {
        return None;
    }
    let mean = rolling_mean_last(&series.trade_time, 120).ok().flatten()?;
    if mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.trade_time[series.trade_time.len() - 1] / mean))
}

fn compute_baseline_156(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n == 0 {
        return None;
    }
    let spread: Vec<f64> = (0..n).map(|i| series.high[i] - series.low[i]).collect();
    rsi_last_from_series(&spread, 14)
}

fn compute_baseline_157(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n <= 30 {
        return None;
    }
    let current = series.close[n - 1];
    let previous = series.close[n - 31];
    if !current.is_finite() || !previous.is_finite() {
        return None;
    }
    let mut volatility = 0.0;
    for i in n - 30..n {
        let lhs = series.close[i];
        let rhs = series.close[i - 1];
        if !lhs.is_finite() || !rhs.is_finite() {
            return None;
        }
        volatility += (lhs - rhs).abs();
    }
    safe_ratio(current - previous, volatility)
}

fn compute_baseline_158(series: &CnSeries<'_>) -> Option<f64> {
    let sma1 = rolling_mean_series(&series.high, 60, 1).ok()?;
    let sma2 = rolling_mean_series_opt(&sma1, 60, 1).ok()?;
    let v1 = last_opt(&sma1)?;
    let v2 = last_opt(&sma2)?;
    finite_opt(Some(2.0 * v1 - v2))
}

fn compute_baseline_159(series: &CnSeries<'_>) -> Option<f64> {
    close_log_return(series)
        .map(f64::sin)
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_baseline_160(series: &CnSeries<'_>) -> Option<f64> {
    close_log_return(series)
        .map(f64::cos)
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_baseline_161(series: &CnSeries<'_>) -> Option<f64> {
    let dc = build_ht_dc_level(&series.close);
    rolling_mean_last_with_min_periods(&dc, 30, 3)
        .ok()
        .flatten()
}

fn compute_baseline_162(series: &CnSeries<'_>) -> Option<f64> {
    let dc = build_ht_dc_phase(&series.close);
    rolling_mean_last_with_min_periods(&dc, 14, 3)
        .ok()
        .flatten()
}

fn compute_baseline_163(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 30 {
        return None;
    }
    let min_close = rolling_min_last(&series.close, 30)?;
    let den = series.close[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((min_close - den) / den))
}

fn compute_baseline_164(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 30 {
        return None;
    }
    let max_close = rolling_max_last(&series.close, 30)?;
    let den = series.close[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((max_close - den) / den))
}

fn compute_baseline_166(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 5)
}

fn compute_baseline_167(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 10)
}

fn compute_baseline_168(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len();
    if n < 35 {
        return None;
    }
    let mut pct = vec![None; n];
    for i in 5..n {
        let prev = series.close[i - 5];
        if prev.abs() > 1e-12 {
            pct[i] = finite_opt(Some((series.close[i] - prev) / prev));
        } else {
            pct[i] = Some(0.0);
        }
    }
    rolling_mean_last_opt_from_series(&pct, 30, 30)
        .ok()
        .flatten()
}

fn compute_baseline_169(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len())
        .min(series.volume.len());
    if n < 14 {
        return None;
    }
    let mut tp = Vec::with_capacity(n);
    let mut mf = Vec::with_capacity(n);
    for i in 0..n {
        let t = (series.high[i] + series.low[i] + series.close[i]) / 3.0;
        tp.push(t);
        mf.push(t * series.volume[i]);
    }
    let mut pos = vec![0.0; n];
    let mut neg = vec![0.0; n];
    for i in 1..n {
        if !tp[i].is_finite() || !tp[i - 1].is_finite() || !mf[i].is_finite() {
            pos[i] = f64::NAN;
            neg[i] = f64::NAN;
            continue;
        }
        if tp[i] > tp[i - 1] {
            pos[i] = mf[i];
        } else if tp[i] < tp[i - 1] {
            neg[i] = mf[i];
        }
    }
    let pos_sum = rolling_sum_last_with_min_periods(&pos, 14, 14)
        .ok()
        .flatten()?;
    let neg_sum = rolling_sum_last_with_min_periods(&neg, 14, 14)
        .ok()
        .flatten()?;
    if pos_sum.abs() <= 1e-12 && neg_sum.abs() <= 1e-12 {
        return Some(0.0);
    }
    if neg_sum.abs() <= 1e-12 {
        return Some(100.0);
    }
    let ratio = pos_sum / neg_sum;
    finite_opt(Some(100.0 - (100.0 / (1.0 + ratio))))
}

fn compute_baseline_170(series: &CnSeries<'_>) -> Option<f64> {
    let ad = build_ad_line_rolling360(series)?;
    last_opt(&ad)
}

fn compute_baseline_171(series: &CnSeries<'_>) -> Option<f64> {
    let obv = build_obv_cumulative(series)?;
    finite_opt(Some(*obv.last()?))
}

fn compute_baseline_172(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 14 {
        return None;
    }
    let mut plus_dm = vec![0.0; n];
    let mut minus_dm = vec![0.0; n];
    let mut tr = vec![None; n];
    for i in 1..n {
        let high = series.high[i];
        let previous_high = series.high[i - 1];
        let low = series.low[i];
        let previous_low = series.low[i - 1];
        let previous_close = series.close[i - 1];
        if !high.is_finite()
            || !previous_high.is_finite()
            || !low.is_finite()
            || !previous_low.is_finite()
            || !previous_close.is_finite()
        {
            plus_dm[i] = f64::NAN;
            minus_dm[i] = f64::NAN;
            tr[i] = Some(f64::NAN);
            continue;
        }
        plus_dm[i] = (high - previous_high).max(0.0);
        minus_dm[i] = (previous_low - low).max(0.0);
        tr[i] = Some(
            (high - low)
                .max((high - previous_close).abs())
                .max((low - previous_close).abs()),
        );
    }
    let plus_ma = rolling_mean_series(&plus_dm, 14, 14).ok()?;
    let minus_ma = rolling_mean_series(&minus_dm, 14, 14).ok()?;
    let tr_ma = rolling_mean_series_opt(&tr, 14, 14).ok()?;
    let dx: Vec<Option<f64>> = (0..n)
        .map(|i| {
            let (p, m, t) = match (plus_ma[i], minus_ma[i], tr_ma[i]) {
                (Some(p), Some(m), Some(t)) if p.is_finite() && m.is_finite() && t.is_finite() => {
                    (p, m, t)
                }
                (Some(_), Some(_), Some(_)) => return Some(f64::NAN),
                _ => return None,
            };
            if t.abs() <= 1e-12 {
                return Some(0.0);
            }
            let plus_di = 100.0 * p / t;
            let minus_di = 100.0 * m / t;
            let den = plus_di + minus_di;
            if den.abs() <= 1e-12 {
                return Some(0.0);
            }
            finite_opt(Some((plus_di - minus_di).abs() / den * 100.0))
        })
        .collect();
    rolling_mean_last_opt_from_series(&dx, 14, 14)
        .ok()
        .flatten()
        .and_then(|v| finite_opt(Some(v)))
}

fn compute_baseline_173(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() <= 14 {
        return None;
    }
    finite_opt(Some(
        (series.close[series.close.len() - 1] - series.close[series.close.len() - 15]).abs(),
    ))
}

fn compute_baseline_174(series: &CnSeries<'_>) -> Option<f64> {
    let high = rolling_max_last(&series.close, 14)?;
    let low = rolling_min_last(&series.close, 14)?;
    finite_opt(Some(high - low))
}

fn compute_baseline_175(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.close, 14).ok().flatten()?;
    finite_opt(Some((series.close[series.close.len() - 1] - mean).abs()))
}

fn compute_baseline_177(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 125 {
        return None;
    }
    let hl: Vec<f64> = (0..n).map(|i| series.high[i] + series.low[i]).collect();
    let mut diff5 = vec![f64::NAN; n];
    for i in 5..n {
        diff5[i] = hl[i] - hl[i - 5];
    }
    let tail: Vec<f64> = diff5[n - 120..n].to_vec();
    if tail.iter().filter(|v| v.is_finite()).count() < 120 {
        return None;
    }
    let den = tail.iter().sum::<f64>() / 120.0;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.high[n - 1] - series.close[n - 1]) / den))
}

fn compute_baseline_178(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 33 {
        return None;
    }
    let hl: Vec<f64> = (0..n).map(|i| series.high[i] + series.low[i]).collect();
    let mut diff3 = vec![f64::NAN; n];
    for i in 3..n {
        diff3[i] = hl[i] - hl[i - 3];
    }
    let tail: Vec<f64> = diff3[n - 30..n].to_vec();
    if tail.iter().filter(|v| v.is_finite()).count() < 30 {
        return None;
    }
    let den = tail.iter().sum::<f64>() / 30.0;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.close[n - 1] - series.low[n - 1]) / den))
}

fn compute_baseline_179(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n == 0 {
        return None;
    }
    let close = series.close[n - 1];
    if close.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.high[n - 1] - series.low[n - 1]) / close))
}

fn compute_baseline_180(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.volume.len());
    if n < 30 {
        return None;
    }
    let mut cv = Vec::with_capacity(n);
    for i in 0..n {
        cv.push(series.close[i] * series.volume[i]);
    }
    let cv_mean = rolling_mean_last(&cv, 30).ok().flatten()?;
    let vol_mean = rolling_mean_last(&series.volume, 30).ok().flatten()?;
    if vol_mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(cv_mean / vol_mean))
}

fn compute_baseline_181(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.volume, 20).ok().flatten()?;
    if mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.volume[series.volume.len() - 1] / mean))
}

fn compute_baseline_182(series: &CnSeries<'_>) -> Option<f64> {
    let mean = rolling_mean_last(&series.volume, 14).ok().flatten()?;
    if mean.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.volume[series.volume.len() - 1] / mean))
}

fn compute_baseline_184(series: &CnSeries<'_>) -> Option<f64> {
    pct_change_last(&series.close, 14)
}

fn compute_baseline_185(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.close.len().min(series.open.len());
    if n < 14 {
        return None;
    }
    let diff: Vec<f64> = (0..n).map(|i| series.close[i] - series.open[i]).collect();
    rolling_mean_last(&diff, 14).ok().flatten()
}

fn compute_baseline_187(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .close
        .len()
        .min(series.open.len())
        .min(series.high.len())
        .min(series.low.len());
    if n == 0 {
        return None;
    }
    let den = series.high[n - 1] + series.low[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((series.close[n - 1] - series.open[n - 1]) / den))
}

fn compute_baseline_188(series: &CnSeries<'_>) -> Option<f64> {
    let std = sample_std_last(&series.close, 20, 20)?;
    finite_opt(Some(4.0 * std))
}

fn compute_baseline_189(series: &CnSeries<'_>) -> Option<f64> {
    let short = rolling_mean_series(&series.close, 20, 20).ok()?;
    let long = rolling_mean_series(&series.close, 90, 90).ok()?;
    let x: Vec<f64> = (0..series.close.len())
        .map(|i| match (short[i], long[i]) {
            (Some(s), Some(l)) => s - l,
            _ => f64::NAN,
        })
        .collect();
    let y: Vec<f64> = long.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
    corr_last_with_min_periods(&x, &y, 100, 30)
}

fn compute_baseline_190(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let mut range = Vec::with_capacity(n);
    for i in 0..n {
        range.push(series.high[i] - series.low[i]);
    }
    rolling_max_last(&range, 14)
}

fn compute_baseline_191(series: &CnSeries<'_>) -> Option<f64> {
    let n = series
        .high
        .len()
        .min(series.low.len())
        .min(series.close.len());
    if n < 16 {
        return None;
    }
    let mut fastk = vec![None; n];
    for i in 13..n {
        let mut low_min = f64::INFINITY;
        let mut high_max = f64::NEG_INFINITY;
        let mut valid = series.close[i].is_finite();
        for j in i + 1 - 14..i + 1 {
            let low = series.low[j];
            let high = series.high[j];
            if !low.is_finite() || !high.is_finite() {
                valid = false;
                continue;
            }
            low_min = f64::min(low_min, low);
            high_max = f64::max(high_max, high);
        }
        if !valid {
            fastk[i] = Some(f64::NAN);
            continue;
        }
        let den = high_max - low_min;
        if den.abs() > 1e-12 {
            fastk[i] = finite_opt(Some(100.0 * (series.close[i] - low_min) / den));
        }
    }
    let slowk = rolling_mean_series_opt(&fastk, 3, 3).ok()?;
    let slowd = rolling_mean_series_opt(&slowk, 3, 3).ok()?;
    let k = last_opt(&slowk)?;
    let d = last_opt(&slowd)?;
    finite_opt(Some(k - d))
}

fn compute_baseline_192(series: &CnSeries<'_>) -> Option<f64> {
    rolling_position_last(&series.close, 14)
}

fn compute_baseline_193(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.buy_amount.len().min(series.sell_amount.len());
    if n == 0 {
        return None;
    }
    let sell = series.sell_amount[n - 1];
    if sell.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(series.buy_amount[n - 1] / sell))
}

fn compute_baseline_194(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.buy_amount.len().min(series.sell_amount.len());
    if n == 0 {
        return None;
    }
    let num = series.buy_amount[n - 1] - series.sell_amount[n - 1];
    let den = series.buy_amount[n - 1] + series.sell_amount[n - 1];
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(num / den))
}

fn compute_baseline_195(series: &CnSeries<'_>) -> Option<f64> {
    let min_val = rolling_min_last(&series.close, 26)?;
    let max_val = rolling_max_last(&series.close, 26)?;
    let den = max_val - min_val;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(
        2.0 * ((series.close[series.close.len() - 1] - min_val) / den) - 1.0,
    ))
}

fn compute_baseline_196(series: &CnSeries<'_>) -> Option<f64> {
    let periods = 10usize;
    if series.large_buy.len() <= periods {
        return None;
    }
    let curr = *series.large_buy.last()?;
    let prev = series.large_buy[series.large_buy.len() - 1 - periods];
    if !curr.is_finite() || !prev.is_finite() {
        return None;
    }
    if prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some((curr - prev) / prev))
}

fn compute_baseline_198(series: &CnSeries<'_>) -> Option<f64> {
    let n = series.high.len().min(series.low.len());
    if n < 14 {
        return None;
    }
    let high_tail: Vec<f64> = (n - 14..n).map(|i| series.high[i]).collect();
    let low_tail: Vec<f64> = (n - 14..n).map(|i| series.low[i]).collect();
    let aroon_up = (first_argmax(&high_tail)? + 1) as f64 / 14.0 * 100.0;
    let aroon_down = (first_argmin(&low_tail)? + 1) as f64 / 14.0 * 100.0;
    finite_opt(Some(aroon_up - aroon_down))
}

fn compute_baseline_199(series: &CnSeries<'_>) -> Option<f64> {
    let low = rolling_min_last(&series.close, 30)?;
    let high = rolling_max_last(&series.close, 30)?;
    let den = high - low;
    if den.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(
        (series.close[series.close.len() - 1] - low) / den * 100.0,
    ))
}

fn compute_baseline_200(series: &CnSeries<'_>) -> Option<f64> {
    if series.close.len() < 30 {
        return None;
    }
    let mean = rolling_mean_last(&series.close, 30).ok().flatten()?;
    let std = sample_std_last(&series.close, 30, 30)?;
    let close = series.close[series.close.len() - 1];
    let kama = mean + (close - mean) * (std / (std + 1.0));
    finite_opt(Some(close - kama))
}
