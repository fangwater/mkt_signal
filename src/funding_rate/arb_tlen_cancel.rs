pub struct ArbCancelGroupEval {
    pub symbol: String,
    pub threshold: f64,
    pub tick_indices: Vec<i64>,
    pub tlens: Vec<f64>,
    pub compared_preview: Vec<String>,
    pub min_tlen: f64,
    pub max_tlen: f64,
    pub matched_preview: Vec<String>,
    pub group_cancel_sent: usize,
}

pub fn build_group_eval_preview(
    group: &crate::signal::arb_signal::ArbCancelCandidateSymbolGroup,
    threshold: f64,
    tlens: &[f64],
) -> ArbCancelGroupEval {
    let symbol = group.get_symbol().to_uppercase();
    let tick_indices = group
        .items
        .iter()
        .map(|item| item.price_qv.get_count())
        .collect::<Vec<_>>();
    let compared_preview = group
        .items
        .iter()
        .zip(tlens.iter().copied())
        .take(12)
        .map(|(item, tlen)| {
            format!(
                "{}@{}:{:.4}{}",
                item.strategy_id,
                item.price_qv.get_count(),
                tlen,
                if tlen < threshold { "<hit" } else { ">=skip" }
            )
        })
        .collect::<Vec<_>>();
    let (min_tlen, max_tlen) = tlens.iter().copied().fold(
        (f64::INFINITY, f64::NEG_INFINITY),
        |(min_v, max_v), value| (min_v.min(value), max_v.max(value)),
    );
    ArbCancelGroupEval {
        symbol,
        threshold,
        tick_indices,
        tlens: tlens.to_vec(),
        compared_preview,
        min_tlen,
        max_tlen,
        matched_preview: Vec::new(),
        group_cancel_sent: 0,
    }
}

pub fn push_match_preview(
    matched_preview: &mut Vec<String>,
    strategy_id: i32,
    price_count: i64,
    tlen: f64,
    threshold: f64,
) {
    if matched_preview.len() < 12 {
        matched_preview.push(format!(
            "{}@{}:{:.4}<{:.4}",
            strategy_id, price_count, tlen, threshold
        ));
    }
}
