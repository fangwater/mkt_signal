#[derive(Debug, Clone, Copy)]
pub struct HedgeOffsetScaleInput {
    pub net_qty_base: f64,
    pub hedge_bid0: f64,
    pub hedge_ask0: f64,
    pub symbol_exposure_u: f64,
    pub final_offset_min: f64,
    pub final_offset_max: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct HedgeOffsetScaleResult {
    pub inv_notional: f64,
    pub scale: f64,
    pub offset_min_scaled: f64,
    pub offset_max_scaled: f64,
}

pub fn scale_offsets_by_inventory(input: HedgeOffsetScaleInput) -> HedgeOffsetScaleResult {
    let eps = 1e-9;
    let mid = ((input.hedge_bid0 + input.hedge_ask0) * 0.5).max(0.0);
    if mid <= 0.0 {
        return HedgeOffsetScaleResult {
            inv_notional: 0.0,
            scale: 1.0,
            offset_min_scaled: input.final_offset_min.max(0.0),
            offset_max_scaled: input.final_offset_max.max(0.0),
        };
    }

    let inv_notional = input.net_qty_base.abs() * mid;
    let x = inv_notional / (input.symbol_exposure_u.max(0.0) + eps);
    let scale = 1.0 / (1.0 + x);

    let limit_lower = input.final_offset_min.max(0.0);
    let limit_upper = input.final_offset_max.max(limit_lower);

    let delta_min = mid * input.final_offset_min;
    let delta_max = mid * input.final_offset_max;
    let delta_min_2 = delta_min * scale;
    let delta_max_2 = delta_max * scale;

    let offset_min_scaled = (delta_min_2 / mid).clamp(limit_lower, limit_upper);
    let raw_offset_max_scaled = (delta_max_2 / mid).max(offset_min_scaled);
    let offset_max_scaled = raw_offset_max_scaled.clamp(limit_lower, limit_upper);

    HedgeOffsetScaleResult {
        inv_notional,
        scale,
        offset_min_scaled,
        offset_max_scaled,
    }
}
