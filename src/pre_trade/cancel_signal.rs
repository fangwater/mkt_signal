  use crate::signal::common::TradingVenue;

  #[derive(Debug, Clone)]
  pub struct TradingLeg {
      pub symbol: String,
      pub venue: TradingVenue,
      pub bid0: f64,
      pub ask0: f64,
  }

  #[derive(Debug, Clone)]
  pub struct ArbCancelCtx {
      pub opening_leg: TradingLeg,   // 开仓腿
      pub hedging_leg: TradingLeg,   // 对冲腿
      pub cancel_threshold: f64,
      pub trigger_ts: i64,
  }
