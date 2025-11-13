把ArbDirection，CompareOpVenuePair  SymbolPair ThresholdKey等和价差无关的定义，提取到funding rate dir下面的common
  rs。然后，我需要一个funding rate factor。    /// 阈值表: (venue1, symbol1, venue2, symbol2) -> ThresholdConfig
      thresholds: RefCell<HashMap<ThresholdKey, ThresholdConfig>>,
  同样要存储一个阈值表。ThresholdConfig的定义不同。


具体来说，资费会产生以下信号
funding rate ma，即60条的均值


# 4h 阈值
"fr_4h_open_upper_threshold": "4h开仓上阈",
"fr_4h_open_lower_threshold": "4h开仓下阈",
"fr_4h_close_lower_threshold": "4h平仓下阈",
"fr_4h_close_upper_threshold": "4h平仓上阈",
# 8h 阈值
"fr_8h_open_upper_threshold": "8h开仓上阈",
"fr_8h_open_lower_threshold": "8h开仓下阈",
"fr_8h_close_lower_threshold": "8h平仓下阈",
"fr_8h_close_upper_threshold": "8h平仓上阈",
