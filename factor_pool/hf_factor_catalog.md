# 高频量价因子整理

来源：`factor_pool/final_factor_pool_update20250622.ipynb` 中 `HF_*` 因子定义（cell 25/27/29/31/33/35/37/39），以及最终入池列表（cell 40）。

## 统一约定

- 输入 `df`：`pandas.DataFrame`。
- 输出：`numpy.ndarray`，长度与输入行数一致；由于 `rolling` / `pct_change`，前若干位置通常为 `NaN`。
- 以下“编号”为本整理文档内编号（非原 notebook 自带编号）。

## 因子清单（编号 / 计算方式 / 输入输出）

| 编号 | 因子名 | 计算方式 | 输入 | 输出 |
|---|---|---|---|---|
| HF-001 | `HF_vol_std` | `df['close'].pct_change().rolling(window).std().values` | 列：`close`；参数：`window=360` | 收益率滚动标准差序列（`ndarray`） |
| HF-002 | `HF_vol_abs_pct_by_vol` | `(df['close'].pct_change().abs() / df['volume']).rolling(window).mean().values` | 列：`close`,`volume`；参数：`window=5` | 绝对收益/成交量的滚动均值（`ndarray`） |
| HF-003 | `HF_highlow_range` | `(df['high'] - df['low']).rolling(window).mean().values` | 列：`high`,`low`；参数：`window=360` | 高低价差滚动均值（`ndarray`） |
| HF-004 | `HF_spread_return` | `df['close'].pct_change(return_period).rolling(ma_window).mean().values` | 列：`close`；参数：`return_period=18`,`ma_window=30` | 多周期收益率的滚动均值（`ndarray`） |
| HF-005 | `HF_volume_mean` | `df['volume'].rolling(window).mean().values` | 列：`volume`；参数：`window=360` | 成交量滚动均值（`ndarray`） |
| HF-006 | `HF_count_mean` | `df['count'].rolling(window).mean().values` | 列：`count`；参数：`window=360` | 成交笔数滚动均值（`ndarray`） |
| HF-007 | `HF_active_rate_std` | `ratio=(buy_amount短窗和)/(sell_amount短窗和)`；`ratio.rolling(long).std().values` | 列：`buy_amount`,`sell_amount`；参数：`short=6`,`long=360` | 主动买卖比率滚动标准差（`ndarray`） |
| HF-008 | `HF_active_rate_rank` | `ratio=(buy_amount短窗和)/(sell_amount短窗和)`；`ratio.rolling(long).rank().values` | 列：`buy_amount`,`sell_amount`；参数：`short=6`,`long=360` | 主动买卖比率滚动排序值（`ndarray`） |
| HF-009 | `HF_netbuy_std` | `df['net_buy_pct'].rolling(window).std().values` | 列：`net_buy_pct`；参数：`window=30` | 净买入占比滚动标准差（`ndarray`） |
| HF-010 | `HF_netbuy_rank` | `df['net_buy'].rolling(window).rank().values` | 列：`net_buy`；参数：`window=360` | 净买入滚动排序值（`ndarray`） |
| HF-011 | `HF_ab_rate` | `((df['buy_amount'] - df['sell_amount']) / df['amount']).rolling(window).mean().values` | 列：`buy_amount`,`sell_amount`,`amount`；参数：`window=6` | 主动买卖差额占比滚动均值（`ndarray`） |
| HF-012 | `HF_large_order_std` | `df['large_order'].rolling(window).std().values` | 列：`large_order`；参数：`window=360` | 大单金额滚动标准差（`ndarray`） |
| HF-013 | `HF_large_order_rate_std` | `(df['large_order'] / df['amount']).rolling(window).std().values` | 列：`large_order`,`amount`；参数：`window=10` | 大单占比滚动标准差（`ndarray`） |
| HF-014 | `HF_medium_order_rate_std` | `(df['medium_order'] / df['amount']).rolling(window).std().values` | 列：`medium_order`,`amount`；参数：`window=10` | 中单占比滚动标准差（`ndarray`） |
| HF-015 | `HF_small_order_rate_std` | `(df['small_order'] / df['amount']).rolling(window).std().values` | 列：`small_order`,`amount`；参数：`window=10` | 小单占比滚动标准差（`ndarray`） |
| HF-016 | `HF_small_order_rate_mean` | `(df['small_order'] / df['amount']).rolling(window).mean().values` | 列：`small_order`,`amount`；参数：`window=6` | 小单占比滚动均值（`ndarray`） |
| HF-017 | `HF_vwap_diff_std` | `(df['buy_avg_price'] - df['sell_avg_price']).rolling(window).std().values` | 列：`buy_avg_price`,`sell_avg_price`；参数：`window=300` | 买卖均价差滚动标准差（`ndarray`） |
| HF-018 | `HF_price_volume_corr` | `df['close'].rolling(window).corr(df['volume']).values` | 列：`close`,`volume`；参数：`window=360` | 价格与成交量滚动相关系数（`ndarray`） |
| HF-019 | `HF_orderbook_buy_amount` | `sum(df[f'bid{i}p'] * df[f'bid{i}v'] for i in range(25)).values` | 列：`bid0p~bid24p`,`bid0v~bid24v` | 买盘 25 档金额总和（`ndarray`） |
| HF-020 | `HF_orderbook_sell_amount` | `sum(df[f'ask{i}p'] * df[f'ask{i}v'] for i in range(25)).values` | 列：`ask0p~ask24p`,`ask0v~ask24v` | 卖盘 25 档金额总和（`ndarray`） |
| HF-021 | `HF_orderbook_skew` | `ratio=(买盘25档金额总和)/(卖盘25档金额总和)`；`ratio.rolling(window).skew().values` | 列：`bid/ask` 25 档价量；参数：`window=360` | 买卖盘金额比滚动偏度（`ndarray`） |
| HF-022 | `HF_vol_volume_combined` | `vol=close收益率滚动std`；`volu=volume滚动mean`；`(vol * volu).values` | 列：`close`,`volume`；参数：`vol_window=360`,`volu_window=360` | 波动率×成交量复合因子（`ndarray`） |

## 备注

- 上述内容保持与 notebook 现有代码一致，未改动原计算逻辑。
- 若需要，我可以继续补一版“经济含义/预期方向/风险点”说明，便于做因子评审。
