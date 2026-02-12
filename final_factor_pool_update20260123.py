#!/usr/bin/env python3
"""
因子池 - 更新版本 20260123
===============================

数据格式说明：
-----------
1. OrderBook数据（20档）：
   - bid{i}p: 买盘第i档价格 (i=0~19)
   - bid{i}v: 买盘第i档数量 (i=0~19)
   - ask{i}p: 卖盘第i档价格 (i=0~19)
   - ask{i}v: 卖盘第i档数量 (i=0~19)

2. Trades数据：
   - open, high, low, close: OHLC价格
   - volume: 成交量
   - amount: 成交额
   - avg_amount: 平均成交额
   - count: 成交笔数
   - buy_count, sell_count: 买卖笔数
   - buy_amount, sell_amount: 买卖成交额
   - buy_volume, sell_volume: 买卖成交量
   - vwap, buy_vwap, sell_vwap: 成交均价
   - net_buy_amount, net_buy_volume: 净买入
   - net_buy_pct: 净买入比例
   - large/medium/small_order: 大中小单金额
   - large/medium/small_buy/sell: 大中小单买卖金额
   - net_buy_large/medium/small: 大中小单净买入

函数规范：
---------
- 输入：pd.DataFrame，包含上述列名
- 输出：np.ndarray，形状为 (N,)，一维数组
- 缺失值：返回 np.nan

作者：guoqi
日期：2026-01-23
"""

import numpy as np
import pandas as pd
from typing import Union


# ============================================================================
# OrderBook因子 - 价量平衡类 (Factor 001-030)
# ============================================================================

def factor_001(df: pd.DataFrame) -> np.ndarray:
    """
    因子001：20档买盘量价加权平均价格的变化率

    计算逻辑：
    1. 计算买盘总量 = sum(bid{i}v)
    2. 计算买盘总值 = sum(bid{i}p * bid{i}v)
    3. VWAP = 总值 / 总量
    4. 返回 diff(VWAP).pct_change()

    参数：
        df: 包含bid{i}p, bid{i}v列的DataFrame

    返回：
        np.ndarray: VWAP变化率
    """
    total_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_value = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(20)], axis=0)
    vwap = np.where(total_volume != 0, total_value / total_volume, np.nan)
    result = pd.Series(vwap).diff().pct_change().values
    return result


def factor_002(df: pd.DataFrame) -> np.ndarray:
    """
    因子002：卖盘总量与买盘总量的比值

    计算逻辑：
    1. 卖盘总量 = sum(ask{i}v)
    2. 买盘总量 = sum(bid{i}v)
    3. 返回 卖盘总量 / 买盘总量

    指标含义：
    - > 1: 卖盘力量强于买盘
    - < 1: 买盘力量强于卖盘

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 卖买盘量比值
    """
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    return np.where(total_bid_volume != 0, total_ask_volume / total_bid_volume, np.nan)


def factor_003(df: pd.DataFrame) -> np.ndarray:
    """
    因子003：前15档买卖价格差异归一化

    计算逻辑：
    1. 买盘总价 = sum(bid{i}p, i=0~14)
    2. 卖盘总价 = sum(ask{i}p, i=0~14)
    3. 价格差 = 买盘总价 - 卖盘总价
    4. 返回 价格差 / (买盘总价 + 卖盘总价)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 归一化价格差异
    """
    total_bid_price = np.sum([df[f'bid{i}p'].values for i in range(15)], axis=0)
    total_ask_price = np.sum([df[f'ask{i}p'].values for i in range(15)], axis=0)
    total_price = total_bid_price + total_ask_price
    price_diff = total_bid_price - total_ask_price
    return np.where(total_price != 0, price_diff / total_price, np.nan)


def factor_004(df: pd.DataFrame) -> np.ndarray:
    """
    因子004：中间价与买入均价的偏离

    计算逻辑：
    1. mid_price = (bid0p + ask0p) / 2
    2. 返回 mid_price - buy_vwap

    指标含义：
    - > 0: 中间价高于买入均价，可能有上涨趋势
    - < 0: 中间价低于买入均价，可能有下跌趋势

    参数：
        df: 包含bid0p, ask0p, buy_vwap的DataFrame

    返回：
        np.ndarray: 价格偏离度
    """
    mid_price = (df['bid0p'] + df['ask0p']) / 2
    result = mid_price - df['buy_vwap']
    return result.values


def factor_005(df: pd.DataFrame) -> np.ndarray:
    """
    因子005：中间价一阶差分

    计算逻辑：
    1. mid_price = (bid0p + ask0p) / 2
    2. 返回 diff(mid_price)

    指标含义：
    - > 0: 价格上涨
    - < 0: 价格下跌

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 中间价变化
    """
    mid_price = (df['bid0p'].values + df['ask0p'].values) / 2
    mid_price_diff = np.diff(mid_price, prepend=np.nan)
    return mid_price_diff


def factor_006(df: pd.DataFrame) -> np.ndarray:
    """
    因子006：前5档买卖力量对比

    计算逻辑：
    1. ask_strength = sum(ask{i}p * ask{i}v, i=0~4)
    2. bid_strength = sum(bid{i}p * bid{i}v, i=0~4)
    3. 返回 (bid_strength - ask_strength) / (bid_strength + ask_strength)

    指标含义：
    - 接近 +1: 买盘力量占绝对优势
    - 接近 -1: 卖盘力量占绝对优势
    - 接近 0: 买卖力量均衡

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 买卖力量比
    """
    ask_strength = np.sum([df[f'ask{i}p'].values * df[f'ask{i}v'].values for i in range(5)], axis=0)
    bid_strength = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(5)], axis=0)
    strength_ratio = np.where(
        (bid_strength + ask_strength) != 0,
        (bid_strength - ask_strength) / (bid_strength + ask_strength),
        np.nan
    )
    return strength_ratio


def factor_007(df: pd.DataFrame) -> np.ndarray:
    """
    因子007：买卖盘总量差

    计算逻辑：
    1. total_bid_volume = sum(bid{i}v, i=0~19)
    2. total_ask_volume = sum(ask{i}v, i=0~19)
    3. 返回 total_bid_volume - total_ask_volume

    指标含义：
    - > 0: 买盘挂单多
    - < 0: 卖盘挂单多

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 买卖量差
    """
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    volume_diff = total_bid_volume - total_ask_volume
    return volume_diff


def factor_008(df: pd.DataFrame) -> np.ndarray:
    """
    因子008：前10档买卖价差的标准差

    计算逻辑：
    1. 计算前10档的加权价格差: bid{i}p*bid{i}v - ask{i}p*ask{i}v
    2. 返回这些价格差的标准差

    指标含义：
    - 值越大：价格分布越分散，市场不确定性高
    - 值越小：价格分布越集中，市场稳定

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 价格差标准差
    """
    bid_prices = np.array([(df[f'bid{i}p'] * df[f'bid{i}v']).values for i in range(10)])
    ask_prices = np.array([(df[f'ask{i}p'] * df[f'ask{i}v']).values for i in range(10)])
    price_diff = bid_prices - ask_prices
    price_diff_std = np.std(price_diff, axis=0)
    return price_diff_std


def factor_009(df: pd.DataFrame) -> np.ndarray:
    """
    因子009：最优买卖价的对数价差

    计算逻辑：
    1. 返回 log(ask0p) - log(bid0p)

    指标含义：
    - 对数价差，反映买卖价差的相对大小
    - 值越大：spread越大，流动性越差

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 对数价差
    """
    log_spread = np.log(df['ask0p'].values) - np.log(df['bid0p'].values)
    return log_spread


def factor_010(df: pd.DataFrame) -> np.ndarray:
    """
    因子010：买盘量价加权平均价格的变化

    计算逻辑：
    1. 计算买盘VWAP
    2. 返回 diff(VWAP)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 买盘VWAP变化
    """
    total_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_value = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(20)], axis=0)
    vwap = np.where(total_volume != 0, total_value / total_volume, np.nan)
    result = pd.Series(vwap).diff().values
    return result


def factor_011(df: pd.DataFrame) -> np.ndarray:
    """
    因子011：卖盘量价加权平均价格的变化

    计算逻辑：
    1. 计算卖盘VWAP
    2. 返回 diff(VWAP)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 卖盘VWAP变化
    """
    total_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_value = np.sum([df[f'ask{i}p'].values * df[f'ask{i}v'].values for i in range(20)], axis=0)
    vwap = np.where(total_volume != 0, total_value / total_volume, np.nan)
    result = pd.Series(vwap).diff().values
    return result


def factor_012(df: pd.DataFrame) -> np.ndarray:
    """
    因子012：买卖盘总价值差的归一化

    计算逻辑：
    1. bid_value = sum(bid{i}p * bid{i}v)
    2. ask_value = sum(ask{i}p * ask{i}v)
    3. 返回 (bid_value - ask_value) / (bid_value + ask_value)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 归一化价值差
    """
    bid_value = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(20)], axis=0)
    ask_value = np.sum([df[f'ask{i}p'].values * df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_value = bid_value + ask_value
    value_diff = bid_value - ask_value
    return np.where(total_value != 0, value_diff / total_value, np.nan)


def factor_013(df: pd.DataFrame) -> np.ndarray:
    """
    因子013：前5档买盘深度与总深度的比值

    计算逻辑：
    1. top5_bid = sum(bid{i}v, i=0~4)
    2. total_bid = sum(bid{i}v, i=0~19)
    3. 返回 top5_bid / total_bid

    指标含义：
    - 值越大：买盘集中在前5档，深度分布不均
    - 值越小：买盘分散，深度分布均匀

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 前5档买盘占比
    """
    top5_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(5)], axis=0)
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    return np.where(total_bid_volume != 0, top5_bid_volume / total_bid_volume, np.nan)


def factor_014(df: pd.DataFrame) -> np.ndarray:
    """
    因子014：前5档卖盘深度与总深度的比值

    计算逻辑：
    1. top5_ask = sum(ask{i}v, i=0~4)
    2. total_ask = sum(ask{i}v, i=0~19)
    3. 返回 top5_ask / total_ask

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 前5档卖盘占比
    """
    top5_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(5)], axis=0)
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    return np.where(total_ask_volume != 0, top5_ask_volume / total_ask_volume, np.nan)


def factor_015(df: pd.DataFrame) -> np.ndarray:
    """
    因子015：买盘价格加权深度

    计算逻辑：
    1. 每档深度乘以价格权重: bid{i}v * bid{i}p / sum(bid{j}p)
    2. 返回加权深度之和

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 价格加权深度
    """
    total_bid_price = np.sum([df[f'bid{i}p'].values for i in range(20)], axis=0)
    weighted_depth = np.sum([
        df[f'bid{i}v'].values * df[f'bid{i}p'].values / (total_bid_price + 1e-6)
        for i in range(20)
    ], axis=0)
    return weighted_depth


def factor_016(df: pd.DataFrame) -> np.ndarray:
    """
    因子016：卖盘价格加权深度

    计算逻辑：
    1. 每档深度乘以价格权重: ask{i}v * ask{i}p / sum(ask{j}p)
    2. 返回加权深度之和

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 价格加权深度
    """
    total_ask_price = np.sum([df[f'ask{i}p'].values for i in range(20)], axis=0)
    weighted_depth = np.sum([
        df[f'ask{i}v'].values * df[f'ask{i}p'].values / (total_ask_price + 1e-6)
        for i in range(20)
    ], axis=0)
    return weighted_depth


def factor_017(df: pd.DataFrame) -> np.ndarray:
    """
    因子017：买盘深度不平衡指标

    计算逻辑：
    1. 计算每档深度与平均深度的偏离
    2. 返回偏离度的标准差

    指标含义：
    - 值越大：深度分布越不平衡
    - 值越小：深度分布越均匀

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 深度不平衡度
    """
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    mean_volume = np.mean(bid_volumes, axis=0)
    imbalance = np.std(bid_volumes / (mean_volume + 1e-6), axis=0)
    return imbalance


def factor_018(df: pd.DataFrame) -> np.ndarray:
    """
    因子018：卖盘深度不平衡指标

    计算逻辑：
    1. 计算每档深度与平均深度的偏离
    2. 返回偏离度的标准差

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 深度不平衡度
    """
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    mean_volume = np.mean(ask_volumes, axis=0)
    imbalance = np.std(ask_volumes / (mean_volume + 1e-6), axis=0)
    return imbalance


def factor_019(df: pd.DataFrame) -> np.ndarray:
    """
    因子019：买盘加权平均价格的滚动变化

    计算逻辑：
    1. 计算买盘VWAP
    2. 返回5期滚动均值的变化

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 滚动VWAP变化
    """
    total_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_value = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(20)], axis=0)
    vwap = np.where(total_volume != 0, total_value / total_volume, np.nan)
    rolling_vwap = pd.Series(vwap).rolling(window=5, min_periods=1).mean()
    result = rolling_vwap.diff().values
    return result


def factor_020(df: pd.DataFrame) -> np.ndarray:
    """
    因子020：卖盘加权平均价格的滚动变化

    计算逻辑：
    1. 计算卖盘VWAP
    2. 返回5期滚动均值的变化

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 滚动VWAP变化
    """
    total_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_value = np.sum([df[f'ask{i}p'].values * df[f'ask{i}v'].values for i in range(20)], axis=0)
    vwap = np.where(total_volume != 0, total_value / total_volume, np.nan)
    rolling_vwap = pd.Series(vwap).rolling(window=5, min_periods=1).mean()
    result = rolling_vwap.diff().values
    return result


def factor_021(df: pd.DataFrame) -> np.ndarray:
    """
    因子021：买卖盘量的波动率比

    计算逻辑：
    1. bid_vol_std = std(total_bid_volume, window=10)
    2. ask_vol_std = std(total_ask_volume, window=10)
    3. 返回 bid_vol_std / ask_vol_std

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 波动率比
    """
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)

    bid_vol_std = pd.Series(total_bid_volume).rolling(window=10, min_periods=1).std()
    ask_vol_std = pd.Series(total_ask_volume).rolling(window=10, min_periods=1).std()

    result = np.where(ask_vol_std != 0, bid_vol_std / ask_vol_std, np.nan)
    return result


def factor_022(df: pd.DataFrame) -> np.ndarray:
    """
    因子022：买卖价差的相对变化

    计算逻辑：
    1. spread = ask0p - bid0p
    2. mid_price = (ask0p + bid0p) / 2
    3. relative_spread = spread / mid_price
    4. 返回 diff(relative_spread)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 相对价差变化
    """
    spread = df['ask0p'].values - df['bid0p'].values
    mid_price = (df['ask0p'].values + df['bid0p'].values) / 2
    relative_spread = np.where(mid_price != 0, spread / mid_price, np.nan)
    result = np.diff(relative_spread, prepend=np.nan)
    return result


def factor_023(df: pd.DataFrame) -> np.ndarray:
    """
    因子023：前10档买盘总量的动量

    计算逻辑：
    1. top10_bid = sum(bid{i}v, i=0~9)
    2. 返回 top10_bid - rolling_mean(top10_bid, 5)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 买盘量动量
    """
    top10_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(10)], axis=0)
    rolling_mean = pd.Series(top10_bid_volume).rolling(window=5, min_periods=1).mean()
    momentum = top10_bid_volume - rolling_mean.values
    return momentum


def factor_024(df: pd.DataFrame) -> np.ndarray:
    """
    因子024：前10档卖盘总量的动量

    计算逻辑：
    1. top10_ask = sum(ask{i}v, i=0~9)
    2. 返回 top10_ask - rolling_mean(top10_ask, 5)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 卖盘量动量
    """
    top10_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(10)], axis=0)
    rolling_mean = pd.Series(top10_ask_volume).rolling(window=5, min_periods=1).mean()
    momentum = top10_ask_volume - rolling_mean.values
    return momentum


def factor_025(df: pd.DataFrame) -> np.ndarray:
    """
    因子025：买盘前3档与后17档的量比

    计算逻辑：
    1. top3_bid = sum(bid{i}v, i=0~2)
    2. bottom17_bid = sum(bid{i}v, i=3~19)
    3. 返回 top3_bid / bottom17_bid

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 前后档位比
    """
    top3_bid = np.sum([df[f'bid{i}v'].values for i in range(3)], axis=0)
    bottom17_bid = np.sum([df[f'bid{i}v'].values for i in range(3, 20)], axis=0)
    return np.where(bottom17_bid != 0, top3_bid / bottom17_bid, np.nan)


def factor_026(df: pd.DataFrame) -> np.ndarray:
    """
    因子026：卖盘前3档与后17档的量比

    计算逻辑：
    1. top3_ask = sum(ask{i}v, i=0~2)
    2. bottom17_ask = sum(ask{i}v, i=3~19)
    3. 返回 top3_ask / bottom17_ask

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 前后档位比
    """
    top3_ask = np.sum([df[f'ask{i}v'].values for i in range(3)], axis=0)
    bottom17_ask = np.sum([df[f'ask{i}v'].values for i in range(3, 20)], axis=0)
    return np.where(bottom17_ask != 0, top3_ask / bottom17_ask, np.nan)


def factor_027(df: pd.DataFrame) -> np.ndarray:
    """
    因子027：买盘深度偏度

    计算逻辑：
    1. 计算20档买盘量的偏度
    2. 偏度 = E[(X-μ)^3] / σ^3

    指标含义：
    - > 0: 正偏，深度分布右偏
    - < 0: 负偏，深度分布左偏

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 买盘深度偏度
    """
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    bid_skew = pd.DataFrame(bid_volumes.T).skew(axis=1).values
    return bid_skew


def factor_028(df: pd.DataFrame) -> np.ndarray:
    """
    因子028：卖盘深度偏度

    计算逻辑：
    1. 计算20档卖盘量的偏度

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 卖盘深度偏度
    """
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    ask_skew = pd.DataFrame(ask_volumes.T).skew(axis=1).values
    return ask_skew


def factor_029(df: pd.DataFrame) -> np.ndarray:
    """
    因子029：买盘深度峰度

    计算逻辑：
    1. 计算20档买盘量的峰度
    2. 峰度 = E[(X-μ)^4] / σ^4 - 3

    指标含义：
    - > 0: 尖峰分布
    - < 0: 平坦分布

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 买盘深度峰度
    """
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    bid_kurt = pd.DataFrame(bid_volumes.T).kurt(axis=1).values
    return bid_kurt


def factor_030(df: pd.DataFrame) -> np.ndarray:
    """
    因子030：卖盘深度峰度

    计算逻辑：
    1. 计算20档卖盘量的峰度

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 卖盘深度峰度
    """
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    ask_kurt = pd.DataFrame(ask_volumes.T).kurt(axis=1).values
    return ask_kurt


# ============================================================================
# OrderBook因子 - 补充部分 (Factor 031-177)
# 自动生成于 2026-01-23
# ============================================================================

# Factor 031: Max bid price for the first 5 levels
def factor_031(df: pd.DataFrame) -> np.ndarray:
    mid_price = (df['bid0p'].values + df['ask0p'].values) / 2
    bid_prices = np.mean(np.array([df[f'bid{i}p'].values for i in range(15)]),axis=0)
    result = pd.Series(bid_prices / mid_price).rolling(500).rank().values
    return result



# Factor 032: Min ask price for the first 5 levels
def factor_032(df: pd.DataFrame) -> np.ndarray:
    ask_prices5 = np.sum(np.array([(df[f'ask{i}p'] * df[f'ask{i}v']).values for i in range(5)]),axis=0) / np.sum(np.array([df[f'ask{i}v'].values for i in range(5)]),axis=0)
    ask_prices20 = np.sum(np.array([(df[f'ask{i}p'] * df[f'ask{i}v']).values for i in range(20)]),axis=0) / np.sum(np.array([df[f'ask{i}v'].values for i in range(20)]),axis=0)
    result = pd.Series(ask_prices5 - ask_prices20).rolling(300).mean().values
    return result



# Factor 033: Sum of bid volumes for the first 10 levels
def factor_033(df: pd.DataFrame) -> np.ndarray:
    var1 = df[[f'bid{i}v' for i in range(10)]].sum(axis=1)
    result = (var1 / var1.rolling(30).mean()).values
    return result



# Factor 034: Sum of ask volumes for the first 10 levels
def factor_034(df: pd.DataFrame) -> np.ndarray:
    var1 = df[[f'ask{i}v' for i in range(10)]].sum(axis=1)
    result = (var1 / var1.rolling(30).mean()).values
    return result



# Factor 035: Mean bid price for the first 15 levels
def factor_035(df: pd.DataFrame) -> np.ndarray:
    bid_prices5 = np.sum(np.array([(df[f'bid{i}p'] * df[f'bid{i}v']).values for i in range(5)]),axis=0) / np.sum(np.array([df[f'bid{i}v'].values for i in range(5)]),axis=0)
    ask_prices5 = np.sum(np.array([(df[f'ask{i}p'] * df[f'ask{i}v']).values for i in range(5)]),axis=0) / np.sum(np.array([df[f'ask{i}v'].values for i in range(5)]),axis=0)
    result = bid_prices5 / ask_prices5
    return result



# Factor 036: Mean ask price for the first 15 levels
def factor_036(df: pd.DataFrame) -> np.ndarray:
    avg_price = df[[f'ask{i}p' for i in range(15)]].mean(axis=1)
    result = ((avg_price - avg_price.rolling(300).mean()) / avg_price.rolling(300).std()).values
    return result



# Factor 037: Sum of squared differences of bid prices for the first 10 levels
def factor_037(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(5)])
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(5)])
    bid_vwap_5 = np.sum(bid_prices * bid_volumes, axis=0) / np.sum(bid_volumes, axis=0)
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(15)])
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(15)])
    bid_vwap_15 = np.sum(bid_prices * bid_volumes, axis=0) / np.sum(bid_volumes, axis=0)
    return bid_vwap_5 - bid_vwap_15



# Factor 038: Sum of squared differences of ask prices for the first 10 levels
def factor_038(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(5)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(5)])
    ask_vwap_5 = np.sum(ask_prices * ask_volumes, axis=0) / np.sum(ask_volumes, axis=0)
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(15)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(15)])
    ask_vwap_15 = np.sum(ask_prices * ask_volumes, axis=0) / np.sum(ask_volumes, axis=0)
    return ask_vwap_5 - ask_vwap_15



# Factor 039: Total volume difference for the first 25 levels
def factor_039(df: pd.DataFrame) -> np.ndarray:
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    return total_bid_volume - total_ask_volume



# Factor 040: Volatility of bid volumes for the first 10 levels
def factor_040(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    return np.std(bid_volumes, axis=0)



# Factor 041: Volatility of ask volumes for the first 10 levels
def factor_041(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    return np.std(ask_volumes, axis=0)



# Factor 042: Ratio of bid volume to ask volume for the first 5 levels
def factor_042(df: pd.DataFrame) -> np.ndarray:
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(5)], axis=0)
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(5)], axis=0)
    return np.where(total_ask_volume != 0, total_bid_volume / total_ask_volume, np.nan)



# Factor 043: Ratio of top 1 bid and ask volumes to the total volume in first 25 levels
def factor_043(df: pd.DataFrame) -> np.ndarray:
    total_volume = np.sum([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(20)], axis=0)
    top1_volume = df['bid0v'].values + df['ask0v'].values
    return np.where(total_volume != 0, top1_volume / total_volume, np.nan)



# Factor 044: Mean of the bid-ask spread for the first 15 levels
def factor_044(df: pd.DataFrame) -> np.ndarray:
    spread = np.array([df[f'ask{i}p'].values - df[f'bid{i}p'].values for i in range(15)])
    return np.mean(spread, axis=0)



# Factor 045: Rolling mean of bid volumes for the first 10 levels
def factor_045(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)]).mean(axis=0)
    return pd.Series(bid_volumes).rolling(window=30).kurt().values



# Factor 046: Rolling mean of ask volumes for the first 10 levels
def factor_046(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)]).mean(axis=0)
    return pd.Series(ask_volumes).rolling(window=30).kurt().values



# Factor 047: Skewness of the ask prices for the first 15 levels
def factor_047(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.array([(df[f'ask{i}p'] * df[f'ask{i}v']).values for i in range(15)]).mean(axis=0)
    result = pd.Series(ask_prices).rolling(60).skew().values
    return result



# Factor 048: Skewness of the bid prices for the first 15 levels
def factor_048(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([(df[f'bid{i}p'] * df[f'bid{i}v']).values for i in range(15)]).mean(axis=0)
    result = pd.Series(bid_prices).rolling(60).skew().values
    return result



# Factor 049: Rolling std of bid prices for the first 5 levels
def factor_049(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(5)]).mean(axis=0)
    return pd.Series(bid_prices).rolling(window=30).std().values



# Factor 050: Rolling std of ask prices for the first 5 levels
def factor_050(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(5)]).mean(axis=0)
    return pd.Series(ask_prices).rolling(window=30).std().to_numpy()



# Factor 051: Weighted average of bid prices with volumes for the first 5 levels
def factor_051(df: pd.DataFrame) -> np.ndarray:
    avg_price_diff = df[[f'ask{i}p' for i in range(5)]].mean(axis=1)
    pos_price = np.where(avg_price_diff > 0, avg_price_diff, 0)
    result = (pd.Series(pos_price) / pd.Series(pos_price).rolling(300).mean()).values
    return result



# Factor 052: Weighted average of ask prices with volumes for the first 5 levels
def factor_052(df: pd.DataFrame) -> np.ndarray:
    avg_price = df[[f'ask{i}p' for i in range(5)]].mean(axis=1)
    neg_price = np.where(avg_price.diff() < 0, avg_price.diff(), 0)
    result = (pd.Series(neg_price).rolling(30).mean() / pd.Series(neg_price).rolling(300).mean()).values
    return result



# Factor 053: Ratio of top 1 bid and ask volumes to top 5 levels
def factor_053(df: pd.DataFrame) -> np.ndarray:
    top1_bid_ask = df['bid0v'].values + df['ask0v'].values
    top5_bid_ask = np.sum([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(5)], axis=0)
    return np.where(top5_bid_ask != 0, top1_bid_ask / top5_bid_ask, np.nan)



# Factor 054: Difference between mean of top 5 ask and bid prices
def factor_054(df: pd.DataFrame) -> np.ndarray:
    vwap_numerator = np.sum([(df[f'bid{i}p'] * df[f'bid{i}v'] + df[f'ask{i}p'] * df[f'ask{i}v']).values for i in range(20)], axis=0)
    vwap_denominator = np.sum([(df[f'bid{i}v'] + df[f'ask{i}v']).values for i in range(20)], axis=0)
    vwap = vwap_numerator / vwap_denominator
    mid_price = ((df['bid0p'] + df['bid0v']) / 2).values
    return mid_price - vwap



# Factor 055: Rolling sum of bid volumes for the first 10 levels
def factor_055(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    return pd.DataFrame(bid_volumes.T).rolling(window=3).sum().iloc[:, -1].values



# Factor 056: Rolling sum of ask volumes for the first 10 levels
def factor_056(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    return pd.DataFrame(ask_volumes.T).rolling(window=3).sum().iloc[:, -1].values



# Factor 057: Total trade volume for the first 25 levels
def factor_057(df: pd.DataFrame) -> np.ndarray:
    return np.sum([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(20)], axis=0)



# Factor 058: Cumulative sum of bid volumes for the first 10 levels
def factor_058(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(5)])
    bid_cumsum = np.cumsum(bid_volumes, axis=0)[-1] 
    return bid_cumsum 



# Factor 059: Cumulative sum of ask volumes for the first 10 levels
def factor_059(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(5)]) 
    ask_cumsum = np.cumsum(ask_volumes, axis=0)[-1] 
    sum_volumes = np.array([(df[f'ask{i}v']+df[f'bid{i}v']).values for i in range(5)]) 
    sum_cumsum = np.cumsum(sum_volumes, axis=0)[-1] 
    result =ask_cumsum / sum_cumsum 
    return result 



# Factor 060: Mean of bid volumes for the first 10 levels
def factor_060(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.mean(np.array([df[f'bid{i}v'].values for i in range(20)]),axis=0)
    result = pd.Series(bid_volumes).pct_change(10).values
    return result



# Factor 061: Mean of ask volumes for the first 10 levels
def factor_061(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.mean(np.array([df[f'ask{i}v'].values for i in range(20)]),axis=0)
    result = pd.Series(ask_volumes).pct_change(10).values
    return result



# Factor 062: Weighted average price difference for the first 5 levels
def factor_062(df: pd.DataFrame) -> np.ndarray:
    weights = np.array([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(5)])
    price_diff = np.array([df[f'ask{i}p'].values - df[f'bid{i}p'].values for i in range(5)])
    return np.average(price_diff, weights=weights, axis=0)



# Factor 063: Maximum volume of bids in top 10 levels
def factor_063(df: pd.DataFrame) -> np.ndarray:
    return np.max([df[f'bid{i}v'].values for i in range(10)], axis=0)



# Factor 064: Maximum volume of asks in top 10 levels
def factor_064(df: pd.DataFrame) -> np.ndarray:
    return np.max([df[f'ask{i}v'].values for i in range(10)], axis=0)



# Factor 065: Minimum volume of bids in top 10 levels
def factor_065(df: pd.DataFrame) -> np.ndarray:
    return np.min([df[f'bid{i}v'].values for i in range(10)], axis=0)



# Factor 066: Minimum volume of asks in top 10 levels
def factor_066(df: pd.DataFrame) -> np.ndarray:
    return np.min([df[f'ask{i}v'].values for i in range(10)], axis=0)



# Factor 067: Difference in cumulative sum of bid and ask volumes for top 10 levels
def factor_067(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.cumsum([df[f'bid{i}v'].values for i in range(10)], axis=0)
    ask_volumes = np.cumsum([df[f'ask{i}v'].values for i in range(10)], axis=0)
    return bid_volumes[-1] - ask_volumes[-1]



# Factor 068: Ratio of bid volume to ask volume for top 10 levels
def factor_068(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.sum([df[f'bid{i}v'].values for i in range(10)], axis=0)
    ask_volumes = np.sum([df[f'ask{i}v'].values for i in range(10)], axis=0)
    return np.where(ask_volumes != 0, bid_volumes / ask_volumes, np.nan)



# Factor 069: Sum of square root of bid volumes for the first 5 levels
def factor_069(df: pd.DataFrame) -> np.ndarray:
    return np.sum([np.sqrt(df[f'bid{i}v'].values) for i in range(5)], axis=0)



# Factor 070: Sum of square root of ask volumes for the first 5 levels
def factor_070(df: pd.DataFrame) -> np.ndarray:
    return np.sum([np.sqrt(df[f'ask{i}v'].values) for i in range(5)], axis=0)



# Factor 071: Mean of square root of bid volumes for the first 5 levels
def factor_071(df: pd.DataFrame) -> np.ndarray:
    return np.mean([np.sqrt(df[f'bid{i}v'].values) for i in range(5)], axis=0)



# Factor 072: Mean of square root of ask volumes for the first 5 levels
def factor_072(df: pd.DataFrame) -> np.ndarray:
    return np.mean([np.sqrt(df[f'ask{i}v'].values) for i in range(5)], axis=0)



# Factor 073: Standard deviation of bid volumes for the first 5 levels
def factor_073(df: pd.DataFrame) -> np.ndarray:
    return np.std([df[f'bid{i}v'].values for i in range(5)], axis=0)



# Factor 074: Standard deviation of ask volumes for the first 5 levels
def factor_074(df: pd.DataFrame) -> np.ndarray:
    return np.std([df[f'ask{i}v'].values for i in range(5)], axis=0)



# Factor 075: Coefficient of variation of bid volumes for the first 5 levels
def factor_075(df: pd.DataFrame) -> np.ndarray:
    mean_bid_volume = np.mean([df[f'bid{i}v'].values for i in range(5)], axis=0)
    std_bid_volume = np.std([df[f'bid{i}v'].values for i in range(5)], axis=0)
    return np.where(mean_bid_volume != 0, std_bid_volume / mean_bid_volume, np.nan)



# Factor 076: Coefficient of variation of ask volumes for the first 5 levels
def factor_076(df: pd.DataFrame) -> np.ndarray:
    mean_ask_volume = np.mean([df[f'ask{i}v'].values for i in range(5)], axis=0)
    std_ask_volume = np.std([df[f'ask{i}v'].values for i in range(5)], axis=0)
    return np.where(mean_ask_volume != 0, std_ask_volume / mean_ask_volume, np.nan)



# Factor 077: Bid and ask volume correlation for top 10 levels
def factor_077(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    correlations = [np.corrcoef(bv, av)[0, 1] for bv, av in zip(bid_volumes.T, ask_volumes.T)]
    return np.array(correlations) 



# Factor 078: Ratio of sum of bid volumes to total volume of first 25 levels
def factor_078(df: pd.DataFrame) -> np.ndarray:
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_volume = np.sum([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(20)], axis=0)
    return np.where(total_volume != 0, total_bid_volume / total_volume, np.nan)



# Factor 079: Ratio of sum of ask volumes to total volume of first 25 levels
def factor_079(df: pd.DataFrame) -> np.ndarray:
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_volume = np.sum([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(20)], axis=0)
    return np.where(total_volume != 0, total_ask_volume / total_volume, np.nan)



# Factor 080: Rolling std deviation of combined volumes for top 10 levels
def factor_080(df: pd.DataFrame) -> np.ndarray:
    combined_volumes = np.array([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(10)])
    return pd.DataFrame(combined_volumes.T).rolling(window=3).std().iloc[:, -1].values



# Factor 081: Rolling mean of combined volumes for top 10 levels
def factor_081(df: pd.DataFrame) -> np.ndarray:
    combined_volumes = np.array([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(10)])
    return pd.DataFrame(combined_volumes.T).rolling(window=3).mean().iloc[:, -1].values



# Factor 082: Rolling sum of combined volumes for top 10 levels
def factor_082(df: pd.DataFrame) -> np.ndarray:
    combined_volumes = np.array([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(10)])
    return pd.DataFrame(combined_volumes.T).rolling(window=3).sum().iloc[:, -1].values



# Factor 083: Difference between max and min of bid volumes for top 10 levels
def factor_083(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    return np.max(bid_volumes, axis=0) - np.min(bid_volumes, axis=0)



# Factor 084: Difference between max and min of ask volumes for top 10 levels
def factor_084(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    return np.max(ask_volumes, axis=0) - np.min(ask_volumes, axis=0)



# Factor 085: Covariance of bid and ask volumes for top 10 levels
def factor_085(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    covariances = [np.cov(bv, av)[0, 1] for bv, av in zip(bid_volumes.T, ask_volumes.T)]
    return np.array(covariances)



# Factor 086: Difference between sum of bid and ask volumes for top 15 levels
def factor_086(df: pd.DataFrame) -> np.ndarray:
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(15)], axis=0)
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(15)], axis=0)
    return total_bid_volume - total_ask_volume



# Factor 087: Ratio of sum of bid volumes to ask volumes for top 20 levels
def factor_087(df: pd.DataFrame) -> np.ndarray:
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    return np.where(total_ask_volume != 0, total_bid_volume / total_ask_volume, np.nan)



# Factor 088: Variance of bid volumes for the first 10 levels
def factor_088(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    return np.var(bid_volumes, axis=0)



# Factor 089: Variance of ask volumes for the first 10 levels
def factor_089(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    return np.var(ask_volumes, axis=0)



# Factor 090: Average of bid and ask price difference for top 10 levels
def factor_090(df: pd.DataFrame) -> np.ndarray:
    price_diff = np.array([df[f'ask{i}p'].values - df[f'bid{i}p'].values for i in range(10)])
    return np.mean(price_diff, axis=0)



# Factor 091: Volatility ratio of bid to ask volumes for top 10 levels
def factor_091(df: pd.DataFrame) -> np.ndarray:
    bid_volatility = np.std([df[f'bid{i}v'].values for i in range(10)], axis=0)
    ask_volatility = np.std([df[f'ask{i}v'].values for i in range(10)], axis=0)
    return np.where(ask_volatility != 0, bid_volatility / ask_volatility, np.nan)



# Factor 092: Volatility ratio of bid to ask prices for top 10 levels
def factor_092(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    bid_volatility = np.std(bid_prices, axis=0)
    ask_volatility = np.std(ask_prices, axis=0)
    return np.where(ask_volatility != 0, bid_volatility / ask_volatility, np.nan)



# Factor 093: Mean of bid prices for the first 5 levels
def factor_093(df: pd.DataFrame) -> np.ndarray:
    avg_price = df[[f'ask{i}p' for i in range(5)]].mean(axis=1)
    result = avg_price.rolling(360).quantile(0.5).values
    return result



# Factor 094: Mean of ask prices for the first 5 levels
def factor_094(df: pd.DataFrame) -> np.ndarray:
    avg_price = df[[f'ask{i}p' for i in range(5)]].mean(axis=1)
    result = avg_price.rolling(100).quantile(0.1).values
    return result



# Factor 095: Median of bid volumes for the first 25 levels
def factor_095(df: pd.DataFrame) -> np.ndarray:
    return np.median([df[f'bid{i}v'].values for i in range(20)], axis=0)



# Factor 096: Median of ask volumes for the first 25 levels
def factor_096(df: pd.DataFrame) -> np.ndarray:
    return np.median([df[f'ask{i}v'].values for i in range(20)], axis=0)



# Factor 097: Ratio of bid volumes to ask volumes for each level, averaged over the first 10 levels
def factor_097(df: pd.DataFrame) -> np.ndarray:
    ratios = np.array([df[f'bid{i}v'].values / df[f'ask{i}v'].values if df[f'ask{i}v'].values.all() != 0 else np.nan for i in range(10)])
    return np.nanmean(ratios, axis=0)



# Factor 098: Range (max-min) of bid prices for the first 15 levels
def factor_098(df: pd.DataFrame) -> np.ndarray:
    return np.max([df[f'bid{i}p'].values for i in range(15)], axis=0) - np.min([df[f'bid{i}p'].values for i in range(15)], axis=0)



# Factor 099: Range (max-min) of ask prices for the first 15 levels
def factor_099(df: pd.DataFrame) -> np.ndarray:
    return np.max([df[f'ask{i}p'].values for i in range(15)], axis=0) - np.min([df[f'ask{i}p'].values for i in range(15)], axis=0)



# Factor 100: Sum of the square of the differences of bid prices for the first 10 levels
def factor_100(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    return np.sum((bid_prices - np.mean(bid_prices, axis=0)) ** 2, axis=0)



# Factor 101: Sum of the square of the differences of ask prices for the first 10 levels
def factor_101(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    return np.sum((ask_prices - np.mean(ask_prices, axis=0)) ** 2, axis=0)



# Factor 102: Harmonic mean of bid volumes for the first 10 levels
def factor_102(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    return np.where(np.all(bid_volumes, axis=0), 10 / np.sum(1.0 / bid_volumes, axis=0), np.nan)



# Factor 103: Harmonic mean of ask volumes for the first 10 levels
def factor_103(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    return np.where(np.all(ask_volumes, axis=0), 10 / np.sum(1.0 / ask_volumes, axis=0), np.nan)



# Factor 104: Weighted mean of bid prices by ask volumes for the first 5 levels
def factor_104(df: pd.DataFrame) -> np.ndarray:
    price_diff = np.array([(df[f'ask{i}p'] - df[f'bid{i}p']).values for i in range(5)])
    return np.mean(price_diff, axis=0)



# Factor 105: Weighted mean of ask prices by bid volumes for the first 5 levels
def factor_105(df: pd.DataFrame) -> np.ndarray:
    price_diff = np.array([(df[f'ask{i}p'] - df[f'bid{i}p']).values for i in range(20)])
    return np.mean(price_diff, axis=0)



# Factor 106: Price range between the maximum bid and minimum ask in the first 20 levels
def factor_106(df: pd.DataFrame) -> np.ndarray:
    max_bid_price = np.max(np.array([df[f'bid{i}p'].values for i in range(20)]), axis=0)
    min_ask_price = np.min(np.array([df[f'ask{i}p'].values for i in range(20)]), axis=0)
    return max_bid_price - min_ask_price



# Factor 107: Correlation between bid prices and volumes for the first 10 levels
def factor_107(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    correlations = np.array([np.corrcoef(bid_prices[:, i], bid_volumes[:, i])[0, 1] for i in range(bid_prices.shape[1])])
    return correlations



# Factor 108: Correlation between ask prices and volumes for the first 10 levels
def factor_108(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    correlations = np.array([np.corrcoef(ask_prices[:, i], ask_volumes[:, i])[0, 1] for i in range(ask_prices.shape[1])])
    return correlations



# Factor 109: Average difference between bid prices and the mid-price for the first 5 levels
def factor_109(df: pd.DataFrame) -> np.ndarray:
    mid_price = (df['bid0p'].values + df['ask0p'].values) / 2
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(5)])
    return np.mean(bid_prices - mid_price, axis=0)



# Factor 110: Average difference between ask prices and the mid-price for the first 5 levels
def factor_110(df: pd.DataFrame) -> np.ndarray:
    mid_price = (df['bid0p'].values + df['ask0p'].values) / 2
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(5)])
    return np.mean(ask_prices - mid_price, axis=0)



# Factor 111: Rolling variance of bid prices for the first 10 levels
def factor_111(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)]).T
    rolling_variance = pd.DataFrame(bid_prices).rolling(window=3).var().values.T
    return rolling_variance[-1]



# Factor 112: Rolling variance of ask prices for the first 10 levels
def factor_112(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)]).T
    rolling_variance = pd.DataFrame(ask_prices).rolling(window=3).var().values.T
    return rolling_variance[-1]



# Factor 113: Harmonic mean of bid prices for the first 10 levels
def factor_113(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].pct_change(30).values for i in range(10)])
    return np.where(np.all(bid_prices, axis=0), 10 / np.sum(1.0 / bid_prices, axis=0), np.nan)



# Factor 114: Harmonic mean of ask prices for the first 10 levels
def factor_114(df: pd.DataFrame) -> np.ndarray:
    ask_prices = np.mean(np.array([df[f'ask{i}p'].pct_change(30).values for i in range(10)]),axis=0)
    result = pd.Series(ask_prices).rolling(600).sum().diff().values
    return result



# Factor 115: Difference in harmonic mean of bid and ask prices for the first 10 levels
def factor_115(df: pd.DataFrame) -> np.ndarray:
    vwap_bid = np.sum([df[f'bid{i}p'] * df[f'bid{i}v'] for i in range(20)], axis=0) / np.sum([df[f'bid{i}v'] for i in range(20)], axis=0)
    vwap_ask = np.sum([df[f'ask{i}p'] * df[f'ask{i}v'] for i in range(20)], axis=0) / np.sum([df[f'ask{i}v'] for i in range(20)], axis=0)
    result = pd.Series(vwap_bid - vwap_ask).pct_change().rolling(60).mean().values
    return result



# Factor 116: Weighted mean of bid prices with inverse volumes for the first 10 levels
def factor_116(df: pd.DataFrame) -> np.ndarray:
    result = df[[f'bid{i}p' for i in range(20)]].diff(3).mean(axis=1).rolling(300,min_periods=50).rank().values
    return result



# Factor 117: Weighted mean of ask prices with inverse volumes for the first 10 levels
def factor_117(df: pd.DataFrame) -> np.ndarray:
    weights = 1 / np.array([df[f'ask{i}v'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    return np.average(ask_prices, weights=weights, axis=0)



# Factor 118: Maximum bid price difference in the first 15 levels
def factor_118(df: pd.DataFrame) -> np.ndarray:
    mid_price = ((df.ask1p + df.bid1p) / 2).values
    bid_vwap = np.array([df[f'bid{i}p'] * df[f'bid{i}v'] for i in range(5)]).sum(axis=0) / np.array([df[f'bid{i}v'] for i in range(5)]).sum(axis=0)
    mid_price_diff = pd.Series(mid_price - bid_vwap)
    result = (mid_price_diff / mid_price_diff.rolling(120).mean()).values
    return result



# Factor 119: Maximum ask price difference in the first 15 levels
def factor_119(df: pd.DataFrame) -> np.ndarray:
    mid_price = ((df.ask1p + df.bid1p) / 2).values
    ask_vwap = np.array([df[f'ask{i}p'] * df[f'ask{i}v'] for i in range(5)]).sum(axis=0) / np.array([df[f'ask{i}v'] for i in range(5)]).sum(axis=0)
    mid_price_diff = pd.Series(mid_price - ask_vwap)
    result = (mid_price_diff / mid_price_diff.rolling(120).mean()).values
    return result



# Factor 120: Median bid volume for the first 10 levels
def factor_120(df: pd.DataFrame) -> np.ndarray:
    return np.median(np.array([df[f'bid{i}v'].values for i in range(10)]), axis=0)



# Factor 121: Median ask volume for the first 10 levels
def factor_121(df: pd.DataFrame) -> np.ndarray:
    return np.median(np.array([df[f'ask{i}v'].values for i in range(10)]), axis=0)



# Factor 122: Top 3 ask volumes to total ask volume ratio in the first 15 levels
def factor_122(df: pd.DataFrame) -> np.ndarray:
    top3_ask_volumes = np.sum(np.array([df[f'ask{i}v'].values for i in range(3)]), axis=0)
    total_ask_volumes = np.sum(np.array([df[f'ask{i}v'].values for i in range(15)]), axis=0)
    return np.where(total_ask_volumes != 0, top3_ask_volumes / total_ask_volumes, np.nan)



# Factor 123: Top 3 bid volumes to total bid volume ratio in the first 15 levels
def factor_123(df: pd.DataFrame) -> np.ndarray:
    top3_bid_volumes = np.sum(np.array([df[f'bid{i}v'].values for i in range(3)]), axis=0)
    total_bid_volumes = np.sum(np.array([df[f'bid{i}v'].values for i in range(15)]), axis=0)
    return np.where(total_bid_volumes != 0, top3_bid_volumes / total_bid_volumes, np.nan)



# Factor 124: Ratio of variance of bid and ask volumes for the first 10 levels
def factor_124(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])
    bid_variance = np.var(bid_volumes, axis=0)
    ask_variance = np.var(ask_volumes, axis=0)
    return np.where(ask_variance != 0, bid_variance / ask_variance, np.nan)



# Factor 125: Median of prices between bid and ask for the first 20 levels
def factor_125(df: pd.DataFrame) -> np.ndarray:
    prices = np.median(np.array([df[f'bid{i}p'].values for i in range(20)] + [df[f'ask{i}p'].values for i in range(20)]), axis=0)
    result = np.where(pd.Series(prices).pct_change(15)<0, pd.Series(prices).pct_change(5), pd.Series(prices).pct_change(20))
    return result



# Factor 126: Coefficient of variation of combined volumes for top 10 levels
def factor_126(df: pd.DataFrame) -> np.ndarray:
    combined_volumes = np.array([df[f'bid{i}v'].values + df[f'ask{i}v'].values for i in range(10)])
    mean_combined_volume = np.mean(combined_volumes, axis=0)
    std_combined_volume = np.std(combined_volumes, axis=0)
    return np.where(mean_combined_volume != 0, std_combined_volume / mean_combined_volume, np.nan)



# Factor 127: Mean of ask prices weighted by bid volumes for the first 10 levels
def factor_127(df: pd.DataFrame) -> np.ndarray:
    result = df[[f'bid{i}p' for i in range(10)]].kurt(axis=1).rolling(300).mean()
    return result



# Factor 128: Mean of bid prices weighted by ask volumes for the first 10 levels
def factor_128(df: pd.DataFrame) -> np.ndarray:
    result1 = df[[f'ask{i}p' for i in range(20)]].diff().sum(axis=1)
    result2 = result1.rolling(90,min_periods=30).skew()
    result = result2.rolling(300,min_periods=50).mean().values
    return result



# Factor 129: Weighted harmonic mean of bid volumes for the first 10 levels
def factor_129(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(10)])  # Shape: (10, len(df))
    weights = np.arange(1, 11)[:, np.newaxis]  # Shape: (10, 1)

    valid_mask = np.all(bid_volumes > 0, axis=0)
    harmonic_means = np.full(len(df), np.nan)

    harmonic_means[valid_mask] = len(weights) / np.sum(weights / bid_volumes[:, valid_mask], axis=0)

    return harmonic_means



# Factor 130: Weighted harmonic mean of ask volumes for the first 10 levels
def factor_130(df: pd.DataFrame) -> np.ndarray:
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(10)])  # Shape: (10, len(df))
    weights = np.arange(1, 11)[:, np.newaxis]  # Shape: (10, 1)

    valid_mask = np.all(ask_volumes > 0, axis=0)
    harmonic_means = np.full(len(df), np.nan)

    harmonic_means[valid_mask] = len(weights) / np.sum(weights / ask_volumes[:, valid_mask], axis=0)

    return harmonic_means



# Factor 131: Volume weighted average price of 25 levels
def factor_131(df: pd.DataFrame) -> np.ndarray:
    result = df[[f'ask{i}p' for i in range(20)]].kurt(axis=1).rolling(200).mean()
    return result



# Factor 132: Ratio of total ask volume to total bid volume
def factor_132(df: pd.DataFrame) -> np.ndarray:
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    result = np.where(total_bid_volume != 0, total_ask_volume / total_bid_volume, np.nan)
    return result



# Factor 133: Price difference normalized by the total price
def factor_133(df: pd.DataFrame) -> np.ndarray:
    total_bid_price = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    total_ask_price = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_price = total_bid_price + total_ask_price
    price_diff = total_bid_price - total_ask_price
    result = np.where(total_price != 0, price_diff / total_price, np.nan)
    return result



# Factor 134: Mid price calculation
def factor_134(df: pd.DataFrame) -> np.ndarray:
    mid_price = (df['bid0p'] + df['ask0p']) / 2
    mid_volume = (df['bid0v'] + df['ask0v']) / 2
    result = mid_price.rolling(300).corr(mid_volume).values
    return result



# Factor 135: Mid price difference over time
def factor_135(df: pd.DataFrame) -> np.ndarray:
    center_price = (df['bid0p'].values + df['ask0p'].values) / 2
    result = np.diff(center_price, prepend=np.nan)
    return result



# Factor 136: Bid and ask strength ratio for the first 5 levels
def factor_136(df: pd.DataFrame) -> np.ndarray:
    ask_strength = np.sum([df[f'ask{i}p'].values * df[f'ask{i}v'].values for i in range(5)], axis=0)
    bid_strength = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(5)], axis=0)
    result = (bid_strength - ask_strength) / (bid_strength + ask_strength)
    return result



# Factor 137: Absolute difference in total bid and ask volume
def factor_137(df: pd.DataFrame) -> np.ndarray:
    total_ask_volume = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    total_bid_volume = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    result = total_bid_volume - total_ask_volume
    return result



# Factor 138: Standard deviation of price differences for the top 10 levels
def factor_138(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    price_diff = bid_prices - ask_prices
    result = np.std(price_diff, axis=0)
    return result



# Factor 139: Mean price difference for the top 5 levels
def factor_139(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(5)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(5)])
    price_diff = bid_prices - ask_prices
    result = np.mean(price_diff, axis=0)
    return result



# Factor 140: Percentage change in bid to ask volume ratio for the first 15 levels
def factor_140(df: pd.DataFrame) -> np.ndarray:
    volume_ratio = np.array([df[f'bid{i}v'].values / (df[f'ask{i}v'].values + df[f'bid{i}v'].values) for i in range(15)])
    volume_ratio_df = pd.DataFrame(volume_ratio.T)
    result = volume_ratio_df.pct_change(axis=1).mean(axis=1).values
    return result



# Factor 141: Sum of bid volumes for the first 20 levels
def factor_141(df: pd.DataFrame) -> np.ndarray:
    result = np.sum([df[f'bid{i}v'].values for i in range(20)], axis=0)
    return result



# Factor 142: Sum of ask volumes for the first 20 levels
def factor_142(df: pd.DataFrame) -> np.ndarray:
    result = np.sum([df[f'ask{i}v'].values for i in range(20)], axis=0)
    return result



# Factor 143: Standard deviation of price differences for the first 10 levels
def factor_143(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    price_diff = bid_prices - ask_prices
    result = np.std(price_diff, axis=0)
    return result



# Factor 144: Mean difference in price changes for 15 levels
def factor_144(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(15)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(15)])
    price_diff = bid_prices - ask_prices
    price_diff_diff = np.diff(price_diff, axis=0, prepend=np.nan)
    result = np.nanmean(price_diff_diff, axis=0)
    return result



# Factor 145: Total bid strength for the first 5 levels
def factor_145(df: pd.DataFrame) -> np.ndarray:
    result = np.sum([df[f'bid{i}p'].values * df[f'bid{i}v'].values for i in range(5)], axis=0)
    return result



# Factor 146: Total ask strength for the first 5 levels
def factor_146(df: pd.DataFrame) -> np.ndarray:
    result = np.sum([df[f'ask{i}p'].values * df[f'ask{i}v'].values for i in range(5)], axis=0)
    return result



# Factor 147: Max price difference for the first 10 levels
def factor_147(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    price_diff = bid_prices - ask_prices
    result = np.max(price_diff, axis=0)
    return result



# Factor 148: Standard deviation of combined volumes for the first 5 levels
def factor_148(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(5)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(5)])
    total_volumes = bid_volumes + ask_volumes
    result = np.std(total_volumes, axis=0)
    return result



# Factor 149: Change rate of cumulative price differences for 25 levels
def factor_149(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(20)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(20)])
    price_diff = bid_prices - ask_prices

    price_diff_cumsum = np.cumsum(price_diff, axis=0)
    price_diff_cumsum_diff = np.diff(price_diff_cumsum, axis=0, prepend=np.nan)
    result = np.nanmean(price_diff_cumsum_diff, axis=0)
    return result



# Factor 150: Skewness of combined volumes for the first 15 levels
def factor_150(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(15)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(15)])
    total_volumes = bid_volumes + ask_volumes
    total_volumes_df = pd.DataFrame(total_volumes.T, index=df.index)
    result = total_volumes_df.skew(axis=1).values
    return result



# Factor 151: Skewness of total volumes for the first 25 levels
def factor_151(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    total_volumes = bid_volumes + ask_volumes
    total_sum = np.sum(total_volumes, axis=0)
    result = pd.Series(total_sum).rolling(45).skew().values
    return result



# Factor 152: Price change rate for the first 10 levels
def factor_152(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    price_diff = bid_prices - ask_prices
    price_diff_df = pd.DataFrame(price_diff.T, index=df.index)
    result = price_diff_df.pct_change().mean(axis=1).values
    return result



# Factor 153: Standard deviation difference of volumes for the first 20 levels
def factor_153(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    bid_volumes_std = np.std(bid_volumes, axis=0)
    ask_volumes_std = np.std(ask_volumes, axis=0)
    result = bid_volumes_std - ask_volumes_std
    return result



# Factor 154: Kurtosis of combined volumes for the first 5 levels
def factor_154(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(5)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(5)])
    total_volumes = bid_volumes + ask_volumes
    total_volumes_df = pd.DataFrame(total_volumes.T, index=df.index)
    result = total_volumes_df.kurt(axis=1).values
    return result



# Factor 155: Standard deviation of volume ratios for the first 15 levels
def factor_155(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(15)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(15)])
    volume_ratios = bid_volumes / (ask_volumes + bid_volumes)
    result = np.std(volume_ratios, axis=0)
    return result



# Factor 156: Standard deviation difference of prices for the first 10 levels
def factor_156(df: pd.DataFrame) -> np.ndarray:
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    bid_prices_std = np.std(bid_prices, axis=0)
    ask_prices_std = np.std(ask_prices, axis=0)
    result = bid_prices_std - ask_prices_std
    return result



# Factor 157: Difference in bid and ask volume volatility for the first 25 levels
def factor_157(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    bid_volumes_df = pd.DataFrame(bid_volumes.T, index=df.index)
    ask_volumes_df = pd.DataFrame(ask_volumes.T, index=df.index)
    bid_volumes_diff_std = bid_volumes_df.diff().std(axis=1).values
    ask_volumes_diff_std = ask_volumes_df.diff().std(axis=1).values
    result = bid_volumes_diff_std - ask_volumes_diff_std
    return result



# Factor 158: Log difference of bid and ask volumes for the first 25 levels
def factor_158(df: pd.DataFrame) -> np.ndarray:
    bid_volumes = np.array([df[f'bid{i}v'].values for i in range(20)])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in range(20)])
    log_bid_volumes = np.log(bid_volumes + 1)
    log_ask_volumes = np.log(ask_volumes + 1)
    result = log_bid_volumes - log_ask_volumes
    return result.mean(axis=0)



# Factor 159: Mean price difference for the first 10 levels
def factor_159(df: pd.DataFrame) -> np.ndarray: # mid_re: ic:0.01--0.015---0.02--0.03 icir: tornover: mid_chg
    bid_prices = np.array([df[f'bid{i}p'].values for i in range(10)])
    ask_prices = np.array([df[f'ask{i}p'].values for i in range(10)])
    bid_mean = np.mean(bid_prices, axis=0)
    ask_mean = np.mean(ask_prices, axis=0)
    result = bid_mean - ask_mean
    return result



# Factor 160: Volume ratio change for random 10 pairs
def factor_160(df: pd.DataFrame) -> np.ndarray:
    np.random.seed(0)
    random_indices = np.random.choice(20, 10, replace=False)
    bid_volumes = np.array([df[f'bid{i}v'].values for i in random_indices])
    ask_volumes = np.array([df[f'ask{i}v'].values for i in random_indices])
    volume_ratios = bid_volumes / (ask_volumes + bid_volumes)
    volume_ratios_df = pd.DataFrame(volume_ratios.T, index=df.index)
    result = volume_ratios_df.pct_change().mean(axis=1).values
    return result



def factor_161(df: pd.DataFrame) -> np.ndarray:
    return df['bid0p'].diff().values



def factor_162(df: pd.DataFrame) -> np.ndarray:
    return df['ask0p'].diff().values



def factor_163(df: pd.DataFrame) -> np.ndarray:
    price_diff = df['ask0p'] - df['bid0p']
    return (price_diff / ((df['ask0p'] + df['bid0p']) / 2)).values



def factor_164(df: pd.DataFrame) -> np.ndarray:
    return df['bid0v'].diff().values



def factor_165(df: pd.DataFrame) -> np.ndarray:
    return df['ask0v'].diff().values



def factor_166(df: pd.DataFrame) -> np.ndarray:
    price_diff = df['ask0p'] - df['ask0v']
    depth = (df['bid0v'] + df['ask0v']) / 2
    return (price_diff / depth).values



def factor_167(df: pd.DataFrame) -> np.ndarray:
    return ((df['bid0v'] + df['ask0v']) / 2).values



def factor_169(df: pd.DataFrame) -> np.ndarray:
    return np.log(df['bid0v'] + df['ask0v']).diff()



def factor_170(df: pd.DataFrame) -> np.ndarray:
    mid_price = (df['ask0p'] + df['bid0p']) / 2
    return mid_price.pct_change(120).values



def factor_173(df: pd.DataFrame) -> np.ndarray:
    return (df['ask0p'] - df['bid0p']).values



def factor_174(df: pd.DataFrame) -> np.ndarray:
    return (df['ask0p'] * df['ask0v'] - df['bid0p'] * df['bid0v']).values



def factor_177(df: pd.DataFrame) -> np.ndarray:
    mid_price = (df['ask0p'] + df['bid0p']) / 2
    return mid_price.diff(3).values

# def baseline_001(df, timeperiod=30):
#     result = talib.MA(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_002(df, timeperiod=30):
#     result = talib.EMA(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_003(df, timeperiod=30):
#     result = talib.WMA(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_004(df, timeperiod=30):
#     result = talib.T3(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_005(df, timeperiod=14):
#     result = talib.RSI(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_006(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     macd, _, _ = talib.MACD(df['close'], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
#     return macd

# def baseline_007(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     _, macdsignal, _ = talib.MACD(df['close'], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
#     return macdsignal

# def baseline_008(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     _, _, macdhist = talib.MACD(df['close'], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
#     return macdhist

# def baseline_009(df, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0):
#     upperband, _, _ = talib.BBANDS(df['close'], timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=matype)
#     return upperband

# def baseline_010(df, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0):
#     _, middleband, _ = talib.BBANDS(df['close'], timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=matype)
#     return middleband

# def baseline_011(df, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0):
#     _, _, lowerband = talib.BBANDS(df['close'], timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=matype)
#     return lowerband


# def baseline_023(df, timeperiod=14):
#     result = talib.CMO(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_024(df: pd.DataFrame) -> np.ndarray:
#     result = talib.TRANGE(df['high'], df['low'], df['close'])
#     return result

# def baseline_027(df, timeperiod=30):
#     result = talib.TRIMA(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_028(df, timeperiod=30):
#     result = talib.KAMA(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_030(df, timeperiod=20, nbdevup=2):
#     ema = talib.EMA(df['close'], timeperiod=timeperiod)
#     stddev = talib.STDDEV(df['close'], timeperiod=timeperiod)
#     upperband = ema + nbdevup * stddev
#     return upperband

# def baseline_031(df, timeperiod=20, nbdevdn=2):
#     ema = talib.EMA(df['close'], timeperiod=timeperiod)
#     stddev = talib.STDDEV(df['close'], timeperiod=timeperiod)
#     lowerband = ema - nbdevdn * stddev
#     return lowerband

# def baseline_032(df, timeperiod=10):
#     result = talib.MOM(df['open'], timeperiod=timeperiod)
#     return result

# def baseline_033(df, fastperiod=12, slowperiod=26):
#     ema_fast = talib.EMA(df['close'], timeperiod=fastperiod)
#     ema_slow = talib.EMA(df['close'], timeperiod=slowperiod)
#     smmao = ema_fast - ema_slow
#     return smmao

# def baseline_034(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     _, macdsignal, macdhist = talib.MACD(df['close'], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
#     smmao = macdhist - macdsignal
#     return smmao

# def baseline_035(df, timeperiod=30):
#     ema1 = talib.EMA(df['close'], timeperiod=timeperiod)
#     ema2 = talib.EMA(ema1, timeperiod=timeperiod)
#     ema3 = talib.EMA(ema2, timeperiod=timeperiod)
#     return ema3

# def baseline_036(df, timeperiod1=12, timeperiod2=26):
#     wma1 = talib.WMA(df['close'], timeperiod=timeperiod1)
#     wma2 = talib.WMA(df['close'], timeperiod=timeperiod2)
#     dwmao = wma1 - wma2
#     return dwmao

# def baseline_038(df, timeperiod=10):
#     mom = talib.MOM(df['close'], timeperiod=timeperiod)
#     smom = talib.SMA(mom, timeperiod=timeperiod//2)
#     return smom

# def baseline_012(df, timeperiod=10):
#     result = talib.MOM(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_013(df, timeperiod=14):
#     result = talib.CCI(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_014(df, timeperiod=14):
#     result = talib.CMO(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_015(df, timeperiod=10):
#     result = talib.ROC(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_016(df, timeperiod=14):
#     result = talib.ADX(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_017(df, timeperiod=14):
#     result = talib.ATR(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_018(df, timeperiod=10):
#     result = talib.ROCP(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_019(df, timeperiod=10):
#     result = talib.ROCR(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_020(df, fastk_period=14, slowk_period=3, slowk_matype=0):
#     slowk, _ = talib.STOCH(df['high'], df['low'], df['close'], fastk_period=fastk_period, slowk_period=slowk_period, slowk_matype=slowk_matype)
#     return slowk

# def baseline_021(df, fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0):
#     _, slowd = talib.STOCH(df['high'], df['low'], df['close'], fastk_period=fastk_period, slowk_period=slowk_period, slowk_matype=slowk_matype, slowd_period=slowd_period, slowd_matype=slowd_matype)
#     return slowd

# def baseline_022(df, timeperiod=14):
#     result = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_039(df, timeperiod=10):
#     mom = talib.MOM(df['close'], timeperiod=timeperiod)
#     spmom = talib.WMA(mom, timeperiod=timeperiod//2)
#     return spmom

# def baseline_040(df: pd.DataFrame) -> np.ndarray:
#     vpt = pd.Series(index=df['close'].index)
#     vpt.iloc[0] = 0
#     for t in range(1, len(df['close'])):
#         vpt.iloc[t] = vpt.iloc[t-1] + df['volume'].iloc[t] * (df['close'].iloc[t] - df['close'].iloc[t-1]) / df['close'].iloc[t-1]
#     return vpt

# def baseline_041(df, shortperiod=12, longperiod=26):
#     short_vo = df['volume'].rolling(window=shortperiod).mean()
#     long_vo = df['volume'].rolling(window=longperiod).mean()
#     vo = short_vo - long_vo
#     return vo

# def baseline_042(df, timeperiod=14):
#     result = talib.RSI(df['volume'], timeperiod=timeperiod)
#     return result

# def baseline_043(df, timeperiod=14):
#     atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     sma_atr = talib.SMA(atr, timeperiod=timeperiod//2)
#     tra = atr - sma_atr
#     return tra

# def baseline_044(df, timeperiod=14):
#     result = talib.LINEARREG_SLOPE(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_045(df, timeperiod=14):
#     result = talib.LINEARREG_INTERCEPT(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_046(df, timeperiod=14):
#     result = talib.LINEARREG(df['close'], timeperiod=timeperiod)
#     return result

# def baseline_047(df: pd.DataFrame) -> np.ndarray:
#     result = (df['high'] + df['low'] + df['close']) / 3
#     return result

# def baseline_048(df: pd.DataFrame) -> np.ndarray:
#     result = (df['close'] * 2 + df['high'] + df['low']) / 4
#     return result

# def baseline_049(df: pd.DataFrame) -> np.ndarray:
#     result = talib.HT_TRENDLINE(df['close'])
#     return result

# def baseline_050(df: pd.DataFrame) -> np.ndarray:
#     result = (df['high'] - df['low']).rolling(window=10).mean()
#     return result

# def baseline_051(df, timeperiod=10):
#     sma = talib.SMA(df['close'], timeperiod=timeperiod)
#     diff = df['close'] - sma
#     return diff

# def baseline_052(df, timeperiod=14):
#     high_ma = talib.SMA(df['high'], timeperiod=timeperiod)
#     low_ma = talib.SMA(df['low'], timeperiod=timeperiod)
#     dhlma = high_ma - low_ma
#     return dhlma

# def baseline_053(df, timeperiod=14):
#     beta = talib.BETA(df['high'], df['low'], timeperiod=timeperiod)
#     return beta

# def baseline_054(df, timeperiod=14):
#     avg_price = (df['high'] + df['low'] + df['close']) / 3
#     vwa = (avg_price * df['volume']).rolling(window=timeperiod).sum() / df['volume'].rolling(window=timeperiod).sum()
#     vo = avg_price - vwa
#     return vo

# def baseline_055(df, timeperiod=14):
#     mean = df['close'].rolling(window=timeperiod).mean()
#     ssd = ((df['close'] - mean) ** 2).rolling(window=timeperiod).sum()
#     return ssd

# def baseline_056(df, fast_period=12, slow_period=26):
#     vwo = talib.MACD(df['volume'], fastperiod=fast_period, slowperiod=slow_period)[0]
#     return vwo

# def baseline_057(df, timeperiod=10):
#     typical_price = (df['high'] + df['low'] + df['close']) / 3
#     oscillator = typical_price.diff(periods=timeperiod)
#     return oscillator

# def baseline_058(df, timeperiod=14):
#     result = talib.STDDEV(df['volume'], timeperiod=timeperiod, nbdev=1)
#     return result

# def baseline_059(df, timeperiod=20, nbdevup=2, nbdevdn=2):
#     rolling_mean = df['close'].rolling(window=timeperiod).mean()
#     rolling_std = df['close'].rolling(window=timeperiod).std()
#     upperband = rolling_mean + (nbdevup * rolling_std)
#     lowerband = rolling_mean - (nbdevdn * rolling_std)
#     bbw = upperband - lowerband
#     return bbw

# def baseline_060(df, timeperiod=10):
#     spread = df['high'] - df['low']
#     psdiff = spread.diff(periods=timeperiod)
#     return psdiff

# def baseline_061(df, short_period=12, long_period=26):
#     short_ma = talib.SMA(df['close'], timeperiod=short_period)
#     long_ma = talib.SMA(df['close'], timeperiod=long_period)
#     tma = short_ma - long_ma
#     return tma

# def baseline_062(df: pd.DataFrame) -> np.ndarray:
#     result = df['high'] / df['low']
#     return result

# def baseline_063(df, timeperiod=20):
#     result = df['close'].rolling(timeperiod).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)
#     return result

# def baseline_064(df, timeperiod=14):
#     result = talib.APO(df['close'], fastperiod=12, slowperiod=26, matype=0)
#     return result

# def baseline_065(df, timeperiod=14):
#     result = talib.BOP(df['open'], df['high'], df['low'], df['close'])
#     return result

# def baseline_066(df, timeperiod=14):
#     result = talib.DX(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_067(df, timeperiod=14):
#     result = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_068(df, timeperiod=14):
#     result = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_069(df, timeperiod=14):
#     result = talib.PLUS_DM(df['high'], df['low'], timeperiod=timeperiod)
#     return result

# def baseline_070(df, fastperiod=3, slowperiod=10):
#     result = talib.PPO(df['close'], fastperiod=fastperiod, slowperiod=slowperiod)
#     return result

# def baseline_071(df, timeperiod=14):
#     result = talib.MINUS_DM(df['high'], df['low'], timeperiod=timeperiod)
#     return result

# def baseline_072(df, timeperiod=5):
#     rolling_std = df['close'].rolling(window=timeperiod).std(ddof=1)
#     return rolling_std

# def baseline_073(df, timeperiod=5):
#     result = talib.VAR(df['close'], timeperiod, 1)
#     return result

# def baseline_074(df, timeperiod=10):
#     median_price = (df['high'] + df['low']) / 2
#     result = talib.TRIMA(median_price, timeperiod=timeperiod)
#     return result

# def baseline_075(df, timeperiod=10):
#     result = talib.EMA((df['high'] + df['low'] + df['close']) / 3, timeperiod)
#     return result

# def baseline_076(df, timeperiod=14):
#     result = talib.NATR(df['high'], df['low'], df['close'], timeperiod=timeperiod)
#     return result

# def baseline_077(df, timeperiod=14):
#     result = talib.ULTOSC(df['high'], df['low'], df['close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)
#     return result

# def baseline_078(df, timeperiod=5):
#     median_price = (df['high'] + df['low']) / 2
#     result = talib.SMA(median_price, timeperiod)
#     return result

# def baseline_079(df, timeperiod=14):
#     result = talib.CORREL(df['close'], df['volume'], timeperiod)
#     return result

# def baseline_080(df, timeperiod=10):
#     result = talib.BETA(df['high'], df['low'], timeperiod)
#     return result

# @title Baseline Functions

def baseline_001(df, timeperiod=30):
    # Simple Moving Average
    result = df['close'].rolling(window=timeperiod).mean().to_numpy()
    return result

def baseline_002(df, timeperiod=30):
    # Exponential Moving Average
    result = df['close'].rolling(timeperiod).mean().to_numpy()
    return result

def baseline_003(df, timeperiod=30):
    # Weighted Moving Average
    weights = np.arange(1, timeperiod + 1)
    result = df['close'].rolling(timeperiod).apply(lambda prices: np.dot(prices, weights) / weights.sum(), raw=True).to_numpy()
    return result

# def baseline_004(df, timeperiod=30):
#     # Triple Exponential Moving Average (T3)
#     ema1 = df['close'].ewm(span=timeperiod, adjust=False).mean()
#     ema2 = ema1.ewm(span=timeperiod, adjust=False).mean()
#     ema3 = ema2.ewm(span=timeperiod, adjust=False).mean()
#     ema4 = ema3.ewm(span=timeperiod, adjust=False).mean()
#     ema5 = ema4.ewm(span=timeperiod, adjust=False).mean()
#     ema6 = ema5.ewm(span=timeperiod, adjust=False).mean() 

#     c1, c2, c3, c4, c5, c6 = 1, 2, 3, 4, 5, 6  # T3 coefficients
#     result = c1 * ema1 - c2 * ema2 + c3 * ema3 - c4 * ema4 + c5 * ema5 - c6 * ema6
#     return result.to_numpy()

def baseline_004(df, timeperiod=30):
    sma1 = df['close'].rolling(window=timeperiod).mean()
    sma2 = sma1.rolling(window=timeperiod).mean()
    sma3 = sma2.rolling(window=timeperiod).mean()
    sma4 = sma3.rolling(window=timeperiod).mean()
    sma5 = sma4.rolling(window=timeperiod).mean()
    sma6 = sma5.rolling(window=timeperiod).mean()
    
    c1, c2, c3, c4, c5, c6 = 1, 2, 3, 4, 5, 6
    result = c1 * sma1 - c2 * sma2 + c3 * sma3 - c4 * sma4 + c5 * sma5 - c6 * sma6
    return result.to_numpy() 

def baseline_005(df, timeperiod=60):
    # Relative Strength Index (RSI)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.to_numpy() 

# def baseline_006(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # MACD
#     fast_ema = df['amount'].ewm(span=fastperiod, adjust=False).mean()
#     slow_ema = df['amount'].ewm(span=slowperiod, adjust=False).mean()
#     macd = fast_ema - slow_ema 
#     return macd.to_numpy() 

def baseline_006(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # MACD
    fast_ema = df['amount'].rolling(window=fastperiod).mean()
    slow_ema = df['amount'].rolling(window=slowperiod).mean()
    macd = fast_ema - slow_ema 
    return macd.to_numpy() 

def baseline_007(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # MACD Signal
    fast_ema = df['net_buy_amount'].rolling(window=fastperiod).mean()
    slow_ema = df['net_buy_amount'].rolling(window=slowperiod).mean()
    macd = fast_ema - slow_ema
    signal = macd.rolling(signalperiod).mean()
    return signal.to_numpy() 
    

def baseline_008(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # MACD Histogram
    fast_ema = df['large_buy'].rolling(window=fastperiod).mean()
    slow_ema = df['large_buy'].rolling(window=slowperiod).mean()
    macd = fast_ema - slow_ema
    signal = macd.rolling(signalperiod).mean()
    macd_hist = macd - signal
    return macd_hist.to_numpy() 

def baseline_009(df, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0):
    # Bollinger Bands - Upper Band
    middle_band = df['close'].pct_change(2) 
    stddev = df['close'].pct_change(10) 
    upper_band = stddev - middle_band 
    return upper_band.to_numpy() 

def baseline_010(df, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0):
    # Bollinger Bands - Middle Band
    middle_band =df['close'].pct_change()
    result = middle_band.pct_change().values
    return result

def baseline_011(df, timeperiod=20):
    # Bollinger Bands - Lower Band
    middle_band = df['high'].rolling(window=timeperiod).max()
    stddev = df['low'].rolling(window=timeperiod).min()
    lower_band = middle_band - stddev
    return lower_band.to_numpy()

def baseline_012(df, timeperiod=10):
    # Momentum
    mom = df['close'].diff(periods=timeperiod)
    return mom.to_numpy()

def baseline_013(df, timeperiod=14):
    # Commodity Channel Index (CCI)
    tp = (df['high'] + df['low'] + df['close']) / 3
    sma_tp = tp.rolling(window=timeperiod).mean()
    mean_dev = tp.rolling(window=timeperiod).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    cci = (tp - sma_tp) / (0.015 * mean_dev)
    return cci.to_numpy()

def baseline_014(df, timeperiod=14):
    # Chande Momentum Oscillator (CMO)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    cmo = 100 * (gain - loss) / (gain + loss)
    return cmo.to_numpy()

def baseline_015(df, timeperiod=10):
    # Rate of Change (ROC)
    roc = df['large_order'].pct_change(periods=timeperiod)
    return roc.to_numpy()

def baseline_016(df, timeperiod=14):
    # Average Directional Index (ADX)
    high = df['high']
    low = df['low']
    close = df['close']
    plus_dm = np.where(high.diff() > low.diff(), high.diff(), 0)
    minus_dm = np.where(low.diff() > high.diff(), low.diff(), 0)
    sma_plus = pd.Series(plus_dm).rolling(window=timeperiod).mean()
    sma_minus = pd.Series(minus_dm).rolling(window=timeperiod).mean()
    tr = np.maximum.reduce([high - low, high - close.shift(1), low - close.shift(1)])
    sma_tr = pd.Series(tr).rolling(window=timeperiod).mean()
    plus_di = 100 * (sma_plus / sma_tr)
    minus_di = 100 * (sma_minus / sma_tr) 
    dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100 
    adx = dx.rolling(window=timeperiod).mean() 
    return adx.to_numpy() 

def baseline_017(df, timeperiod=14):
    # Average True Range (ATR)
    high = df['high']
    low = df['low']
    close = df['close']
    tr = np.maximum.reduce([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))])
    atr = pd.Series(tr).rolling(window=timeperiod).mean()
    return atr.to_numpy()

def baseline_018(df, timeperiod=60):
    # Rate of Change Percentage (ROCP)
    rocp = (df['buy_volume'] / df['sell_volume']).pct_change(periods=timeperiod)
    return rocp.to_numpy()

def baseline_019(df, timeperiod=3):
    # Rate of Change Ratio (ROCR)
    rocr = (df['close'] / df['close'].shift(timeperiod))
    return rocr.to_numpy()

def baseline_020(df, fastk_period=14, slowk_period=3, slowk_matype=0):
    # Stochastic Oscillator - %K
    low_min = df['low'].rolling(window=fastk_period).min()
    high_max = df['high'].rolling(window=fastk_period).max()
    slowk = (df['close'] - low_min) / (high_max - low_min) * 100
    result = slowk.rolling(window=slowk_period).mean()
    return result.to_numpy()

def baseline_021(df, fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0):
    # Stochastic Oscillator - %D
    low_min = df['low'].rolling(window=fastk_period).min()
    high_max = df['high'].rolling(window=fastk_period).max()
    slowk = (df['close'] - low_min) / (high_max - low_min) * 100
    slowkd = slowk.rolling(window=slowk_period).mean()
    slowd = slowkd.rolling(window=slowd_period).mean()
    return slowd.to_numpy()

def baseline_022(df, timeperiod=14):
    # Williams %R
    high_max = df['high'].rolling(window=timeperiod).max()
    low_min = df['low'].rolling(window=timeperiod).min()
    willr = (high_max - df['close']) / (high_max - low_min) * -100
    return willr.to_numpy()

def baseline_023(df, timeperiod=14):
    # Chande Momentum Oscillator (CMO) - Duplicate
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    cmo = 100 * (gain - loss) / (gain + loss)
    return cmo.to_numpy()

def baseline_024(df: pd.DataFrame) -> np.ndarray:
    # True Range
    tr = np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])
    return tr

def baseline_025(df, timeperiod=30):
    # Volume Weighted Moving Average (VWMA)
    vwma = (df['close'] * df['volume']).rolling(window=timeperiod).sum() / df['count'].rolling(window=timeperiod).sum()
    diff = (vwma - df['close']).rolling(window=timeperiod).std()
    return vwma.to_numpy()

def baseline_026(df, timeperiod=30):
    # Typical Price VWMA
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    result = typical_price.diff(3).diff(3).values
    return result

def baseline_027(df, timeperiod=30):
    typical_price = df['high'] - df['low']
    result = typical_price.diff().pct_change().values
    return result

def baseline_028(df, timeperiod=30):
    # Kaufman's Adaptive Moving Average (KAMA) 
    change = df['close'].diff(timeperiod) 
    volatility = df['close'].diff().abs().rolling(window=timeperiod).quantile(0.3)
    er = change / volatility 
    return er.to_numpy() 

# def baseline_029(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # MACD Difference
#     fast_ema = df['close'].ewm(span=fastperiod, min_periods=fastperiod).mean()
#     slow_ema = df['close'].ewm(span=slowperiod, min_periods=slowperiod).mean()
#     macd = fast_ema - slow_ema
#     signal = macd.ewm(span=signalperiod, min_periods=signalperiod).mean()
#     macdhist = macd - signal 
#     return macdhist - macd + signal 

def baseline_029(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # 简单移动平均 (SMA) 替代 EMA
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=fastperiod).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=slowperiod).mean()
    macd = fast_sma - slow_sma
    signal = macd.rolling(window=signalperiod, min_periods=signalperiod).mean()
    macdhist = macd - signal
    return (macdhist - macd + signal).to_numpy()

def baseline_030(df, timeperiod1=9, timeperiod2=150):
    # EMA Upper Band
    ema1 = df['close'].rolling(timeperiod1).mean()
    ema2 = df['close'].rolling(timeperiod2).mean()
    upperband = ema1 + ema2 
    return upperband.to_numpy() 

def baseline_031(df, timeperiod=20, nbdevdn=2):
    # EMA Lower Band
    ema = df['close'].rolling(timeperiod).mean()
    stddev = df['close'].rolling(window=timeperiod).std()
    upperband = ema + nbdevdn * stddev
    lowerband = ema - nbdevdn * stddev
    (upperband - lowerband) / (upperband - lowerband).rolling(120).mean()
    return lowerband.to_numpy()

def baseline_032(df, timeperiod=10):
    # Momentum on Open
    mom = df['open'].diff(periods=timeperiod)
    return mom.to_numpy()

def baseline_033(df, fastperiod=12, slowperiod=26):
    # Simple Moving Average Oscillator (SMAO)
    ema_fast = df['close'].rolling(fastperiod).mean()
    ema_slow = df['close'].rolling(slowperiod).mean()
    smmao = ema_fast - ema_slow
    return smmao.to_numpy()

def baseline_034(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # MACD Signal Minus Histogram (SMAO Variant)
    fast_ema = df['close'].rolling(fastperiod).mean()
    slow_ema = df['close'].rolling(slowperiod).mean()
    macd = fast_ema - slow_ema
    signal = macd.rolling(signalperiod).mean()
    macdhist = macd - signal
    smmao = macdhist - signal
    return smmao.to_numpy()

def baseline_035(df, timeperiod=30):
    # Triple Exponential Moving Average (TRIX)
    ema1 = df['large_order'].rolling(timeperiod).mean()
    ema2 = df['small_order'].rolling(timeperiod).mean()
    ema3 = (ema2 - ema1).rolling(timeperiod).mean()
    return ema3.to_numpy()

def baseline_036(df, timeperiod1=12, timeperiod2=26): 
    # Double Weighted Moving Average Oscillator (DWMAO) 
    wma1 = df['close'].rolling(window=timeperiod1).apply(lambda prices: np.dot(prices, np.arange(1, timeperiod1 + 1)) / sum(np.arange(1, timeperiod1 + 1)), raw=True)
    wma2 = df['close'].rolling(window=timeperiod2).apply(lambda prices: np.dot(prices, np.arange(1, timeperiod2 + 1)) / sum(np.arange(1, timeperiod2 + 1)), raw=True)
    dwmao = wma1 - wma2 
    return dwmao.to_numpy() 

def baseline_037(df, timeperiod=14):
    # Standard Error of Volatility Indicator (SEVI)
    max_price = df['high'].rolling(window=timeperiod).max()
    min_price = df['low'].rolling(window=timeperiod).min()
    sevi = max_price - min_price 
    return sevi.to_numpy() 

def baseline_038(df, timeperiod=10):
    # Smoothed Momentum
    mom = df['close'].diff(periods=timeperiod)
    smom = mom.rolling(window=timeperiod//2).mean()
    return smom.to_numpy()

def baseline_039(df, timeperiod=10):
    # Smoothed Weighted Momentum
    mom = df['close'].diff(periods=timeperiod)
    weights = np.arange(1, (timeperiod//2) + 1)
    spmom = mom.rolling(window=timeperiod//2).apply(lambda x: np.dot(x, weights)/weights.sum(), raw=True)
    return spmom.to_numpy()

# def baseline_040(df: pd.DataFrame) -> np.ndarray:
#     # Volume Price Trend (VPT)
#     vpt = pd.Series(dtype=np.float64, index=df.index)
#     vpt.iloc[0] = 0 
#     for t in range(1, len(df)):
#         vpt.iloc[t] = vpt.iloc[t-1] + (df['volume'].iloc[t] * (df['close'].iloc[t] - df['close'].iloc[t-1]) / df['close'].iloc[t-1])
#     return vpt.to_numpy() 

def baseline_040(df: pd.DataFrame) -> np.ndarray:
    # Volume Price Trend (VPT)
    delta_price = df['close'].diff()
    pct_change = delta_price / df['close'].shift(1)
    vpt = (pct_change * df['volume']).fillna(0).rolling(360).sum() 
    return vpt.to_numpy()

def baseline_041(df, shortperiod=12, longperiod=26):
    # Volume Oscillator
    short_vo = df['volume'].rolling(window=shortperiod).mean()
    long_vo = df['volume'].rolling(window=longperiod).mean()
    vo = short_vo - long_vo
    return vo.to_numpy()

def baseline_042(df, timeperiod=14):
    # Relative Strength Index (RSI) on Volume
    delta = df['volume'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.to_numpy()

def baseline_043(df, timeperiod=14):
    # True Range Average
    high = df['high']
    low = df['low']
    close = df['close']
    tr = np.maximum.reduce([high - low, np.abs(high - close.shift(1)), np.abs(low - close.shift(1))])
    atr = pd.Series(tr).rolling(window=timeperiod).mean()
    sma_atr = atr.rolling(window=timeperiod//2).mean()
    tra = atr - sma_atr
    return tra.to_numpy()

def baseline_044(df, timeperiod=14):
    # Linear Regression Slope
    result = df['close'].rolling(window=timeperiod).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)
    return result.to_numpy()

def baseline_045(df, timeperiod=90):
    # Linear Regression Intercept
    result = (df['buy_amount'] - df['sell_amount']).rolling(window=timeperiod).apply(lambda x: np.polyfit(range(len(x)), x, 1)[1], raw=True)
    return result.to_numpy()

def baseline_046(df, timeperiod=30):
    # Linear Regression
    result = (df['large_buy'] - df['large_sell']).rolling(window=timeperiod).apply(lambda x: np.polyval(np.polyfit(range(len(x)), x, 1), len(x)-1), raw=True)
    return result.to_numpy()

def baseline_047(df, timeperiod=300):
    # Typical Price
    result = (df['small_buy'] - df['small_sell']).rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_048(df: pd.DataFrame) -> np.ndarray:
    # Weighted Close Price
    result = df['active_buy_ratio_5m'].rolling(window=30).mean()
    return result.to_numpy()

def baseline_049(df: pd.DataFrame) -> np.ndarray:
    # HT Trendline (Approximation)
    result = (df['small_buy'] - df['small_sell']).rolling(150).rank()
    return result.to_numpy()

def baseline_050(df: pd.DataFrame) -> np.ndarray:
    # Average True Range over 10 periods
    result = (df['high'] - df['low']).rolling(window=10).mean()
    return result.to_numpy()

def baseline_051(df, timeperiod=10):
    # Close minus SMA
    sma = df['close'].rolling(window=timeperiod).mean()
    diff = df['close'] - sma
    return diff.to_numpy()

def baseline_052(df, timeperiod=14):
    # Difference between high and low SMA
    high_ma = df['high'].rolling(window=timeperiod).mean()
    low_ma = df['low'].rolling(window=timeperiod).mean()
    dhlma = high_ma - low_ma
    return dhlma.to_numpy()

def baseline_053(df, timeperiod=14):
    # Beta between high and low
    high = df['high']
    low = df['low']
    cov = high.rolling(window=timeperiod).cov(low)
    var = low.rolling(window=timeperiod).var()
    beta = cov / var
    return beta.to_numpy()

def baseline_054(df, timeperiod=14):
    # Volume Weighted Average Price Oscillator
    avg_price = (df['high'] + df['low'] + df['close']) / 3
    vwa = (avg_price * df['volume']).rolling(window=timeperiod).sum() / df['volume'].rolling(window=timeperiod).sum()
    vo = avg_price - vwa
    return vo.to_numpy()

def baseline_055(df, timeperiod=14):
    # Sum of Squared Deviations
    mean = df['close'].rolling(window=timeperiod).mean()
    ssd = ((df['close'] - mean) ** 2).rolling(window=timeperiod).sum()
    return ssd.to_numpy()

def baseline_056(df, fast_period=12, slow_period=26):
    # Volume Weighted Oscillator (Approximation)
    short_ma = df['volume'].rolling(window=fast_period).mean()
    long_ma = df['volume'].rolling(window=slow_period).mean()
    vwo = short_ma - long_ma
    return vwo.to_numpy()

def baseline_057(df, timeperiod=10):
    # Simple Oscillator
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    oscillator = typical_price.diff(periods=timeperiod)
    return oscillator.to_numpy()

def baseline_058(df, timeperiod=14):
    # Standard Deviation of Volume
    result = df['volume'].rolling(window=timeperiod).std()
    return result.to_numpy()

def baseline_059(df, timeperiod=20, nbdevup=2, nbdevdn=2):
    # Bollinger Band Width
    rolling_mean = df['close'].rolling(window=timeperiod).mean()
    rolling_std = df['close'].rolling(window=timeperiod).std()
    upperband = rolling_mean + (nbdevup * rolling_std)
    lowerband = rolling_mean - (nbdevdn * rolling_std)
    bbw = upperband - lowerband
    return bbw.to_numpy()

def baseline_060(df, timeperiod=10):
    # Price Spread Difference
    spread = df['high'] - df['low']
    psdiff = spread.diff(periods=timeperiod)
    return psdiff.to_numpy()

def baseline_061(df, short_period=12, long_period=26):
    # Triple Moving Average (TMA)
    short_ma = df['close'].rolling(window=short_period).mean()
    long_ma = df['close'].rolling(window=long_period).mean()
    tma = short_ma - long_ma
    return tma.to_numpy()

def baseline_062(df: pd.DataFrame) -> np.ndarray:
    # Ratio of High to Low
    result = df['high'] / df['low']
    return result.to_numpy()

def baseline_063(df, timeperiod=20):
    # Polynomial Fit Slope
    result = df['close'].rolling(window=timeperiod).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)
    return result.to_numpy()

def baseline_064(df, timeperiod=14):
    # Absolute Price Oscillator (APO) 
    fast_ema = df['close'].rolling(12).mean() 
    slow_ema = df['close'].rolling(26).mean() 
    apo = fast_ema - slow_ema 
    return apo.to_numpy() 

def baseline_065(df: pd.DataFrame) -> np.ndarray: 
    # Balance of Power (BOP) 
    bop = (df['close'] - df['open']) / (df['high'] - df['low']) 
    return bop.to_numpy() 

def baseline_066(df, timeperiod=14):
    # Directional Movement Index (DX) 
    plus_dm = np.where(df['high'].diff() > df['low'].diff(), df['high'].diff(), 0) 
    minus_dm = np.where(df['low'].diff() > df['high'].diff(), df['low'].diff(), 0) 
    tr = np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])
    sma_tr = pd.Series(tr).rolling(window=timeperiod).mean()
    plus_di = 100 * pd.Series(plus_dm).rolling(window=timeperiod).mean() / sma_tr
    minus_di = 100 * pd.Series(minus_dm).rolling(window=timeperiod).mean() / sma_tr
    dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.to_numpy()

def baseline_067(df, timeperiod=14):
    # Minus Directional Indicator
    minus_dm = np.where(df['low'].diff() > df['high'].diff(), df['low'].diff(), 0)
    tr = np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])
    minus_di = 100 * pd.Series(minus_dm).rolling(window=timeperiod).mean() / pd.Series(tr).rolling(window=timeperiod).mean()
    return minus_di.to_numpy()

def baseline_068(df, timeperiod=14):
    # Plus Directional Indicator
    plus_dm = np.where(df['high'].diff() > df['low'].diff(), df['high'].diff(), 0)
    tr = np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])
    plus_di = 100 * pd.Series(plus_dm).rolling(window=timeperiod).mean() / pd.Series(tr).rolling(window=timeperiod).mean()
    return plus_di.to_numpy()

def baseline_069(df, timeperiod=14):
    # Plus Directional Movement
    plus_dm = np.where(df['high'].diff() > df['low'].diff(), df['high'].diff(), 0)
    return pd.Series(plus_dm).rolling(window=timeperiod).mean().to_numpy()

def baseline_070(df, fastperiod=3, slowperiod=10):
    # Percentage Price Oscillator (PPO)
    fast_ema = df['close'].rolling(fastperiod).mean()
    slow_ema = df['close'].rolling(slowperiod).mean()
    ppo = ((fast_ema - slow_ema) / slow_ema) * 100
    return ppo.to_numpy()

def baseline_071(df, timeperiod=14):
    # Minus Directional Movement
    minus_dm = np.where(df['low'].diff() > df['high'].diff(), df['low'].diff(), 0)
    return pd.Series(minus_dm).rolling(window=timeperiod).mean().to_numpy()

def baseline_072(df, timeperiod=5):
    # Rolling Standard Deviation
    rolling_std = df['close'].rolling(window=timeperiod).std(ddof=1)
    return rolling_std.to_numpy()

def baseline_073(df, timeperiod=5):
    # Variance
    result = df['close'].rolling(window=timeperiod).var()
    return result.to_numpy()

def baseline_074(df, timeperiod=10):
    # Triangular Moving Average
    median_price = (df['high'] + df['low']) / 2
    result = median_price.rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_075(df, timeperiod=10):
    # Exponential Moving Average of Typical Price
    result = df['large_pct_30m'] / df['large_pct_30m'].rolling(300).mean()
    return result.to_numpy()

def baseline_076(df, timeperiod=14):
    # Normalized Average True Range
    high = df['high']
    low = df['low']
    close = df['close']
    tr = np.maximum(0, high - low)  # 1. High - Low
    tr = np.maximum(tr, np.abs(high - np.roll(close, 1)))  # 2. High - Previous Close
    tr = np.maximum(tr, np.abs(low - np.roll(close, 1)))   # 3. Low - Previous Close
    atr = pd.Series(tr).rolling(window=timeperiod).mean()
    natr = (atr / close) * 100
    return natr.to_numpy()

def baseline_077(df, timeperiod=14):
    # Ultimate Oscillator
    high = df['high']
    low = df['low']
    close = df['close']
    bp = close - np.minimum(low, close.shift(1))
    tr = np.maximum(high - low, np.maximum(np.abs(high - close.shift(1)), np.abs(low - close.shift(1))))
    avg7 = pd.Series(bp).rolling(7).sum() / pd.Series(tr).rolling(7).sum()
    avg14 = pd.Series(bp).rolling(14).sum() / pd.Series(tr).rolling(14).sum()
    avg28 = pd.Series(bp).rolling(28).sum() / pd.Series(tr).rolling(28).sum()
    ultosc = (4 * avg7 + 2 * avg14 + avg28) / (4 + 2 + 1) * 100
    return ultosc.to_numpy()

def baseline_078(df, timeperiod=5):
    # Simple Moving Average of Median Price
    result = df['large_pct_120m'] / df['large_pct_120m'].rolling(300).mean()
    return result.to_numpy()

def baseline_079(df, timeperiod=14):
    # Correlation between Close and Volume
    result = df['close'].rolling(window=timeperiod).corr(df['volume'])
    return result.to_numpy()

def baseline_080(df, timeperiod=10):
    # Beta
    high = df['high']
    low = df['low']
    cov = high.rolling(window=timeperiod).cov(low)
    var = low.rolling(window=timeperiod).var()
    beta = cov / var
    return beta.to_numpy()

def baseline_081(df: pd.DataFrame) -> np.ndarray:
    # 价格高点与收盘价之差对滚动平均价格的比例
    avg_price_roll = df['vwap'].rolling(30).mean()
    result = (df['high'] - df['close']) / avg_price_roll
    return result.to_numpy()

def baseline_082(df: pd.DataFrame) -> np.ndarray:
    # 收盘价与最低价之差对卖出和买入差额的比例
    result = (df['close'] - df['low']) / (df['sell_amount'] - df['buy_amount'])
    return result.to_numpy()

def baseline_083(df, timeperiod=14):
    # 收盘价在最近一段时间内的相对位置
    min_close = df['close'].rolling(window=timeperiod).min() 
    max_close = df['close'].rolling(window=timeperiod).max() 
    result = (df['close'] - min_close) / (max_close - min_close) 
    return result.to_numpy() 

def baseline_084(df: pd.DataFrame) -> np.ndarray:
    # 收盘价与开盘价之差对卖出平均价格的比例
    result = (df['close'] - df['open']) / df['sell_vwap'] 
    return result.to_numpy()

def baseline_085(df, timeperiod=30):
    # 收盘价与其滚动均值和标准差的z分数
    sma_close = df['close'].rolling(window=timeperiod, min_periods=5).mean()
    stddev_close = df['close'].rolling(window=timeperiod, min_periods=5).std()
    result = (df['close'] - sma_close) / stddev_close
    return result.to_numpy()

def baseline_086(df, timeperiod=14):
    # 货币流量指标（MFI）
    tp = (df['high'] + df['low'] + df['close']) / 3
    mf = tp * df['volume']
    posmf = (tp - tp.shift(1)).clip(lower=0) * df['volume']
    negmf = (tp.shift(1) - tp).clip(lower=0) * df['volume']
    result = 100 * posmf.rolling(window=timeperiod).sum() / (posmf.rolling(window=timeperiod).sum() + negmf.rolling(window=timeperiod).sum())
    return result.to_numpy()

def baseline_087(df, timeperiod=14):
    # 累积/分配线
    mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    mfv = mfm * df['volume']
    acc_dist = mfv.rolling(360).sum()
    return acc_dist.to_numpy()

def baseline_088(df, timeperiod=14):
    # EMA三次平滑差分百分比
    ema1 = df['close'].rolling(timeperiod).mean()
    ema2 = ema1.rolling(timeperiod).mean()
    ema3 = ema2.rolling(timeperiod).mean()
    result = ((ema1 - ema2) / ema3.shift(1)) * 100
    return result.to_numpy()

def baseline_089(df, fastperiod=12, slowperiod=26):
    # 快速和慢速EMA的差分
    short_ema = df['close'].rolling(fastperiod).mean()
    long_ema = df['close'].rolling(slowperiod).mean()
    result = short_ema - long_ema
    return result.to_numpy()

def baseline_090(df, timeperiod=14):
    # 典型价格与其滚动平均和标准差的标准化差异
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    sma_typical_price = typical_price.rolling(window=timeperiod, min_periods=5).mean()
    stddev_typical_price = typical_price.rolling(window=timeperiod, min_periods=5).std()
    result = (typical_price - sma_typical_price) / stddev_typical_price
    return result.to_numpy()

def baseline_091(df, timeperiod=14):
    # 成交量滚动均值下的OBV指标
    obv = pd.Series(index=df.index, dtype=np.float64)
    obv.iloc[0] = 0
    for i in range(1, len(df)):
        change = 0
        if df['close'].iloc[i] > df['close'].iloc[i - 1]:
            change = df['volume'].iloc[i]
        elif df['close'].iloc[i] < df['close'].iloc[i - 1]:
            change = -df['volume'].iloc[i]
        obv.iloc[i] = obv.iloc[i - 1] + change
    return obv.to_numpy()

def baseline_092(df, timeperiod=10):
    # 收盘价在近期极值之间的相对位置
    min_close = df['close'].rolling(window=timeperiod).min()
    max_close = df['close'].rolling(window=timeperiod).max()
    result = (df['close'] - min_close) / (max_close - min_close)
    return result.to_numpy()

def baseline_093(df, timeperiod=10):
    # 多项式回归斜率
    typical_price = (df[['high', 'low', 'close']].sum(axis=1)) / 3
    slopes = []
    for end in range(timeperiod, len(df) + 1):
        prices_segment = typical_price[end - timeperiod:end]
        slope = np.polyfit(np.arange(timeperiod), prices_segment, 1)[0]
        slopes.append(slope)

    slopes = [np.nan] * (timeperiod - 1) + slopes
    return np.array(slopes)

def baseline_094(df, timeperiod=30):
    result = df['small_pct_30m'] / df['small_pct_30m'].rolling(300).mean()
    return result.to_numpy()

def baseline_095(df: pd.DataFrame) -> np.ndarray:
    # 滚动均值
    result = df['small_pct_120m'] / df['small_pct_120m'].rolling(300).mean()
    return result.to_numpy()

def baseline_096(df: pd.DataFrame) -> np.ndarray:
    # 滚动均值
    inphase = df['close'].rolling(window=5).mean()
    return inphase.to_numpy()

def baseline_097(df: pd.DataFrame) -> np.ndarray:
    # 滚动均值
    sine = df['close'].rolling(window=45).mean()
    return sine.to_numpy()

def baseline_098(df, timeperiod=14):
    # 收盘价最小值的索引
    min_idx = df['close'].rolling(window=timeperiod).min() / df['close'].rolling(window=timeperiod).max()
    return min_idx.values

def baseline_099(df, timeperiod=14):
    # 随机RSI
    min_price = df['close'].rolling(window=timeperiod).min()
    max_price = df['close'].rolling(window=timeperiod).max()
    stoch_rsi = (df['close'] - min_price) / (max_price - min_price)
    return stoch_rsi.to_numpy()

def baseline_100(df, timeperiod=180):
    # EMA
    result = df['close'].rolling(timeperiod).mean()
    return result.to_numpy()

def baseline_101(df, timeperiod=14):
    # 十字星判断
    doji = np.where((df['open'] == df['close']), 1, 0)
    return doji

def baseline_102(df, timeperiod=120):
    # 中位价格
    result = df['net_buy_small_pct_15m'].rolling(timeperiod).mean()
    return result.to_numpy()

def baseline_103(df, timeperiod=5, nbdevup=2, nbdevdn=2):
    # 典型价格布林带宽度
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    rolling_mean = typical_price.rolling(window=timeperiod).mean()
    rolling_std = typical_price.rolling(window=timeperiod).std()
    upperband = rolling_mean + (nbdevup * rolling_std)
    lowerband = rolling_mean - (nbdevdn * rolling_std)
    bbw = upperband - lowerband
    return bbw.to_numpy()

def calculate_ema(series, period):
    # 指数移动平均
    return series.rolling(period).mean()

def calculate_dema(series, period):
    # 双指数移动平均
    ema = calculate_ema(series, period)
    dema = 2 * ema - calculate_ema(ema, period)
    return dema

def baseline_104(df, timeperiod=60):
    # DEMA
    result = (df['high'] / df['low']).rolling(timeperiod).mean().values
    return result

def baseline_105(df, timeperiod=14):
    # 收盘价差分
    return df['close'].diff(periods=timeperiod).to_numpy()

def baseline_106(df, timeperiod=20):
    # EMA和高低价差
    ema_close = df['close'].rolling(timeperiod).mean()
    sma_high = df['high'].rolling(window=timeperiod).mean()
    sma_low = df['low'].rolling(window=timeperiod).mean()
    qlty = ema_close + (sma_high - sma_low)
    return qlty.to_numpy()

def baseline_107(df, timeperiod=10):
    # 典型价格与SMA的差异
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    sma_typical = typical_price.rolling(window=timeperiod).mean()
    result = (typical_price - sma_typical) / sma_typical
    return result.to_numpy()

def baseline_108(df: pd.DataFrame) -> np.ndarray:
    # 收盘价百分比变化
    result = (df['close'] - df['close'].shift(1)) / df['close'].shift(1)
    return result.to_numpy()

def baseline_109(df, timeperiod=14):
    # 上影线长度和
    uppershad = df['high'] - np.maximum(df['close'], df['open'])
    return uppershad.rolling(window=timeperiod).sum().to_numpy()

def baseline_110(df, timeperiod=14):
    # 下影线长度和
    lowershad = np.minimum(df['close'], df['open']) - df['low']
    return lowershad.rolling(window=timeperiod).sum().to_numpy()

def baseline_111(df, timeperiod=10):
    # 成交量总和
    return df['volume'].rolling(window=timeperiod).sum().to_numpy()

def baseline_112(df, timeperiod=14):
    # 收盘价变化百分比
    return (df['close'] / df['close'].shift(timeperiod) - 1).to_numpy()

def baseline_113(df, timeperiod=90):
    # 中位价格滚动均值
    midprice = np.log((df['high'] + df['low']) / 2)
    return midprice.rolling(window=timeperiod).quantile(0.2).to_numpy()

def baseline_114(df, timeperiod=10):
    # 收盘价的对数变化百分比
    result = 100 * np.log(df['close'] / df['close'].shift(timeperiod))
    return result.to_numpy()

def baseline_115(df, timeperiod=20):
    # 价格和区间差的累积百分比
    price_diff = df['close'] - df['open']
    range_diff = df['high'] - df['low']
    result = 100 * price_diff.rolling(window=timeperiod).sum() / range_diff.rolling(window=timeperiod).sum()
    return result.to_numpy()

def baseline_116(df, timeperiod=14):
    # 钱流均值和的差异
    tp = (df['high'] + df['low'] + df['close']) / 3
    mf = tp * df['volume']
    posmf = (tp - tp.shift(1)).clip(lower=0) * df['volume']
    negmf = (tp.shift(1) - tp).clip(lower=0) * df['volume']
    result = posmf.rolling(window=timeperiod).sum() - negmf.rolling(window=timeperiod).sum()
    return result.to_numpy()

def baseline_117(df, short_period=12, long_period=26):
    # 移动均值正负变化比
    close_price_diff = df['close'].diff()
    positive_diff = close_price_diff.where(close_price_diff > 0, 0)
    negative_diff = close_price_diff.where(close_price_diff < 0, 0).abs()
    pos_avg = positive_diff.rolling(window=short_period).mean()
    neg_avg = negative_diff.rolling(window=long_period).mean()
    result = pos_avg / neg_avg
    return result.to_numpy()

def baseline_118(df, timeperiod=140):
    # 中位价格滚动均值
    midpoint = df['close'].diff(3).rolling(window=timeperiod).corr(df['close'].pct_change(1))
    return midpoint.to_numpy()

def baseline_119(df, timeperiod=14):
    # 三重指数移动平均
    ema1 = df['close'].rolling(timeperiod).mean()
    ema2 = ema1.rolling(timeperiod).mean()
    ema3 = ema2.rolling(timeperiod).mean()
    tema = 3 * (ema1 - ema2) + ema3
    return tema.to_numpy()

def baseline_120(df: pd.DataFrame) -> np.ndarray:
    # 加权收盘价格
    wclprice = ((df['high'] + df['low'] + 2 * df['close']) / 4).pct_change(50).rolling(300).mean()
    return wclprice.to_numpy()

def baseline_121(df, timeperiod=14):
    # 上下影线与实体之间的滚动和
    upper_shadow = df['high'] - df[['close', 'open']].max(axis=1)
    lower_shadow = df[['close', 'open']].min(axis=1) - df['low']
    real_body = np.abs(df['close'] - df['open'])
    result = (upper_shadow - lower_shadow - real_body).rolling(window=timeperiod).sum()
    return result.to_numpy()

def baseline_122(df, timeperiod=14):
    # 平均价格变化
    high_change = df['high'].diff().abs()
    low_change = df['low'].diff().abs()
    close_change = df['close'].diff().abs()
    mean_change = (high_change + low_change + close_change) / 3
    return mean_change.to_numpy()

def baseline_123(df, timeperiod=14):
    # 价格变化均值
    high_change = df['high'].diff()
    low_change = df['low'].diff()
    close_change = df['close'].diff()
    mean_change = (high_change + low_change + close_change) / 3
    return mean_change.to_numpy()

def baseline_124(df: pd.DataFrame) -> np.ndarray:
    return df['buy_vwap'] / df['sell_vwap'].values

def baseline_125(df, timeperiod=20):
    # 移动均值增长率
    close_diff = df['close'].diff()
    up = np.where(close_diff > 0, close_diff, 0)
    down = np.where(close_diff < 0, -close_diff, 0)
    au = pd.Series(up).rolling(window=timeperiod).mean()
    ad = pd.Series(down).rolling(window=timeperiod).mean()
    result = au / (au + ad)
    return result.to_numpy()

def baseline_126(df, timeperiod=14):
    # Aroon震荡指标
    high = df['high'].values
    low = df['low'].values

    aroon_up = np.full_like(high, np.nan, dtype=np.float64)
    aroon_down = np.full_like(low, np.nan, dtype=np.float64)

    for i in range(timeperiod, len(high)):
        period_high = high[i-timeperiod:i+1]
        period_low = low[i-timeperiod:i+1]
        aroon_up[i] = 100 * np.argmax(period_high) / timeperiod
        aroon_down[i] = 100 * np.argmin(period_low) / timeperiod

    aroon_osc = aroon_up - aroon_down
    return aroon_osc

def baseline_127(df, timeperiod=14):
    # 布林带中线与上线的间距
    middle_band = df['close'].rolling(window=timeperiod).mean()
    upper_band = middle_band + 2 * df['close'].rolling(window=timeperiod).std()
    result = middle_band - upper_band
    return result.to_numpy()

def baseline_128(df, timeperiod=14):
    # 高低价最大间距
    result = (df['high'] - df['low']).rolling(window=timeperiod).max()
    return result.to_numpy()

def baseline_129(df, timeperiod=14):
    # 收盘价变化最大值
    result = (df['close'] - df['close'].shift(timeperiod)).rolling(window=timeperiod).max()
    return result.to_numpy()

# def baseline_130(df, timeperiod=14):
#     # 高低价KAMA差异
#     kama_high = df['high'].ewm(com=(timeperiod - 1) / 2).mean()
#     kama_low = df['low'].ewm(com=(timeperiod - 1) / 2).mean()
#     result = kama_high - kama_low
#     return result.to_numpy()

def baseline_130(df, timeperiod=14):
    # 高低价SMA差异
    sma_high = df['high'].rolling(window=timeperiod, min_periods=1).mean()
    sma_low = df['low'].rolling(window=timeperiod, min_periods=1).mean()
    result = sma_high - sma_low
    return result.to_numpy()

def baseline_131(df, timeperiod=14):
    # 成交量与其均值的比率
    result = df['volume'] / df['volume'].rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_132(df, timeperiod=10):
    # 高点减去收盘价的滚动和
    result = (df['high'] - df['close']).rolling(window=timeperiod).sum()
    return result.to_numpy()

def baseline_133(df, timeperiod=10):
    # 收盘价减去低点的滚动和
    result = (df['close'] - df['low']).rolling(window=timeperiod).sum()
    return result.to_numpy()

def baseline_134(df: pd.DataFrame) -> np.ndarray:
    # 判断锤子线形态
    is_hammer = (
        (df['high'] - df['low'] > 3 * (df['open'] - df['close'])) &
        ((df['close'] - df['low']) / (0.001 + df['high'] - df['low']) > 0.6) &
        ((df['open'] - df['low']) / (0.001 + df['high'] - df['low']) > 0.6)
    ).astype(int)
    return is_hammer.to_numpy()

def baseline_135(df: pd.DataFrame) -> np.ndarray:
    # 判断倒锤子线形态
    is_hangingman = (
        (df['high'] - df['low'] > 3 * abs(df['open'] - df['close'])) &
        (abs(df['close'] - df['high']) / (0.0001 + df['high'] - df['low']) > 0.6)
    ).astype(int)
    return is_hangingman.to_numpy()

def baseline_136(df: pd.DataFrame) -> np.ndarray:
    # 判断倒锤子线形态
    body = np.abs(df['close'] - df['open'])
    upper_shadow = df['high'] - np.maximum(df['close'], df['open'])
    lower_shadow = np.minimum(df['close'], df['open']) - df['low']
    inverted_hammer = (upper_shadow > body * 2) & (lower_shadow < body)
    return inverted_hammer.astype(int).values

def baseline_137(df: pd.DataFrame) -> np.ndarray:
    # 收盘与开盘价差分比成交量
    result = (df['close'] - df['open']) / df['volume']
    return result.values

def baseline_138(df, timeperiod=20):
    # Sum of absolute difference between close and open
    real_body = np.abs(df['close'] - df['open'])
    result = real_body.rolling(window=timeperiod).sum()
    return result.to_numpy()

def baseline_139(df: pd.DataFrame) -> np.ndarray:
    # Volume over its rolling mean
    volume_ratio = (df['volume'] / df['volume'].rolling(window=10).mean()).rolling(window=10).mean()
    return volume_ratio.to_numpy()

def baseline_140(df, timeperiod=60):
    # Minimum low over the period
    result = df['low'].rolling(window=timeperiod).min() - df['close']
    return result.to_numpy()

def baseline_141(df, timeperiod=60):
    # Maximum high over the period
    result = df['high'].rolling(window=timeperiod).max() - df['close']
    return result.to_numpy()

def baseline_142(df, timeperiod=14):
    # High-Low range over the period
    highest_high = df['high'].rolling(window=timeperiod).max()
    lowest_low = df['low'].rolling(window=timeperiod).min()
    result = highest_high - lowest_low
    return result.to_numpy()

def baseline_143(df: pd.DataFrame) -> np.ndarray:
    # High-Low range
    result = df['high'] - df['low']
    return result.to_numpy()

def baseline_144(df: pd.DataFrame) -> np.ndarray:
    # Close-Open difference
    result = df['close'] - df['open']
    return result.to_numpy()

def baseline_145(df, timeperiod=14):
    # Close price over its rolling mean
    result = df['close'] / df['close'].rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_146(df, timeperiod=19):
    # Exponential moving average
    result = (df['close'] / df['open']).rolling(timeperiod).mean()
    return result.to_numpy()

def baseline_147(df, timeperiod=14):
    # Relative Strength Index (RSI)
    diff = df['close'].diff(1)
    gains = np.where(diff > 0, diff, 0)
    losses = np.where(diff < 0, -diff, 0)
    avg_gain = pd.Series(gains).rolling(window=timeperiod).mean()
    avg_loss = pd.Series(losses).rolling(window=timeperiod).mean()
    rs = avg_gain / avg_loss
    result = 100 - (100 / (1 + rs))
    return result.to_numpy()

def baseline_148(df: pd.DataFrame) -> np.ndarray:
    # Binary indication of close > open
    result = (df['close'] > df['open']).astype(int)
    return result.to_numpy()

def baseline_149(df, timeperiod=14):
    # Accumulation/Distribution Line average
    mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    mfv = mfm * df['volume']
    ad_line = mfv.rolling(360).sum() 
    result = ad_line.rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_150(df, timeperiod=14):
    # Average True Range (ATR)
    tr1 = df['high'] - df['low']
    tr2 = np.abs(df['high'] - df['close'].shift(1))
    tr3 = np.abs(df['low'] - df['close'].shift(1))
    true_range = np.maximum.reduce([tr1, tr2, tr3])
    result = pd.Series(true_range).rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_151(df, timeperiod=14):
    # Rolling On-Balance Volume (OBV)
    obv = (df['close'].diff() > 0).astype(int) * df['volume'] - (df['close'].diff() < 0).astype(int) * df['volume']
    obv_cumsum = obv.rolling(360).sum()
    result = obv_cumsum.rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_152(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # MACD histogram
    ema_fast = df['close'].rolling(fastperiod).mean()
    ema_slow = df['close'].rolling(slowperiod).mean()
    macd = ema_fast - ema_slow
    signal = macd.rolling(signalperiod).mean()
    result = macd - signal
    return result.to_numpy()

def baseline_153(df, timeperiod=14):
    # Standard deviation of the True Range
    tr1 = df['high'] - df['low']
    tr2 = np.abs(df['high'] - df['close'].shift(1))
    tr3 = np.abs(df['low'] - df['close'].shift(1))
    true_range = np.maximum.reduce([tr1, tr2, tr3])
    result = pd.Series(true_range).rolling(window=timeperiod).std()
    return result.to_numpy()

def baseline_154(df, timeperiod=120):
    # EMA of Typical Price
    result = df['trade_time'] / df['trade_time'].rolling(timeperiod).mean()
    return result.to_numpy()

def baseline_155(df, timeperiod=150):
    # SMA of Typical Price
    result = df['active_buy_ratio_240m'].rolling(window=timeperiod).mean()
    return result.to_numpy()

def baseline_156(df, timeperiod=14):
    # RSI of the High-Low spread
    hl_spread = df['high'] - df['low']
    diff = hl_spread.diff(1)
    gains = np.where(diff > 0, diff, 0)
    losses = np.where(diff < 0, -diff, 0)
    avg_gain = pd.Series(gains).rolling(window=timeperiod).mean()
    avg_loss = pd.Series(losses).rolling(window=timeperiod).mean()
    rs = avg_gain / avg_loss
    result = 100 - (100 / (1 + rs))
    return result.to_numpy()

def baseline_157(df, timeperiod=30):
    # Kaufman's Adaptive Moving Average (approximation)
    close = df['volume']  # Incorrect input used, assuming it meant 'close'
    change = close.diff(timeperiod)
    volatility = close.diff().abs().rolling(window=timeperiod).sum()
    er = change / volatility
    return er.to_numpy()

# def baseline_158(df, timeperiod=60):
#     # Double Exponential Moving Average (DEMA)
#     ema = df['high'].ewm(span=timeperiod, adjust=False).mean()
#     dema = 2 * ema - ema.ewm(span=timeperiod, adjust=False).mean()
#     return dema.to_numpy()

def baseline_158(df, timeperiod=60):
    # Double Simple Moving Average (DSMA)
    sma1 = df['high'].rolling(window=timeperiod, min_periods=1).mean()
    sma2 = sma1.rolling(window=timeperiod, min_periods=1).mean()
    dsma = 2 * sma1 - sma2
    return dsma.to_numpy()

def baseline_159(df, timeperiod=14):
    # Sine of close prices
    result = np.sin(df['close'])
    return result.values

def baseline_160(df, timeperiod=14):
    # Cosine of close prices
    result = np.cos(df['close'])
    return result.values

def ht_dclevel(series):
    # Hilbert Transform DC Level approximation
    close = series.values
    dc = np.zeros(len(close))
    for i in range(6, len(close)):
        dc[i] = (2 * close[i] - close[i - 4]) / 3
    return dc

def ht_dcphase(series):
    # Hilbert Transform DC Phase approximation
    close = series.values
    phase = np.zeros(len(close))
    for i in range(6, len(close)):
        phase[i] = (close[i] - close[i - 4]) / 3
    return phase

def baseline_161(df, timeperiod=30):
    # HT DC Level smoothed
    ht_dc = ht_dclevel(df['close'])
    result = pd.Series(ht_dc).rolling(window=timeperiod, min_periods=3).mean()
    return result.values

def baseline_162(df, timeperiod=14):
    # HT DC Phase smoothed
    ht_dc = ht_dcphase(df['close'])
    result = pd.Series(ht_dc).rolling(window=timeperiod, min_periods=3).mean()
    return result.values

def baseline_163(df: pd.DataFrame) -> np.ndarray:
    # Difference between rolling min close and current close
    min_val = df['close'].rolling(30).min()
    result = (min_val - df['close']) / df['close']
    return result.values

def baseline_164(df: pd.DataFrame) -> np.ndarray:
    # Difference between rolling max close and current close
    max_val = df['close'].rolling(30).max()
    result = (max_val - df['close']) / df['close']
    return result.values

def baseline_165(df: pd.DataFrame) -> np.ndarray:
    # Daily percentage change
    result = df['close'].pct_change(periods=1)
    return result.values

def baseline_166(df: pd.DataFrame) -> np.ndarray:
    # 5-day percentage change
    result = df['close'].pct_change(periods=5)
    return result.values

def baseline_167(df: pd.DataFrame) -> np.ndarray:
    # 10-day percentage change
    result = df['close'].pct_change(periods=10)
    return result.values

def baseline_168(df: pd.DataFrame) -> np.ndarray:
    # Rolling 30-day mean of 5-day percentage change
    result = df['close'].pct_change(periods=5).rolling(30).mean()
    return result.values

def baseline_169(df, period=14):
    # Money Flow Index approximation
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    mf = typical_price * df['volume']
    pos_mf = mf.where(typical_price.diff() > 0, 0).rolling(window=period).sum()
    neg_mf = mf.where(typical_price.diff() < 0, 0).rolling(window=period).sum()
    mfi = 100 - (100 / (1 + (pos_mf / neg_mf)))
    return mfi.to_numpy()

def baseline_170(df, period=14):
    # Accumulation/Distribution Line
    mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    mfv = mfm * df['volume']
    acc_dist = mfv.rolling(360).sum()
    return acc_dist.to_numpy()

def baseline_171(df: pd.DataFrame) -> np.ndarray:
    # On-Balance Volume
    obv = pd.Series(index=df.index, dtype=np.float64)
    obv.iloc[0] = 0
    for i in range(1, len(df)):
        if df['close'].iloc[i] > df['close'].iloc[i - 1]:
            obv.iloc[i] = obv.iloc[i - 1] + df['volume'].iloc[i]
        elif df['close'].iloc[i] < df['close'].iloc[i - 1]:
            obv.iloc[i] = obv.iloc[i - 1] - df['volume'].iloc[i]
        else:
            obv.iloc[i] = obv.iloc[i - 1]
    return obv.to_numpy()

def baseline_172(df, period=14):
    # Average Directional Index approximation
    plus_dm = df['high'].diff().clip(lower=0)
    minus_dm = df['low'].diff().clip(upper=0).abs()
    tr = np.maximum(df['high'] - df['low'], np.maximum(abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))))
    tr_ema = tr.rolling(window=period).mean()
    plus_di = 100 * plus_dm.rolling(window=period).mean() / tr_ema
    minus_di = 100 * minus_dm.rolling(window=period).mean() / tr_ema
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.rolling(window=period).mean()
    return adx.to_numpy()

def baseline_173(df, period=14):
    # Absolute difference from shifted close
    result = (df['close'] - df['close'].shift(period)).abs()
    return result.values

def baseline_174(df, period=14):
    # Difference between rolling max and min close
    result = df['close'].rolling(period).max() - df['close'].rolling(period).min()
    return result.values

def baseline_175(df, period=14):
    # Deviation from rolling mean
    result = (df['close'] - df['close'].rolling(period).mean()).abs()
    return result.values

def baseline_176(df: pd.DataFrame) -> np.ndarray:
    # Relative position of close to open
    result = (df['close'] - df['open']) / df['open']
    return result.values

def baseline_177(df: pd.DataFrame) -> np.ndarray:
    # Close-high ratio with a shifted sum of high and low
    result = (df['high'] - df['close']) / (df['high'] + df['low']).diff(5).rolling(120).mean()
    return result.values

def baseline_178(df, period=14):
    # Close-low ratio with a shifted sum of high and low
    result = (df['close'] - df['low']) / (df['high'] + df['low']).diff(3).rolling(30).mean()
    return result.values

def baseline_179(df: pd.DataFrame) -> np.ndarray:
    # High-low range as a ratio of close
    result = (df['high'] - df['low']) / df['close']
    return result.values

def baseline_180(df: pd.DataFrame) -> np.ndarray:
    # Volume weighted average price
    result = (df['close'] * df['volume']).rolling(30).mean() / df['volume'].rolling(30).mean()
    return result.values

def baseline_181(df: pd.DataFrame) -> np.ndarray:
    # Volume ratio compared to rolling mean
    volume = df['volume'].values
    rolling_mean = pd.Series(volume).rolling(20).mean().values
    res = volume / rolling_mean
    return np.nan_to_num(res)

def baseline_182(df, period=14):
    # Volume ratio compared to rolling mean
    volume = df['volume'].values
    rolling_mean = pd.Series(volume).rolling(period).mean().values
    res = volume / rolling_mean
    return np.nan_to_num(res)

def baseline_183(df: pd.DataFrame) -> np.ndarray:
    # Close price percentage difference
    close = df['close'].values
    res = np.diff(close, prepend=close[0]) / close
    return res

def baseline_184(df, period=14):
    # Percentage change from shifted close
    close = df['close'].values
    shifted = np.roll(close, period)
    shifted[:period] = close[0]
    res = (close - shifted) / shifted
    return res

def baseline_185(df, period=14):
    # Average close-open over period
    result = pd.Series(df['close'] - df['open']).rolling(window=period).mean().values
    return result

def baseline_186(df, period=14):
    # Average high-low range over period
    result = (df['high'] - df['low']).rolling(window=period).mean().values
    return result

def baseline_187(df: pd.DataFrame) -> np.ndarray:
    # Relative change in close and open to high-low sum
    open_ = df['open'].values
    close = df['close'].values
    high = df['high'].values
    low = df['low'].values
    result = (close - open_) / (high + low)
    return result

def baseline_188(df, period=20):
    # Bollinger Bands width
    close = df['close'].values
    rolling_mean = pd.Series(close).rolling(window=period).mean().values
    rolling_std = pd.Series(close).rolling(window=period).std().values
    upper_band = rolling_mean + (2 * rolling_std)
    lower_band = rolling_mean - (2 * rolling_std)
    bandwidth = upper_band - lower_band
    return bandwidth

def baseline_189(df, short_period=20, long_period=90):
    # EMA difference
    short_ema = df['close'].rolling(short_period).mean()
    long_ema = df['close'].rolling(long_period).mean()
    result = (short_ema - long_ema).rolling(window=100,min_periods=30).corr(long_ema).values
    return result

def baseline_190(df: pd.DataFrame) -> np.ndarray:
    # Rolling max high-low range over 14 days
    result = (df['high'] - df['low']).rolling(window=14).max().values
    return result

def baseline_191(df, fastk_period=14, slowk_period=3, slowd_period=3):
    # Stochastic Oscillator difference
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    lowest_low = pd.Series(low).rolling(window=fastk_period).min().values
    highest_high = pd.Series(high).rolling(window=fastk_period).max().values
    fastk = 100 * ((close - lowest_low) / (highest_high - lowest_low))
    slowk = pd.Series(fastk).rolling(window=slowk_period).mean().values
    slowd = pd.Series(slowk).rolling(window=slowd_period).mean().values
    result = slowk - slowd
    return result

def baseline_192(df, period=14):
    # Relative strength compared to period min-max
    close = df['close'].values
    rolling_min = pd.Series(close).rolling(window=period).min().values
    rolling_max = pd.Series(close).rolling(window=period).max().values
    result = (close - rolling_min) / (rolling_max - rolling_min)
    return np.nan_to_num(result)

def baseline_193(df: pd.DataFrame) -> np.ndarray:
    # Momentum over timeperiod
    result = df['buy_amount'] / df['sell_amount']
    return result

def baseline_194(df, timeperiod=14):
    # Close deviation from SMA
    result = (df['buy_amount'] - df['sell_amount']) / (df['buy_amount'] + df['sell_amount'])
    return result

def baseline_195(df, short_period=12, long_period=26):
    # Normalized close deviation
    min_val = df['close'].rolling(window=long_period).min().values
    max_val = df['close'].rolling(window=long_period).max().values
    result = 2 * ((df['close'] - min_val) / (max_val - min_val)) - 1
    return result

def baseline_196(df, short_period=12, long_period=26):
    # EMA difference
    result = df['large_buy'].pct_change(10).values
    return result

def baseline_197(df, period=14):
    # RSI calculation
    close = df['close'].values
    diff = np.diff(close, prepend=close[0])
    gain = np.maximum(diff, 0)
    loss = -np.minimum(diff, 0)
    avg_gain = pd.Series(gain).rolling(window=period).mean().values
    avg_loss = pd.Series(loss).rolling(window=period).mean().values
    rs = avg_gain / avg_loss
    result = 100 - (100 / (1 + rs))
    return np.nan_to_num(result)

def baseline_198(df: pd.DataFrame) -> np.ndarray:
    # Aroon Oscillator approximation
    high = df['high'].values
    low = df['low'].values
    aroon_up = pd.Series(high).rolling(14).apply(lambda x: np.argmax(x)+1).values / 14 * 100
    aroon_down = pd.Series(low).rolling(14).apply(lambda x: np.argmin(x)+1).values / 14 * 100
    result = aroon_up - aroon_down
    return result

def baseline_199(df: pd.DataFrame) -> np.ndarray:
    # Stochastic RSI
    close = df['close'].values
    rolling_min = pd.Series(close).rolling(window=30).min().values
    rolling_max = pd.Series(close).rolling(window=30).max().values
    result = ((close - rolling_min) / (rolling_max - rolling_min)) * 100
    return np.nan_to_num(result)

def baseline_200(df: pd.DataFrame) -> np.ndarray:
    # Deviation from KAMA
    close = df['close'].values
    rolling_mean = pd.Series(close).rolling(window=30).mean().values
    rolling_std = pd.Series(close).rolling(window=30).std().values
    kama = rolling_mean + (close - rolling_mean) * (rolling_std / (rolling_std + 1))
    result = close - kama
    return result


# 一、趋势指标（Trend Indicators），关注不同时间周期的价格数据如何相互交叉Overlap Studies（交叉分析）

def TD_TI_001(df, timeperiod=5):
    # 计算简单移动平均线（SMA）
    result = df['close'].rolling(window=timeperiod).mean().values
    return result

def TD_TI_002(df, timeperiod=500):
    # 计算最高价和最低价的简单移动平均（SMA）
    result = ((df['high'] + df['low']) / 2).rolling(window=timeperiod).mean().values
    return result

def TD_TI_003(df, timeperiod=14):
    # 计算最高价和最低价的 SMA 之差
    high_ma = df['high'].rolling(window=timeperiod).mean().values
    low_ma = df['low'].rolling(window=timeperiod).mean().values
    dhlma = high_ma - low_ma
    return dhlma

def TD_TI_004(df, timeperiod=10):
    # 计算收盘价与简单移动平均（SMA）的差值
    sma = df['close'].rolling(window=timeperiod).mean().values
    diff = df['close'].values - sma
    return diff

def TD_TI_005(df, short_period=9, long_period=25):
    # 计算短期和长期 SMA 之差（趋势动量）
    short_ma = df['close'].rolling(window=short_period).mean().values
    long_ma = df['close'].rolling(window=long_period).mean().values
    tma = short_ma - long_ma
    return tma

def TD_TI_006(df, timeperiod=120):
    # 计算指数移动平均线（EMA）
    result = df['close'].rolling(timeperiod).mean().values
    return result

def TD_TI_007(df, timeperiod=10):
    # 计算典型价格（Typical Price）的简单移动平均（SMA）
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    result = typical_price.rolling(window=timeperiod).mean().values
    return result

def TD_TI_008(df, timeperiod=120):
    # 同上，计算典型价格的 SMA
    typical_price = df['high']/df['low']
    result = typical_price.rolling(window=timeperiod).mean().values
    return result

def TD_TI_009(df, timeperiod=20):
    # 结合 EMA 和 SMA
    ema = df['close'].rolling(timeperiod).mean().values
    sma_high = df['high'].rolling(window=timeperiod).mean().values
    sma_low = df['low'].rolling(window=timeperiod).mean().values
    qlty = ema + (sma_high - sma_low)
    return qlty

def TD_TI_010(df, timeperiod=30):
    # 计算三层 EMA 嵌套
    ema1 = df['close'].rolling(timeperiod).mean()
    ema2 = ema1.rolling(timeperiod).mean()
    ema3 = ema2.rolling(timeperiod).mean()
    return ema3.values

def TD_TI_011(df, timeperiod=14):
    # DEMA 双移动平均线
    ema = df['close'].rolling(timeperiod).mean()
    dema = 2 * ema - ema.rolling(timeperiod).mean()
    return dema.values

def TD_TI_012(df, timeperiod=14):
    # 计算高价的 DEMA
    ema = df['close'].rolling(timeperiod).mean()
    dema = 2 * ema - ema.rolling(timeperiod).mean()
    return dema.values

def TD_TI_013(df, timeperiod=30):
    # 移动平均
    result = (df['close']*df['volume']).rolling(window=timeperiod).mean().values
    return result

def TD_TI_014(df, timeperiod=20, nbdevup=2, nbdevdn=2):
    # 布林带上轨
    middle_band = df['close'].rolling(window=timeperiod).mean()
    std_dev = df['close'].rolling(window=timeperiod).std()
    upper_band = middle_band + nbdevup * std_dev
    return upper_band.values

def TD_TI_015(df, timeperiod=20, nbdevup=2, nbdevdn=2):
    # 布林带中轨
    middle_band = (df['close'] / df['open']).rolling(window=timeperiod).std()
    std_dev = (df['high'] / df['low']).rolling(window=timeperiod).std()
    return (middle_band - std_dev).rolling(window=timeperiod).sum().values

def TD_TI_016(df, timeperiod=20, nbdevup=2, nbdevdn=2):
    # 布林带下轨
    middle_band = df['close'].rolling(window=timeperiod).mean()
    std_dev = df['close'].rolling(window=timeperiod).std()
    lower_band = middle_band - nbdevdn * std_dev
    return lower_band.values

def TD_TI_017(df, timeperiod=20, nbdevup=2):
    # 基于 EMA 和标准差的上轨
    ema = df['close'].diff().rolling(timeperiod).mean()
    stddev = df['close'].diff().rolling(window=timeperiod).std()
    upper_band = ema / stddev
    return upper_band.values

def TD_TI_018(df, timeperiod=20):
    # 基于 EMA 和标准差的下轨
    ema = df['open'].pct_change(5).rolling(timeperiod).mean()
    stddev = df['open'].pct_change(5).rolling(window=timeperiod).std()
    lower_band = ema / stddev
    return lower_band.values

def TD_TI_019(df, timeperiod=20, nbdevup=2, nbdevdn=2):
    # 布林带宽度
    middle_band = df['close'].rolling(window=timeperiod).mean()
    std_dev = df['close'].rolling(window=timeperiod).std()
    upper_band = middle_band + nbdevup * std_dev
    lower_band = middle_band - nbdevdn * std_dev
    return (upper_band - lower_band).values

def TD_TI_020(df, timeperiod=5, nbdevup=2, nbdevdn=2):
    # 根据典型价格计算的布林带宽度
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    rolling_mean = typical_price.rolling(window=timeperiod).mean()
    rolling_std = typical_price.rolling(window=timeperiod).std()
    upper_band = rolling_mean + nbdevup * rolling_std
    lower_band = rolling_mean - nbdevdn * rolling_std
    return (upper_band - lower_band).values

def TD_TI_021(df, timeperiod=30):
    # 卡夫卡自适应移动平均（KAMA）
    kama = df['close'].copy() 
    for i in range(1, len(df)):
        kama.iloc[i] = kama.iloc[i-1] + (df['close'].iloc[i] - kama.iloc[i-1]) / timeperiod
    return kama.rolling(timeperiod).mean().values

def TD_TI_022(df, timeperiod=14):
    # 成交量自适应移动平均
    kama = df['amount'].copy()
    for i in range(1, len(df)):
        kama.iloc[i] = kama.iloc[i-1] + (df['amount'].iloc[i] - kama.iloc[i-1]) / timeperiod
    return kama.rolling(timeperiod).mean().values

# def TD_TI_023(df, timeperiod=14):
#     # 高低价的卡玛指数差异
#     kama_high = df['high'].ewm(com=(timeperiod - 1) / 2).mean()
#     kama_low = df['low'].ewm(com=(timeperiod - 1) / 2).mean()
#     return (kama_high - kama_low).values

def TD_TI_023(df, timeperiod=14):
    # 高低价的简单移动平均差异
    sma_high = df['high'].rolling(window=timeperiod, min_periods=1).mean()
    sma_low = df['low'].rolling(window=timeperiod, min_periods=1).mean()
    return (sma_high - sma_low).values

def TD_TI_024(df: pd.DataFrame) -> np.ndarray:
    # 价格与其卡玛平均线之差
    kama = df['close'].ewm(com=(30 - 1) / 2).mean()
    return (df['close'] - kama).values

def TD_TI_025(df, timeperiod=14):
    # 计算给定时间周期内的中间价格
    high_roll = df['high'].rolling(window=timeperiod).max()
    low_roll = df['low'].rolling(window=timeperiod).min()
    return ((high_roll + low_roll) / 2).values

def TD_TI_026(df, short_period=12, long_period=26):
    # 计算抛物线转向指标，用于识别趋势的潜在反转点
    sar = pd.Series(np.where(df['close'].shift(1) < df['close'], df['high'], df['low']), index=df.index)
    return (df['close'] - sar).values

def TD_TI_027(df, timeperiod=14):
    # 计算更复杂的抛物线转向/加速指标
    sar = pd.Series(np.where(df['close'].shift(1) < df['close'], df['high'], df['low']), index=df.index).rolling(window=timeperiod).max()  # Simplified SAR
    return (df['close'] - sar).values

# def TD_TI_028(df, timeperiod=30):
#     # 计算三重指数移动平均
#     single = df['close'].ewm(span=timeperiod).mean()
#     double = single.ewm(span=timeperiod).mean()
#     triple = double.ewm(span=timeperiod).mean()
#     return (3 * single - 3 * double + triple).values

def TD_TI_028(df, timeperiod=30):
    # 计算三重简单移动平均
    sma1 = df['close'].rolling(window=timeperiod, min_periods=1).mean()
    sma2 = sma1.rolling(window=timeperiod, min_periods=1).mean()
    sma3 = sma2.rolling(window=timeperiod, min_periods=1).mean()
    return (3 * sma1 - 3 * sma2 + sma3).values

def TD_TI_029(df, timeperiod=10):
    # 计算三角移动平均
    median_price = (df['high'] + df['low']) / 2
    tri_ma = median_price.rolling(window=timeperiod).apply(lambda x: np.mean(x[-(timeperiod // 2):]), raw=True)
    return tri_ma.values

def TD_TI_030(df, timeperiod=300):
    # 计算加权移动平均线
    weights = np.arange(1, timeperiod + 1)
    wma = df['close'].rolling(window=timeperiod).apply(lambda prices: np.dot(prices, weights) / weights.sum(), raw=True)
    return wma.values

# def TD_TI_031(df, timeperiod1=12, timeperiod2=26):
#     # 计算两个不同时间周期的加权移动平均线之差
#     wma1 = pd.Series(np.convolve(df['close'].values, np.arange(1, timeperiod1 + 1)/sum(np.arange(1, timeperiod1 + 1)), 'valid'), index=df.index[-len(df)+timeperiod1-1:])
#     wma2 = pd.Series(np.convolve(df['close'].values, np.arange(1, timeperiod2 + 1)/sum(np.arange(1, timeperiod2 + 1)), 'valid'), index=df.index[-len(df)+timeperiod2-1:])
#     return (wma1 - wma2).values

def TD_TI_031(df, timeperiod=60):
    # Calculate weights for WMA
    return df['large_order'].rolling(window=timeperiod,min_periods=10).corr(df['small_order']).values

def TD_TI_032(df, timeperiod=60):
    return (df['high'] - df['low']).rolling(window=timeperiod).rank().values

def TD_TI_033(df,timeperiod=300):
    # 指数加权移动平均线
    return df['open'].rolling(window=timeperiod,min_periods=100).corr(df['volume']).values

def TD_TI_034(df, timeperiod=14):
    # 计算平均趋向指数
    high_diff = df['net_buy_large'].diff(1)
    low_diff = df['net_buy_small'].diff(1)
    return (high_diff / low_diff).values

def TD_TI_035(df, timeperiod=14):
    # ADXR平均趋向指数的趋向指数
    return df['net_buy_amount'] / df['net_buy_amount'].rolling(30).mean().values

def TD_TI_036(df, timeperiod=14):
    # Aroon上线
    rolling_high = df['high'].rolling(window=timeperiod+1, center=False)
    aroon_up = 100 * rolling_high.apply(lambda x: x.argmax(), raw=True) / timeperiod
    return aroon_up.values

def TD_TI_037(df, timeperiod=14):
    # Aroon下线
    rolling_low = df['low'].rolling(window=timeperiod+1, center=False)
    aroon_down = 100 * rolling_low.apply(lambda x: x.argmin(), raw=True) / timeperiod
    return aroon_down.values

def TD_TI_038(df, timeperiod=14):
    # Aroon振荡指数
    rolling_high = df['high'].rolling(window=timeperiod+1, center=False)
    aroon_up = 100 * rolling_high.apply(lambda x: x.argmax(), raw=True) / timeperiod
    rolling_low = df['low'].rolling(window=timeperiod+1, center=False)
    aroon_down = 100 * rolling_low.apply(lambda x: x.argmin(), raw=True) / timeperiod
    return aroon_up - aroon_down

def TD_TI_039(df, timeperiod=14):
    # AROONOSC振荡指数，衡量价格达到新高或新低的速度
    rolling_high = df['high'].rolling(window=timeperiod+1, center=False)
    aroon_up = 100 * rolling_high.apply(lambda x: x.argmax(), raw=True) / timeperiod
    rolling_low = df['low'].rolling(window=timeperiod+1, center=False)
    aroon_down = 100 * rolling_low.apply(lambda x: x.argmin(), raw=True) / timeperiod
    aroonosc = pd.Series(aroon_up - aroon_down).pct_change(10)
    return aroonosc.values

def TD_TI_040(df, timeperiod=14):
    # 计算过去一段时间内的最低价
    result = df['low'].rolling(window=timeperiod).min().values
    return result

def TD_TI_041(df, timeperiod=14):
    # 计算过去一段时间内的最高价
    result = df['high'].rolling(window=timeperiod).max().values
    return result

def TD_TI_042(df, timeperiod=14):
    # 计算过去一段时间内最高价与最低价之差
    highest_high = df['high'].rolling(window=timeperiod).max()
    lowest_low = df['low'].rolling(window=timeperiod).min()
    result = highest_high - lowest_low
    return result.values

def TD_TI_043(df, timeperiod=30):
    # 计算当前收盘价与过去一段时间内平均收盘价的比率
    result = df['close'] / df['close'].rolling(window=timeperiod).mean()
    return result.values

def TD_TI_044(df, timeperiod=5):
    # 通过取当前价格的两倍减去四天前价格的平均值（再除以 3）来计算，并计算其滚动平均值
    ht_dc = (2 * df['close'] - df['close'].rolling(window=timeperiod).mean()) / 3
    return ht_dc.rolling(window=timeperiod).mean().values

def TD_TI_045(df, timeperiod=14):
    # 通过取当前价格与5min前价格的差值（再除以 3）来计算，并计算其滚动平均值
    ht_dc = (df['close'] - df['close'].diff(5)) / 3
    return ht_dc.rolling(window=timeperiod).mean().values


# 二、动量指标（Momentum Indicators），基于价格或成交量的动态变化，用于识别趋势的强度和可能的趋势反转

def TD_MT_001(df, timeperiod=10):
    # 计算当前收盘价与过去特定周期收盘价的差值来实现的动量指标
    result = df['close'].diff(timeperiod)
    return result.to_numpy()

def TD_MT_002(df, timeperiod=30):
    # 计算给定时间周期内开盘价的动量
    result = df['open'].diff(timeperiod)
    return result.to_numpy()

def TD_MT_003(df, timeperiod=60):
    # 计算动量的简单移动平均线
    mom = df['close'].diff(timeperiod)
    smom = mom.rolling(window=timeperiod//2).mean()
    return smom.to_numpy()

def TD_MT_004(df, timeperiod=3):
    # 计算动量的加权移动平均线
    mom = df['close'].diff(timeperiod)
    spmom = mom.rolling(window=timeperiod//2).apply(lambda x: np.average(x, weights=np.arange(1, len(x)+1)), raw=True)
    return spmom.to_numpy()

# def TD_MT_005(df, fastperiod=12, slowperiod=26):
#     # 计算价格震荡指数（Absolute Price Oscillator）
#     fast_ema = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     slow_ema = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     result = fast_ema - slow_ema
#     return result.to_numpy()

def TD_MT_005(df, fastperiod=12, slowperiod=26):
    # 使用简单移动平均计算价格震荡指标
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    result = fast_sma - slow_sma
    return result.to_numpy()

def TD_MT_006(df, timeperiod=14):
    # 计算商品渠道指数（CCI）
    tp = (df['high'] + df['low'] + df['close']) / 3
    sma_tp = tp.rolling(window=timeperiod).mean()
    mean_dev = tp.rolling(window=timeperiod).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    cci = (tp - sma_tp) / (0.015 * mean_dev)
    return cci.to_numpy()

def TD_MT_007(df, timeperiod=14):
    # 钱德动量振荡器（Chande Momentum Oscillator）
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    cmo = 100 * (gain - loss) / (gain + loss)
    return cmo.to_numpy()

def TD_MT_008(df, timeperiod=14):
    # 定向运动指数（Directional Movement Index）
    plus_dm = np.where(df['high'].diff() > df['low'].diff(), df['high'].diff(), 0)
    minus_dm = np.where(df['low'].diff() > df['high'].diff(), df['low'].diff(), 0)
    tr = np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])
    sma_tr = pd.Series(tr).rolling(window=timeperiod).mean()
    plus_di = 100 * pd.Series(plus_dm).rolling(window=timeperiod).mean() / sma_tr
    minus_di = 100 * pd.Series(minus_dm).rolling(window=timeperiod).mean() / sma_tr
    dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.to_numpy()

# def TD_MT_009(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # 计算MACD线
#     ema_fast = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     ema_slow = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     macd = ema_fast - ema_slow
#     return macd.to_numpy()

def TD_MT_009(df, fastperiod=12, slowperiod=26, signalperiod=9):
    # 使用简单移动平均代替 EMA 计算 MACD 线
    sma_fast = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    sma_slow = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    macd = sma_fast - sma_slow
    return macd.to_numpy()

# def TD_MT_010(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # 计算MACD信号线
#     ema_fast = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     ema_slow = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     macd = ema_fast - ema_slow
#     macdsignal = macd.ewm(span=signalperiod, adjust=False).mean()
#     return macdsignal.to_numpy()

def TD_MT_010(df, fastperiod=12, slowperiod=26, signalperiod=9):
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    macd = fast_sma - slow_sma
    macdsignal = macd.rolling(window=signalperiod, min_periods=1).mean()
    return macdsignal.to_numpy()

# def TD_MT_011(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # 计算MACD柱状图
#     ema_fast = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     ema_slow = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     macd = ema_fast - ema_slow
#     signal = macd.ewm(span=signalperiod, adjust=False).mean()
#     macdhist = macd - signal
#     return macdhist.to_numpy()

def TD_MT_011(df, fastperiod=12, slowperiod=26, signalperiod=9):
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    macd = fast_sma - slow_sma
    signal = macd.rolling(window=signalperiod, min_periods=1).mean()
    macdhist = macd - signal
    return macdhist.to_numpy()

# def TD_MT_012(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # 计算MACD扩展线
#     ema_fast = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     ema_slow = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     macd = ema_fast - ema_slow
#     return macd.to_numpy() 

def TD_MT_012(df, fastperiod=12, slowperiod=26, signalperiod=9):
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    macd = fast_sma - slow_sma
    return macd.to_numpy()

# def TD_MT_013(df, fastperiod=12, slowperiod=26, signalperiod=9):
#     # 计算MACD扩展信号线
#     ema_fast = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     ema_slow = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     macd = ema_fast - ema_slow
#     macdsignal = macd.ewm(span=signalperiod, adjust=False).mean()
#     return macdsignal.to_numpy()

def TD_MT_013(df, fastperiod=12, slowperiod=26, signalperiod=9):
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    macd = fast_sma - slow_sma
    macdsignal = macd.rolling(window=signalperiod, min_periods=1).mean()
    return macdsignal.to_numpy()

# def TD_MT_014(df, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0):
#     # 计算MACD扩展柱状图
#     ema_fast = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     ema_slow = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     macd = ema_fast - ema_slow
#     signal = macd.ewm(span=signalperiod, adjust=False).mean()
#     macdhist = macd - signal
#     return macdhist.to_numpy()

def TD_MT_014(df, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0):
    # fastmatype, slowmatype, signalmatype 可拓展为不同类型MA，目前仅用SMA
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    macd = fast_sma - slow_sma
    signal = macd.rolling(window=signalperiod, min_periods=1).mean()
    macdhist = macd - signal
    return macdhist.to_numpy()

def TD_MT_015(df, timeperiod=14):
    # 负向运动指数
    minus_dm = np.where(df['low'].diff() > df['high'].diff(), df['low'].diff(), 0)
    tr = np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])
    minus_di = 100 * pd.Series(minus_dm).rolling(window=timeperiod).mean() / pd.Series(tr).rolling(window=timeperiod).mean()
    return minus_di.to_numpy()

def TD_MT_016(df, timeperiod=14):
    # 资金流量指标
    tp = (df['high'] + df['low'] + df['close']) / 3
    mf = tp * df['volume']
    posmf = (tp - tp.shift(1)).clip(lower=0) * df['volume']
    negmf = (tp.shift(1) - tp).clip(lower=0) * df['volume']
    mfi = 100 - (100 / (1 + (posmf.rolling(window=timeperiod).sum() / negmf.rolling(window=timeperiod).sum())))
    return mfi.to_numpy()

def TD_MT_017(df, timeperiod=14):
    # 负向变动值
    minus_dm = np.where(df['low'].diff() > df['high'].diff(), df['low'].diff(), 0)
    result = pd.Series(minus_dm).rolling(window=timeperiod).mean()
    return result.to_numpy()

def TD_MT_018(df, timeperiod=10):
    # 典型价格相对于其滚动平均的百分比变化
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    typical_price_sma = typical_price.rolling(window=timeperiod).mean()
    result = (typical_price - typical_price_sma) / typical_price_sma
    return result.to_numpy()

def TD_MT_019(df, timeperiod=30):
    # 计算价格与移动平均和标准偏差的偏差进行标准化的动量指标
    sma_close = df['close'].rolling(window=timeperiod).mean()
    stddev_close = df['close'].rolling(window=timeperiod).std()
    result = (df['close'] - sma_close) / stddev_close
    return result.to_numpy()

def TD_MT_020(df, timeperiod=14):
    # 计算典型价格与其滚动平均和标准偏差的偏差进行标准化
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    sma_typical_price = typical_price.rolling(window=timeperiod).mean()
    stddev_typical_price = typical_price.rolling(window=timeperiod).std()
    result = (typical_price - sma_typical_price) / stddev_typical_price
    return result.to_numpy()

def TD_MT_021(df, timeperiod=20):
    # 市场质量指标，考虑了收盘价与开盘价之差相对于高低价之差的百分比
    clo_open_diff = (df['close'] - df['open']).rolling(window=timeperiod).sum()
    high_low_diff = (df['high'] - df['low']).rolling(window=timeperiod).sum()
    result = 100 * clo_open_diff / high_low_diff
    return result.to_numpy() 

def TD_MT_022(df: pd.DataFrame) -> np.ndarray:
    # 计算连续两天收盘价的相对变化百分比
    result = (df['close'] - df['close'].shift(1)) / df['close'].shift(1)
    return result.to_numpy()

def TD_MT_023(df, timeperiod=14):
    # 计算相对强弱指数（RSI）
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.to_numpy()

def TD_MT_024(df, timeperiod=14):
    # 计算成交量的RSI
    delta = df['volume'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.to_numpy()

def TD_MT_025(df, timeperiod=14):
    # 计算高价与低价差的RSI
    range_diff = df['high'] - df['low']
    delta = range_diff.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=timeperiod).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=timeperiod).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.to_numpy()

def TD_MT_026(df, timeperiod=14):
    # 计算相对强弱指数的变种，通过比较平均收益和平均损失
    diff = df['close'].diff()
    gains = diff.where(diff > 0, 0)
    losses = -diff.where(diff < 0, 0)
    avg_gain = gains.rolling(window=timeperiod).mean()
    avg_loss = losses.rolling(window=timeperiod).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.to_numpy()

def TD_MT_027(df, timeperiod=10):
    # 计算价格相对于其过去一定时期内最高价和最低价的相对强度指标
    max_close = df['close'].rolling(window=timeperiod).max()
    min_close = df['close'].rolling(window=timeperiod).min()
    result = (df['close'] - min_close) / (max_close - min_close)
    return result.to_numpy()

def TD_MT_028(df, timeperiod=14):
    # 同TD_MT_027: 计算价格相对最高最低价格的RSI变种
    max_close = df['close'].rolling(window=timeperiod).max()
    min_close = df['close'].rolling(window=timeperiod).min()
    result = (df['close'] - min_close) / (max_close - min_close)
    return result.to_numpy()

def TD_MT_029(df: pd.DataFrame) -> np.ndarray:
    # 计算价格相对于其过去 30 天内最高价和最低价的RSI（未乘以100）
    max_close = df['close'].rolling(window=30).max()
    min_close = df['close'].rolling(window=30).min()
    result = (df['close'] - min_close) / (max_close - min_close)
    return result.to_numpy()

def TD_MT_030(df: pd.DataFrame) -> np.ndarray:
    # 计算连续两天收盘价的百分比变化
    result = df['close'].pct_change()
    return result.to_numpy()

def TD_MT_031(df, timeperiod=10):
    # 计算给定时间周期内的对数收益率
    log_return = np.log(df['close'] / df['close'].shift(timeperiod))
    return log_return.to_numpy()

def TD_MT_032(df, timeperiod=14):
    # 计算给定时间周期内最高、最低及收盘价变化的平均值
    high_change = df['high'].diff()
    low_change = df['low'].diff()
    close_change = df['close'].diff()
    mean_change = (high_change.abs() + low_change.abs() + close_change.abs()) / 3
    averaged_changes = mean_change.rolling(window=timeperiod).mean()
    return averaged_changes.to_numpy()

def TD_MT_033(df, timeperiod=1):
    # 计算给定时间周期内收盘价相对于其之前时间周期的变化量的最大值
    max_close_change = df['close'].diff(timeperiod).rolling(window=timeperiod).max()
    return max_close_change.to_numpy()

def TD_MT_034(df: pd.DataFrame) -> np.ndarray:
    # 计算收盘价 5 天变化百分比的 30 天滚动平均值
    pct_change = df['close'].pct_change(5)
    rolling_mean = pct_change.rolling(window=30).mean()
    return rolling_mean.to_numpy()

def TD_MT_035(df: pd.DataFrame) -> np.ndarray:
    # 计算收盘价 5 天变化百分比的 1 天滚动平均值
    pct_change = df['close'].pct_change(5)
    rolling_mean = pct_change.rolling(window=1).mean()
    return rolling_mean.to_numpy()

def TD_MT_036(df: pd.DataFrame) -> np.ndarray:
    # 计算收盘价 5 天变化百分比的 5 天滚动平均值
    pct_change = df['close'].pct_change(5)
    rolling_mean = pct_change.rolling(window=5).mean()
    return rolling_mean.to_numpy()

def TD_MT_037(df: pd.DataFrame) -> np.ndarray:
    # 计算收盘价 5 天变化百分比的 10 天滚动平均值
    pct_change = df['close'].pct_change(5)
    rolling_mean = pct_change.rolling(window=10).mean()
    return rolling_mean.to_numpy()

def TD_MT_038(df: pd.DataFrame) -> np.ndarray:
    # 计算收盘价 5 天变化百分比的 14 天滚动平均值
    pct_change = df['close'].pct_change(5)
    rolling_mean = pct_change.rolling(window=14).mean()
    return rolling_mean.to_numpy()

def TD_MT_039(df, timeperiod=10):
    # 计算过去一段时间内，每日收盘价与最低价之差的累积和
    close_low_diff_sum = (df['close'] - df['low']).rolling(window=timeperiod).sum()
    return close_low_diff_sum.to_numpy()

def TD_MT_040(df, timeperiod=10):
    # 计算过去一段时间内，每日最高价与收盘价之差的累积和
    high_close_diff_sum = (df['high'] - df['close']).rolling(window=timeperiod).sum()
    return high_close_diff_sum.to_numpy()

def TD_MT_041(df, timeperiod=14):
    # 计算威廉指标（Williams %R）
    high_roll = df['high'].rolling(window=timeperiod).max()
    low_roll = df['low'].rolling(window=timeperiod).min()
    willr = -100 * (high_roll - df['close']) / (high_roll - low_roll)
    return willr.to_numpy()

def TD_MT_042(df, timeperiod=26):
    # 计算类似于威廉指标（Williams %R）的一个变种，用于衡量当前价格相对于过去一定时期内的最高价和最低价的位置
    clo_max_diff = df['close'].values  - df['close'].rolling(window=timeperiod).min().values
    max_min_diff = df['close'].rolling(window=timeperiod).max().values - df['close'].rolling(window=timeperiod).min().values
    result = np.where( max_min_diff != 0, 2 * ( clo_max_diff / max_min_diff ) - 1, np.nan)
    return result

def TD_MT_043(df, timeperiod=360):
    '''历史成交量的均值，和成交概率和平仓能力高度有效'''
    result = df['volume'].rolling(360).mean().values
    return result

def TD_MT_044(df, timeperiod=360):
    '''历史成交量的均值，和成交概率和平仓能力高度有效'''
    result = (df['high'] - df['low']).rolling(360).mean().values
    return result

#  三、 量价指标（Volume Price Indicators），价格和成交量数据，用于分析市场供需关系

def TP_VPI_001(df, time_period=360):
    """计算累积/派发线（AD），一种基于价格和成交量的技术分析工具。"""
    clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    ad = (clv * df['volume']).rolling(time_period).sum()
    return ad.to_numpy()

def TP_VPI_002(df, timeperiod=14,timeperiod2=360):
    """计算累积/分配线（Accumulation/Distribution Line, AD）的滚动平均值。"""
    clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    ad = (clv * df['volume']).rolling(timeperiod2).sum() 
    return ad.rolling(window=timeperiod).mean().values 

def TP_VPI_003(df, timeperiod=20):
    """计算累积/分配线（A/D线），用于衡量资金流向。"""
    close_diff = df['close'].diff()
    up = close_diff.where(close_diff > 0, 0.0)
    down = -close_diff.where(close_diff < 0, 0.0)
    au = up.rolling(window=timeperiod).sum()
    ad = down.rolling(window=timeperiod).sum()
    total = au + ad
    result = np.where(total > 0, au / total, np.nan)
    return result

# def TP_VPI_004(df, fastperiod=12, slowperiod=26):
#     """计算加速/减速振荡器（ADOSC）震荡指标，结合价格和成交量来分析市场的动能。"""
#     clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
#     ad = (clv * df['volume']).cumsum()
#     fast_ema = ad.ewm(span=fastperiod).mean()
#     slow_ema = ad.ewm(span=slowperiod).mean()
#     adosc = fast_ema - slow_ema
#     return adosc.values

def TP_VPI_004(df, fastperiod=12, slowperiod=26):
    """计算加速/减速振荡器（ADOSC）震荡指标"""
    clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    clv = clv.fillna(0)  # 避免除以零产生 NaN
    ad = (clv * df['volume']).rolling(360).sum()

    fast_sma = ad.rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = ad.rolling(window=slowperiod, min_periods=1).mean()
    adosc = fast_sma - slow_sma
    return adosc.to_numpy()

# def TP_VPI_005(df: pd.DataFrame) -> np.ndarray:
#     """计算累积/派发量（OBV）能量潮，基于成交量变化来预测价格走势。"""
#     obv = df['volume'].where(df['close'].diff() > 0).fillna(-df['volume'].where(df['close'].diff() < 0).fillna(0)).cumsum()
#     return obv.to_numpy() 

def TP_VPI_005(df: pd.DataFrame) -> np.ndarray:
    """
    计算能量潮指标（OBV）：用于衡量成交量随价格走势的累积效应。
    """
    price_diff = df['close'].diff().fillna(0)
    obv_delta = np.where(price_diff > 0, df['volume'],np.where(price_diff < 0, -df['volume'], 0))
    obv = pd.Series(obv_delta, index=df.index).rolling(360).sum()
    return obv.to_numpy()

# def TP_VPI_006(df, timeperiod=14):
#     """计算平衡交易量（On Balance Volume, OBV）的滚动平均值。"""
#     obv = df['volume'].where(df['close'].diff() > 0).fillna(-df['volume'].where(df['close'].diff() < 0).fillna(0)).cumsum()
#     return obv.rolling(window=timeperiod).mean().values

def TP_VPI_006(df, timeperiod=14):
    """
    计算 OBV 的滚动平均值，用于平滑趋势。
    """
    price_diff = df['close'].diff().fillna(0)
    obv_delta = np.where(price_diff > 0, df['volume'],np.where(price_diff < 0, -df['volume'], 0))
    obv = pd.Series(obv_delta, index=df.index).rolling(360).sum()
    return obv.rolling(window=timeperiod).mean().to_numpy()


def TP_VPI_007(df, timeperiod=30):
    """计算加权成交量平均价格（Volume Weighted Average Price, VWAP）的变种，这里使用滚动窗口来计算。"""
    total = (df['close'] * df['volume']).rolling(window=timeperiod).sum()
    volume_total = df['volume'].rolling(window=timeperiod).sum()
    vwma = total / volume_total
    return vwma.to_numpy()

def TP_VPI_008(df, timeperiod=30):
    """计算典型价格加权成交量平均（类似于VWAP，但使用典型价格）。"""
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    total = (typical_price * df['volume']).rolling(window=timeperiod,min_periods=10).sum()
    volume_total = df['volume'].rolling(window=timeperiod,min_periods=10).sum()
    vwa = total / volume_total
    return vwa.to_numpy()

def TP_VPI_009(df, timeperiod=14):
    """计算加权成交量平均价格（VWA）与平均价格的差值"""
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    total = (typical_price * df['volume']).rolling(window=timeperiod,min_periods=10).sum()
    volume_total = df['volume'].rolling(window=timeperiod,min_periods=10).sum()
    vwa = total / volume_total
    diff = typical_price - vwa
    return diff.to_numpy()

def TP_VPI_010(df: pd.DataFrame) -> np.ndarray:
    """计算价量趋势指标(VPT)，用于衡量成交量对价格变动的影响。"""
    vpt = pd.Series(index=df.index, dtype=float)
    vpt.iloc[0] = 0
    for i in range(1, len(df)):
        vpt.iloc[i] = vpt.iloc[i-1] + df['volume'].iloc[i] * ((df['close'].iloc[i] - df['close'].iloc[i-1]) / df['close'].iloc[i-1])
    return vpt.to_numpy()

def TP_VPI_011(df, shortperiod=12, longperiod=26):
    """计算短期平均成交量与长期平均成交量之差，用于识别成交量的趋势。"""
    short_vo = df['volume'].rolling(window=shortperiod).mean()
    long_vo = df['volume'].rolling(window=longperiod).mean()
    vo = short_vo - long_vo
    return vo.to_numpy()

def TP_VPI_012(df, timeperiod=14):
    """计算成交量与其n天滚动平均值的比值，反映成交量的相对变化。"""
    result = df['volume'] / df['volume'].rolling(timeperiod).mean()
    return result.to_numpy()

def TP_VPI_013(df, timeperiod=20):
    """计算成交量与其n天滚动平均值的比值，反映成交量的相对变化。"""
    res = df['volume'] / df['volume'].rolling(timeperiod).mean()
    return res.to_numpy()

def TP_VPI_014(df, timeperiod=20):
    """计算成交量与其10日平均值的比率，再对这个比率进行10日平均。"""
    volume_ratio = (df['volume'] / df['volume'].rolling(window=timeperiod).mean()).rolling(window=timeperiod).mean()
    return volume_ratio.to_numpy()

def TP_VPI_015(df: pd.DataFrame) -> np.ndarray:
    """计算每日收盘价与开盘价之差与成交量的比率。"""
    price_diff = df['close'] - df['open']
    vol_ratio = price_diff / df['volume']
    return vol_ratio.replace([np.inf, -np.inf], np.nan).fillna(0).to_numpy()

def TP_VPI_016(df, timeperiod=14):
    """计算加权收盘价（收盘价乘以交易量后取14天滚动平均值，再除以交易量14天滚动平均值）。"""
    vwma_numerator = (df['close'] * df['volume']).rolling(window=timeperiod).mean()
    vwma_denominator = df['volume'].rolling(window=timeperiod).mean()
    vwma = vwma_numerator / vwma_denominator
    return vwma.fillna(0).to_numpy()

def TP_VPI_017(df, timeperiod=14):
    """计算正钱潮（MF）和负钱潮的总和之差，用于衡量资金流向。"""
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    money_flow = typical_price * df['volume']
    pos_flow = np.where(df['close'].diff() > 0, money_flow, 0)
    neg_flow = np.where(df['close'].diff() < 0, money_flow, 0)
    pos_sum = pd.Series(pos_flow).rolling(window=timeperiod).sum()
    neg_sum = pd.Series(neg_flow).rolling(window=timeperiod).sum()
    money_flow_index = pos_sum - neg_sum
    return money_flow_index.to_numpy()

def TP_VPI_018(df, timeperiod=20):
    """使用过去一段时间（timeperiod）内，每日真实波幅（开盘价、收盘价、最高价、最低价中最高与最低值的差）的累积和。"""
    true_range = np.maximum(df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1)))
    cum_true_range = true_range.rolling(window=timeperiod).sum()
    return cum_true_range.to_numpy()

def TP_VPI_019(df, timeperiod=10):
    """计算给定时间周期内的成交量总和。"""
    vol_sum = df['volume'].rolling(window=timeperiod).sum()
    return vol_sum.to_numpy()

#  四、 波动率指标（Volatility Indicators），衡量市场的价格波动性，通常用于识别市场的平静期和波动期

def TD_VI_001(df, timeperiod=14):
    # 计算波动率指标 - 价格范围（Sevi）
    max_price = df['high'].rolling(window=timeperiod, min_periods=1).max().values
    min_price = df['low'].rolling(window=timeperiod, min_periods=1).min().values
    sevi = max_price - min_price
    return sevi

def TD_VI_002(df, timeperiod=5):
    # 计算波动率指标 - 收盘价标准差
    rolling_std = df['close'].rolling(window=timeperiod, min_periods=1).std(ddof=1)
    return rolling_std.values

def TD_VI_003(df, timeperiod=14):
    # 计算波动率指标 - 简化平均真实范围（ATR）
    result = (df['high'] - df['low']).rolling(window=timeperiod, min_periods=1).mean().values
    return result

def TD_VI_004(df, timeperiod=14):
    # 计算波动率指标 - 平均真实范围（ATR）
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    tr = np.maximum.reduce([high_low, high_close, low_close])
    atr = pd.Series(tr).rolling(window=timeperiod, min_periods=1).mean()
    return atr.values

def TD_VI_005(df, timeperiod=14):
    # 计算波动率指标 - ATR与简单移动平均线之差（TRA）
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    tr = np.maximum.reduce([high_low, high_close, low_close])
    atr = pd.Series(tr).rolling(window=timeperiod, min_periods=1).mean()
    sma_atr = pd.Series(tr).rolling(window=timeperiod // 2, min_periods=1).mean()
    tra = (atr - sma_atr).values
    return tra

def TD_VI_006(df, timeperiod=14):
    # Calculate True Range components
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))

    # Calculate True Range
    tr = np.maximum(high_low, np.maximum(high_close, low_close))

    # Calculate the Average True Range (ATR)
    atr = pd.Series(tr).rolling(window=timeperiod, min_periods=1).mean().values
    natr = (atr / df['close']) * 100
    return natr

def TD_VI_007(df: pd.DataFrame) -> np.ndarray:
    # 计算波动率指标 - 真实范围（TRANGE）
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    tr = np.maximum.reduce([high_low, high_close, low_close])
    return tr

def TD_VI_008(df, timeperiod=14):
    # 计算波动率指标 - 真实波幅（TR）滚动平均值
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    tr = np.maximum.reduce([high_low, high_close, low_close])
    result = pd.Series(tr).rolling(window=timeperiod, min_periods=1).mean()
    return result.values

def TD_VI_009(df, timeperiod=14):
    # 计算波动率指标 - 真实波幅（TR）滚动标准差
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    tr = np.maximum.reduce([high_low, high_close, low_close])
    result = pd.Series(tr).rolling(window=timeperiod, min_periods=1).std()
    return result.values

def TD_VI_010(df, timeperiod=14):
    # 计算波动率指标 - 正向运动指数（+DI）
    plus_dm = np.where(df['high'].diff() > df['low'].diff(), df['high'].diff(), 0)
    return plus_dm

def TD_VI_011(df, timeperiod=14):
    # 计算波动率指标 - 正向动力（+DM）
    plus_dm = np.where(df['high'].diff() > df['low'].diff(), df['high'].diff(), 0)
    result = pd.Series(plus_dm).rolling(window=timeperiod, min_periods=1).sum().values
    return result

# def TD_VI_012(df, fastperiod=3, slowperiod=10):
#     # 计算波动率指标 - 百分比价格振荡器（PPO）
#     fast_ema = df['close'].ewm(span=fastperiod, adjust=False).mean()
#     slow_ema = df['close'].ewm(span=slowperiod, adjust=False).mean()
#     ppo = ((fast_ema - slow_ema) / slow_ema) * 100
#     return ppo.values

def TD_VI_012(df, fastperiod=3, slowperiod=10):
    # 计算波动率指标 - 百分比价格振荡器（PPO）
    fast_sma = df['close'].rolling(window=fastperiod, min_periods=1).mean()
    slow_sma = df['close'].rolling(window=slowperiod, min_periods=1).mean()
    ppo = ((fast_sma - slow_sma) / slow_sma) * 100
    return ppo.values


def TD_VI_013(df, timeperiod=10):
    # 计算波动率指标 - 变动率（ROC）
    result = df['close'].pct_change(periods=timeperiod) * 100
    return result.values

def TD_VI_014(df, timeperiod=10):
    # 计算波动率指标 - 价格变动百分比（ROCP）
    result = df['close'].pct_change(periods=timeperiod)
    return result.values

def TD_VI_015(df, timeperiod=10):
    # 计算波动率指标 - 价格变动比率（ROCR）
    result = (df['close'] / df['close'].shift(timeperiod))
    return result.values


def TD_VI_016(df, timeperiod=10):
    # 计算波动率指标 - 变动百分率（ROCR100）
    result = (df['close'] / df['close'].shift(timeperiod)) * 100
    return result.values

def TD_VI_017(df, fastk_period=14, slowk_period=3, slowk_matype=0):
    # 计算波动率指标 - 随机慢速K值（Slow Stochastic %K）
    low_min = df['low'].rolling(window=fastk_period, min_periods=1).min()
    high_max = df['high'].rolling(window=fastk_period, min_periods=1).max()
    slowk = (df['close'] - low_min) / (high_max - low_min) * 100
    result = slowk.rolling(window=slowk_period, min_periods=1).mean()
    return result.values

def TD_VI_018(df, fastk_period=14, slowk_period=3, slowk_matype=0):
    # 计算波动率指标 - 随机慢速D值（Slow Stochastic %D）
    low_min = df['low'].rolling(window=fastk_period, min_periods=1).min()
    high_max = df['high'].rolling(window=fastk_period, min_periods=1).max()
    slowk = (df['close'] - low_min) / (high_max - low_min) * 100
    slowd = slowk.rolling(window=slowk_period, min_periods=1).mean()
    return slowd.values

def TD_VI_020(df, fastk_period=5, fastd_period=3, fastd_matype=0):
    # 计算波动率指标 - 快速随机指标（STOCHF - fastk）
    low_min = df['low'].rolling(window=fastk_period, min_periods=1).min()
    high_max = df['high'].rolling(window=fastk_period, min_periods=1).max()
    fastk = (df['close'] - low_min) / (high_max - low_min) * 100
    result = fastk.rolling(window=fastd_period, min_periods=1).mean()
    return result.values

def TD_VI_021(df, fastk_period=5, fastd_period=3, fastd_matype=0):
    # 计算波动率指标 - 快速随机指标（STOCHF - fastd）
    low_min = df['low'].rolling(window=fastk_period, min_periods=1).min()
    high_max = df['high'].rolling(window=fastk_period, min_periods=1).max()
    fastk = (df['close'] - low_min) / (high_max - low_min) * 100
    fastd = fastk.rolling(window=fastd_period, min_periods=1).mean()
    return fastd.values

def TD_VI_022(df, timeperiod=14):
    # 计算波动率指标 - 随机振荡器（STOCHRSI）
    close_min = df['close'].rolling(window=timeperiod, min_periods=1).min()
    close_max = df['close'].rolling(window=timeperiod, min_periods=1).max()
    stochrsi = (df['close'] - close_min) / (close_max - close_min)
    return stochrsi.values

# def TD_VI_023(df, timeperiod=14):
#     # 计算波动率指标 - 三重指数平滑移动平均（TRIX）
#     ema1 = df['close'].ewm(span=timeperiod, adjust=False).mean()
#     ema2 = ema1.ewm(span=timeperiod, adjust=False).mean()
#     ema3 = ema2.ewm(span=timeperiod, adjust=False).mean()
#     trix = (ema3 - ema3.shift(1)) / ema3.shift(1)
#     return trix.values

def TD_VI_023(df, timeperiod=14):
    # 计算波动率指标 - 三重指数平滑移动平均（TRIX）
    sma1 = df['close'].rolling(window=timeperiod, min_periods=1).mean()
    sma2 = sma1.rolling(window=timeperiod, min_periods=1).mean()
    sma3 = sma2.rolling(window=timeperiod, min_periods=1).mean()
    trix = (sma3 - sma3.shift(1)) / sma3.shift(1)
    return trix.values

def TD_VI_024(df, timeperiod=14):
    # 计算波动率指标 - 平衡价格振荡器（BOP）
    bop = (df['close'] - df['open']) / (df['high'] - df['low'] + 0.0001)
    return bop.values

def TD_VI_025(df, timeperiod=14):
    # 计算波动率指标 - 终极波动指标（ULTOSC）
    bp = df['close'] - np.minimum(df['low'], df['close'].shift(1))
    tr = np.maximum(df['high'] - df['low'], np.maximum(abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))))
    avg7 = bp.rolling(window=7, min_periods=1).sum() / tr.rolling(window=7, min_periods=1).sum()
    avg14 = bp.rolling(window=14, min_periods=1).sum() / tr.rolling(window=14, min_periods=1).sum()
    avg28 = bp.rolling(window=28, min_periods=1).sum() / tr.rolling(window=28, min_periods=1).sum()
    ultosc = (4 * avg7 + 2 * avg14 + avg28) / (4 + 2 + 1)
    return ultosc.values

def TD_VI_026(df, timeperiod=14):
    # 计算波动率指标 - 给定时间周期内上影线与下影线长度差减实体长度总和
    upper_shadow = df['high'] - df[['close', 'open']].max(axis=1)
    lower_shadow = df[['close', 'open']].min(axis=1) - df['low']
    real_body = np.abs(df['close'] - df['open'])
    result = (upper_shadow - lower_shadow - real_body).rolling(window=timeperiod, min_periods=1).sum() 
    return result.values

def TD_VI_027(df, timeperiod=14):
    # 计算波动率指标 - 给定时间周期内每日高低价差最大值
    result = (df['high'] - df['low']).rolling(window=timeperiod, min_periods=1).max()
    return result.values

def TD_VI_028(df, timeperiod=14):
    # 计算波动率指标 - 给定时间周期内上影线长度总和
    uppershad = df['high'] - np.maximum(df['close'], df['open'])
    result = uppershad.rolling(window=timeperiod, min_periods=1).sum()
    return result.values

def TD_VI_029(df, timeperiod=14):
    # 计算波动率指标 - 给定时间周期内下影线长度总和
    lowershad = np.minimum(df['close'], df['open']) - df['low']
    result = lowershad.rolling(window=timeperiod, min_periods=1).sum()
    return result.values 


#  五、价格变化函数（Price Transform Functions），关注价格的变化，包括价格的变动幅度和速度

def TD_PT_001(df, timeperiod=14):
    # 计算收盘价的正弦值。
    result = np.sin(df['close'].values)
    return result

def TD_PT_002(df, timeperiod=14):
    # 计算收盘价的余弦值
    result = np.cos(df['close'].values)
    return result

def TD_PT_003(df: pd.DataFrame) -> np.ndarray: # 已入库因子 收益回撤比：1.83  IC：0.98718   IR：0.33927
    vwap = df['amount'] / df['volume']
    numerator_5d = vwap.rolling(6).mean()
    denominator_5d = df['amount'].rolling(6).sum() / df['volume'].rolling(6).sum()
    apb = np.log(numerator_5d / denominator_5d)
    apb = apb.rolling(18).mean()
    return apb

# def TD_PT_004(df, fast=9, slow=25):
#     # 蔡金摆动指标 (Chaikin Oscillator)
#     var1 = (df['close'] - df['open']) / (df['high'] - df['low']) * (df['volume'] / df['volume'].rolling(120).mean())
#     accum = var1.rolling(60).sum()

#     ema_fast = pd.Series(accum).ewm(span=fast, adjust=False).mean().values  # 短期 EMA
#     ema_slow = pd.Series(accum).ewm(span=slow, adjust=False).mean().values  # 长期 EMA
#     result = ema_fast - ema_slow  # CHO 值
#     return np.nan_to_num(result) 

def TD_PT_004(df, fast=9, slow=25):
    # 蔡金摆动指标 (Chaikin Oscillator)，使用SMA替代EMA
    var1 = (df['close'] - df['open']) / (df['high'] - df['low']) * (df['volume'] / df['volume'].rolling(120).mean())
    accum = var1.rolling(60).sum()

    sma_fast = accum.rolling(window=fast, min_periods=1).mean().values  # 短期 SMA
    sma_slow = accum.rolling(window=slow, min_periods=1).mean().values  # 长期 SMA
    result = sma_fast - sma_slow  # CHO 值
    return np.nan_to_num(result)

def TD_PT_005(df: pd.DataFrame) -> np.ndarray:
    # 中位数价格（MEDPRICE）
    result = ((df['high'] + df['low']) / 2).values
    return result

def TD_PT_006(df: pd.DataFrame) -> np.ndarray:
    # 代表性价格（TYPPRICE）
    result = ((df['high'] + df['low'] + df['close']) / 3).values
    return result

def TD_PT_007(df: pd.DataFrame) -> np.ndarray:
    # 加权收盘价（WCLPRICE）
    result = ((df['high'] + df['low'] + 2 * df['close']) / 4).values
    return result

def TD_PT_008(df, timeperiod=14):
    # 计算给定时间周期内的收盘价的变化量（差分）
    result = df['close'].diff(periods=timeperiod).values
    return result

def TD_PT_009(df, nDay=10):
    # 计算 nDay 前的收盘价
    mid = (df['close'].shift(nDay) + df['high'].rolling(window=nDay).min() + df['low'].rolling(window=nDay).min()) / 3
    llv_low = df['low'].rolling(window=nDay).min()
    llv_high = df['high'].rolling(window=nDay).min()
    yl2 = mid + llv_high - llv_low
    return (df['close'] - yl2).values

def TD_PT_010(df, nDay=15):
    # 计算加权典型价格，给予收盘价更高的权重
    mid = (df['close'] + df['high'] + df['low']) / 3
    llv_low = df['low'].rolling(window=nDay).min()
    llv_high = df['high'].rolling(window=nDay).min()
    zc2 = mid - llv_high + llv_low
    return (df['close'] - zc2).values

def TD_PT_011(df, timeperiod=10):
    # 计算典型价格的差分振荡器
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    oscillator = typical_price.diff(periods=timeperiod)
    return oscillator.values

def TD_PT_012(df, timeperiod=10):
    # 计算高低价差的差分
    spread = df['high'] - df['low']
    psdiff = spread.diff(periods=timeperiod)
    return psdiff.values

def TD_PT_013(df: pd.DataFrame) -> np.ndarray:
    # 计算最高价与最低价的比率
    result = (df['high'] / df['low']).values
    return result

def TD_PT_014(df: pd.DataFrame) -> np.ndarray:
    # 计算高低价差与收盘价的比值
    result = ((df['high'] - df['low']) / df['close']).values
    return result

def TD_PT_015(df: pd.DataFrame) -> np.ndarray:
    # 计算开盘价与收盘价的差异百分比
    res = ((df['close'] - df['open']) / df['open']).values
    return res

def TD_PT_016(df, timeperiod=14):
    # 计算给定时间周期内的收盘价变化百分比
    result = (df['close'] / df['close'].shift(timeperiod) - 1).values
    return result

def TD_PT_017(df, timeperiod=14):
    # 计算 n 天收盘价的相对变化百分比
    res = ((df['close'] - df['close'].shift(timeperiod)) / df['close'].shift(timeperiod)).values
    return res

def TD_PT_018(df, timeperiod=14):
    # 计算开盘价与收盘价差异的 n 天滚动平均值
    result = (df['close'] - df['open']).rolling(window=timeperiod).mean().values
    return result

def TD_PT_019(df, timeperiod=14):
    # 计算高低价差之和的 n 天滚动平均值
    result = (df['high'] - df['low']).rolling(window=timeperiod).sum().values / timeperiod
    return result

def TD_PT_020(df: pd.DataFrame) -> np.ndarray:
    # 计算过去 14 天内高低价差的最大值
    result = (df['high'] - df['low']).rolling(window=14).max().values
    return result

def TD_PT_021(df, timeperiod=14):
    # 计算收盘价相对于其过去 n 天平均值的绝对差异，反映了价格变化的幅度
    res = (df['close'] - df['close'].shift(timeperiod)).abs().values
    return res

def TD_PT_022(df, timeperiod=14):
    # 过去 n 天最高价与最低价的差值，反映了价格变化的幅度
    res = (df['close'].rolling(timeperiod).max() - df['close'].rolling(timeperiod).min()).values
    return res

def TD_PT_023(df, timeperiod=14):
    # 收盘价与其 n 天滚动平均值的绝对差，反映了价格变化的幅度
    res = (df['close'] - df['close'].rolling(timeperiod).mean()).abs().values
    return res

def TD_PT_024(df: pd.DataFrame) -> np.ndarray:
     # 计算每个周期的顶部百分比，表示收盘价接近最高价的程度
    hig_low_diff = (df['high'] - df['low']).values
    hig_clo_diff = (df['high'] - df['close']).values
    result = np.where(hig_low_diff != 0, hig_clo_diff / hig_low_diff, np.nan)
    return result

def TD_PT_025(df: pd.DataFrame) -> np.ndarray:
    # 计算每个周期的底部百分比，表示收盘价接近最低价的程度
    hig_low_diff = (df['high'] - df['low']).values
    clo_low_diff = (df['close'] - df['low']).values
    result = np.where(hig_low_diff != 0, clo_low_diff / hig_low_diff, np.nan)
    return result

def TD_PT_026(df, timeperiod=14):
    # 计算收盘价相对于开盘价在价格范围内的位置
    hig_low_diff = (df['high'] - df['low']).values
    clo_ope_diff = (df['close'] - df['open']).values
    result = np.where(hig_low_diff != 0, clo_ope_diff / hig_low_diff, np.nan)
    return result

def TD_PT_027(df, timeperiod=14):
    # 计算最小最大指数（MINMAXINDEX），用于识别价格序列中的最小和最大值位置
    min_idx = df['close'].rolling(window=timeperiod).apply(lambda x: np.argmin(x), raw=True).values
    # max_idx = df['close'].rolling(window=timeperiod).apply(lambda x: np.argmax(x), raw=True)
    return min_idx

def TD_PT_028(df, timeperiod=30):
    # 计算过去 30 天内的最低价相对于当前收盘价的差异百分比,统计价格在特定时间段内的相对位置情况
    min_clo = df['close'].rolling(timeperiod).min()
    result = ((min_clo - df['close']) / df['close']).values
    return result

def TD_PT_029(df, timeperiod=30):
    # 计算过去 30 天内的最高价相对于当前收盘价的差异百分比
    max_clo = df['close'].rolling(timeperiod).max()
    result = ((max_clo - df['close']) / df['close']).values
    return result

#  六、 周期指标（Cycle Indicators），识别市场周期性波动

def TD_CI_001(df: pd.DataFrame) -> np.ndarray:
    """Calculate the daily range (High - Low)."""
    result = df['high'] - df['low']
    return result.values

def TD_CI_002(df: pd.DataFrame) -> np.ndarray:
    """Calculate the difference between the close and open prices each day."""
    result = df['close'] - df['open']
    return result.values

def TD_CI_003(df, timeperiod=30):
    """Estimate Hilbert Transform - Dominant Cycle Period."""
    diff_sma = df['close'].diff().rolling(window=timeperiod, min_periods=1).mean()
    close_sma = df['close'].rolling(window=timeperiod, min_periods=1).mean()
    instantaneous_phase = np.arctan(diff_sma / close_sma)
    result = (2 * np.pi / instantaneous_phase).rolling(window=timeperiod, min_periods=1).mean()
    return result.values

def TD_CI_004(df: pd.DataFrame) -> np.ndarray:
    """Estimate Hilbert Transform - Dominant Cycle Phase."""
    diff_sma = df['close'].diff().rolling(window=30, min_periods=1).mean()
    close_sma = df['close'].rolling(window=30, min_periods=1).mean()
    instantaneous_phase = np.arctan(diff_sma / close_sma)
    result = np.rad2deg(instantaneous_phase)
    return result.values

def TD_CI_005(df: pd.DataFrame) -> np.ndarray:
    """Calculate the in-phase component of the Hilbert Transform."""
    inphase = df['close'].rolling(window=30, min_periods=1).mean()
    return inphase.values

def TD_CI_006(df: pd.DataFrame) -> np.ndarray:
    """Calculate the quadrature component of the Hilbert Transform."""
    quadrature = df['close'].diff().rolling(window=30, min_periods=1).mean()
    return quadrature.values

def TD_CI_007(df: pd.DataFrame) -> np.ndarray:
    """Calculate the sine component of the Hilbert Transform SineWave."""
    period = 30
    sine = np.sin(df['close'].rolling(window=period, min_periods=1).mean())
    return sine.values

def TD_CI_008(df: pd.DataFrame) -> np.ndarray:
    """Calculate the lead sine component of the Hilbert Transform SineWave."""
    period = 30
    lead_sine = np.cos(df['close'].rolling(window=period, min_periods=1).mean())
    return lead_sine.values

def TD_CI_009(df: pd.DataFrame) -> np.ndarray:
    """Estimate the Hilbert Transform Trendline."""
    trendline = df['close'].rolling(window=30, min_periods=1).mean()
    return trendline.values

def TD_CI_010(df: pd.DataFrame) -> np.ndarray:
    """Determine if the Hilbert Transform indicates a trend or cycle mode."""
    period = 30
    rolling_std = df['close'].rolling(window=period, min_periods=1).std()
    sma_std = df['close'].rolling(window=period, min_periods=1).std()
    trend_mode = (rolling_std > sma_std).astype(int)
    return trend_mode.values

# def TD_CI_003(df, timeperiod=30):
#     """Estimate Hilbert Transform - Dominant Cycle Period."""
#     instantaneous_phase = np.arctan(df['close'].diff().ewm(span=timeperiod).mean() / df['close'].ewm(span=timeperiod).mean())
#     result = (2 * np.pi / instantaneous_phase).rolling(window=timeperiod).mean()
#     return result.values

# def TD_CI_004(df: pd.DataFrame) -> np.ndarray:
#     """Estimate Hilbert Transform - Dominant Cycle Phase."""
#     instantaneous_phase = np.arctan(df['close'].diff().ewm(span=30).mean() / df['close'].ewm(span=30).mean())
#     result = np.rad2deg(instantaneous_phase)
#     return result.values

# def TD_CI_005(df: pd.DataFrame) -> np.ndarray:
#     """Calculate the in-phase component of the Hilbert Transform."""
#     inphase = df['close'].ewm(span=30).mean()
#     return inphase.values

# def TD_CI_006(df: pd.DataFrame) -> np.ndarray:
#     """Calculate the quadrature component of the Hilbert Transform."""
#     quadrature = df['close'].diff().ewm(span=30).mean()
#     return quadrature.values

# def TD_CI_007(df: pd.DataFrame) -> np.ndarray:
#     """Calculate the sine component of the Hilbert Transform SineWave."""
#     period = 30
#     sine = np.sin(df['close'].ewm(span=period).mean())
#     return sine.values

# def TD_CI_008(df: pd.DataFrame) -> np.ndarray:
#     """Calculate the lead sine component of the Hilbert Transform SineWave."""
#     period = 30
#     lead_sine = np.cos(df['close'].ewm(span=period).mean())
#     return lead_sine.values

# def TD_CI_009(df: pd.DataFrame) -> np.ndarray:
#     """Estimate the Hilbert Transform Trendline."""
#     trendline = df['close'].ewm(span=30).mean()
#     return trendline.values

# def TD_CI_010(df: pd.DataFrame) -> np.ndarray:
#     """Determine if the Hilbert Transform indicates a trend or cycle mode."""
#     period = 30
#     trend_mode = (df['close'].rolling(window=period).std() > df['close'].ewm(span=period).std()).astype(int)
#     return trend_mode.values


#  七、统计学指标（Statistical Indicators）

def TD_SI_001(df: pd.DataFrame) -> np.ndarray:
    """Return a boolean array indicating if the closing price is above the opening price."""
    is_higher = (df['close'] > df['open']).astype(int)
    return is_higher.values

# Re-implementing LINEARREG functions using numpy's polyfit, which performs linear regression.
def TD_SI_002(df, timeperiod=14):
    """Calculate linear regression line."""
    result = [np.polyfit(range(timeperiod), df['close'].values[i-timeperiod:i], 1)[1] for i in range(timeperiod, len(df)+1)]
    # Extending result with NaNs for aligning with the length of the dataframe.
    return np.concatenate([np.full(timeperiod-1, np.nan), result])

def TD_SI_003(df, timeperiod=14):
    """Calculate the angle of linear regression."""
    result = [np.rad2deg(np.arctan(np.polyfit(range(timeperiod), df['close'].values[i-timeperiod:i], 1)[0])) for i in range(timeperiod, len(df)+1)]
    return np.concatenate([np.full(timeperiod-1, np.nan), result])

def TD_SI_004(df, timeperiod=14):
    """Calculate the slope of the linear regression."""
    result = [np.polyfit(range(timeperiod), df['close'].values[i-timeperiod:i], 1)[0] for i in range(timeperiod, len(df)+1)]
    return np.concatenate([np.full(timeperiod-1, np.nan), result])

def TD_SI_005(df, timeperiod=14):
    """Calculate the intercept of the linear regression."""
    result = [np.polyfit(range(timeperiod), df['close'].values[i-timeperiod:i], 1)[1] for i in range(timeperiod, len(df)+1)]
    return np.concatenate([np.full(timeperiod-1, np.nan), result])

def TD_SI_006(df, timeperiod=14):
    """Calculate the linear regression values."""
    result = []
    for i in range(timeperiod-1, len(df)):
        reg = np.polyfit(range(timeperiod), df['close'].values[i-timeperiod+1:i+1], 1)
        # Calculate the value at the end of the period
        reg_val = reg[0] * (timeperiod-1) + reg[1]
        result.append(reg_val)
    return np.concatenate([np.full(timeperiod-1, np.nan), result])

def TD_SI_007(df, timeperiod=14):
    """Calculate BETA between high and low prices."""
    # Calculate covariance and variance
    def beta(high, low):
        cov = np.cov(high, low)[0][1]
        var = np.var(low)
        return cov/var if var else 0
    beta_values = [beta(df['high'].values[i-timeperiod:i], df['low'].values[i-timeperiod:i]) for i in range(timeperiod, len(df) + 1)]
    return np.concatenate([np.full(timeperiod-1, np.nan), beta_values])

def TD_SI_008(df, timeperiod=14):
    """Calculate correlation between close prices and volume."""
    corr = [np.corrcoef(df['close'].values[i-timeperiod:i], df['volume'].values[i-timeperiod:i])[0][1] for i in range(timeperiod, len(df) + 1)]
    return np.concatenate([np.full(timeperiod-1, np.nan), corr])

def TD_SI_009(df, timeperiod=14):
    """Calculate standard deviation of volume."""
    result = df['volume'].rolling(window=timeperiod).std()
    return result.values

def TD_SI_010(df, timeperiod=14):
    """Calculate Time Series Forecast of closing prices."""
    result = []
    for i in range(timeperiod-1, len(df)):
        reg = np.polyfit(range(timeperiod), df['close'].values[i-timeperiod+1:i+1], 1)
        forecast = reg[0] * timeperiod + reg[1]
        result.append(forecast)
    return np.concatenate([np.full(timeperiod-1, np.nan), result])

# Alternative function to replace lines using Rolling and Expanding
def TD_SI_011(df, timeperiod=10):
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    tsf_values = [np.polyfit(range(timeperiod), typical_price.values[i-timeperiod:i], 1)[1] for i in range(timeperiod, len(typical_price) + 1)]
    return np.concatenate([np.full(timeperiod-1, np.nan), tsf_values])

def TD_SI_012(df, timeperiod=5):
    """Calculate the variance over a given period."""
    var = df['close'].rolling(window=timeperiod).var()
    return var.values

def TD_SI_013(df, timeperiod=14):
    """Calculate sum of squared deviations (SSD) concerning the mean over given period."""
    mean = df['close'].rolling(window=timeperiod).mean()
    ssd = ((df['close'] - mean) ** 2).rolling(window=timeperiod).sum()
    return ssd.values

# 八、形态指标（Pattern Recognition Indicators），识别特定价格模式的指标，这些模式可能预示着未来市场走势的变化

def TD_PR_001(df: pd.DataFrame) -> np.ndarray:
    # 倒锤头
    body = np.abs(df['close'] - df['open'])
    upper_shadow = df['high'] - np.maximum(df['close'], df['open'])
    lower_shadow = np.minimum(df['close'], df['open']) - df['low']
    inverted_hammer = (
        (upper_shadow > 2 * body) &  # 上影线至少为实体的两倍
        (lower_shadow < 0.1 * body)  # 几乎无下影线
    ).astype(int)
    return inverted_hammer.values

def TD_PR_002(df: pd.DataFrame) -> np.ndarray:
    # 锤头
    body = np.abs(df['close'] - df['open'])
    upper_shadow = df['high'] - np.maximum(df['close'], df['open'])
    lower_shadow = np.minimum(df['close'], df['open']) - df['low']
    hammer = (
        (lower_shadow > 2 * body) &  # 下影线至少为实体的两倍
        (upper_shadow < 0.1 * body)  # 几乎无上影线
    ).astype(int)
    return hammer.values

def TD_PR_003(df: pd.DataFrame) -> np.ndarray:
    # 上吊线
    body = np.abs(df['close'] - df['open'])
    upper_shadow = df['high'] - np.maximum(df['close'], df['open'])
    lower_shadow = np.minimum(df['close'], df['open']) - df['low']
    hanging_man = (
        (lower_shadow > 2 * body) &
        (upper_shadow < 0.1 * body)
    ).astype(int)
    return hanging_man.values

def TD_PR_004(df: pd.DataFrame) -> np.ndarray:
    # 射击之星
    body = np.abs(df['close'] - df['open'])
    upper_shadow = df['high'] - np.maximum(df['close'], df['open'])
    lower_shadow = np.minimum(df['close'], df['open']) - df['low']
    shooting_star = (
        (upper_shadow > 2 * body) &  # 上影线至少为实体的两倍
        (lower_shadow < 0.1 * body)  # 几乎无下影线
    ).astype(int)
    return shooting_star.values

def TD_PR_005(df: pd.DataFrame) -> np.ndarray:
    # 早晨之星
    doji = (df['open'] == df['close'])  # 第二天通常是Doji，但这里简化为较小实体
    small_body = np.abs(df['close'].shift(1) - df['open'].shift(1)) < (df['high'].shift(1) - df['low'].shift(1)) * 0.1
    morning_star = (
        (df['close'].shift(2) < df['open'].shift(2)) &  # 第一天跌
        small_body &  # 第二天小实体
        (df['close'] > df['open'])  # 第三天涨
    ).astype(int)
    return morning_star.values

def TD_PR_006(df: pd.DataFrame) -> np.ndarray:
    # 三只乌鸦
    three_black_crows = (
        (df['close'].shift(2) < df['open'].shift(2)) &
        # (df['open'].shift(1) < df['close'].shift(2)) &
        (df['close'].shift(1) < df['open'].shift(1)) &
        # (df['open'] < df['close'].shift(1)) &
        (df['close'] < df['open'])
    ).astype(int)
    return three_black_crows.values

def TD_PR_007(df: pd.DataFrame) -> np.ndarray:
    # 乌云盖顶
    dark_cloud_cover = (
        (df['close'].shift(1) > df['open'].shift(1)) &  # 第一天阳线
        (df['open'] > df['close'].shift(1)) &  # 第二天开盘高于第一天收盘
        (df['close'] < (df['open'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) / 2)) &  # 收盘价在第一天实体的中部以下
        (df['close'] < df['open'])  # 第二天阴线
    ).astype(int)
    return dark_cloud_cover.values

def TD_PR_008(df: pd.DataFrame) -> np.ndarray:
    # 十字星
    doji_star = (
        (df['open'] == df['close']) &  # 开盘价和收盘价基本相等
        (df['high'] - df['low'] > 3 * (df['open'] - df['close']))  # 上下影线较长
    ).astype(int)
    return doji_star.values

def TD_PR_009(df: pd.DataFrame) -> np.ndarray:
    # 十字
    doji = (df['open'] == df['close']).astype(int)
    return doji.values

def TD_PR_010(df: pd.DataFrame) -> np.ndarray:
    # 两只乌鸦
    two_crows = (
        (df['close'].shift(2) > df['open'].shift(2)) &  # 第一天长阳
        (df['open'].shift(1) > df['close'].shift(2)) &  # 第二天高开
        (df['close'].shift(1) < df['open'].shift(1)) &  # 第二天收阴
        (df['open'] > df['close'].shift(1)) &  # 第三天再次高开
        (df['close'] < df['close'].shift(1))  # 第三天继续收阴
    ).astype(int)
    return two_crows.values

def TD_PR_011(df: pd.DataFrame) -> np.ndarray:
    # CDLSHORTLINE（Short Line Candle 短蜡烛线）
    # 一日 K 线模式，实体短信号
    result = np.where(
        (abs(df['close'] - df['open']) < 0.3 * (df['high'] - df['low'])),
        100,
        0
    )
    return result

def TD_PR_012(df: pd.DataFrame) -> np.ndarray:
    # CDLDRAGONFLYDOJI（Dragonfly Doji 蜻蜓十字）
    # 一日 K 线模式，预示趋势反转
    result = np.where(
        (df['open'] == df['close']) & (df['low'] < df['open']) & (df['high'] == df['close']),
        100,
        0
    )
    return result

def TD_PR_013(df: pd.DataFrame) -> np.ndarray:
    # 一日 K 线模式，市场不确定性信号
    result1 = np.where((df['open'] - df['close']) > 0, -1,np.where((df['open'] - df['close']) < 0,1,0))
    result2 = pd.Series(result1).rolling(window=20).sum().values
    return result2

def TD_PR_014(df: pd.DataFrame) -> np.ndarray:
    # CDLLONGLINE（Long Line Candle 长蜡烛线）
    # 一日 K 线模式，实体长，无上下影线信号
    result = np.where(
        (abs(df['close'] - df['open']) > 0.6 * (df['high'] - df['low'])),
        100,
        0
    )
    return result

def TD_PR_015(df: pd.DataFrame) -> np.ndarray:
    # CDLMARUBOZU（Marubozu 光头光脚/缺影线）
    # 一日 K 线模式，表趋势持续或反转信号
    result = np.where(
        (df['open'] == df['low']) | (df['open'] == df['high']),
        100,
        0
    )
    return result

def TD_PR_016(df: pd.DataFrame) -> np.ndarray:
    # CDLRICKSHAWMAN（Rickshaw Man 黄包车夫）
    var1 = np.where((df['open'] - df['close']) > 0, -1,np.where((df['open'] - df['close']) < 0,1,0))
    var2 = (df['open'] - df['close']) / (df['open'] - df['close']).rolling(90).mean().values
    var3 = df['volume'] / df['volume'].rolling(90).mean().values
    result = var1 * var2 * var3
    return result

def TD_PR_017(df: pd.DataFrame) -> np.ndarray:
    # CDLTAKURI（Takuri (Dragonfly Doji with very long lower shadow) 探水竿）
    # 一日 K 线模式，大致与蜻蜓十字相同
    result = np.where(
        (df['open'] == df['close']) & (df['low'] < df['open']) & (df['close'] - df['low'] > 0.5 * (df['high'] - df['low'])),
        100,
        0
    )
    return result


# Factor 001: Log of total amount



# ============================================================================
# OrderBook因子 - 深度层次类 (Factor 031-080)
# ============================================================================
# 注：由于因子数量较多，这里仅展示部分代表性因子的完整实现
# 完整的177个OrderBook因子将继续补充...











def factor_168(df: pd.DataFrame) -> np.ndarray:
    """
    因子168：相对买卖价差

    计算逻辑：
    1. spread = ask0p - bid0p
    2. mid_price = (ask0p + bid0p) / 2
    3. 返回 spread / mid_price

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 相对价差
    """
    spread = df['ask0p'] - df['bid0p']
    mid_price = (df['ask0p'] + df['bid0p']) / 2
    return np.where(mid_price != 0, spread / mid_price, np.nan)




def factor_171(df: pd.DataFrame) -> np.ndarray:
    """
    因子171：中间价动量

    计算逻辑：
    1. mid_price = (bid0p + ask0p) / 2
    2. 返回 mid_price - rolling_mean(mid_price, 10)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 中间价动量
    """
    mid_price = (df['bid0p'] + df['ask0p']) / 2
    rolling_mean = mid_price.rolling(window=10, min_periods=1).mean()
    momentum = mid_price - rolling_mean
    return momentum.values


def factor_172(df: pd.DataFrame) -> np.ndarray:
    """
    因子172：最优买价滚动标准差

    计算逻辑：
    1. 返回 rolling_std(bid0p, 10)

    指标含义：
    - 值越大：价格波动越大
    - 值越小：价格波动越小

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 价格波动率
    """
    return df['bid0p'].rolling(window=10, min_periods=1).std().values




def factor_175(df: pd.DataFrame) -> np.ndarray:
    """
    因子175：买卖价差的滚动标准差

    计算逻辑：
    1. spread = ask0p - bid0p
    2. 返回 rolling_std(spread, 5)

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 价差滚动标准差
    """
    spread = df['ask0p'] - df['bid0p']
    return spread.rolling(window=5, min_periods=1).std().values


def factor_176(df: pd.DataFrame) -> np.ndarray:
    """
    因子176：最优档深度比

    计算逻辑：
    1. 返回 bid0v / ask0v

    参数：
        df: OrderBook数据

    返回：
        np.ndarray: 最优档深度比
    """
    return np.where(df['ask0v'] != 0, df['bid0v'] / df['ask0v'], np.nan)



def factor_trades_001(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_001：成交额的对数

    计算逻辑：
    1. 返回 log(amount)

    指标含义：
    - 对数变换，减小极端值影响

    参数：
        df: Trades数据

    返回：
        np.ndarray: 对数成交额
    """
    return np.log(df['amount'].values + 1e-6)


def factor_trades_002(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_002：成交量的对数

    计算逻辑：
    1. 返回 log(volume)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 对数成交量
    """
    return np.log(df['volume'].values + 1e-6)


def factor_trades_003(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_003：买入成交额占比

    计算逻辑：
    1. 返回 buy_amount / amount

    指标含义：
    - 接近 1: 买盘主导
    - 接近 0: 卖盘主导

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买入占比
    """
    return np.where(df['amount'] != 0, df['buy_amount'] / df['amount'], np.nan)


def factor_trades_004(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_004：卖出成交额占比

    计算逻辑：
    1. 返回 sell_amount / amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 卖出占比
    """
    return np.where(df['amount'] != 0, df['sell_amount'] / df['amount'], np.nan)


def factor_trades_005(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_005：买入成交量占比

    计算逻辑：
    1. 返回 buy_volume / volume

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买入量占比
    """
    return np.where(df['volume'] != 0, df['buy_volume'] / df['volume'], np.nan)


def factor_trades_006(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_006：卖出成交量占比

    计算逻辑：
    1. 返回 sell_volume / volume

    参数：
        df: Trades数据

    返回：
        np.ndarray: 卖出量占比
    """
    return np.where(df['volume'] != 0, df['sell_volume'] / df['volume'], np.nan)


def factor_trades_007(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_007：净买入金额

    计算逻辑：
    1. 返回 net_buy_amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 净买入金额
    """
    return df['net_buy_amount'].values


def factor_trades_008(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_008：净买入量

    计算逻辑：
    1. 返回 net_buy_volume

    参数：
        df: Trades数据

    返回：
        np.ndarray: 净买入量
    """
    return df['net_buy_volume'].values


def factor_trades_009(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_009：净买入比例

    计算逻辑：
    1. 返回 net_buy_pct

    指标含义：
    - 接近 +1: 强买盘
    - 接近 -1: 强卖盘
    - 接近 0: 买卖平衡

    参数：
        df: Trades数据

    返回：
        np.ndarray: 净买入比例
    """
    return df['net_buy_pct'].values


def factor_trades_010(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_010：买卖均价差

    计算逻辑：
    1. 返回 buy_vwap - sell_vwap

    指标含义：
    - > 0: 买入价高于卖出价，市场看涨
    - < 0: 卖出价高于买入价，市场看跌

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买卖价差
    """
    return (df['buy_vwap'] - df['sell_vwap']).values


def factor_trades_011(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_011：买卖均价比

    计算逻辑：
    1. 返回 buy_vwap / sell_vwap

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买卖价比
    """
    return np.where(df['sell_vwap'] != 0, df['buy_vwap'] / df['sell_vwap'], np.nan)


def factor_trades_012(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_012：成交笔数占比

    计算逻辑：
    1. 返回 buy_count / count

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买入笔数占比
    """
    return np.where(df['count'] != 0, df['buy_count'] / df['count'], np.nan)


def factor_trades_013(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_013：平均每笔成交额

    计算逻辑：
    1. 返回 avg_amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 平均成交额
    """
    return df['avg_amount'].values


def factor_trades_014(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_014：平均买单金额

    计算逻辑：
    1. 返回 buy_amount / buy_count

    参数：
        df: Trades数据

    返回：
        np.ndarray: 平均买单金额
    """
    return np.where(df['buy_count'] != 0, df['buy_amount'] / df['buy_count'], np.nan)


def factor_trades_015(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_015：平均卖单金额

    计算逻辑：
    1. 返回 sell_amount / sell_count

    参数：
        df: Trades数据

    返回：
        np.ndarray: 平均卖单金额
    """
    return np.where(df['sell_count'] != 0, df['sell_amount'] / df['sell_count'], np.nan)


def factor_trades_016(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_016：大单金额占比

    计算逻辑：
    1. 返回 large_order / amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 大单占比
    """
    return np.where(df['amount'] != 0, df['large_order'] / df['amount'], np.nan)


def factor_trades_017(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_017：中单金额占比

    计算逻辑：
    1. 返回 medium_order / amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 中单占比
    """
    return np.where(df['amount'] != 0, df['medium_order'] / df['amount'], np.nan)


def factor_trades_018(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_018：小单金额占比

    计算逻辑：
    1. 返回 small_order / amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 小单占比
    """
    return np.where(df['amount'] != 0, df['small_order'] / df['amount'], np.nan)


def factor_trades_019(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_019：大单净买入

    计算逻辑：
    1. 返回 net_buy_large

    参数：
        df: Trades数据

    返回：
        np.ndarray: 大单净买入
    """
    return df['net_buy_large'].values


def factor_trades_020(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_020：中单净买入

    计算逻辑：
    1. 返回 net_buy_medium

    参数：
        df: Trades数据

    返回：
        np.ndarray: 中单净买入
    """
    return df['net_buy_medium'].values


def factor_trades_021(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_021：小单净买入

    计算逻辑：
    1. 返回 net_buy_small

    参数：
        df: Trades数据

    返回：
        np.ndarray: 小单净买入
    """
    return df['net_buy_small'].values


def factor_trades_022(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_022：大单净买入比例

    计算逻辑：
    1. 返回 net_buy_large / large_order

    参数：
        df: Trades数据

    返回：
        np.ndarray: 大单净买比
    """
    return np.where(df['large_order'] != 0, df['net_buy_large'] / df['large_order'], np.nan)


def factor_trades_023(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_023：中单净买入比例

    计算逻辑：
    1. 返回 net_buy_medium / medium_order

    参数：
        df: Trades数据

    返回：
        np.ndarray: 中单净买比
    """
    return np.where(df['medium_order'] != 0, df['net_buy_medium'] / df['medium_order'], np.nan)


def factor_trades_024(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_024：小单净买入比例

    计算逻辑：
    1. 返回 net_buy_small / small_order

    参数：
        df: Trades数据

    返回：
        np.ndarray: 小单净买比
    """
    return np.where(df['small_order'] != 0, df['net_buy_small'] / df['small_order'], np.nan)


def factor_trades_025(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_025：VWAP与收盘价偏离

    计算逻辑：
    1. 返回 (close - vwap) / vwap

    参数：
        df: Trades数据

    返回：
        np.ndarray: VWAP偏离度
    """
    return np.where(df['vwap'] != 0, (df['close'] - df['vwap']) / df['vwap'], np.nan)


def factor_trades_026(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_026：成交额累积占比

    计算逻辑：
    1. cumsum_amount = cumsum(amount)
    2. 返回 amount / cumsum_amount

    参数：
        df: Trades数据

    返回：
        np.ndarray: 成交额占比
    """
    cumsum_amount = df['amount'].cumsum()
    return np.where(cumsum_amount != 0, df['amount'] / cumsum_amount, np.nan)


def factor_trades_027(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_027：成交量累积占比

    计算逻辑：
    1. cumsum_volume = cumsum(volume)
    2. 返回 volume / cumsum_volume

    参数：
        df: Trades数据

    返回：
        np.ndarray: 成交量占比
    """
    cumsum_volume = df['volume'].cumsum()
    return np.where(cumsum_volume != 0, df['volume'] / cumsum_volume, np.nan)


def factor_trades_028(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_028：买入强度

    计算逻辑：
    1. 返回 buy_amount * buy_count

    指标含义：
    - 综合考虑买入金额和买入频率

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买入强度
    """
    return (df['buy_amount'] * df['buy_count']).values


def factor_trades_029(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_029：卖出强度

    计算逻辑：
    1. 返回 sell_amount * sell_count

    参数：
        df: Trades数据

    返回：
        np.ndarray: 卖出强度
    """
    return (df['sell_amount'] * df['sell_count']).values


def factor_trades_030(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_030：买卖强度比

    计算逻辑：
    1. buy_strength = buy_amount * buy_count
    2. sell_strength = sell_amount * sell_count
    3. 返回 buy_strength / sell_strength

    参数：
        df: Trades数据

    返回：
        np.ndarray: 买卖强度比
    """
    buy_strength = df['buy_amount'] * df['buy_count']
    sell_strength = df['sell_amount'] * df['sell_count']
    return np.where(sell_strength != 0, buy_strength / sell_strength, np.nan)


# ============================================================================
# Trades因子 - 技术指标类 (Factor_trades 031-050)
# ============================================================================

def factor_trades_031(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_031：收益率

    计算逻辑：
    1. 返回 (close - open) / open

    参数：
        df: Trades数据

    返回：
        np.ndarray: 收益率
    """
    return np.where(df['open'] != 0, (df['close'] - df['open']) / df['open'], np.nan)


def factor_trades_032(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_032：对数收益率

    计算逻辑：
    1. 返回 log(close / open)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 对数收益率
    """
    return np.log(df['close'] / (df['open'] + 1e-6))


def factor_trades_033(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_033：价格振幅

    计算逻辑：
    1. 返回 (high - low) / open

    参数：
        df: Trades数据

    返回：
        np.ndarray: 价格振幅
    """
    return np.where(df['open'] != 0, (df['high'] - df['low']) / df['open'], np.nan)


def factor_trades_034(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_034：上影线比例

    计算逻辑：
    1. 返回 (high - max(open, close)) / (high - low)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 上影线比例
    """
    upper_shadow = df['high'] - df[['open', 'close']].max(axis=1)
    range_hl = df['high'] - df['low']
    return np.where(range_hl != 0, upper_shadow / range_hl, np.nan)


def factor_trades_035(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_035：下影线比例

    计算逻辑：
    1. 返回 (min(open, close) - low) / (high - low)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 下影线比例
    """
    lower_shadow = df[['open', 'close']].min(axis=1) - df['low']
    range_hl = df['high'] - df['low']
    return np.where(range_hl != 0, lower_shadow / range_hl, np.nan)


def factor_trades_036(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_036：实体比例

    计算逻辑：
    1. 返回 abs(close - open) / (high - low)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 实体比例
    """
    body = np.abs(df['close'] - df['open'])
    range_hl = df['high'] - df['low']
    return np.where(range_hl != 0, body / range_hl, np.nan)


def factor_trades_037(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_037：收盘价相对位置

    计算逻辑：
    1. 返回 (close - low) / (high - low)

    指标含义：
    - 接近 1: 收盘价接近最高价
    - 接近 0: 收盘价接近最低价

    参数：
        df: Trades数据

    返回：
        np.ndarray: 收盘价相对位置
    """
    range_hl = df['high'] - df['low']
    return np.where(range_hl != 0, (df['close'] - df['low']) / range_hl, np.nan)


def factor_trades_038(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_038：价格动量（5期）

    计算逻辑：
    1. 返回 close - close.shift(5)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 价格动量
    """
    return (df['close'] - df['close'].shift(5)).values


def factor_trades_039(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_039：价格动量（10期）

    计算逻辑：
    1. 返回 close - close.shift(10)

    参数：
        df: Trades数据

    返回：
        np.ndarray: 价格动量
    """
    return (df['close'] - df['close'].shift(10)).values


def factor_trades_040(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_040：价格指数加权移动平均（5期）

    计算逻辑：
    1. 返回 close.ewm(span=5).mean()

    参数：
        df: Trades数据

    返回：
        np.ndarray: EMA
    """
    return df['close'].ewm(span=5, adjust=False).mean().values


def factor_trades_041(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_041：价格指数加权移动平均（10期）

    计算逻辑：
    1. 返回 close.ewm(span=10).mean()

    参数：
        df: Trades数据

    返回：
        np.ndarray: EMA
    """
    return df['close'].ewm(span=10, adjust=False).mean().values


def factor_trades_042(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_042：成交量滚动均值（5期）

    计算逻辑：
    1. 返回 volume.rolling(5).mean()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 成交量均值
    """
    return df['volume'].rolling(window=5, min_periods=1).mean().values


def factor_trades_043(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_043：成交量滚动标准差（5期）

    计算逻辑：
    1. 返回 volume.rolling(5).std()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 成交量标准差
    """
    return df['volume'].rolling(window=5, min_periods=1).std().values


def factor_trades_044(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_044：价格滚动标准差（10期）

    计算逻辑：
    1. 返回 close.rolling(10).std()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 价格波动率
    """
    return df['close'].rolling(window=10, min_periods=1).std().values


def factor_trades_045(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_045：价格与均线的偏离度（5期）

    计算逻辑：
    1. ma5 = close.rolling(5).mean()
    2. 返回 (close - ma5) / ma5

    参数：
        df: Trades数据

    返回：
        np.ndarray: 价格偏离度
    """
    ma5 = df['close'].rolling(window=5, min_periods=1).mean()
    return np.where(ma5 != 0, (df['close'] - ma5) / ma5, np.nan)


def factor_trades_046(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_046：成交量与均量的比值

    计算逻辑：
    1. ma_vol = volume.rolling(5).mean()
    2. 返回 volume / ma_vol

    参数：
        df: Trades数据

    返回：
        np.ndarray: 量比
    """
    ma_vol = df['volume'].rolling(window=5, min_periods=1).mean()
    return np.where(ma_vol != 0, df['volume'] / ma_vol, np.nan)


def factor_trades_047(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_047：价格变化率的滚动均值

    计算逻辑：
    1. pct_change = close.pct_change()
    2. 返回 pct_change.rolling(5).mean()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 收益率均值
    """
    pct_change = df['close'].pct_change()
    return pct_change.rolling(window=5, min_periods=1).mean().values


def factor_trades_048(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_048：价格变化率的滚动偏度

    计算逻辑：
    1. pct_change = close.pct_change()
    2. 返回 pct_change.rolling(10).skew()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 收益率偏度
    """
    pct_change = df['close'].pct_change()
    return pct_change.rolling(window=10, min_periods=3).skew().values


def factor_trades_049(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_049：价格变化率的滚动峰度

    计算逻辑：
    1. pct_change = close.pct_change()
    2. 返回 pct_change.rolling(10).kurt()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 收益率峰度
    """
    pct_change = df['close'].pct_change()
    return pct_change.rolling(window=10, min_periods=4).kurt().values


def factor_trades_050(df: pd.DataFrame) -> np.ndarray:
    """
    因子trades_050：买卖力度差的滚动均值

    计算逻辑：
    1. 返回 net_buy_amount.rolling(5).mean()

    参数：
        df: Trades数据

    返回：
        np.ndarray: 净买力均值
    """
    return df['net_buy_amount'].rolling(window=5, min_periods=1).mean().values

def volume(df):
    return df.volume.values

def amount(df):
    return df.amount.values

def count(df):
    return df['count'].values

def trade_time(df):
    return df.trade_time.values

def buy_amount(df):
    return df.buy_amount.values

def sell_amount(df):
    return df.sell_amount.values

def buy_volume(df):
    return df.buy_volume.values

def sell_volume(df):
    return df.sell_volume.values

def large_order(df):
    return df.large_order.values

def medium_order(df):
    return df.medium_order.values

def small_order(df):
    return df.small_order.values

def large_buy(df):
    return df.large_buy.values

def large_sell(df):
    return df.large_sell.values

def medium_buy(df):
    return df.medium_buy.values

def medium_sell(df):
    return df.medium_sell.values

def small_buy(df):
    return df.small_buy.values

def small_sell(df):
    return df.small_sell.values

def avg_price(df):
    return df['vwap'].rolling(250).kurt().values

def buy_avg_price(df):
    return df['buy_vwap'].rolling(500).skew().values

# def sell_avg_price(df):
#     return df['buy_avg_price'].rolling(1440).rank().values

def sell_avg_price(df):
    return (df['vwap'] - df['sell_vwap']).rolling(360).mean().values

def net_buy(df):
    return df.net_buy_amount.values

def net_buy_pct(df):
    return df.net_buy_pct.values

def net_buy_large(df):
    return df.net_buy_large.values

def net_buy_medium(df):
    return df.net_buy_medium.values

def net_buy_small(df):
    return df.net_buy_small.values

# Time-aggregated percentage and statistics accessor functions
# These fields are expected to exist in the input DataFrame
def large_pct_5m(df):
    return df.large_pct_5m.values

def medium_pct_5m(df):
    return df.medium_pct_5m.values

def small_pct_5m(df):
    return df.small_pct_5m.values

def net_buy_large_pct_5m(df):
    return df.net_buy_large_pct_5m.values

def net_buy_medium_pct_5m(df):
    return df.net_buy_medium_pct_5m.values

def net_buy_small_pct_5m(df):
    return df.net_buy_small_pct_5m.values

def active_buy_mean_5m(df):
    return df.active_buy_mean_5m.values

def active_buy_std_5m(df):
    return df.active_buy_std_5m.values

def active_buy_ratio_5m(df):
    return df.active_buy_ratio_5m.values

def active_sell_mean_5m(df):
    return df.active_sell_mean_5m.values

def active_sell_std_5m(df):
    return df.active_sell_std_5m.values

def active_sell_ratio_5m(df):
    return df.active_sell_ratio_5m.values

def large_pct_15m(df):
    return df.large_pct_15m.values

def medium_pct_15m(df):
    return df.medium_pct_15m.values

def small_pct_15m(df):
    return df.small_pct_15m.values

def net_buy_large_pct_15m(df):
    return df.net_buy_large_pct_15m.values

def net_buy_medium_pct_15m(df):
    return df.net_buy_medium_pct_15m.values

def net_buy_small_pct_15m(df):
    return df.net_buy_small_pct_15m.values

def active_buy_mean_15m(df):
    return df.active_buy_mean_15m.values

def active_buy_std_15m(df):
    return df.active_buy_std_15m.values

def active_buy_ratio_15m(df):
    return df.active_buy_ratio_15m.values

def active_sell_mean_15m(df):
    return df.active_sell_mean_15m.values

def active_sell_std_15m(df):
    return df.active_sell_std_15m.values

def active_sell_ratio_15m(df):
    return df.active_sell_ratio_15m.values

def large_pct_30m(df):
    return df.large_pct_30m.values

def medium_pct_30m(df):
    return df.medium_pct_30m.values

def small_pct_30m(df):
    return df.small_pct_30m.values

def net_buy_large_pct_30m(df):
    return df.net_buy_large_pct_30m.values

def net_buy_medium_pct_30m(df):
    return df.net_buy_medium_pct_30m.values

def net_buy_small_pct_30m(df):
    return df.net_buy_small_pct_30m.values

def active_buy_mean_30m(df):
    return df.active_buy_mean_30m.values

def active_buy_std_30m(df):
    return df.active_buy_std_30m.values

def active_buy_ratio_30m(df):
    return df.active_buy_ratio_30m.values

def active_sell_mean_30m(df):
    return df.active_sell_mean_30m.values

def active_sell_std_30m(df):
    return df.active_sell_std_30m.values

def active_sell_ratio_30m(df):
    return df.active_sell_ratio_30m.values

def large_pct_60m(df):
    return df.large_pct_60m.values

def medium_pct_60m(df):
    return df.medium_pct_60m.values

def small_pct_60m(df):
    return df.small_pct_60m.values

def net_buy_large_pct_60m(df):
    return df.net_buy_large_pct_60m.values

def net_buy_medium_pct_60m(df):
    return df.net_buy_medium_pct_60m.values

def net_buy_small_pct_60m(df):
    return df.net_buy_small_pct_60m.values

def active_buy_mean_60m(df):
    return df.active_buy_mean_60m.values

def active_buy_std_60m(df):
    return df.active_buy_std_60m.values

def active_buy_ratio_60m(df):
    return df.active_buy_ratio_60m.values

def active_sell_mean_60m(df):
    return df.active_sell_mean_60m.values

def active_sell_std_60m(df):
    return df.active_sell_std_60m.values

def active_sell_ratio_60m(df):
    return df.active_sell_ratio_60m.values

def large_pct_120m(df):
    return df.large_pct_120m.values

def medium_pct_120m(df):
    return df.medium_pct_120m.values

def small_pct_120m(df):
    return df.small_pct_120m.values

def net_buy_large_pct_120m(df):
    return df.net_buy_large_pct_120m.values

def net_buy_medium_pct_120m(df):
    return df.net_buy_medium_pct_120m.values

def net_buy_small_pct_120m(df):
    return df.net_buy_small_pct_120m.values

def active_buy_mean_120m(df):
    return df.active_buy_mean_120m.values

def active_buy_std_120m(df):
    return df.active_buy_std_120m.values

def active_buy_ratio_120m(df):
    return df.active_buy_ratio_120m.values

def active_sell_mean_120m(df):
    return df.active_sell_mean_120m.values

def active_sell_std_120m(df):
    return df.active_sell_std_120m.values

def active_sell_ratio_120m(df):
    return df.active_sell_ratio_120m.values

def large_pct_240m(df):
    return df.large_pct_240m.values

def medium_pct_240m(df):
    return df.medium_pct_240m.values

def small_pct_240m(df):
    return df.small_pct_240m.values

def net_buy_large_pct_240m(df):
    return df.net_buy_large_pct_240m.values

def net_buy_medium_pct_240m(df):
    return df.net_buy_medium_pct_240m.values

def net_buy_small_pct_240m(df):
    return df.net_buy_small_pct_240m.values

def active_buy_mean_240m(df):
    return df.active_buy_mean_240m.values

def active_buy_std_240m(df):
    return df.active_buy_std_240m.values

def active_buy_ratio_240m(df):
    return df.active_buy_ratio_240m.values

def active_sell_mean_240m(df):
    return df.active_sell_mean_240m.values

def active_sell_std_240m(df):
    return df.active_sell_std_240m.values

def active_sell_ratio_240m(df):
    return df.active_sell_ratio_240m.values