#!/usr/bin/env python3
"""
对比 Rust factor_test 输出与 Python 因子池的计算结果。
覆盖所有 172 个 Rust 有效因子（factor_NNN, baseline_NNN, TD_*, TP_VPI_*, factor_trades_*, extra）。

对比方式：
- 对同一组合成输入序列（600 bars），Python 计算整个序列后取最后一个值
- 与 Rust 输出的最终值对比
- 对于时序因子（rolling/diff/pct_change），两端都基于完整序列计算

使用方法：
1. cargo run --bin factor_test -- --output /tmp/factor_test_output.json
2. python3 compare_factor_test.py
"""

import json
import sys
import traceback
import numpy as np
import pandas as pd

sys.path.insert(0, '.')
import final_factor_pool_update20260123 as fpool


def load_rust_output(path="/tmp/factor_test_output.json"):
    with open(path, 'r') as f:
        return json.load(f)['scenarios']


def build_dataframe(input_data):
    """从 Rust 输出的 input 构建 DataFrame，包含 trade_flow 字段 + 20档 depth"""
    df_dict = {}
    for field_name, values in input_data.items():
        if field_name not in ['depth_bids', 'depth_asks']:
            df_dict[field_name] = values
    df = pd.DataFrame(df_dict)

    depth_bids = input_data['depth_bids']
    depth_asks = input_data['depth_asks']
    for i in range(20):
        df[f'bid{i}p'] = [bar[i][0] if i < len(bar) else 0.0 for bar in depth_bids]
        df[f'bid{i}v'] = [bar[i][1] if i < len(bar) else 0.0 for bar in depth_bids]
        df[f'ask{i}p'] = [bar[i][0] if i < len(bar) else 0.0 for bar in depth_asks]
        df[f'ask{i}v'] = [bar[i][1] if i < len(bar) else 0.0 for bar in depth_asks]
    return df


# ============================================================================
# Rust 因子名 -> Python 函数的映射
# ============================================================================

def resolve_python_func(rust_name):
    """
    将 Rust 输出的因子名映射到 Python 函数。
    返回 (func, needs_df_only) 或 None。
    
    Rust 命名格式:
      factor_001 -> fpool.factor_001(df)
      baseline_004 -> fpool.baseline_004(df)
      TD_TI_010 -> fpool.TD_TI_010(df)
      TP_VPI_001 -> fpool.TP_VPI_001(df)
      factor_trades_014 -> fpool.factor_trades_014(df)
      avg_price / buy_avg_price / sell_avg_price / small_buy / small_sell / net_buy_large
        -> 直接从 df 列取最后一个值
    """
    # extra 因子：Python 中有同名函数
    # avg_price -> fpool.avg_price(df) = df['vwap'].rolling(250).kurt()
    # buy_avg_price -> fpool.buy_avg_price(df) = df['buy_vwap'].rolling(500).skew()
    # sell_avg_price -> fpool.sell_avg_price(df) = (df['vwap'] - df['sell_vwap']).rolling(360).mean()
    # small_buy -> fpool.small_buy(df) = df.small_buy.values
    # small_sell -> fpool.small_sell(df) = df.small_sell.values
    # net_buy_large -> fpool.net_buy_large(df) = df.net_buy_large.values

    # 直接查找函数
    func = getattr(fpool, rust_name, None)
    if func is not None and callable(func):
        return ('func', func)

    return None


def compute_python_value(rust_name, df):
    """
    计算单个因子的 Python 值。
    返回 (value, error_msg)。
    对于时序因子，计算整个序列后取最后一个有效值。
    """
    resolved = resolve_python_func(rust_name)
    if resolved is None:
        return np.nan, f"Python 中无对应函数: {rust_name}"

    kind, payload = resolved

    if kind == 'extra':
        col_name = payload
        if col_name in df.columns:
            return float(df[col_name].iloc[-1]), None
        return np.nan, f"列 {col_name} 不存在"

    if kind == 'func':
        func = payload
        try:
            result = func(df)
            if isinstance(result, pd.Series):
                result = result.values
            if len(result) == 0:
                return np.nan, "空结果"
            # 取最后一个值
            val = float(result[-1])
            return val, None
        except Exception as e:
            return np.nan, f"{type(e).__name__}: {e}"

    return np.nan, "未知类型"


def compare_values(rust_val, py_val):
    """
    比较两个值，返回 (match, abs_diff, rel_diff, status)
    """
    rust_nan = rust_val is None or (isinstance(rust_val, float) and np.isnan(rust_val))
    py_nan = np.isnan(py_val) if isinstance(py_val, (int, float)) else True

    if rust_nan and py_nan:
        return True, 0.0, 0.0, "both_nan"

    if rust_nan != py_nan:
        return False, float('inf'), float('inf'), "nan_mismatch"

    rv = float(rust_val)
    pv = float(py_val)
    abs_diff = abs(rv - pv)
    denom = max(abs(rv), abs(pv), 1e-10)
    rel_diff = abs_diff / denom

    # 容差: 相对误差 < 1e-6 视为匹配
    matched = rel_diff < 1e-6
    return matched, abs_diff, rel_diff, "ok"


def main():
    scenarios = load_rust_output()
    print(f"加载了 {len(scenarios)} 个场景\n")

    all_results = {}

    for scenario_name, scenario_data in scenarios.items():
        print(f"{'='*90}")
        print(f"场景: {scenario_name}")
        print(f"{'='*90}")

        df = build_dataframe(scenario_data['input'])
        rust_factors = scenario_data['fusion_factors']
        print(f"数据: {len(df)} bars, Rust 有效因子: {sum(1 for v in rust_factors.values() if v is not None)}")

        # 只对比 Rust 有值的因子
        valid_rust = {k: v for k, v in rust_factors.items() if v is not None}

        match_count = 0
        mismatch_count = 0
        skip_count = 0
        mismatches = []
        errors = []

        for rust_name in sorted(valid_rust.keys()):
            rust_val = valid_rust[rust_name]
            py_val, err = compute_python_value(rust_name, df)

            if err:
                skip_count += 1
                errors.append((rust_name, err))
                continue

            matched, abs_diff, rel_diff, status = compare_values(rust_val, py_val)

            if matched:
                match_count += 1
            else:
                mismatch_count += 1
                r_str = "NaN" if rust_val is None else f"{float(rust_val):.10g}"
                p_str = "NaN" if np.isnan(py_val) else f"{float(py_val):.10g}"
                mismatches.append((rust_name, r_str, p_str, abs_diff, rel_diff, status))

        total = match_count + mismatch_count + skip_count
        print(f"\n结果统计:")
        print(f"  总计: {total} | 匹配: {match_count} | 不匹配: {mismatch_count} | 跳过: {skip_count}")

        if mismatches:
            mismatches.sort(key=lambda x: -x[4] if x[4] != float('inf') else 1e30)
            print(f"\n不匹配的因子 ({len(mismatches)} 个):")
            print(f"  {'因子':<20s} {'Rust':>18s} {'Python':>18s} {'绝对差':>14s} {'相对差':>14s} {'状态'}")
            print(f"  {'-'*90}")
            for name, r_str, p_str, ad, rd, st in mismatches:
                ad_s = f"{ad:.6e}" if ad != float('inf') else "inf"
                rd_s = f"{rd:.6e}" if rd != float('inf') else "inf"
                print(f"  {name:<20s} {r_str:>18s} {p_str:>18s} {ad_s:>14s} {rd_s:>14s} {st}")

        if errors:
            print(f"\n跳过的因子 ({len(errors)} 个):")
            for name, err in errors[:20]:
                print(f"  {name:<20s} {err}")
            if len(errors) > 20:
                print(f"  ... 还有 {len(errors)-20} 个")

        all_results[scenario_name] = {
            'match': match_count,
            'mismatch': mismatch_count,
            'skip': skip_count,
            'mismatches': mismatches,
        }
        print()

    # 总结
    print(f"{'='*90}")
    print("总结")
    print(f"{'='*90}")
    for sname, r in all_results.items():
        total = r['match'] + r['mismatch'] + r['skip']
        pct = r['match'] / max(total - r['skip'], 1) * 100
        print(f"  {sname:<20s} 匹配: {r['match']:>3d}/{total-r['skip']:<3d} ({pct:.1f}%)  "
              f"不匹配: {r['mismatch']:>3d}  跳过: {r['skip']:>3d}")

    # 汇总所有场景中出现的不匹配因子
    all_mismatched = set()
    for r in all_results.values():
        for m in r['mismatches']:
            all_mismatched.add(m[0])
    if all_mismatched:
        print(f"\n所有场景中出现不匹配的因子 ({len(all_mismatched)} 个):")
        for name in sorted(all_mismatched):
            print(f"  {name}")


if __name__ == "__main__":
    main()
