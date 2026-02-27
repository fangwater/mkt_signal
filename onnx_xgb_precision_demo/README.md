# ONNX vs XGBoost Precision Demo

这个目录下的脚本用于对比同一个模型在两条推理链路上的输出差异：

- `onnxruntime`（ONNX）
- `xgboost` 原生 Python 推理（JSON Booster）

脚本会用大量样本（默认 `100000`）做批量推理，并输出误差统计指标（`MAE/RMSE/P99/max` 等）。

## 依赖

```bash
pip install numpy xgboost onnxruntime
```

## 用法 1：直接从 model_manager 拉取

`model_manager` 提供：

- `GET /api/models/{model_name}/model/{symbol}` -> `payload.model_json`
- `GET /api/models/{model_name}/model_onnx/{symbol}` -> onnx binary

示例：

```bash
python3 onnx_xgb_precision_demo/compare_onnx_xgb.py \
  --base-url http://54.64.147.69:6300 \
  --model-name binance-futures-mm-xgb-test \
  --symbol SOLUSDT \
  --n-samples 200000 \
  --seed 7 \
  --dist normal
```

下载后的模型会缓存到 `/tmp/onnx_xgb_precision_demo/`。

## 用法 2：本地文件对比

```bash
python3 onnx_xgb_precision_demo/compare_onnx_xgb.py \
  --onnx-path /tmp/mkt_signal_model_pub_onnx/binance_futures_mm_xgb_test/SOLUSDT.xxx.onnx \
  --xgb-path /home/fanghaizhou/project/mkt_signal/model/SOLUSDT_mid_chg_1m_model.json \
  --n-samples 100000 \
  --seed 42
```

## 常用参数

- `--n-samples`: 样本数（建议 `>= 50000`）
- `--dist`: 随机输入分布（`normal|uniform|laplace`）
- `--value-scale`: 输入缩放
- `--sample-npy`: 使用真实样本（`[N, F]` 的 `.npy`），会覆盖随机采样
- `--feature-dim`: 手工指定特征维度
- `--output-json`: 保存完整结果到 JSON 文件
- `--ort-intra-threads/--ort-inter-threads/--xgb-nthread`: 线程配置（默认都为 `1`，便于复现）

## 结果说明

重点看以下字段：

- `mae`, `rmse`, `max_abs_diff`
- `abs_diff_quantile`（尤其 `p95/p99`）
- `threshold_pass`（不同阈值下通过率）
- `top_errors`（最大误差样本）
