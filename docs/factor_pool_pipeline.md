# Factor Pool Config Pipeline

目标：用单个配置文件完成以下流程：
- 从 `model_manager` 获取 `factors`、`model_json`、`model_onnx`
- 读取 H5 因子数据并按模型 `factors` 重构
- 用 Python 跑 XGBoost 推理
- 用 Rust 跑 ONNX 推理
- 输出对比 CSV 与误差指标 JSON

## 文件

- Pipeline 脚本：`scripts/factor_pool_pipeline.py`
- Rust ONNX binary：`src/bin/onnx_csv_predict.rs`
- Pipeline 配置：`config/factor_pool_pipeline.toml`
- Rust ONNX 配置模板：`config/onnx_csv_predict.toml`

## 依赖

Python:

```bash
pip install numpy pandas tables xgboost
```

Rust:

```bash
cargo build --release --bin onnx_csv_predict
```

## 运行

```bash
python3 scripts/factor_pool_pipeline.py --config config/factor_pool_pipeline.toml
```

脚本会自动调用 Rust binary（由 `config/factor_pool_pipeline.toml` 中 `[rust_onnx]` 控制）。

## 输出

默认输出目录由 `output.work_dir` 控制，主要产出：
- `rebuilt_model_factors.h5`：重构后的 H5
- `rebuilt_features.csv`：给 Rust ONNX 的特征矩阵
- `xgb_pred.csv`：Python XGBoost 预测
- `onnx_pred.csv`：Rust ONNX 预测
- `pred_compare.csv`：逐行对比（含 `abs_diff`、`rel_abs_diff`）
- `metrics.json`：误差统计（`MAE/RMSE/P95/P99/max_abs_diff/top_errors`）

说明：Rust ONNX 会直接从 `model_manager` 拉取模型，不依赖本地 `onnx_path`。
