# 批量设置 Binance UM 杠杆（Python 脚本）

本脚本用于批量设置组合保证金（PAPI）UM 合约的杠杆倍数。读取 JSON 配置，遍历常用交易对逐一设置。

- 脚本路径：`scripts/set_um_leverage.py`
- 默认配置：`config/binance_um_leverage.json`
- 依赖：Python 3 标准库（不依赖第三方包）

## 准备
- 在环境变量设置密钥（不要写入仓库）：
  - `export BINANCE_API_KEY=...`
  - `export BINANCE_API_SECRET=...`
- 核对配置（如需）：编辑 `config/binance_um_leverage.json`
  ```json
  {
    "base_url": "https://papi.binance.com",
    "default_leverage": 3,
    "symbols": [
      "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
      "ADAUSDT", "DOGEUSDT", "TRXUSDT", "MATICUSDT", "DOTUSDT",
      "LTCUSDT", "BCHUSDT", "LINKUSDT", "AVAXUSDT", "ATOMUSDT",
      "FILUSDT", "APTUSDT", "OPUSDT", "ARBUSDT", "NEARUSDT"
    ]
  }
  ```
  - `default_leverage`：未指定的 symbol 使用该默认值（默认 3）
  - `symbols`：可写字符串或对象，若个别交易对要单独设置：
    ```json
    { "symbols": ["BTCUSDT", {"symbol": "ETHUSDT", "leverage": 5}] }
    ```

## 运行
- 基本用法：
  ```bash
  python3 scripts/set_um_leverage.py
  ```
- 指定配置文件路径：
  ```bash
  python3 scripts/set_um_leverage.py path/to/your.json
  ```

## 输出示例
- 每条设置会输出状态码、已用权重和下单计数（来自响应头）：
  ```
  [1/20] BTCUSDT -> 3: OK 200; used_weight=10, order_count=1
  ```
- 失败会打印 body，便于诊断（如权限、白名单、参数等）。

## 说明
- 本脚本针对 PAPI（组合保证金）UM 接口：`POST /papi/v1/um/leverage`。
- 若你的环境是 FAPI（普通 U 本位），请使用对应域名与路径，或另写脚本。
- 建议仅在首次接入时批量设置；后续杠杆倍数可长期保持，不必频繁修改。

