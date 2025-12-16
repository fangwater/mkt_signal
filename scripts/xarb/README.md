# xarb scripts

用于“跨所/跨 venue”部署的小脚本集合（与 `fr_signal` 新增的 `--open-venue/--hedge-venue` 参数配套）。

- 启动 `fr_signal`（指定 open/hedge）：
  - `bash scripts/xarb/start_fr_signal_pair.sh binance-margin okex-futures`
- 复制 symbol lists 到 “venue pair” 后缀（可选）：
  - `python scripts/xarb/sync_symbol_lists_pair.py --open-venue binance-margin --hedge-venue okex-futures --source okex`

