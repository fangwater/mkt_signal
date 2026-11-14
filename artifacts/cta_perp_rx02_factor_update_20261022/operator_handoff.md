# RX02 操作交接

目标：`jp-meta-elvpn:/home/ubuntu/binance-cta-perp-rx02`，Binance Futures 1min。
正式清单是 `config/cta_perp_rx02/` 下的两个文件，23 币、22 因子、69 项。
保留线上 `cta_perp.json` 的 `enabled=true`、`position_algorithm=v1`、50 USDT
及原入场/退出参数。保留原 Manager JP/SG 地址、令牌、策略前缀、份数和 CPU18。
本交接不使用 2026-10-13 的旧 special→perp 激活脚本；那个脚本处理不同的旧环境、
不同的 Manager 绑定，并要求所有因子 ready，不适用这次更新。

已补充 `operator_switch.py`，默认仅做只读预检；只有操作员传入 `--execute`
才执行下述切换。该脚本使用已核验的隔离审计包，不需要在生产机编译。助手只运行
只读预检，不执行实盘切换。操作员在自己的终端保持 SSH 连接直至脚本退出：

```bash
ssh jp-meta-elvpn 'python3 /home/ubuntu/cta-audits/rx02-factor-replay-20261022T0540Z/operator_switch.py --execute'
```

脚本先检查线上文件仍与回放前一致、Redis/信号/文件的清单相同，并保留现场
交易参数和金额。备份后停 RX02 signal、factor publisher 和配置服务，保留原仓位
flags 和信号 journal；新 publisher 的新 generation 追到 Kafka 实时后启动配置服务
和 signal。就绪门槛不包含全因子 ready。最终检查两个 Manager 成功接收目标，
但不把目标投递成功当作成交证明，成交仍按下文第 7 步只读核对。
任何阶段失败都不自动恢复旧策略，错误后需检查当前状态；已生成备份保留在
RX02 的 `run/rx02_factor_before_<UTC>/`。不得未经检查再次运行切换脚本。

已校验的待安装文件在同主机
`/home/ubuntu/cta-audits/rx02-factor-replay-20261022T0540Z/`：根目录为二进制和
两个配置文件，`payload/scripts/` 为三个脚本/allowlist 文件。SHA-256 见
`build_manifest.json`；该目录未连接线上目标发布链路。

以下步骤由操作员在自己的终端完成，修改清单并启动信号会触发真实仓位变化。

1. 重新核对实盘配置、Redis 清单和进程，确认与 `before/` 和
   `configuration_diff.json` 一致，避免覆盖并发修改。备份线上二进制、配置、
   脚本、Redis 清单、`run/cta_perp_position_flags.json` 和历史；包含 `env.sh`
   的备份只能存主机权限 0700 的私有目录，不显示或回传凭据。
2. 在 RX02 目录通过 `scripts/stop_cta_perp_signal.sh` 停仓位发布器，再通过
   `scripts/stop_cta_perp_factor_model_1m_pub.sh` 停旧因子发布器，确认退出。
   不停止 Exec、账户监控、Manager、Viz、交易引擎或其他 RX 环境。
3. 将已校验 release 二进制、候选 publisher TOML、候选因子 JSON、最新
   `scripts/cta_perp_factor_config.py`、`factor_validation_allowlist.txt` 和
   `scripts/start_cta_perp_factor_model_1m_pub.sh` 原子安装到对应位置。
   保留旧 `factor_history` 目录为备份，让正式启动写新的默认历史目录。
   不删除或重建仓位 flags、信号 journal、恢复状态或 Exec RocksDB。
4. 在 RX02 目录加载现有 `env.sh`，通过现有 helper 原子替换 Redis 清单：

   ```bash
   python3 scripts/cta_perp_factor_config.py --env-name binance-cta-perp-rx02 --config config/cta_perp_symbol_factors.json --execute
   ```

   该 helper 使用当前 revision 和 Redis CAS，保存前验证 publisher 可计算范围。
5. 通过 `scripts/start_cta_perp_factor_model_1m_pub.sh` 启动新发布器，核对运行
   二进制 SHA-256、CPU18、新 history generation。等待新 `recovery.json` 的
   `caught_up=true`、23 个币最新闭合分钟持续推进，日志没有解码/计算错误。
   **不检查全因子 ready 作为启动信号的前置条件。** 不复用审计目录的
   `recovery.json`；replay-only 不代表实时 publisher 已拥有输出通道。
6. 通过 `scripts/start_cta_perp_config_server.sh` 重新加载配置 helper，再通过
   `scripts/start_cta_perp_signal.sh` 启动原信号二进制。保留原仓位状态；被移除
   的 24 币通过原策略零目标清理，仍在清单的未就绪币保留原数量。不要删账本
   或将数量初始化为零。首次新方向只使用启动后的实时闭合分钟。
7. 只读确认新的 69 项清单、无 configuration/publication/journal 错误，JP 和 SG
   两个 Manager 都接收新目标，Exec 接收目标一致。再确认切换后新目标对应的
   订单或成交事件，区分新策略产生的开仓与移出币的清理成交。仅服务在线或零
   目标投递成功，不算新仓位开始执行。

如果失败，保留现场和已发布目标，不自动恢复旧因子清单，以免已成交的目标被
旧策略重新改动。标准 stop wrapper 会匹配同目录的 publisher；本流程没有在
同目录并行启动候选 publisher，不能直接用于双进程热交接。
