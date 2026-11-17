# 国内期货 CTA 目标手数进程

最后更新：2026-11-17

`cta-cn_futures-01` 位于 `/home/fanghaizhou/cta-cn_futures-01`，由环境脚本包装
PM2 管理。范围为 `SHFE/SN`、1h、12 条规则，每条输出 `-1/0/+1`，乘以
`lots_per_rule` 后合计为绝对目标手数。复用 CN 原始因子、Prep 小时归一化、
百分位和开平仓判断，不做金额到数量的换算，不连接账户或下单服务。
该进程使用公司 ZMQ；本机禁止执行 CTP SDK 的约束保持有效。

## 数据接续

1. 启动后立即订阅公司 ZMQ `tcp://10.61.20.12:13005`，在内存中持续接收。
2. 从 REST `http://127.0.0.1:18313/api/v1/dominant` 选择 `xsge/SN` 的原生合约。
   下一原生交易日只采用明确的 `scheduled_next`，不推算交割月份。
3. 从 `http://127.0.0.1:18312` 的 `cn_market.v1.CnFuturesMarket` 查询分钟、阈值
   和时段。参数未齐时暂存原始 ZMQ；参数齐后复用共享 `cn_futures_core`，持续合成
   1min baseline。首次接入的残缺分钟丢弃，第一根完整分钟固定为接续边界。
4. gRPC 因 MDL 延迟而落后是正常情况。本地继续保留第一根完整分钟和所有后续分钟，
   每 5 秒补查 gRPC，等它到达并最终完成这根固定分钟。不能移动起点追最新分钟。
5. 核对边界分钟的成交、分桶、OHLC、mid 和五档输入。用 gRPC 前缀（包含边界分钟）
   预热，再按顺序消费边界之后的 ZMQ 缓存。边界只消费一次，保留尚未完整的小时。
6. 缓存追完后接实时，释放 gRPC 历史分钟和启动缓存，结束历史查询。gRPC 仅用于
   进程初始化；下一次查询历史须等进程重启。追赶只恢复因子窗口，保留事实目标
   账本和决策时钟；激活后新闭合的小时才可产生新目标。

网络请求期间 ZMQ 捕获持续运行；控制器先排空排队的数据和 reset 标记，再对齐。
分钟主要由来源的下一分钟快照确认闭合，不能仅因 gRPC 请求耗时、本机时间推进，
就将滞后来源中的残缺分钟当作完整分钟。时段末尾只有在队列已排空、末秒快照新鲜
并经过短暂宽限后才允许收尾。

初始化期间，断连、身份冲突、队列丢失或缓冲耗尽会重置接续边界；gRPC 暂时失败
或落后不会清除本地分钟缓存。初始化完成后，普通交易日切换继续使用 ZMQ、启动时
获取的参数和已经预热的因子窗口。运行期间发生来源缺口、合约换主力或缺少新月份
参数时，保留已发布目标并标记 `restart_required`；重启后重新初始化，不在运行代
重新调用 gRPC。主力 REST 查询继续独立运行。

小时沿用研究口径：连续源分钟从片段第一分钟起每 60 根一组，保留时段短尾。
真实零成交分钟占用时间槽；休市和源缺行不凭空补分钟。mid 优先使用盘口，缺少时
采用用户允许的 close 近似。缺少五档、分桶或适用月份阈值时暂停，不用零值替代。

## gRPC 契约

- `GetMinuteBars`：响应仅 `contract / bars / data_state`，按 30 日分页读取 120 天，
  `include_empty_minutes=true`；分钟暂态数据可反复更新，覆盖查询区间时删除已消失的行。
- `GetSizeThresholds`：查询 SHFE/SN，按 `applies_month`（上海自然月）使用产品 P50/P90。
  不在策略侧维护导出器或阈值文件，不默认使用别的月份。
- `GetTradingSessions`：查询 SHFE/SN 时段；快照日期必须与分钟数据一致。
- 归档分钟为最终数据。暂态分钟须满足 `open_time_ms + 60000 <= live_through_ms`
  才能作为 gRPC 接续边界。来源尚未补到边界时继续等待并积累本地分钟。

从 2026-11 起，服务端分桶采用前一完整上海自然月、逐交易日主力合约合并后的
成交区间金额阈值；更早归档保留服务端定义的旧分类。大小指快照推断成交区间，
不是交易所提供的逐笔订单大小。相比旧部署的前月全合约阈值，口径发生变化，
应从服务端数据重新预热，不能宣称新旧分桶相同。

策略不再调用 `StreamIntradayFrames`，不读写历史 PB 页、行情 JSONL、会话文件或
阈值文件，也不承担市场服务的落盘工作。保留的文件是事实 `positions.json`、
`targets.json`、配置及其审计、访问凭据、进程锁和运维验证收据。

## 配置面板与状态

公网配置面板：`http://124.223.189.200/cta-cn-futures/`。复用 Prep 配置页面，
固定 SN / 1h，以手数替代金额，可编辑因子、符号、NQ、开关及开平阈值。
GET `/api/config` 无需授权；POST `/api/preview`、`/api/config` 需要环境私有凭据。
配置读取不包含连接地址、路径或凭据。公网继续使用现有反向隧道，云端 Nginx
配置为 `config/cta_cn_futures_public.nginx.conf`，通过环境本地的
`scripts/cta_cn_futures_public.sh` 管理。

保存前校验 revision、字段和实际已观测小时上的因子就绪状态。保存重建公式窗口，
不改事实仓位，不补历史目标。关闭策略停止新决策，保留旧目标。外部直接改配置
需要经环境脚本重启。调整手数只影响后续实时决策，不重算已有目标。

内部只读接口仍为 `127.0.0.1:19401/state` 和 `/targets`，不通过公网面板转发。
`/state` 提供因子就绪、实时就绪、错误、`initialization_complete` 和 `restart_required`；
初始化期间额外提供 gRPC 水位、固定首分钟和缓存分钟数。
消费 `/targets` 必须检查 `factor_ready / live_ready / decision_ms`。换主力后的
首次有效决策同时输出旧合约目标 0 和新合约目标，重复读取不产生增量交易。

## 运维和发布

仅通过环境本地脚本：

```bash
cd /home/fanghaizhou/cta-cn_futures-01
./scripts/cta_cn_futures_process.sh status
./scripts/cta_cn_futures_process.sh preflight --execute
./scripts/cta_cn_futures_process.sh restart --execute
```

省略 `--execute` 只打印计划。preflight 在临时目录仅复制事实账本，通过 gRPC
验证新参数接口、输入和因子就绪；失败则不停止现有进程。支持
`preflight --binary <候选文件> --config <候选配置> --execute`，便于安装前检查。
停止或重启前保存日志与账本，不使用裸 PM2 命令操作环境。

网络接续改造使用已推送的 `market_replay` 正式协议提交
`5ee7d1d793e1fd02f78280c224d710390767aa59`，proto 和共享 core 固定到同一个 rev，
不依赖临时 proto 或路径覆盖。部署仍须先验证生产服务提供新参数方法、历史输入
和实时分钟接续；预检失败则保留当前代。环境配置只移除旧
`session_url / size_thresholds` 两项，保留用户参数、凭据和事实账本。
本次发布更新 worker、环境本地生命周期脚本和配置；保留配置访问凭据及事实账本。
源码和构建校验收据位于 `data/ops/cta_cn_grpc_refactor_check/`，发布收据由
`data/ops/cta_cn_futures_01_latest.txt` 指向（均不提交到 Git）。

当前验证：23 项信号测试、8 项 SDK-free 行情测试和 3 项生命周期检查通过；
真实 gRPC 预热 `sn2612` 的 36,720 根分钟形成 712 根小时，12/12 就绪，事实账本不变。
盘后启动的版本先保留已有目标，等待下一交易时段形成首根完整 ZMQ 分钟，并完成
gRPC 对齐；`initialization_complete / live_ready` 以内部接口实际返回为准。
