from pathlib import Path
import json

root = Path(__file__).resolve().parents[2]
here = Path(__file__).resolve().parent
live = json.loads((here / 'live_validation.json').read_text())
assert live['signal_enabled'] and len(live['target_checks']) == 34
stamp = live['checked_at'][:16].replace('T', ' ') + ' UTC'
common = f'''{stamp} RX03 CTA Perp 已接入独立 `binance_exec_trade07` 实盘，
Manager source `trade07 / lgq_cta_RX03` 跟随 `virtual04 / cta_perp_rx03`，
倍率1，五因子各1份，14币/34组合，每组合100 USDT。启用前账户为空仓、无挂单；
新Exec使用独立namespace、Redis键及RocksDB，没有迁移旧持仓或历史。
执行模板 `binance_cta_perp_rx03_ctaker` 复制RX02当前 `lgq_cta_ctaker`，
五份实际运行参数逐项相同；买卖各最多10个挂单，10秒200单、每分钟400单。
Maker费率-0.00002、Taker 0.000175、理论TWAP 0.0004。
34个实时目标与Exec一致，账户position_ready、发布日志健康，Manager无待发布项。
证据：`artifacts/cta_perp_rx03_activation/`。
'''

path = root / 'docs/core_allocation.md'
text = path.read_text()
text = text.replace('最后更新: 2026-10-14 07:30 UTC', '最后更新: ' + stamp, 1)
marker = '2026-10-14 07:10 UTC 新增观察环境'
text = text.replace(marker, common + '''Exec07的六个进程均使用housekeeping CPU0-5,48-95；未增加隔离核。
RX03因子publisher仍为PID2038356、5线程CPU19，signal加载新Manager配置后重启；
config/dashboard保留。现有其他环境的进程与配置核对保持。

此前观察部署记录：
''' + marker, 1)
text = text.replace('2026-10-14 新增独立观察实例，PID2038356/5线程', '2026-10-14 独立RX03实盘信号publisher，PID2038356/5线程')
path.write_text(text)

path = root / 'docs/jp-meta-elvpn_ip_binding.md'
text = path.read_text().replace('最后更新: 2026-10-14 07:30 UTC', '最后更新: ' + stamp, 1)
marker = '2026-10-14 07:10 UTC 新增 `binance-cta-perp-rx03`'
text = text.replace(marker, common + '''Exec07交易与认证账户查询固定使用 `172.31.35.228`，沿用RX03原RapidX白名单；
未新增EIP/ENI/路由。该环境trade_engine.toml加入Manager Kline交易IP排除清单。
4191网关新增带Manager鉴权的 `/exec_trade07/`、`config/`；对应loopback10047/18167。
信号页面继续使用 `/cta/binance-cta-perp-rx03/` 和loopback10197/19187。

此前观察部署记录：
''' + marker, 1)
text = text.replace('；rx03 停盘；Exec `trade02`–`trade04`', '；RX03 Perp 接 Exec `trade07`；Exec `trade02`–`trade04`')
text = text.replace('binance-cta-perp-rx03        默认出口 / 172.31.35.228       独立Perp观察，loopback10197/19187，发布关闭', 'binance-cta-perp-rx03        默认出口 / 172.31.35.228       独立Perp实盘信号，loopback10197/19187，发布到Exec07')
line = 'binance_exec_trade06         172.31.35.228                  RX01 RapidX，现货/永续在线，follow virtual03/cta_spread_rx01，ctaker'
text = text.replace(line, line + '\nbinance_exec_trade07         172.31.35.228                  RX03 RapidX，永续在线，follow virtual04/cta_perp_rx03，ctaker')
path.write_text(text)

path = root / 'docs/cta_signal_families.md'
text = path.read_text().replace('最后更新：2026-10-14 07:30 UTC', '最后更新：' + stamp, 1)
text = text.replace('## RX03 Perp 观察部署', '## RX03 Perp 实盘\n\n' + common + '''
信号发布已开启，NQ 120/min60、因子分位2880/min1440、zscore1440/min1000，
开仓0.9/0.1、NQ过滤0.5/0.5、退出0.7/0.3。运行环境保留100 USDT，
仓库profile默认enabled=false，重新部署必须先检查独立Manager/Exec绑定。
页面：`/cta/binance-cta-perp-rx03/`、`/exec_trade07/`，均由4191网关提供。

## RX03 Perp 初始观察部署（历史）''', 1)
path.write_text(text)

path = root / 'config/cta_perp_rx03/README.md'
text = path.read_text().replace('JP observation environment:', 'JP signal environment:')
text += f'''
Live activation verified {stamp}: the deployed environment publishes to
Manager virtual04 (`cta_perp_rx03`), followed by `binance_exec_trade07` at
multiplier 1 with five bindings at one share each. Runtime publication is enabled;
this provisioning profile retains its disabled default. Exec Viz/Config use
loopback ports 10047/18167 through `/exec_trade07/` on the 4191 gateway.
The independent `binance_cta_perp_rx03_ctaker` template and account order limits
match RX02 at activation. Maker/taker/theoretical rates are
-0.00002 / 0.000175 / 0.0004. The amount remains 100 USDT per pair.
See `artifacts/cta_perp_rx03_activation/` for checks and release hashes.
'''
path.write_text(text)

(here / 'README.md').write_text(f'''# RX03 CTA Perp / Exec07

Verified {stamp}. Live publication enabled for 14 symbols, five factors and
34 signed bindings at 100 USDT each. Signal: `binance-cta-perp-rx03`.
Execution: `binance_exec_trade07`, RapidX Binance Futures, source IP172.31.35.228.
Manager: virtual04/cta_perp_rx03, shares1, follower multiplier1.

`catalog_verified.json` records the five bindings, exact RX02 execution-template
comparison, risk limits and fees. `live_validation.json` verifies every target,
fresh factors, account readiness and protected existing processes/configurations.
`health.json` records process affinity, publication logs and gateway checks.
`release.json` contains locally built binary hashes; `publish.log` records remote
SHA-256 verification and atomic installation. All52 Exec Python checks passed.

The account was flat with no orders before startup. No old ledger or fabricated
inventory was imported. The signal publisher stayed warm on CPU19; Exec processes
use housekeeping CPUs0-5,48-95. Existing RX01/RX02 processes and settings remained.
Publication credentials remain only in the remote signal env; exchange credentials
remain only in the remote Exec env. Temporary administrative sessions were removed.

Signal URL: http://13.115.227.29:4191/cta/binance-cta-perp-rx03/
Exec URL: http://13.115.227.29:4191/exec_trade07/
Manager URL: http://13.115.227.29:4191/manager/config/
''')
print('Updated live inventories, signal profile documentation and deployment evidence')
