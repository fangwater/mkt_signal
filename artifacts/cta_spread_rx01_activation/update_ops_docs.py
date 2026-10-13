import json,pathlib,re
root=pathlib.Path('/home/fanghaizhou/mkt_signal')
e=json.loads((root/'artifacts/cta_spread_rx01_activation/live_validation.json').read_text())
stamp=e['validated_at'][:16].replace('T',' ')+' UTC'
assert e['signal']['status']['enabled'] and e['rx02_enabled']
ps=e['processes']['binance_exec_trade06']
plist='、'.join(p['comm']+' PID'+str(p['pid'])+' / CPU '+p['cpu_mask'] for p in ps)
blocks={
'docs/core_allocation.md':f"""{stamp} RX01 已正式启用，独立执行环境 `binance_exec_trade06`，RapidX Binance
现货开方向、退出时永续对冲；五因子、16币、46组合，单份100 USDT。
六个Exec进程：{plist}。
未新增隔离核绑定。Spread publisher PID663465 保持 CPU17；signal PID3132535
使用 CPU0-5,48-95；配置/页面 PID3183433/3390010 使用 CPU0-5,48-95。
旧RX01仅保留账户监控和历史持久化（后者CPU15），没有启动旧执行链。
Manager source06 / virtual03 已建立，lgq 可见并可管理。现货与期货快照均就绪，
12条旧库存腿由独立inventory策略承接；RX02保持启用。下方09:39预部署状态为历史。
验收：`artifacts/cta_spread_rx01_activation/live_validation.json`。

""",
'docs/jp-meta-elvpn_ip_binding.md':f"""{stamp} `binance_exec_trade06` 正式接管RX01现货/永续，使用已有
`local_ips=["172.31.35.228"]`；未新增EIP/ENI或修改路由。
Manager source06 别名 `lgq_cta_RX01`，跟随 `virtual03/cta_spread_rx01` 倍率1。
Viz/Config分别监听loopback10046/18166，通过4191鉴权网关 `/exec_trade06/`
和 `/exec_trade06/config/` 访问；lgq 已获配置管理权限。
Spread 发布已启用、46/46组合实时就绪，旧库存独立承接。下方预部署状态为历史。

""",
'docs/cta_signal_families.md':f"""{stamp} **RX01 Spread 已启用实盘发布并接入 Exec06**。操作者确认沿用
现货开方向、退出时期货对冲；开仓至退出期间允许方向敞口。
现场原单份金额实际为100 USDT，本次保持100；下方200及未启用状态为旧记录。

- RX01/RX02 均按符号修正后的因子分位：q>0.90做多、q<0.70退多，
  q<0.10做空、q>0.30退空；严格不等式，原有过滤保留。RX01价差q30/q70，
  RX02 NQ q50/q50过滤启用。
- RX01五因子16币46组合，发布在线且46/46就绪、journal健康、publication_error为空。
- `binance_exec_trade06` 跟随 `virtual03/cta_spread_rx01` 倍率1；五个
  `binance_cta_spread_rx01_` 因子策略与独立 `inventory` 策略各1份，ctaker执行。
  7币12腿旧库存按启用前账户净现货和期货实际数量继承，并写入账户/库存策略NAV基准。
  启用前分配全部核对一致，待下单与活动订单量为零。
- lgq 对source06有configure权限，对virtual03与六个策略有管理权限；
  `/exec_trade06/`、`/exec_trade06/config/` 已以lgq身份验证。
- Manager费用参数对齐RX02：maker=-0.00002，taker=0.000175，理论TWAP=0.0004；
  这是管理端配置口径，未变更交易所费率。
- 本地arbmm release发布，SHA-256校验和原子安装；`dfb70364` 修复双市场Exec
  风控字段加载，`c484f85d` 按Exec市场查询现货快照，`480bc990` 补RapidX资产
  快照完成标记。Manager另修复非USDT现货规则的误拒绝，现金规则480/永续744。
  风控18项、快照runner6项、trade_engine357项、Manager295项测试通过（3项忽略）。
- 验收及部署证据：`artifacts/cta_spread_rx01_activation/`。远端证据在
  `~/binance_exec_trade06/run/activation_20261013/`；该目录含环境备份，禁止整体导出。

"""
}
for name,block in blocks.items():
 p=root/name;s=p.read_text();lines=s.splitlines(True)
 idx=next(i for i,l in enumerate(lines) if l.startswith('最后更新'))
 if name.endswith('cta_signal_families.md'): lines[idx]='最后更新：'+stamp+'\n'
 else: lines[idx]=re.sub(r'最后更新: .*?UTC', '最后更新: '+stamp,lines[idx],count=1)
 insert=idx+2
 if name.endswith('core_allocation.md'): insert=idx+3
 lines.insert(insert,block)
 s=''.join(lines)
 s=s.replace('RX01 RapidX，运行包已安装，全部进程停止；尚未绑定virtual','RX01 RapidX，现货/永续在线，follow virtual03/cta_spread_rx01，ctaker')
 p.write_text(s)
