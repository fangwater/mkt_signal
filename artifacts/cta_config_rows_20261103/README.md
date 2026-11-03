# CTA 因子四列 JSON 输入修正

用户明确的唯一输入格式为：

```json
[{"标的":"1INCH","nq":"0.50","符号":-1,"因子":"TP_VPI_005"}]
```

标的是单个字符串，nq 小写；JSON 不包含频率。每条规则使用页面当前选择的频段，
按策略组/频段分别保存。读取、格式化、校验与保存均使用该四列格式；分组标的数组、
大写 NQ、行内频率及其他字段均拒绝。显示时去除 USDT，保存仍使用原内部币对契约。
规则顺序、各独立 NQ 条件和 Spread 来源市场保持；没有替换已有因子清单。

发布源 arbmm / `212a531e`，本地干净提交源码构建 Config release，上传并哈希校验、
原子替换后仅通过环境脚本重启配置服务，PID3578063。所有配置文件哈希/revision 保持，
81 个受保护进程身份一致；Prep PID3522829 / start_ticks137144251 不变。

本地真实 Chromium 测试覆盖七份配置、四列显示、错误格式拒绝、按频段隔离保存、
重复规则去重、不同 NQ 保留、并发冲突、隐藏字段与 Spread 顺序/来源保留、390px 无溢出。
Config HTTP 测试通过。线上 browser.mjs 仅校验预览，不保存，核对七份配置，
并严格比较用户示例的前五条 1INCH/AAVE 规则，临时登录会话已删除。

`local-browser-verification.json` 为本地临时目录结果；`browser-verification.json` 为线上页面结果；
`deployment-verification.json` 为运行配置及进程保持校验，`manifest.json` 记录发布哈希。
复用安装工具 `../cta_config_simplify_20261103/deploy.py`，本目录不复制运行二进制入 Git。
配置 URL 保持原值，源 IP、监听端口和执行绑定不变。
