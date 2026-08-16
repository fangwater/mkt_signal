# mkt_signal 主机访问

PEM 文件 gitignore，禁止写入文档或提交。隔离核分配见 `core_allocation.md`。

## 主机

本机构造 SSH 别名（`~/.ssh/config`），不要用旧公网 IP `54.64.147.69`。

| 别名 | 角色 |
| --- | --- |
| `jp-meta-elvpn` | 东京交易机（c7i.metal-24xl，Binance/Gate/Bitget/OKX 行情与 intra/MM） |
| `sg` | 新加坡交易机（c7a.4xlarge，Bybit 行情与 intra） |

```bash
ssh -o BatchMode=yes jp-meta-elvpn hostname
ssh -o BatchMode=yes sg hostname
```

远端部署用 `FR_DEPLOY_HOST` 指向上述别名，不要默认旧 JP 主机。
