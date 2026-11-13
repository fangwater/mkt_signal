# mkt_signal 主机访问

最后更新：2026-11-13

PEM 文件 gitignore，禁止写入文档或提交。隔离核见 `core_allocation.md`。JP 交易机 source IP 见 `jp-meta-elvpn_ip_binding.md`。部署时若改绑核或 `local_ips`，两份都要更新。

## 主机

本机构造 SSH 别名（`~/.ssh/config`），不要用旧公网 IP `54.64.147.69`。

| 别名 | 角色 |
| --- | --- |
| `jp-meta-elvpn` | 东京交易机（c7i.metal-24xl，Binance/Gate/Bitget/OKX 行情与 intra/MM） |
| `sg` | 新加坡交易机（c7a.4xlarge，Bybit 行情与 intra） |
| `sh_dev` | 国内期货部署／验证主机；已部署易达 paper＋公网 Viz，CTP 尚未部署，见 [运行说明](sh_dev_yida_paper.md) |

```bash
ssh -o BatchMode=yes jp-meta-elvpn hostname
ssh -o BatchMode=yes sg hostname
```

FR 远端部署的 `FR_DEPLOY_HOST` 使用 `jp-meta-elvpn` 或 `sg`，不要默认旧 JP 主机。

CTP 使用 `ssh sh_dev`，不使用 `FR_DEPLOY_HOST` 路由。该主机已核对为 Linux x86_64、glibc 2.39，允许 CPU 0–3、NUMA 节点 0；实际 CTP 部署前还须核对现有易达环境的资源占用及 [固定布局要求](ctp_layout.md)。二进制在本地 `arbmm` 构建并校验后上传，原始 SSH 地址及凭据不入库。当前常驻服务为 `/home/ubuntu/yida_paper01` 的只读易达 paper 和公网 8811 端口 Viz。
