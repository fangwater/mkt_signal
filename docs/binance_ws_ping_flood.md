# Binance WS ping 洪泛阈值实测

测试时间：2026-07-06 · 机器：binance-intra-arb01（ap-northeast-1，colo 同 AZ）
工具：一次性 `binance_ws_ping_flood_test`（仅 session.logon + 协议级 ping，不下单；测完已删）

## 背景

合约（UM taker）下单连接是**稀疏**的（只有对冲那一下才发单），平时空闲，
路径丢包无法从真实流量里持续测出。曾考虑用**高频 WS ping 帧**当"廉价丢包
探针"（不占订单 rate-limit），本测试用于验证 Binance 能接受的 ping 频率上限。

## 方法

单独起一条 Binance WS 连接 → Ed25519 `session.logon` → 按固定间隔 `PING_MS`
发协议级 Ping 帧 → 监听 Close/错误，记录存活时长与断开原因。

```bash
source /home/ubuntu/binance-intra-arb01/env.sh          # 载入 ED25519 凭据
# 合约（默认端点）
PING_MS=250 RUN_SECS=90 ./target/release/binance_ws_ping_flood_test
# 现货
WS_URL="wss://ws-api.binance.com:443/ws-api/v3" \
  PING_MS=250 RUN_SECS=40 ./target/release/binance_ws_ping_flood_test
```

## 结果

| 频率 | 间隔 | 合约 ws-fapi | 现货 ws-api |
|---|---|---|---|
| 2/s  | 500ms | ✅ 存活（180 帧/90s） | ✅ 存活（60 帧/30s） |
| 4/s  | 250ms | ✅ 存活（359 帧/90s） | ✅ 存活（160 帧/40s） |
| 5/s  | 200ms | ❌ 第 6 帧 ~1.0s 断 | ❌ 第 8 帧 ~1.4s 断 |
| 6.7/s| 150ms | ❌ 第 6 帧 ~0.8s 断 | —（未测，同类） |
| 10/s | 100ms | ❌ 第 6 帧 ~0.5s 断 | ❌ 第 6 帧 ~0.5s 断 |

断开原因统一：`CLOSED code=Policy reason='too many ping/pong frames'`。
同一个 Ed25519 API key 现货 logon 也成功（status=200）。

## 结论

- **Binance WS（现货+合约）ping 上限统一 ≈ 4/s**，阈值在 4/s ~ 5/s 之间。
- 行为像**令牌桶**：容量约 5–8 帧、补充速率约 4/s。所以超阈值时都是放行头几帧
  后、约第 6–8 帧触发 Policy close；4/s 正好压在补充速率下，永不触发。
- **别贴着 4/s 跑**（正好等于补充速率，抖动/突发即可能瞬时超限被踢）；
  留余量建议 **3/s（333ms）**。

## 对"合约腿丢包探针"的影响

- ❌ **高频 WS ping 探针不可行**——两条线都在 5/s 秒断。
- 低频（≤4/s ≈ 180–240 帧/min）作 retrans 探针：几十秒内能识别 **>5% 的明显
  坏路径**，但**分辨不了 0.5% vs 1%**（样本太少）。
- cancel probe（order.cancel 假单）能高频但占订单 rate-limit，且用 RTT 测丢包
  无效（RTT 对稀疏丢包是盲的），已弃用删除（见 commit `删除 Binance UM cancel probe`）。

## 收敛方向

对必须立即成交、又不能双发的 taker 腿，路径健康的务实做法是
**make-before-break**：在**真实订单**的 `TCP_INFO`（`tcpi_total_retrans`）上做
文章——某条连接 retrans 率超阈值就后台建新连接（新源端口 → 新 ECMP 路径）
logon 成功后再切走，全程零中断，不额外探测、不占 rate-limit。
