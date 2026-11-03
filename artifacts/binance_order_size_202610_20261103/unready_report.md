# Prep 因子未就绪明细

核验时间：2026-11-03 08:33:52 UTC。Prep01 / Prep02 合并计算后按频段、因子和标的去重；不含停止中的 Spread。

404 项中 331 项就绪、73 项未就绪，涉及 30 个因子名；58 项（79.5%）属于15min。
51币大小单阈值已覆盖，94项NQ全部就绪。仓位推送及交易保持关闭。

| 频段 | 总数 | 就绪 | 未就绪 |
|---|---:|---:|---:|
| 1min | 134 | 132 | 2 |
| 5min | 69 | 56 | 13 |
| 15min | 201 | 143 | 58 |

Kafka目前只保留约两天行情，即约200根15min线。已确认 `baseline_040` 需要360根（90小时），
`baseline_151` 需要373根（93.25小时），随后还需标准化和排名预热。
其他原始值缺失尚不能仅凭快照判断具体根因，不将所有空值都归因于历史长度。

## 15min：原始值为空（28项）

| 因子 | 标的 |
|---|---|
| `TD_TI_030` | DASH |
| `TP_VPI_001` | KNC |
| `TP_VPI_002` | SNX |
| `TP_VPI_005` | 1INCH, SOL |
| `avg_price` | QTUM |
| `baseline_040` | BEL, EGLD, IOST, IOTA, KSM, ONT, RLC, SKL, SUSHI, ZRX |
| `baseline_047` | SNX |
| `baseline_120` | SUSHI |
| `baseline_151` | AXS |
| `factor_032` | IOTA |
| `factor_051` | KAVA, SKL |
| `factor_052` | IOTA |
| `factor_093` | BEL |
| `factor_114` | ENJ |
| `factor_134` | CRV |
| `sell_avg_price` | KSM, SUSHI |

## 15min：标准化值为空（3项）

已有原始值，但尚未输出标准化值；此表不推断更细的根因。

| 因子 | 标的 |
|---|---|
| `baseline_100` | 1INCH, CRV, RUNE |

## 15min：有效样本不足（27项）

这些项已有原始值、标准化值和分位数，但未满足排名有效样本门槛。

| 因子 | 标的 |
|---|---|
| `TD_TI_008` | ENJ |
| `TD_TI_015` | IOTA |
| `baseline_035` | FIL |
| `baseline_104` | AXS, BAT, BNB, EGLD, ETC, IOTA, KNC, KSM, LINK, QTUM, SOL, VET, YFI, ZEN |
| `baseline_141` | DASH |
| `baseline_189` | SOL |
| `factor_048` | ONT, RSR |
| `factor_094` | ZIL |
| `factor_170` | ATOM, IOST, KAVA, SKL, YFI |

## 5min：有效样本不足（13项）

这些项已有原始值、标准化值和分位数，但未满足排名有效样本门槛。

| 因子 | 标的 |
|---|---|
| `TD_TI_008` | DOGE |
| `TD_TI_033` | BEL, SKL |
| `TP_VPI_001` | VET |
| `TP_VPI_006` | SKL |
| `avg_price` | YFI |
| `baseline_040` | BAND, SKL |
| `baseline_118` | DASH |
| `baseline_120` | SKL |
| `factor_032` | SOL |
| `factor_051` | NEAR |
| `sell_avg_price` | BEL |

## 1min：有效样本不足（2项）

这些项已有原始值、标准化值和分位数，但未满足排名有效样本门槛。

| 因子 | 标的 |
|---|---|
| `buy_avg_price` | ALGO, ZRX |

明细数据：`unready_details.csv` 与 `unready_details.json`。
代码依据：`crates/factor_engine/src/baseline.rs` 的 `compute_baseline_040` / `compute_baseline_151`，
`crates/factor_engine/src/intermediates.rs` 的 `compute_tp_vpi_006`。
