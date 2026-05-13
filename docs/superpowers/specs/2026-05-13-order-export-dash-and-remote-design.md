# order_export: dash-form env names + remote binary sync

## 背景

`order_export` 二进制用于把 `persist_manager` 的 RocksDB 按 UTC 日或时间窗口导出成三份 parquet。

当前两个限制让它跑不动现网 intra arb 环境:

1. **Validator 只认下划线**: `src/bin/order_export.rs:262` 的 `validate_supported_env_name` 用 `split_once("_intra_")` 等下划线 token 校验,但现网实际 env 目录都是横线:
   ```
   binance-intra-arb01
   okex-intra-arb01
   gate-intra-arb01
   bybit-intra-arb01
   bitget-intra-arb01
   ```
   在这些目录里 `cd` 后跑 `order_export` 会被校验直接拒掉。
2. **线上 binary 不在远端**: `scripts/deploy_order_export.sh` 只做本地 `cargo install`,但 `deploy_fr_{binance,gate,bitget}.sh` 这三家的 env(包括 persist_manager RocksDB)都跑在远端 `ubuntu@54.64.147.69` 上。远端没有 `order_export` 这个 binary。

## 目标

- Validator 同时接受 `mm` / `fr` / `intra` 三类 token 的下划线和横线两种 separator 写法。
- `cross` token 不变,只接 `_cross_`(下划线模式),因为现网 cross env 目录用下划线,横线模式下 pair 的写法没有干净自然的形式。
- 同一个 env_name 必须**全程同 separator**,禁止 `binance_intra-arb01` 这种穿插。
- `scripts/deploy_order_export.sh` 默认同时落本地 + 推远端 host;失败时不回滚本地。

## 非目标

- 不引入版本号或校验和(rsync 自带 mtime+size 判断)。
- 不动 nginx / pm2 / systemd(`order_export` 是一次性 CLI,无常驻)。
- 不支持多远端 host(目前只有 `54.64.147.69`,真有第二台再扩)。
- 不改 `order_export` 的 CLI / 默认路径解析 / 输出目录命名规则。

## 设计

### 1. Validator (`src/bin/order_export.rs`)

新增 separator 维度。token 表从 `&[&str]` 升级为 `&[(char, [&str; 3])]`,显式承载 separator 字符:

```rust
const TOKEN_GROUPS: &[(char, [&str; 3])] = &[
    ('_', ["_mm_", "_fr_", "_intra_"]),
    ('-', ["-mm-", "-fr-", "-intra-"]),
];
```

`validate_supported_env_name` 的循环:

```rust
for (sep, tokens) in TOKEN_GROUPS {
    for token in tokens {
        if let Some((exchange, suffix)) = env_name.split_once(*token) {
            if is_valid_exchange_name(exchange) && is_valid_env_suffix_for(suffix, *sep) {
                return Ok(());
            }
        }
    }
}
// cross 分支保持原样, 只接 "_cross_" + pair 用 "-"
```

`is_valid_env_suffix_for(suffix, sep)`:
- `sep == '_'`: 接受 `[a-z0-9_-]`(沿用现有 `is_valid_env_suffix` 语义)。
- `sep == '-'`: 收紧到 `[a-z0-9-]`,**不**接受下划线,以防 `okex-intra-arb_01` 这类混合写法被放进来。
- exchange 段(token 左侧)始终是 `[a-z0-9]`,不变。

**这样保证**:
- `binance-intra-arb01` ✓ (横线全程)
- `okex_intra_hf01` ✓ (下划线全程,旧)
- `binance_intra-arb01` ✗ (token 是下划线但 suffix 带横线 → suffix 校验 OK,但 token 不匹配 `_intra_`,会失败)
- `binance-intra_arb01` ✗ (token 是横线但 suffix 带下划线 → `is_valid_env_suffix_for(_, '-')` 拒)

### 2. Deploy 脚本 (`scripts/deploy_order_export.sh`)

#### 流程

```
parse args (--skip-local, --skip-remote, -h)
[--skip-local off]  cargo install --root /home/$USER/order_export
[--skip-remote off] source scripts/lib/fr_remote_deploy.sh
                    fr_remote_init_ssh "$ROOT_DIR"        # 复用 key + 探活
                    OPTS="$(_fr_ssh_opts)"                # 拿到 -i KEY -o ... 串
                    REMOTE_BIN_DIR="$FR_REMOTE_HOME/order_export/bin"
                    ssh $OPTS "$FR_DEPLOY_HOST" "mkdir -p $REMOTE_BIN_DIR"
                    rsync -a -e "ssh $OPTS" \
                        "$INSTALL_ROOT/bin/order_export" \
                        "$FR_DEPLOY_HOST:$REMOTE_BIN_DIR/order_export"
print usage hints for both local and remote
```

#### 复用 helper

`scripts/lib/fr_remote_deploy.sh` 已经提供:
- `fr_remote_init_ssh "$ROOT_DIR"` — 装载 `FR_DEPLOY_KEY` (默认 `aws-jp-srv-1.pem`)、`chmod 400`、ssh 探活。
- `FR_DEPLOY_HOST` / `FR_REMOTE_HOME` — 全局变量,source 后直接读。
- `_fr_ssh_opts()` — 函数,返回 ssh/rsync 用的 `-i KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15` 串。

不复用 `fr_remote_sync_env_dir` / `fr_remote_sync_binaries`(目录粒度的 rsync,这里要的是单文件 `order_export` 传输),自写一行 `rsync -a` 即可。

#### 路径

| 端 | 路径 |
|---|---|
| 本地 | `/home/$USER/order_export/bin/order_export` (现状,不变) |
| 远端 | `$FR_REMOTE_HOME/order_export/bin/order_export` = `/home/ubuntu/order_export/bin/order_export` |

#### Flags

- `--skip-local`: 跳过 `cargo install`(假设之前已构建好)。
- `--skip-remote`: 只装本地,不推远端(纯本地试用或离线时)。
- 两者互斥也无所谓 — 都开就是空跑+打印 usage,人为错。
- `-h` / `--help`: usage。

#### 失败语义

- 本地 `cargo install` 失败 → `set -euo pipefail` 直接 exit,远端那步根本不跑。
- 远端 ssh 探活 / mkdir / rsync 任何一步失败 → exit non-zero,但**本地已落地的 binary 不动**。运维可独立重跑 `--skip-local` 重推。

#### Usage 文案

部署完成后打印两行:

```text
[INFO] local : /home/$USER/order_export/bin/order_export
[INFO] remote: ssh -i $FR_DEPLOY_KEY $FR_DEPLOY_HOST '/home/ubuntu/order_export/bin/order_export --date YYYY-MM-DD'
```

### 3. 不新增单测

按用户偏好,Rust 改动不写新 `#[test]`,也不把 `cargo test` 列为 deploy/CI 必过项。

验证手段:
- `cargo build --release --bin order_export` 通过。
- `cargo clippy --bin order_export -- -D warnings` 无 warning。
- 真实 env 跑一次: `cd ~/okex-intra-arb01 && ~/order_export/bin/order_export --date <date>` 不再被 validator 拒。

`mod tests` 里既有的 `validate_supported_env_name_*` / `export_window_*` / `parse_utc_datetime_*` 等老测试**保留不动**,出问题时再单独跑。

## 风险与回滚

- **风险一**: 远端 ssh key 不存在 / 权限不对。`fr_remote_init_ssh` 已经做了 key 探活 + chmod,会在 deploy 早期 fail。规避:首次部署前先确认 `aws-jp-srv-1.pem` 在 repo 根目录。
- **风险二**: rsync 时远端 `order_export` 正在被某个进程 open。`order_export` 是短时 CLI,不常驻,被替换概率几乎为零。即便发生,Linux 允许在文件被打开时 rsync(产生新 inode),老进程读老文件,新进程读新文件,无破坏。
- **回滚**: 此改动是纯增量,改 validator 是放宽校验(不会让原本能跑的 env 跑不了);改 deploy 是新增远端 step(本地路径完全不变)。回滚直接 `git revert` 即可,不需要清理远端文件(旧 binary 留着也无害)。

## 验收

- `cargo build --release --bin order_export` 通过、`cargo clippy -- -D warnings` 无 warning。
- 在 `~/okex-intra-arb01` 下跑 `~/order_export/bin/order_export --date 2026-05-12`,不再被 validator 拒。
- `bash scripts/deploy_order_export.sh` 本地装好,且远端 `/home/ubuntu/order_export/bin/order_export` 出现(`ls -l` 时间戳是新的)。
- `bash scripts/deploy_order_export.sh --skip-remote` 不发起 ssh,只装本地。
- `bash scripts/deploy_order_export.sh --skip-local` 不动本地 binary,只 rsync 当前已存在的 `/home/$USER/order_export/bin/order_export` 到远端。
