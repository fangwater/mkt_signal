# dat_pbs / spread_pbs 部署：binance / bitget / gate 远程分流

## 背景

`scripts/deploy_fr_*.sh`、`scripts/deploy_intra_*.sh`、`scripts/deploy_mm_*.sh` 已经统一把
binance / bitget / gate 三个所的 deploy 重定向到 AWS 远端主机
`ubuntu@54.64.147.69:/home/ubuntu/`，okex / bybit 留在本地。`scripts/lib/fr_remote_deploy.sh`
封装了 ssh key、ssh probe、rsync env 目录、nginx 推送 / reload 等流程。

`dat_pbs` 和 `spread_pbs` 现在仍是纯本地部署：

- `scripts/dat_pbs/deploy_dat_pbs.sh`：构建 `dat_pbs`，把二进制 + scripts 铺到 `$HOME/dat_pbs/<venue>/`，
  把 `mkt_cfg.yaml` / `iceoryx2.toml` 同步到 `$HOME/dat_pbs/config/`。
- `scripts/spread_pbs/deploy_spread_pbs.sh`：相同模式，根目录是 `$HOME/spread_pbs/`，10 个 venue
  （5 CEX × {margin, futures}）。

两者都没有 viz/nginx/env.sh，相比 FR/intra/MM 简单一截。spread_pbs 通过 iceoryx2 与 dat_pbs
通信，两者必须在同一台机器，因此 binance/bitget/gate 这 3 个所的 dat_pbs 与 spread_pbs 必须
同时迁移到远端。

## 目标

- 沿用 FR/intra/MM 的 `fr_remote_deploy.sh` 框架，给 `deploy_dat_pbs.sh` /
  `deploy_spread_pbs.sh` 加 "binance/bitget/gate venue 远端 rsync" 的尾段。
- 保留单脚本入口（不拆 per-exchange 子脚本）；同一条命令既能 `--all` 一把铺，也能 `--exchange`
  / `--venue` 按粒度铺，分流逻辑内置。
- 远端目标固定 `ubuntu@54.64.147.69:/home/ubuntu/<root>/<venue>/`（`<root>` ∈ {dat_pbs,
  spread_pbs}），与现有 FR/intra/MM 远端布局风格一致。
- 部署仅做 build + rsync，不启动进程。

非目标：

- 不修改顶层包装器 `scripts/deploy_dat_pbs.sh` / `scripts/deploy_spread_pbs.sh`（仅 exec 内层）。
- 不引入 `--bin` / `--runtime-only` 模式。dat_pbs/spread_pbs 部署量小，全量 rsync 即可。
- 不做远端 nginx；二者无 web 端口。
- 不自动启动远端进程；mirror FR/intra/MM。

## 设计

### 远端分流规则

```
binance-{futures,margin}  -> remote
bitget-{futures,margin}   -> remote
gate-{futures,margin}     -> remote
okex-{futures,margin}     -> local
bybit-{futures,margin}    -> local
aster-{futures,margin}    -> local            # 只 dat_pbs 有 aster
```

判定用正则 `^(binance|bitget|gate)-(futures|margin)$`，与 FR/intra/MM 现有列表保持一致。

### `scripts/lib/fr_remote_deploy.sh` 扩展

新增两个 helper（不改现有函数 / 不改现有调用方）：

```bash
# Lightweight ssh-only init: load FR_DEPLOY_KEY + ssh probe, no nginx staging.
# Used by callers (dat_pbs / spread_pbs) that don't need nginx mapping.
fr_remote_init_ssh() {
  local root_dir="$1"
  if [[ -z "$FR_DEPLOY_KEY" ]]; then
    FR_DEPLOY_KEY="$root_dir/aws-jp-srv-1.pem"
  fi
  if [[ ! -f "$FR_DEPLOY_KEY" ]]; then
    echo "[ERROR] missing ssh key: $FR_DEPLOY_KEY" >&2
    return 1
  fi
  chmod 400 "$FR_DEPLOY_KEY" 2>/dev/null || true
  echo "[INFO] remote target : $FR_DEPLOY_HOST:$FR_REMOTE_HOME"
  echo "[INFO] ssh key       : $FR_DEPLOY_KEY"
  local opts; opts="$(_fr_ssh_opts)"
  # shellcheck disable=SC2086
  if ! ssh $opts "$FR_DEPLOY_HOST" 'echo ok' >/dev/null 2>&1; then
    echo "[ERROR] ssh probe to $FR_DEPLOY_HOST failed" >&2
    return 1
  fi
}

# Rsync $HOME/<rel_path>/ -> $FR_DEPLOY_HOST:$FR_REMOTE_HOME/<rel_path>/.
# Same excludes as fr_remote_sync_env_dir (env.sh / data / *.rocksdb / logs /
# pids / __pycache__).
fr_remote_sync_path() {
  local rel_path="$1"
  local local_dir="$HOME/$rel_path/"
  local remote_dir="$FR_REMOTE_HOME/$rel_path/"
  local opts; opts="$(_fr_ssh_opts)"
  if [[ ! -d "$local_dir" ]]; then
    echo "[ERROR] local dir missing: $local_dir" >&2
    return 1
  fi
  echo "[INFO] rsync $local_dir -> $FR_DEPLOY_HOST:$remote_dir"
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    --exclude='env.sh' \
    --exclude='data/' \
    --exclude='persist/' \
    --exclude='*.rocksdb/' \
    --exclude='logs/' \
    --exclude='*.log' \
    --exclude='*.pid' \
    --exclude='__pycache__/' \
    -e "ssh $opts" \
    "$local_dir" "$FR_DEPLOY_HOST:$remote_dir"
}
```

注意：`fr_remote_init` 内的 `mktemp -t "fr_nginx_locations.${env_name}.XXXXXX"` 与含 `/` 的
env_name 不兼容，因此不能直接复用 `fr_remote_init`；上面的 `fr_remote_init_ssh` 跳过 nginx
staging 文件创建，恰好满足需求。`FR_DEPLOY_HOST` / `FR_REMOTE_HOME` / `FR_DEPLOY_KEY` 默认值
不变。

### `scripts/dat_pbs/deploy_dat_pbs.sh` 改动

在 `# dat_pbs 运行时固定读取 ...` 段落（即同步共享 config 之后、`echo "[INFO] $BIN_NAME 部署完成"`
之前）插入分流段：

```bash
# 远端分流：binance/bitget/gate 的 venue 推到 AWS 远端主机
REMOTE_VENUE_REGEX='^(binance|bitget|gate)-(futures|margin)$'
REMOTE_VENUES=()
LOCAL_VENUES=()
for v in "${VENUES[@]}"; do
  if [[ "$v" =~ $REMOTE_VENUE_REGEX ]]; then
    REMOTE_VENUES+=("$v")
  else
    LOCAL_VENUES+=("$v")
  fi
done

if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  # shellcheck source=../lib/fr_remote_deploy.sh
  source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "$ROOT_DIR"
  for v in "${REMOTE_VENUES[@]}"; do
    fr_remote_sync_path "dat_pbs/$v"
  done
  fr_remote_sync_path "dat_pbs/config"
fi
```

随后把结尾的总结输出从单行 `venues: ${VENUES[*]}` 改为按 local / remote 分组（见"输出文案"小节）。

### `scripts/spread_pbs/deploy_spread_pbs.sh` 改动

与 dat_pbs 对称。在共享 config rsync 完成后、最终 `echo "[INFO] $BIN_NAME 部署完成"` 之前
插入同样的分流段，唯一差异是 `fr_remote_sync_path "spread_pbs/$v"` 与
`fr_remote_sync_path "spread_pbs/config"`。

`spread_pbs` 仅 5 个 CEX × 2 markets = 10 venue（无 aster），分流后 6 远端 + 4 本地。

### 输出文案

两个脚本统一替换尾部输出：

```
[INFO] <bin> 部署完成
[INFO] root_dir: <local_root>            # 例: /home/fanghaizhou/dat_pbs
[INFO] local venues:
  - okex-margin   -> <local_root>/okex-margin/
  - okex-futures  -> <local_root>/okex-futures/
  ...
[INFO] remote venues (ubuntu@54.64.147.69:/home/ubuntu/<root>/):
  - binance-margin
  - binance-futures
  - bitget-margin
  ...
[INFO] config:   <local_root>/config/mkt_cfg.yaml
[INFO] 启动:
  - 本地 venue: cd <local_root>/<venue> && ./scripts/start_<bin>.sh
  - 远端 venue: ssh ubuntu@54.64.147.69 'cd /home/ubuntu/<root>/<venue> && ./scripts/start_<bin>.sh'
```

`local venues` / `remote venues` 中任一为空时，省略对应小节。

### CLI 行为（保持不变）

- `--exchange binance` → `VENUES=(binance-margin binance-futures)`，全部走 remote 分支；触发一次
  共享 config rsync。
- `--exchange okex` → 全部 local；不触发 ssh、不 rsync 共享 config。
- `--venue gate-margin` → 单 venue 远端推送 + 一次共享 config rsync。
- `--all` → 全部 10/12 venue；4 本地 + 6 远端 + 一次共享 config 远端 rsync。
- 失败传播：ssh probe 失败时 `fr_remote_init_ssh` 立即 fail，整脚本 `set -euo pipefail` 即退出。
  rsync 失败同理。

## 测试计划

手工测试（脚本无单测）：

1. `bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange okex`
   - 期望：cargo build + 本地 stage 完成；不触发 `[INFO] remote target`；尾部仅打印 local 段。
2. `bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange binance`
   - 期望：本地 stage + ssh probe + rsync 到 `/home/ubuntu/dat_pbs/binance-futures/` 与
     `binance-margin/` + `/home/ubuntu/dat_pbs/config/`；尾部仅打印 remote 段。
3. `bash scripts/dat_pbs/deploy_dat_pbs.sh --venue gate-margin`
   - 期望：单 venue 远端推送 + 共享 config 推送。
4. `bash scripts/dat_pbs/deploy_dat_pbs.sh --venue okex-margin --venue binance-margin`
   - 期望：1 local + 1 remote + 共享 config 远端推送。
5. `bash scripts/spread_pbs/deploy_spread_pbs.sh --all`
   - 期望：6 远端 venue + 4 本地 venue + 共享 config 远端 rsync；总结输出两段。
6. `FR_DEPLOY_HOST=invalid@127.0.0.1 bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange binance`
   - 期望：ssh probe 失败，立即退出，本地 stage 已完成不影响。

## 取舍 & YAGNI

- 不为 dat_pbs/spread_pbs 单独造一个 `pbs_remote_deploy.sh`。两段分流逻辑短（~25 行），直接内联
  到各自 deploy 脚本里读起来更直观。
- 不做单独的 dry-run 选项。FR/intra/MM 也未做。
- 不在 deploy 阶段触发远端 `start_*.sh`，由 ops 手动在远端执行，与现有规范一致。
- 不做远端旧 venue 目录 cleanup（rsync 增量更新即可，残留 venue 目录不影响新部署运行）。
