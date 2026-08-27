#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="delist_risk_server"
BIN_PATH="${ROOT_DIR}/target/release/${BIN_NAME}"
SSH_HOST="${DELIST_DEPLOY_HOST:-jp-meta-elvpn}"
LOCAL_TARGET=""
DO_BUILD=1
DO_SCRIPTS=1
APPLY_NGINX=1
SKIP_START=0
SKIP_PG=0

usage() {
  cat <<'USAGE'
Usage:
  deploy_delist_risk_server.sh [--host <ssh>] [--target-dir <path>]
                               [--scripts-only|--bin-only]
                               [--no-nginx] [--no-start] [--skip-pg]
                               [--local]

Defaults:
  host: jp-meta-elvpn
  remote directory: $HOME/delist_risk_server

The deploy preserves an existing config/delist_risk_server.env file.
Postgres DB/role are created with sudo -u postgres when ubuntu cannot createdb.
Nginx only upserts /delist/ into the existing 4191 server; it never rewrites
crypto_proxy_4191.conf from nginx_locations.txt (that dropped /manager/).
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      SSH_HOST="${2:-}"
      if [[ -z "$SSH_HOST" ]]; then
        echo "[ERROR] --host requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --target-dir)
      LOCAL_TARGET="${2:-}"
      if [[ -z "$LOCAL_TARGET" ]]; then
        echo "[ERROR] --target-dir requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --scripts-only)
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    --no-nginx)
      APPLY_NGINX=0
      shift
      ;;
    --no-start)
      SKIP_START=1
      shift
      ;;
    --skip-pg)
      SKIP_PG=1
      shift
      ;;
    --local)
      SSH_HOST=""
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] building ${BIN_NAME} (release)"
  cargo build --release --bin "$BIN_NAME"
  if [[ ! -x "$BIN_PATH" ]]; then
    echo "[ERROR] missing binary: $BIN_PATH" >&2
    exit 1
  fi
fi

install_tree() {
  local dest="$1"
  mkdir -p "$dest/scripts" "$dest/config" "$dest/data" "$dest/docs" "$dest/web/delist_risk"
  if [[ "$DO_BUILD" -eq 1 ]]; then
    local tmp="${dest}/.${BIN_NAME}.new"
    cp "$BIN_PATH" "$tmp"
    chmod +x "$tmp"
    mv -f "$tmp" "$dest/$BIN_NAME"
  fi
  if [[ "$DO_SCRIPTS" -eq 1 ]]; then
    for script in start_delist_risk_server.sh stop_delist_risk_server.sh deploy_delist_risk_server.sh; do
      if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
        cp "$ROOT_DIR/scripts/$script" "$dest/scripts/$script"
        chmod +x "$dest/scripts/$script"
      fi
    done
    if [[ -f "$ROOT_DIR/docs/delist_risk_server.md" ]]; then
      cp "$ROOT_DIR/docs/delist_risk_server.md" "$dest/docs/delist_risk_server.md"
    fi
    if [[ -f "$ROOT_DIR/web/delist_risk/index.html" ]]; then
      cp "$ROOT_DIR/web/delist_risk/index.html" "$dest/web/delist_risk/index.html"
    fi
    if [[ ! -f "$dest/config/delist_risk_server.env" ]]; then
      cp "$ROOT_DIR/config/delist_risk_server.env.example" "$dest/config/delist_risk_server.env"
      chmod 600 "$dest/config/delist_risk_server.env"
      echo "[WARN] created credential config: $dest/config/delist_risk_server.env"
    else
      echo "[INFO] preserving existing credential config"
    fi
  fi
}

ensure_pg_local() {
  local dest="$1"
  local env_file="$dest/config/delist_risk_server.env"
  if [[ "$SKIP_PG" -eq 1 ]]; then
    return 0
  fi
  if ! command -v psql >/dev/null 2>&1 && ! sudo -n -u postgres command -v psql >/dev/null 2>&1; then
    echo "[WARN] psql not found; skip postgres bootstrap" >&2
    return 0
  fi
  local password=""
  if [[ -f "$env_file" ]]; then
    password="$(awk -F= '/^DELIST_PG_URL=/{print $2}' "$env_file" | sed -n 's#.*://[^:]*:\([^@]*\)@.*#\1#p' | tail -n1 || true)"
  fi
  if [[ -z "$password" ]]; then
    password="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(18))
PY
)"
  fi
  sudo -n -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'delist_risk') THEN
    CREATE ROLE delist_risk LOGIN PASSWORD '${password}';
  ELSE
    ALTER ROLE delist_risk WITH LOGIN PASSWORD '${password}';
  END IF;
END
\$\$;
SELECT 'CREATE DATABASE delist_risk OWNER delist_risk'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'delist_risk')\\gexec
GRANT ALL PRIVILEGES ON DATABASE delist_risk TO delist_risk;
SQL
  if [[ -f "$env_file" ]]; then
    if grep -q '^DELIST_PG_URL=' "$env_file"; then
      sed -i "s|^DELIST_PG_URL=.*|DELIST_PG_URL=postgres://delist_risk:${password}@127.0.0.1:5432/delist_risk|" "$env_file"
    else
      echo "DELIST_PG_URL=postgres://delist_risk:${password}@127.0.0.1:5432/delist_risk" >> "$env_file"
    fi
    chmod 600 "$env_file"
  fi
  echo "[INFO] postgres database delist_risk ready"
}

upsert_nginx_mapping() {
  local mapping="$1"
  local begin="# BEGIN managed: delist_risk_server"
  local end="# END managed: delist_risk_server"
  mkdir -p "$(dirname "$mapping")"
  touch "$mapping"
  local tmp
  tmp="$(mktemp)"
  awk -v begin="$begin" -v end="$end" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
      in_block = 0
      print begin
      print "# delist risk public HTTP"
      print "/delist/ http://127.0.0.1:8787/"
      print end
      next
    }
    in_block { next }
    { print }
    END {
      if (!replaced) {
        print ""
        print begin
        print "# delist risk public HTTP"
        print "/delist/ http://127.0.0.1:8787/"
        print end
      }
    }
  ' "$mapping" > "$tmp"
  mv "$tmp" "$mapping"
  echo "[INFO] nginx mapping updated: $mapping"
}

# Patch the live 4191 server in place. Never rewrite crypto_proxy_4191.conf from
# nginx_locations.txt — that dropped /manager/ and other hand-maintained routes.
upsert_delist_nginx_conf() {
  local conf="${NGINX_CONF_PATH:-/etc/nginx/sites-available/crypto_proxy_4191.conf}"
  if [[ ! -f "$conf" ]]; then
    echo "[ERROR] missing nginx conf: $conf" >&2
    echo "[ERROR] refusing to create a mapping-only 4191 server" >&2
    return 1
  fi
  if grep -Eq 'location[[:space:]]+/delist/' "$conf"; then
    echo "[INFO] nginx already has /delist/: $conf"
    return 0
  fi
  local sudo_cmd=()
  if [[ "$(id -u)" -ne 0 ]]; then
    sudo_cmd=(sudo -n)
  fi
  local tmp
  tmp="$(mktemp)"
  python3 - "$conf" "$tmp" <<'PY'
import sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
text = src.read_text()
block = """    location /delist/ {
        proxy_pass http://127.0.0.1:8787/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
"""
idx = text.rfind("\n}")
if idx < 0:
    raise SystemExit(f"cannot find closing server brace in {src}")
dst.write_text(text[:idx] + "\n" + block + text[idx:])
PY
  "${sudo_cmd[@]}" cp "$conf" "${conf}.bak.delist.$(date -u +%Y%m%dT%H%M%SZ)"
  "${sudo_cmd[@]}" cp "$tmp" "$conf"
  rm -f "$tmp"
  "${sudo_cmd[@]}" nginx -t
  if command -v systemctl >/dev/null 2>&1; then
    "${sudo_cmd[@]}" systemctl reload nginx
  else
    "${sudo_cmd[@]}" nginx -s reload
  fi
  echo "[INFO] patched /delist/ into $conf and reloaded nginx"
}

upsert_nginx_local() {
  local mapping="${HOME}/nginx_locations.txt"
  if [[ ! -f "$mapping" && -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
    cp "${ROOT_DIR}/config/nginx_locations.txt" "$mapping"
  fi
  upsert_nginx_mapping "$mapping"
  if [[ "$APPLY_NGINX" -eq 1 ]]; then
    upsert_delist_nginx_conf
  fi
}

if [[ -z "$SSH_HOST" ]]; then
  TARGET_DIR="${LOCAL_TARGET:-$HOME/delist_risk_server}"
  echo "[INFO] local deploy -> $TARGET_DIR"
  install_tree "$TARGET_DIR"
  ensure_pg_local "$TARGET_DIR"
  upsert_nginx_local
  if [[ "$SKIP_START" -eq 0 ]]; then
    (cd "$TARGET_DIR" && ./scripts/start_delist_risk_server.sh)
  fi
  echo "[INFO] delist_risk_server deployed to $TARGET_DIR"
  exit 0
fi

SSH=(ssh -o BatchMode=yes)
SCP=(scp -o BatchMode=yes)
echo "[INFO] remote deploy host=${SSH_HOST} dir=~/delist_risk_server"

STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT
install_tree "$STAGING"

"${SSH[@]}" "$SSH_HOST" "mkdir -p ~/delist_risk_server/scripts ~/delist_risk_server/config ~/delist_risk_server/data ~/delist_risk_server/docs ~/delist_risk_server/web/delist_risk"
if [[ "$DO_BUILD" -eq 1 ]]; then
  "${SCP[@]}" "$STAGING/$BIN_NAME" "$SSH_HOST:~/delist_risk_server/.${BIN_NAME}.new"
  "${SSH[@]}" "$SSH_HOST" "chmod +x ~/delist_risk_server/.${BIN_NAME}.new && mv -f ~/delist_risk_server/.${BIN_NAME}.new ~/delist_risk_server/${BIN_NAME}"
fi
if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  "${SCP[@]}" "$STAGING/scripts/"*.sh "$SSH_HOST:~/delist_risk_server/scripts/"
  "${SSH[@]}" "$SSH_HOST" "chmod +x ~/delist_risk_server/scripts/*.sh"
  if [[ -f "$STAGING/docs/delist_risk_server.md" ]]; then
    "${SCP[@]}" "$STAGING/docs/delist_risk_server.md" "$SSH_HOST:~/delist_risk_server/docs/delist_risk_server.md"
  fi
  if [[ -f "$STAGING/web/delist_risk/index.html" ]]; then
    "${SCP[@]}" "$STAGING/web/delist_risk/index.html" "$SSH_HOST:~/delist_risk_server/web/delist_risk/index.html"
  fi
  if ! "${SSH[@]}" "$SSH_HOST" "test -f ~/delist_risk_server/config/delist_risk_server.env"; then
    "${SCP[@]}" "$ROOT_DIR/config/delist_risk_server.env.example" "$SSH_HOST:~/delist_risk_server/config/delist_risk_server.env"
    "${SSH[@]}" "$SSH_HOST" "chmod 600 ~/delist_risk_server/config/delist_risk_server.env"
    echo "[WARN] created remote credential config"
  else
    echo "[INFO] preserving remote credential config"
  fi
fi

if [[ "$SKIP_PG" -eq 0 ]]; then
  "${SSH[@]}" "$SSH_HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
ENV_FILE="$HOME/delist_risk_server/config/delist_risk_server.env"
password=""
if [[ -f "$ENV_FILE" ]]; then
  password="$(awk -F= "/^DELIST_PG_URL=/{print \$2}" "$ENV_FILE" | sed -n "s#.*://[^:]*:\\([^@]*\\)@.*#\\1#p" | tail -n1 || true)"
fi
if [[ -z "$password" ]]; then
  password="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(18))
PY
)"
fi
sudo -n -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'delist_risk') THEN
    CREATE ROLE delist_risk LOGIN PASSWORD '${password}';
  ELSE
    ALTER ROLE delist_risk WITH LOGIN PASSWORD '${password}';
  END IF;
END
\$\$;
SELECT 'CREATE DATABASE delist_risk OWNER delist_risk'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'delist_risk')\\gexec
GRANT ALL PRIVILEGES ON DATABASE delist_risk TO delist_risk;
SQL
if grep -q '^DELIST_PG_URL=' "$ENV_FILE"; then
  sed -i "s|^DELIST_PG_URL=.*|DELIST_PG_URL=postgres://delist_risk:${password}@127.0.0.1:5432/delist_risk|" "$ENV_FILE"
else
  echo "DELIST_PG_URL=postgres://delist_risk:${password}@127.0.0.1:5432/delist_risk" >> "$ENV_FILE"
fi
chmod 600 "$ENV_FILE"
echo "[INFO] postgres database delist_risk ready"
REMOTE
fi

if [[ "$APPLY_NGINX" -eq 1 ]]; then
  "${SSH[@]}" "$SSH_HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
mapping="$HOME/nginx_locations.txt"
begin="# BEGIN managed: delist_risk_server"
end="# END managed: delist_risk_server"
touch "$mapping"
tmp="$(mktemp)"
awk -v begin="$begin" -v end="$end" '
  BEGIN { in_block = 0; replaced = 0 }
  $0 == begin { in_block = 1; replaced = 1; next }
  in_block && $0 == end {
    in_block = 0
    print begin
    print "# delist risk public HTTP"
    print "/delist/ http://127.0.0.1:8787/"
    print end
    next
  }
  in_block { next }
  { print }
  END {
    if (!replaced) {
      print ""
      print begin
      print "# delist risk public HTTP"
      print "/delist/ http://127.0.0.1:8787/"
      print end
    }
  }
' "$mapping" > "$tmp"
mv "$tmp" "$mapping"
echo "[INFO] nginx mapping updated: $mapping"

conf="/etc/nginx/sites-available/crypto_proxy_4191.conf"
if [[ ! -f "$conf" ]]; then
  echo "[ERROR] missing nginx conf: $conf; refusing mapping-only rewrite" >&2
  exit 1
fi
if grep -Eq 'location[[:space:]]+/delist/' "$conf"; then
  echo "[INFO] nginx already has /delist/: $conf"
  exit 0
fi
patch_tmp="$(mktemp)"
python3 - "$conf" "$patch_tmp" <<'PY'
import sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
text = src.read_text()
block = """    location /delist/ {
        proxy_pass http://127.0.0.1:8787/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
"""
idx = text.rfind("\n}")
if idx < 0:
    raise SystemExit(f"cannot find closing server brace in {src}")
dst.write_text(text[:idx] + "\n" + block + text[idx:])
PY
sudo -n cp "$conf" "${conf}.bak.delist.$(date -u +%Y%m%dT%H%M%SZ)"
sudo -n cp "$patch_tmp" "$conf"
rm -f "$patch_tmp"
sudo -n nginx -t
sudo -n systemctl reload nginx
echo "[INFO] patched /delist/ into $conf and reloaded nginx"
REMOTE
fi

if [[ "$SKIP_START" -eq 0 ]]; then
  "${SSH[@]}" "$SSH_HOST" "cd ~/delist_risk_server && ./scripts/start_delist_risk_server.sh"
fi

echo "[INFO] delist_risk_server deployed on ${SSH_HOST}:~/delist_risk_server"
echo "[INFO] public: http://<jp-host>:4191/delist/healthz"
echo "[INFO] local:  ssh ${SSH_HOST} 'curl -sS http://127.0.0.1:8787/status'"
