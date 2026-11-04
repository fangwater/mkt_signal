#!/usr/bin/env bash
set +x
set -euo pipefail

# Run interactively as the user that runs delist_risk_server. Credentials stay
# on the terminal and in a private rclone config, never in process arguments.
if [[ ! -t 0 || ! -t 1 ]]; then
  echo "Run this script interactively in a terminal." >&2
  exit 1
fi
if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone is required." >&2
  exit 1
fi

config_dir="${HOME}/.config/rclone"
config_path="${config_dir}/fr-cos.conf"
if [[ -e "$config_path" || -L "$config_path" ]]; then
  echo "Refusing to overwrite existing ${config_path}." >&2
  exit 1
fi
umask 077
mkdir -p "$config_dir"
read -r -s -p 'Tencent Cloud SecretId: ' cos_secret_id
printf '\n'
read -r -s -p 'Tencent Cloud SecretKey: ' cos_secret_key
printf '\n'
if [[ -z "$cos_secret_id" || -z "$cos_secret_key" || "$cos_secret_id" == *[$'\n\r']* || "$cos_secret_key" == *[$'\n\r']* ]]; then
  echo "Credentials must be nonempty single-line values." >&2
  exit 1
fi
temp_file="$(mktemp "${config_dir}/.fr-cos.XXXXXXXX")"
trap 'rm -f -- "$temp_file"' EXIT
{
  printf '%s\n' '[tencent-fr]' 'type = s3' 'provider = TencentCOS' 'env_auth = false'
  printf 'access_key_id = %s\n' "$cos_secret_id"
  printf 'secret_access_key = %s\n' "$cos_secret_key"
  printf '%s\n' 'region = ap-tokyo' 'endpoint = cos.ap-tokyo.myqcloud.com'
} > "$temp_file"
unset cos_secret_id cos_secret_key
chmod 600 "$temp_file"
ln -- "$temp_file" "$config_path"
rm -f -- "$temp_file"
trap - EXIT
echo "Private rclone COS configuration created at ${config_path}."
if timeout 30 rclone --config "$config_path" lsf --max-depth 1 'tencent-fr:jp-fr-config-1307417638' >/dev/null 2>&1; then
  echo "Bucket listing succeeded. Upload and read-back permissions will be checked separately."
else
  echo "Bucket listing failed. Check the credentials, region, bucket permissions and network." >&2
  exit 1
fi
