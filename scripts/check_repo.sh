#!/usr/bin/env bash
# Fast repository sanity checks suitable for CI and pre-push use.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export ROOT

cleanup() {
  if [[ -n "${tmp_proxy_file:-}" ]]; then
    rm -f "${tmp_proxy_file}"
  fi
  while IFS= read -r cache_dir; do
    rm -rf "${cache_dir}"
  done < <(find "${ROOT}/scripts" -type d -name __pycache__ -print)
}
trap cleanup EXIT

python3 -m py_compile "${ROOT}"/scripts/*.py

for file in "${ROOT}"/scripts/*.sh "${ROOT}"/docker/*.sh; do
  bash -n "${file}"
done

tmp_proxy_file="$(mktemp)"
printf '127.0.0.1:8080\n127.0.0.2:8080\n' > "${tmp_proxy_file}"
PROXY_FILE="${tmp_proxy_file}" \
PROXY_PARTITION_INDEX=1 \
PROXY_PARTITION_COUNT=2 \
python3 - <<'PY'
import os
import sys
sys.path.insert(0, os.path.join(os.environ["ROOT"], "scripts"))
from common import require_proxies

proxies = require_proxies()
assert proxies == ["http://127.0.0.2:8080/"], proxies
PY

if git -C "${ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if git -C "${ROOT}" ls-files | grep -E '(^|/)(__pycache__|.*\.pyc|.*\.tar|manifest\.(jsonl|parquet))$'; then
    echo "ERROR: generated corpus/build artifacts are tracked" >&2
    exit 1
  fi
fi
