#!/usr/bin/env bash
# Pod-side SciELO ArticleMeta shard fetch. Run inside an RCP Run:ai CPU pod.

set -euo pipefail

RCP_USER="${RCP_USER:-${USER:-}}"
REPO_DIR="${REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
PROXY_FILE="${PROXY_FILE:-/mloscratch/secrets/scielo_proxies.txt}"
SCIELO_CONTACT_EMAIL="${SCIELO_CONTACT_EMAIL:-${CONTACT_EMAIL:-}}"
RPM_PER_PROXY="${RPM_PER_PROXY:-1}"
N_SHARDS="${N_SHARDS:-8}"
SHARD_ID="${SHARD_ID:-}"
ARTICLEMETA_SUB_SHARDS="${ARTICLEMETA_SUB_SHARDS:-1}"
ARTICLEMETA_SUB_SHARD_ID="${ARTICLEMETA_SUB_SHARD_ID:-0}"
DATE_FROM="${DATE_FROM:-1900-01-01}"
DATE_UNTIL="${DATE_UNTIL:-2026-06-19}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-90}"
REQUEST_RETRIES="${REQUEST_RETRIES:-4}"
ARTICLEMETA_WORKERS="${ARTICLEMETA_WORKERS:-4}"
ARTICLEMETA_PREFETCH_PER_WORKER="${ARTICLEMETA_PREFETCH_PER_WORKER:-4}"
ARTICLEMETA_LIMIT="${ARTICLEMETA_LIMIT:-}"
CHECK_IDENTIFIER_COMPLETE="${CHECK_IDENTIFIER_COMPLETE:-1}"
WAIT_FOR_IDENTIFIERS="${WAIT_FOR_IDENTIFIERS:-0}"
WAIT_SLEEP_SECONDS="${WAIT_SLEEP_SECONDS:-300}"

if [[ -z "${SCIELO_CONTACT_EMAIL}" && -z "${SCIELO_USER_AGENT:-}" ]]; then
  echo "ERROR: set SCIELO_CONTACT_EMAIL or SCIELO_USER_AGENT" >&2
  exit 1
fi
if [[ ! -r "${PROXY_FILE}" ]]; then
  echo "ERROR: proxy file not readable at ${PROXY_FILE}" >&2
  exit 1
fi
if [[ ! -d "${REPO_DIR}" ]]; then
  echo "ERROR: repo not found at ${REPO_DIR}" >&2
  exit 1
fi
if [[ -z "${SHARD_ID}" ]]; then
  echo "ERROR: SHARD_ID is required" >&2
  exit 1
fi
if [[ ! "${N_SHARDS}" =~ ^[0-9]+$ || "${N_SHARDS}" -lt 1 ]]; then
  echo "ERROR: N_SHARDS must be a positive integer" >&2
  exit 1
fi
if [[ ! "${SHARD_ID}" =~ ^[0-9]+$ || "${SHARD_ID}" -ge "${N_SHARDS}" ]]; then
  echo "ERROR: SHARD_ID must satisfy 0 <= SHARD_ID < N_SHARDS" >&2
  exit 1
fi
if [[ ! "${ARTICLEMETA_SUB_SHARDS}" =~ ^[0-9]+$ || "${ARTICLEMETA_SUB_SHARDS}" -lt 1 ]]; then
  echo "ERROR: ARTICLEMETA_SUB_SHARDS must be a positive integer" >&2
  exit 1
fi
if [[ ! "${ARTICLEMETA_SUB_SHARD_ID}" =~ ^[0-9]+$ || "${ARTICLEMETA_SUB_SHARD_ID}" -ge "${ARTICLEMETA_SUB_SHARDS}" ]]; then
  echo "ERROR: ARTICLEMETA_SUB_SHARD_ID must satisfy 0 <= ARTICLEMETA_SUB_SHARD_ID < ARTICLEMETA_SUB_SHARDS" >&2
  exit 1
fi
if [[ ! "${RPM_PER_PROXY}" =~ ^[0-9]+$ || "${RPM_PER_PROXY}" -lt 1 ]]; then
  echo "ERROR: RPM_PER_PROXY must be a positive integer" >&2
  exit 1
fi
if [[ ! "${ARTICLEMETA_WORKERS}" =~ ^[0-9]+$ || "${ARTICLEMETA_WORKERS}" -lt 1 ]]; then
  echo "ERROR: ARTICLEMETA_WORKERS must be a positive integer" >&2
  exit 1
fi
if [[ ! "${ARTICLEMETA_PREFETCH_PER_WORKER}" =~ ^[0-9]+$ || "${ARTICLEMETA_PREFETCH_PER_WORKER}" -lt 1 ]]; then
  echo "ERROR: ARTICLEMETA_PREFETCH_PER_WORKER must be a positive integer" >&2
  exit 1
fi

export PROXY_FILE SCIELO_CONTACT_EMAIL RPM_PER_PROXY
mkdir -p "${CORPUS_ROOT}/logs"
cd "${REPO_DIR}"

identifier_state="${CORPUS_ROOT}/state/harvest_identifiers_window-${DATE_FROM}_${DATE_UNTIL}.json"

check_identifier_state() {
  python3 - "${identifier_state}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print(f"missing:{path}")
    raise SystemExit(2)
try:
    state = json.loads(path.read_text(encoding="utf-8"))
except json.JSONDecodeError:
    print(f"invalid:{path}")
    raise SystemExit(3)
if state.get("complete") is True:
    print(f"complete:next_offset={state.get('next_offset')} total={state.get('reported_total')}")
    raise SystemExit(0)
print(f"incomplete:next_offset={state.get('next_offset')} total={state.get('reported_total')}")
raise SystemExit(1)
PY
}

if [[ "${CHECK_IDENTIFIER_COMPLETE}" == "1" ]]; then
  until check_identifier_state; do
    status=$?
    if [[ "${WAIT_FOR_IDENTIFIERS}" != "1" ]]; then
      echo "ERROR: identifier harvest is not complete; refusing ArticleMeta fetch" >&2
      exit "${status}"
    fi
    echo "identifier harvest not complete; sleeping ${WAIT_SLEEP_SECONDS}s"
    sleep "${WAIT_SLEEP_SECONDS}"
  done
fi

args=(
  --corpus-root "${CORPUS_ROOT}"
  --n-shards "${N_SHARDS}"
  --shard-id "${SHARD_ID}"
  --sub-shards "${ARTICLEMETA_SUB_SHARDS}"
  --sub-shard-id "${ARTICLEMETA_SUB_SHARD_ID}"
  --request-timeout "${REQUEST_TIMEOUT}"
  --retries "${REQUEST_RETRIES}"
  --workers "${ARTICLEMETA_WORKERS}"
  --prefetch-per-worker "${ARTICLEMETA_PREFETCH_PER_WORKER}"
)
if [[ -n "${ARTICLEMETA_LIMIT}" ]]; then
  args+=(--limit "${ARTICLEMETA_LIMIT}")
fi

echo "CORPUS_ROOT=${CORPUS_ROOT}"
echo "N_SHARDS=${N_SHARDS}"
echo "SHARD_ID=${SHARD_ID}"
echo "ARTICLEMETA_SUB_SHARDS=${ARTICLEMETA_SUB_SHARDS}"
echo "ARTICLEMETA_SUB_SHARD_ID=${ARTICLEMETA_SUB_SHARD_ID}"
echo "RPM_PER_PROXY=${RPM_PER_PROXY}"
echo "ARTICLEMETA_WORKERS=${ARTICLEMETA_WORKERS}"
echo "ARTICLEMETA_PREFETCH_PER_WORKER=${ARTICLEMETA_PREFETCH_PER_WORKER}"
echo "stage=fetch_articlemeta"
python3 -u scripts/fetch_articlemeta.py "${args[@]}"
