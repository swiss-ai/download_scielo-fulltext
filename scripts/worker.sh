#!/usr/bin/env bash
# Thin RCP pod entrypoint for one SciELO shard.

set -euo pipefail

: "${SHARD_ID:?SHARD_ID env var required}"
if [[ ! "${SHARD_ID}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: SHARD_ID must be digits only, got '${SHARD_ID}'" >&2
  exit 1
fi

CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
RCP_USER="${RCP_USER:-${USER:-}}"
REPO_DIR="${REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
if [[ "${REPAIR_RETRIES:-0}" == "1" ]]; then
  WORKER_PY="${REPO_DIR}/scripts/repair_retries.py"
else
  WORKER_PY="${REPO_DIR}/scripts/download_worker.py"
fi

if [[ ! -f "${WORKER_PY}" ]]; then
  echo "ERROR: ${WORKER_PY} not found. Stage the repo onto /mloscratch first." >&2
  exit 1
fi

args=(--corpus-root "${CORPUS_ROOT}" --shard-id "${SHARD_ID}")
if [[ -n "${SUBTAR_ID:-}" ]]; then
  args+=(--subtar-id "${SUBTAR_ID}")
fi
if [[ -n "${MAX_SUBTARS:-}" ]]; then
  args+=(--max-subtars "${MAX_SUBTARS}")
fi
if [[ -n "${MIN_FREE_GB:-}" ]]; then
  args+=(--min-free-gb "${MIN_FREE_GB}")
fi

export CORPUS_ROOT REPO_DIR SHARD_ID
exec python3 -u "${WORKER_PY}" "${args[@]}"
