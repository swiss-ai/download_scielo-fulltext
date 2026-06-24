#!/usr/bin/env bash
# RCP pod entrypoint for SciELO post-download verification and aggregation.

set -euo pipefail

: "${POSTPROCESS_MODE:?POSTPROCESS_MODE env var required: verify or aggregate}"

CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
RCP_USER="${RCP_USER:-${USER:-}}"
REPO_DIR="${REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"

mkdir -p "${CORPUS_ROOT}/state"

case "${POSTPROCESS_MODE}" in
  verify)
    : "${SHARD_ID:?SHARD_ID env var required for verify mode}"
    if [[ ! "${SHARD_ID}" =~ ^[0-9]+$ ]]; then
      echo "ERROR: SHARD_ID must be digits only, got '${SHARD_ID}'" >&2
      exit 1
    fi
    shard_pad="$(printf '%02d' "${SHARD_ID}")"
    out="${CORPUS_ROOT}/state/verify-shard-${shard_pad}.json"
    tmp="${out}.tmp.$$"
    done_marker="${CORPUS_ROOT}/state/shard-${shard_pad}.verify.done"
    echo "verifying shard ${shard_pad}"
    python3 -u "${REPO_DIR}/scripts/verify_shard.py" \
      --corpus-root "${CORPUS_ROOT}" \
      --shard-id "${SHARD_ID}" > "${tmp}"
    mv "${tmp}" "${out}"
    python3 - <<PY
import json
from pathlib import Path
src = Path("${out}")
dst = Path("${done_marker}")
data = json.loads(src.read_text(encoding="utf-8"))
tmp = dst.with_suffix(dst.suffix + ".tmp")
tmp.write_text(json.dumps(data, sort_keys=True) + "\\n", encoding="utf-8")
tmp.replace(dst)
PY
    ;;
  aggregate)
    out="${CORPUS_ROOT}/manifest.jsonl"
    echo "aggregating manifests into ${out}"
    python3 -u "${REPO_DIR}/scripts/aggregate_manifest.py" \
      --corpus-root "${CORPUS_ROOT}" \
      --output "${out}"
    ;;
  *)
    echo "ERROR: unsupported POSTPROCESS_MODE=${POSTPROCESS_MODE}; expected verify or aggregate" >&2
    exit 1
    ;;
esac
