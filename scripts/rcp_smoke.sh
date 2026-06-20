#!/usr/bin/env bash
# Pod-side SciELO smoke test. Run inside an RCP Run:ai CPU pod.

set -euo pipefail

RCP_USER="${RCP_USER:-${USER:-}}"
REPO_DIR="${REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext-smoke-$(date +%Y%m%d-%H%M%S)}"
PROXY_FILE="${PROXY_FILE:-/mloscratch/secrets/scielo_proxies.txt}"
SCIELO_CONTACT_EMAIL="${SCIELO_CONTACT_EMAIL:-${CONTACT_EMAIL:-}}"
RPM_PER_PROXY="${RPM_PER_PROXY:-6}"
SAMPLE_FROM="${SAMPLE_FROM:-2025-01-01}"
SAMPLE_UNTIL="${SAMPLE_UNTIL:-2025-03-31}"
SAMPLE_COLLECTION="${SAMPLE_COLLECTION:-}"
IDENTIFIER_LIMIT="${IDENTIFIER_LIMIT:-25}"
ARTICLEMETA_LIMIT="${ARTICLEMETA_LIMIT:-12}"
N_SHARDS="${N_SHARDS:-2}"
ARTICLES_PER_SUBTAR="${ARTICLES_PER_SUBTAR:-6}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-20}"
REQUEST_RETRIES="${REQUEST_RETRIES:-1}"
FIGURE_RETRIES="${FIGURE_RETRIES:-1}"

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

export PROXY_FILE SCIELO_CONTACT_EMAIL RPM_PER_PROXY
mkdir -p "${ROOT}/logs"
cd "${REPO_DIR}"

echo "ROOT=${ROOT}"
echo "stage=harvest_identifiers"
harvest_args=(
  --corpus-root "${ROOT}"
  --from "${SAMPLE_FROM}"
  --until "${SAMPLE_UNTIL}"
  --max-pages 1
  --limit "${IDENTIFIER_LIMIT}"
  --request-timeout "${REQUEST_TIMEOUT}"
  --retries "${REQUEST_RETRIES}"
)
if [[ -n "${SAMPLE_COLLECTION}" ]]; then
  harvest_args+=(--collection "${SAMPLE_COLLECTION}")
fi
python3 scripts/harvest_identifiers.py "${harvest_args[@]}"

echo "stage=fetch_articlemeta"
python3 scripts/fetch_articlemeta.py \
  --corpus-root "${ROOT}" \
  --limit "${ARTICLEMETA_LIMIT}" \
  --request-timeout "${REQUEST_TIMEOUT}" \
  --retries "${REQUEST_RETRIES}"

echo "stage=build_manifest"
python3 scripts/build_manifest.py --corpus-root "${ROOT}"

echo "stage=build_shards"
python3 scripts/build_shards.py \
  --corpus-root "${ROOT}" \
  --n-shards "${N_SHARDS}" \
  --articles-per-subtar "${ARTICLES_PER_SUBTAR}"

SHARD="$(
  python3 - "${ROOT}/index/shards.meta.json" <<'PY'
import json, sys
meta = json.load(open(sys.argv[1]))
for i, count in enumerate(meta["shard_article_counts"]):
    if count:
        print(i)
        break
else:
    raise SystemExit("no nonempty shard")
PY
)"

echo "stage=download_worker shard=${SHARD}"
python3 scripts/download_worker.py \
  --corpus-root "${ROOT}" \
  --shard-id "${SHARD}" \
  --max-subtars 1 \
  --min-free-gb 1 \
  --timeout "${REQUEST_TIMEOUT}" \
  --retries "${REQUEST_RETRIES}" \
  --figure-retries "${FIGURE_RETRIES}" \
  --rpm-per-proxy "${RPM_PER_PROXY}" \
  --log-every 1

echo "stage=verify"
python3 scripts/verify_shard.py --corpus-root "${ROOT}" --shard-id "${SHARD}"

echo "stage=aggregate"
python3 scripts/aggregate_manifest.py --corpus-root "${ROOT}"

echo "stage=estimate"
python3 scripts/estimate_volume.py \
  --manifest "${ROOT}/manifest.jsonl" \
  --total-identifiers 1409144
