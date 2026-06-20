#!/usr/bin/env bash
# Pod-side full SciELO ArticleMeta identifier harvest.

set -euo pipefail

RCP_USER="${RCP_USER:-${USER:-}}"
REPO_DIR="${REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
PROXY_FILE="${PROXY_FILE:-/mloscratch/secrets/scielo_proxies.txt}"
SCIELO_CONTACT_EMAIL="${SCIELO_CONTACT_EMAIL:-${CONTACT_EMAIL:-}}"
RPM_PER_PROXY="${RPM_PER_PROXY:-10}"
DATE_FROM="${DATE_FROM:-1900-01-01}"
DATE_UNTIL="${DATE_UNTIL:-2026-06-19}"
IDENTIFIER_LIMIT="${IDENTIFIER_LIMIT:-1000}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-90}"
REQUEST_RETRIES="${REQUEST_RETRIES:-4}"

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
mkdir -p "${CORPUS_ROOT}/logs"
cd "${REPO_DIR}"

echo "CORPUS_ROOT=${CORPUS_ROOT}"
echo "DATE_FROM=${DATE_FROM}"
echo "DATE_UNTIL=${DATE_UNTIL}"
echo "stage=harvest_identifiers"
python3 -u scripts/harvest_identifiers.py \
  --corpus-root "${CORPUS_ROOT}" \
  --from "${DATE_FROM}" \
  --until "${DATE_UNTIL}" \
  --limit "${IDENTIFIER_LIMIT}" \
  --request-timeout "${REQUEST_TIMEOUT}" \
  --retries "${REQUEST_RETRIES}"
