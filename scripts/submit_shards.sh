#!/usr/bin/env bash
# Submit guarded one-pod-per-shard RCP jobs for SciELO XML+figure packing.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EPFML_DIR="${EPFML_DIR:-${HOME}/dev/epfml-getting-started}"
LOCAL_META="${LOCAL_META:-${REPO_DIR}/config/shards.meta.json}"

IMAGE="${IMAGE:-registry.rcp.epfl.ch/scielo-fulltext/downloader:1}"
CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
RCP_USER="${RCP_USER:-${USER:-}}"
POD_REPO_DIR="${POD_REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
JOB_PREFIX="${JOB_PREFIX:-scielo}"
TIMEOUT="${TIMEOUT:-2d}"
CPUS_PER_POD="${CPUS_PER_POD:-4}"
MEMORY_PER_POD="${MEMORY_PER_POD:-16G}"
NODE_TYPE="${NODE_TYPE:-}"
TARGET_PROXY_RPM="${TARGET_PROXY_RPM:-10}"
RPM_PER_PROXY="${RPM_PER_PROXY:-${TARGET_PROXY_RPM}}"
DOWNLOAD_WORKERS="${DOWNLOAD_WORKERS:-1}"
MAX_SUBTARS="${MAX_SUBTARS:-}"
MIN_FREE_GB="${MIN_FREE_GB:-50}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-}"
FIGURE_TIMEOUT="${FIGURE_TIMEOUT:-}"
REQUEST_RETRIES="${REQUEST_RETRIES:-}"
FIGURE_RETRIES="${FIGURE_RETRIES:-}"
LOG_EVERY="${LOG_EVERY:-}"
HEARTBEAT_EVERY="${HEARTBEAT_EVERY:-}"
MAX_FIGURE_BYTES="${MAX_FIGURE_BYTES:-}"

usage() {
  cat <<EOF
Usage: $0 [options] [shard_id ...]

Submit explicit shard IDs by default. Submitting all shards requires --yes-all.

Options:
  --dry-run             Print csub.py commands; submit nothing
  --yes-all             Allow submitting every shard from metadata
  --image IMG           Override image
  --epfml-dir PATH      Path to epfml-getting-started
  --timeout T           csub.py -t (default: ${TIMEOUT})
  --cpus N              csub.py --cpus (default: ${CPUS_PER_POD})
  --memory M            csub.py --memory (default: ${MEMORY_PER_POD})
  --node-type T         csub.py --node-type
  --local-meta PATH     local shards.meta.json copy
  -h, --help            this help

Important env:
  PROXY_FILE            Secret proxy list path visible inside pods
  SCIELO_CONTACT_EMAIL  Contact identity for request headers
  TARGET_PROXY_RPM      Desired request/minute budget per proxy
  RPM_PER_PROXY         Direct override for per-proxy RPM
  DOWNLOAD_WORKERS      Concurrent row workers per pod; one shared proxy limiter
  REQUEST_TIMEOUT       Optional per-request timeout in seconds
  FIGURE_TIMEOUT        Optional figure-request timeout in seconds
  REQUEST_RETRIES       Optional XML request retries per row
  FIGURE_RETRIES        Optional figure request retries per media URL
  LOG_EVERY             Optional row log interval for ok rows
  HEARTBEAT_EVERY       Optional row heartbeat interval
  MAX_FIGURE_BYTES      Optional max bytes per source figure before retry-queue
  MAX_SUBTARS           Optional smoke limiter per shard
EOF
}

DRY_RUN=0
YES_ALL=0
EXPLICIT_IDS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --yes-all) YES_ALL=1; shift ;;
    --image) IMAGE="$2"; shift 2 ;;
    --epfml-dir) EPFML_DIR="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --cpus) CPUS_PER_POD="$2"; shift 2 ;;
    --memory) MEMORY_PER_POD="$2"; shift 2 ;;
    --node-type) NODE_TYPE="$2"; shift 2 ;;
    --local-meta) LOCAL_META="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage; exit 1 ;;
    *) EXPLICIT_IDS+=("$1"); shift ;;
  esac
done

if [[ -d "${EPFML_DIR}" ]]; then
  EPFML_DIR="$(cd "${EPFML_DIR}" && pwd)"
fi

if [[ ! -f "${LOCAL_META}" ]]; then
  echo "ERROR: ${LOCAL_META} not found." >&2
  echo "       Copy ${CORPUS_ROOT}/index/shards.meta.json here after planning shards." >&2
  exit 1
fi

if [[ -n "${PROXY_URL:-}" ]]; then
  echo "ERROR: PROXY_URL is set locally; inline proxy credentials are not submitted." >&2
  echo "       Put proxies in a secret file on /mloscratch and pass PROXY_FILE=/path/to/file." >&2
  exit 1
fi

if [[ -z "${PROXY_FILE:-}" ]]; then
  echo "ERROR: PROXY_FILE is required for RCP jobs." >&2
  echo "       Bulk external requests from RCP must use approved proxies." >&2
  exit 1
fi

if [[ -z "${SCIELO_CONTACT_EMAIL:-}" && -z "${CONTACT_EMAIL:-}" && -z "${SCIELO_USER_AGENT:-}" ]]; then
  echo "ERROR: operator identity is required. Set SCIELO_CONTACT_EMAIL or SCIELO_USER_AGENT." >&2
  exit 1
fi

if [[ ! "${RPM_PER_PROXY}" =~ ^[0-9]+$ || "${RPM_PER_PROXY}" -lt 1 ]]; then
  echo "ERROR: RPM_PER_PROXY must be a positive integer." >&2
  exit 1
fi

N_SHARDS="$(python3 -c "import json; print(json.load(open('${LOCAL_META}'))['n_shards'])")"
PAD="$(python3 -c "import json; print(json.load(open('${LOCAL_META}'))['pad_shard'])")"

if [[ ${#EXPLICIT_IDS[@]} -eq 0 ]]; then
  if [[ "${YES_ALL}" != "1" ]]; then
    echo "ERROR: refusing all-shard submission without --yes-all." >&2
    exit 1
  fi
  IDS=( $(seq 0 $((N_SHARDS - 1))) )
else
  IDS=( "${EXPLICIT_IDS[@]}" )
fi

for s in "${IDS[@]}"; do
  if [[ ! "${s}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: shard id '${s}' is not an integer" >&2
    exit 1
  fi
  if (( s >= N_SHARDS )); then
    echo "ERROR: shard id ${s} >= N_SHARDS ${N_SHARDS}" >&2
    exit 1
  fi
done

CSUB="${EPFML_DIR}/csub.py"
if [[ ! -f "${CSUB}" ]]; then
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "WARN: csub.py not found at ${CSUB}; continuing because this is a dry-run." >&2
  else
    echo "ERROR: csub.py not found at ${CSUB}" >&2
    exit 1
  fi
fi

echo "Submitting ${#IDS[@]} of ${N_SHARDS} shard(s): ${IDS[*]}"
echo "  image=${IMAGE} corpus=${CORPUS_ROOT}"
echo "  timeout=${TIMEOUT} cpus=${CPUS_PER_POD} memory=${MEMORY_PER_POD}"
echo "  rpm_per_proxy=${RPM_PER_PROXY}"
echo "  download_workers=${DOWNLOAD_WORKERS}"
[[ -n "${REQUEST_TIMEOUT}" ]] && echo "  request_timeout=${REQUEST_TIMEOUT}"
[[ -n "${FIGURE_TIMEOUT}" ]] && echo "  figure_timeout=${FIGURE_TIMEOUT}"
[[ -n "${REQUEST_RETRIES}" ]] && echo "  request_retries=${REQUEST_RETRIES}"
[[ -n "${FIGURE_RETRIES}" ]] && echo "  figure_retries=${FIGURE_RETRIES}"
[[ -n "${MAX_FIGURE_BYTES}" ]] && echo "  max_figure_bytes=${MAX_FIGURE_BYTES}"
[[ -n "${HEARTBEAT_EVERY}" ]] && echo "  heartbeat_every=${HEARTBEAT_EVERY}"
echo "  proxy_partitioning=enabled"
[[ -n "${PROXY_FILE:-}" ]] && echo "  proxy_file configured"
[[ -n "${SCIELO_CONTACT_EMAIL:-}${CONTACT_EMAIL:-}" ]] && echo "  contact identity configured"
[[ -n "${SCIELO_USER_AGENT:-}" ]] && echo "  custom user-agent configured"
[[ -n "${MAX_SUBTARS:-}" ]] && echo "  max_subtars=${MAX_SUBTARS}"
[[ -n "${NODE_TYPE}" ]] && echo "  node_type=${NODE_TYPE}"
echo

append_export() {
  local key="$1"
  local value="$2"
  local quoted
  printf -v quoted "%q" "${value}"
  command="${command} ${key}=${quoted}"
}

for sid_int in "${IDS[@]}"; do
  sid="$(printf "%0${PAD}d" "${sid_int}")"
  job_name="${JOB_PREFIX}-${sid}"

  command="export"
  append_export SHARD_ID "${sid_int}"
  append_export CORPUS_ROOT "${CORPUS_ROOT}"
  append_export REPO_DIR "${POD_REPO_DIR}"
  append_export RCP_USER "${RCP_USER}"
  append_export RPM_PER_PROXY "${RPM_PER_PROXY}"
  append_export DOWNLOAD_WORKERS "${DOWNLOAD_WORKERS}"
  [[ -n "${REQUEST_TIMEOUT}" ]] && append_export REQUEST_TIMEOUT "${REQUEST_TIMEOUT}"
  [[ -n "${FIGURE_TIMEOUT}" ]] && append_export FIGURE_TIMEOUT "${FIGURE_TIMEOUT}"
  [[ -n "${REQUEST_RETRIES}" ]] && append_export REQUEST_RETRIES "${REQUEST_RETRIES}"
  [[ -n "${FIGURE_RETRIES}" ]] && append_export FIGURE_RETRIES "${FIGURE_RETRIES}"
  [[ -n "${LOG_EVERY}" ]] && append_export LOG_EVERY "${LOG_EVERY}"
  [[ -n "${HEARTBEAT_EVERY}" ]] && append_export HEARTBEAT_EVERY "${HEARTBEAT_EVERY}"
  [[ -n "${MAX_FIGURE_BYTES}" ]] && append_export MAX_FIGURE_BYTES "${MAX_FIGURE_BYTES}"
  append_export PROXY_PARTITION_INDEX "${sid_int}"
  append_export PROXY_PARTITION_COUNT "${N_SHARDS}"
  append_export PROXY_FILE "${PROXY_FILE}"
  append_export MIN_FREE_GB "${MIN_FREE_GB}"
  if [[ -n "${MAX_SUBTARS:-}" ]]; then
    append_export MAX_SUBTARS "${MAX_SUBTARS}"
  fi
  if [[ -n "${SCIELO_CONTACT_EMAIL:-}" ]]; then
    append_export SCIELO_CONTACT_EMAIL "${SCIELO_CONTACT_EMAIL}"
  elif [[ -n "${CONTACT_EMAIL:-}" ]]; then
    append_export SCIELO_CONTACT_EMAIL "${CONTACT_EMAIL}"
  fi
  if [[ -n "${SCIELO_USER_AGENT:-}" ]]; then
    append_export SCIELO_USER_AGENT "${SCIELO_USER_AGENT}"
  fi
  command="${command}; exec bash '${POD_REPO_DIR}/scripts/worker.sh'"

  args=(
    -n "${job_name}"
    -g 0
    -t "${TIMEOUT}"
    --train
    --cpus "${CPUS_PER_POD}"
    --memory "${MEMORY_PER_POD}"
    -i "${IMAGE}"
    --command "${command}"
  )
  [[ -n "${NODE_TYPE}" ]] && args+=(--node-type "${NODE_TYPE}")

  if [[ "${DRY_RUN}" == "1" ]]; then
    printf 'DRY: ( cd %q && python3 %q' "${EPFML_DIR}" "${CSUB}"
    for a in "${args[@]}"; do printf ' %q' "${a}"; done
    printf ' )\n'
    continue
  fi

  echo "Submitting ${job_name}"
  if ! ( cd "${EPFML_DIR}" && python3 "${CSUB}" "${args[@]}" ); then
    echo "WARN: submission of ${job_name} failed." >&2
    echo "      Check whether it already exists: runai list jobs | grep ${job_name}" >&2
  fi
done

echo
echo "Monitor with:"
echo "  runai list jobs | grep ${JOB_PREFIX}"
echo "  runai logs ${JOB_PREFIX}-<sid>"
