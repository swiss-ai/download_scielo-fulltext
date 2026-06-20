#!/usr/bin/env bash
# Submit one Run:ai CPU job per SciELO ArticleMeta metadata shard.

set -euo pipefail

RUNAI_PROJECT="${RUNAI_PROJECT:-mlo-shcherba}"
IMAGE="${IMAGE:-ic-registry.epfl.ch/mlo/mlo-base:uv1}"
PVC="${PVC:-mlo-scratch:/mloscratch}"
CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
RCP_USER="${RCP_USER:-${USER:-}}"
POD_REPO_DIR="${POD_REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
JOB_PREFIX="${JOB_PREFIX:-scielo-meta}"
CPUS_PER_POD="${CPUS_PER_POD:-1}"
MEMORY_PER_POD="${MEMORY_PER_POD:-4G}"
NB_GROUP="${NB_GROUP:-MLO-unit}"
N_SHARDS="${N_SHARDS:-8}"
TARGET_PROXY_RPM="${TARGET_PROXY_RPM:-1}"
RPM_PER_PROXY="${RPM_PER_PROXY:-${TARGET_PROXY_RPM}}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-90}"
REQUEST_RETRIES="${REQUEST_RETRIES:-4}"
ARTICLEMETA_WORKERS="${ARTICLEMETA_WORKERS:-4}"
ARTICLEMETA_PREFETCH_PER_WORKER="${ARTICLEMETA_PREFETCH_PER_WORKER:-4}"
ARTICLEMETA_SUB_SHARDS="${ARTICLEMETA_SUB_SHARDS:-1}"
ARTICLEMETA_SUB_SHARD_ID="${ARTICLEMETA_SUB_SHARD_ID:-0}"
PROXY_PARTITIONING="${PROXY_PARTITIONING:-1}"
DATE_FROM="${DATE_FROM:-1900-01-01}"
DATE_UNTIL="${DATE_UNTIL:-2026-06-19}"
WAIT_FOR_IDENTIFIERS="${WAIT_FOR_IDENTIFIERS:-0}"
CHECK_IDENTIFIER_COMPLETE="${CHECK_IDENTIFIER_COMPLETE:-1}"
WAIT_SLEEP_SECONDS="${WAIT_SLEEP_SECONDS:-300}"
DRY_RUN=0
YES_ALL=0
EXPLICIT_IDS=()

usage() {
  cat <<EOF
Usage: $0 [options] [shard_id ...]

Submit explicit ArticleMeta shard IDs by default. Submitting all shards requires
--yes-all.

Options:
  --dry-run                 Print runai commands; submit nothing
  --yes-all                 Submit all shard IDs 0..N_SHARDS-1
  --wait-for-identifiers    Let pods wait until identifier harvest is complete
  --project NAME            Run:ai project (default: ${RUNAI_PROJECT})
  --image IMG               Container image (default: ${IMAGE})
  --n-shards N              Number of ArticleMeta shards (default: ${N_SHARDS})
  --job-prefix PREFIX       Job name prefix (default: ${JOB_PREFIX})
  --cpus N                  CPU request per pod (default: ${CPUS_PER_POD})
  --memory M                Memory request per pod (default: ${MEMORY_PER_POD})
  -h, --help                Show this help

Important env:
  PROXY_FILE                Secret proxy list path visible inside pods
  SCIELO_CONTACT_EMAIL      Contact identity for request headers
  ARTICLEMETA_WORKERS       Concurrent fetch threads per pod (default: ${ARTICLEMETA_WORKERS})
  ARTICLEMETA_SUB_SHARDS    Optional per-shard split count (default: ${ARTICLEMETA_SUB_SHARDS})
  TARGET_PROXY_RPM          Desired request/minute budget per proxy
  RPM_PER_PROXY             Direct override for per-proxy RPM. Proxy lines are
                            partitioned across shards, so full-run global rate
                            is roughly proxy_count * RPM_PER_PROXY.
  PROXY_PARTITIONING        Set to 0 to give each pod the full proxy list
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --yes-all) YES_ALL=1; shift ;;
    --wait-for-identifiers) WAIT_FOR_IDENTIFIERS=1; shift ;;
    --project) RUNAI_PROJECT="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --n-shards) N_SHARDS="$2"; shift 2 ;;
    --job-prefix) JOB_PREFIX="$2"; shift 2 ;;
    --cpus) CPUS_PER_POD="$2"; shift 2 ;;
    --memory) MEMORY_PER_POD="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage; exit 1 ;;
    *) EXPLICIT_IDS+=("$1"); shift ;;
  esac
done

if [[ -n "${PROXY_URL:-}" ]]; then
  echo "ERROR: PROXY_URL is set locally; inline proxy credentials are not submitted." >&2
  echo "       Put proxies in a secret file on /mloscratch and pass PROXY_FILE=/path/to/file." >&2
  exit 1
fi
if [[ -z "${PROXY_FILE:-}" ]]; then
  echo "ERROR: PROXY_FILE is required for RCP jobs." >&2
  exit 1
fi
if [[ -z "${SCIELO_CONTACT_EMAIL:-}" && -z "${CONTACT_EMAIL:-}" && -z "${SCIELO_USER_AGENT:-}" ]]; then
  echo "ERROR: set SCIELO_CONTACT_EMAIL or SCIELO_USER_AGENT." >&2
  exit 1
fi
if [[ ! "${N_SHARDS}" =~ ^[0-9]+$ || "${N_SHARDS}" -lt 1 ]]; then
  echo "ERROR: N_SHARDS must be a positive integer" >&2
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
if [[ ! "${ARTICLEMETA_SUB_SHARDS}" =~ ^[0-9]+$ || "${ARTICLEMETA_SUB_SHARDS}" -lt 1 ]]; then
  echo "ERROR: ARTICLEMETA_SUB_SHARDS must be a positive integer" >&2
  exit 1
fi
if [[ ! "${ARTICLEMETA_SUB_SHARD_ID}" =~ ^[0-9]+$ || "${ARTICLEMETA_SUB_SHARD_ID}" -ge "${ARTICLEMETA_SUB_SHARDS}" ]]; then
  echo "ERROR: ARTICLEMETA_SUB_SHARD_ID must satisfy 0 <= ARTICLEMETA_SUB_SHARD_ID < ARTICLEMETA_SUB_SHARDS" >&2
  exit 1
fi
if [[ "${PROXY_PARTITIONING}" != "0" && "${PROXY_PARTITIONING}" != "1" ]]; then
  echo "ERROR: PROXY_PARTITIONING must be 0 or 1" >&2
  exit 1
fi

if [[ ${#EXPLICIT_IDS[@]} -eq 0 ]]; then
  if [[ "${YES_ALL}" != "1" ]]; then
    echo "ERROR: refusing all-shard submission without --yes-all." >&2
    exit 1
  fi
  IDS=( $(seq 0 $((N_SHARDS - 1))) )
else
  IDS=( "${EXPLICIT_IDS[@]}" )
fi

for shard_id in "${IDS[@]}"; do
  if [[ ! "${shard_id}" =~ ^[0-9]+$ || "${shard_id}" -ge "${N_SHARDS}" ]]; then
    echo "ERROR: shard id '${shard_id}' must satisfy 0 <= shard_id < ${N_SHARDS}" >&2
    exit 1
  fi
done

DEFAULT_RUN_AS_UID="$(id -u)"
DEFAULT_RUN_AS_GID="$(id -g)"
SCRATCH_HOME="/mnt/mlo/scratch/homes/${RCP_USER}"
if [[ -d "${SCRATCH_HOME}" ]]; then
  DEFAULT_RUN_AS_UID="$(stat -c %u "${SCRATCH_HOME}")"
  DEFAULT_RUN_AS_GID="$(stat -c %g "${SCRATCH_HOME}")"
fi
RUN_AS_UID="${RUN_AS_UID:-${DEFAULT_RUN_AS_UID}}"
RUN_AS_GID="${RUN_AS_GID:-${DEFAULT_RUN_AS_GID}}"
CONTACT_VALUE="${SCIELO_CONTACT_EMAIL:-${CONTACT_EMAIL:-}}"

echo "Submitting ${#IDS[@]} of ${N_SHARDS} ArticleMeta shard(s): ${IDS[*]}"
echo "  project=${RUNAI_PROJECT}"
echo "  image=${IMAGE}"
echo "  corpus=${CORPUS_ROOT}"
echo "  cpus=${CPUS_PER_POD} memory=${MEMORY_PER_POD}"
echo "  rpm_per_proxy=${RPM_PER_PROXY} proxy_partitioning=${PROXY_PARTITIONING}"
echo "  articlemeta_workers=${ARTICLEMETA_WORKERS}"
echo "  articlemeta_sub_shards=${ARTICLEMETA_SUB_SHARDS} sub_shard_id=${ARTICLEMETA_SUB_SHARD_ID}"
echo "  wait_for_identifiers=${WAIT_FOR_IDENTIFIERS}"
echo

shell_quote() {
  printf "%q" "$1"
}

for shard_id in "${IDS[@]}"; do
  job_name="${JOB_PREFIX}-${shard_id}"
  payload="$(
    cat <<EOF
source ~/.zshrc 2>/dev/null || true
export RCP_USER=$(shell_quote "${RCP_USER}")
export REPO_DIR=$(shell_quote "${POD_REPO_DIR}")
export CORPUS_ROOT=$(shell_quote "${CORPUS_ROOT}")
export PROXY_FILE=$(shell_quote "${PROXY_FILE}")
export SCIELO_CONTACT_EMAIL=$(shell_quote "${CONTACT_VALUE}")
export RPM_PER_PROXY=$(shell_quote "${RPM_PER_PROXY}")
export ARTICLEMETA_WORKERS=$(shell_quote "${ARTICLEMETA_WORKERS}")
export ARTICLEMETA_PREFETCH_PER_WORKER=$(shell_quote "${ARTICLEMETA_PREFETCH_PER_WORKER}")
export ARTICLEMETA_SUB_SHARDS=$(shell_quote "${ARTICLEMETA_SUB_SHARDS}")
export ARTICLEMETA_SUB_SHARD_ID=$(shell_quote "${ARTICLEMETA_SUB_SHARD_ID}")
EOF
  )"
  payload+=$'\n'
  if [[ "${PROXY_PARTITIONING}" == "1" ]]; then
    payload+="$(cat <<EOF
export PROXY_PARTITION_INDEX=$(shell_quote "${shard_id}")
export PROXY_PARTITION_COUNT=$(shell_quote "${N_SHARDS}")
EOF
    )"
    payload+=$'\n'
  fi
  payload+="$(cat <<EOF
export N_SHARDS=$(shell_quote "${N_SHARDS}")
export SHARD_ID=$(shell_quote "${shard_id}")
export DATE_FROM=$(shell_quote "${DATE_FROM}")
export DATE_UNTIL=$(shell_quote "${DATE_UNTIL}")
export REQUEST_TIMEOUT=$(shell_quote "${REQUEST_TIMEOUT}")
export REQUEST_RETRIES=$(shell_quote "${REQUEST_RETRIES}")
export CHECK_IDENTIFIER_COMPLETE=$(shell_quote "${CHECK_IDENTIFIER_COMPLETE}")
export WAIT_FOR_IDENTIFIERS=$(shell_quote "${WAIT_FOR_IDENTIFIERS}")
export WAIT_SLEEP_SECONDS=$(shell_quote "${WAIT_SLEEP_SECONDS}")
exec bash $(shell_quote "${POD_REPO_DIR}/scripts/rcp_fetch_articlemeta.sh")
EOF
  )"
  payload_b64="$(printf "%s" "${payload}" | base64 | tr -d '\n')"
  runai_args=(
    submit
    --name "${job_name}"
    --project "${RUNAI_PROJECT}"
    --image "${IMAGE}"
    --gpu 0
    --run-as-uid "${RUN_AS_UID}"
    --run-as-gid "${RUN_AS_GID}"
    --pvc "${PVC}"
    --image-pull-policy Always
    --allow-privilege-escalation true
    --cpu "${CPUS_PER_POD}"
    --memory "${MEMORY_PER_POD}"
    --backoff-limit 0
    --environment HOME="/home/${RCP_USER}"
    --environment NB_USER="${RCP_USER}"
    --environment NB_UID="${RUN_AS_UID}"
    --environment NB_GROUP="${NB_GROUP}"
    --environment NB_GID="${RUN_AS_GID}"
    --environment WORKING_DIR="/mloscratch/homes/${RCP_USER}"
    --environment SCRATCH_HOME="/mloscratch/homes/${RCP_USER}"
    --environment SCRATCH_HOME_ROOT="/mloscratch/homes"
    --environment EPFML_LDAP="${RCP_USER}"
    --environment HF_HOME="/mloscratch/hf_cache"
    --environment UV_PYTHON_VERSION=3.11
    --environment GIT_CONFIG_GLOBAL="/mloscratch/homes/${RCP_USER}/.gitconfig"
    --environment UV_CACHE_DIR="/mloscratch/homes/${RCP_USER}/.cache/uv"
    --environment UV_PYTHON_INSTALL_DIR="/mloscratch/homes/${RCP_USER}/.uv"
    --environment TZ=Europe/Zurich
    --
    /bin/zsh
    -lc
    "echo ${payload_b64} | base64 -d > /tmp/job_payload.sh && cat /tmp/job_payload.sh && /bin/bash /tmp/job_payload.sh"
  )

  if [[ "${DRY_RUN}" == "1" ]]; then
    printf 'DRY: runai'
    for arg in "${runai_args[@]}"; do
      printf ' %q' "${arg}"
    done
    printf '\n'
    continue
  fi

  echo "Submitting ${job_name}"
  if ! runai "${runai_args[@]}"; then
    echo "WARN: submission of ${job_name} failed." >&2
    echo "      Check whether it already exists: runai list jobs | grep ${job_name}" >&2
  fi
done

echo
echo "Monitor with:"
echo "  runai list jobs -p ${RUNAI_PROJECT} | grep ${JOB_PREFIX}"
echo "  runai logs ${JOB_PREFIX}-<id> -p ${RUNAI_PROJECT} --tail 50"
