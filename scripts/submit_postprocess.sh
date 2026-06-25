#!/usr/bin/env bash
# Submit SciELO post-download verification or aggregate jobs on RCP.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EPFML_DIR="${EPFML_DIR:-${HOME}/dev/epfml-getting-started}"
LOCAL_META="${LOCAL_META:-${REPO_DIR}/config/shards.meta.json}"

IMAGE="${IMAGE:-registry.rcp.epfl.ch/scielo-fulltext/downloader:1}"
CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
RCP_USER="${RCP_USER:-${USER:-}}"
POD_REPO_DIR="${POD_REPO_DIR:-/mloscratch/homes/${RCP_USER}/download_scielo-fulltext}"
JOB_PREFIX="${JOB_PREFIX:-scielo-post}"
TIMEOUT="${TIMEOUT:-12h}"
CPUS_PER_POD="${CPUS_PER_POD:-2}"
MEMORY_PER_POD="${MEMORY_PER_POD:-8G}"
NODE_TYPE="${NODE_TYPE:-}"

usage() {
  cat <<EOF
Usage:
  $0 verify [options] [shard_id ...]
  $0 aggregate [options]
  $0 pipeline [options]
  $0 converter [options]

Options:
  --dry-run             Print csub.py commands; submit nothing
  --yes-all             For verify mode, submit every shard from metadata
  --image IMG           Override image
  --epfml-dir PATH      Path to epfml-getting-started
  --timeout T           csub.py -t (default: ${TIMEOUT})
  --cpus N              csub.py --cpus (default: ${CPUS_PER_POD})
  --memory M            csub.py --memory (default: ${MEMORY_PER_POD})
  --node-type T         csub.py --node-type
  --local-meta PATH     local shards.meta.json copy
  -h, --help            this help

Verify jobs write:
  \${CORPUS_ROOT}/state/verify-shard-XX.json
  \${CORPUS_ROOT}/state/shard-XX.verify.done

Aggregate writes:
  \${CORPUS_ROOT}/manifest.jsonl
  \${CORPUS_ROOT}/manifest.summary.json

Pipeline waits for all shard repair markers, verifies every shard, and then
aggregates. Useful env:
  VERIFY_PARALLELISM     Shards to verify concurrently inside the pod (default 4)
  WAIT_SECONDS           Repair marker polling interval (default 60)
  WAIT_TIMEOUT_SECONDS   0 means no timeout

Converter writes accepted-only JSONL/parquet, tar member inventory, retry-blocked
provenance, checksums, and summary under \${CORPUS_ROOT}/converter. Useful env:
  INSTALL_PYARROW        1 to pip-install pyarrow in the pod if absent
  CONVERTER_OUTPUT_DIR   Override output directory
  REQUIRE_PARQUET        0 to permit JSONL-only output
EOF
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

MODE="$1"
shift
case "${MODE}" in
  verify|aggregate|pipeline|converter) ;;
  -h|--help) usage; exit 0 ;;
  *) echo "ERROR: first argument must be verify, aggregate, pipeline, or converter" >&2; usage; exit 1 ;;
esac

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
  exit 1
fi

N_SHARDS="$(python3 -c "import json; print(json.load(open('${LOCAL_META}'))['n_shards'])")"
PAD="$(python3 -c "import json; print(json.load(open('${LOCAL_META}'))['pad_shard'])")"

if [[ "${MODE}" == "aggregate" || "${MODE}" == "pipeline" || "${MODE}" == "converter" ]]; then
  if [[ ${#EXPLICIT_IDS[@]} -ne 0 || "${YES_ALL}" == "1" ]]; then
    echo "ERROR: ${MODE} mode does not take shard IDs or --yes-all." >&2
    exit 1
  fi
  IDS=("${MODE}")
else
  if [[ ${#EXPLICIT_IDS[@]} -eq 0 ]]; then
    if [[ "${YES_ALL}" != "1" ]]; then
      echo "ERROR: refusing all-shard verification without --yes-all." >&2
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
fi

CSUB="${EPFML_DIR}/csub.py"
if [[ ! -f "${CSUB}" ]]; then
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "WARN: csub.py not found at ${CSUB}; continuing because this is a dry-run." >&2
  else
    echo "ERROR: csub.py not found at ${CSUB}" >&2
    exit 1
  fi
fi

append_export() {
  local key="$1"
  local value="$2"
  local quoted
  printf -v quoted "%q" "${value}"
  command="${command} ${key}=${quoted}"
}

submit_one() {
  local job_name="$1"
  local command="$2"
  local args=(
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
    return
  fi

  echo "Submitting ${job_name}"
  if ! ( cd "${EPFML_DIR}" && python3 "${CSUB}" "${args[@]}" ); then
    echo "WARN: submission of ${job_name} failed." >&2
  fi
}

echo "Submitting ${MODE} postprocess job(s)"
echo "  image=${IMAGE} corpus=${CORPUS_ROOT}"
echo "  timeout=${TIMEOUT} cpus=${CPUS_PER_POD} memory=${MEMORY_PER_POD}"
[[ -n "${NODE_TYPE}" ]] && echo "  node_type=${NODE_TYPE}"

if [[ "${MODE}" == "aggregate" || "${MODE}" == "pipeline" || "${MODE}" == "converter" ]]; then
  command="export"
  append_export POSTPROCESS_MODE "${MODE}"
  append_export CORPUS_ROOT "${CORPUS_ROOT}"
  append_export REPO_DIR "${POD_REPO_DIR}"
  append_export RCP_USER "${RCP_USER}"
  [[ -n "${VERIFY_PARALLELISM:-}" ]] && append_export VERIFY_PARALLELISM "${VERIFY_PARALLELISM}"
  [[ -n "${WAIT_SECONDS:-}" ]] && append_export WAIT_SECONDS "${WAIT_SECONDS}"
  [[ -n "${WAIT_TIMEOUT_SECONDS:-}" ]] && append_export WAIT_TIMEOUT_SECONDS "${WAIT_TIMEOUT_SECONDS}"
  [[ -n "${WAIT_FOR_REPAIR_DONE:-}" ]] && append_export WAIT_FOR_REPAIR_DONE "${WAIT_FOR_REPAIR_DONE}"
  [[ -n "${FORCE_VERIFY:-}" ]] && append_export FORCE_VERIFY "${FORCE_VERIFY}"
  [[ -n "${INSTALL_PYARROW:-}" ]] && append_export INSTALL_PYARROW "${INSTALL_PYARROW}"
  [[ -n "${PYARROW_PACKAGE:-}" ]] && append_export PYARROW_PACKAGE "${PYARROW_PACKAGE}"
  [[ -n "${CONVERTER_OUTPUT_DIR:-}" ]] && append_export CONVERTER_OUTPUT_DIR "${CONVERTER_OUTPUT_DIR}"
  [[ -n "${REQUIRE_PARQUET:-}" ]] && append_export REQUIRE_PARQUET "${REQUIRE_PARQUET}"
  [[ -n "${WRITE_PARQUET:-}" ]] && append_export WRITE_PARQUET "${WRITE_PARQUET}"
  [[ -n "${STRICT_MEMBERS:-}" ]] && append_export STRICT_MEMBERS "${STRICT_MEMBERS}"
  command="${command}; exec bash '${POD_REPO_DIR}/scripts/postprocess_worker.sh'"
  submit_one "${JOB_PREFIX}-${MODE}" "${command}"
else
  echo "  shards=${IDS[*]}"
  for sid_int in "${IDS[@]}"; do
    sid="$(printf "%0${PAD}d" "${sid_int}")"
    command="export"
    append_export POSTPROCESS_MODE verify
    append_export SHARD_ID "${sid_int}"
    append_export CORPUS_ROOT "${CORPUS_ROOT}"
    append_export REPO_DIR "${POD_REPO_DIR}"
    append_export RCP_USER "${RCP_USER}"
    command="${command}; exec bash '${POD_REPO_DIR}/scripts/postprocess_worker.sh'"
    submit_one "${JOB_PREFIX}-verify-${sid}" "${command}"
  done
fi

echo
echo "Monitor with:"
echo "  runai list jobs | grep ${JOB_PREFIX}"
