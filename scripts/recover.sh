#!/usr/bin/env bash
# Classify SciELO shard states and print explicit resubmission candidates.

set -euo pipefail

CORPUS_ROOT="${CORPUS_ROOT:-/mloscratch/scielo-fulltext}"
STALE_SECONDS="${STALE_SECONDS:-900}"
META_FILE="${CORPUS_ROOT}/index/shards.meta.json"
STATE_DIR="${CORPUS_ROOT}/state"

if [[ ! -f "${META_FILE}" ]]; then
  echo "ERROR: missing ${META_FILE}. Run build_shards.py first." >&2
  exit 1
fi

N_SHARDS="$(python3 -c "import json; print(json.load(open('${META_FILE}'))['n_shards'])")"
PAD="$(python3 -c "import json; print(json.load(open('${META_FILE}'))['pad_shard'])")"
SUB_COUNT="$(python3 -c "import json; print(json.load(open('${META_FILE}'))['sub_count_per_shard'])")"
NOW="$(date +%s)"

DONE=()
ACTIVE=()
PARTIAL=()
STALE=()
MISSING=()

for ((i=0; i<N_SHARDS; i++)); do
  sid="$(printf "%0${PAD}d" "${i}")"
  shard_done="${STATE_DIR}/shard-${sid}.done"
  heart="${STATE_DIR}/shard-${sid}.heartbeat"
  shard_subdir="${STATE_DIR}/shard-${sid}"
  sub_done_count=0
  incomplete_count=0
  if [[ -d "${shard_subdir}" ]]; then
    sub_done_count="$(find "${shard_subdir}" -maxdepth 1 -name 'sub-*.done' 2>/dev/null | wc -l | tr -d ' ')"
    incomplete_count="$(find "${shard_subdir}" -maxdepth 1 -name 'sub-*.incomplete' 2>/dev/null | wc -l | tr -d ' ')"
  fi
  if [[ -f "${shard_done}" ]]; then
    DONE+=("${i}")
  elif [[ -f "${heart}" ]]; then
    mtime="$(python3 -c "import os; print(int(os.path.getmtime('${heart}')))")"
    age=$((NOW - mtime))
    if (( age > STALE_SECONDS )); then
      STALE+=("${i}:${sub_done_count}/${SUB_COUNT}:stale_${age}s")
    else
      ACTIVE+=("${i}:${sub_done_count}/${SUB_COUNT}:active_${age}s")
    fi
  elif (( sub_done_count > 0 || incomplete_count > 0 )); then
    PARTIAL+=("${i}:${sub_done_count}/${SUB_COUNT}:incomplete_${incomplete_count}")
  else
    MISSING+=("${i}")
  fi
done

echo "Shard states under ${STATE_DIR}:"
echo "  done:    ${#DONE[@]}"
echo "  active:  ${#ACTIVE[@]} ${ACTIVE[*]:-}"
echo "  partial: ${#PARTIAL[@]} ${PARTIAL[*]:-}"
echo "  stale:   ${#STALE[@]} ${STALE[*]:-}"
echo "  missing: ${#MISSING[@]} ${MISSING[*]:-}"

RESUBMIT=()
for item in "${PARTIAL[@]-}" "${STALE[@]-}" "${MISSING[@]-}"; do
  [[ -z "${item}" ]] && continue
  RESUBMIT+=("${item%%:*}")
done

if [[ ${#RESUBMIT[@]} -eq 0 ]]; then
  echo "No resubmission needed."
else
  echo
  echo "Resubmit candidates:"
  echo "  bash scripts/submit_shards.sh ${RESUBMIT[*]}"
fi
