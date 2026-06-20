# Runbook

## 0. Configure Proxy And Identity

```bash
export PROXY_FILE=/path/to/proxies.txt
export SCIELO_CONTACT_EMAIL=you@example.org
export RPM_PER_PROXY=10
```

No external-fetch script runs without a proxy.

Before publishing or submitting bulk jobs:

```bash
bash scripts/check_repo.sh
```

## 1. Harvest Identifier Pages

Use processing-date windows.

```bash
CORPUS_ROOT=/mloscratch/scielo-fulltext

python3 scripts/harvest_identifiers.py \
  --corpus-root "$CORPUS_ROOT" \
  --from 2025-01-01 --until 2025-12-31
```

## 2. Fetch ArticleMeta Records

On RCP, fetch ArticleMeta in a small number of CPU shards. Keep the aggregate
proxy rate bounded. Proxy lines are partitioned across shards; for a full
all-shard run the effective global rate is approximately
`proxy_count * RPM_PER_PROXY`.
For the first full pass we use 8 shards at 1 request/minute/proxy.

```bash
export PROXY_FILE=/mloscratch/secrets/scielo_proxies.txt
export SCIELO_CONTACT_EMAIL=you@example.org
export N_SHARDS=8
export RPM_PER_PROXY=1

scripts/submit_articlemeta_shards.sh --yes-all
```

The pod-side runner refuses to start unless the identifier harvest state is
complete. To submit waiting pods before harvest completion, add
`--wait-for-identifiers`, but this occupies Run:ai pods while they sleep.

For a single-machine smoke or local debug run:

```bash
python3 scripts/fetch_articlemeta.py \
  --corpus-root "$CORPUS_ROOT"
```

## 3. Build Manifest And Shards

```bash
python3 scripts/build_manifest.py --corpus-root "$CORPUS_ROOT"

python3 scripts/build_shards.py \
  --corpus-root "$CORPUS_ROOT" \
  --n-shards 40 \
  --articles-per-subtar 1000
```

## 4. Run A Single Shard Smoke

Build shards only after reviewing the manifest summary and license-filter
expectations. Use explicit shard IDs first. Submitting every XML+figure shard
requires `--yes-all`.

```bash
python3 scripts/download_worker.py \
  --corpus-root "$CORPUS_ROOT" \
  --shard-id 0 \
  --max-subtars 1

python3 scripts/verify_shard.py \
  --corpus-root "$CORPUS_ROOT" \
  --shard-id 0
```

## 5. Aggregate And Estimate

```bash
python3 scripts/aggregate_manifest.py --corpus-root "$CORPUS_ROOT"

python3 scripts/estimate_volume.py \
  --manifest "$CORPUS_ROOT/manifest.jsonl" \
  --total-identifiers 1409144
```

Use `ok` and `no_figures` as complete accepted statuses. Retry
`partial_figures` and `figures_failed`; do not publish them as complete
figure-inclusive content.

## 6. Recovery

```bash
CORPUS_ROOT=/mloscratch/scielo-fulltext \
bash scripts/recover.sh
```

The recovery helper classifies shards as done, active, partial, stale, or
missing, then prints explicit resubmission candidates.
