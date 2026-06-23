# download_scielo-fulltext

Resumable downloader for SciELO full-text XML and referenced figure media.

The source surface is indirect:

1. Article identifiers from ArticleMeta:
   `https://articlemeta.scielo.org/api/v1/article/identifiers/`
2. Per-article ArticleMeta records with `body=true`, used to discover full-text
   HTML URLs.
3. Canonical XML endpoints derived from those HTML URLs. Modern OPAC URLs use
   `?format=xml&lang=...`; legacy `scielo.php?script=sci_arttext&pid=...`
   URLs use the SciELO `articleXML.php` endpoint. The worker can fall back to
   resolving final HTML URLs for edge cases.

This repo follows the operational style of `download_copernicus-publications`:
raw source pages, explicit manifests, deterministic shard plans, tar packing,
per-subtar manifests, verification, and recovery-friendly state markers.

## Safety Defaults

Proxy use is mandatory. Scripts that make external HTTP requests refuse to run
unless `PROXY_URL`, `PROXY_FILE`, `HTTPS_PROXY`, or `HTTP_PROXY` is configured.
This is intentional to avoid blocking university / cluster shared IPs.

Set an attributable request identity:

```bash
export SCIELO_CONTACT_EMAIL=you@example.org
```

For parallel RCP runs, proxy lines can be partitioned with
`PROXY_PARTITION_INDEX` and `PROXY_PARTITION_COUNT`. The submit helpers set
these automatically so full-shard runs preserve a global per-proxy budget instead
of multiplying it by the number of workers.

High-throughput runs can instead set `PROXY_PARTITIONING=0`. In that mode every
pod receives the full proxy list and `PROXY_SHARED_RATE_DIR` coordinates a
filesystem-backed per-proxy limiter across pods. This avoids leaving a pod's
small proxy slice idle while that pod is resuming cached rows, assembling a
subtar, or blocked on slow figure media.

`DOWNLOAD_WORKERS` controls row-level concurrency inside each pod. Threads share
the pod's proxy pool and per-proxy rate limiters, so concurrency can fill the
assigned proxy quota without multiplying the configured RPM per proxy.

## Quick Smoke

With a proxy configured:

```bash
ROOT=/tmp/scielo-smoke

python3 scripts/harvest_identifiers.py \
  --corpus-root "$ROOT" \
  --from 2025-01-01 --until 2025-01-31 \
  --max-pages 1

python3 scripts/fetch_articlemeta.py \
  --corpus-root "$ROOT" \
  --limit 25

python3 scripts/build_manifest.py \
  --corpus-root "$ROOT"

python3 scripts/build_shards.py \
  --corpus-root "$ROOT" \
  --n-shards 2 \
  --articles-per-subtar 10

python3 scripts/download_worker.py \
  --corpus-root "$ROOT" \
  --shard-id 0 \
  --max-subtars 1

python3 scripts/verify_shard.py \
  --corpus-root "$ROOT" \
  --shard-id 0

python3 scripts/aggregate_manifest.py \
  --corpus-root "$ROOT"

python3 scripts/estimate_volume.py \
  --manifest "$ROOT/manifest.jsonl" \
  --total-identifiers 1409144
```

## Layout

```text
/mloscratch/scielo-fulltext/
  raw/articlemeta_identifiers/window-YYYY-MM-DD_YYYY-MM-DD/page_NNNNNN.json.gz
  index/articlemeta.jsonl.gz
  index/manifest_seed.jsonl
  index/shards/shard-NN/sub-MMM.plan.jsonl
  data/shard-NN/sub-MMM.tar
  manifests/shard-NN/sub-MMM.jsonl
  manifests/shard-NN.jsonl
  manifest.jsonl
  manifest.parquet
```

Tar members:

```text
scielo-{collection}-{pid}/article.xml
scielo-{collection}-{pid}/source.json
scielo-{collection}-{pid}/figures/NNN-{hash}-{basename}
```

TIFF figure originals and oversized raster images are rendered to bounded JPEG
derivatives by default before packing. The tar stores the derivative, not the
huge source raster; the manifest keeps `original_bytes`, `original_sha256`,
dimensions, render settings, and rendered byte counts. This mirrors the arXiv
media-conversion policy: normalize heavy or awkward raster formats at the
converter boundary and record failures explicitly.

Only articles whose parsed XML license is CC BY, CC0, or public-domain-like are
packaged. Ambiguous, missing, NC, ND, or SA licenses are manifest-only and are
not packaged as usable content. Figure/table captions with third-party signals
such as `permission`, `copyright`, `adapted from`, or `reproduced from` are
rejected by default as `license_figure_ambiguous`.

For final clean-corpus accounting, use `ok` and `no_figures` as complete
accepted rows. Treat `partial_figures` and `figures_failed` as retry queues, not
accepted deliverables.

For high-throughput runs, prefer row-level requeues over sleeping request
retries:

```bash
REQUEST_RETRIES=0
FIGURE_RETRIES=0
ROW_RETRIES=2
RETRY_CACHED_ROWS=1
```

`ROW_RETRIES` immediately requeues retryable row outcomes inside the subtar
work queue, so other rows keep filling the shared proxy quota. Retryable row
outcomes include transient XML HTTP statuses (`403`, `408`, `429`, and `5xx`),
XML fetch/parse/html-response failures, and incomplete figure rows
(`partial_figures`, `figures_failed`). After the configured row attempts are
exhausted, the manifest records `retry_blocked_<final_status>` with
`retry_history`. `RETRY_CACHED_ROWS=1` makes resumed subtars drop old unblocked
retryable row caches so they can enter the same queue; blocked rows remain
stable until explicitly forced.

## Repository Checks

```bash
bash scripts/check_repo.sh
```

The check script compiles all Python files and syntax-checks shell scripts. The
GitHub Actions workflow runs the same command on pushes and pull requests.

## GitHub Metadata

Repository metadata lives under `.github/`: CI workflow, pull request template,
issue template, and a `repository.yml` settings file with the suggested
description and topics. Fill in the real CODEOWNERS team after the GitHub repo is
created.

## License

The downloader code is Apache-2.0 licensed. Downloaded SciELO article content is
not covered by this repository license; the pipeline packages only XML-declared
CC BY, CC0, or public-domain-like content.
