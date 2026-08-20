# download_scielo-fulltext

Acquires SciELO full-text XML and referenced figure media for
[`convert_scielo-fulltext`](https://github.com/swiss-ai/convert_scielo-fulltext).

## Source

| Item | Location |
|---|---|
| Article identifiers | `https://articlemeta.scielo.org/api/v1/article/identifiers/` |
| Article metadata | ArticleMeta records requested with `body=true` |
| Full text | canonical XML endpoint derived from each article URL |
| API documentation | [ArticleMeta](https://github.com/scieloorg/articles_meta/blob/master/docs/source/index.rst) |
| Operations | [`docs/RUNBOOK.md`](docs/RUNBOOK.md) |

Modern OPAC pages provide XML through `?format=xml&lang=...`. Legacy
`scielo.php` records resolve through the SciELO `articleXML.php` endpoint.
Resolved HTML URLs provide a bounded fallback for irregular records.

## Collection

~~~text
identifier windows -> retained ArticleMeta records -> XML and license manifest
  -> shard assignment -> XML and media packages -> verification -> typed manifest
~~~

External HTTP requests require an approved proxy and
`SCIELO_CONTACT_EMAIL`. RCP workers use deterministic proxy partitions or a
shared filesystem-backed per-proxy rate limiter. Internal row concurrency fills
the assigned quota without multiplying the configured request rate.

Submission requires an immutable container image. Completion markers, retained
source pages, per-subtar manifests, bounded retries, and repair utilities make
the collection resumable.

## Data layout

~~~
<corpus-root>/
  raw/articlemeta_identifiers/window-*/page_NNNNNN.json.gz
  index/articlemeta.jsonl.gz
  index/manifest_seed.jsonl
  index/shards/shard-NN/sub-MMM.plan.jsonl
  data/shard-NN/sub-MMM.tar
  manifests/shard-NN/sub-MMM.jsonl
  manifests/shard-NN.jsonl
  manifest.parquet
  manifest.parquet.summary.json
  logs/
  state/
~~~

Each package records the authoritative XML member, ordered package members,
per-member SHA-256 values, exact byte/file totals, and a deterministic package
hash over names and payloads.

`manifest.parquet` uses the shared
[`docgraph`](https://github.com/swiss-ai/docgraph) download-manifest schema.
It joins every worker outcome to the ArticleMeta seed and retains source
metadata, explicit failures and rejections, license evidence, package
provenance, media counts, and quality flags.

## Licensing

Only explicit `CC BY`, `CC0`, and public-domain-like records are admitted.
NC, ND, SA, missing, unknown, malformed, and inconsistent records remain
reason-coded in the manifest and receive no conversion compute. The converter
repeats the shared gate before parsing.

## Local smoke

~~~bash
ROOT=/tmp/scielo-smoke
export SCIELO_CONTACT_EMAIL=you@example.org

python3 scripts/harvest_identifiers.py \
  --corpus-root "$ROOT" --from 2025-01-01 --until 2025-01-31 --max-pages 1
python3 scripts/fetch_articlemeta.py --corpus-root "$ROOT" --limit 25
python3 scripts/build_manifest.py --corpus-root "$ROOT"
python3 scripts/build_shards.py \
  --corpus-root "$ROOT" --n-shards 2 --articles-per-subtar 10
python3 scripts/download_worker.py \
  --corpus-root "$ROOT" --shard-id 0 --max-subtars 1
python3 scripts/verify_shard.py --corpus-root "$ROOT" --shard-id 0
uv run python scripts/aggregate_manifest.py \
  --corpus-root "$ROOT" --backfill-package-provenance
~~~

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for RCP execution,
[`docs/CAVEATS.md`](docs/CAVEATS.md) for source behavior, and
[`docs/REFERENCES.md`](docs/REFERENCES.md) for upstream specifications.

## Development

~~~bash
bash scripts/check_repo.sh
~~~

## Repository layout

~~~
scripts/  discovery, metadata, sharding, workers, recovery, verification
docs/     runbook, caveats, repository settings, and references
tests/    manifest, licensing, media, package, and orchestration fixtures
~~~
