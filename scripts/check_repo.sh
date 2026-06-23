#!/usr/bin/env bash
# Fast repository sanity checks suitable for CI and pre-push use.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export ROOT

cleanup() {
  if [[ -n "${tmp_proxy_file:-}" ]]; then
    rm -f "${tmp_proxy_file}"
  fi
  while IFS= read -r cache_dir; do
    rm -rf "${cache_dir}"
  done < <(find "${ROOT}/scripts" -type d -name __pycache__ -print)
}
trap cleanup EXIT

python3 -m py_compile "${ROOT}"/scripts/*.py

for file in "${ROOT}"/scripts/*.sh "${ROOT}"/docker/*.sh; do
  bash -n "${file}"
done

tmp_proxy_file="$(mktemp)"
printf '127.0.0.1:8080\n127.0.0.2:8080\n' > "${tmp_proxy_file}"
PROXY_FILE="${tmp_proxy_file}" \
PROXY_PARTITION_INDEX=1 \
PROXY_PARTITION_COUNT=2 \
python3 - <<'PY'
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, os.path.join(os.environ["ROOT"], "scripts"))
import download_worker
from common import ProxyPool
from common import require_proxies
from common import xml_url_from_final

proxies = require_proxies()
assert proxies == ["http://127.0.0.2:8080/"], proxies
with tempfile.TemporaryDirectory() as tmp:
    pool = ProxyPool(["http://127.0.0.1:8080/"], 60000, shared_rate_dir=tmp)
    proxy, limiter = pool.pick(0)
    assert proxy == "http://127.0.0.1:8080/", proxy
    limiter.wait()
    assert list(Path(tmp).glob("*.rate"))
old_style = xml_url_from_final(
    "http://www.scielo.org.bo/scielo.php?script=sci_arttext&pid=S1012-29662021000200265&tlng=es",
    "es",
)
assert old_style == (
    "http://www.scielo.org.bo/scieloOrg/php/articleXML.php?"
    "pid=S1012-29662021000200265&lang=es"
), old_style
modern = xml_url_from_final("https://www.scielo.br/j/abc/a/example/?lang=en", "en")
assert modern == "https://www.scielo.br/j/abc/a/example/?format=xml&lang=en", modern

accepted_xml = b"""<?xml version="1.0" encoding="utf-8"?>
<article xmlns:xlink="http://www.w3.org/1999/xlink">
  <front>
    <journal-meta><journal-title>Smoke Journal</journal-title></journal-meta>
    <article-meta>
      <article-id pub-id-type="doi">10.0000/smoke</article-id>
      <title-group><article-title>Accepted smoke row</article-title></title-group>
      <permissions>
        <license xlink:href="https://creativecommons.org/licenses/by/4.0/">
          <license-p>Creative Commons Attribution 4.0 International</license-p>
        </license>
      </permissions>
    </article-meta>
  </front>
  <body><p>Enough body text for the accepted-row smoke path.</p></body>
</article>
"""

def fake_fetch_xml(row, proxy_pool, idx, args):
    return accepted_xml, "https://example.test/article?format=xml", 200, {"Content-Type": "application/xml"}

original_fetch_xml = download_worker.fetch_xml
try:
    download_worker.fetch_xml = fake_fetch_xml
    out = download_worker.process_row(
        {
            "status": "planned_xml",
            "source_id": "scielo-smoke-S0000",
            "pid": "S0000",
            "collection": "smoke",
            "doi": "10.0000/smoke",
        },
        "data/shard-00/sub-000.tar",
        None,
        0,
        argparse.Namespace(
            allow_third_party_caption_figures=False,
            min_body_chars=0,
            skip_figures=False,
        ),
    )
finally:
    download_worker.fetch_xml = original_fetch_xml
assert out["status"] == "no_figures", out
assert out["source_id"] == "scielo-smoke-S0000", out
assert any(member.endswith("/source.json") for member, _ in out["_files"]), out["_files"]

assert download_worker.retryable_row_status("xml_http_502")
assert download_worker.retryable_row_status("partial_figures")
assert not download_worker.retryable_row_status("xml_http_404")
assert not download_worker.retryable_row_status("license_skip")

with tempfile.TemporaryDirectory() as tmp:
    work_dir = Path(tmp)
    retry_args = argparse.Namespace(row_retries=2, retry_cached_rows=True)
    download_worker.write_row_cache(work_dir, 0, {
        "source": "scielo",
        "source_id": "scielo-smoke-cached-retry",
        "status": "xml_http_502",
        "error": "HTTP 502",
    })
    assert download_worker.read_row_cache(work_dir, 0, retry_args) is None
    retry_state = download_worker.read_row_retry_state(work_dir, 0)
    assert retry_state["retries_used"] == 1, retry_state
    assert retry_state["history"][0]["status"] == "xml_http_502", retry_state

with tempfile.TemporaryDirectory() as tmp:
    corpus = Path(tmp)
    plan = corpus / "index/shards/shard-00/sub-000.plan.jsonl"
    plan.parent.mkdir(parents=True)
    plan.write_text('{"status":"planned_xml","source_id":"scielo-smoke-retry"}\n', encoding="utf-8")
    calls = {"n": 0}

    def fake_process_row_safe(row, tar_rel, proxy_pool, idx, args):
        calls["n"] += 1
        base = {
            "source": "scielo",
            "source_id": row["source_id"],
            "pid": "",
            "collection": "",
            "doi": "",
            "shard": "",
            "subtar": "",
            "tar_path": tar_rel,
            "fetched_at": "2026-01-01T00:00:00+00:00",
        }
        if calls["n"] < 3:
            return {**base, "status": "xml_http_502", "error": "HTTP 502"}
        return {**base, "status": "no_figures", "_files": []}

    original_process_row_safe = download_worker.process_row_safe
    try:
        download_worker.process_row_safe = fake_process_row_safe
        retry_args = argparse.Namespace(
            workers=1,
            row_retries=2,
            retry_cached_rows=True,
            force=False,
            heartbeat_every=0,
            heartbeat_seconds=0,
            log_every=0,
        )
        retry_counts = download_worker.process_subtar(corpus, plan, "00", "000", None, retry_args)
    finally:
        download_worker.process_row_safe = original_process_row_safe

    manifest_row = json.loads((corpus / "manifests/shard-00/sub-000.jsonl").read_text().splitlines()[0])
    assert calls["n"] == 3, calls
    assert retry_counts["no_figures"] == 1, retry_counts
    assert manifest_row["status"] == "no_figures", manifest_row
    assert manifest_row["row_attempts"] == 3, manifest_row
    assert manifest_row["row_retries"] == 2, manifest_row
    assert len(manifest_row["retry_history"]) == 2, manifest_row
PY

if git -C "${ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if git -C "${ROOT}" ls-files | grep -E '(^|/)(__pycache__|.*\.pyc|.*\.tar|manifest\.(jsonl|parquet))$'; then
    echo "ERROR: generated corpus/build artifacts are tracked" >&2
    exit 1
  fi
fi
