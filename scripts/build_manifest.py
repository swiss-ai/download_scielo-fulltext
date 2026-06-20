#!/usr/bin/env python3
"""Build a SciELO seed manifest from ArticleMeta rows."""
from __future__ import annotations

import argparse
import collections
from pathlib import Path

from common import atomic_write_json, choose_fulltext, iso_utc_now, read_jsonl, source_id, write_jsonl


def is_completed_jsonl(path: Path) -> bool:
    return path.name.endswith(".jsonl") or path.name.endswith(".jsonl.gz")


def iter_articlemeta_rows(path: Path):
    if path.is_dir():
        for part in sorted(path.glob("*.jsonl*")):
            if not is_completed_jsonl(part):
                continue
            yield from read_jsonl(part)
        return
    yield from read_jsonl(path)


def articlemeta_value(d: dict, *keys: str) -> str:
    for key in keys:
        val = d.get(key)
        if isinstance(val, str) and val:
            return val
        if isinstance(val, int):
            return str(val)
    return ""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--input", default=None)
    p.add_argument("--output", default=None)
    args = p.parse_args()

    corpus = Path(args.corpus_root)
    if args.input:
        src = Path(args.input)
    else:
        single = corpus / "index" / "articlemeta.jsonl.gz"
        sharded = corpus / "index" / "articlemeta"
        src = single if single.exists() else sharded
    output = Path(args.output) if args.output else corpus / "index" / "manifest_seed.jsonl"
    summary_path = output.with_suffix(".summary.json")

    rows = []
    counters = collections.Counter()
    seen: set[tuple[str, str]] = set()
    if not src.exists():
        raise SystemExit(f"ERROR: ArticleMeta input not found: {src}")
    for i, raw in enumerate(iter_articlemeta_rows(src)):
        code = raw.get("code") or ""
        collection = raw.get("collection") or ""
        key = (collection, code)
        if key in seen:
            counters["duplicate_pid"] += 1
            continue
        seen.add(key)
        d = raw.get("articlemeta") or {}
        fulltexts = d.get("fulltexts") or {}
        lang, html_url = choose_fulltext(fulltexts)
        doi = articlemeta_value(d, "doi")
        sid = source_id(collection, code, doi)
        if raw.get("status") != "ok":
            status = "articlemeta_error"
        elif not html_url:
            status = "no_fulltext_url"
        else:
            status = "planned_xml"
        row = {
            "source": "scielo",
            "source_id": sid,
            "pid": code,
            "collection": collection,
            "doi": doi,
            "document_type": articlemeta_value(d, "document_type"),
            "publication_year": articlemeta_value(d, "publication_year"),
            "publication_date": articlemeta_value(d, "publication_date"),
            "processing_date": articlemeta_value(d, "processing_date"),
            "article_title": "",
            "journal_title": "",
            "articlemeta_row_index": i,
            "status": status,
            "preferred_lang": lang,
            "fulltext_html_url": html_url,
            "fulltexts": fulltexts,
            "license_code": None,
            "license_policy": "pending_xml",
        }
        rows.append(row)
        counters[status] += 1

    n = write_jsonl(output, rows)
    atomic_write_json(summary_path, {
        "created_at": iso_utc_now(),
        "input": str(src),
        "output": str(output),
        "rows_written": n,
        "status_counts": dict(counters),
    })
    print(f"wrote {n} rows to {output}")
    print(f"summary: {summary_path}")


if __name__ == "__main__":
    main()
