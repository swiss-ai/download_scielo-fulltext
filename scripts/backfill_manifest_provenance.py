#!/usr/bin/env python3
"""Backfill exact package provenance in an existing typed SciELO manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
from aggregate_manifest import backfill_package_provenance
from common import atomic_write_json, iso_utc_now
from docgraph import (
    manifest_row_is_convertible,
    write_manifest_parquet,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(row: dict[str, object]) -> tuple[str, str]:
    return str(row.get("source") or ""), str(row.get("source_id") or "")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--corpus-root", required=True, type=Path)
    parser.add_argument("--package-producer-commit", required=True)
    parser.add_argument("--enrichment-commit", required=True)
    args = parser.parse_args()

    if args.output.exists():
        raise FileExistsError(f"refusing existing output: {args.output}")
    rows = pq.read_table(args.input).to_pylist()
    identities = [_identity(row) for row in rows]
    if len(identities) != len(set(identities)):
        raise ValueError("input manifest contains duplicate identities")

    before_nonadmitted = {
        _identity(row): json.dumps(row, ensure_ascii=False, sort_keys=True)
        for row in rows
        if not manifest_row_is_convertible(row)
    }
    admitted = [row for row in rows if manifest_row_is_convertible(row)]
    missing_before = sum(
        not row.get("source_package_hash")
        or set(dict(row.get("hashes") or []))
        != set(str(value) for value in row.get("package_members") or [])
        for row in admitted
    )
    enriched = backfill_package_provenance(rows, args.corpus_root)
    incomplete = [
        str(row.get("source_id"))
        for row in rows
        if manifest_row_is_convertible(row)
        and (
            not row.get("source_package_hash")
            or set(dict(row.get("hashes") or {}))
            != set(str(value) for value in row.get("package_members") or [])
            or row.get("file_count") != len(row.get("package_members") or [])
            or row.get("total_bytes") is None
        )
    ]
    if incomplete:
        raise ValueError(
            f"admitted rows still have incomplete package provenance: {incomplete[:20]}"
        )
    after_nonadmitted = {
        _identity(row): json.dumps(row, ensure_ascii=False, sort_keys=True)
        for row in rows
        if not manifest_row_is_convertible(row)
    }
    if before_nonadmitted != after_nonadmitted:
        raise ValueError("nonadmitted rows changed during package backfill")

    write_manifest_parquet(rows, args.output)
    output_rows = pq.read_table(args.output).to_pylist()
    if {_identity(row) for row in output_rows} != set(identities):
        raise ValueError("output manifest identity population changed")
    status_counts = Counter(str(row.get("status") or "") for row in output_rows)
    summary = {
        "created_at": iso_utc_now(),
        "status": "package_provenance_backfill_complete",
        "input": str(args.input),
        "input_sha256": _sha256(args.input),
        "output": str(args.output),
        "output_sha256": _sha256(args.output),
        "package_producer_commit": args.package_producer_commit,
        "enrichment_commit": args.enrichment_commit,
        "rows": len(output_rows),
        "admitted_rows": len(admitted),
        "admitted_rows_missing_before": missing_before,
        "admitted_rows_enriched": enriched,
        "status_counts": dict(sorted(status_counts.items())),
        "nonadmitted_rows_unchanged": True,
    }
    atomic_write_json(args.output.with_suffix(args.output.suffix + ".summary.json"), summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
