#!/usr/bin/env python3
"""Backfill a full SciELO manifest without materializing the corpus in RAM."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from aggregate_manifest import backfill_package_provenance
from backfill_manifest_provenance import bind_backfill_attribution
from common import atomic_write_json, iso_utc_now
from docgraph import (
    manifest_row_is_convertible,
    write_manifest_parquet_stream,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _identity(row: dict[str, object]) -> tuple[str, str]:
    return str(row.get("source") or ""), str(row.get("source_id") or "")


def _restore_arrow_maps(row: dict[str, Any]) -> dict[str, Any]:
    """Restore JSON-round-tripped Arrow maps to accepted Python mappings."""

    for field in ("source_identifiers", "hashes"):
        value = row.get(field)
        if isinstance(value, list):
            row[field] = dict(value)
    return row


def _update_framed_digest(digest: Any, value: str) -> None:
    payload = value.encode("utf-8")
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _iter_rows(path: Path, *, batch_size: int) -> Iterator[dict[str, Any]]:
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=batch_size):
        yield from batch.to_pylist()


def _new_database(path: Path) -> sqlite3.Connection:
    path.unlink(missing_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute(
        """
        CREATE TABLE identities (
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            PRIMARY KEY (source, source_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE admitted (
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            tar_path TEXT NOT NULL,
            row_json TEXT NOT NULL,
            PRIMARY KEY (source, source_id)
        )
        """
    )
    connection.execute("CREATE INDEX admitted_tar_path ON admitted(tar_path)")
    return connection


def _stage_admitted(
    input_path: Path,
    connection: sqlite3.Connection,
    *,
    batch_size: int,
) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    rows = 0
    admitted = 0
    identity_digest = hashlib.sha256()
    nonadmitted_digest = hashlib.sha256()
    connection.execute("BEGIN")
    try:
        for row in _iter_rows(input_path, batch_size=batch_size):
            identity = _identity(row)
            if not all(identity):
                raise ValueError(f"manifest row has incomplete identity: {identity!r}")
            try:
                connection.execute(
                    "INSERT INTO identities VALUES (?, ?)",
                    identity,
                )
            except sqlite3.IntegrityError as error:
                raise ValueError(
                    f"duplicate manifest identity: {identity!r}"
                ) from error
            rows += 1
            status_counts[str(row.get("status") or "")] += 1
            _update_framed_digest(identity_digest, "\0".join(identity))
            if manifest_row_is_convertible(row):
                admitted += 1
                if row.get("source_package_hash"):
                    raise ValueError(
                        f"{row.get('source_id')}: full historical backfill "
                        "requires an unhashed admitted input population"
                    )
                tar_path = str(row.get("tar_path") or "")
                package_members = [
                    str(value) for value in row.get("package_members") or []
                ]
                if not tar_path or not package_members:
                    raise ValueError(
                        f"{row.get('source_id')}: admitted row lacks package location"
                    )
                connection.execute(
                    "INSERT INTO admitted VALUES (?, ?, ?, ?)",
                    (*identity, tar_path, _canonical_json(row)),
                )
            else:
                _update_framed_digest(nonadmitted_digest, _canonical_json(row))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return {
        "rows": rows,
        "admitted_rows": admitted,
        "status_counts": dict(sorted(status_counts.items())),
        "identity_digest": identity_digest.hexdigest(),
        "nonadmitted_digest": nonadmitted_digest.hexdigest(),
    }


def _enrich_admitted(
    connection: sqlite3.Connection,
    *,
    corpus_root: Path,
    historical_package_producer_attribution: str,
    input_manifest_commit: str,
    input_manifest_sha256: str,
    enrichment_commit: str,
) -> dict[str, int]:
    tar_paths = [
        str(row[0])
        for row in connection.execute(
            "SELECT DISTINCT tar_path FROM admitted ORDER BY tar_path"
        )
    ]
    enriched = 0
    order_differed = 0
    for tar_index, tar_path in enumerate(tar_paths, start=1):
        selected = connection.execute(
            """
            SELECT source, source_id, row_json
            FROM admitted
            WHERE tar_path = ?
            ORDER BY source, source_id
            """,
            (tar_path,),
        ).fetchall()
        rows = [_restore_arrow_maps(json.loads(str(item[2]))) for item in selected]
        changed = backfill_package_provenance(rows, corpus_root)
        if changed != len(rows):
            raise ValueError(
                f"{tar_path}: enriched {changed} of {len(rows)} admitted rows"
            )
        bind_backfill_attribution(
            rows,
            historical_package_producer_attribution=(
                historical_package_producer_attribution
            ),
            input_manifest_commit=input_manifest_commit,
            input_manifest_sha256=input_manifest_sha256,
            enrichment_commit=enrichment_commit,
        )
        updates: list[tuple[str, str, str]] = []
        for selected_row, row in zip(selected, rows, strict=True):
            members = [str(value) for value in row.get("package_members") or []]
            hashes = dict(row.get("hashes") or [])
            if (
                not row.get("source_package_hash")
                or set(hashes) != set(members)
                or row.get("file_count") != len(members)
                or row.get("total_bytes") is None
            ):
                raise ValueError(
                    f"{row.get('source_id')}: package provenance remains incomplete"
                )
            backfill = json.loads(str(row["source_meta_json"]))[
                "package_provenance_backfill"
            ]
            order_differed += bool(
                backfill["historical_tar_member_order_differed"]
            )
            updates.append(
                (_canonical_json(row), str(selected_row[0]), str(selected_row[1]))
            )
        connection.executemany(
            """
            UPDATE admitted
            SET row_json = ?
            WHERE source = ? AND source_id = ?
            """,
            updates,
        )
        connection.commit()
        enriched += changed
        print(
            f"backfilled tar {tar_index}/{len(tar_paths)}: "
            f"{tar_path} ({len(rows)} admitted packages)",
            flush=True,
        )
    return {
        "admitted_rows_enriched": enriched,
        "historical_tar_order_differed_rows": order_differed,
        "tar_files_opened": len(tar_paths),
    }


def _merged_rows(
    input_path: Path,
    connection: sqlite3.Connection,
    *,
    batch_size: int,
) -> Iterator[dict[str, Any]]:
    for row in _iter_rows(input_path, batch_size=batch_size):
        if not manifest_row_is_convertible(row):
            yield row
            continue
        identity = _identity(row)
        selected = connection.execute(
            """
            SELECT row_json FROM admitted
            WHERE source = ? AND source_id = ?
            """,
            identity,
        ).fetchone()
        if selected is None:
            raise ValueError(f"admitted identity was not staged: {identity!r}")
        yield _restore_arrow_maps(json.loads(str(selected[0])))


def _verify_output(
    output_path: Path,
    *,
    batch_size: int,
    expected: dict[str, Any],
    historical_package_producer_attribution: str,
    input_manifest_commit: str,
    input_manifest_sha256: str,
    enrichment_commit: str,
) -> dict[str, Any]:
    rows = 0
    admitted = 0
    status_counts: Counter[str] = Counter()
    identity_digest = hashlib.sha256()
    nonadmitted_digest = hashlib.sha256()
    order_differed = 0
    for row in _iter_rows(output_path, batch_size=batch_size):
        identity = _identity(row)
        rows += 1
        status_counts[str(row.get("status") or "")] += 1
        _update_framed_digest(identity_digest, "\0".join(identity))
        if not manifest_row_is_convertible(row):
            _update_framed_digest(nonadmitted_digest, _canonical_json(row))
            continue
        admitted += 1
        members = [str(value) for value in row.get("package_members") or []]
        if (
            not row.get("source_package_hash")
            or set(dict(row.get("hashes") or [])) != set(members)
            or row.get("file_count") != len(members)
            or row.get("total_bytes") is None
        ):
            raise ValueError(
                f"{row.get('source_id')}: output package provenance is incomplete"
            )
        backfill = json.loads(str(row.get("source_meta_json") or "{}")).get(
            "package_provenance_backfill"
        )
        if not isinstance(backfill, dict):
            raise ValueError(f"{row.get('source_id')}: output attribution is missing")
        if (
            backfill.get("historical_package_producer_attribution")
            != historical_package_producer_attribution
            or backfill.get("input_manifest_commit") != input_manifest_commit
            or backfill.get("input_manifest_sha256") != input_manifest_sha256
            or backfill.get("enrichment_commit") != enrichment_commit
        ):
            raise ValueError(
                f"{row.get('source_id')}: output attribution does not match pins"
            )
        order_differed += bool(
            backfill.get("historical_tar_member_order_differed")
        )
    observed = {
        "rows": rows,
        "admitted_rows": admitted,
        "status_counts": dict(sorted(status_counts.items())),
        "identity_digest": identity_digest.hexdigest(),
        "nonadmitted_digest": nonadmitted_digest.hexdigest(),
        "historical_tar_order_differed_rows": order_differed,
    }
    for key in (
        "rows",
        "admitted_rows",
        "status_counts",
        "identity_digest",
        "nonadmitted_digest",
    ):
        if observed[key] != expected[key]:
            raise ValueError(
                f"output {key} changed: expected={expected[key]!r}, "
                f"observed={observed[key]!r}"
            )
    return observed


def backfill_manifest_bounded(
    *,
    input_path: Path,
    output_path: Path,
    corpus_root: Path,
    database_path: Path,
    historical_package_producer_attribution: str,
    input_manifest_commit: str,
    enrichment_commit: str,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    """Enrich admitted rows while preserving nonadmitted rows exactly."""

    if output_path.exists():
        raise FileExistsError(f"refusing existing output: {output_path}")
    input_sha256 = _sha256(input_path)
    connection = _new_database(database_path)
    try:
        staged = _stage_admitted(
            input_path,
            connection,
            batch_size=batch_size,
        )
        enriched = _enrich_admitted(
            connection,
            corpus_root=corpus_root,
            historical_package_producer_attribution=(
                historical_package_producer_attribution
            ),
            input_manifest_commit=input_manifest_commit,
            input_manifest_sha256=input_sha256,
            enrichment_commit=enrichment_commit,
        )
        if enriched["admitted_rows_enriched"] != staged["admitted_rows"]:
            raise ValueError("not every admitted row was enriched")
        written = write_manifest_parquet_stream(
            _merged_rows(input_path, connection, batch_size=batch_size),
            output_path,
            batch_size=batch_size,
            identity_check="prevalidated",
        )
        if written != staged["rows"]:
            raise ValueError(
                f"wrote {written} rows from {staged['rows']} input rows"
            )
    finally:
        connection.close()

    verified = _verify_output(
        output_path,
        batch_size=batch_size,
        expected=staged,
        historical_package_producer_attribution=(
            historical_package_producer_attribution
        ),
        input_manifest_commit=input_manifest_commit,
        input_manifest_sha256=input_sha256,
        enrichment_commit=enrichment_commit,
    )
    if (
        verified["historical_tar_order_differed_rows"]
        != enriched["historical_tar_order_differed_rows"]
    ):
        raise ValueError("historical tar-order count changed after output write")
    summary = {
        "created_at": iso_utc_now(),
        "status": "package_provenance_backfill_complete",
        "input": str(input_path),
        "input_sha256": input_sha256,
        "output": str(output_path),
        "output_sha256": _sha256(output_path),
        "historical_package_producer_attribution": (
            historical_package_producer_attribution
        ),
        "input_manifest_commit": input_manifest_commit,
        "enrichment_commit": enrichment_commit,
        "rows": staged["rows"],
        "admitted_rows": staged["admitted_rows"],
        "admitted_rows_missing_before": staged["admitted_rows"],
        "admitted_rows_enriched": enriched["admitted_rows_enriched"],
        "package_hash_order": "canonical_manifest_package_members",
        "historical_tar_order_differed_rows": (
            enriched["historical_tar_order_differed_rows"]
        ),
        "status_counts": staged["status_counts"],
        "identity_digest": staged["identity_digest"],
        "nonadmitted_digest": staged["nonadmitted_digest"],
        "nonadmitted_rows_unchanged": True,
        "tar_access": "seekable_headers_plus_selected_admitted_members",
        "tar_files_opened": enriched["tar_files_opened"],
        "bounded_memory": True,
        "batch_size": batch_size,
    }
    atomic_write_json(
        output_path.with_suffix(output_path.suffix + ".summary.json"),
        summary,
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--corpus-root", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument(
        "--historical-package-producer-attribution",
        required=True,
    )
    parser.add_argument("--input-manifest-commit", required=True)
    parser.add_argument("--enrichment-commit", required=True)
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    summary = backfill_manifest_bounded(
        input_path=args.input,
        output_path=args.output,
        corpus_root=args.corpus_root,
        database_path=args.database,
        historical_package_producer_attribution=(
            args.historical_package_producer_attribution
        ),
        input_manifest_commit=args.input_manifest_commit,
        enrichment_commit=args.enrichment_commit,
        batch_size=args.batch_size,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
