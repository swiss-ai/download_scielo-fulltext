#!/usr/bin/env python3
"""Aggregate SciELO worker rows into the canonical typed root manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import tarfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator

from common import atomic_write_json, iso_utc_now, read_jsonl
from docgraph import (
    MANIFEST_SCHEMA_VERSION,
    decide_license,
    license_codes_agree,
    manifest_row_is_convertible,
    write_manifest_parquet_stream,
)

_COMPLETE = {"ok", "no_figures"}
_PARTIAL = {"partial_figures", "figures_failed", "figures_skipped"}
_RETRYABLE = {"xml_fetch_error", "xml_html_response"}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _publication_year(value: Any) -> int | None:
    text = str(value or "").strip()
    return int(text) if text.isdigit() and len(text) == 4 else None


def _status(source_status: str, row: dict[str, Any]) -> tuple[str, str | None, bool]:
    if source_status in _COMPLETE:
        return "complete", None, False
    if source_status in _PARTIAL:
        reason = {
            "partial_figures": "media_partial",
            "figures_failed": "media_failed",
            "figures_skipped": "media_not_fetched",
        }[source_status]
        return "partial", reason, True
    if source_status.startswith("license_"):
        return "rejected", "license_not_allowed", False
    if source_status.startswith("retry_blocked_") or row.get("retry_blocked"):
        return "failed", "retry_exhausted", False
    if source_status in _RETRYABLE or source_status.startswith("xml_http_5"):
        return "retry", "source_temporarily_unavailable", True
    reason = {
        "xml_no_body": "xml_no_body",
        "xml_parse_error": "xml_parse_error",
        "xml_http_404": "xml_not_found",
        "row_error": "worker_row_error",
        "articlemeta_error": "metadata_fetch_error",
        "no_fulltext_url": "fulltext_url_missing",
    }.get(source_status, "source_status_failed")
    return "failed", reason, False


def _prefer(worker: dict[str, Any], seed: dict[str, Any], key: str) -> Any:
    value = worker.get(key)
    return value if value not in (None, "", [], {}) else seed.get(key)


def _strings(*values: Any) -> list[str]:
    """Flatten, trim, and de-duplicate retained source evidence."""
    output: list[str] = []
    for value in values:
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text and text not in output:
                output.append(text)
    return output


def normalize_record(worker: dict[str, Any], seed: dict[str, Any] | None) -> dict[str, Any]:
    seed = seed or {}
    source_id = str(worker.get("source_id") or seed.get("source_id") or "")
    if not source_id:
        raise ValueError("manifest row without source_id")
    source_status = str(worker.get("status") or "")
    status, reason_code, retryable = _status(source_status, worker)
    license_texts = _strings(
        worker.get("license_text"),
        worker.get("license_code"),
        seed.get("license_text"),
        seed.get("license_code"),
    )
    license_urls = _strings(worker.get("license_urls"), seed.get("license_urls"))
    license_raw = " ".join(_strings(*license_texts, *license_urls)) or None
    individual_decisions = [
        decide_license(value, evidence_source="scielo_jats")
        for value in (*license_texts, *license_urls)
    ]
    # A permissive URL must not mask an explicit NC/ND/SA source field.
    # Otherwise use the complete retained record so generic attribution prose
    # can be resolved by an accompanying exact Creative Commons URL.
    decision = next(
        (item for item in individual_decisions if item.reason == "restrictive_license"),
        decide_license(
            license_raw,
            evidence_url=license_urls[0] if license_urls else None,
            evidence_source="scielo_jats",
        ),
    )
    evidence_codes = [
        item.normalized_code
        for item in individual_decisions
        if item.allowed and item.normalized_code
    ]
    license_evidence_conflict = not license_codes_agree(evidence_codes)
    if status in {"complete", "partial"} and not decision.allowed:
        status, reason_code, retryable = "rejected", decision.reason, False
    figures = [item for item in worker.get("figures") or [] if isinstance(item, dict)]
    package_members = [str(value) for value in worker.get("package_members") or [] if value]
    if not package_members:
        package_members = [
            str(value) for value in (worker.get("xml_member"), worker.get("source_member")) if value
        ]
        package_members.extend(
            str(item["member"])
            for item in figures
            if item.get("status") == "ok" and item.get("member")
        )
    expected = _int(worker.get("expected_figure_files"))
    if expected is None:
        expected = len(figures)
    downloaded = _int(worker.get("downloaded_figure_files"))
    if downloaded is None:
        downloaded = sum(item.get("status") == "ok" for item in figures)
    quality_flags = list(worker.get("quality_flags") or [])
    if status not in {"complete", "rejected"}:
        quality_flags.append(reason_code or source_status)
    if worker.get("third_party_caption_flag"):
        quality_flags.append("third_party_caption_terms_detected")
    retry_final = worker.get("retry_final_status")
    if retry_final:
        quality_flags.append(f"retry_final_status:{retry_final}")
    identifiers = {
        key: str(value)
        for key, value in {
            "pid": _prefer(worker, seed, "pid"),
            "collection": _prefer(worker, seed, "collection"),
            "articlemeta_row_index": _prefer(worker, seed, "articlemeta_row_index"),
        }.items()
        if value not in (None, "")
    }
    hashes = {
        str(key): str(value)
        for key, value in dict(worker.get("hashes") or {}).items()
    }
    if not hashes and worker.get("xml_sha256"):
        hashes = {"xml_sha256": str(worker["xml_sha256"])}
    source_package_hash = worker.get("source_package_hash") or None
    total_bytes = _int(worker.get("total_bytes"))
    file_count = _int(worker.get("file_count")) or len(package_members)
    if source_package_hash:
        if set(hashes) != set(package_members):
            raise ValueError(
                f"{source_id}: package member/hash keys differ"
            )
        if file_count != len(package_members):
            raise ValueError(
                f"{source_id}: package file count differs from members"
            )
        if total_bytes is None:
            raise ValueError(
                f"{source_id}: hashed package is missing total_bytes"
            )
    evidence = {"seed_record": seed, "worker_record": worker}
    record_json = _canonical_json(evidence)
    source_url = _prefer(worker, seed, "fulltext_html_url") or worker.get("xml_url")
    normalized = {
        "source": "scielo",
        "source_id": source_id,
        "status": status,
        "reason_code": reason_code,
        "retryable": retryable,
        "source_status": source_status,
        "license_code": decision.normalized_code,
        "license_raw": license_raw,
        "license_policy": decision.policy_version,
        "license_urls": license_urls,
        "license_evidence": decision.to_dict(),
        "doi": _prefer(worker, seed, "doi") or None,
        "title": _prefer(worker, seed, "article_title") or None,
        "language": _prefer(worker, seed, "preferred_lang") or None,
        "publication_year": _publication_year(_prefer(worker, seed, "publication_year")),
        "article_type": _prefer(worker, seed, "document_type") or "journal_article",
        "source_url": source_url or None,
        "source_record_id": str(_prefer(worker, seed, "pid") or source_id),
        "source_updated_at": _prefer(worker, seed, "processing_date") or None,
        "source_identifiers": identifiers,
        "shard": _int(worker.get("shard")),
        "sub": _int(worker.get("subtar")),
        "tar_path": worker.get("tar_path") or None,
        "xml_member": worker.get("xml_member") or None,
        "source_member": worker.get("source_member") or None,
        "package_members": package_members,
        "expected_media_count": expected,
        "downloaded_media_count": downloaded,
        "missing_media_count": max(0, expected - downloaded),
        "total_bytes": total_bytes,
        "file_count": file_count,
        "source_record_hash": hashlib.sha256(record_json.encode()).hexdigest(),
        "source_package_hash": source_package_hash,
        "hashes": hashes,
        "fetched_at": worker.get("fetched_at") or None,
        "quality_flags": sorted(set(quality_flags)),
        "source_meta_json": json.dumps(evidence, ensure_ascii=False, sort_keys=True),
    }
    if normalized["status"] in {"complete", "partial"} and (
        license_evidence_conflict or not manifest_row_is_convertible(normalized)
    ):
        normalized["status"] = "quarantined"
        normalized["reason_code"] = "license_evidence_inconsistent"
        normalized["retryable"] = False
        normalized["quality_flags"] = sorted(
            set([*normalized["quality_flags"], "license_evidence_inconsistent"])
        )
    return normalized


def _update_package_digest(digest: Any, member: str, data: bytes) -> None:
    member_bytes = member.encode("utf-8")
    digest.update(len(member_bytes).to_bytes(8, "big"))
    digest.update(member_bytes)
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def backfill_package_provenance(
    rows: list[dict[str, Any]],
    corpus: Path,
) -> int:
    """Recover exact admitted-package hashes from one or more historical tars.

    License admission has already run in ``normalize_record``. Rejected and
    quarantined package members are never selected for extraction here. The
    package identity follows the canonical ``package_members`` order rather
    than incidental physical tar order. The latter is retained as provenance.
    """

    grouped: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        if not manifest_row_is_convertible(row):
            continue
        members = [str(value) for value in row.get("package_members") or []]
        if not members or row.get("source_package_hash"):
            continue
        tar_path = str(row.get("tar_path") or "")
        if not tar_path:
            raise ValueError(
                f"{row.get('source_id')}: package members without tar_path"
            )
        grouped[tar_path].append(index)

    enriched = 0
    for tar_rel in sorted(grouped):
        indexes = grouped[tar_rel]
        member_owner: dict[str, int] = {}
        package_root_owner: dict[str, int] = {}
        for index in indexes:
            expected = [
                str(value) for value in rows[index].get("package_members") or []
            ]
            package_root = expected[0].split("/", 1)[0]
            if not package_root or any(
                not member.startswith(f"{package_root}/")
                for member in expected
            ):
                raise ValueError(
                    f"{rows[index].get('source_id')}: package members do not "
                    "share one archive root"
                )
            if package_root in package_root_owner:
                raise ValueError(
                    f"duplicate package archive root: {package_root}"
                )
            package_root_owner[package_root] = index
            for member in expected:
                if member in member_owner:
                    raise ValueError(
                        f"duplicate package member across rows: {member}"
                    )
                member_owner[member] = index

        archive_path = corpus / tar_rel
        if not archive_path.is_file():
            raise ValueError(f"missing historical package tar: {archive_path}")
        with tarfile.open(archive_path, mode="r") as archive:
            tar_members = archive.getmembers()
            infos_by_name: dict[str, list[tarfile.TarInfo]] = defaultdict(list)
            for info in tar_members:
                if info.isfile() and info.name in member_owner:
                    infos_by_name[info.name].append(info)

            for index in indexes:
                row = rows[index]
                expected = [
                    str(value) for value in row.get("package_members") or []
                ]
                observed = [
                    info.name
                    for info in tar_members
                    if info.isfile()
                    and package_root_owner.get(info.name.split("/", 1)[0])
                    == index
                ]
                unexpected = sorted(set(observed).difference(expected))
                if unexpected:
                    raise ValueError(
                        f"{row.get('source_id')}: unmanifested tar members: "
                        f"{unexpected!r}"
                    )
                hashes: dict[str, str] = {}
                total_bytes = 0
                digest = hashlib.sha256()
                identical_duplicates = 0
                for member in expected:
                    infos = infos_by_name.get(member) or []
                    if not infos:
                        raise ValueError(
                            f"{row.get('source_id')}: missing tar member: "
                            f"{tar_rel}:{member}"
                        )
                    source = archive.extractfile(infos[0])
                    if source is None:
                        raise ValueError(
                            f"unreadable package member: {tar_rel}:{member}"
                        )
                    data = source.read()
                    for duplicate in infos[1:]:
                        duplicate_source = archive.extractfile(duplicate)
                        if duplicate_source is None:
                            raise ValueError(
                                f"unreadable duplicate package member: "
                                f"{tar_rel}:{member}"
                            )
                        if duplicate_source.read() != data:
                            raise ValueError(
                                f"{row.get('source_id')}: duplicate tar "
                                f"member has different bytes: {tar_rel}:{member}"
                            )
                        identical_duplicates += 1
                    hashes[member] = hashlib.sha256(data).hexdigest()
                    total_bytes += len(data)
                    _update_package_digest(digest, member, data)

                row["hashes"] = hashes
                row["total_bytes"] = total_bytes
                row["file_count"] = len(expected)
                row["source_package_hash"] = digest.hexdigest()
                source_meta = json.loads(row.get("source_meta_json") or "{}")
                source_meta["package_provenance_backfill"] = {
                    "method": "canonical_manifest_order_tar_member_hash_v2",
                    "package_hash_order": "manifest_package_members",
                    "historical_tar_member_order": observed,
                    "historical_tar_member_order_differed": observed != expected,
                    "identical_duplicate_tar_members_ignored": (
                        identical_duplicates
                    ),
                }
                row["source_meta_json"] = json.dumps(
                    source_meta, ensure_ascii=False, sort_keys=True
                )
                enriched += 1
    return enriched


def _seed_database(seed_path: Path, database_path: Path) -> sqlite3.Connection:
    database_path.unlink(missing_ok=True)
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("CREATE TABLE seed (source_id TEXT PRIMARY KEY, record_json TEXT NOT NULL)")
    connection.execute("BEGIN")
    try:
        for row in read_jsonl(seed_path):
            source_id = str(row.get("source_id") or "")
            if not source_id:
                raise ValueError(f"seed row without source_id in {seed_path}")
            connection.execute(
                "INSERT INTO seed VALUES (?, ?)",
                (source_id, _canonical_json(row)),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        connection.close()
        database_path.unlink(missing_ok=True)
        raise
    return connection


def load_rows(
    paths: Iterable[Path],
    *,
    seed_path: Path | None = None,
    seed_database_path: Path | None = None,
    corpus: Path | None = None,
    backfill_provenance: bool = False,
) -> Iterator[dict[str, Any]]:
    connection = None
    if seed_path is not None:
        if seed_database_path is None:
            raise ValueError("seed_database_path is required when seed_path is set")
        connection = _seed_database(seed_path, seed_database_path)
    seen: set[str] = set()
    try:
        for path in paths:
            normalized_rows: list[dict[str, Any]] = []
            for worker in read_jsonl(path):
                source_id = str(worker.get("source_id") or "")
                if not source_id:
                    raise ValueError(f"worker row without source_id in {path}")
                if source_id in seen:
                    raise ValueError(f"duplicate final source_id {source_id!r} in {path}")
                seen.add(source_id)
                seed = None
                if connection is not None:
                    result = connection.execute(
                        "SELECT record_json FROM seed WHERE source_id = ?", (source_id,)
                    ).fetchone()
                    if result is None:
                        raise ValueError(
                            f"final source_id {source_id!r} missing from seed manifest"
                        )
                    seed = json.loads(result[0])
                normalized_rows.append(normalize_record(worker, seed))
            if backfill_provenance:
                if corpus is None:
                    raise ValueError(
                        "corpus is required when backfill_provenance is enabled"
                    )
                backfill_package_provenance(
                    normalized_rows,
                    corpus,
                )
            yield from normalized_rows
    finally:
        if connection is not None:
            connection.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    parser.add_argument("--output")
    parser.add_argument("--seed-manifest")
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument(
        "--backfill-package-provenance",
        action="store_true",
        help=(
            "stream admitted historical package members once to recover exact "
            "member hashes, byte totals, and ordered package hashes"
        ),
    )
    args = parser.parse_args()
    corpus = Path(args.corpus_root)
    output = Path(args.output) if args.output else corpus / "manifest.parquet"
    seed_path = (
        Path(args.seed_manifest) if args.seed_manifest else corpus / "index" / "manifest_seed.jsonl"
    )
    paths = sorted((corpus / "manifests").glob("shard-*/sub-*.jsonl"))
    if not paths:
        sys.stderr.write(f"ERROR: no sub manifest JSONL files under {corpus / 'manifests'}\n")
        return 0 if args.allow_empty else 2
    if not seed_path.is_file():
        sys.stderr.write(f"ERROR: seed manifest missing: {seed_path}\n")
        return 2
    seed_database_path = output.parent / ".scielo-seed-manifest.sqlite.tmp"
    counts: Counter[str] = Counter()

    def observed_rows() -> Iterator[dict[str, Any]]:
        for row in load_rows(
            paths,
            seed_path=seed_path,
            seed_database_path=seed_database_path,
            corpus=corpus,
            backfill_provenance=args.backfill_package_provenance,
        ):
            counts[row["status"]] += 1
            yield row

    try:
        row_count = write_manifest_parquet_stream(observed_rows(), output)
    except (ValueError, sqlite3.IntegrityError) as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2
    finally:
        seed_database_path.unlink(missing_ok=True)
    if not row_count and not args.allow_empty:
        sys.stderr.write("ERROR: no manifest rows found\n")
        return 2
    summary = {
        "created_at": iso_utc_now(),
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "corpus_root": str(corpus),
        "seed_manifest": str(seed_path),
        "input_sub_manifests": len(paths),
        "package_provenance_backfilled": args.backfill_package_provenance,
        "rows": row_count,
        "status_counts": dict(sorted(counts.items())),
        "output": str(output),
        "output_sha256": _sha256(output),
    }
    atomic_write_json(output.with_suffix(output.suffix + ".summary.json"), summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
