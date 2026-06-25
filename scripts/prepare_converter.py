#!/usr/bin/env python3
"""Prepare converter-facing SciELO manifests and accounting artifacts."""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import tarfile
from pathlib import Path
from typing import Iterable

from common import atomic_write_json, compact_json, iso_utc_now, read_jsonl


ACCEPTED_STATUSES = {"ok", "no_figures"}
ALLOWED_LICENSE_CODES = {"CC BY", "CC0", "PD"}
JSON_FIELDS = {
    "license_urls",
    "missing_figure_urls",
    "third_party_caption_terms",
    "figures",
    "fulltexts",
}
SEED_FIELDS = (
    "document_type",
    "publication_year",
    "publication_date",
    "processing_date",
    "preferred_lang",
    "fulltext_html_url",
    "fulltexts",
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def iter_manifest(path: Path) -> Iterable[dict]:
    yield from read_jsonl(path)


def load_seed(seed_path: Path) -> dict[str, dict]:
    if not seed_path.is_file():
        return {}
    out = {}
    for row in read_jsonl(seed_path):
        sid = row.get("source_id")
        if isinstance(sid, str) and sid:
            out[sid] = {k: row.get(k) for k in SEED_FIELDS if k in row}
    return out


def enrich_row(row: dict, seed_rows: dict[str, dict]) -> dict:
    enriched = dict(row)
    seed = seed_rows.get(row.get("source_id") or "")
    if seed:
        for key, value in seed.items():
            if key not in enriched or enriched.get(key) in (None, ""):
                enriched[key] = value
    return enriched


def is_converter_accepted(row: dict) -> bool:
    return (
        row.get("status") in ACCEPTED_STATUSES
        and row.get("license_policy") == "keep"
        and row.get("license_code") in ALLOWED_LICENSE_CODES
        and bool(row.get("tar_path"))
        and bool(row.get("xml_member"))
        and bool(row.get("source_member"))
    )


def ok_figures(row: dict) -> list[dict]:
    return [f for f in row.get("figures") or [] if f.get("status") == "ok" and f.get("member")]


def json_value(value) -> str:
    return compact_json(value if value is not None else [])


def int_value(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def flat_row(row: dict) -> dict:
    figures = ok_figures(row)
    out = {
        "source": row.get("source") or "scielo",
        "source_id": row.get("source_id") or "",
        "pid": row.get("pid") or "",
        "collection": row.get("collection") or "",
        "doi": row.get("doi") or row.get("xml_doi") or "",
        "xml_doi": row.get("xml_doi") or "",
        "status": row.get("status") or "",
        "license_code": row.get("license_code") or "",
        "license_policy": row.get("license_policy") or "",
        "license_text": row.get("license_text") or "",
        "tar_path": row.get("tar_path") or "",
        "xml_member": row.get("xml_member") or "",
        "source_member": row.get("source_member") or "",
        "xml_url": row.get("xml_url") or "",
        "article_title": row.get("article_title") or "",
        "journal_title": row.get("journal_title") or "",
        "document_type": row.get("document_type") or "",
        "publication_year": row.get("publication_year") or "",
        "publication_date": row.get("publication_date") or "",
        "processing_date": row.get("processing_date") or "",
        "preferred_lang": row.get("preferred_lang") or "",
        "fulltext_html_url": row.get("fulltext_html_url") or "",
        "shard": str(row.get("shard") or ""),
        "subtar": str(row.get("subtar") or ""),
        "fetched_at": row.get("fetched_at") or "",
        "xml_bytes": int_value(row.get("xml_bytes")),
        "text_chars": int_value(row.get("text_chars")),
        "body_text_chars": int_value(row.get("body_text_chars")),
        "figure_count": int_value(row.get("figure_count")),
        "table_count": int_value(row.get("table_count")),
        "formula_count": int_value(row.get("formula_count")),
        "expected_figure_files": int_value(row.get("expected_figure_files")),
        "downloaded_figure_files": int_value(row.get("downloaded_figure_files")),
        "figure_bytes": int_value(row.get("figure_bytes")),
        "original_figure_bytes": int_value(row.get("original_figure_bytes") or row.get("figure_bytes")),
        "rendered_figure_files": int_value(row.get("rendered_figure_files")),
        "third_party_caption_flag": bool(row.get("third_party_caption_flag")),
        "xml_sha256": row.get("xml_sha256") or "",
        "figure_members_json": json_value([f.get("member") for f in figures]),
        "figure_urls_json": json_value([f.get("url") for f in figures]),
    }
    for key in JSON_FIELDS:
        out[f"{key}_json"] = json_value(row.get(key))
    return out


def accepted_members(row: dict) -> list[tuple[str, str]]:
    members = [("xml", row["xml_member"]), ("source", row["source_member"])]
    for fig in ok_figures(row):
        members.append(("figure", fig["member"]))
    return members


def scan_tar_members(
    corpus: Path,
    accepted_rows: list[dict],
    members_path: Path,
    tar_inventory_path: Path,
    strict: bool,
) -> dict:
    rows_by_tar: dict[str, list[dict]] = collections.defaultdict(list)
    for row in accepted_rows:
        rows_by_tar[row["tar_path"]].append(row)

    totals = collections.Counter()
    missing: list[dict] = []
    with members_path.open("w", encoding="utf-8") as members_out, tar_inventory_path.open("w", encoding="utf-8") as tar_out:
        for tar_rel in sorted(rows_by_tar):
            tar_path = corpus / tar_rel
            tar_size = tar_path.stat().st_size if tar_path.is_file() else 0
            if not tar_path.is_file():
                missing.append({"tar_path": tar_rel, "member": None, "error": "missing_tar"})
                if strict:
                    continue
                member_sizes = {}
            else:
                with tarfile.open(tar_path, "r") as tar:
                    member_sizes = {m.name: int(m.size or 0) for m in tar.getmembers() if m.isfile()}

            tar_counts = collections.Counter()
            for row in rows_by_tar[tar_rel]:
                source_id = row.get("source_id") or ""
                tar_counts["accepted_rows"] += 1
                for kind, member in accepted_members(row):
                    size = member_sizes.get(member)
                    if size is None:
                        missing.append({"tar_path": tar_rel, "member": member, "source_id": source_id, "error": "missing_member"})
                        size = 0
                    record = {
                        "source_id": source_id,
                        "kind": kind,
                        "tar_path": tar_rel,
                        "member": member,
                        "size_bytes": size,
                    }
                    members_out.write(compact_json(record) + "\n")
                    tar_counts["accepted_members"] += 1
                    tar_counts[f"accepted_{kind}_members"] += 1
                    tar_counts["accepted_payload_bytes"] += size
                    tar_counts[f"accepted_{kind}_bytes"] += size
            tar_record = {
                "tar_path": tar_rel,
                "tar_size_bytes": tar_size,
                **dict(tar_counts),
            }
            tar_out.write(compact_json(tar_record) + "\n")
            totals.update(tar_counts)
            totals["tar_files"] += 1
            totals["tar_size_bytes"] += tar_size

    if missing and strict:
        raise RuntimeError(f"missing accepted tar members: {missing[:10]}")
    return {"member_totals": dict(totals), "missing_members": missing[:100], "missing_member_count": len(missing)}


def write_parquet(path: Path, rows: list[dict], require: bool) -> bool:
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001
        if require:
            raise RuntimeError("pyarrow is required to write converter parquet") from exc
        return False
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd")
    return True


def write_readme(path: Path, summary: dict) -> None:
    text = f"""# SciELO Converter Artifacts

Created: {summary["created_at"]}

This directory is converter-facing. It contains only complete accepted rows in
`accepted_manifest.jsonl` and `accepted_manifest.parquet`.

Accepted filter:

- `status` is `ok` or `no_figures`
- `license_policy` is `keep`
- `license_code` is one of `CC BY`, `CC0`, or `PD`
- required tar, XML, and source members are present

Files:

- `accepted_manifest.jsonl`: enriched full accepted manifest rows
- `accepted_manifest.parquet`: flattened converter schema
- `accepted_members.jsonl`: XML/source/figure member inventory and exact tar member sizes
- `tar_inventory.jsonl`: tar-level accepted row/member accounting
- `retry_blocked_manifest.jsonl`: retry-blocked provenance rows excluded from conversion
- `converter_summary.json`: counts, byte totals, status counts, and output paths
- `checksums.sha256`: SHA-256 for converter artifacts in this directory
"""
    path.write_text(text, encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--manifest", default=None)
    p.add_argument("--seed-manifest", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--no-parquet", action="store_true")
    p.add_argument("--require-parquet", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--no-strict-members", action="store_true")
    args = p.parse_args()

    corpus = Path(args.corpus_root)
    manifest_path = Path(args.manifest) if args.manifest else corpus / "manifest.jsonl"
    seed_path = Path(args.seed_manifest) if args.seed_manifest else corpus / "index" / "manifest_seed.jsonl"
    output_dir = Path(args.output_dir) if args.output_dir else corpus / "converter"
    output_dir.mkdir(parents=True, exist_ok=True)

    accepted_path = output_dir / "accepted_manifest.jsonl"
    blocked_path = output_dir / "retry_blocked_manifest.jsonl"
    parquet_path = output_dir / "accepted_manifest.parquet"
    members_path = output_dir / "accepted_members.jsonl"
    tar_inventory_path = output_dir / "tar_inventory.jsonl"
    summary_path = output_dir / "converter_summary.json"
    checksums_path = output_dir / "checksums.sha256"
    readme_path = output_dir / "README.converter.md"

    seed_rows = load_seed(seed_path)
    status_counts = collections.Counter()
    license_counts = collections.Counter()
    accepted_license_counts = collections.Counter()
    blocked_status_counts = collections.Counter()
    accepted_rows: list[dict] = []
    flat_rows: list[dict] = []
    rejected_accepted_like = []
    manifest_rows = 0

    tmp_accepted = accepted_path.with_suffix(".jsonl.tmp")
    tmp_blocked = blocked_path.with_suffix(".jsonl.tmp")
    with tmp_accepted.open("w", encoding="utf-8") as accepted_out, tmp_blocked.open("w", encoding="utf-8") as blocked_out:
        for raw in iter_manifest(manifest_path):
            manifest_rows += 1
            status = raw.get("status") or "unknown"
            status_counts[status] += 1
            license_counts[str(raw.get("license_code") or "NONE")] += 1
            row = enrich_row(raw, seed_rows)
            if is_converter_accepted(row):
                accepted_rows.append(row)
                flat_rows.append(flat_row(row))
                accepted_license_counts[str(row.get("license_code") or "NONE")] += 1
                accepted_out.write(compact_json(row) + "\n")
            elif status in ACCEPTED_STATUSES:
                rejected_accepted_like.append({
                    "source_id": row.get("source_id"),
                    "status": status,
                    "license_code": row.get("license_code"),
                    "license_policy": row.get("license_policy"),
                    "tar_path": row.get("tar_path"),
                    "xml_member": row.get("xml_member"),
                    "source_member": row.get("source_member"),
                })
            if isinstance(status, str) and status.startswith("retry_blocked_"):
                blocked_status_counts[status] += 1
                blocked_out.write(compact_json(row) + "\n")
    tmp_accepted.replace(accepted_path)
    tmp_blocked.replace(blocked_path)

    parquet_written = False
    if not args.no_parquet:
        tmp_parquet = parquet_path.with_suffix(".parquet.tmp")
        parquet_written = write_parquet(tmp_parquet, flat_rows, args.require_parquet)
        if parquet_written:
            tmp_parquet.replace(parquet_path)
    elif parquet_path.exists():
        parquet_path.unlink()

    member_scan = scan_tar_members(
        corpus,
        accepted_rows,
        members_path.with_suffix(".jsonl.tmp"),
        tar_inventory_path.with_suffix(".jsonl.tmp"),
        strict=not args.no_strict_members,
    )
    members_path.with_suffix(".jsonl.tmp").replace(members_path)
    tar_inventory_path.with_suffix(".jsonl.tmp").replace(tar_inventory_path)

    byte_totals = {
        "xml_bytes_manifest": sum(int_value(r.get("xml_bytes")) for r in accepted_rows),
        "figure_bytes_manifest": sum(int_value(r.get("figure_bytes")) for r in accepted_rows),
        "original_figure_bytes_manifest": sum(int_value(r.get("original_figure_bytes") or r.get("figure_bytes")) for r in accepted_rows),
        "expected_figure_files": sum(int_value(r.get("expected_figure_files")) for r in accepted_rows),
        "downloaded_figure_files": sum(int_value(r.get("downloaded_figure_files")) for r in accepted_rows),
        "rendered_figure_files": sum(int_value(r.get("rendered_figure_files")) for r in accepted_rows),
    }
    member_totals = member_scan["member_totals"]
    summary = {
        "created_at": iso_utc_now(),
        "manifest": str(manifest_path),
        "seed_manifest": str(seed_path),
        "output_dir": str(output_dir),
        "manifest_rows": manifest_rows,
        "accepted_rows": len(accepted_rows),
        "accepted_status_counts": dict(collections.Counter(r.get("status") for r in accepted_rows)),
        "accepted_license_counts": dict(accepted_license_counts),
        "status_counts": dict(status_counts),
        "license_counts": dict(license_counts),
        "retry_blocked_status_counts": dict(blocked_status_counts),
        "retry_blocked_rows": sum(blocked_status_counts.values()),
        "rejected_accepted_like_count": len(rejected_accepted_like),
        "rejected_accepted_like_examples": rejected_accepted_like[:20],
        "byte_totals": byte_totals,
        "member_scan": member_scan,
        "volume_gb": {
            "xml_manifest": round(byte_totals["xml_bytes_manifest"] / 1e9, 3),
            "figures_manifest": round(byte_totals["figure_bytes_manifest"] / 1e9, 3),
            "original_figures_manifest": round(byte_totals["original_figure_bytes_manifest"] / 1e9, 3),
            "accepted_payload_exact_members": round(int_value(member_totals.get("accepted_payload_bytes")) / 1e9, 3),
            "tar_files_referenced": int_value(member_totals.get("tar_files")),
            "tar_files_referenced_size": round(int_value(member_totals.get("tar_size_bytes")) / 1e9, 3),
        },
        "outputs": {
            "accepted_manifest_jsonl": str(accepted_path),
            "accepted_manifest_parquet": str(parquet_path) if parquet_written else None,
            "accepted_members_jsonl": str(members_path),
            "tar_inventory_jsonl": str(tar_inventory_path),
            "retry_blocked_manifest_jsonl": str(blocked_path),
            "checksums": str(checksums_path),
            "readme": str(readme_path),
        },
    }
    write_readme(readme_path, summary)
    atomic_write_json(summary_path, summary)

    checksum_files = [accepted_path, members_path, tar_inventory_path, blocked_path, summary_path, readme_path]
    if parquet_written:
        checksum_files.append(parquet_path)
    with checksums_path.open("w", encoding="utf-8") as out:
        for path in sorted(checksum_files):
            out.write(f"{sha256_file(path)}  {path.name}\n")

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
