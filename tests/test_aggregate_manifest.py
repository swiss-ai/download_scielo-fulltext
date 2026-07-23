from __future__ import annotations

import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

import pyarrow.parquet as pq
from docgraph import validate_manifest_rows

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from aggregate_manifest import (  # noqa: E402
    backfill_package_provenance,
    load_rows,
    normalize_record,
)
from backfill_manifest_provenance import bind_backfill_attribution  # noqa: E402
from backfill_manifest_provenance_bounded import (  # noqa: E402
    backfill_manifest_bounded,
)


def _seed(**updates):
    row = {
        "source": "scielo",
        "source_id": "scielo-scl-S0001",
        "status": "planned_xml",
        "pid": "S0001",
        "collection": "scl",
        "doi": "10.1234/example",
        "document_type": "research-article",
        "publication_year": "2020",
        "processing_date": "2021-01-02",
        "preferred_lang": "es",
        "fulltext_html_url": "https://example.test/article",
        "fulltexts": {"es": "https://example.test/article"},
        "articlemeta_row_index": 42,
    }
    row.update(updates)
    return row


def _worker(**updates):
    row = {
        "source": "scielo",
        "source_id": "scielo-scl-S0001",
        "status": "no_figures",
        "pid": "S0001",
        "collection": "scl",
        "shard": "01",
        "subtar": "002",
        "tar_path": "data/shard-01/sub-002.tar",
        "xml_member": "scielo-scl-S0001/article.xml",
        "source_member": "scielo-scl-S0001/source.json",
        "license_code": "CC BY",
        "license_text": "Creative Commons Attribution 4.0",
        "license_urls": ["https://creativecommons.org/licenses/by/4.0/"],
        "expected_figure_files": 0,
        "downloaded_figure_files": 0,
        "figures": [],
    }
    row.update(updates)
    return row


def test_normalize_record_enriches_worker_from_seed():
    row = normalize_record(_worker(), _seed())
    assert validate_manifest_rows([row], expected_source="scielo") == []
    assert row["status"] == "complete"
    assert row["publication_year"] == 2020
    assert row["language"] == "es"
    assert row["package_members"] == [
        "scielo-scl-S0001/article.xml",
        "scielo-scl-S0001/source.json",
    ]
    evidence = json.loads(row["source_meta_json"])
    assert evidence["seed_record"]["fulltexts"]["es"] == "https://example.test/article"


def _package_hash(files: list[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for member, data in files:
        member_bytes = member.encode("utf-8")
        digest.update(len(member_bytes).to_bytes(8, "big"))
        digest.update(member_bytes)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def test_normalize_record_preserves_complete_package_provenance():
    files = [
        ("scielo-scl-S0001/article.xml", b"<article/>"),
        ("scielo-scl-S0001/source.json", b"{}\n"),
    ]
    row = normalize_record(
        _worker(
            package_members=[member for member, _data in files],
            file_count=len(files),
            total_bytes=sum(len(data) for _member, data in files),
            hashes={
                member: hashlib.sha256(data).hexdigest()
                for member, data in files
            },
            source_package_hash=_package_hash(files),
        ),
        _seed(),
    )

    assert row["source_package_hash"] == _package_hash(files)
    assert row["hashes"] == {
        member: hashlib.sha256(data).hexdigest()
        for member, data in files
    }
    assert row["total_bytes"] == sum(len(data) for _member, data in files)


def test_normalize_record_rejects_incomplete_package_hash_map():
    try:
        normalize_record(
            _worker(
                package_members=[
                    "scielo-scl-S0001/article.xml",
                    "scielo-scl-S0001/source.json",
                ],
                file_count=2,
                total_bytes=12,
                hashes={"scielo-scl-S0001/article.xml": "a" * 64},
                source_package_hash="b" * 64,
            ),
            _seed(),
        )
    except ValueError as error:
        assert "package member/hash keys differ" in str(error)
    else:
        raise AssertionError("incomplete package hash map was accepted")


def test_backfill_package_provenance_hashes_admitted_exact_members(tmp_path):
    files = [
        ("scielo-scl-S0001/article.xml", b"<article/>"),
        ("scielo-scl-S0001/source.json", b'{"source":"scielo"}\n'),
        ("scielo-scl-S0001/figure-000.jpg", b"\xff\xd8figure\xff\xd9"),
    ]
    tar_path = tmp_path / "data/shard-01/sub-002.tar"
    tar_path.parent.mkdir(parents=True)
    with tarfile.open(tar_path, "w") as archive:
        for member, data in files:
            info = tarfile.TarInfo(member)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    row = normalize_record(
        _worker(
            package_members=[member for member, _data in files],
            figures=[
                {
                    "status": "ok",
                    "member": "scielo-scl-S0001/figure-000.jpg",
                }
            ],
            expected_figure_files=1,
            downloaded_figure_files=1,
        ),
        _seed(),
    )

    assert backfill_package_provenance([row], tmp_path) == 1
    assert row["source_package_hash"] == _package_hash(files)
    assert row["hashes"] == {
        member: hashlib.sha256(data).hexdigest()
        for member, data in files
    }
    assert row["file_count"] == 3
    assert row["total_bytes"] == sum(len(data) for _member, data in files)
    evidence = json.loads(row["source_meta_json"])
    assert evidence["package_provenance_backfill"]["method"] == (
        "canonical_manifest_order_tar_member_hash_v2"
    )
    assert evidence["package_provenance_backfill"]["package_hash_order"] == (
        "manifest_package_members"
    )
    assert evidence["package_provenance_backfill"][
        "historical_tar_member_order"
    ] == [member for member, _data in files]
    assert evidence["package_provenance_backfill"][
        "historical_tar_member_order_differed"
    ] is False
    bind_backfill_attribution(
        [row],
        historical_package_producer_attribution="historical-commit-unavailable",
        input_manifest_commit="manifest-commit",
        input_manifest_sha256="a" * 64,
        enrichment_commit="enrichment-commit",
    )
    evidence = json.loads(row["source_meta_json"])[
        "package_provenance_backfill"
    ]
    assert evidence["historical_package_producer_attribution"] == (
        "historical-commit-unavailable"
    )
    assert evidence["input_manifest_commit"] == "manifest-commit"
    assert evidence["input_manifest_sha256"] == "a" * 64
    assert evidence["enrichment_commit"] == "enrichment-commit"


def test_bounded_backfill_preserves_rejected_rows_and_enriches_admitted(tmp_path):
    accepted_files = [
        ("scielo-scl-S0001/article.xml", b"<article/>"),
        ("scielo-scl-S0001/source.json", b'{"source":"scielo"}\n'),
    ]
    rejected_files = [
        ("scielo-scl-S0002/article.xml", b"<article/>"),
        ("scielo-scl-S0002/source.json", b'{"source":"scielo"}\n'),
    ]
    tar_path = tmp_path / "corpus/data/shard-01/sub-002.tar"
    tar_path.parent.mkdir(parents=True)
    with tarfile.open(tar_path, "w") as archive:
        for member, data in [
            accepted_files[1],
            accepted_files[0],
            *rejected_files,
        ]:
            info = tarfile.TarInfo(member)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))

    accepted = normalize_record(
        _worker(
            package_members=[member for member, _data in accepted_files],
        ),
        _seed(),
    )
    rejected = normalize_record(
        _worker(
            source_id="scielo-scl-S0002",
            pid="S0002",
            package_members=[member for member, _data in rejected_files],
            xml_member=rejected_files[0][0],
            source_member=rejected_files[1][0],
            license_code="CC BY-NC",
            license_text="Creative Commons Attribution-NonCommercial 4.0",
            license_urls=[
                "https://creativecommons.org/licenses/by-nc/4.0/"
            ],
        ),
        _seed(source_id="scielo-scl-S0002", pid="S0002"),
    )
    assert rejected["status"] == "rejected"
    from docgraph import write_manifest_parquet

    input_path = tmp_path / "input.parquet"
    output_path = tmp_path / "output.parquet"
    write_manifest_parquet([rejected, accepted], input_path)
    rejected_json = json.dumps(
        next(
            row
            for row in pq.read_table(input_path).to_pylist()
            if row["source_id"] == "scielo-scl-S0002"
        ),
        ensure_ascii=False,
        sort_keys=True,
    )
    summary = backfill_manifest_bounded(
        input_path=input_path,
        output_path=output_path,
        corpus_root=tmp_path / "corpus",
        database_path=tmp_path / "backfill.sqlite",
        historical_package_producer_attribution="historical-unavailable",
        input_manifest_commit="manifest-commit",
        enrichment_commit="enrichment-commit",
        batch_size=1,
    )

    rows = {
        row["source_id"]: row for row in pq.read_table(output_path).to_pylist()
    }
    admitted = rows["scielo-scl-S0001"]
    assert admitted["source_package_hash"] == _package_hash(accepted_files)
    assert set(dict(admitted["hashes"])) == {
        member for member, _data in accepted_files
    }
    evidence = json.loads(admitted["source_meta_json"])[
        "package_provenance_backfill"
    ]
    assert evidence["historical_tar_member_order"] == [
        accepted_files[1][0],
        accepted_files[0][0],
    ]
    assert evidence["historical_tar_member_order_differed"] is True
    assert evidence["input_manifest_commit"] == "manifest-commit"
    assert evidence["enrichment_commit"] == "enrichment-commit"
    assert (
        json.dumps(
            rows["scielo-scl-S0002"],
            ensure_ascii=False,
            sort_keys=True,
        )
        == rejected_json
    )
    assert summary["rows"] == 2
    assert summary["admitted_rows"] == 1
    assert summary["admitted_rows_enriched"] == 1
    assert summary["nonadmitted_rows_unchanged"] is True
    assert summary["historical_tar_order_differed_rows"] == 1


def test_backfill_package_hash_uses_canonical_not_physical_tar_order(tmp_path):
    canonical_files = [
        ("scielo-scl-S0001/article.xml", b"<article/>"),
        ("scielo-scl-S0001/source.json", b'{"source":"scielo"}\n'),
        ("scielo-scl-S0001/figure-000.jpg", b"\xff\xd8figure\xff\xd9"),
    ]
    physical_files = [canonical_files[1], canonical_files[0], canonical_files[2]]
    tar_path = tmp_path / "data/shard-01/sub-002.tar"
    tar_path.parent.mkdir(parents=True)
    with tarfile.open(tar_path, "w") as archive:
        for member, data in physical_files:
            info = tarfile.TarInfo(member)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    row = normalize_record(
        _worker(
            package_members=[member for member, _data in canonical_files],
            figures=[
                {
                    "status": "ok",
                    "member": "scielo-scl-S0001/figure-000.jpg",
                }
            ],
            expected_figure_files=1,
            downloaded_figure_files=1,
        ),
        _seed(),
    )

    assert backfill_package_provenance([row], tmp_path) == 1
    assert row["source_package_hash"] == _package_hash(canonical_files)
    evidence = json.loads(row["source_meta_json"])[
        "package_provenance_backfill"
    ]
    assert evidence["historical_tar_member_order"] == [
        member for member, _data in physical_files
    ]
    assert evidence["historical_tar_member_order_differed"] is True


def test_backfill_rejects_unmanifested_file_in_package_root(tmp_path):
    files = [
        ("scielo-scl-S0001/article.xml", b"<article/>"),
        ("scielo-scl-S0001/source.json", b'{"source":"scielo"}\n'),
        ("scielo-scl-S0001/unmanifested.bin", b"hidden"),
    ]
    tar_path = tmp_path / "data/shard-01/sub-002.tar"
    tar_path.parent.mkdir(parents=True)
    with tarfile.open(tar_path, "w") as archive:
        for member, data in files:
            info = tarfile.TarInfo(member)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    row = normalize_record(_worker(), _seed())

    try:
        backfill_package_provenance([row], tmp_path)
    except ValueError as error:
        assert "unmanifested tar members" in str(error)
        assert "unmanifested.bin" in str(error)
    else:
        raise AssertionError("unmanifested package file was accepted")


def test_backfill_never_extracts_rejected_package(tmp_path):
    row = normalize_record(
        _worker(
            license_code="CC BY-NC 4.0",
            license_text="Creative Commons Attribution NonCommercial 4.0",
            package_members=[
                "scielo-scl-S0001/article.xml",
                "scielo-scl-S0001/source.json",
            ],
        ),
        _seed(),
    )

    assert row["status"] == "rejected"
    assert backfill_package_provenance([row], tmp_path) == 0
    assert row["source_package_hash"] is None


def test_complete_package_with_restrictive_license_is_rejected():
    row = normalize_record(
        _worker(
            license_code="CC BY-NC 4.0",
            license_text="Creative Commons Attribution NonCommercial 4.0",
            license_urls=[],
        ),
        _seed(),
    )

    assert row["status"] == "rejected"
    assert row["reason_code"] == "restrictive_license"
    assert row["license_evidence"]["allowed"] is False


def test_generic_attribution_prose_uses_exact_cc_url():
    row = normalize_record(
        _worker(
            license_code=None,
            license_text=(
                "This is an open-access article distributed under the terms of "
                "the Creative Commons Attribution License"
            ),
            license_urls=["https://creativecommons.org/licenses/by/4.0/"],
        ),
        _seed(),
    )

    assert row["status"] == "complete"
    assert row["license_code"] == "CC-BY-4.0"
    assert row["license_urls"] == ["https://creativecommons.org/licenses/by/4.0/"]


def test_permissive_url_does_not_mask_restrictive_text():
    row = normalize_record(
        _worker(
            license_code=None,
            license_text="CC BY 4.0 nd",
            license_urls=["https://creativecommons.org/licenses/by/4.0/"],
        ),
        _seed(),
    )

    assert row["status"] == "rejected"
    assert row["reason_code"] == "restrictive_license"
    assert row["license_code"] == "CC-BY-ND-4.0"


def test_worker_and_seed_license_urls_are_both_retained_and_checked():
    row = normalize_record(
        _worker(license_urls=["https://creativecommons.org/licenses/by/4.0/"]),
        _seed(license_urls=["https://creativecommons.org/licenses/by/3.0/"]),
    )

    assert row["status"] == "quarantined"
    assert row["reason_code"] == "license_evidence_inconsistent"
    assert row["license_urls"] == [
        "https://creativecommons.org/licenses/by/4.0/",
        "https://creativecommons.org/licenses/by/3.0/",
    ]


def test_embedded_attribution_version_conflict_is_quarantined():
    row = normalize_record(
        _worker(
            license_code=None,
            license_text=(
                "This is an Open Access article distributed under the terms of "
                "the Creative Commons Attribution 4.0 international License."
            ),
            license_urls=["http://creativecommons.org/licenses/by/3.0/"],
        ),
        _seed(),
    )

    assert row["status"] == "quarantined"
    assert row["reason_code"] == "license_evidence_inconsistent"


def test_nonexistent_cc_version_is_not_admitted():
    row = normalize_record(
        _worker(
            license_code=None,
            license_text="Este é um artigo publicado sob uma licença Creative Commons",
            license_urls=["https://creativecommons.org/licenses/by/40/"],
        ),
        _seed(),
    )

    assert row["status"] == "rejected"
    assert row["reason_code"] == "unknown_license"


def test_complete_package_with_conflicting_permissive_versions_is_quarantined():
    row = normalize_record(
        _worker(
            license_code="CC BY 4.0",
            license_text="Creative Commons Attribution 4.0",
            license_urls=["https://creativecommons.org/licenses/by/3.0/"],
        ),
        _seed(),
    )

    assert row["status"] == "quarantined"
    assert row["reason_code"] == "license_evidence_inconsistent"
    assert row["retryable"] is False
    assert "license_evidence_inconsistent" in row["quality_flags"]


def test_load_rows_joins_seed_and_preserves_retry_history(tmp_path):
    seed_path = tmp_path / "seed.jsonl"
    worker_path = tmp_path / "sub-000.jsonl"
    seed_path.write_text(json.dumps(_seed()) + "\n")
    worker_path.write_text(
        json.dumps(
            _worker(
                status="retry_blocked_xml_http_502",
                retry_blocked=True,
                retry_final_status="xml_http_502",
                retry_history=[{"attempt": 1, "status": "xml_http_502"}],
                license_code=None,
                license_text=None,
                license_urls=[],
                xml_member=None,
                source_member=None,
            )
        )
        + "\n"
    )
    rows = list(
        load_rows(
            [worker_path],
            seed_path=seed_path,
            seed_database_path=tmp_path / "seed.sqlite",
        )
    )
    assert rows[0]["status"] == "failed"
    assert rows[0]["reason_code"] == "retry_exhausted"
    assert "retry_final_status:xml_http_502" in rows[0]["quality_flags"]
