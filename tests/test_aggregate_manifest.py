from __future__ import annotations

import json
import sys
from pathlib import Path

from docgraph import validate_manifest_rows

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from aggregate_manifest import load_rows, normalize_record  # noqa: E402


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
