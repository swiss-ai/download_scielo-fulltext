#!/usr/bin/env python3
"""Estimate full SciELO XML + figure volume from a manifest sample."""
from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from pathlib import Path

from common import read_jsonl


COMPLETE_ACCEPTED = {"ok", "no_figures"}
PACKAGED_INCOMPLETE = {"partial_figures", "figures_failed"}


def mean(vals: list[int]) -> float:
    return statistics.mean(vals) if vals else 0.0


def median(vals: list[int]) -> float:
    return statistics.median(vals) if vals else 0.0


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True)
    p.add_argument("--total-identifiers", type=int, default=1409144)
    args = p.parse_args()

    rows = list(read_jsonl(Path(args.manifest)))
    counts = Counter(r.get("status", "unknown") for r in rows)
    complete = [r for r in rows if r.get("status") in COMPLETE_ACCEPTED]
    incomplete = [r for r in rows if r.get("status") in PACKAGED_INCOMPLETE]
    xml = [int(r.get("xml_bytes") or 0) for r in complete if int(r.get("xml_bytes") or 0) > 0]
    fig_bytes = [int(r.get("figure_bytes") or 0) for r in complete]
    original_fig_bytes = [int(r.get("original_figure_bytes") or r.get("figure_bytes") or 0) for r in complete]
    fig_counts = [int(r.get("expected_figure_files") or 0) for r in complete]
    rendered_counts = [int(r.get("rendered_figure_files") or 0) for r in complete]
    complete_fraction = len(complete) / len(rows) if rows else 0.0
    incomplete_fraction = len(incomplete) / len(rows) if rows else 0.0
    projected_complete = args.total_identifiers * complete_fraction
    projected_incomplete = args.total_identifiers * incomplete_fraction
    out = {
        "sample_rows": len(rows),
        "status_counts": dict(counts),
        "complete_accepted_rows": len(complete),
        "complete_accepted_fraction": complete_fraction,
        "incomplete_packaged_rows": len(incomplete),
        "incomplete_packaged_fraction": incomplete_fraction,
        "projected_complete_accepted_articles": round(projected_complete),
        "projected_incomplete_packaged_articles": round(projected_incomplete),
        "xml_bytes_mean": round(mean(xml)),
        "xml_bytes_median": round(median(xml)),
        "figure_bytes_per_article_mean": round(mean(fig_bytes)),
        "figure_bytes_per_article_median": round(median(fig_bytes)),
        "original_figure_bytes_per_article_mean": round(mean(original_fig_bytes)),
        "original_figure_bytes_per_article_median": round(median(original_fig_bytes)),
        "figures_per_article_mean": mean(fig_counts),
        "figures_per_article_median": median(fig_counts),
        "rendered_figures_per_article_mean": mean(rendered_counts),
        "rendered_figures_per_article_median": median(rendered_counts),
        "projected_xml_gb": round(projected_complete * mean(xml) / 1e9, 2),
        "projected_figure_gb": round(projected_complete * mean(fig_bytes) / 1e9, 2),
        "projected_original_figure_gb": round(projected_complete * mean(original_fig_bytes) / 1e9, 2),
        "projected_total_gb": round(projected_complete * (mean(xml) + mean(fig_bytes)) / 1e9, 2),
        "note": "Projection uses only complete figure-inclusive accepted rows: ok and no_figures. partial_figures and figures_failed are reported separately.",
    }
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
