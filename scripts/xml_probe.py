#!/usr/bin/env python3
"""Tiny XML probe wrapper for an existing manifest seed."""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from common import read_jsonl, write_jsonl


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--limit", type=int, default=20)
    args = p.parse_args()
    corpus = Path(args.corpus_root)
    seed = corpus / "index" / "manifest_seed.jsonl"
    rows = []
    for row in read_jsonl(seed):
        if row.get("status") == "planned_xml":
            rows.append(row)
            if len(rows) >= args.limit:
                break
    out = corpus / "index" / "probe.plan.jsonl"
    write_jsonl(out, rows)
    print(f"wrote {len(rows)} probe rows to {out}")


if __name__ == "__main__":
    main()
