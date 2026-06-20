#!/usr/bin/env python3
"""Aggregate SciELO per-subtar manifests."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import atomic_write_json, iso_utc_now


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--output", default=None)
    args = p.parse_args()
    corpus = Path(args.corpus_root)
    output = Path(args.output) if args.output else corpus / "manifest.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    n = 0
    with tmp.open("w", encoding="utf-8") as out:
        for path in sorted((corpus / "manifests").glob("shard-*/sub-*.jsonl")):
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        out.write(line)
                        n += 1
    tmp.replace(output)
    atomic_write_json(output.with_suffix(".summary.json"), {
        "created_at": iso_utc_now(),
        "rows": n,
        "output": str(output),
    })
    print(f"wrote {n} rows to {output}")


if __name__ == "__main__":
    main()
