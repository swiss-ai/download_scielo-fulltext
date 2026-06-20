#!/usr/bin/env python3
"""Partition SciELO manifest rows into shard/subtar plans."""
from __future__ import annotations

import argparse
import math
import shutil
import sys
from collections import defaultdict
from pathlib import Path

from common import atomic_write_json, iso_utc_now, read_jsonl, stable_shard, write_jsonl


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--manifest", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--n-shards", type=int, default=40)
    p.add_argument("--articles-per-subtar", type=int, default=1000)
    p.add_argument("--status", action="append", default=None)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    corpus = Path(args.corpus_root)
    manifest = Path(args.manifest) if args.manifest else corpus / "index" / "manifest_seed.jsonl"
    out_dir = Path(args.output_dir) if args.output_dir else corpus / "index" / "shards"
    meta_path = corpus / "index" / "shards.meta.json"
    statuses = set(args.status or ["planned_xml"])
    if out_dir.exists() and any(out_dir.iterdir()):
        if not args.force:
            sys.stderr.write(f"ERROR: {out_dir} not empty; pass --force\n")
            sys.exit(2)
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = [r for r in read_jsonl(manifest) if r.get("status") in statuses]
    rows.sort(key=lambda r: r.get("source_id", ""))
    if not rows:
        sys.stderr.write("ERROR: no rows selected for sharding\n")
        sys.exit(2)
    sub_count = max(1, math.ceil((len(rows) / args.n_shards) / args.articles_per_subtar))
    pad_shard = max(2, len(str(args.n_shards - 1)))
    pad_sub = max(3, len(str(sub_count - 1)))
    buckets: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for row in rows:
        sid = row["source_id"]
        shard = stable_shard(sid, args.n_shards)
        sub = stable_shard(sid + "|sub", sub_count)
        row["planned_shard"] = str(shard).zfill(pad_shard)
        row["planned_subtar"] = str(sub).zfill(pad_sub)
        buckets[(shard, sub)].append(row)

    shard_counts = [0] * args.n_shards
    plans = 0
    for shard in range(args.n_shards):
        shard_dir = out_dir / f"shard-{str(shard).zfill(pad_shard)}"
        shard_dir.mkdir(parents=True, exist_ok=True)
        for sub in range(sub_count):
            plan = shard_dir / f"sub-{str(sub).zfill(pad_sub)}.plan.jsonl"
            plan_rows = buckets.get((shard, sub), [])
            write_jsonl(plan, plan_rows)
            shard_counts[shard] += len(plan_rows)
            plans += 1

    atomic_write_json(meta_path, {
        "created_at": iso_utc_now(),
        "manifest": str(manifest),
        "n_shards": args.n_shards,
        "sub_count_per_shard": sub_count,
        "articles_per_subtar_target": args.articles_per_subtar,
        "pad_shard": pad_shard,
        "pad_sub": pad_sub,
        "total_articles": len(rows),
        "plans_written": plans,
        "status_filter": sorted(statuses),
        "shard_article_counts": shard_counts,
    })
    print(f"wrote {plans} plan files for {len(rows)} articles under {out_dir}")
    print(f"meta: {meta_path}")


if __name__ == "__main__":
    main()
