#!/usr/bin/env python3
"""Verify tar/member consistency for one SciELO shard."""
from __future__ import annotations

import argparse
import json
import tarfile
from collections import Counter
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--shard-id", type=int, required=True)
    args = p.parse_args()
    corpus = Path(args.corpus_root)
    meta = json.loads((corpus / "index" / "shards.meta.json").read_text(encoding="utf-8"))
    shard_id = str(args.shard_id).zfill(meta["pad_shard"])
    counts = Counter()
    errors = []
    for manifest in sorted((corpus / "manifests" / f"shard-{shard_id}").glob("sub-*.jsonl")):
        rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
        tar_paths = sorted(set(r.get("tar_path") for r in rows if r.get("tar_path")))
        for rel in tar_paths:
            tar_path = corpus / rel
            if not tar_path.is_file():
                errors.append(f"missing tar {tar_path}")
                continue
            with tarfile.open(tar_path, "r") as tar:
                members = set(tar.getnames())
            for row in rows:
                counts[row.get("status", "unknown")] += 1
                xml_member = row.get("xml_member")
                if row.get("status") in {"ok", "partial_figures", "figures_failed", "no_figures"}:
                    if not xml_member or xml_member not in members:
                        errors.append(f"missing xml member {xml_member} in {tar_path}")
                    for fig in row.get("figures") or []:
                        if fig.get("status") == "ok" and fig.get("member") not in members:
                            errors.append(f"missing figure member {fig.get('member')} in {tar_path}")
    print(json.dumps({"shard": shard_id, "status_counts": dict(counts), "errors": errors[:20], "error_count": len(errors)}, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
