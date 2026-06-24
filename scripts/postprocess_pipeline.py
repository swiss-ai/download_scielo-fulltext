#!/usr/bin/env python3
"""Wait for SciELO repair, verify shards, and aggregate manifests."""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json_atomic(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def load_meta(corpus: Path) -> dict:
    return json.loads((corpus / "index" / "shards.meta.json").read_text(encoding="utf-8"))


def shard_ids(meta: dict) -> list[tuple[int, str]]:
    pad = int(meta["pad_shard"])
    return [(idx, str(idx).zfill(pad)) for idx in range(int(meta["n_shards"]))]


def repair_done_count(corpus: Path, ids: list[tuple[int, str]]) -> int:
    return sum(1 for _, sid in ids if (corpus / "state" / f"shard-{sid}.repair.done").is_file())


def wait_for_repair(corpus: Path, ids: list[tuple[int, str]], poll_seconds: int, timeout_seconds: int) -> None:
    start = time.monotonic()
    total = len(ids)
    while True:
        done = repair_done_count(corpus, ids)
        write_json_atomic(corpus / "state" / "postprocess-pipeline.heartbeat", {
            "status": "waiting_for_repair",
            "repair_done": done,
            "repair_total": total,
            "updated_at": utc_now(),
        })
        print(f"repair done markers: {done}/{total}", flush=True)
        if done == total:
            return
        if timeout_seconds and time.monotonic() - start > timeout_seconds:
            raise TimeoutError(f"timed out waiting for repair markers: {done}/{total}")
        time.sleep(max(1, poll_seconds))


def verify_one(script_dir: Path, corpus: Path, idx: int, sid: str, force: bool) -> dict:
    out = corpus / "state" / f"verify-shard-{sid}.json"
    done_marker = corpus / "state" / f"shard-{sid}.verify.done"
    if done_marker.is_file() and not force:
        try:
            data = json.loads(done_marker.read_text(encoding="utf-8"))
            data["skipped_existing"] = True
            return data
        except json.JSONDecodeError:
            pass

    tmp = out.with_suffix(out.suffix + f".tmp.{os.getpid()}")
    with tmp.open("w", encoding="utf-8") as f:
        proc = subprocess.run(
            [
                sys.executable,
                str(script_dir / "verify_shard.py"),
                "--corpus-root",
                str(corpus),
                "--shard-id",
                str(idx),
            ],
            stdout=f,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    if proc.returncode != 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"verify shard {sid} failed: {proc.stderr[-2000:]}")

    data = json.loads(tmp.read_text(encoding="utf-8"))
    tmp.replace(out)
    write_json_atomic(done_marker, data)
    return data


def verify_all(script_dir: Path, corpus: Path, ids: list[tuple[int, str]], parallelism: int, force: bool) -> None:
    parallelism = max(1, parallelism)
    completed = 0
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(verify_one, script_dir, corpus, idx, sid, force): sid
            for idx, sid in ids
        }
        for fut in concurrent.futures.as_completed(futures):
            sid = futures[fut]
            try:
                data = fut.result()
            except Exception as exc:
                errors.append(f"shard {sid}: {exc}")
                print(f"verify shard {sid}: failed: {exc}", flush=True)
            else:
                completed += 1
                skipped = " skipped" if data.get("skipped_existing") else ""
                print(f"verify shard {sid}: ok{skipped}", flush=True)
            write_json_atomic(corpus / "state" / "postprocess-pipeline.heartbeat", {
                "status": "verifying",
                "verify_done": completed,
                "verify_total": len(ids),
                "errors": errors[:20],
                "updated_at": utc_now(),
            })
    if errors:
        raise RuntimeError("verification failed: " + "; ".join(errors[:5]))


def aggregate(script_dir: Path, corpus: Path) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(script_dir / "aggregate_manifest.py"),
            "--corpus-root",
            str(corpus),
            "--output",
            str(corpus / "manifest.jsonl"),
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"aggregate failed: {proc.stderr[-2000:]}")
    summary_path = corpus / "manifest.summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    write_json_atomic(corpus / "state" / "aggregate.done", {
        "status": "aggregate_done",
        "summary": summary,
        "updated_at": utc_now(),
    })
    print(proc.stdout, end="", flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--repo-dir", default=str(Path(__file__).resolve().parent.parent))
    p.add_argument("--wait-repair-done", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--poll-seconds", type=int, default=int(os.environ.get("WAIT_SECONDS", "60")))
    p.add_argument("--timeout-seconds", type=int, default=int(os.environ.get("WAIT_TIMEOUT_SECONDS", "0")))
    p.add_argument("--verify-parallelism", type=int, default=int(os.environ.get("VERIFY_PARALLELISM", "4")))
    p.add_argument("--force-verify", action=argparse.BooleanOptionalAction, default=os.environ.get("FORCE_VERIFY", "0") == "1")
    args = p.parse_args()

    corpus = Path(args.corpus_root)
    script_dir = Path(args.repo_dir) / "scripts"
    ids = shard_ids(load_meta(corpus))
    if args.wait_repair_done:
        wait_for_repair(corpus, ids, args.poll_seconds, args.timeout_seconds)
    verify_all(script_dir, corpus, ids, args.verify_parallelism, args.force_verify)
    aggregate(script_dir, corpus)
    write_json_atomic(corpus / "state" / "postprocess-pipeline.done", {
        "status": "done",
        "shards": len(ids),
        "updated_at": utc_now(),
    })
    write_json_atomic(corpus / "state" / "postprocess-pipeline.heartbeat", {
        "status": "done",
        "shards": len(ids),
        "updated_at": utc_now(),
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
