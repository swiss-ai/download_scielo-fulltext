#!/usr/bin/env python3
"""Repair retryable terminal rows inside finalized SciELO subtars."""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import copy
import io
import json
import os
import shutil
import socket
import tarfile
import time
from pathlib import Path

import download_worker
from common import (
    ProxyPool,
    atomic_write_text,
    build_user_agent,
    compact_json,
    contact_email_from_env,
    fetch_stats,
    iso_utc_now,
    log,
    read_jsonl,
    require_proxies,
    write_jsonl,
)


def disk_free_gb(path: Path) -> int:
    path.mkdir(parents=True, exist_ok=True)
    return int(shutil.disk_usage(path).free / (1024**3))


def retry_target_status(status: str | None) -> bool:
    if not status:
        return False
    if status.startswith("retry_blocked_"):
        return download_worker.retryable_row_status(status.removeprefix("retry_blocked_"))
    return download_worker.retryable_row_status(status)


def row_payload_members(row: dict) -> list[str]:
    members = []
    for key in ("source_member", "xml_member"):
        if row.get(key):
            members.append(row[key])
    for fig in row.get("figures") or []:
        if fig.get("status") == "ok" and fig.get("member"):
            members.append(fig["member"])
    return members


def copy_tar_member(src: tarfile.TarFile, dst: tarfile.TarFile, member_name: str) -> None:
    info = src.getmember(member_name)
    info = copy.copy(info)
    if info.isfile():
        source = src.extractfile(member_name)
        if source is None:
            raise RuntimeError(f"cannot extract tar member {member_name}")
        data = source.read()
        info.size = len(data)
        dst.addfile(info, io.BytesIO(data))
    else:
        dst.addfile(info)


def repair_history_seed(row: dict) -> list[dict]:
    history = row.get("retry_history") or []
    if isinstance(history, list):
        return list(history)
    return []


def annotate_repair_attempt(entry: dict, repair_label: str) -> dict:
    out = dict(entry)
    out["repair_pass"] = repair_label
    return out


def repair_subtar(
    corpus: Path,
    plan: Path,
    shard_id: str,
    sub_id: str,
    proxy_pool: ProxyPool,
    args: argparse.Namespace,
) -> collections.Counter:
    rows = list(read_jsonl(plan))
    tar_path = corpus / "data" / f"shard-{shard_id}" / f"sub-{sub_id}.tar"
    manifest_path = corpus / "manifests" / f"shard-{shard_id}" / f"sub-{sub_id}.jsonl"
    done_marker = corpus / "state" / f"shard-{shard_id}" / f"sub-{sub_id}.done"
    work_dir = corpus / "state" / f"shard-{shard_id}" / f"sub-{sub_id}.repair.work"
    incomplete = corpus / "state" / f"shard-{shard_id}" / f"sub-{sub_id}.repair.incomplete"

    if not manifest_path.is_file():
        raise RuntimeError(f"missing manifest {manifest_path}")
    if not tar_path.is_file():
        raise RuntimeError(f"missing tar {tar_path}")

    existing_rows = list(read_jsonl(manifest_path))
    if len(existing_rows) != len(rows):
        raise RuntimeError(f"plan/manifest length mismatch for shard {shard_id} sub {sub_id}: {len(rows)} != {len(existing_rows)}")

    targets = [idx for idx, row in enumerate(existing_rows) if retry_target_status(row.get("status"))]
    if args.max_repair_rows:
        targets = targets[: args.max_repair_rows]
    if not targets:
        return collections.Counter({"repair_noop": len(rows)})

    target_set = set(targets)
    out_rows: list[dict | None] = [dict(row) for row in existing_rows]
    counts = collections.Counter(row.get("status", "unknown") for idx, row in enumerate(existing_rows) if idx not in target_set)
    missing_indices = []
    attempts: collections.Counter[int] = collections.Counter()
    retry_histories: dict[int, list[dict]] = collections.defaultdict(list)
    retry_counts: collections.Counter = collections.Counter()

    row_retries = max(0, args.row_retries)
    for idx in targets:
        cached = download_worker.read_row_cache(work_dir, idx)
        if cached is not None:
            out_rows[idx] = cached
            counts[cached.get("status", "unknown")] += 1
            continue
        out_rows[idx] = None
        missing_indices.append(idx)
        retry_histories[idx] = repair_history_seed(existing_rows[idx])
        state = download_worker.read_row_retry_state(work_dir, idx)
        if state:
            history = state.get("history") or []
            if isinstance(history, list):
                retry_histories[idx] = history
            try:
                attempts[idx] = min(max(0, int(state.get("retries_used") or 0)), row_retries)
            except (TypeError, ValueError):
                attempts[idx] = 0

    if not missing_indices:
        log(f"repair shard {shard_id} sub {sub_id}: using cached repair rows for {len(targets)} targets")
    else:
        log(f"repair shard {shard_id} sub {sub_id}: retrying {len(missing_indices)}/{len(rows)} rows")

    def write_repair_heartbeat() -> None:
        download_worker.write_shard_heartbeat(corpus, shard_id, {
            "status": "repair_running",
            "current_subtar": sub_id,
            "repair_targets_total": len(targets),
            "repair_targets_done": len(targets) - len([i for i in target_set if out_rows[i] is None]),
            "repair_status_counts": dict(counts),
            "repair_retry_counts": dict(retry_counts),
            "request_stats": fetch_stats(),
        })

    def finish_attempt(idx: int, out: dict, retry_queue: collections.deque[int]) -> bool:
        status = out.get("status")
        history = retry_histories[idx]
        if row_retries > 0 and download_worker.retryable_row_status(status):
            total_attempts = len(history) + 1
            history.append(annotate_repair_attempt(download_worker.retry_history_entry(out, total_attempts), args.repair_label))
            if attempts[idx] < row_retries:
                attempts[idx] += 1
                download_worker.write_row_retry_state(work_dir, idx, attempts[idx], history)
                retry_counts[f"requeue_{status or 'unknown'}"] += 1
                retry_queue.append(idx)
                log(
                    f"repair shard {shard_id} sub {sub_id} row {idx + 1}/{len(rows)} "
                    f"{out.get('source_id')}: requeue {status} repair attempt {attempts[idx]}/{row_retries}"
                )
                write_repair_heartbeat()
                return False
            out = download_worker.mark_retry_blocked(out, total_attempts, len(history), history)
        elif history:
            out = download_worker.annotate_retry_success(out, len(history) + 1, len(history), history)

        cached = download_worker.write_row_cache(work_dir, idx, out)
        download_worker.delete_row_retry_state(work_dir, idx)
        out_rows[idx] = cached
        counts[cached.get("status", "unknown")] += 1
        if cached.get("status") != "ok" or (args.log_every and (idx + 1) % args.log_every == 0):
            log(f"repair shard {shard_id} sub {sub_id} row {idx + 1}/{len(rows)} {cached.get('source_id')}: {cached.get('status')}")
        write_repair_heartbeat()
        return True

    retry_queue = collections.deque(missing_indices)
    if args.workers <= 1:
        while retry_queue:
            idx = retry_queue.popleft()
            out = download_worker.process_row_safe(rows[idx], str(tar_path.relative_to(corpus)), proxy_pool, idx, args)
            finish_attempt(idx, out, retry_queue)
    else:
        pending: dict[concurrent.futures.Future[dict], int] = {}
        max_pending = max(args.workers, args.workers * 2)

        def fill_pending(executor: concurrent.futures.Executor) -> None:
            while retry_queue and len(pending) < max_pending:
                idx = retry_queue.popleft()
                pending[executor.submit(download_worker.process_row_safe, rows[idx], str(tar_path.relative_to(corpus)), proxy_pool, idx, args)] = idx

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            fill_pending(executor)
            last_heartbeat = time.monotonic()
            while pending or retry_queue:
                if not pending:
                    fill_pending(executor)
                    continue
                done, _ = concurrent.futures.wait(
                    pending,
                    timeout=args.heartbeat_seconds if args.heartbeat_seconds else None,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if not done:
                    write_repair_heartbeat()
                    last_heartbeat = time.monotonic()
                    continue
                for fut in done:
                    idx = pending.pop(fut)
                    finish_attempt(idx, fut.result(), retry_queue)
                fill_pending(executor)
                if args.heartbeat_seconds and time.monotonic() - last_heartbeat >= args.heartbeat_seconds:
                    write_repair_heartbeat()
                    last_heartbeat = time.monotonic()

    missing = [idx for idx in target_set if out_rows[idx] is None]
    if missing:
        raise RuntimeError(f"repair has missing target rows: {missing[:10]}")

    tar_tmp = tar_path.with_suffix(tar_path.suffix + f".repair.tmp.{os.getpid()}")
    manifest_tmp = manifest_path.with_suffix(manifest_path.suffix + f".repair.tmp.{os.getpid()}")
    copied_members: set[str] = set()
    try:
        with tarfile.open(tar_path, "r") as old_tar, tarfile.open(tar_tmp, "w") as new_tar:
            for idx, row in enumerate(out_rows):
                if row is None:
                    raise RuntimeError(f"missing row {idx}")
                if idx in target_set:
                    if row.get("_payload_tar"):
                        _, payload_path = download_worker.row_cache_paths(work_dir, idx)
                        download_worker.append_payload_tar(new_tar, payload_path)
                    continue
                for member in row_payload_members(row):
                    if member in copied_members:
                        continue
                    copy_tar_member(old_tar, new_tar, member)
                    copied_members.add(member)
        write_jsonl(manifest_tmp, [download_worker.final_manifest_row(row) for row in out_rows if row is not None])
        tar_tmp.replace(tar_path)
        manifest_tmp.replace(manifest_path)
        atomic_write_text(done_marker, compact_json({
            "finished_at": iso_utc_now(),
            "host": socket.gethostname(),
            "plan": str(plan),
            "tar": str(tar_path),
            "manifest": str(manifest_path),
            "repair_pass": args.repair_label,
            "repair_targets": len(targets),
            "status_counts": dict(counts),
            "request_stats": fetch_stats(),
        }) + "\n")
        incomplete.unlink(missing_ok=True)
    except Exception as exc:
        atomic_write_text(incomplete, compact_json({
            "failed_at": iso_utc_now(),
            "host": socket.gethostname(),
            "plan": str(plan),
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
        }) + "\n")
        raise
    finally:
        tar_tmp.unlink(missing_ok=True)
        manifest_tmp.unlink(missing_ok=True)

    shutil.rmtree(work_dir, ignore_errors=True)
    return counts


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--shard-id", type=int, required=True)
    p.add_argument("--subtar-id", type=int)
    p.add_argument("--max-subtars", type=int, default=0)
    p.add_argument("--max-repair-rows", type=int, default=int(os.environ.get("MAX_REPAIR_ROWS", "0")))
    p.add_argument("--timeout", type=int, default=int(os.environ.get("REQUEST_TIMEOUT", "90")))
    p.add_argument("--figure-timeout", type=int, default=int(os.environ.get("FIGURE_TIMEOUT", os.environ.get("REQUEST_TIMEOUT", "90"))))
    p.add_argument("--retries", type=int, default=int(os.environ.get("REQUEST_RETRIES", "0")))
    p.add_argument("--figure-retries", type=int, default=int(os.environ.get("FIGURE_RETRIES", "0")))
    p.add_argument("--row-retries", type=int, default=int(os.environ.get("ROW_RETRIES", "2")))
    p.add_argument("--rpm-per-proxy", type=int, default=int(os.environ.get("RPM_PER_PROXY", "10")))
    p.add_argument("--workers", type=int, default=int(os.environ.get("DOWNLOAD_WORKERS", "1")))
    p.add_argument("--resolve-final-url", action=argparse.BooleanOptionalAction, default=os.environ.get("RESOLVE_FINAL_URL", "0") == "1")
    p.add_argument("--xml-resolve-fallback", action=argparse.BooleanOptionalAction, default=os.environ.get("XML_RESOLVE_FALLBACK", "1") != "0")
    p.add_argument("--max-figure-bytes", type=int, default=int(os.environ.get("MAX_FIGURE_BYTES", str(200 * 1024 * 1024))))
    p.add_argument("--render-tiff", action=argparse.BooleanOptionalAction, default=os.environ.get("RENDER_TIFF", "1") != "0")
    p.add_argument("--normalize-large-raster", action=argparse.BooleanOptionalAction, default=os.environ.get("NORMALIZE_LARGE_RASTER", "1") != "0")
    p.add_argument("--max-passthrough-raster-bytes", type=int, default=int(os.environ.get("MAX_PASSTHROUGH_RASTER_BYTES", str(2 * 1024 * 1024))))
    p.add_argument("--render-max-side", type=int, default=int(os.environ.get("RENDER_MAX_SIDE", "2048")))
    p.add_argument("--render-max-pixels", type=int, default=int(os.environ.get("RENDER_MAX_PIXELS", str(2048 * 2048))))
    p.add_argument("--max-decode-pixels", type=int, default=int(os.environ.get("MAX_DECODE_PIXELS", str(100_000_000))))
    p.add_argument("--render-jpeg-quality", type=int, default=int(os.environ.get("RENDER_JPEG_QUALITY", "90")))
    p.add_argument("--keep-original-tiff-on-render-failure", action="store_true")
    p.add_argument("--min-body-chars", type=int, default=200)
    p.add_argument("--min-free-gb", type=int, default=int(os.environ.get("MIN_FREE_GB", "5")))
    p.add_argument("--skip-figures", action="store_true")
    p.add_argument("--allow-third-party-caption-figures", action="store_true")
    p.add_argument("--log-every", type=int, default=int(os.environ.get("LOG_EVERY", "100")))
    p.add_argument("--heartbeat-seconds", type=int, default=int(os.environ.get("HEARTBEAT_SECONDS", "30")))
    p.add_argument("--repair-label", default=os.environ.get("REPAIR_LABEL", "retry-repair"))
    p.add_argument("--user-agent", default=None)
    p.add_argument("--contact-email", default=None)
    args = p.parse_args()

    proxies = require_proxies()
    proxy_pool = ProxyPool(proxies, args.rpm_per_proxy, os.environ.get("PROXY_SHARED_RATE_DIR"))
    args.user_agent = build_user_agent(args.user_agent, args.contact_email)
    args.contact_email = contact_email_from_env(args.contact_email)
    corpus = Path(args.corpus_root)
    if disk_free_gb(corpus / "data") < args.min_free_gb:
        raise SystemExit("ERROR: insufficient free disk")

    meta = json.loads((corpus / "index" / "shards.meta.json").read_text(encoding="utf-8"))
    shard_id = str(args.shard_id).zfill(meta["pad_shard"])
    shard_dir = corpus / "index" / "shards" / f"shard-{shard_id}"
    plans = sorted(shard_dir.glob("sub-*.plan.jsonl"))
    if args.subtar_id is not None:
        plans = [shard_dir / f"sub-{str(args.subtar_id).zfill(meta['pad_sub'])}.plan.jsonl"]
    if args.max_subtars:
        plans = plans[: args.max_subtars]

    total = collections.Counter()
    download_worker.write_shard_heartbeat(corpus, shard_id, {
        "status": "repair_running",
        "plans_total": len(plans),
        "plans_done": 0,
        "request_stats": fetch_stats(),
    })
    for plan in plans:
        sub_id = plan.name.removeprefix("sub-").removesuffix(".plan.jsonl")
        counts = repair_subtar(corpus, plan, shard_id, sub_id, proxy_pool, args)
        total.update(counts)
        done_count = len([
            pth for pth in plans
            if (corpus / "state" / f"shard-{shard_id}" / f"sub-{pth.name.removeprefix('sub-').removesuffix('.plan.jsonl')}.done").exists()
        ])
        download_worker.write_shard_heartbeat(corpus, shard_id, {
            "status": "repair_running",
            "last_subtar": sub_id,
            "plans_total": len(plans),
            "plans_done": done_count,
            "status_counts": dict(total),
            "request_stats": fetch_stats(),
        })
        log(f"repair shard {shard_id} sub {sub_id}: {dict(counts)}")

    atomic_write_text(corpus / "state" / f"shard-{shard_id}.repair.done", compact_json({
        "finished_at": iso_utc_now(),
        "host": socket.gethostname(),
        "shard": shard_id,
        "plans_total": len(plans),
        "status_counts": dict(total),
        "request_stats": fetch_stats(),
    }) + "\n")
    download_worker.write_shard_heartbeat(corpus, shard_id, {
        "status": "repair_done",
        "plans_total": len(plans),
        "status_counts": dict(total),
        "request_stats": fetch_stats(),
    })
    log(f"done repair shard {shard_id}: {dict(total)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
