#!/usr/bin/env python3
"""Fetch per-article ArticleMeta records with fulltext links."""
from __future__ import annotations

import argparse
import concurrent.futures
import collections
import gzip
import json
import os
import re
import uuid
from pathlib import Path
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from common import (
    ProxyPool,
    build_user_agent,
    contact_email_from_env,
    compact_json,
    fetch_bytes,
    iso_utc_now,
    log,
    read_json_gz,
    require_proxies,
    stable_shard,
)

BASE = "https://articlemeta.scielo.org/api/v1/article/"


def article_key(collection: str, code: str) -> str:
    return f"{collection}|{code}"


def stable_sub_shard(value: str, n_shards: int) -> int:
    return stable_shard("articlemeta-sub|" + value, n_shards)


def iter_identifiers(corpus: Path):
    seen: set[tuple[str, str]] = set()
    for page in sorted((corpus / "raw" / "articlemeta_identifiers").glob("**/page_*.json.gz")):
        obj = read_json_gz(page)
        for item in obj.get("objects") or []:
            code = item.get("code") or ""
            collection = item.get("collection") or ""
            key = (collection, code)
            if not code or key in seen:
                continue
            seen.add(key)
            yield page, item


def iter_existing_rows(path: Path):
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    log(f"warning: stopped resume read at invalid JSON line {lineno} in {path}")
                    return
    except (EOFError, OSError, gzip.BadGzipFile) as exc:
        log(f"warning: stopped resume read for partial gzip {path}: {type(exc).__name__}")


def newest_resume_tmp(output: Path) -> Path | None:
    candidates = [
        p
        for p in output.parent.glob(output.name + ".tmp.*")
        if p.is_file() and p.stat().st_size > 0
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: (p.stat().st_mtime, p.stat().st_size))


def should_keep_resume_source(resume_path: Path | None, output: Path) -> bool:
    if resume_path is None:
        return False
    return not resume_path.name.startswith(output.name + ".tmp.")


def unique_tmp_path(output: Path) -> Path:
    raw_run = os.environ.get("RUNAI_JOB_NAME") or os.environ.get("HOSTNAME") or "local"
    safe_run = re.sub(r"[^A-Za-z0-9._-]+", "-", raw_run).strip("-")[:80] or "run"
    for _ in range(100):
        suffix = f".tmp.{os.getpid()}.{safe_run}.{uuid.uuid4().hex[:8]}"
        tmp = output.with_suffix(output.suffix + suffix)
        try:
            fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o660)
        except FileExistsError:
            continue
        else:
            os.close(fd)
            return tmp
    raise RuntimeError(f"could not allocate unique temp path for {output}")


def fetch_articlemeta_row(
    *,
    corpus: Path,
    page: Path,
    ident: dict,
    global_idx: int,
    request_idx: int,
    proxy_pool: ProxyPool,
    user_agent: str,
    contact_email: str | None,
    timeout: int,
    retries: int,
) -> dict:
    code = ident.get("code") or ""
    collection = ident.get("collection") or ""
    params = {"code": code, "collection": collection, "body": "true"}
    url = BASE + "?" + urlencode(params)
    row = {
        "source": "scielo",
        "code": code,
        "collection": collection,
        "identifier_page": str(page.relative_to(corpus)),
        "identifier_global_index": global_idx,
        "fetched_at": iso_utc_now(),
        "status": "articlemeta_error",
    }
    try:
        data, headers, final_url, status = fetch_bytes(
            url,
            proxy_pool,
            request_idx,
            user_agent,
            contact_email,
            timeout=timeout,
            retries=retries,
        )
        row.update({
            "status": "ok",
            "http_status": status,
            "final_url": final_url,
            "articlemeta": json.loads(data.decode("utf-8")),
        })
    except HTTPError as e:
        row.update({"status": "http_error", "http_status": e.code, "error": f"HTTP {e.code}"})
    except (URLError, TimeoutError, OSError, IncompleteRead, json.JSONDecodeError) as e:
        row.update({"status": "request_error", "error": type(e).__name__})
    return row


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--output", default=None)
    p.add_argument("--limit", type=int)
    p.add_argument("--n-shards", type=int, default=1)
    p.add_argument("--shard-id", type=int, default=0)
    p.add_argument("--sub-shards", type=int, default=int(os.environ.get("ARTICLEMETA_SUB_SHARDS", "1")))
    p.add_argument("--sub-shard-id", type=int, default=int(os.environ.get("ARTICLEMETA_SUB_SHARD_ID", "0")))
    p.add_argument("--request-timeout", type=int, default=90)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--rpm-per-proxy", type=int, default=int(os.environ.get("RPM_PER_PROXY", "10")))
    p.add_argument("--workers", type=int, default=int(os.environ.get("ARTICLEMETA_WORKERS", "1")))
    p.add_argument(
        "--prefetch-per-worker",
        type=int,
        default=int(os.environ.get("ARTICLEMETA_PREFETCH_PER_WORKER", "4")),
    )
    p.add_argument("--no-resume", action="store_false", dest="resume")
    p.add_argument("--user-agent", default=None)
    p.add_argument("--contact-email", default=None)
    args = p.parse_args()

    proxies = require_proxies()
    proxy_pool = ProxyPool(proxies, args.rpm_per_proxy)
    user_agent = build_user_agent(args.user_agent, args.contact_email)
    contact_email = contact_email_from_env(args.contact_email)
    corpus = Path(args.corpus_root)
    if args.n_shards < 1:
        raise SystemExit("ERROR: --n-shards must be >= 1")
    if args.shard_id < 0 or args.shard_id >= args.n_shards:
        raise SystemExit("ERROR: --shard-id must satisfy 0 <= shard_id < n_shards")
    if args.sub_shards < 1:
        raise SystemExit("ERROR: --sub-shards must be >= 1")
    if args.sub_shard_id < 0 or args.sub_shard_id >= args.sub_shards:
        raise SystemExit("ERROR: --sub-shard-id must satisfy 0 <= sub_shard_id < sub_shards")
    if args.workers < 1:
        raise SystemExit("ERROR: --workers must be >= 1")
    if args.prefetch_per_worker < 1:
        raise SystemExit("ERROR: --prefetch-per-worker must be >= 1")
    base_output: Path | None = None
    if args.output:
        output = Path(args.output)
    elif args.n_shards == 1:
        output = corpus / "index" / "articlemeta.jsonl.gz"
    else:
        pad = max(3, len(str(args.n_shards - 1)))
        base_output = corpus / "index" / "articlemeta" / f"shard-{args.shard_id:0{pad}d}.jsonl.gz"
        if args.sub_shards == 1:
            output = base_output
        else:
            pad_sub = max(2, len(str(args.sub_shards - 1)))
            output = (
                corpus
                / "index"
                / "articlemeta"
                / (
                    f"shard-{args.shard_id:0{pad}d}"
                    f"-part-{args.sub_shard_id:0{pad_sub}d}-of-{args.sub_shards:0{pad_sub}d}.jsonl.gz"
                )
            )
    summary_path = output.with_suffix(output.suffix + ".summary.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and args.resume:
        log(f"output exists; treating shard as complete: {output}")
        return

    counters: collections.Counter[str] = collections.Counter()
    done_keys: set[str] = set()
    restored = 0
    fetched_this_run = 0
    selected_seen = 0
    submitted = 0
    resume_path = newest_resume_tmp(output) if args.resume else None
    if args.resume and resume_path is None and args.sub_shards > 1 and base_output is not None:
        resume_path = newest_resume_tmp(base_output)
    tmp = unique_tmp_path(output)

    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        if resume_path:
            log(f"resuming from {resume_path}")
            for row in iter_existing_rows(resume_path):
                key = article_key(row.get("collection") or "", row.get("code") or "")
                if not key.strip("|") or key in done_keys:
                    continue
                if args.n_shards > 1 and stable_shard(key, args.n_shards) != args.shard_id:
                    continue
                if args.sub_shards > 1 and stable_sub_shard(key, args.sub_shards) != args.sub_shard_id:
                    continue
                done_keys.add(key)
                counters[row.get("status") or "unknown"] += 1
                restored += 1
                out.write(compact_json(row) + "\n")
            log(f"restored {restored} existing ArticleMeta rows")

        def tasks():
            nonlocal selected_seen, submitted
            for global_idx, (page, ident) in enumerate(iter_identifiers(corpus)):
                code = ident.get("code") or ""
                collection = ident.get("collection") or ""
                key = article_key(collection, code)
                if args.n_shards > 1 and stable_shard(key, args.n_shards) != args.shard_id:
                    continue
                if args.sub_shards > 1 and stable_sub_shard(key, args.sub_shards) != args.sub_shard_id:
                    continue
                if args.limit is not None and selected_seen >= args.limit:
                    break
                selected_seen += 1
                if key in done_keys:
                    continue
                request_idx = submitted
                submitted += 1
                yield {
                    "corpus": corpus,
                    "page": page,
                    "ident": ident,
                    "global_idx": global_idx,
                    "request_idx": request_idx,
                    "proxy_pool": proxy_pool,
                    "user_agent": user_agent,
                    "contact_email": contact_email,
                    "timeout": args.request_timeout,
                    "retries": args.retries,
                }

        pending: dict[concurrent.futures.Future[dict], None] = {}
        task_iter = iter(tasks())
        max_pending = args.workers * args.prefetch_per_worker

        def fill_pending(executor: concurrent.futures.Executor) -> None:
            while len(pending) < max_pending:
                try:
                    task = next(task_iter)
                except StopIteration:
                    return
                pending[executor.submit(fetch_articlemeta_row, **task)] = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            fill_pending(executor)
            while pending:
                done, _ = concurrent.futures.wait(
                    pending,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    pending.pop(future)
                    row = future.result()
                    key = article_key(row.get("collection") or "", row.get("code") or "")
                    if key in done_keys:
                        continue
                    done_keys.add(key)
                    out.write(compact_json(row) + "\n")
                    fetched_this_run += 1
                    counters[row["status"]] += 1
                    written = restored + fetched_this_run
                    if written % 100 == 0:
                        log(
                            f"shard {args.shard_id}/{args.n_shards}: wrote {written} "
                            f"ArticleMeta records (restored={restored} new={fetched_this_run})"
                        )
                fill_pending(executor)
    written = restored + fetched_this_run
    tmp.replace(output)
    if resume_path and not should_keep_resume_source(resume_path, output):
        try:
            resume_path.unlink()
        except FileNotFoundError:
            pass
    summary_path.write_text(json.dumps({
        "created_at": iso_utc_now(),
        "output": str(output),
        "rows_written": written,
        "rows_restored": restored,
        "rows_fetched_this_run": fetched_this_run,
        "selected_identifiers": selected_seen,
        "workers": args.workers,
        "prefetch_per_worker": args.prefetch_per_worker,
        "status_counts": dict(counters),
        "n_shards": args.n_shards,
        "shard_id": args.shard_id,
        "sub_shards": args.sub_shards,
        "sub_shard_id": args.sub_shard_id,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    log(f"wrote {written} rows to {output}")
    log(f"summary: {summary_path}")


if __name__ == "__main__":
    main()
