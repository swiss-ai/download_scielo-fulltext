#!/usr/bin/env python3
"""Harvest SciELO ArticleMeta article identifier pages."""
from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
from urllib.parse import urlencode

from common import (
    ProxyPool,
    atomic_write_bytes,
    atomic_write_json,
    build_user_agent,
    contact_email_from_env,
    fetch_bytes,
    iso_utc_now,
    log,
    require_proxies,
)

BASE = "https://articlemeta.scielo.org/api/v1/article/identifiers/"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--from", dest="date_from", default="1900-01-01")
    p.add_argument("--until", dest="date_until", required=True)
    p.add_argument("--collection", default=None)
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--max-pages", type=int)
    p.add_argument("--request-timeout", type=int, default=90)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--rpm-per-proxy", type=int, default=int(os.environ.get("RPM_PER_PROXY", "10")))
    p.add_argument("--user-agent", default=None)
    p.add_argument("--contact-email", default=None)
    args = p.parse_args()

    proxies = require_proxies()
    proxy_pool = ProxyPool(proxies, args.rpm_per_proxy)
    user_agent = build_user_agent(args.user_agent, args.contact_email)
    contact_email = contact_email_from_env(args.contact_email)
    log(f"using {len(proxies)} proxy endpoint(s); rpm_per_proxy={args.rpm_per_proxy}")

    corpus = Path(args.corpus_root)
    label = f"window-{args.date_from}_{args.date_until}"
    if args.collection:
        label += f"_collection-{args.collection}"
    raw_dir = corpus / "raw" / "articlemeta_identifiers" / label
    state_path = corpus / "state" / f"harvest_identifiers_{label}.json"
    raw_dir.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("complete"):
            log(f"already complete: {state_path}")
            return
        offset = int(state.get("next_offset", 0))
        page = int(state.get("next_page", 0))
    else:
        offset = 0
        page = 0

    pages_this_run = 0
    while True:
        if args.max_pages is not None and pages_this_run >= args.max_pages:
            log("reached --max-pages")
            return
        params = {
            "limit": str(args.limit),
            "offset": str(offset),
            "from": args.date_from,
            "until": args.date_until,
        }
        if args.collection:
            params["collection"] = args.collection
        url = BASE + "?" + urlencode(params)
        data, headers, final_url, status = fetch_bytes(
            url,
            proxy_pool,
            page,
            user_agent,
            contact_email,
            timeout=args.request_timeout,
            retries=args.retries,
        )
        obj = json.loads(data.decode("utf-8"))
        objects = obj.get("objects") or []
        total = int((obj.get("meta") or {}).get("total") or 0)
        page_path = raw_dir / f"page_{page:06d}.json.gz"
        if page_path.exists():
            raise RuntimeError(f"{page_path} exists; refusing overwrite")
        atomic_write_bytes(page_path, gzip.compress(data, compresslevel=6))
        next_offset = offset + len(objects)
        complete = not objects or (total and next_offset >= total)
        atomic_write_json(state_path, {
            "complete": complete,
            "from": args.date_from,
            "until": args.date_until,
            "collection": args.collection,
            "last_http_status": status,
            "last_final_url": final_url,
            "last_page_records": len(objects),
            "next_offset": next_offset,
            "next_page": page + 1,
            "reported_total": total,
            "updated_at": iso_utc_now(),
        })
        log(f"{label} page={page} offset={offset} records={len(objects)} total={total}")
        if complete:
            return
        offset = next_offset
        page += 1
        pages_this_run += 1


if __name__ == "__main__":
    main()
