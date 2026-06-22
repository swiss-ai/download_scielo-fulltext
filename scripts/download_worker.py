#!/usr/bin/env python3
"""Download one SciELO shard and pack accepted XML + figures into tar files."""
from __future__ import annotations

import argparse
import concurrent.futures
import collections
import hashlib
import io
import json
import os
import shutil
import socket
import tarfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError

from common import (
    ProxyPool,
    atomic_write_text,
    build_user_agent,
    caption_flags,
    compact_json,
    contact_email_from_env,
    fetch_bytes,
    first_text,
    iso_utc_now,
    license_info,
    log,
    media_member_name,
    media_urls,
    parse_xml_article,
    read_jsonl,
    request_headers,
    require_proxies,
    rewrite_media,
    source_id,
    text_of,
    iter_by_local,
    article_id,
    is_xml_response,
    xml_url_from_final,
    write_jsonl,
)

ET.register_namespace("xlink", "http://www.w3.org/1999/xlink")


def disk_free_gb(path: Path) -> int:
    path.mkdir(parents=True, exist_ok=True)
    return int(shutil.disk_usage(path).free / (1024**3))


def tar_add_bytes(tar: tarfile.TarFile, name: str, data: bytes, mode: int = 0o644) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = mode
    info.mtime = int(time.time())
    tar.addfile(info, io.BytesIO(data))


def member_with_ext(member: str, ext: str) -> str:
    base, old_ext = os.path.splitext(member)
    return base + ext if old_ext else member + ext


def capped_size(width: int, height: int, max_pixels: int, max_side: int) -> tuple[int, int]:
    if width <= 0 or height <= 0:
        return width, height
    scale = 1.0
    if max_side and max(width, height) > max_side:
        scale = min(scale, max_side / max(width, height))
    if max_pixels and width * height * scale * scale > max_pixels:
        scale = min(scale, (max_pixels / (width * height)) ** 0.5)
    if scale >= 1.0:
        return width, height
    return max(1, round(width * scale)), max(1, round(height * scale))


def is_tiff_payload(url: str, member: str, headers: dict, data: bytes) -> bool:
    path = (urlparse(url).path or member).lower()
    ctype = ""
    for k, v in headers.items():
        if k.lower() == "content-type":
            ctype = v.lower()
            break
    return (
        path.endswith((".tif", ".tiff"))
        or "image/tif" in ctype
        or data.startswith((b"II*\x00", b"MM\x00*"))
    )


def flatten_alpha_on_white(image):
    from PIL import Image

    rgba = image.convert("RGBA")
    background = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
    background.alpha_composite(rgba)
    return background.convert("RGB")


def render_raster_derivative(
    data: bytes,
    member: str,
    args: argparse.Namespace,
    reason: str,
) -> tuple[bytes, str, dict]:
    try:
        from PIL import Image
    except ImportError as e:
        raise RuntimeError("pillow_unavailable") from e

    try:
        Image.MAX_IMAGE_PIXELS = args.max_decode_pixels
        with Image.open(io.BytesIO(data)) as image:
            actual_format = image.format or "RASTER"
            original_width, original_height = image.size
            n_frames = getattr(image, "n_frames", 1)
            if original_width * original_height > args.max_decode_pixels:
                raise RuntimeError(f"image_pixels_exceed_limit:{original_width * original_height}")
            if getattr(image, "is_animated", False):
                image.seek(0)
            image.load()
            target_size = capped_size(
                original_width,
                original_height,
                args.render_max_pixels,
                args.render_max_side,
            )
            resized = target_size != (original_width, original_height)
            if resized:
                image = image.resize(target_size, Image.Resampling.LANCZOS)
            flattened_alpha = "A" in image.getbands()
            if flattened_alpha:
                image = flatten_alpha_on_white(image)
            elif image.mode != "RGB":
                image = image.convert("RGB")
            out = io.BytesIO()
            image.save(out, format="JPEG", quality=args.render_jpeg_quality, optimize=True)
            final_width, final_height = image.size
    except Exception as e:
        raise RuntimeError(f"raster_render_failed:{type(e).__name__}:{e}") from e

    rendered = out.getvalue()
    rendered_member = member_with_ext(member, ".jpg")
    meta = {
        "rendered": True,
        "render_reason": reason,
        "renderer": "Pillow",
        "source_format": actual_format,
        "output_format": "jpeg",
        "original_width": original_width,
        "original_height": original_height,
        "width": final_width,
        "height": final_height,
        "n_frames": n_frames,
        "resized": resized,
        "flattened_alpha": flattened_alpha,
        "render_max_side": args.render_max_side,
        "render_max_pixels": args.render_max_pixels,
        "jpeg_quality": args.render_jpeg_quality,
    }
    return rendered, rendered_member, meta


def resolve_final_url(row: dict, proxy_pool: ProxyPool, idx: int, args: argparse.Namespace) -> str:
    data, headers, final_url, status = fetch_bytes(
        row["fulltext_html_url"],
        proxy_pool,
        idx,
        args.user_agent,
        args.contact_email,
        timeout=args.timeout,
        retries=args.retries,
        read_limit=4096,
    )
    return final_url


def fetch_xml_url(
    xml_url: str,
    proxy_pool: ProxyPool,
    idx: int,
    args: argparse.Namespace,
) -> tuple[bytes, str, int, dict]:
    data, headers, final, status = fetch_bytes(
        xml_url,
        proxy_pool,
        idx,
        args.user_agent,
        args.contact_email,
        timeout=args.timeout,
        retries=args.retries,
        extra_headers={"Accept": "application/xml,text/xml,*/*"},
    )
    if not is_xml_response(data, headers):
        raise ValueError(f"xml_html_response:{headers.get('Content-Type', '')}")
    return data, xml_url, status, headers


def fetch_xml(row: dict, proxy_pool: ProxyPool, idx: int, args: argparse.Namespace) -> tuple[bytes, str, int, dict]:
    preferred_lang = row.get("preferred_lang") or ""
    html_url = row["fulltext_html_url"]
    if args.resolve_final_url:
        final_url = resolve_final_url(row, proxy_pool, idx, args)
        return fetch_xml_url(xml_url_from_final(final_url, preferred_lang), proxy_pool, idx, args)

    xml_url = xml_url_from_final(html_url, preferred_lang)
    direct_error: Exception | None = None
    try:
        return fetch_xml_url(xml_url, proxy_pool, idx, args)
    except Exception as exc:
        direct_error = exc
        if not args.xml_resolve_fallback:
            raise

    final_url = resolve_final_url(row, proxy_pool, idx, args)
    fallback_url = xml_url_from_final(final_url, preferred_lang)
    if fallback_url == xml_url:
        raise direct_error
    return fetch_xml_url(fallback_url, proxy_pool, idx, args)


def fetch_figure(
    url: str,
    member: str,
    proxy_pool: ProxyPool,
    idx: int,
    args: argparse.Namespace,
) -> dict:
    try:
        data, headers, final_url, status = fetch_bytes(
            url,
            proxy_pool,
            idx,
            args.user_agent,
            args.contact_email,
            timeout=args.timeout,
            retries=args.figure_retries,
        )
        if args.max_figure_bytes and len(data) > args.max_figure_bytes:
            return {
                "url": url,
                "member": member,
                "status": "error",
                "error": f"bytes_exceed_limit:{len(data)}",
            }
        original_bytes = len(data)
        original_sha256 = hashlib.sha256(data).hexdigest()
        render_meta = {
            "rendered": False,
            "original_bytes": original_bytes,
            "original_sha256": original_sha256,
        }
        is_tiff = is_tiff_payload(final_url, member, headers, data)
        render_reason = ""
        if args.render_tiff and is_tiff:
            render_reason = "tiff_to_jpeg"
        elif args.normalize_large_raster and original_bytes > args.max_passthrough_raster_bytes:
            render_reason = "large_raster_to_jpeg"
        if render_reason:
            try:
                rendered, rendered_member, tiff_meta = render_raster_derivative(data, member, args, render_reason)
                data = rendered
                member = rendered_member
                render_meta.update(tiff_meta)
            except Exception as e:  # noqa: BLE001
                if is_tiff and not args.keep_original_tiff_on_render_failure:
                    return {
                        "url": url,
                        "member": member,
                        "status": "error",
                        "http_status": status,
                        "content_type": headers.get("Content-Type", ""),
                        "bytes": original_bytes,
                        "sha256": original_sha256,
                        "error": str(e)[:300],
                        "rendered": False,
                        "render_failed": True,
                    }
                render_meta.update({"rendered": False, "render_failed": True, "render_error": str(e)[:300]})
        return {
            "url": url,
            "member": member,
            "status": "ok",
            "http_status": status,
            "content_type": headers.get("Content-Type", ""),
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            **render_meta,
            "data": data,
        }
    except HTTPError as e:
        return {"url": url, "member": member, "status": "error", "http_status": e.code, "error": f"HTTP {e.code}"}
    except (URLError, TimeoutError, OSError, ValueError) as e:
        return {"url": url, "member": member, "status": "error", "error": type(e).__name__}


def article_text_chars(article: ET.Element) -> tuple[int, int]:
    return len(text_of(article)), sum(len(text_of(el)) for el in iter_by_local(article, "body"))


def process_row(row: dict, tar_rel: str, proxy_pool: ProxyPool, idx: int, args: argparse.Namespace) -> dict:
    sid = row.get("source_id") or source_id(row.get("collection", ""), row.get("pid", ""), row.get("doi", ""))
    prefix = sid
    base_out = {
        "source": "scielo",
        "source_id": sid,
        "pid": row.get("pid", ""),
        "collection": row.get("collection", ""),
        "doi": row.get("doi", ""),
        "shard": row.get("planned_shard", ""),
        "subtar": row.get("planned_subtar", ""),
        "tar_path": tar_rel,
        "fetched_at": iso_utc_now(),
    }
    if row.get("status") != "planned_xml":
        return {**base_out, "status": row.get("status") or "skipped"}
    try:
        xml_bytes, xml_url, xml_status, xml_headers = fetch_xml(row, proxy_pool, idx, args)
    except HTTPError as e:
        return {**base_out, "status": f"xml_http_{e.code}", "error": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        err = str(e)
        status = "xml_html_response" if err.startswith("xml_html_response") else "xml_fetch_error"
        return {**base_out, "status": status, "error": err[:300]}

    try:
        article = parse_xml_article(xml_bytes)
    except Exception as e:  # noqa: BLE001
        return {**base_out, "status": "xml_parse_error", "xml_url": xml_url, "error": str(e)[:300]}

    lic_code, lic_policy, lic_text, lic_urls = license_info(article)
    third_party_flag, third_party_terms = caption_flags(article)
    text_chars, body_chars = article_text_chars(article)
    if lic_policy != "keep":
        return {
            **base_out,
            "status": f"license_{lic_policy}",
            "xml_url": xml_url,
            "license_code": lic_code,
            "license_policy": lic_policy,
            "license_text": lic_text,
            "license_urls": lic_urls,
            "xml_bytes": len(xml_bytes),
        }
    if third_party_flag and not args.allow_third_party_caption_figures:
        return {
            **base_out,
            "status": "license_figure_ambiguous",
            "xml_url": xml_url,
            "license_code": lic_code,
            "license_policy": lic_policy,
            "license_text": lic_text,
            "license_urls": lic_urls,
            "third_party_caption_flag": third_party_flag,
            "third_party_caption_terms": third_party_terms,
            "xml_bytes": len(xml_bytes),
        }
    if body_chars < args.min_body_chars:
        return {
            **base_out,
            "status": "xml_no_body",
            "xml_url": xml_url,
            "license_code": lic_code,
            "license_policy": lic_policy,
            "body_text_chars": body_chars,
            "xml_bytes": len(xml_bytes),
        }

    urls = media_urls(article, xml_url)
    url_to_member = {url: media_member_name(prefix, i, url) for i, url in enumerate(urls)}
    figure_manifest: list[dict] = []
    figure_files: list[tuple[str, bytes]] = []
    if args.skip_figures:
        figure_manifest = [{"url": url, "member": url_to_member[url], "status": "skipped"} for url in urls]
    else:
        for i, url in enumerate(urls):
            res = fetch_figure(url, url_to_member[url], proxy_pool, idx + i + 1, args)
            data = res.pop("data", None)
            if res.get("status") == "ok" and data is not None:
                figure_files.append((res["member"], data))
            figure_manifest.append(res)

    ok_map = {f["url"]: f["member"] for f in figure_manifest if f.get("status") == "ok"}
    rewrite_media(article, ok_map, xml_url)
    packaged_xml = ET.tostring(article, encoding="utf-8", xml_declaration=True)
    xml_member = f"{prefix}/article.xml"
    source_member = f"{prefix}/source.json"
    source_payload = {
        "source_id": sid,
        "pid": row.get("pid", ""),
        "collection": row.get("collection", ""),
        "fulltext_html_url": row.get("fulltext_html_url", ""),
        "xml_url": xml_url,
        "xml_http_status": xml_status,
        "xml_content_type": xml_headers.get("Content-Type", ""),
        "license_code": lic_code,
        "license_policy": lic_policy,
        "license_urls": lic_urls,
        "downloaded_at": iso_utc_now(),
    }
    expected = len(urls)
    downloaded = len(figure_files)
    status = "ok" if expected == downloaded else ("partial_figures" if downloaded else ("no_figures" if expected == 0 else "figures_failed"))
    return {
        **base_out,
        "status": status,
        "xml_url": xml_url,
        "xml_member": xml_member,
        "source_member": source_member,
        "xml_bytes": len(packaged_xml),
        "xml_sha256": hashlib.sha256(packaged_xml).hexdigest(),
        "text_chars": text_chars,
        "body_text_chars": body_chars,
        "article_title": first_text(article, "article-title"),
        "journal_title": first_text(article, "journal-title"),
        "xml_doi": article_id(article, "doi"),
        "license_code": lic_code,
        "license_policy": lic_policy,
        "license_text": lic_text,
        "license_urls": lic_urls,
        "figure_count": sum(1 for _ in iter_by_local(article, "fig")),
        "table_count": sum(1 for _ in iter_by_local(article, "table-wrap")),
        "formula_count": sum(1 for _ in iter_by_local(article, "disp-formula", "inline-formula", "math")),
        "expected_figure_files": expected,
        "downloaded_figure_files": downloaded,
        "figure_bytes": sum(int(f.get("bytes", 0) or 0) for f in figure_manifest if f.get("status") == "ok"),
        "original_figure_bytes": sum(int(f.get("original_bytes", f.get("bytes", 0)) or 0) for f in figure_manifest if f.get("status") == "ok"),
        "rendered_figure_files": sum(1 for f in figure_manifest if f.get("status") == "ok" and f.get("rendered")),
        "missing_figure_urls": [f["url"] for f in figure_manifest if f.get("status") != "ok"],
        "figures": figure_manifest,
        "third_party_caption_flag": third_party_flag,
        "third_party_caption_terms": third_party_terms,
        "_files": [
            (xml_member, packaged_xml),
            (source_member, compact_json(source_payload).encode("utf-8") + b"\n"),
            *figure_files,
        ],
    }


def write_result_files(tar: tarfile.TarFile, out: dict) -> None:
    for member, data in out.pop("_files", []):
        tar_add_bytes(tar, member, data)


def row_cache_paths(work_dir: Path, idx: int) -> tuple[Path, Path]:
    stem = f"row-{idx:06d}"
    return work_dir / f"{stem}.json", work_dir / f"{stem}.tar"


def write_row_cache(work_dir: Path, idx: int, out: dict) -> dict:
    work_dir.mkdir(parents=True, exist_ok=True)
    manifest_path, payload_path = row_cache_paths(work_dir, idx)
    files = out.pop("_files", [])
    cache_out = dict(out)
    cache_out["_payload_tar"] = bool(files)
    if files:
        payload_tmp = payload_path.with_suffix(payload_path.suffix + f".tmp.{os.getpid()}")
        with tarfile.open(payload_tmp, "w") as payload_tar:
            for member, data in files:
                tar_add_bytes(payload_tar, member, data)
        payload_tmp.replace(payload_path)
    atomic_write_text(manifest_path, compact_json(cache_out) + "\n")
    return cache_out


def read_row_cache(work_dir: Path, idx: int) -> dict | None:
    manifest_path, payload_path = row_cache_paths(work_dir, idx)
    if not manifest_path.exists():
        return None
    try:
        out = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if out.get("_payload_tar") and not payload_path.exists():
        return None
    return out


def final_manifest_row(row: dict) -> dict:
    return {k: v for k, v in row.items() if not k.startswith("_")}


def append_payload_tar(final_tar: tarfile.TarFile, payload_path: Path) -> None:
    with tarfile.open(payload_path, "r") as payload_tar:
        for member in payload_tar.getmembers():
            source = payload_tar.extractfile(member) if member.isfile() else None
            final_tar.addfile(member, source)


def assemble_subtar(tar_path: Path, manifest_path: Path, work_dir: Path, out_rows: list[dict | None]) -> None:
    missing = [i for i, row in enumerate(out_rows) if row is None]
    if missing:
        raise RuntimeError(f"cannot assemble subtar with missing row caches: {missing[:10]}")
    tar_tmp = tar_path.with_suffix(tar_path.suffix + f".tmp.{os.getpid()}")
    with tarfile.open(tar_tmp, "w") as final_tar:
        for idx, row in enumerate(out_rows):
            if row and row.get("_payload_tar"):
                _, payload_path = row_cache_paths(work_dir, idx)
                append_payload_tar(final_tar, payload_path)
    tar_tmp.replace(tar_path)
    write_jsonl(manifest_path, [final_manifest_row(row) for row in out_rows if row is not None])


def log_row_result(shard_id: str, sub_id: str, idx: int, total_rows: int, out: dict, args: argparse.Namespace) -> None:
    if args.log_every and ((idx + 1) % args.log_every == 0 or out["status"] != "ok"):
        log(f"shard {shard_id} sub {sub_id} row {idx + 1}/{total_rows} {out.get('source_id')}: {out['status']}")


def process_subtar(corpus: Path, plan: Path, shard_id: str, sub_id: str, proxy_pool: ProxyPool, args: argparse.Namespace) -> collections.Counter:
    rows = list(read_jsonl(plan))
    tar_path = corpus / "data" / f"shard-{shard_id}" / f"sub-{sub_id}.tar"
    manifest_path = corpus / "manifests" / f"shard-{shard_id}" / f"sub-{sub_id}.jsonl"
    done_marker = corpus / "state" / f"shard-{shard_id}" / f"sub-{sub_id}.done"
    work_dir = corpus / "state" / f"shard-{shard_id}" / f"sub-{sub_id}.work"
    if done_marker.exists() and not args.force:
        if work_dir.exists():
            shutil.rmtree(work_dir)
        log(f"skip done {done_marker}")
        return collections.Counter({"skipped_done": len(rows)})
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    tar_rel = str(tar_path.relative_to(corpus))
    out_rows: list[dict | None] = [None] * len(rows)
    counts = collections.Counter()

    missing_indices = []
    for idx in range(len(rows)):
        cached = read_row_cache(work_dir, idx)
        if cached is None:
            missing_indices.append(idx)
            continue
        out_rows[idx] = cached
        counts[cached["status"]] += 1
    if missing_indices and len(missing_indices) != len(rows):
        log(f"resume shard {shard_id} sub {sub_id}: cached {len(rows) - len(missing_indices)}/{len(rows)} rows")

    if args.workers <= 1:
        for idx in missing_indices:
            out = process_row(rows[idx], tar_rel, proxy_pool, idx, args)
            cached = write_row_cache(work_dir, idx, out)
            out_rows[idx] = cached
            counts[cached["status"]] += 1
            log_row_result(shard_id, sub_id, idx, len(rows), cached, args)
    else:
        pending: dict[concurrent.futures.Future[dict], int] = {}
        missing_pos = 0
        max_pending = max(args.workers, args.workers * 2)

        def fill_pending(executor: concurrent.futures.Executor) -> None:
            nonlocal missing_pos
            while missing_pos < len(missing_indices) and len(pending) < max_pending:
                idx = missing_indices[missing_pos]
                fut = executor.submit(process_row, rows[idx], tar_rel, proxy_pool, idx, args)
                pending[fut] = idx
                missing_pos += 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            fill_pending(executor)
            while pending:
                done, _ = concurrent.futures.wait(
                    pending,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for fut in done:
                    idx = pending.pop(fut)
                    out = fut.result()
                    cached = write_row_cache(work_dir, idx, out)
                    out_rows[idx] = cached
                    counts[cached["status"]] += 1
                    log_row_result(shard_id, sub_id, idx, len(rows), cached, args)
                fill_pending(executor)

    assemble_subtar(tar_path, manifest_path, work_dir, out_rows)
    atomic_write_text(done_marker, compact_json({
        "finished_at": iso_utc_now(),
        "host": socket.gethostname(),
        "plan": str(plan),
        "tar": str(tar_path),
        "manifest": str(manifest_path),
        "status_counts": dict(counts),
    }) + "\n")
    shutil.rmtree(work_dir, ignore_errors=True)
    return counts


def write_shard_heartbeat(corpus: Path, shard_id: str, payload: dict) -> None:
    path = corpus / "state" / f"shard-{shard_id}.heartbeat"
    data = {
        "updated_at": iso_utc_now(),
        "host": socket.gethostname(),
        "shard": shard_id,
        **payload,
    }
    atomic_write_text(path, compact_json(data) + "\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-root", default="/mloscratch/scielo-fulltext")
    p.add_argument("--shard-id", type=int, required=True)
    p.add_argument("--subtar-id", type=int)
    p.add_argument("--max-subtars", type=int, default=0)
    p.add_argument("--timeout", type=int, default=int(os.environ.get("REQUEST_TIMEOUT", "90")))
    p.add_argument("--retries", type=int, default=int(os.environ.get("REQUEST_RETRIES", "3")))
    p.add_argument("--figure-retries", type=int, default=int(os.environ.get("FIGURE_RETRIES", "3")))
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
    p.add_argument("--force", action="store_true")
    p.add_argument("--user-agent", default=None)
    p.add_argument("--contact-email", default=None)
    args = p.parse_args()

    proxies = require_proxies()
    proxy_pool = ProxyPool(proxies, args.rpm_per_proxy)
    args.user_agent = build_user_agent(args.user_agent, args.contact_email)
    args.contact_email = contact_email_from_env(args.contact_email)
    corpus = Path(args.corpus_root)
    if disk_free_gb(corpus / "data") < args.min_free_gb:
        raise SystemExit("ERROR: insufficient free disk")
    meta = json.loads((corpus / "index" / "shards.meta.json").read_text(encoding="utf-8"))
    pad_shard = meta["pad_shard"]
    pad_sub = meta["pad_sub"]
    shard_id = str(args.shard_id).zfill(pad_shard)
    shard_dir = corpus / "index" / "shards" / f"shard-{shard_id}"
    plans = sorted(shard_dir.glob("sub-*.plan.jsonl"))
    if args.subtar_id is not None:
        plans = [shard_dir / f"sub-{str(args.subtar_id).zfill(pad_sub)}.plan.jsonl"]
    if args.max_subtars:
        plans = plans[: args.max_subtars]
    total = collections.Counter()
    full_shard_run = args.subtar_id is None and not args.max_subtars
    write_shard_heartbeat(corpus, shard_id, {"status": "running", "plans_total": len(plans), "plans_done": 0})
    for plan in plans:
        sub_id = plan.name.removeprefix("sub-").removesuffix(".plan.jsonl")
        try:
            counts = process_subtar(corpus, plan, shard_id, sub_id, proxy_pool, args)
        except Exception as exc:
            incomplete = corpus / "state" / f"shard-{shard_id}" / f"sub-{sub_id}.incomplete"
            atomic_write_text(incomplete, compact_json({
                "failed_at": iso_utc_now(),
                "host": socket.gethostname(),
                "plan": str(plan),
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
            }) + "\n")
            write_shard_heartbeat(corpus, shard_id, {
                "status": "failed",
                "failed_subtar": sub_id,
                "plans_total": len(plans),
                "plans_done": sum(total.values()),
            })
            raise
        total.update(counts)
        write_shard_heartbeat(corpus, shard_id, {
            "status": "running",
            "last_subtar": sub_id,
            "plans_total": len(plans),
            "plans_done": len([p for p in plans if (corpus / "state" / f"shard-{shard_id}" / f"sub-{p.name.removeprefix('sub-').removesuffix('.plan.jsonl')}.done").exists()]),
            "status_counts": dict(total),
        })
        log(f"shard {shard_id} sub {sub_id}: {dict(counts)}")
    if full_shard_run:
        shard_done = corpus / "state" / f"shard-{shard_id}.done"
        atomic_write_text(shard_done, compact_json({
            "finished_at": iso_utc_now(),
            "host": socket.gethostname(),
            "shard": shard_id,
            "plans_total": len(plans),
            "status_counts": dict(total),
        }) + "\n")
    log(f"done shard {shard_id}: {dict(total)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
