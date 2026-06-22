#!/usr/bin/env python3
"""Shared helpers for SciELO full-text download scripts."""
from __future__ import annotations

import gzip
import collections
import fcntl
import hashlib
import html
import json
import os
import random
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
from http.client import HTTPException, IncompleteRead
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlsplit, urlunsplit, unquote
from urllib.request import ProxyHandler, Request, build_opener

XLINK_HREF = "{http://www.w3.org/1999/xlink}href"
DEFAULT_USER_AGENT_PRODUCT = "SwissAI-Apertus-SciELODownloader/0.1"
TRANSIENT_HTTP = {408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
_FETCH_STATS = collections.Counter()
_FETCH_STATS_LOCK = threading.Lock()


def add_fetch_stat(key: str, value: int = 1) -> None:
    with _FETCH_STATS_LOCK:
        _FETCH_STATS[key] += value


def fetch_stats() -> dict[str, int]:
    with _FETCH_STATS_LOCK:
        return dict(_FETCH_STATS)


def reset_fetch_stats() -> None:
    with _FETCH_STATS_LOCK:
        _FETCH_STATS.clear()


def iso_utc_now() -> str:
    import datetime as dt

    return dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds")


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def compact_json(data) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(data)
    tmp.replace(path)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def atomic_write_json(path: Path, data: dict) -> None:
    atomic_write_text(path, json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    n = 0
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(tmp, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(compact_json(row) + "\n")
            n += 1
    tmp.replace(path)
    return n


def read_jsonl(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def read_json_gz(path: Path) -> dict:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads(path.read_text(encoding="utf-8"))


def contact_email_from_env(default: str | None = None) -> str | None:
    return os.environ.get("SCIELO_CONTACT_EMAIL") or os.environ.get("CONTACT_EMAIL") or default


def build_user_agent(user_agent: str | None = None, contact_email: str | None = None) -> str:
    explicit = user_agent or os.environ.get("SCIELO_USER_AGENT")
    if explicit:
        return explicit
    contact = contact_email_from_env(contact_email)
    parts = ["project=https://github.com/swiss-ai/apertus-program"]
    if contact:
        parts.insert(0, f"mailto:{contact}")
    run_id = os.environ.get("RUN_ID") or os.environ.get("RUNAI_JOB_NAME") or os.environ.get("HOSTNAME")
    if run_id:
        safe_run = re.sub(r"[^A-Za-z0-9._:-]+", "-", run_id)[:80]
        parts.append(f"run={safe_run}")
    return f"{DEFAULT_USER_AGENT_PRODUCT} ({'; '.join(parts)})"


def request_headers(
    user_agent: str | None = None,
    contact_email: str | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    contact = contact_email_from_env(contact_email)
    headers = {
        "User-Agent": build_user_agent(user_agent, contact),
        "Accept-Encoding": "identity",
    }
    if contact:
        headers["From"] = contact
    if extra:
        headers.update(extra)
    return headers


def parse_proxy_line(line: str) -> str:
    if "://" in line:
        return line
    if "@" in line:
        return "http://" + line.rstrip("/") + "/"
    parts = line.split(":")
    if len(parts) == 4:
        host, port, user, password = parts
        return f"http://{user}:{password}@{host}:{port}/"
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}/"
    return line


def proxy_urls_from_env() -> list[str]:
    urls: list[tuple[int, str]] = []
    seq = 0
    one = os.environ.get("PROXY_URL")
    if one:
        urls.append((seq, one.strip()))
        seq += 1
    proxy_file = os.environ.get("PROXY_FILE")
    if proxy_file:
        path = Path(proxy_file)
        if path.is_file():
            for raw in path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if line and not line.startswith("#"):
                    urls.append((seq, parse_proxy_line(line)))
                    seq += 1
    for key in ("HTTPS_PROXY", "HTTP_PROXY"):
        val = os.environ.get(key) or os.environ.get(key.lower())
        if val:
            urls.append((seq, val.strip()))
            seq += 1

    part_index_raw = os.environ.get("PROXY_PARTITION_INDEX")
    part_count_raw = os.environ.get("PROXY_PARTITION_COUNT")
    if part_index_raw or part_count_raw:
        try:
            part_index = int(part_index_raw or "")
            part_count = int(part_count_raw or "")
        except ValueError as exc:
            raise SystemExit("ERROR: PROXY_PARTITION_INDEX/COUNT must be integers") from exc
        if part_count < 1 or part_index < 0 or part_index >= part_count:
            raise SystemExit("ERROR: require 0 <= PROXY_PARTITION_INDEX < PROXY_PARTITION_COUNT")
        urls = [(i, url) for i, url in urls if i % part_count == part_index]

    out: list[str] = []
    seen: set[str] = set()
    for _, url in urls:
        if url and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def require_proxies() -> list[str]:
    proxies = proxy_urls_from_env()
    if not proxies:
        raise SystemExit(
            "ERROR: no proxy configured. Set PROXY_URL/PROXY_FILE/HTTPS_PROXY/HTTP_PROXY; "
            "direct university/cluster IP egress is intentionally disabled."
        )
    return proxies


def opener_for_proxy(proxy_url: str | None):
    if not proxy_url:
        return build_opener()
    return build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))


class RateLimiter:
    def __init__(self, rpm: int | None):
        self.rpm = rpm if rpm and rpm > 0 else None
        self.min_interval = 60.0 / self.rpm if self.rpm else 0.0
        self._next = 0.0
        self._lock = threading.Lock()

    def expected_wait(self) -> float:
        if not self.rpm:
            return 0.0
        with self._lock:
            now = time.monotonic()
            return max(0.0, self._next - now)

    def reserve_wait(self) -> float:
        if not self.rpm:
            return 0.0
        with self._lock:
            now = time.monotonic()
            start_at = max(now, self._next)
            self._next = start_at + self.min_interval
            return max(0.0, start_at - now)

    def wait(self) -> float:
        wait_s = self.reserve_wait()
        if wait_s > 0:
            time.sleep(wait_s)
        return wait_s


class SharedRateLimiter:
    def __init__(self, rpm: int | None, state_dir: Path, proxy_url: str):
        self.rpm = rpm if rpm and rpm > 0 else None
        self.min_interval = 60.0 / self.rpm if self.rpm else 0.0
        digest = hashlib.sha256(proxy_url.encode("utf-8")).hexdigest()[:24]
        self.path = state_dir / f"{digest}.rate"
        self._lock = threading.Lock()

    @staticmethod
    def _read_next(f) -> float:
        f.seek(0)
        raw = f.read().strip()
        if not raw:
            return 0.0
        try:
            return float(raw)
        except ValueError:
            return 0.0

    def expected_wait(self) -> float:
        if not self.rpm:
            return 0.0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self.path.open("a+", encoding="utf-8") as f:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return self.min_interval
                last_started = self._read_next(f)
                wait_s = max(0.0, last_started + self.min_interval - time.time())
                fcntl.flock(f, fcntl.LOCK_UN)
                return wait_s

    def reserve_wait(self) -> float:
        return self.wait()

    def wait(self) -> float:
        if not self.rpm:
            return 0.0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self.path.open("a+", encoding="utf-8") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                now = time.time()
                last_started = self._read_next(f)
                wait_s = max(0.0, last_started + self.min_interval - now)
                if wait_s > 0:
                    time.sleep(wait_s)
                f.seek(0)
                f.truncate()
                f.write(f"{time.time():.6f}\n")
                f.flush()
                fcntl.flock(f, fcntl.LOCK_UN)
                return wait_s


class ProxyPool:
    def __init__(self, proxies: list[str], rpm_per_proxy: int, shared_rate_dir: str | Path | None = None):
        if not proxies:
            raise ValueError("ProxyPool requires at least one proxy")
        self.proxies = proxies
        self.rpm_per_proxy = rpm_per_proxy
        shared_raw = shared_rate_dir or os.environ.get("PROXY_SHARED_RATE_DIR") or os.environ.get("SHARED_PROXY_RATE_DIR")
        self.shared_rate_dir = Path(shared_raw) if shared_raw else None
        limiter_cls = SharedRateLimiter if self.shared_rate_dir else RateLimiter
        self.limiters = {
            proxy: (
                limiter_cls(rpm_per_proxy, self.shared_rate_dir, proxy)
                if self.shared_rate_dir
                else limiter_cls(rpm_per_proxy)
            )
            for proxy in proxies
        }
        default_window = "16" if self.shared_rate_dir else "1"
        try:
            self.pick_window = max(1, int(os.environ.get("PROXY_PICK_WINDOW", default_window)))
        except ValueError:
            self.pick_window = int(default_window)
        seed = os.environ.get("PROXY_POOL_OFFSET") or os.environ.get("SHARD_ID") or os.environ.get("RUNAI_JOB_NAME") or ""
        self.offset = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16) if seed else random.randrange(len(proxies))

    def _candidate_indices(self, url_index: int, attempt: int) -> list[int]:
        n = len(self.proxies)
        window = min(n, self.pick_window)
        if window <= 1:
            return [(url_index + attempt + self.offset) % n]
        if self.shared_rate_dir:
            return random.sample(range(n), window)
        start = (url_index + attempt + self.offset) % n
        return [(start + i) % n for i in range(window)]

    def pick(self, url_index: int, attempt: int = 0) -> tuple[str, RateLimiter]:
        candidates = self._candidate_indices(url_index, attempt)
        best_idx = min(candidates, key=lambda i: (self.limiters[self.proxies[i]].expected_wait(), i))
        proxy = self.proxies[best_idx]
        return proxy, self.limiters[proxy]


def retry_sleep(attempt: int, base: float = 1.0, cap: float = 60.0, retry_after: float | None = None) -> None:
    if retry_after is not None:
        time.sleep(min(cap, retry_after))
        return
    wait = min(cap, base * (2**attempt))
    time.sleep(wait + random.uniform(0, min(1.0, wait * 0.25)))


def retry_after_seconds(headers) -> float | None:
    raw = headers.get("Retry-After") if headers else None
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def fetch_bytes(
    url: str,
    proxy_pool: ProxyPool,
    url_index: int,
    user_agent: str,
    contact_email: str | None,
    timeout: int = 90,
    retries: int = 3,
    extra_headers: dict[str, str] | None = None,
    read_limit: int | None = None,
) -> tuple[bytes, dict, str, int]:
    last_error = None
    for attempt in range(retries + 1):
        proxy, limiter = proxy_pool.pick(url_index, attempt)
        req = Request(url, headers=request_headers(user_agent, contact_email, extra_headers))
        try:
            waited = limiter.wait()
            if waited > 0:
                add_fetch_stat("proxy_wait_events")
                add_fetch_stat("proxy_wait_ms", int(waited * 1000))
            add_fetch_stat("requests_started")
            with opener_for_proxy(proxy).open(req, timeout=timeout) as resp:
                if read_limit is None:
                    data = resp.read()
                else:
                    data = resp.read(read_limit)
                add_fetch_stat("requests_ok")
                add_fetch_stat(f"http_status_{resp.getcode()}")
                add_fetch_stat("bytes_read", len(data))
                return data, dict(resp.headers.items()), resp.geturl(), resp.getcode()
        except HTTPError as e:
            add_fetch_stat("requests_http_error")
            add_fetch_stat(f"http_status_{e.code}")
            last_error = e
            retryable = e.code in TRANSIENT_HTTP or e.code in {403}
            retry_after = retry_after_seconds(e.headers)
            if attempt < retries and retryable:
                retry_sleep(attempt, retry_after=retry_after)
                continue
            raise
        except (URLError, TimeoutError, OSError, HTTPException) as e:
            add_fetch_stat("requests_network_error")
            add_fetch_stat(f"network_error_{type(e).__name__}")
            last_error = e
            if attempt < retries:
                retry_sleep(attempt)
                continue
            raise
    raise RuntimeError(f"unreachable fetch loop: {last_error}")


def stable_shard(value: str, n_shards: int) -> int:
    h = hashlib.md5(value.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") % n_shards


def safe_part(value: str, fallback: str = "x") -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value or "").strip("-")
    return value or fallback


def source_id(collection: str, pid: str, doi: str = "") -> str:
    base = f"{safe_part(collection, 'col')}-{safe_part(pid, '')}"
    if base.endswith("-"):
        base += hashlib.sha256((doi or pid).encode("utf-8")).hexdigest()[:16]
    return "scielo-" + base[:180]


def localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def text_of(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return " ".join("".join(el.itertext()).split())


def iter_by_local(root: ET.Element, *names: str):
    wanted = set(names)
    for el in root.iter():
        if localname(el.tag) in wanted:
            yield el


def first_by_local(root: ET.Element, *names: str) -> ET.Element | None:
    return next(iter_by_local(root, *names), None)


def attr_href(el: ET.Element) -> str:
    return (
        el.attrib.get(XLINK_HREF)
        or el.attrib.get("href")
        or el.attrib.get("xlink:href")
        or ""
    ).strip()


def url_basename(url: str) -> str:
    path = unquote(urlparse(url).path)
    base = path.rsplit("/", 1)[-1]
    base = re.sub(r"[^A-Za-z0-9._+ -]+", "_", base).strip()
    return base or hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def choose_fulltext(fulltexts: dict) -> tuple[str, str]:
    html_map = (fulltexts or {}).get("html") or {}
    if isinstance(html_map, str):
        return "und", html_map
    if not isinstance(html_map, dict):
        return "", ""
    for lang in ("en", "pt", "es"):
        if html_map.get(lang):
            return lang, html_map[lang]
    for lang, url in html_map.items():
        if url:
            return lang, url
    return "", ""


def xml_url_from_final(final_url: str, lang: str) -> str:
    split = urlsplit(final_url)
    query = parse_qs(split.query)
    chosen_lang = (lang or (query.get("lang") or query.get("tlng") or query.get("lng") or ["en"])[0] or "en")
    pid = (query.get("pid") or [""])[0]
    script = (query.get("script") or [""])[0]
    if split.path.endswith("/scielo.php") and pid and script == "sci_arttext":
        return urlunsplit((
            split.scheme,
            split.netloc,
            "/scieloOrg/php/articleXML.php",
            urlencode({"pid": pid, "lang": chosen_lang}),
            "",
        ))
    new_query = urlencode({"format": "xml", "lang": chosen_lang})
    return urlunsplit((split.scheme, split.netloc, split.path, new_query, ""))


def is_xml_response(data: bytes, headers: dict) -> bool:
    ctype = ""
    for k, v in headers.items():
        if k.lower() == "content-type":
            ctype = v.lower()
            break
    start = data.lstrip()[:128].lower()
    return "xml" in ctype or start.startswith((b"<?xml", b"<!doctype article", b"<article"))


def parse_xml_article(data: bytes) -> ET.Element:
    root = ET.fromstring(data)
    if localname(root.tag) != "article":
        raise ValueError(f"not_article:{localname(root.tag)}")
    return root


def normalize_license(text: str, urls: list[str]) -> tuple[str | None, str]:
    hay = html.unescape(" ".join([text or "", *urls])).lower()
    compact = re.sub(r"[^a-z0-9]+", "", hay)
    if not hay.strip():
        return None, "missing"
    blocked = (
        "bync",
        "noncommercial",
        "bynd",
        "noderivatives",
        "bysa",
        "sharealike",
    )
    if any(x in compact for x in blocked):
        return None, "skip"
    if "creativecommons.org/publicdomain/zero" in hay or "cc0" in hay:
        return "CC0", "keep"
    if "creativecommons.org/publicdomain/mark" in hay or "public domain" in hay:
        return "PD", "keep"
    if "creativecommons.org/licenses/by/" in hay or "creative commons attribution" in hay:
        return "CC BY", "keep"
    return None, "unknown"


def license_info(article: ET.Element) -> tuple[str | None, str, str, list[str]]:
    texts: list[str] = []
    urls: list[str] = []
    for lic in iter_by_local(article, "license"):
        texts.append(text_of(lic))
        href = attr_href(lic)
        if href:
            urls.append(href)
        for child in iter_by_local(lic, "ext-link", "uri"):
            href = attr_href(child) or text_of(child)
            if href:
                urls.append(href)
    text = " ".join(t for t in texts if t)
    code, policy = normalize_license(text, list(dict.fromkeys(urls)))
    return code, policy, text, list(dict.fromkeys(urls))


THIRD_PARTY_TERMS = (
    "adapted from",
    "modified from",
    "reprinted from",
    "reproduced from",
    "permission",
    "copyright",
    "courtesy",
    "source:",
)


def caption_flags(article: ET.Element) -> tuple[bool, list[str]]:
    found: set[str] = set()
    for container in iter_by_local(article, "fig", "table-wrap"):
        cap = first_by_local(container, "caption")
        if cap is None:
            continue
        txt = text_of(cap).lower()
        for term in THIRD_PARTY_TERMS:
            if term in txt:
                found.add(term)
    return bool(found), sorted(found)


def article_id(article: ET.Element, pub_id_type: str) -> str:
    for el in iter_by_local(article, "article-id"):
        if (el.attrib.get("pub-id-type") or "").lower() == pub_id_type:
            return text_of(el)
    return ""


def first_text(article: ET.Element, name: str) -> str:
    return text_of(first_by_local(article, name))


def media_urls(article: ET.Element, base_url: str) -> list[str]:
    urls: list[str] = []
    for el in article.iter():
        name = localname(el.tag)
        if name not in {"graphic", "inline-graphic", "media", "supplementary-material"}:
            continue
        href = attr_href(el)
        if not href:
            continue
        urls.append(urljoin(base_url, href))
    return list(dict.fromkeys(urls))


def rewrite_media(article: ET.Element, url_to_member: dict[str, str], base_url: str) -> None:
    for el in article.iter():
        if localname(el.tag) not in {"graphic", "inline-graphic", "media", "supplementary-material"}:
            continue
        href = attr_href(el)
        if not href:
            continue
        absolute = urljoin(base_url, href)
        member = url_to_member.get(absolute)
        if not member:
            continue
        if XLINK_HREF in el.attrib:
            el.set(XLINK_HREF, member)
        elif "href" in el.attrib:
            el.set("href", member)
        else:
            el.set(XLINK_HREF, member)


def media_member_name(prefix: str, idx: int, url: str) -> str:
    base = url_basename(url)
    if len(base) > 100:
        stem, dot, ext = base.rpartition(".")
        base = (stem[:80] + "." + ext[:16]) if dot else base[:96]
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}/figures/{idx:03d}-{digest}-{base}"
