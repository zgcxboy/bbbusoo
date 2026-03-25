#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
下载多个 Clash/Clash.Meta YAML 或 URI 订阅源，自动识别并合并成一个统一 YAML，再更新到 Cloud（底层使用 GitHub Gist）。
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from threading import Lock
from urllib.error import HTTPError
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import yaml

try:
    from bbbusoo_issue91_collector import (
        CollectorError,
        DEFAULT_GIST_ID,
        DEFAULT_GIST_TOKEN,
        DEFAULT_USER_AGENT,
        GITHUB_API_BASE,
        GITHUB_API_VERSION,
        extract_nodes_from_text,
        http_request_json,
        resolve_gist_token,
        validate_gist_filename,
    )
except ModuleNotFoundError:
    class CollectorError(RuntimeError):
        """收集与上传流程的统一异常类型。"""


    DEFAULT_GIST_ID = ""
    DEFAULT_GIST_TOKEN = ""
    DEFAULT_USER_AGENT = "bbbusoo-gist-uploader/1.0"
    GITHUB_API_BASE = "https://api.github.com"
    GITHUB_API_VERSION = "2022-11-28"
    NODE_URI_RE = re.compile(
        r"(?:(?:vmess|vless|ss|ssr|trojan|hysteria2|hy2|tuic|naive\+https)://[^\s\"'<>`]+)",
        flags=re.IGNORECASE,
    )

    def extract_nodes_from_text(content: str) -> list[str]:
        nodes: list[str] = []
        seen: set[str] = set()
        for match in NODE_URI_RE.finditer(str(content or "")):
            value = match.group(0).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            nodes.append(value)
        return nodes

    def resolve_gist_token(value: str | None) -> str:
        candidates = (
            value,
            os.environ.get("GIST_TOKEN", ""),
            os.environ.get("GITHUB_GIST_TOKEN", ""),
            os.environ.get("GITHUB_TOKEN", ""),
        )
        for candidate in candidates:
            normalized = str(candidate or "").strip()
            if normalized:
                return normalized
        return ""

    def validate_gist_filename(filename: str, arg_name: str) -> str:
        normalized = str(filename or "").strip()
        if not normalized:
            raise CollectorError(f"{arg_name} 不能为空")
        if "/" in normalized or "\\" in normalized:
            raise CollectorError(f"{arg_name} 不能包含路径分隔符")
        stem = Path(normalized).stem.casefold()
        suffix = Path(normalized).suffix.casefold()
        if stem.startswith("gistfile") and stem[8:].isdigit():
            raise CollectorError(f'{arg_name} 不能使用 Gist 保留文件名模式 "gistfileN"')
        if suffix == ".":
            raise CollectorError(f"{arg_name} 文件名后缀不合法")
        return normalized

    def http_request_json(
        *,
        url: str,
        method: str,
        timeout: int,
        headers: dict[str, str] | None = None,
        payload: dict[str, object] | None = None,
    ) -> dict | list:
        request_headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": DEFAULT_USER_AGENT,
        }
        if headers:
            request_headers.update(headers)

        request_body = None
        if payload is not None:
            request_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json; charset=utf-8")

        req = Request(
            url=url,
            headers=request_headers,
            data=request_body,
            method=str(method or "GET").upper(),
        )
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            message = detail or str(exc.reason or "").strip() or "未知错误"
            raise CollectorError(f"GitHub API 请求失败: HTTP {exc.code} {message}") from exc
        except Exception as exc:
            raise CollectorError(f"GitHub API 请求失败: {exc}") from exc

        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise CollectorError("GitHub API 返回的不是合法 JSON") from exc


DEFAULT_SOURCE_URLS = [
    "https://gist.githubusercontent.com/WLget/aeb222e378a8dcbd74e06413dbacf400/raw/all.yaml",
    "https://gist.githubusercontent.com/Methellli/28a02ed3c3085510415c07eb9fc238a2/raw/all.yaml",
    "https://gist.githubusercontent.com/ansir999/f785ca3a330b9e324664608c42b92048/raw/all.yaml",
    "https://gist.githubusercontent.com/BXXCAXCA/62d4baf00398ad57b8409b1de5bed85f/raw/clash_config.yaml",
    "https://gist.githubusercontent.com/liumingyu1234567-dotcom/bfe6a8f13ba616a7c53c7296819654f7/raw/clash.yaml",
    "https://gist.githubusercontent.com/JunfenZhang1/ad9a1adea520b1a4582e40c9073e6d0d/raw/all.yaml",
    "https://gist.githubusercontent.com/Tdison/6b745663fd038e3c63e0880ecc652bcf/raw/all.yaml",
    "https://gist.githubusercontent.com/viertagius/1c0d9c1679c8df4a35986c21233c1c2a/raw/clash_lite.txt",
    "https://gist.githubusercontent.com/murphyblu/0b7be82aedb4668f01da153d1fee2328/raw/clash.yaml",
    "https://gist.githubusercontent.com/m941032412/259406595455040919a5841008914242/raw/clash.yaml",
]
DEFAULT_TIMEOUT_SEC = 45
DEFAULT_OUTPUT_DIR = "."
DEFAULT_OUTPUT_PREFIX = "merged_clash_yaml"
DEFAULT_GIST_FILENAME = "merged_clash_meta.yaml"
DEFAULT_UPLOAD_GIST = True
DEFAULT_SAVE_LOCAL = False
DEFAULT_UPLOAD_FILE = ""
DEFAULT_EMIT_SOURCE_URLS_FILE = ""
DEFAULT_PROXY_FALLBACK_BASE = "https://proxy.api.030101.xyz/"
DEFAULT_DISCOVER_OUTPUT_PREFIX = "gist_recent_raw_urls"
DEFAULT_SEARCH_QUERIES = [
    "all.yaml",
    "clash.yaml",
    "mihomo.yaml",
    "sub",
    "subscribe",
    "proxies",
    "proxy",
    "vmess",
    "trojan",
    "vless",
    "hysteria",
    "hysteria2",
    "tuic",
    "simple-auth",
    "ss",
    "ssr",
    "v2ray",
    "xray",
    "reality",
    "naive",
    "hysteria2",
]

DEFAULT_DISCOVER_GIST_SEARCH = bool(DEFAULT_SEARCH_QUERIES)
DEFAULT_SEARCH_QUERY = "\n".join(DEFAULT_SEARCH_QUERIES)
DEFAULT_SEARCH_LANGUAGE = ""
DEFAULT_SEARCH_SORT = "updated"
DEFAULT_SEARCH_ORDER = "desc"
DEFAULT_SEARCH_START_PAGE = 1
DEFAULT_SEARCH_MAX_PAGES = 20
DEFAULT_SEARCH_WITHIN_HOURS = 48.0
DEFAULT_SEARCH_MAX_GISTS = 0
DEFAULT_SEARCH_MAX_RAW_FILES = 0
DEFAULT_MAX_WORKERS = 8
DEFAULT_SEARCH_QUERY_WORKERS = 2
DEFAULT_SEARCH_PAGE_DELAY_SECONDS = 1.0
DEFAULT_SEARCH_RATE_LIMIT_RETRIES = 3
DEFAULT_SEARCH_RATE_LIMIT_BACKOFF_SECONDS = 15.0
DEFAULT_FETCH_RETRY_COUNT = 3
DEFAULT_FETCH_RETRY_BACKOFF_SECONDS = 5.0
DEFAULT_DISCOVER_CONTENT_WORKERS = 4
RETRYABLE_HTTP_STATUS_CODES = {408, 429, 500, 502, 503, 504}
NOT_FOUND_RETRY_HTTP_STATUS_CODES = {404}
NOT_FOUND_RETRY_WAIT_SECONDS = 5.0
GIST_UPLOAD_RETRY_MARKERS = (
    "http 403",
    "http 408",
    "http 429",
    "http 500",
    "http 502",
    "http 503",
    "http 504",
    "secondary rate limit",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "spammed",
)
DEFAULT_GIST_UPLOAD_RETRIES = 2
DEFAULT_GIST_UPLOAD_BACKOFF_SECONDS = 20.0
RETRYABLE_ERROR_MARKERS = (
    "timed out",
    "timeout",
    "too many requests",
    "secondary rate limit",
    "unexpected eof",
    "connection reset",
    "connection aborted",
    "remote end closed",
    "temporarily unavailable",
    "temporary failure",
)
RAW_FILENAME_SUFFIXES = {".yaml", ".yml", ".txt", ".conf", ".cfg", ".sub", ".list"}
RAW_FILENAME_KEYWORDS = {
    "all",
    "base64",
    "clash",
    "config",
    "home",
    "hysteria",
    "hy2",
    "loon",
    "mihomo",
    "mixed",
    "node",
    "nodes",
    "point",
    "proxies",
    "proxy",
    "qx",
    "reality",
    "speed",
    "sub",
    "subscribe",
    "subscription",
    "surge",
    "trojan",
    "tuic",
    "v2ray",
    "vless",
    "vmess",
    "work",
    "xray",
}
GIST_SEARCH_BASE = "https://gist.github.com/search"
GIST_DETAIL_BASE = "https://gist.github.com"
GIST_RAW_BASE = "https://gist.githubusercontent.com"
SEARCH_RESULT_RE = re.compile(
    r'<a[^>]+href="/(?P<owner>[A-Za-z0-9_.-]+)/(?P<gist_id>[0-9a-f]{32})"[^>]*>\s*'
    r"<strong[^>]*>(?P<title>.*?)</strong>\s*</a>.*?"
    r'<relative-time[^>]+datetime="(?P<updated_at>[^"]+)"',
    flags=re.IGNORECASE | re.DOTALL,
)
RAW_LINK_RE = re.compile(
    r'href="(?P<path>/(?P<owner>[A-Za-z0-9_.-]+)/(?P<gist_id>[0-9a-f]{32})/raw/(?P<commit>[0-9a-f]+)/(?P<filename>[^"#?]+))"',
    flags=re.IGNORECASE,
)
URL_RE = re.compile(r"https?://[^\s\]]+", flags=re.IGNORECASE)
GIST_ID_RE = re.compile(r"\b[0-9a-f]{32}\b", flags=re.IGNORECASE)
GIST_OWNER_FIELD_RE = re.compile(r"(\bowner=)([A-Za-z0-9_.-]+)", flags=re.IGNORECASE)
GIST_OWNER_LOG_RE = re.compile(r"(\[GIST\]\s)([A-Za-z0-9_.-]+)")
PRINT_LOCK = Lock()


def sanitize_sensitive_text(message: str) -> str:
    def _mask_gist_id(value: str) -> str:
        text = str(value or "").strip()
        if len(text) <= 2:
            return "**"
        return f"{text[:-2]}**"

    def _mask_username(value: str) -> str:
        text = str(value or "").strip()
        if len(text) <= 2:
            return "**"
        return f"{text[:-2]}**"

    text = str(message or "")
    def _mask_middle(value: str, keep_start: int, keep_end: int) -> str:
        if len(value) <= keep_start + keep_end:
            if len(value) <= 4:
                return "*" * len(value)
            keep_start_local = max(1, len(value) // 3)
            keep_end_local = max(1, len(value) // 3)
            hidden_len = max(1, len(value) - keep_start_local - keep_end_local)
            return f"{value[:keep_start_local]}{'*' * hidden_len}{value[-keep_end_local:]}"
        hidden_len = len(value) - keep_start - keep_end
        return f"{value[:keep_start]}{'*' * hidden_len}{value[-keep_end:]}"

    text = GIST_OWNER_FIELD_RE.sub(lambda match: f"{match.group(1)}{_mask_username(match.group(2))}", text)
    text = GIST_OWNER_LOG_RE.sub(lambda match: f"{match.group(1)}{_mask_username(match.group(2))}", text)
    text = URL_RE.sub(lambda match: _mask_middle(match.group(0), 12, 10), text)
    text = GIST_ID_RE.sub(lambda match: _mask_gist_id(match.group(0)), text)
    return text


def log_line(message: str) -> None:
    text = str(message or "")
    # 搜索页 URL 仅用于核查抓取过程，不包含目标 gist ID/raw 订阅内容，保留原样输出。
    output = text if text.startswith("[SEARCH] ") else sanitize_sensitive_text(text)
    with PRINT_LOCK:
        print(output, flush=True)


def print_line(message: str) -> None:
    log_line(message)


def sleep_with_base_interval(base_seconds: float) -> None:
    interval = float(base_seconds or 0)
    if interval <= 0:
        return
    time.sleep(random.uniform(interval, interval + 1.0))


def should_consider_raw_filename(filename: str) -> bool:
    normalized = unquote(str(filename or "")).strip().casefold()
    if not normalized:
        return False
    suffix = Path(normalized).suffix
    if suffix in RAW_FILENAME_SUFFIXES:
        return True
    stem = Path(normalized).stem if suffix else normalized
    return any(keyword in stem for keyword in RAW_FILENAME_KEYWORDS)


def normalize_gist_raw_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.netloc != "gist.githubusercontent.com":
        return url

    match = re.match(r"^(?P<prefix>/[^/]+/[^/]+/raw)/(?P<commit>[^/]+)/(?P<filename>[^?#/]+)$", parts.path)
    if not match:
        return url

    clean_path = f"{match.group('prefix')}/{match.group('filename')}"
    return urlunsplit((parts.scheme, parts.netloc, clean_path, parts.query, parts.fragment))


def build_proxy_fallback_url(url: str) -> str:
    return f"{DEFAULT_PROXY_FALLBACK_BASE.rstrip('/')}/{url}"


def should_retry_via_proxy(url: str) -> bool:
    value = str(url or "").strip()
    if not value:
        return False
    if value.startswith(DEFAULT_PROXY_FALLBACK_BASE):
        return False
    parts = urlsplit(value)
    return parts.scheme in {"http", "https"} and bool(parts.netloc)


def _is_retryable_http_status(code: int | None) -> bool:
    return code in RETRYABLE_HTTP_STATUS_CODES


def _is_terminal_http_status(code: int | None) -> bool:
    return code is not None and 400 <= code < 500 and code not in RETRYABLE_HTTP_STATUS_CODES


def _should_retry_not_found_once(error_entries: list[dict], already_retried: bool) -> bool:
    if already_retried or not error_entries:
        return False
    if any(_is_retryable_http_status(entry.get("code")) for entry in error_entries):
        return False
    return any(entry.get("code") in NOT_FOUND_RETRY_HTTP_STATUS_CODES for entry in error_entries)


def _looks_retryable_error_message(message: str) -> bool:
    lowered = str(message or "").casefold()
    return any(marker in lowered for marker in RETRYABLE_ERROR_MARKERS)


def _format_request_error_summary(error_entry: dict) -> str:
    mode = error_entry.get("mode", "direct")
    code = error_entry.get("code")
    if code is not None:
        reason = str(error_entry.get("reason", "")).strip()
        if reason:
            return f"{mode}: HTTP {code} {reason}"
        return f"{mode}: HTTP {code}"
    summary = str(error_entry.get("summary", "")).strip() or str(error_entry.get("detail", "")).strip()
    return f"{mode}: {summary}"


def _format_request_error_detail(error_entry: dict) -> str:
    mode = error_entry.get("mode", "direct")
    code = error_entry.get("code")
    if code is not None:
        detail = str(error_entry.get("detail", "")).strip() or str(error_entry.get("reason", "")).strip()
        if detail:
            return f"{mode}: HTTP {code} {detail}"
        return f"{mode}: HTTP {code}"
    detail = str(error_entry.get("detail", "")).strip() or str(error_entry.get("summary", "")).strip()
    return f"{mode}: {detail}"


def _should_retry_request_errors(error_entries: list[dict]) -> bool:
    if not error_entries:
        return False
    if any(_is_terminal_http_status(entry.get("code")) for entry in error_entries):
        return False
    if any(_is_retryable_http_status(entry.get("code")) for entry in error_entries):
        return True
    return any(
        _looks_retryable_error_message(entry.get("detail", "")) or _looks_retryable_error_message(entry.get("summary", ""))
        for entry in error_entries
    )


def _download_text_source_with_retries(
    *,
    url: str,
    timeout: int,
    accept: str,
    failure_prefix: str,
    empty_prefix: str,
    retry_count: int,
    retry_backoff_seconds: float,
    retry_context: str,
) -> tuple[str, dict]:
    request_candidates: list[tuple[str, str]] = [("direct", url)]
    if should_retry_via_proxy(url):
        request_candidates.append(("proxy", build_proxy_fallback_url(url)))

    base_attempts = max(0, retry_count) + 1
    attempts = base_attempts + 1
    last_errors: list[dict] = []
    not_found_retry_used = False
    for attempt in range(1, attempts + 1):
        errors: list[dict] = []
        for mode, request_url in request_candidates:
            req = Request(
                url=request_url,
                headers={
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Accept": accept,
                },
                method="GET",
            )
            try:
                with urlopen(req, timeout=timeout) as resp:
                    content = resp.read().decode("utf-8", errors="replace")
                    if not content.strip():
                        raise CollectorError(f"{empty_prefix}: {request_url}")
                    meta = {
                        "source_url": url,
                        "request_url": request_url,
                        "final_url": resp.geturl(),
                        "content_type": resp.headers.get("Content-Type", ""),
                        "content_length": resp.headers.get("Content-Length", ""),
                        "used_proxy": mode == "proxy",
                        "request_attempt": attempt,
                        "retry_count": retry_count,
                    }
                    if mode == "proxy":
                        meta["proxy_url"] = request_url
                    return content, meta
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace").strip()
                errors.append(
                    {
                        "mode": mode,
                        "code": exc.code,
                        "reason": str(exc.reason or "").strip(),
                        "summary": str(exc.reason or "").strip(),
                        "detail": detail or str(exc.reason or "").strip(),
                    }
                )
            except CollectorError as exc:
                detail = str(exc)
                errors.append(
                    {
                        "mode": mode,
                        "code": None,
                        "reason": "",
                        "summary": detail,
                        "detail": detail,
                    }
                )
            except Exception as exc:
                detail = str(exc)
                errors.append(
                    {
                        "mode": mode,
                        "code": None,
                        "reason": "",
                        "summary": detail,
                        "detail": detail,
                    }
                )

            if mode == "direct" and len(request_candidates) > 1:
                log_line(f"[WARN] direct fetch failed, retry via proxy: {url}")

        last_errors = errors
        retry_not_found_once = _should_retry_not_found_once(errors, not_found_retry_used)
        if retry_not_found_once:
            not_found_retry_used = True
            summary = "; ".join(_format_request_error_summary(entry) for entry in errors)
            log_line(
                f"[WARN] {retry_context}返回404，{NOT_FOUND_RETRY_WAIT_SECONDS:.0f}秒后重试一次: "
                f"attempt={attempt}/{attempts} url={url} error={summary}"
            )
            time.sleep(NOT_FOUND_RETRY_WAIT_SECONDS)
            continue

        if attempt >= base_attempts or not _should_retry_request_errors(errors):
            break
        wait_seconds = retry_backoff_seconds * attempt
        summary = "; ".join(_format_request_error_summary(entry) for entry in errors)
        log_line(
            f"[WARN] {retry_context}失败，{wait_seconds:.0f}秒后重试: "
            f"attempt={attempt}/{attempts} url={url} error={summary}"
        )
        time.sleep(wait_seconds)

    detail_message = "; ".join(_format_request_error_detail(entry) for entry in last_errors)
    raise CollectorError(f"{failure_prefix}: {detail_message} [{url}]")

def download_text_source(url: str, timeout: int, accept: str = "text/plain,*/*") -> tuple[str, dict]:
    return _download_text_source_with_retries(
        url=url,
        timeout=timeout,
        accept=accept,
        failure_prefix="下载内容失败",
        empty_prefix="下载内容为空",
        retry_count=DEFAULT_FETCH_RETRY_COUNT,
        retry_backoff_seconds=DEFAULT_FETCH_RETRY_BACKOFF_SECONDS,
        retry_context="下载内容",
    )


def fetch_search_page_html(
    *,
    search_url: str,
    query: str,
    page: int,
    timeout: int,
) -> tuple[str, dict]:
    return _download_text_source_with_retries(
        url=search_url,
        timeout=timeout,
        accept="text/html,*/*",
        failure_prefix="下载内容失败",
        empty_prefix="下载内容为空",
        retry_count=DEFAULT_SEARCH_RATE_LIMIT_RETRIES,
        retry_backoff_seconds=DEFAULT_SEARCH_RATE_LIMIT_BACKOFF_SECONDS,
        retry_context=f"搜索页抓取 query={query} page={page}",
    )


def normalize_source_urls(values: list[str] | None) -> list[str]:
    raw_items = values or []
    normalized: list[str] = []
    seen: set[str] = set()

    for item in raw_items:
        for part in re.split(r"[\r\n,]+", str(item or "").strip()):
            url = normalize_gist_raw_url(part.strip())
            if not url or url in seen:
                continue
            normalized.append(url)
            seen.add(url)
    return normalized


def normalize_search_queries(values: list[str] | None) -> list[str]:
    raw_items = values or []
    normalized: list[str] = []
    seen: set[str] = set()

    for item in raw_items:
        for part in re.split(r"[\r\n,]+", str(item or "").strip()):
            query = part.strip()
            if not query:
                continue
            key = query.casefold()
            if key in seen:
                continue
            normalized.append(query)
            seen.add(key)
    return normalized


def configure_stdio_utf8() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            continue


def download_yaml_source(url: str, timeout: int) -> tuple[str, dict]:
    return _download_text_source_with_retries(
        url=url,
        timeout=timeout,
        accept="application/x-yaml,text/yaml,text/plain,*/*",
        failure_prefix="下载源失败",
        empty_prefix="下载源内容为空",
        retry_count=DEFAULT_FETCH_RETRY_COUNT,
        retry_backoff_seconds=DEFAULT_FETCH_RETRY_BACKOFF_SECONDS,
        retry_context="下载源",
    )


def parse_iso_datetime(value: str) -> datetime:
    normalized = (value or "").strip()
    if not normalized:
        raise CollectorError("时间字段为空")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise CollectorError(f"无法解析时间: {value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_gist_search_url(
    *,
    query: str,
    language: str,
    sort: str,
    order: str,
    page: int,
) -> str:
    params = {
        "q": query,
        "s": sort,
        "o": order,
    }
    if str(language or "").strip():
        params["l"] = str(language).strip()
    if page > 1:
        params["p"] = str(page)
    return f"{GIST_SEARCH_BASE}?{urlencode(params)}"


def parse_gist_search_results(html_text: str) -> list[dict]:
    results: list[dict] = []
    seen: set[str] = set()
    for match in SEARCH_RESULT_RE.finditer(html_text or ""):
        gist_id = (match.group("gist_id") or "").strip()
        if not gist_id or gist_id in seen:
            continue
        owner = unescape((match.group("owner") or "").strip())
        title = unescape((match.group("title") or "").strip())
        updated_at = parse_iso_datetime(match.group("updated_at") or "")
        results.append(
            {
                "owner": owner,
                "gist_id": gist_id,
                "title": title,
                "updated_at": updated_at,
                "detail_url": f"{GIST_DETAIL_BASE}/{owner}/{gist_id}",
            }
        )
        seen.add(gist_id)
    return results


def extract_gist_raw_links_from_html(html_text: str) -> list[dict]:
    raw_items: list[dict] = []
    seen: set[str] = set()
    for match in RAW_LINK_RE.finditer(html_text or ""):
        rel_path = (match.group("path") or "").strip()
        if not rel_path:
            continue
        filename = unquote((match.group("filename") or "").strip())
        if not should_consider_raw_filename(filename):
            continue
        raw_url_with_commit = f"{GIST_RAW_BASE}{rel_path}"
        raw_url = normalize_gist_raw_url(raw_url_with_commit)
        if raw_url in seen:
            continue
        raw_items.append(
            {
                "owner": (match.group("owner") or "").strip(),
                "gist_id": (match.group("gist_id") or "").strip(),
                "filename": filename,
                "raw_url_with_commit": raw_url_with_commit,
                "raw_url": raw_url,
            }
        )
        seen.add(raw_url)
    return raw_items


def inspect_node_share_content(content: str, source_url: str) -> dict:
    try:
        yaml_proxies = load_proxies_from_yaml(content, source_url)
    except CollectorError:
        yaml_proxies = []

    if yaml_proxies:
        return {
            "matched": True,
            "kind": "clash_yaml",
            "proxy_count": len(yaml_proxies),
            "uri_count": 0,
        }

    try:
        client_proxies, client_format, _ = load_proxies_from_client_config(content, source_url)
    except CollectorError:
        client_proxies = []
        client_format = ""

    if client_proxies:
        return {
            "matched": True,
            "kind": client_format,
            "proxy_count": len(client_proxies),
            "uri_count": 0,
        }

    uri_nodes = extract_nodes_from_text(content)
    valid_uri_count = 0
    for node in uri_nodes:
        proxy, _ = parse_proxy_uri(node)
        if proxy is not None:
            valid_uri_count += 1
    if valid_uri_count:
        return {
            "matched": True,
            "kind": "node_uri",
            "proxy_count": 0,
            "uri_count": valid_uri_count,
        }

    return {
        "matched": False,
        "kind": "",
        "proxy_count": 0,
        "uri_count": 0,
    }


def fetch_source_result(index: int, source_url: str, timeout: int) -> dict:
    yaml_text, meta = download_yaml_source(source_url, timeout=timeout)
    proxies, source_format, parse_stats = load_proxies_from_mixed_content(yaml_text, source_url=source_url)
    meta["source_format"] = source_format
    meta["parse_stats"] = parse_stats
    return {
        "index": index,
        "source_url": source_url,
        "yaml_text": yaml_text,
        "meta": meta,
        "proxies": proxies,
    }


def fetch_gist_detail_result(index: int, gist_item: dict, timeout: int) -> dict:
    detail_html, _ = download_text_source(
        gist_item["detail_url"],
        timeout=timeout,
        accept="text/html,*/*",
    )
    sleep_with_base_interval(DEFAULT_SEARCH_PAGE_DELAY_SECONDS)
    return {
        "index": index,
        "gist_item": gist_item,
        "raw_items": extract_gist_raw_links_from_html(detail_html),
    }


def inspect_gist_raw_result(task_index: int, gist_item: dict, raw_item: dict, timeout: int) -> dict:
    raw_text, raw_meta = download_text_source(
        raw_item["raw_url"],
        timeout=timeout,
        accept="application/x-yaml,text/yaml,text/plain,*/*",
    )
    inspect_result = inspect_node_share_content(raw_text, raw_item["raw_url"])
    sleep_with_base_interval(DEFAULT_SEARCH_PAGE_DELAY_SECONDS)
    return {
        "task_index": task_index,
        "gist_item": gist_item,
        "raw_item": raw_item,
        "raw_meta": raw_meta,
        "inspect_result": inspect_result,
    }


def load_proxies_from_yaml(yaml_text: str, source_url: str) -> list[dict]:
    try:
        parsed = yaml.safe_load(yaml_text)
    except Exception as exc:
        raise CollectorError(f"YAML 解析失败: {source_url}: {exc}") from exc

    if not isinstance(parsed, dict):
        raise CollectorError(f"YAML 顶层不是对象: {source_url}")

    proxies = parsed.get("proxies", []) or []
    if not isinstance(proxies, list):
        raise CollectorError(f"proxies 字段不是列表: {source_url}")

    normalized: list[dict] = []
    for item in proxies:
        if isinstance(item, dict):
            normalized.append(dict(item))
    return normalized


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


def _get_param(params: dict[str, list[str]], *names: str) -> str:
    for name in names:
        values = params.get(name, [])
        if values:
            return values[0]
    return ""


def _decode_b64(data: str) -> str:
    raw = "".join((data or "").strip().split())
    if not raw:
        return ""
    padding = (-len(raw)) % 4
    if padding:
        raw += "=" * padding
    try:
        return base64.urlsafe_b64decode(raw.encode("utf-8")).decode("utf-8", errors="replace")
    except Exception:
        try:
            return base64.b64decode(raw.encode("utf-8")).decode("utf-8", errors="replace")
        except Exception:
            return ""


def _build_name(parts, fallback: str) -> str:
    fragment = unquote(parts.fragment or "").strip()
    return fragment or fallback


def _build_ws_opts(params: dict[str, list[str]]) -> dict:
    path = _get_param(params, "path")
    host = _get_param(params, "host")
    headers = {}
    if host:
        headers["Host"] = host
    result: dict[str, object] = {}
    if path:
        result["path"] = path
    if headers:
        result["headers"] = headers
    return result


def _build_grpc_opts(params: dict[str, list[str]]) -> dict:
    service_name = _get_param(params, "serviceName", "grpc-service-name")
    result: dict[str, object] = {}
    if service_name:
        result["grpc-service-name"] = service_name
    return result


def _parse_ss_uri(node: str) -> dict | None:
    raw = node[len("ss://") :]
    fragment = ""
    if "#" in raw:
        raw, fragment = raw.split("#", 1)
    query = ""
    if "?" in raw:
        raw, query = raw.split("?", 1)

    userinfo = ""
    server_part = ""
    if "@" in raw:
        userinfo, server_part = raw.rsplit("@", 1)
        decoded_userinfo = _decode_b64(userinfo)
        if decoded_userinfo:
            userinfo = decoded_userinfo
    else:
        decoded = _decode_b64(raw)
        if "@" in decoded:
            userinfo, server_part = decoded.rsplit("@", 1)
        else:
            return None

    if ":" not in userinfo or ":" not in server_part:
        return None
    cipher, password = userinfo.split(":", 1)
    server, port_text = server_part.rsplit(":", 1)

    proxy: dict[str, object] = {
        "name": unquote(fragment) or f"ss_{server}:{port_text}",
        "type": "ss",
        "server": server.strip("[]"),
        "port": int(port_text),
        "cipher": cipher,
        "password": unquote(password),
    }

    params = parse_qs(query, keep_blank_values=True)
    plugin_raw = _get_param(params, "plugin")
    if plugin_raw:
        plugin_parts = [part for part in plugin_raw.split(";") if part]
        if plugin_parts:
            proxy["plugin"] = plugin_parts[0]
            plugin_opts: dict[str, object] = {}
            for item in plugin_parts[1:]:
                if "=" in item:
                    key, value = item.split("=", 1)
                    plugin_opts[key] = unquote(value)
                else:
                    plugin_opts[item] = True
            if plugin_opts:
                proxy["plugin-opts"] = plugin_opts
    return proxy


def _parse_vmess_uri(node: str) -> dict | None:
    payload = _decode_b64(node[len("vmess://") :])
    if not payload:
        return None
    try:
        data = json.loads(payload)
    except Exception:
        return None

    server = str(data.get("add") or "").strip()
    port = str(data.get("port") or "").strip()
    uuid = str(data.get("id") or "").strip()
    if not server or not port or not uuid:
        return None

    net = str(data.get("net") or "tcp").strip() or "tcp"
    proxy: dict[str, object] = {
        "name": str(data.get("ps") or f"vmess_{server}:{port}"),
        "type": "vmess",
        "server": server,
        "port": int(port),
        "uuid": uuid,
        "alterId": int(str(data.get("aid") or "0") or "0"),
        "cipher": "auto",
        "network": net,
    }
    if str(data.get("tls") or "").strip():
        proxy["tls"] = True
    if str(data.get("sni") or "").strip():
        proxy["servername"] = str(data.get("sni")).strip()
    host = str(data.get("host") or "").strip()
    path = str(data.get("path") or "").strip()
    if net == "ws":
        ws_opts: dict[str, object] = {}
        if path:
            ws_opts["path"] = path
        if host:
            ws_opts["headers"] = {"Host": host}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif net == "grpc" and path:
        proxy["grpc-opts"] = {"grpc-service-name": path}
    return proxy


def _parse_trojan_uri(node: str) -> dict | None:
    parts = urlsplit(node)
    if not parts.hostname or not parts.port or not parts.username:
        return None
    params = parse_qs(parts.query, keep_blank_values=True)
    network = _get_param(params, "type") or "tcp"
    proxy: dict[str, object] = {
        "name": _build_name(parts, f"trojan_{parts.hostname}:{parts.port}"),
        "type": "trojan",
        "server": parts.hostname,
        "port": parts.port,
        "password": unquote(parts.username),
    }
    sni = _get_param(params, "sni", "peer")
    if sni:
        proxy["sni"] = sni
    insecure = _parse_bool(_get_param(params, "allowInsecure", "insecure", "skip-cert-verify"))
    if insecure is not None:
        proxy["skip-cert-verify"] = insecure
    if network and network != "tcp":
        proxy["network"] = network
    if network == "ws":
        ws_opts = _build_ws_opts(params)
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        grpc_opts = _build_grpc_opts(params)
        if grpc_opts:
            proxy["grpc-opts"] = grpc_opts
    return proxy


def _parse_vless_uri(node: str) -> dict | None:
    parts = urlsplit(node)
    if not parts.hostname or not parts.port or not parts.username:
        return None
    params = parse_qs(parts.query, keep_blank_values=True)
    network = _get_param(params, "type") or "tcp"
    security = (_get_param(params, "security") or "").strip().lower()
    proxy: dict[str, object] = {
        "name": _build_name(parts, f"vless_{parts.hostname}:{parts.port}"),
        "type": "vless",
        "server": parts.hostname,
        "port": parts.port,
        "uuid": unquote(parts.username),
        "network": network,
        "udp": True,
    }
    if security in {"tls", "reality"}:
        proxy["tls"] = True
    if security == "reality":
        reality_opts: dict[str, object] = {}
        public_key = _get_param(params, "pbk")
        short_id = _get_param(params, "sid")
        if public_key:
            reality_opts["public-key"] = public_key
        if short_id:
            reality_opts["short-id"] = short_id
        if reality_opts:
            proxy["reality-opts"] = reality_opts
    servername = _get_param(params, "sni", "servername")
    if servername:
        proxy["servername"] = servername
    fingerprint = _get_param(params, "fp")
    if fingerprint:
        proxy["client-fingerprint"] = fingerprint
    flow = _get_param(params, "flow")
    if flow:
        proxy["flow"] = flow
    alpn = _get_param(params, "alpn")
    if alpn:
        proxy["alpn"] = [item for item in alpn.split(",") if item]
    if network == "ws":
        ws_opts = _build_ws_opts(params)
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        grpc_opts = _build_grpc_opts(params)
        if grpc_opts:
            proxy["grpc-opts"] = grpc_opts
    return proxy


def _parse_hysteria2_uri(node: str) -> dict | None:
    parts = urlsplit(node)
    if not parts.hostname or not parts.port or not parts.username:
        return None
    params = parse_qs(parts.query, keep_blank_values=True)
    proxy: dict[str, object] = {
        "name": _build_name(parts, f"hysteria2_{parts.hostname}:{parts.port}"),
        "type": "hysteria2",
        "server": parts.hostname,
        "port": parts.port,
        "password": unquote(parts.username),
    }
    sni = _get_param(params, "sni")
    if sni:
        proxy["sni"] = sni
    insecure = _parse_bool(_get_param(params, "insecure", "skip-cert-verify"))
    if insecure is not None:
        proxy["skip-cert-verify"] = insecure
    obfs = _get_param(params, "obfs")
    if obfs:
        proxy["obfs"] = obfs
    obfs_password = _get_param(params, "obfs-password")
    if obfs_password:
        proxy["obfs-password"] = obfs_password
    return proxy


def _parse_hysteria_uri(node: str) -> dict | None:
    parts = urlsplit(node)
    if not parts.hostname or not parts.port:
        return None
    params = parse_qs(parts.query, keep_blank_values=True)
    auth = unquote(parts.username or _get_param(params, "auth", "auth-str"))
    proxy: dict[str, object] = {
        "name": _build_name(parts, f"hysteria_{parts.hostname}:{parts.port}"),
        "type": "hysteria",
        "server": parts.hostname,
        "port": parts.port,
    }
    if auth:
        proxy["auth-str"] = auth
    sni = _get_param(params, "sni", "peer")
    if sni:
        proxy["sni"] = sni
    insecure = _parse_bool(_get_param(params, "insecure", "skip-cert-verify"))
    if insecure is not None:
        proxy["skip-cert-verify"] = insecure
    up = _get_param(params, "upmbps", "up")
    down = _get_param(params, "downmbps", "down")
    if up:
        proxy["up"] = up
    if down:
        proxy["down"] = down
    return proxy


def _parse_tuic_uri(node: str) -> dict | None:
    parts = urlsplit(node)
    if not parts.hostname or not parts.port or not parts.username:
        return None
    params = parse_qs(parts.query, keep_blank_values=True)
    proxy: dict[str, object] = {
        "name": _build_name(parts, f"tuic_{parts.hostname}:{parts.port}"),
        "type": "tuic",
        "server": parts.hostname,
        "port": parts.port,
        "uuid": unquote(parts.username),
    }
    if parts.password:
        proxy["password"] = unquote(parts.password)
    congestion = _get_param(params, "congestion_control")
    if congestion:
        proxy["congestion-controller"] = congestion
    sni = _get_param(params, "sni")
    if sni:
        proxy["sni"] = sni
    insecure = _parse_bool(_get_param(params, "allow_insecure", "insecure", "skip-cert-verify"))
    if insecure is not None:
        proxy["skip-cert-verify"] = insecure
    alpn = _get_param(params, "alpn")
    if alpn:
        proxy["alpn"] = [item for item in alpn.split(",") if item]
    return proxy


def _parse_simple_auth_uri(node: str, proxy_type: str) -> dict | None:
    parts = urlsplit(node)
    if not parts.hostname or not parts.port:
        return None
    proxy: dict[str, object] = {
        "name": _build_name(parts, f"{proxy_type}_{parts.hostname}:{parts.port}"),
        "type": proxy_type,
        "server": parts.hostname,
        "port": parts.port,
    }
    if parts.username:
        proxy["username"] = unquote(parts.username)
    if parts.password:
        proxy["password"] = unquote(parts.password)
    return proxy


def parse_proxy_uri(node: str) -> tuple[dict | None, str]:
    lowered = node.lower()
    try:
        if lowered.startswith("ss://"):
            return _parse_ss_uri(node), "ss"
        if lowered.startswith("vmess://"):
            return _parse_vmess_uri(node), "vmess"
        if lowered.startswith("vless://"):
            return _parse_vless_uri(node), "vless"
        if lowered.startswith("trojan://"):
            return _parse_trojan_uri(node), "trojan"
        if lowered.startswith("hysteria2://"):
            return _parse_hysteria2_uri(node), "hysteria2"
        if lowered.startswith("hysteria://"):
            return _parse_hysteria_uri(node), "hysteria"
        if lowered.startswith("tuic://"):
            return _parse_tuic_uri(node), "tuic"
        if lowered.startswith("socks5://"):
            return _parse_simple_auth_uri(node, "socks5"), "socks5"
        if lowered.startswith("https://"):
            proxy = _parse_simple_auth_uri(node, "http")
            if proxy is not None:
                proxy["tls"] = True
            return proxy, "http"
        if lowered.startswith("http://"):
            return _parse_simple_auth_uri(node, "http"), "http"
    except Exception:
        return None, lowered.split("://", 1)[0]
    return None, lowered.split("://", 1)[0]


SURGE_LIKE_PROXY_SECTIONS = {"proxy", "proxy_local", "proxy-local", "proxy local"}
QUANTUMULT_X_PROXY_SECTIONS = {"server_local", "server_remote", "server-local", "server-remote"}
SUPPORTED_CLIENT_PROXY_TYPES = {
    "ss",
    "shadowsocks",
    "vmess",
    "vless",
    "trojan",
    "http",
    "https",
    "socks5",
    "hysteria",
    "hysteria2",
    "hy2",
    "tuic",
}


def _normalize_option_key(name: str) -> str:
    return str(name or "").strip().casefold().replace("_", "-")


def _clean_config_value(value: str) -> str:
    return unquote(str(value or "").strip().strip('"').strip("'"))


def _split_config_fields(value: str) -> list[str]:
    fields: list[str] = []
    current: list[str] = []
    quote = ""
    for char in str(value or ""):
        if char in {'"', "'"}:
            if quote == char:
                quote = ""
            elif not quote:
                quote = char
            current.append(char)
            continue
        if char == "," and not quote:
            field = "".join(current).strip()
            if field:
                fields.append(field)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        fields.append(tail)
    return fields


def _parse_config_tokens(tokens: list[str], positional_limit: int = 0) -> tuple[list[str], dict[str, str]]:
    positional: list[str] = []
    options: dict[str, str] = {}
    last_option_key = ""
    for token in tokens:
        item = str(token or "").strip()
        if not item:
            continue
        if "=" in item:
            key, value = item.split("=", 1)
            normalized_key = _normalize_option_key(key)
            options[normalized_key] = _clean_config_value(value)
            last_option_key = normalized_key
            continue
        cleaned = _clean_config_value(item)
        if len(positional) < positional_limit:
            positional.append(cleaned)
            last_option_key = ""
            continue
        if last_option_key:
            options[last_option_key] = ",".join(
                part for part in [options.get(last_option_key, ""), cleaned] if part
            )
            continue
        positional.append(cleaned)
    return positional, options


def _get_option(options: dict[str, str], *names: str) -> str:
    for name in names:
        key = _normalize_option_key(name)
        value = str(options.get(key, "")).strip()
        if value:
            return value
    return ""


def _parse_int_value(value: str | None) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _split_host_port(value: str) -> tuple[str, int] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.startswith("[") and "]:" in raw:
        host, port_text = raw[1:].split("]:", 1)
    elif ":" in raw:
        host, port_text = raw.rsplit(":", 1)
    else:
        return None
    port = _parse_int_value(port_text)
    if not host or port is None:
        return None
    return host.strip("[]"), port


def _normalize_client_type(proxy_type: str) -> str:
    normalized = str(proxy_type or "").strip().casefold()
    if normalized == "shadowsocks":
        return "ss"
    if normalized == "hy2":
        return "hysteria2"
    if normalized == "https":
        return "http"
    return normalized


def _normalize_network(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    if normalized in {"websocket", "wss", "ws"}:
        return "ws"
    if normalized in {"grpc", "gun"}:
        return "grpc"
    return normalized


def _detect_client_network(options: dict[str, str]) -> str:
    explicit = _normalize_network(_get_option(options, "network", "transport", "type"))
    if explicit:
        return explicit
    if _parse_bool(_get_option(options, "ws")) is True:
        return "ws"
    if _parse_bool(_get_option(options, "grpc")) is True:
        return "grpc"
    obfs = _normalize_network(_get_option(options, "obfs"))
    if obfs in {"ws", "grpc"}:
        return obfs
    return "tcp"


def _extract_host_from_headers(value: str) -> str:
    for item in re.split(r"[|;]", str(value or "")):
        if ":" not in item:
            continue
        key, header_value = item.split(":", 1)
        if key.strip().casefold() == "host":
            return _clean_config_value(header_value)
    return ""


def _apply_skip_cert_verify(proxy: dict[str, object], options: dict[str, str]) -> None:
    skip = _parse_bool(_get_option(options, "skip-cert-verify", "insecure", "allow-insecure", "allowinsecure"))
    verify = _parse_bool(_get_option(options, "tls-verification", "tls-verify"))
    if skip is None and verify is not None:
        skip = not verify
    if skip is not None:
        proxy["skip-cert-verify"] = skip


def _apply_ws_or_grpc_options(proxy: dict[str, object], options: dict[str, str], network: str) -> None:
    if network == "ws":
        host = _get_option(options, "ws-host", "host", "obfs-host")
        ws_headers = _get_option(options, "ws-headers", "ws-header", "headers")
        if not host and ws_headers:
            host = _extract_host_from_headers(ws_headers)
        path = _get_option(options, "ws-path", "path", "obfs-uri")
        ws_opts: dict[str, object] = {}
        if path:
            ws_opts["path"] = path
        if host:
            ws_opts["headers"] = {"Host": host}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        service_name = _get_option(options, "grpc-service-name", "service-name", "servicename")
        if service_name:
            proxy["grpc-opts"] = {"grpc-service-name": service_name}


def _apply_tls_fields(proxy: dict[str, object], options: dict[str, str], *, sni_field: str) -> None:
    tls_enabled = False
    tls_flag = _parse_bool(_get_option(options, "tls", "over-tls", "tls-enable"))
    if tls_flag is True:
        tls_enabled = True
    security = _get_option(options, "security").casefold()
    if security in {"tls", "reality"}:
        tls_enabled = True
    if _normalize_network(_get_option(options, "obfs")) == "ws" and str(_get_option(options, "obfs")).casefold() == "wss":
        tls_enabled = True
    if tls_enabled:
        proxy["tls"] = True
    sni = _get_option(options, "sni", "peer", "tls-host", "servername", "server-name")
    if not sni and tls_enabled:
        sni = _get_option(options, "obfs-host")
    if sni:
        proxy[sni_field] = sni
    alpn = _get_option(options, "alpn")
    if alpn:
        proxy["alpn"] = [item.strip() for item in alpn.split(",") if item.strip()]
    client_fp = _get_option(options, "client-fingerprint", "fingerprint", "fp")
    if client_fp:
        proxy["client-fingerprint"] = client_fp
    _apply_skip_cert_verify(proxy, options)


def _build_client_proxy(
    *,
    name: str,
    proxy_type: str,
    server: str,
    port: int,
    options: dict[str, str],
    positional: list[str],
) -> dict | None:
    normalized_type = _normalize_client_type(proxy_type)
    if normalized_type not in {_normalize_client_type(item) for item in SUPPORTED_CLIENT_PROXY_TYPES}:
        return None

    if normalized_type == "ss":
        method = _get_option(options, "encrypt-method", "method", "cipher") or (positional[0] if positional else "")
        password = _get_option(options, "password", "passwd") or (positional[1] if len(positional) > 1 else "")
        if not method or not password:
            return None
        proxy: dict[str, object] = {
            "name": name,
            "type": "ss",
            "server": server,
            "port": port,
            "cipher": method,
            "password": password,
        }
        plugin = _get_option(options, "plugin")
        obfs = str(_get_option(options, "obfs")).casefold()
        if plugin or obfs in {"ws", "wss"}:
            proxy["plugin"] = plugin or "v2ray-plugin"
            plugin_opts: dict[str, object] = {}
            if obfs in {"ws", "wss"}:
                plugin_opts["mode"] = "websocket"
                if obfs == "wss":
                    plugin_opts["tls"] = True
            host = _get_option(options, "plugin-host", "obfs-host", "host")
            path = _get_option(options, "plugin-path", "obfs-uri", "path", "ws-path")
            if host:
                plugin_opts["host"] = host
            if path:
                plugin_opts["path"] = path
            if plugin_opts:
                proxy["plugin-opts"] = plugin_opts
        return proxy

    if normalized_type == "vmess":
        uuid = _get_option(options, "username", "uuid", "id", "password")
        if not uuid:
            return None
        network = _detect_client_network(options)
        proxy = {
            "name": name,
            "type": "vmess",
            "server": server,
            "port": port,
            "uuid": uuid,
            "alterId": _parse_int_value(_get_option(options, "alterid", "aid")) or 0,
            "cipher": _get_option(options, "cipher", "method") or "auto",
            "network": network,
        }
        _apply_tls_fields(proxy, options, sni_field="servername")
        _apply_ws_or_grpc_options(proxy, options, network)
        return proxy

    if normalized_type == "vless":
        uuid = _get_option(options, "username", "uuid", "id", "password")
        if not uuid:
            return None
        network = _detect_client_network(options)
        proxy = {
            "name": name,
            "type": "vless",
            "server": server,
            "port": port,
            "uuid": uuid,
            "network": network,
            "udp": _parse_bool(_get_option(options, "udp-relay", "udp")) is not False,
        }
        _apply_tls_fields(proxy, options, sni_field="servername")
        _apply_ws_or_grpc_options(proxy, options, network)
        security = _get_option(options, "security").casefold()
        public_key = _get_option(options, "public-key", "pbk")
        short_id = _get_option(options, "short-id", "sid")
        if security == "reality" or public_key or short_id:
            proxy["tls"] = True
            reality_opts: dict[str, object] = {}
            if public_key:
                reality_opts["public-key"] = public_key
            if short_id:
                reality_opts["short-id"] = short_id
            if reality_opts:
                proxy["reality-opts"] = reality_opts
        flow = _get_option(options, "flow")
        if flow:
            proxy["flow"] = flow
        return proxy

    if normalized_type == "trojan":
        password = _get_option(options, "password", "passwd", "username")
        if not password:
            return None
        network = _detect_client_network(options)
        proxy = {
            "name": name,
            "type": "trojan",
            "server": server,
            "port": port,
            "password": password,
        }
        if network != "tcp":
            proxy["network"] = network
        _apply_tls_fields(proxy, options, sni_field="sni")
        _apply_ws_or_grpc_options(proxy, options, network)
        return proxy

    if normalized_type == "hysteria2":
        password = _get_option(options, "password", "auth", "auth-str", "username")
        if not password:
            return None
        proxy = {
            "name": name,
            "type": "hysteria2",
            "server": server,
            "port": port,
            "password": password,
        }
        sni = _get_option(options, "sni", "peer", "tls-host")
        if sni:
            proxy["sni"] = sni
        obfs = _get_option(options, "obfs")
        if obfs:
            proxy["obfs"] = obfs
        obfs_password = _get_option(options, "obfs-password")
        if obfs_password:
            proxy["obfs-password"] = obfs_password
        _apply_skip_cert_verify(proxy, options)
        return proxy

    if normalized_type == "hysteria":
        proxy = {
            "name": name,
            "type": "hysteria",
            "server": server,
            "port": port,
        }
        auth = _get_option(options, "auth", "auth-str", "password", "username")
        if auth:
            proxy["auth-str"] = auth
        sni = _get_option(options, "sni", "peer", "tls-host")
        if sni:
            proxy["sni"] = sni
        up = _get_option(options, "up", "upmbps")
        down = _get_option(options, "down", "downmbps")
        if up:
            proxy["up"] = up
        if down:
            proxy["down"] = down
        _apply_skip_cert_verify(proxy, options)
        return proxy

    if normalized_type == "tuic":
        uuid = _get_option(options, "uuid", "username", "id")
        if not uuid:
            return None
        proxy = {
            "name": name,
            "type": "tuic",
            "server": server,
            "port": port,
            "uuid": uuid,
        }
        password = _get_option(options, "password", "token")
        if password:
            proxy["password"] = password
        sni = _get_option(options, "sni", "peer", "tls-host")
        if sni:
            proxy["sni"] = sni
        congestion = _get_option(options, "congestion-controller", "congestion-control")
        if congestion:
            proxy["congestion-controller"] = congestion
        alpn = _get_option(options, "alpn")
        if alpn:
            proxy["alpn"] = [item.strip() for item in alpn.split(",") if item.strip()]
        _apply_skip_cert_verify(proxy, options)
        return proxy

    if normalized_type in {"http", "socks5"}:
        proxy = {
            "name": name,
            "type": normalized_type,
            "server": server,
            "port": port,
        }
        username = _get_option(options, "username", "user")
        password = _get_option(options, "password", "passwd")
        if username:
            proxy["username"] = username
        if password:
            proxy["password"] = password
        if str(proxy_type or "").strip().casefold() == "https" or _parse_bool(_get_option(options, "over-tls")) is True:
            proxy["tls"] = True
        _apply_tls_fields(proxy, options, sni_field="sni")
        return proxy

    return None


def _looks_like_surge_proxy_line(line: str) -> bool:
    if "=" not in line:
        return False
    _, spec = line.split("=", 1)
    fields = _split_config_fields(spec)
    if len(fields) < 3:
        return False
    return _normalize_client_type(fields[0]) in {_normalize_client_type(item) for item in SUPPORTED_CLIENT_PROXY_TYPES}


def _looks_like_quantumult_x_proxy_line(line: str) -> bool:
    if "=" not in line:
        return False
    proxy_type, spec = line.split("=", 1)
    if _normalize_client_type(proxy_type) not in {_normalize_client_type(item) for item in SUPPORTED_CLIENT_PROXY_TYPES}:
        return False
    fields = _split_config_fields(spec)
    if not fields:
        return False
    return _split_host_port(fields[0]) is not None


def _parse_surge_like_proxy_line(line: str) -> tuple[dict | None, str]:
    if "=" not in line:
        return None, ""
    name_part, spec = line.split("=", 1)
    name = _clean_config_value(name_part)
    fields = _split_config_fields(spec)
    if len(fields) < 3:
        return None, ""
    proxy_type = fields[0]
    port = _parse_int_value(fields[2])
    if port is None:
        return None, _normalize_client_type(proxy_type)
    positional_limit = 2 if _normalize_client_type(proxy_type) == "ss" else 0
    positional, options = _parse_config_tokens(fields[3:], positional_limit=positional_limit)
    proxy = _build_client_proxy(
        name=name or f"{_normalize_client_type(proxy_type)}_{fields[1]}:{port}",
        proxy_type=proxy_type,
        server=fields[1].strip("[]"),
        port=port,
        options=options,
        positional=positional,
    )
    return proxy, _normalize_client_type(proxy_type)


def _parse_quantumult_x_proxy_line(line: str) -> tuple[dict | None, str]:
    if "=" not in line:
        return None, ""
    proxy_type, spec = line.split("=", 1)
    normalized_type = _normalize_client_type(proxy_type)
    fields = _split_config_fields(spec)
    if not fields:
        return None, normalized_type
    endpoint = _split_host_port(fields[0])
    if endpoint is None:
        return None, normalized_type
    server, port = endpoint
    positional, options = _parse_config_tokens(fields[1:], positional_limit=0)
    name = _get_option(options, "tag", "remarks", "name") or f"{normalized_type}_{server}:{port}"
    proxy = _build_client_proxy(
        name=name,
        proxy_type=proxy_type,
        server=server,
        port=port,
        options=options,
        positional=positional,
    )
    return proxy, normalized_type


def load_proxies_from_client_config(content: str, source_url: str) -> tuple[list[dict], str, dict]:
    proxies: list[dict] = []
    skipped: list[str] = []
    detected_formats: set[str] = set()
    raw_items = 0
    current_section = ""

    for raw_line in str(content or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";", "//")):
            continue
        if line.startswith("[") and "]" in line:
            current_section = _normalize_option_key(line[1 : line.index("]")])
            continue

        parser_name = ""
        proxy: dict | None = None
        if current_section in SURGE_LIKE_PROXY_SECTIONS or (not current_section and _looks_like_surge_proxy_line(line)):
            raw_items += 1
            detected_formats.add("surge_like")
            proxy, parser_name = _parse_surge_like_proxy_line(line)
        elif current_section in QUANTUMULT_X_PROXY_SECTIONS or (not current_section and _looks_like_quantumult_x_proxy_line(line)):
            raw_items += 1
            detected_formats.add("quantumult_x")
            proxy, parser_name = _parse_quantumult_x_proxy_line(line)
        else:
            continue

        if proxy is None:
            if parser_name:
                skipped.append(parser_name)
            continue
        proxies.append(proxy)

    if not proxies:
        raise CollectorError(f"客户端配置解析失败: {source_url}: 未提取到可用节点")

    if detected_formats == {"surge_like"}:
        source_format = "client_config_surge_like"
    elif detected_formats == {"quantumult_x"}:
        source_format = "client_config_quantumult_x"
    else:
        source_format = "client_config_mixed"

    stats = {
        "raw_items": raw_items,
        "converted_items": len(proxies),
        "skipped_items": max(raw_items - len(proxies), 0),
    }
    if skipped:
        stats["skipped_types"] = sorted(set(skipped))
    return proxies, source_format, stats


def load_proxies_from_mixed_content(content: str, source_url: str) -> tuple[list[dict], str, dict]:
    yaml_error = ""
    client_error = ""
    try:
        yaml_proxies = load_proxies_from_yaml(content, source_url)
        if yaml_proxies:
            return yaml_proxies, "clash_yaml", {"raw_items": len(yaml_proxies), "converted_items": len(yaml_proxies), "skipped_items": 0}
    except Exception as exc:
        yaml_error = str(exc)

    try:
        client_proxies, client_format, client_stats = load_proxies_from_client_config(content, source_url)
        if client_proxies:
            return client_proxies, client_format, client_stats
    except Exception as exc:
        client_error = str(exc)

    nodes = extract_nodes_from_text(content)
    if nodes:
        proxies: list[dict] = []
        skipped: list[str] = []
        for node in nodes:
            proxy, detected_type = parse_proxy_uri(node)
            if proxy is None:
                skipped.append(detected_type)
                continue
            proxies.append(proxy)
        if proxies:
            stats = {
                "raw_items": len(nodes),
                "converted_items": len(proxies),
                "skipped_items": len(skipped),
            }
            if skipped:
                stats["skipped_types"] = sorted(set(skipped))
            return proxies, "proxy_uri", stats

    detail_parts = [part for part in [yaml_error, client_error] if part]
    if detail_parts:
        raise CollectorError(f"混合模式解析失败: {source_url}: {'; '.join(detail_parts)}")
    raise CollectorError(f"混合模式解析失败: {source_url}: 未识别为 Clash YAML、客户端配置或 URI 订阅")


FINGERPRINT_IGNORE_FIELDS = {
    "name",
    "client-fingerprint",
    "tfo",
}
FINGERPRINT_DROP_FALSE_FIELDS = {
    "skip-cert-verify",
}
FINGERPRINT_HOST_FIELDS = {
    "server",
    "servername",
    "sni",
}
FINGERPRINT_SORTED_LIST_FIELDS = {
    "alpn",
}


def _normalize_fingerprint_value(key: str, value):
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for child_key, child_value in value.items():
            normalized_child = _normalize_fingerprint_value(str(child_key), child_value)
            if normalized_child in (None, "", [], {}):
                continue
            normalized[str(child_key)] = normalized_child
        return normalized

    if isinstance(value, list):
        normalized_items = []
        for item in value:
            normalized_item = _normalize_fingerprint_value(key, item)
            if normalized_item in (None, "", [], {}):
                continue
            normalized_items.append(normalized_item)
        if key in FINGERPRINT_SORTED_LIST_FIELDS:
            unique_items = []
            seen_items: set[str] = set()
            for item in normalized_items:
                token = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                if token in seen_items:
                    continue
                seen_items.add(token)
                unique_items.append(item)
            return sorted(unique_items, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return normalized_items

    if isinstance(value, str):
        normalized_text = value.strip()
        if key in FINGERPRINT_HOST_FIELDS:
            return normalized_text.casefold()
        if key in FINGERPRINT_SORTED_LIST_FIELDS:
            return normalized_text.casefold()
        return normalized_text

    return value


def fingerprint_proxy(proxy: dict) -> str:
    identity: dict[str, object] = {}
    for key, value in proxy.items():
        if key in FINGERPRINT_IGNORE_FIELDS:
            continue
        normalized_value = _normalize_fingerprint_value(str(key), value)
        if key in FINGERPRINT_DROP_FALSE_FIELDS and normalized_value is not True:
            continue
        if normalized_value in (None, "", [], {}):
            continue
        identity[str(key)] = normalized_value
    return json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def allocate_proxy_name(raw_name: str, used_names: set[str], seq_no: int) -> str:
    base = str(raw_name or "").strip() or f"node_{seq_no}"
    candidate = base
    suffix = 2
    while candidate in used_names:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used_names.add(candidate)
    return candidate


def merge_proxy_sources(source_results: list[dict]) -> tuple[dict, dict]:
    merged_proxies: list[dict] = []
    used_names: set[str] = set()
    seen_fingerprints: set[str] = set()
    source_stats: list[dict] = []
    total_raw = 0
    total_kept = 0
    total_duplicates = 0

    for source in source_results:
        proxies = source["proxies"]
        raw_count = len(proxies)
        kept_count = 0
        duplicate_count = 0

        for proxy in proxies:
            total_raw += 1
            current = dict(proxy)
            fp = fingerprint_proxy(current)
            if fp in seen_fingerprints:
                duplicate_count += 1
                total_duplicates += 1
                continue

            current["name"] = allocate_proxy_name(current.get("name", ""), used_names, len(merged_proxies) + 1)
            merged_proxies.append(current)
            seen_fingerprints.add(fp)
            kept_count += 1
            total_kept += 1

        source_stats.append(
            {
                "source_url": source["meta"]["source_url"],
                "final_url": source["meta"]["final_url"],
                "content_type": source["meta"]["content_type"],
                "source_format": source["meta"].get("source_format", ""),
                "raw_proxies": raw_count,
                "kept_proxies": kept_count,
                "duplicate_proxies": duplicate_count,
                "parse_stats": source["meta"].get("parse_stats", {}),
            }
        )

    proxy_names = [proxy["name"] for proxy in merged_proxies]
    merged_config = {
        "mixed-port": 7890,
        "allow-lan": True,
        "mode": "Rule",
        "log-level": "info",
        "ipv6": True,
        "proxies": merged_proxies,
        "proxy-groups": [
            {
                "name": "PROXY",
                "type": "select",
                "proxies": ["DIRECT"] + proxy_names,
            }
        ],
        "rules": [
            "MATCH,PROXY",
        ],
    }
    meta = {
        "source_count": len(source_results),
        "raw_proxy_total": total_raw,
        "merged_proxy_total": total_kept,
        "duplicate_proxy_total": total_duplicates,
        "sources": source_stats,
    }
    return merged_config, meta


def save_text_results(output_dir: Path, output_prefix: str, lines: list[str], meta: dict) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = output_dir / f"{output_prefix}_{ts}.txt"
    json_path = output_dir / f"{output_prefix}_{ts}.json"
    txt_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    json_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return txt_path, json_path


def resolve_discover_output_prefix(output_prefix: str, discover_only: bool) -> str:
    normalized = (output_prefix or "").strip() or DEFAULT_OUTPUT_PREFIX
    if discover_only:
        if normalized == DEFAULT_OUTPUT_PREFIX:
            return DEFAULT_DISCOVER_OUTPUT_PREFIX
        return normalized
    return f"{normalized}_sources"


def discover_recent_gist_raw_urls(
    *,
    query: str,
    language: str,
    sort: str,
    order: str,
    start_page: int,
    max_pages: int,
    within_hours: float,
    max_gists: int,
    max_raw_files: int,
    timeout: int,
) -> tuple[list[dict], dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=within_hours)
    results: list[dict] = []
    errors: list[dict] = []
    page_stats: list[dict] = []
    seen_gists: set[str] = set()
    seen_raw_urls: set[str] = set()
    total_recent_gists = 0
    total_raw_checked = 0
    stop_reason = "max_pages_reached"

    for page in range(start_page, start_page + max_pages):
        search_url = build_gist_search_url(
            query=query,
            language=language,
            sort=sort,
            order=order,
            page=page,
        )
        log_line(f"[SEARCH] page={page} url={search_url}")
        try:
            html_text, _ = fetch_search_page_html(
                search_url=search_url,
                query=query,
                page=page,
                timeout=timeout,
            )
        except CollectorError as exc:
            stop_reason = "fetch_error"
            page_stats.append(
                {
                    "page": page,
                    "search_url": search_url,
                    "result_count": 0,
                    "recent_count": 0,
                    "oldest_updated_at": "",
                    "error": str(exc),
                }
            )
            log_line(f"[WARN] 搜索页抓取失败: query={query} page={page} error={exc}")
            break
        page_results = parse_gist_search_results(html_text)
        if not page_results:
            stop_reason = "no_results"
            break

        recent_results = [item for item in page_results if item["updated_at"] >= cutoff]
        oldest_updated_at = min(item["updated_at"] for item in page_results)
        page_stats.append(
            {
                "page": page,
                "search_url": search_url,
                "result_count": len(page_results),
                "recent_count": len(recent_results),
                "oldest_updated_at": oldest_updated_at.isoformat().replace("+00:00", "Z"),
            }
        )
        log_line(
            f"[SEARCH] page={page} results={len(page_results)} recent={len(recent_results)} "
            f"oldest={oldest_updated_at.isoformat().replace('+00:00', 'Z')}"
        )

        for gist_item in recent_results:
            if max_gists > 0 and total_recent_gists >= max_gists:
                stop_reason = "max_gists_reached"
                break
            gist_id = gist_item["gist_id"]
            if gist_id in seen_gists:
                continue
            seen_gists.add(gist_id)
            total_recent_gists += 1

            try:
                detail_html, _ = download_text_source(
                    gist_item["detail_url"],
                    timeout=timeout,
                    accept="text/html,*/*",
                )
            except CollectorError as exc:
                errors.append(
                    {
                        "stage": "detail_page",
                        "gist_id": gist_id,
                        "detail_url": gist_item["detail_url"],
                        "error": str(exc),
                    }
                )
                print_line(f"[WARN] gist详情页抓取失败: error={exc}")
                continue

            raw_items = extract_gist_raw_links_from_html(detail_html)
            print_line(
                f"[GIST] detail_ok updated={gist_item['updated_at'].isoformat().replace('+00:00', 'Z')} "
                f"raw_candidates={len(raw_items)}"
            )

            for raw_item in raw_items:
                if max_raw_files > 0 and total_raw_checked >= max_raw_files:
                    stop_reason = "max_raw_files_reached"
                    break
                total_raw_checked += 1
                try:
                    raw_text, raw_meta = download_text_source(
                        raw_item["raw_url"],
                        timeout=timeout,
                        accept="application/x-yaml,text/yaml,text/plain,*/*",
                    )
                except CollectorError as exc:
                    errors.append(
                        {
                            "stage": "raw_file",
                            "gist_id": gist_id,
                            "raw_url": raw_item["raw_url"],
                            "error": str(exc),
                        }
                    )
                    print_line(f"[WARN] raw文件抓取失败: error={exc}")
                    continue

                inspect_result = inspect_node_share_content(raw_text, raw_item["raw_url"])
                if not inspect_result["matched"]:
                    continue
                if raw_item["raw_url"] in seen_raw_urls:
                    continue
                seen_raw_urls.add(raw_item["raw_url"])

                result = {
                    "owner": gist_item["owner"],
                    "gist_id": gist_item["gist_id"],
                    "title": gist_item["title"],
                    "detail_url": gist_item["detail_url"],
                    "search_updated_at": gist_item["updated_at"].isoformat().replace("+00:00", "Z"),
                    "filename": raw_item["filename"],
                    "raw_url": raw_item["raw_url"],
                    "raw_url_with_commit": raw_item["raw_url_with_commit"],
                    "match_kind": inspect_result["kind"],
                    "proxy_count": inspect_result["proxy_count"],
                    "uri_count": inspect_result["uri_count"],
                    "content_type": raw_meta.get("content_type", ""),
                    "final_url": raw_meta.get("final_url", ""),
                }
                results.append(result)
                print_line(f"[MATCH] kind={result['match_kind']} matched=1")

            if stop_reason == "max_raw_files_reached":
                break

        if stop_reason in {"max_gists_reached", "max_raw_files_reached"}:
            break
        if oldest_updated_at < cutoff:
            stop_reason = "reached_cutoff"
            break
        sleep_with_base_interval(DEFAULT_SEARCH_PAGE_DELAY_SECONDS)

    meta = {
        "query": query,
        "language": language,
        "sort": sort,
        "order": order,
        "start_page": start_page,
        "max_pages": max_pages,
        "within_hours": within_hours,
        "max_gists": max_gists,
        "max_raw_files": max_raw_files,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "cutoff_at": cutoff.isoformat().replace("+00:00", "Z"),
        "page_stats": page_stats,
        "recent_gist_count": total_recent_gists,
        "matched_raw_count": len(results),
        "raw_checked_count": total_raw_checked,
        "stop_reason": stop_reason,
        "errors": errors,
        "items": results,
    }
    return results, meta


def collect_recent_gist_candidates(
    *,
    query: str,
    language: str,
    sort: str,
    order: str,
    start_page: int,
    max_pages: int,
    within_hours: float,
    timeout: int,
) -> tuple[list[dict], dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=within_hours)
    candidates: list[dict] = []
    page_stats: list[dict] = []
    stop_reason = "max_pages_reached"

    for page in range(start_page, start_page + max_pages):
        search_url = build_gist_search_url(
            query=query,
            language=language,
            sort=sort,
            order=order,
            page=page,
        )
        log_line(f"[SEARCH] page={page} url={search_url}")
        try:
            html_text, _ = fetch_search_page_html(
                search_url=search_url,
                query=query,
                page=page,
                timeout=timeout,
            )
        except CollectorError as exc:
            stop_reason = "fetch_error"
            page_stats.append(
                {
                    "page": page,
                    "search_url": search_url,
                    "result_count": 0,
                    "recent_count": 0,
                    "oldest_updated_at": "",
                    "error": str(exc),
                }
            )
            log_line(f"[WARN] 搜索页抓取失败: query={query} page={page} error={exc}")
            break
        page_results = parse_gist_search_results(html_text)
        if not page_results:
            stop_reason = "no_results"
            break

        recent_results = [item for item in page_results if item["updated_at"] >= cutoff]
        oldest_updated_at = min(item["updated_at"] for item in page_results)
        page_stats.append(
            {
                "page": page,
                "search_url": search_url,
                "result_count": len(page_results),
                "recent_count": len(recent_results),
                "oldest_updated_at": oldest_updated_at.isoformat().replace("+00:00", "Z"),
            }
        )
        log_line(
            f"[SEARCH] page={page} results={len(page_results)} recent={len(recent_results)} "
            f"oldest={oldest_updated_at.isoformat().replace('+00:00', 'Z')}"
        )

        for item in recent_results:
            candidate = dict(item)
            candidate["query"] = query
            candidates.append(candidate)

        if oldest_updated_at < cutoff:
            stop_reason = "reached_cutoff"
            break
        sleep_with_base_interval(DEFAULT_SEARCH_PAGE_DELAY_SECONDS)

    meta = {
        "query": query,
        "within_hours": within_hours,
        "cutoff_at": cutoff.isoformat().replace("+00:00", "Z"),
        "page_stats": page_stats,
        "candidate_gist_count": len(candidates),
        "stop_reason": stop_reason,
    }
    return candidates, meta


def discover_recent_gist_raw_urls_for_queries(
    *,
    queries: list[str],
    language: str,
    sort: str,
    order: str,
    start_page: int,
    max_pages: int,
    within_hours: float,
    max_gists: int,
    max_raw_files: int,
    max_workers: int,
    timeout: int,
) -> tuple[list[dict], dict]:
    aggregated_errors: list[dict] = []
    aggregated_page_stats: list[dict] = []
    candidate_items: list[dict] = []
    query_meta_map: dict[str, dict] = {}
    latest_by_owner: dict[str, dict] = {}
    results: list[dict] = []
    seen_raw_urls: set[str] = set()
    total_raw_checked = 0
    stop_reason = "completed"
    cutoff_at = datetime.now(timezone.utc) - timedelta(hours=within_hours)

    worker_count = max(1, min(DEFAULT_SEARCH_QUERY_WORKERS, max_workers, len(queries) or 1))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                collect_recent_gist_candidates,
                query=query,
                language=language,
                sort=sort,
                order=order,
                start_page=start_page,
                max_pages=max_pages,
                within_hours=within_hours,
                timeout=timeout,
            ): query
            for query in queries
        }
        for future in as_completed(future_map):
            query = future_map[future]
            try:
                query_candidates, query_meta = future.result()
            except CollectorError as exc:
                aggregated_errors.append(
                    {
                        "stage": "search_query",
                        "query": query,
                        "error": str(exc),
                    }
                )
                query_meta_map[query] = {
                    "candidate_gist_count": 0,
                    "stop_reason": "fetch_error",
                }
                log_line(f"[WARN] 搜索任务失败，已跳过: query={query} error={exc}")
                continue
            query_meta_map[query] = {
                "candidate_gist_count": len(query_candidates),
                "stop_reason": query_meta.get("stop_reason", ""),
            }
            for item in query_meta.get("page_stats", []) or []:
                page_item = dict(item)
                page_item["query"] = query
                aggregated_page_stats.append(page_item)
            candidate_items.extend(query_candidates)

    query_runs = [
        {
            "query": query,
            "candidate_gist_count": query_meta_map.get(query, {}).get("candidate_gist_count", 0),
            "stop_reason": query_meta_map.get(query, {}).get("stop_reason", ""),
        }
        for query in queries
    ]

    for item in candidate_items:
        owner_key = str(item.get("owner", "")).casefold()
        if not owner_key:
            continue
        previous = latest_by_owner.get(owner_key)
        if previous is None or item["updated_at"] > previous["updated_at"]:
            latest_by_owner[owner_key] = item

    selected_candidates = sorted(
        latest_by_owner.values(),
        key=lambda item: item["updated_at"],
        reverse=True,
    )
    owner_dedup_skipped = len(candidate_items) - len(selected_candidates)
    if owner_dedup_skipped > 0:
        log_line(
            f"[SELECT] candidate_gists={len(candidate_items)} "
            f"latest_owner_gists={len(selected_candidates)} skipped_by_owner={owner_dedup_skipped}"
        )
    else:
        log_line(f"[SELECT] candidate_gists={len(candidate_items)} latest_owner_gists={len(selected_candidates)}")

    if max_gists > 0 and len(selected_candidates) > max_gists:
        selected_candidates = selected_candidates[:max_gists]
        stop_reason = "max_gists_reached"

    processed_gists = 0
    total_selected_candidates = len(selected_candidates)
    completed_detail_count = 0
    detail_results: list[dict] = []
    detail_worker_count = max(1, min(DEFAULT_DISCOVER_CONTENT_WORKERS, len(selected_candidates) or 1))
    if selected_candidates:
        with ThreadPoolExecutor(max_workers=detail_worker_count) as executor:
            future_map = {
                executor.submit(fetch_gist_detail_result, index, gist_item, timeout): (index, gist_item)
                for index, gist_item in enumerate(selected_candidates, 1)
            }
            for future in as_completed(future_map):
                index, gist_item = future_map[future]
                gist_id = gist_item["gist_id"]
                try:
                    payload = future.result()
                except CollectorError as exc:
                    completed_detail_count += 1
                    aggregated_errors.append(
                        {
                            "stage": "detail_page",
                            "query": gist_item.get("query", ""),
                            "gist_id": gist_id,
                            "detail_url": gist_item["detail_url"],
                            "error": str(exc),
                        }
                    )
                    log_line(
                        f"[DETAIL] completed={completed_detail_count}/{total_selected_candidates} "
                        f"slot={index} status=failed"
                    )
                    log_line(f"[WARN] gist详情页抓取失败: {gist_item['detail_url']} error={exc}")
                    continue
                completed_detail_count += 1
                detail_results.append(payload)
                log_line(
                    f"[DETAIL] completed={completed_detail_count}/{total_selected_candidates} "
                    f"slot={index} status=ok raw_candidates={len(payload['raw_items'])}"
                )

    detail_results.sort(key=lambda item: item["index"])

    raw_tasks: list[dict] = []
    for detail_payload in detail_results:
        gist_item = detail_payload["gist_item"]
        raw_items = detail_payload["raw_items"]
        processed_gists += 1
        log_line(
            f"[GIST] {gist_item['owner']}/{gist_item['title']} "
            f"updated={gist_item['updated_at'].isoformat().replace('+00:00', 'Z')} "
            f"raw_candidates={len(raw_items)}"
        )

        for raw_item in raw_items:
            if max_raw_files > 0 and len(raw_tasks) >= max_raw_files:
                stop_reason = "max_raw_files_reached"
                break
            raw_tasks.append(
                {
                    "task_index": len(raw_tasks) + 1,
                    "gist_item": gist_item,
                    "raw_item": raw_item,
                }
            )

        if stop_reason == "max_raw_files_reached":
            break

    total_raw_checked = len(raw_tasks)
    completed_raw_count = 0
    if raw_tasks:
        raw_worker_count = max(1, min(DEFAULT_DISCOVER_CONTENT_WORKERS, len(raw_tasks) or 1))
        with ThreadPoolExecutor(max_workers=raw_worker_count) as executor:
            future_map = {
                executor.submit(
                    inspect_gist_raw_result,
                    task["task_index"],
                    task["gist_item"],
                    task["raw_item"],
                    timeout,
                ): task
                for task in raw_tasks
            }
            for future in as_completed(future_map):
                task = future_map[future]
                gist_item = task["gist_item"]
                raw_item = task["raw_item"]
                gist_id = gist_item["gist_id"]
                try:
                    payload = future.result()
                except CollectorError as exc:
                    completed_raw_count += 1
                    aggregated_errors.append(
                        {
                            "stage": "raw_file",
                            "query": gist_item.get("query", ""),
                            "gist_id": gist_id,
                            "raw_url": raw_item["raw_url"],
                            "error": str(exc),
                        }
                    )
                    log_line(
                        f"[RAW] completed={completed_raw_count}/{total_raw_checked} "
                        f"slot={task['task_index']} status=failed"
                    )
                    log_line(f"[WARN] raw文件抓取失败: {raw_item['raw_url']} error={exc}")
                    continue

                completed_raw_count += 1
                inspect_result = payload["inspect_result"]
                log_line(
                    f"[RAW] completed={completed_raw_count}/{total_raw_checked} "
                    f"slot={task['task_index']} status=ok matched={1 if inspect_result['matched'] else 0}"
                )
                if not inspect_result["matched"]:
                    continue

                raw_meta = payload["raw_meta"]
                raw_url = raw_item["raw_url"]
                if raw_url in seen_raw_urls:
                    continue
                seen_raw_urls.add(raw_url)
                result = {
                    "_task_index": payload["task_index"],
                    "owner": gist_item["owner"],
                    "gist_id": gist_item["gist_id"],
                    "title": gist_item["title"],
                    "detail_url": gist_item["detail_url"],
                    "query": gist_item.get("query", ""),
                    "search_updated_at": gist_item["updated_at"].isoformat().replace("+00:00", "Z"),
                    "filename": raw_item["filename"],
                    "raw_url": raw_item["raw_url"],
                    "raw_url_with_commit": raw_item["raw_url_with_commit"],
                    "match_kind": inspect_result["kind"],
                    "proxy_count": inspect_result["proxy_count"],
                    "uri_count": inspect_result["uri_count"],
                    "content_type": raw_meta.get("content_type", ""),
                    "final_url": raw_meta.get("final_url", ""),
                }
                results.append(result)
                log_line(
                    f"[MATCH] {result['filename']} kind={result['match_kind']} "
                    f"raw={result['raw_url']}"
                )

    results.sort(key=lambda item: item.pop("_task_index", 0))

    if stop_reason == "completed":
        if not candidate_items and query_runs:
            if all(str(item.get("stop_reason", "")) == "reached_cutoff" for item in query_runs):
                stop_reason = "reached_cutoff"
        elif max_gists > 0 and len(latest_by_owner) > max_gists:
            stop_reason = "max_gists_reached"

    meta = {
        "queries": queries,
        "language": language,
        "sort": sort,
        "order": order,
        "start_page": start_page,
        "max_pages": max_pages,
        "within_hours": within_hours,
        "max_gists": max_gists,
        "max_raw_files": max_raw_files,
        "max_workers": max_workers,
        "search_query_workers": worker_count,
        "search_page_delay_seconds": DEFAULT_SEARCH_PAGE_DELAY_SECONDS,
        "search_rate_limit_retries": DEFAULT_SEARCH_RATE_LIMIT_RETRIES,
        "discover_content_workers": DEFAULT_DISCOVER_CONTENT_WORKERS,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "cutoff_at": cutoff_at.isoformat().replace("+00:00", "Z"),
        "page_stats": aggregated_page_stats,
        "candidate_gist_count": len(candidate_items),
        "owner_deduped_gist_count": len(latest_by_owner),
        "owner_dedup_skipped": owner_dedup_skipped,
        "recent_gist_count": processed_gists,
        "matched_raw_count": len(results),
        "raw_checked_count": total_raw_checked,
        "stop_reason": stop_reason,
        "errors": aggregated_errors,
        "query_runs": query_runs,
        "items": results,
    }
    return results, meta


def save_merged_yaml(output_dir: Path, output_prefix: str, yaml_text: str, meta: dict) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    yaml_path = output_dir / f"{output_prefix}_{ts}.yaml"
    json_path = output_dir / f"{output_prefix}_{ts}.json"

    yaml_path.write_text(yaml_text, encoding="utf-8")
    json_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return yaml_path, json_path


def save_source_urls(output_path: Path, source_urls: list[str]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(source_urls)
    if content:
        content += "\n"
    output_path.write_text(content, encoding="utf-8")
    return output_path


def load_local_upload_text(upload_file: str) -> tuple[Path, str]:
    upload_path = Path(str(upload_file or "").strip()).expanduser()
    if not upload_path:
        raise CollectorError("--upload-file 不能为空")
    if not upload_path.is_file():
        raise CollectorError(f"--upload-file 指定的文件不存在: {upload_path}")
    try:
        content = upload_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise CollectorError(f"--upload-file 不是 UTF-8 文本文件: {upload_path}") from exc
    if not content.strip():
        raise CollectorError(f"--upload-file 文件内容为空: {upload_path}")
    return upload_path, content


def should_retry_gist_upload_error(error: Exception) -> bool:
    message = str(error or "").casefold()
    return any(marker in message for marker in GIST_UPLOAD_RETRY_MARKERS)


def upload_text_to_gist(
    *,
    token: str,
    gist_id: str,
    gist_public: bool,
    gist_filename: str,
    gist_description: str,
    file_content: str,
    timeout: int,
) -> dict:
    payload: dict[str, object] = {
        "description": gist_description,
        "files": {
            gist_filename: {"content": file_content},
        },
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }

    gist_id = gist_id.strip()
    attempts = DEFAULT_GIST_UPLOAD_RETRIES + 1
    for attempt in range(1, attempts + 1):
        try:
            if gist_id:
                response = http_request_json(
                    url=f"{GITHUB_API_BASE}/gists/{gist_id}",
                    method="PATCH",
                    timeout=timeout,
                    headers=headers,
                    payload=payload,
                )
                action = "updated"
            else:
                create_payload = dict(payload)
                create_payload["public"] = gist_public
                response = http_request_json(
                    url=f"{GITHUB_API_BASE}/gists",
                    method="POST",
                    timeout=timeout,
                    headers=headers,
                    payload=create_payload,
                )
                action = "created"
            break
        except CollectorError as exc:
            if attempt >= attempts or not should_retry_gist_upload_error(exc):
                raise
            wait_seconds = DEFAULT_GIST_UPLOAD_BACKOFF_SECONDS * attempt
            log_line(
                f"[WARN] Cloud 上传失败，{wait_seconds:.0f}秒后重试: "
                f"attempt={attempt}/{attempts} error={exc}"
            )
            time.sleep(wait_seconds)
    else:
        raise CollectorError("Cloud 上传失败：重试后仍未成功")

    if not isinstance(response, dict):
        raise CollectorError("Cloud API 返回了异常响应")

    return {
        "action": action,
        "id": response.get("id", ""),
        "html_url": response.get("html_url", ""),
        "public": response.get("public"),
        "files": sorted((response.get("files") or {}).keys()),
    }


def upload_yaml_to_gist(
    *,
    token: str,
    gist_id: str,
    gist_public: bool,
    gist_filename: str,
    gist_description: str,
    yaml_text: str,
    timeout: int,
) -> dict:
    return upload_text_to_gist(
        token=token,
        gist_id=gist_id,
        gist_public=gist_public,
        gist_filename=gist_filename,
        gist_description=gist_description,
        file_content=yaml_text,
        timeout=timeout,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="下载多个 Clash/Clash.Meta YAML 或 URI 订阅源并上传到 Cloud（底层使用 GitHub Gist），也支持直接上传本地订阅文件。"
    )
    parser.set_defaults(upload_gist=DEFAULT_UPLOAD_GIST)
    parser.set_defaults(save_local=DEFAULT_SAVE_LOCAL)
    parser.set_defaults(discover_gist_search=DEFAULT_DISCOVER_GIST_SEARCH)
    parser.add_argument(
        "--upload-file",
        default=DEFAULT_UPLOAD_FILE,
        help="直接读取本地文本文件并上传到 Cloud；启用后跳过下载、搜索和 YAML 合并流程",
    )
    parser.add_argument(
        "--emit-source-urls-file",
        default=DEFAULT_EMIT_SOURCE_URLS_FILE,
        help="将当前脚本可用的来源 URL 列表导出到指定 txt 文件；启用后跳过下载、合并和上传流程",
    )
    parser.add_argument(
        "--source-url",
        action="append",
        default=[],
        help="来源地址，可重复传入；支持 Clash YAML、常见客户端配置和 URI 订阅，默认使用脚本内置的 9 个来源",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SEC, help=f"下载和上传超时秒数，默认 {DEFAULT_TIMEOUT_SEC}")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="本地输出目录，默认当前目录")
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX, help="本地输出文件名前缀")
    parser.add_argument("--upload-gist", dest="upload_gist", action="store_true", help="下载完成后上传到 Cloud")
    parser.add_argument("--no-upload-gist", dest="upload_gist", action="store_false", help="下载完成后不上传到 Cloud")
    parser.add_argument("--gist-token", default=DEFAULT_GIST_TOKEN, help="Cloud token；默认使用脚本内置值或环境变量 GIST_TOKEN/GITHUB_GIST_TOKEN/GITHUB_TOKEN")
    parser.add_argument("--gist-id", default=DEFAULT_GIST_ID, help="已有 Cloud 目标 ID；默认更新脚本内置的目标 Gist")
    parser.add_argument("--gist-public", action="store_true", help="创建新 Cloud 目标时设为公开")
    parser.add_argument("--gist-filename", default=DEFAULT_GIST_FILENAME, help=f"上传到 Cloud 的文件名，默认 {DEFAULT_GIST_FILENAME}")
    parser.add_argument("--gist-description", default="", help="Cloud 描述，默认自动生成")
    parser.add_argument("--discover-gist-search", action="store_true", help="从 Cloud 搜索结果分页发现最近更新的节点 raw 地址")
    parser.add_argument("--discover-only", action="store_true", help="仅输出发现到的 raw 地址 txt/json，不继续合并 YAML")
    parser.add_argument("--search-query", default=DEFAULT_SEARCH_QUERY, help=f"Cloud 搜索关键词，默认 {DEFAULT_SEARCH_QUERY}")
    parser.add_argument("--search-language", default=DEFAULT_SEARCH_LANGUAGE, help=f"Cloud 搜索语言过滤，默认 {DEFAULT_SEARCH_LANGUAGE}")
    parser.add_argument("--search-sort", default=DEFAULT_SEARCH_SORT, help=f"Cloud 搜索排序字段，默认 {DEFAULT_SEARCH_SORT}")
    parser.add_argument("--search-order", default=DEFAULT_SEARCH_ORDER, choices=["asc", "desc"], help=f"Cloud 搜索排序方向，默认 {DEFAULT_SEARCH_ORDER}")
    parser.add_argument("--search-start-page", type=int, default=DEFAULT_SEARCH_START_PAGE, help=f"Cloud 搜索起始页，默认 {DEFAULT_SEARCH_START_PAGE}")
    parser.add_argument("--search-max-pages", type=int, default=DEFAULT_SEARCH_MAX_PAGES, help=f"Cloud 搜索最大翻页数，默认 {DEFAULT_SEARCH_MAX_PAGES}")
    parser.add_argument("--search-within-hours", type=float, default=DEFAULT_SEARCH_WITHIN_HOURS, help=f"仅保留最近多少小时内更新，默认 {DEFAULT_SEARCH_WITHIN_HOURS}")
    parser.add_argument("--search-max-gists", type=int, default=DEFAULT_SEARCH_MAX_GISTS, help="最多检查多少个 gist，0 表示不限制")
    parser.add_argument("--search-max-raw-files", type=int, default=DEFAULT_SEARCH_MAX_RAW_FILES, help="最多检查多少个 raw 文件，0 表示不限制")
    parser.add_argument(
        "--fetch-retry-count",
        type=int,
        default=DEFAULT_FETCH_RETRY_COUNT,
        help=f"详情页和 raw 内容抓取的最大重试次数，默认 {DEFAULT_FETCH_RETRY_COUNT}",
    )
    parser.add_argument(
        "--fetch-retry-backoff-seconds",
        type=float,
        default=DEFAULT_FETCH_RETRY_BACKOFF_SECONDS,
        help=f"详情页和 raw 内容抓取的退避基准秒数，默认 {DEFAULT_FETCH_RETRY_BACKOFF_SECONDS}",
    )
    parser.add_argument(
        "--search-query-workers",
        type=int,
        default=DEFAULT_SEARCH_QUERY_WORKERS,
        help=f"Cloud 搜索关键词并发数，默认 {DEFAULT_SEARCH_QUERY_WORKERS}",
    )
    parser.add_argument(
        "--search-page-delay-seconds",
        type=float,
        default=DEFAULT_SEARCH_PAGE_DELAY_SECONDS,
        help=f"Cloud 搜索翻页间隔秒数，默认 {DEFAULT_SEARCH_PAGE_DELAY_SECONDS}",
    )
    parser.add_argument(
        "--search-rate-limit-retries",
        type=int,
        default=DEFAULT_SEARCH_RATE_LIMIT_RETRIES,
        help=f"Cloud 搜索遇到限流时的最大重试次数，默认 {DEFAULT_SEARCH_RATE_LIMIT_RETRIES}",
    )
    parser.add_argument(
        "--search-rate-limit-backoff-seconds",
        type=float,
        default=DEFAULT_SEARCH_RATE_LIMIT_BACKOFF_SECONDS,
        help=f"Cloud 搜索遇到限流时的退避基准秒数，默认 {DEFAULT_SEARCH_RATE_LIMIT_BACKOFF_SECONDS}",
    )
    parser.add_argument(
        "--discover-content-workers",
        type=int,
        default=DEFAULT_DISCOVER_CONTENT_WORKERS,
        help=f"Cloud 详情页和 raw 内容检查并发数，默认 {DEFAULT_DISCOVER_CONTENT_WORKERS}",
    )
    parser.add_argument("--save-local", dest="save_local", action="store_true", help="保存本地 txt/json/yaml 文件")
    parser.add_argument("--no-save-local", dest="save_local", action="store_false", help="不保存本地 txt/json/yaml 文件")
    parser.add_argument("--no-discover-gist-search", dest="discover_gist_search", action="store_false", help="关闭 Cloud 搜索，只使用内置和手动来源")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help=f"并发线程数，默认 {DEFAULT_MAX_WORKERS}")
    return parser


def main() -> int:
    global DEFAULT_FETCH_RETRY_COUNT
    global DEFAULT_FETCH_RETRY_BACKOFF_SECONDS
    global DEFAULT_SEARCH_QUERY_WORKERS
    global DEFAULT_SEARCH_PAGE_DELAY_SECONDS
    global DEFAULT_SEARCH_RATE_LIMIT_RETRIES
    global DEFAULT_SEARCH_RATE_LIMIT_BACKOFF_SECONDS
    global DEFAULT_DISCOVER_CONTENT_WORKERS

    configure_stdio_utf8()
    parser = build_parser()
    args = parser.parse_args()

    if args.timeout <= 0:
        raise CollectorError("--timeout 必须大于 0")

    upload_file = str(args.upload_file or "").strip()
    if upload_file:
        gist_token = ""
        if args.upload_gist:
            gist_token = resolve_gist_token(args.gist_token)
            if not gist_token:
                raise CollectorError("启用 --upload-gist 时，必须通过 --gist-token 或环境变量 GIST_TOKEN/GITHUB_GIST_TOKEN/GITHUB_TOKEN 提供 Cloud token")
        upload_path, upload_content = load_local_upload_text(upload_file)
        gist_filename_input = args.gist_filename or DEFAULT_GIST_FILENAME
        if gist_filename_input == DEFAULT_GIST_FILENAME:
            gist_filename_input = upload_path.name
        gist_filename = validate_gist_filename(gist_filename_input, "--gist-filename")

        print_line(f"[INFO] upload_size_bytes: {len(upload_content.encode('utf-8'))}")
        print_line(f"[INFO] upload_gist: {'on' if args.upload_gist else 'off'}")
        if args.upload_gist:
            gist_mode = "update" if args.gist_id else f"create:{'public' if args.gist_public else 'secret'}"
            print_line(f"[INFO] gist_mode: {gist_mode}")

            gist_result = upload_text_to_gist(
                token=gist_token,
                gist_id=args.gist_id,
                gist_public=args.gist_public,
                gist_filename=gist_filename,
                gist_description=args.gist_description
                or f"{gist_filename} updated at {datetime.now().isoformat(timespec='seconds')} ({len(upload_content.encode('utf-8'))} bytes)",
                file_content=upload_content,
                timeout=args.timeout,
            )
            print_line("[DONE] 上传成功到 cloud")
        else:
            print_line("[DONE] 已读取本地文件，未启用 Cloud 上传。")
        return 0

    source_urls = normalize_source_urls(args.source_url or list(DEFAULT_SOURCE_URLS))
    if not source_urls:
        raise CollectorError("未提供有效的 YAML 来源")

    source_urls = normalize_source_urls(list(DEFAULT_SOURCE_URLS) + (args.source_url or []))
    base_source_urls = list(source_urls)
    if args.max_workers <= 0:
        raise CollectorError("--max-workers 必须大于 0")
    if args.fetch_retry_count < 0:
        raise CollectorError("--fetch-retry-count 不能小于 0")
    if args.fetch_retry_backoff_seconds < 0:
        raise CollectorError("--fetch-retry-backoff-seconds 不能小于 0")
    if args.search_query_workers <= 0:
        raise CollectorError("--search-query-workers 必须大于 0")
    if args.search_page_delay_seconds < 0:
        raise CollectorError("--search-page-delay-seconds 不能小于 0")
    if args.search_rate_limit_retries < 0:
        raise CollectorError("--search-rate-limit-retries 不能小于 0")
    if args.search_rate_limit_backoff_seconds < 0:
        raise CollectorError("--search-rate-limit-backoff-seconds 不能小于 0")
    if args.discover_content_workers <= 0:
        raise CollectorError("--discover-content-workers 必须大于 0")

    DEFAULT_FETCH_RETRY_COUNT = args.fetch_retry_count
    DEFAULT_FETCH_RETRY_BACKOFF_SECONDS = args.fetch_retry_backoff_seconds
    DEFAULT_SEARCH_QUERY_WORKERS = args.search_query_workers
    DEFAULT_SEARCH_PAGE_DELAY_SECONDS = args.search_page_delay_seconds
    DEFAULT_SEARCH_RATE_LIMIT_RETRIES = args.search_rate_limit_retries
    DEFAULT_SEARCH_RATE_LIMIT_BACKOFF_SECONDS = args.search_rate_limit_backoff_seconds
    DEFAULT_DISCOVER_CONTENT_WORKERS = args.discover_content_workers

    search_queries = normalize_search_queries([args.search_query] if args.search_query is not None else [])
    if not search_queries:
        search_queries = normalize_search_queries(list(DEFAULT_SEARCH_QUERIES))

    if args.discover_only and not args.discover_gist_search:
        raise CollectorError("--discover-only 必须和 --discover-gist-search 一起使用")
    if args.discover_gist_search:
        if args.search_start_page <= 0:
            raise CollectorError("--search-start-page 必须大于 0")
        if args.search_max_pages <= 0:
            raise CollectorError("--search-max-pages 必须大于 0")
        if args.search_within_hours <= 0:
            raise CollectorError("--search-within-hours 必须大于 0")
        if args.search_max_gists < 0:
            raise CollectorError("--search-max-gists 不能小于 0")
        if args.search_max_raw_files < 0:
            raise CollectorError("--search-max-raw-files 不能小于 0")

        discovered_items, discover_meta = discover_recent_gist_raw_urls_for_queries(
            queries=search_queries,
            language=args.search_language,
            sort=args.search_sort,
            order=args.search_order,
            start_page=args.search_start_page,
            max_pages=args.search_max_pages,
            within_hours=args.search_within_hours,
            max_gists=args.search_max_gists,
            max_raw_files=args.search_max_raw_files,
            max_workers=args.max_workers,
            timeout=args.timeout,
        )
        discovered_urls = [item["raw_url"] for item in discovered_items]
        print_line(f"[DONE] discovered raw count: {len(discovered_urls)}")
        if args.save_local:
            discover_prefix = resolve_discover_output_prefix(
                args.output_prefix,
                discover_only=args.discover_only,
            )
            raw_txt_path, raw_meta_path = save_text_results(
                output_dir=Path(args.output_dir),
                output_prefix=discover_prefix,
                lines=discovered_urls,
                meta=discover_meta,
            )
            print_line(f"[DONE] raw  file: {raw_txt_path}")
            print_line(f"[DONE] meta file: {raw_meta_path}")
        elif args.discover_only:
            print_line(f"[DONE] discovered raw count: {len(discovered_urls)}")
        if args.discover_only:
            return 0
        source_urls = normalize_source_urls(base_source_urls + discovered_urls)
        if not source_urls:
            raise CollectorError("最近 24 小时内未发现可用的节点 raw 地址")

    emit_source_urls_file = str(args.emit_source_urls_file or "").strip()
    if emit_source_urls_file:
        output_path = save_source_urls(Path(emit_source_urls_file), source_urls)
        print_line(f"[DONE] source url file: {output_path}")
        print_line(f"[DONE] source url count: {len(source_urls)}")
        return 0

    if not args.upload_gist and not args.save_local:
        print_line("[WARN] 当前未启用上传且未保存本地文件，结果只会显示在终端日志中")

    gist_filename = validate_gist_filename(args.gist_filename or DEFAULT_GIST_FILENAME, "--gist-filename")
    if not re.search(r"\.ya?ml$", gist_filename, flags=re.IGNORECASE):
        gist_filename = f"{gist_filename}.yaml"

    print_line(f"[INFO] source_count: {len(source_urls)}")
    print_line(f"[INFO] max_workers: {args.max_workers}")
    print_line(f"[INFO] upload_gist: {'on' if args.upload_gist else 'off'}")
    if args.upload_gist:
        gist_token = resolve_gist_token(args.gist_token)
        if not gist_token:
            raise CollectorError("启用 --upload-gist 时，必须通过 --gist-token 或环境变量 GIST_TOKEN/GITHUB_GIST_TOKEN/GITHUB_TOKEN 提供 Cloud token")
        gist_mode = "update" if args.gist_id else f"create:{'public' if args.gist_public else 'secret'}"
        print_line(f"[INFO] gist_mode: {gist_mode}")

    source_results: list[dict] = []
    source_failures: list[dict] = []
    worker_count = max(1, min(args.max_workers, len(source_urls) or 1))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(fetch_source_result, index, source_url, args.timeout): (index, source_url)
            for index, source_url in enumerate(source_urls, 1)
        }
        for future in as_completed(future_map):
            index, source_url = future_map[future]
            print_line(f"[FETCH] ({index}/{len(source_urls)})")
            try:
                payload = future.result()
            except CollectorError as exc:
                source_failures.append(
                    {
                        "index": index,
                        "source_url": source_url,
                        "error": str(exc),
                    }
                )
                print_line(f"[WARN] skip source index={index} error={exc}")
                continue

            meta = payload["meta"]
            parse_stats = meta.get("parse_stats", {})
            source_format = meta.get("source_format", "")
            print_line(
                f"[FETCH] format={source_format} proxies={len(payload['proxies'])} "
                f"content_type={meta['content_type']}"
            )
            if parse_stats.get("skipped_items"):
                print_line(
                    f"[FETCH] skipped={parse_stats['skipped_items']} "
                    f"types={parse_stats.get('skipped_types', [])}"
                )
            source_results.append(payload)

    source_results.sort(key=lambda item: item["index"])
    source_failures.sort(key=lambda item: item.get("index", 0))

    if not source_results:
        raise CollectorError("所有来源都下载或解析失败，无法生成 YAML")

    merged_config, meta = merge_proxy_sources(source_results)
    merged_yaml = yaml.safe_dump(merged_config, allow_unicode=True, sort_keys=False, width=120)
    meta["source_urls"] = source_urls
    meta["source_failures"] = source_failures
    meta["generated_at"] = datetime.now().isoformat(timespec="seconds")
    meta["gist_filename"] = gist_filename
    if args.discover_gist_search:
        meta["discover_meta"] = discover_meta

    print_line(f"[DONE] merged proxies: {meta['merged_proxy_total']}")
    print_line(f"[DONE] duplicate skip: {meta['duplicate_proxy_total']}")
    if args.save_local:
        yaml_path, meta_path = save_merged_yaml(
            output_dir=Path(args.output_dir),
            output_prefix=args.output_prefix,
            yaml_text=merged_yaml,
            meta=meta,
        )
        print_line(f"[DONE] yaml file: {yaml_path}")
        print_line(f"[DONE] meta file: {meta_path}")

    if args.upload_gist:
        gist_result = upload_yaml_to_gist(
            token=gist_token,
            gist_id=args.gist_id,
            gist_public=args.gist_public,
            gist_filename=gist_filename,
            gist_description=args.gist_description or f"{gist_filename} updated at {datetime.now().isoformat(timespec='seconds')} ({meta['merged_proxy_total']} proxies)",
            yaml_text=merged_yaml,
            timeout=args.timeout,
        )
        print_line("[DONE] 上传成功到 cloud")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print_line("\n[STOP] 手动中断。")
        sys.exit(130)
    except CollectorError as exc:
        print_line(f"[ERROR] {exc}")
        sys.exit(1)
