"""HTTP helpers: retries, timeouts, and streaming downloads (httpx)."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx

logger = logging.getLogger(__name__)

# Filesystem-safe basename (preserve dots for extensions)
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

# Content-Disposition: filename="..." or RFC 5987 filename*=UTF-8''...
_CD_FILENAME_STAR_RE = re.compile(
    r"filename\*=(?:UTF-8|utf-8)''([^;\s]+)",
    re.IGNORECASE,
)
_CD_FILENAME_QUOTED_RE = re.compile(r'filename\s*=\s*"((?:[^"\\]|\\.)*)"')
_CD_FILENAME_UNQUOTED_RE = re.compile(r"filename\s*=\s*([^;\s]+)")


@dataclass(frozen=True)
class HttpGetResult:
    status_code: int
    text: str | None
    error: str | None


@dataclass(frozen=True)
class StreamDownloadResult:
    url: str
    dest_path: Path
    status_code: int
    bytes_written: int
    content_type: str | None
    etag: str | None
    last_modified: str | None
    error: str | None
    skipped: bool = False


def _backoff_seconds(attempt: int) -> float:
    return min(8.0, 0.5 * (2**attempt))


def suggested_local_filename(url: str) -> str:
    """Derive a filesystem-safe basename from the MRF URL path (EIN naming convention)."""
    parsed = urlparse(url)
    path = unquote(parsed.path or "")
    base = Path(path).name
    base = base.strip()
    if base and base not in (".", ".."):
        cleaned = _SAFE_NAME_RE.sub("_", base)
        return cleaned[:200] if len(cleaned) > 200 else cleaned
    if "aspx" in (parsed.path or "").lower() or "reports.aspx" in url.lower():
        return "mrf_report.aspx"
    return "mrf_download.bin"


def parse_content_disposition_filename(header_value: str | None) -> str | None:
    """Extract filename from Content-Disposition header, or None if absent/unusable."""
    if not header_value:
        return None
    raw = header_value.strip()
    m = _CD_FILENAME_STAR_RE.search(raw)
    if m:
        try:
            return unquote(m.group(1).strip())
        except Exception:  # noqa: BLE001 — defensive decode
            return None
    m = _CD_FILENAME_QUOTED_RE.search(raw)
    if m:
        return m.group(1).replace("\\", "")
    m = _CD_FILENAME_UNQUOTED_RE.search(raw)
    if m:
        name = m.group(1).strip().strip('"')
        if name.lower().startswith("utf-8''"):
            return unquote(name.split("''", 1)[1])
        return name
    return None


def extension_from_content_type(content_type: str | None) -> str | None:
    """Map common Content-Type values to a file extension (including leading dot)."""
    if not content_type:
        return None
    main = content_type.split(";", 1)[0].strip().lower()
    mapping = {
        "text/csv": ".csv",
        "application/csv": ".csv",
        "application/json": ".json",
        "application/x-ndjson": ".jsonl",
        "text/json": ".json",
    }
    return mapping.get(main)


def sanitize_download_basename(name: str) -> str:
    """Keep only the last path segment and apply safe character filter."""
    base = Path(name).name.strip()
    if not base or base in (".", ".."):
        return "mrf_download.bin"
    cleaned = _SAFE_NAME_RE.sub("_", base)
    return cleaned[:200] if len(cleaned) > 200 else cleaned


def resolve_download_basename(
    *,
    content_disposition: str | None,
    content_type: str | None,
    url: str,
) -> str:
    """
    Pick local basename: Content-Disposition filename, else URL-derived name,
    then add/repair extension from Content-Type when helpful.
    """
    name: str | None = None
    cd = parse_content_disposition_filename(content_disposition)
    if cd:
        name = sanitize_download_basename(cd)
    if not name:
        name = suggested_local_filename(url)

    ext_hint = extension_from_content_type(content_type)
    path_obj = Path(name)
    suffix = path_obj.suffix.lower()

    if ext_hint:
        if not suffix:
            name = name + ext_hint
        elif suffix == ".bin" and ext_hint != ".bin":
            name = str(path_obj.with_suffix(ext_hint))
    return name


def get_text_with_retries(
    url: str,
    *,
    timeout_sec: float,
    max_retries: int,
    user_agent: str,
) -> HttpGetResult:
    """GET a URL and return decoded text (for small resources like cms-hpt.txt)."""
    headers = {"User-Agent": user_agent}
    last_err: str | None = None
    for attempt in range(max_retries + 1):
        try:
            with httpx.Client(
                timeout=timeout_sec,
                follow_redirects=True,
                headers=headers,
            ) as client:
                r = client.get(url)
                if r.status_code >= 400:
                    last_err = f"HTTP {r.status_code} for {url}"
                    if attempt < max_retries and r.status_code >= 500:
                        time.sleep(_backoff_seconds(attempt))
                        continue
                    return HttpGetResult(r.status_code, None, last_err)
                return HttpGetResult(r.status_code, r.text, None)
        except httpx.HTTPError as e:
            last_err = f"{type(e).__name__}: {e} (url={url})"
            logger.warning("HTTP GET attempt %s failed: %s", attempt + 1, last_err)
            if attempt >= max_retries:
                break
            time.sleep(_backoff_seconds(attempt))
    return HttpGetResult(0, None, last_err or "unknown HTTP error")


def download_to_path(
    url: str,
    dest_dir: Path,
    *,
    force: bool = False,
    timeout_sec: float,
    max_retries: int,
    user_agent: str,
    chunk_size: int = 1024 * 1024,
) -> StreamDownloadResult:
    """
    Stream URL into dest_dir using a filename from Content-Disposition, else URL, plus
    Content-Type extension hints. Atomic replace from .part file.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    last_err: str | None = None
    for attempt in range(max_retries + 1):
        headers = {"User-Agent": user_agent}
        content_type: str | None = None
        etag: str | None = None
        last_modified: str | None = None
        total = 0
        status_code = 0
        tmp: Path | None = None
        try:
            with httpx.Client(
                timeout=timeout_sec,
                follow_redirects=True,
                headers=headers,
            ) as client:
                with client.stream("GET", url) as response:
                    status_code = response.status_code
                    content_type = response.headers.get("content-type")
                    etag = response.headers.get("etag")
                    last_modified = response.headers.get("last-modified")
                    cd = response.headers.get("content-disposition")
                    if response.status_code >= 400:
                        last_err = f"HTTP {response.status_code} for {url}"
                        guess = dest_dir / suggested_local_filename(url)
                        if attempt < max_retries and response.status_code >= 500:
                            time.sleep(_backoff_seconds(attempt))
                            continue
                        return StreamDownloadResult(
                            url,
                            guess,
                            status_code,
                            0,
                            content_type,
                            etag,
                            last_modified,
                            last_err,
                        )

                    basename = resolve_download_basename(
                        content_disposition=cd,
                        content_type=content_type,
                        url=url,
                    )
                    dest = dest_dir / basename
                    if dest.is_file() and not force:
                        response.close()
                        logger.info(
                            "download: skipping existing file %s (use force to re-download)",
                            dest,
                        )
                        return StreamDownloadResult(
                            url,
                            dest,
                            status_code,
                            0,
                            content_type,
                            etag,
                            last_modified,
                            None,
                            skipped=True,
                        )

                    tmp = dest.parent / (dest.name + ".part")
                    if tmp.exists():
                        tmp.unlink(missing_ok=True)
                    with tmp.open("wb") as fh:
                        for chunk in response.iter_bytes(chunk_size):
                            fh.write(chunk)
                            total += len(chunk)
            assert tmp is not None
            tmp.replace(dest)
            return StreamDownloadResult(
                url,
                dest,
                status_code,
                total,
                content_type,
                etag,
                last_modified,
                None,
            )
        except httpx.HTTPError as e:
            last_err = f"{type(e).__name__}: {e}"
            logger.warning("Download attempt %s failed for %s: %s", attempt + 1, url, e)
            guess = dest_dir / suggested_local_filename(url)
            if tmp and tmp.exists():
                tmp.unlink(missing_ok=True)
            if attempt >= max_retries:
                return StreamDownloadResult(
                    url,
                    guess,
                    0,
                    0,
                    content_type,
                    etag,
                    last_modified,
                    last_err,
                )
            time.sleep(_backoff_seconds(attempt))
        except OSError as e:
            guess = dest_dir / suggested_local_filename(url)
            if tmp and tmp.exists():
                tmp.unlink(missing_ok=True)
            return StreamDownloadResult(url, guess, 0, 0, None, None, None, f"OSError: {e}")
    guess = dest_dir / suggested_local_filename(url)
    return StreamDownloadResult(url, guess, 0, 0, None, None, None, last_err or "download failed")

