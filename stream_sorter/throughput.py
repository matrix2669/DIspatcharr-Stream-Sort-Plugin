from __future__ import annotations

import json
import os
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from .scoring import classify_throughput


DEFAULT_CACHE_PATH = "/data/dispatcharr_stream_sort_analysis.json"
LEGACY_CACHE_PATH = "/data/dispatcharr_stream_sort_throughput.json"
DEFAULT_USER_AGENT = "VLC/3.0.20 LibVLC/3.0.20"


def _read_json(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items()}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {}


def _write_json_atomic(data: dict[str, Any], path: str) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".stream-sort-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _expired(value: Any) -> bool:
    if not value:
        return False
    try:
        expires = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= expires.astimezone(timezone.utc)


def load_cache(path: str = DEFAULT_CACHE_PATH) -> dict[str, dict[str, Any]]:
    """Load throughput entries from the unified analysis cache.

    The old standalone throughput file is read as a migration fallback. Nested
    entries carry their own expiration time, so an old stored 30-minute plugin
    setting cannot expire a still-valid 6-hour throughput measurement.
    """
    data = _read_json(path)
    nested: dict[str, dict[str, Any]] = {}
    direct: dict[str, dict[str, Any]] = {}
    for key, value in data.items():
        if not isinstance(value, dict):
            continue
        throughput = value.get("throughput")
        if isinstance(throughput, dict):
            entry = dict(throughput)
            if _expired(entry.get("expires_at")):
                entry["status"] = "unknown"
                entry["error"] = "cached throughput measurement expired"
            # Freshness for unified entries is owned by expires_at. Removing
            # tested_at from this copy prevents the legacy scorer TTL from
            # applying a second, contradictory expiration policy.
            entry.pop("tested_at", None)
            nested[str(key)] = entry
        elif "status" in value and (
            "measured_mbps" in value or "bytes" in value or "nominal_video_kbps" in value
        ):
            direct[str(key)] = dict(value)
    if nested:
        return nested
    if direct:
        return direct
    if path == DEFAULT_CACHE_PATH and path != LEGACY_CACHE_PATH:
        legacy = _read_json(LEGACY_CACHE_PATH)
        return {str(key): dict(value) for key, value in legacy.items() if isinstance(value, dict)}
    return {}


def save_cache(cache: dict[str, dict[str, Any]], path: str = DEFAULT_CACHE_PATH) -> None:
    """Persist throughput without overwriting media-analysis data."""
    if path != DEFAULT_CACHE_PATH:
        _write_json_atomic(cache, path)
        return
    data = _read_json(path)
    for key, throughput in cache.items():
        existing = data.get(str(key))
        if not isinstance(existing, dict) or (
            "status" in existing
            and "stats" not in existing
            and "stream_id" not in existing
            and "media_checked_at" not in existing
        ):
            existing = {}
        existing["throughput"] = dict(throughput)
        data[str(key)] = existing
    _write_json_atomic(data, path)


def _split_url_headers(raw_url: str) -> tuple[str, dict[str, str]]:
    """Support common M3U `url|Header=value&Header2=value` syntax."""
    if "|" not in raw_url:
        return raw_url, {}
    url, raw_headers = raw_url.split("|", 1)
    headers: dict[str, str] = {}
    for pair in raw_headers.split("&"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        key = urllib.parse.unquote_plus(key).strip()
        value = urllib.parse.unquote_plus(value).strip()
        if key:
            headers[key] = value
    return url, headers


def probe_stream(
    raw_url: str,
    *,
    nominal_video_kbps: float | None,
    duration_seconds: float = 8.0,
    timeout_seconds: float = 10.0,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, Any]:
    """Measure sustained bytes delivered during a bounded live-stream read.

    Probe failures are UNKNOWN, never DEAD. Media health is owned by the built-in
    stream analyzer; throughput only describes delivery capacity.
    """
    tested_at = datetime.now(timezone.utc).isoformat()
    if not raw_url:
        return {"status": "unknown", "tested_at": tested_at, "error": "missing URL"}
    url, extra_headers = _split_url_headers(raw_url)
    headers = {"User-Agent": user_agent, "Accept": "*/*"}
    headers.update(extra_headers)
    request = urllib.request.Request(url, headers=headers)
    started = time.monotonic()
    total = 0
    edge_host = None
    try:
        response = urllib.request.urlopen(request, timeout=timeout_seconds)
        try:
            edge_host = urllib.parse.urlparse(response.geturl()).netloc or None
        except Exception:
            pass
        try:
            deadline = started + max(1.0, float(duration_seconds))
            while True:
                if time.monotonic() >= deadline:
                    break
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
        finally:
            response.close()
    except Exception as exc:
        return {
            "status": "unknown",
            "tested_at": tested_at,
            "bytes": total,
            "edge_host": edge_host,
            "error": f"{type(exc).__name__}: {exc}",
        }
    elapsed = max(time.monotonic() - started, 0.001)
    if total < 64 * 1024 and elapsed < 1.0:
        return {
            "status": "unknown",
            "tested_at": tested_at,
            "bytes": total,
            "elapsed_seconds": round(elapsed, 4),
            "edge_host": edge_host,
            "error": "probe returned too little data for a sustained live-stream measurement",
        }
    measured_mbps = (total * 8.0) / elapsed / 1_000_000.0
    status = classify_throughput(measured_mbps, nominal_video_kbps)
    return {
        "status": status,
        "tested_at": tested_at,
        "bytes": total,
        "elapsed_seconds": round(elapsed, 4),
        "measured_mbps": round(measured_mbps, 4),
        "nominal_video_kbps": nominal_video_kbps,
        "edge_host": edge_host,
    }
