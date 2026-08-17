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

DEFAULT_CACHE_PATH = "/data/dispatcharr_stream_sort_throughput.json"
DEFAULT_USER_AGENT = "VLC/3.0.20 LibVLC/3.0.20"


def load_cache(path: str = DEFAULT_CACHE_PATH) -> dict[str, dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as handle: data = json.load(handle)
        if isinstance(data, dict): return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    except (FileNotFoundError, json.JSONDecodeError, OSError): pass
    return {}


def save_cache(cache: dict[str, dict[str, Any]], path: str = DEFAULT_CACHE_PATH) -> None:
    directory = os.path.dirname(path) or "."; os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".stream-sort-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(cache, handle, indent=2, sort_keys=True); handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try: os.unlink(tmp_path)
        except OSError: pass
        raise


def _split_url_headers(raw_url: str) -> tuple[str, dict[str, str]]:
    if "|" not in raw_url: return raw_url, {}
    url, raw_headers = raw_url.split("|", 1); headers = {}
    for pair in raw_headers.split("&"):
        if "=" not in pair: continue
        key, value = pair.split("=", 1); key = urllib.parse.unquote_plus(key).strip(); value = urllib.parse.unquote_plus(value).strip()
        if key: headers[key] = value
    return url, headers


def probe_stream(raw_url: str, *, nominal_video_kbps: float | None, duration_seconds: float = 2.5, timeout_seconds: float = 6.0, max_bytes: int = 12 * 1024 * 1024, user_agent: str = DEFAULT_USER_AGENT) -> dict[str, Any]:
    tested_at = datetime.now(timezone.utc).isoformat()
    if not raw_url: return {"status": "dead", "tested_at": tested_at, "error": "missing URL"}
    url, extra_headers = _split_url_headers(raw_url); headers = {"User-Agent": user_agent, "Accept": "*/*"}; headers.update(extra_headers); request = urllib.request.Request(url, headers=headers)
    started = time.monotonic(); total = 0
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            while total < max_bytes:
                chunk = response.read(min(64 * 1024, max_bytes - total))
                if not chunk: break
                total += len(chunk)
                if time.monotonic() - started >= duration_seconds: break
    except Exception as exc:
        return {"status": "dead", "tested_at": tested_at, "bytes": total, "error": f"{type(exc).__name__}: {exc}"}
    elapsed = max(time.monotonic() - started, 0.001); measured_mbps = (total * 8.0) / elapsed / 1_000_000.0; status = classify_throughput(measured_mbps, nominal_video_kbps)
    return {"status": status, "tested_at": tested_at, "bytes": total, "elapsed_seconds": round(elapsed, 4), "measured_mbps": round(measured_mbps, 4), "nominal_video_kbps": nominal_video_kbps}
