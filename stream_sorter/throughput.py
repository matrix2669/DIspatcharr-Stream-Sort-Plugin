from __future__ import annotations

import json
import math
import os
import signal
import shutil
import sys
import subprocess
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
_CAPTURE_WORKER_PATH = os.path.join(os.path.dirname(__file__), "capture_worker.py")


def capture_stream_sample(
    url: str,
    *,
    nominal_video_kbps: float,
    duration_seconds: float = 8.0,
    timeout_seconds: float = 10.0,
    user_agent: str | None = None,
    ffmpeg_path: str = "ffmpeg",
    temp_directory: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    """Capture one wall-clock-bounded provider sample for throughput and local analysis.

    The caller owns the returned path and must remove it. Output bytes are
    measured from the MPEG-TS stream-copy sample, so no video decode is needed
    while the provider reservation is held.
    """
    tested_at = datetime.now(timezone.utc).isoformat()
    if not url:
        return {
            "status": "unknown",
            "tested_at": tested_at,
            "error_type": "stream_unreachable",
            "error": "Stream URL is empty",
        }, None

    clean_url, embedded_headers = _split_url_headers(url)
    headers = dict(embedded_headers)
    headers.setdefault("User-Agent", user_agent or DEFAULT_USER_AGENT)
    header_blob = "".join(f"{key}: {value}\r\n" for key, value in headers.items())
    started = time.monotonic()
    fd = None
    sample_path = None
    intentionally_stopped = False
    capture_elapsed = 0.0
    stderr = ""
    try:
        fd, sample_path = tempfile.mkstemp(
            prefix="stream-sort-capture-",
            suffix=".ts",
            dir=temp_directory,
        )
        os.close(fd)
        fd = None
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-rw_timeout",
            str(int(max(1.0, timeout_seconds) * 1_000_000)),
        ]
        if header_blob:
            command.extend(["-headers", header_blob])
        command.extend([
            "-i",
            clean_url,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-f",
            "mpegts",
            sample_path,
        ])
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            _stdout, stderr = process.communicate(timeout=max(0.1, duration_seconds))
            capture_elapsed = max(time.monotonic() - started, 0.001)
            minimum_elapsed = max(0.1, duration_seconds * 0.9)
            if capture_elapsed < minimum_elapsed:
                raise RuntimeError(
                    f"FFmpeg capture ended after {capture_elapsed:.3f}s; expected at least {minimum_elapsed:.3f}s"
                )
        except subprocess.TimeoutExpired:
            intentionally_stopped = True
            capture_elapsed = max(time.monotonic() - started, 0.001)
            process.send_signal(signal.SIGINT)
            try:
                _stdout, stderr = process.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    _stdout, stderr = process.communicate(timeout=1.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    _stdout, stderr = process.communicate()
        captured_bytes = os.path.getsize(sample_path)
        if captured_bytes <= 0 or (process.returncode and not intentionally_stopped):
            error = (stderr or "FFmpeg produced no usable capture data").strip()
            raise RuntimeError(error)

        measured_mbps = captured_bytes * 8.0 / capture_elapsed / 1_000_000.0
        return {
            "status": classify_throughput(measured_mbps, nominal_video_kbps),
            "tested_at": tested_at,
            "measured_mbps": round(measured_mbps, 3),
            "nominal_video_kbps": round(float(nominal_video_kbps), 1),
            "bytes": captured_bytes,
            "elapsed_seconds": round(capture_elapsed, 4),
            "measurement_source": "ffmpeg_stream_copy",
        }, sample_path
    except Exception as exc:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if sample_path:
            try:
                os.unlink(sample_path)
            except OSError:
                pass
        return {
            "status": "unknown",
            "tested_at": tested_at,
            "error_type": "stream_unreachable",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": round(max(time.monotonic() - started, 0.0), 4),
            "measurement_source": "ffmpeg_stream_copy",
        }, None


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


def load_cache(path: str = DEFAULT_CACHE_PATH) -> dict[str, dict[str, Any]]:
    """Load throughput entries from the unified analysis cache.

    The old standalone throughput file is read as a migration fallback.
    Freshness is evaluated by scoring from checked_at, status-specific current
    TTL settings, and stable stream jitter rather than stored expiration data.
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
            entry.pop("expires_at", None)
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


def _capture_python():
    # Embedded Python hosts such as uWSGI may put their own binary in
    # sys.executable. Prefer the actual interpreter in that environment.
    candidates = [sys.executable, os.path.join(sys.prefix, 'bin', 'python3'),
                  shutil.which('python3')]
    for candidate in candidates:
        if candidate and os.path.basename(candidate).startswith('python') and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError('Python interpreter unavailable for bounded acquisition')


def _capture_config(raw_url, *, duration_seconds, timeout_seconds, user_agent,
                    max_bytes, path, started):
    if not 1 <= duration_seconds <= 30 or timeout_seconds < duration_seconds:
        raise ValueError('Shared capture requires 1-30 seconds and a sufficient timeout')
    if not 0 < max_bytes <= 64 * 1024 * 1024:
        raise ValueError('Shared capture byte bound must not exceed 64 MiB')
    url, headers = _split_url_headers(raw_url)
    headers.setdefault('User-Agent', user_agent)
    headers.setdefault('Accept', '*/*')
    if not url.lower().startswith(('http://', 'https://')):
        raise ValueError('Shared RAM capture supports HTTP(S) only')
    return {'url': url, 'headers': headers, 'duration_seconds': duration_seconds,
            'timeout_seconds': timeout_seconds, 'max_bytes': max_bytes,
            'path': path, 'started': started}


# Treat the child boundary as untrusted diagnostics: never forward arbitrary
# strings or extra fields into a result, a log, or the scan summary.
_CAPTURE_CHILD_CODES = frozenset({
    'capture_ok', 'capture_setup_failed', 'capture_open_failed',
    'capture_finite_object', 'capture_byte_limit', 'capture_eof',
    'capture_too_little_data', 'capture_read_failed', 'capture_write_failed',
    'capture_close_failed',
})
_CAPTURE_FAILURE_CODES = (_CAPTURE_CHILD_CODES - {'capture_ok'}) | frozenset({
    'capture_canceled', 'capture_deadline', 'capture_invalid_config',
    'capture_unsupported_protocol', 'capture_storage_failed',
    'capture_start_failed', 'capture_worker_failed', 'capture_invalid_result',
})


def _capture_number(value):
    """Only actual finite nonnegative numbers cross the diagnostic boundary."""
    if type(value) not in (int, float):
        return None
    try:
        return value if math.isfinite(value) and value >= 0 else None
    except (OverflowError, ValueError):
        return None


def capture_bounded_sample(raw_url, *, duration_seconds, timeout_seconds, temp_directory,
                           user_agent=DEFAULT_USER_AGENT, max_bytes=32 * 1024 * 1024,
                           stop_requested=lambda: False):
    """Killable raw acquisition with one TOTAL deadline, including DNS/HTTP.

    The process is killed and reaped before return on timeout or cancellation.
    Successful elapsed time is the live acquisition wall time, including setup;
    local analysis and parent shutdown/cleanup never contribute to throughput.
    """
    tested = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    path = None
    process = None
    accepted = False
    code = 'capture_invalid_config'
    child_elapsed = None
    child_bytes = None
    failure = {'tested_at': tested, 'status': 'unknown', 'complete': False,
               'bytes': 0, 'elapsed_seconds': None,
               'measurement_source': 'raw_http_capture',
               'elapsed_source': 'parent_wall', 'error_type': 'capture_incomplete',
               'error': 'Bounded sample incomplete; direct reserved fallback required'}
    try:
        if stop_requested():
            code = 'capture_canceled'
            raise RuntimeError
        if isinstance(raw_url, str) and not raw_url.lower().startswith(('http://', 'https://')):
            code = 'capture_unsupported_protocol'
            raise ValueError
        config = _capture_config(raw_url, duration_seconds=duration_seconds,
            timeout_seconds=timeout_seconds, user_agent=user_agent, max_bytes=max_bytes,
            path=None, started=started)
        code = 'capture_storage_failed'
        fd, path = tempfile.mkstemp(prefix='stream-sort-bounded-', suffix='.sample', dir=temp_directory)
        os.close(fd)
        config['path'] = path
        # No URL/headers in argv, stderr, or exception messages. -I/-S avoids
        # loading plugin/app startup hooks in the acquisition child.
        code = 'capture_start_failed'
        command = [_capture_python(), '-I', '-S', _CAPTURE_WORKER_PATH]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, text=True)
        deadline = started + min(30.0, timeout_seconds)
        payload = json.dumps(config)
        code = 'capture_worker_failed'
        while True:
            remaining = deadline - time.monotonic()
            if stop_requested():
                code = 'capture_canceled'
                raise RuntimeError
            if remaining <= 0:
                code = 'capture_deadline'
                raise RuntimeError
            try:
                output, _ = process.communicate(input=payload, timeout=min(.05, remaining))
                break
            except subprocess.TimeoutExpired:
                payload = None
        # The child's independent watchdog uses 124 if the parent disappears or
        # loses the race to the deadline. Its unflushed byte count is unknown.
        if process.returncode == 124:
            code = 'capture_deadline'
            raise RuntimeError
        code = 'capture_invalid_result'
        measured = json.loads(output)
        if not isinstance(measured, dict):
            raise ValueError
        child_elapsed = _capture_number(measured.get('elapsed_seconds'))
        child_bytes = _capture_number(measured.get('bytes'))
        if type(child_bytes) is not int or child_bytes > max_bytes:
            child_bytes = None
        total = os.path.getsize(path)
        elapsed = child_elapsed or 0
        if not measured.get('complete'):
            child_code = measured.get('capture_code')
            code = child_code if isinstance(child_code, str) and child_code in _CAPTURE_CHILD_CODES - {'capture_ok'} else 'capture_worker_failed'
            raise RuntimeError
        if (process.returncode or not measured.get('complete') or not 65536 <= total <= max_bytes
                or measured.get('bytes') != total or elapsed < duration_seconds):
            raise RuntimeError('Incomplete live acquisition')
        accepted = True
        return {'tested_at': tested, 'bytes': total, 'elapsed_seconds': elapsed,
                'measured_mbps': total * 8 / elapsed / 1_000_000,
                'measurement_source': 'raw_http_capture', 'complete': True,
                'elapsed_source': 'acquisition', 'capture_code': 'capture_ok'}, path
    except Exception:
        failure['capture_code'] = code
        failure['elapsed_seconds'] = (child_elapsed if child_elapsed is not None
                                      else max(time.monotonic() - started, 0.0))
        failure['elapsed_source'] = 'acquisition' if child_elapsed is not None else 'parent_wall'
        return failure, None
    finally:
        try:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    # No reservation release while its child consumes a source.
                    process.communicate(timeout=1.0)
                else:
                    process.wait(timeout=1.0)
        finally:
            if path and not accepted:
                failure['bytes'] = child_bytes
                try:
                    on_disk = os.path.getsize(path)
                    if failure['bytes'] is None:
                        failure['bytes'] = on_disk
                except OSError:
                    pass
                finally:
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass
