"""Cross-entry-point analysis execution and cancellation controls."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from functools import wraps
import json
import os
import tempfile
import time
import uuid


_RUNTIME_DIR = (
    "/data"
    if os.path.isdir("/data") and os.access("/data", os.W_OK)
    else tempfile.gettempdir()
)
ANALYSIS_EXECUTION_LOCK_PATH = os.path.join(
    _RUNTIME_DIR, "dispatcharr_stream_sort_analysis_execution.lock"
)
ANALYSIS_EXECUTION_STATE_PATH = os.path.join(
    _RUNTIME_DIR, "dispatcharr_stream_sort_analysis_execution.json"
)
ANALYSIS_CANCEL_PATH = os.path.join(
    _RUNTIME_DIR, "dispatcharr_stream_sort_analysis_cancel.json"
)
ANALYSIS_CONTROL_LOCK_PATH = os.path.join(
    _RUNTIME_DIR, "dispatcharr_stream_sort_analysis_control.lock"
)


class AnalysisAlreadyRunning(RuntimeError):
    """Raised when another analysis entry point owns the execution lease."""


class AnalysisCancelled(RuntimeError):
    """Raised after a requested cancellation has safely drained active probes."""

    def __init__(self, message, *, result=None):
        super().__init__(message)
        self.result = result


_active_token = None


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return {}


def _write_json(path, value):
    directory = os.path.dirname(path) or "."
    fd, temporary_path = tempfile.mkstemp(prefix=".stream-sort-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass


def _remove_if_token_matches(path, token):
    if _read_json(path).get("token") != token:
        return
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


@contextmanager
def _control_lock():
    os.makedirs(os.path.dirname(ANALYSIS_CONTROL_LOCK_PATH) or ".", exist_ok=True)
    handle = open(ANALYSIS_CONTROL_LOCK_PATH, "a+", encoding="utf-8")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _execution_lease_is_held():
    os.makedirs(os.path.dirname(ANALYSIS_EXECUTION_LOCK_PATH) or ".", exist_ok=True)
    handle = open(ANALYSIS_EXECUTION_LOCK_PATH, "a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    finally:
        handle.close()


def analysis_cancel_requested():
    token = _active_token
    return bool(token and _read_json(ANALYSIS_CANCEL_PATH).get("token") == token)


def raise_if_analysis_cancelled():
    if analysis_cancel_requested():
        raise AnalysisCancelled("Stream analysis was canceled")


def request_analysis_cancel():
    with _control_lock():
        state = _read_json(ANALYSIS_EXECUTION_STATE_PATH)
        token = state.get("token")
        if not token or not _execution_lease_is_held():
            if token:
                _remove_if_token_matches(ANALYSIS_EXECUTION_STATE_PATH, token)
                _remove_if_token_matches(ANALYSIS_CANCEL_PATH, token)
            return {"status": "skipped", "message": "No analysis scan is currently running."}
        if not state.get("accepting_cancel", True):
            return {
                "status": "skipped",
                "message": "The scan has finished probing and is already committing its results.",
            }
        _write_json(
            ANALYSIS_CANCEL_PATH,
            {
                "token": token,
                "requested_at": time.time(),
                "owner_pid": state.get("owner_pid"),
            },
        )
    return {
        "status": "ok",
        "message": (
            "Stop requested. Active probes will finish and release their provider "
            "reservations before the scan exits."
        ),
    }


def close_analysis_cancel_window():
    """Close the stop window and return whether cancellation was requested."""
    token = _active_token
    if not token:
        return False
    with _control_lock():
        canceled = _read_json(ANALYSIS_CANCEL_PATH).get("token") == token
        state = _read_json(ANALYSIS_EXECUTION_STATE_PATH)
        if state.get("token") != token:
            return canceled
        state["accepting_cancel"] = False
        state["commit_started_at"] = time.time()
        _write_json(ANALYSIS_EXECUTION_STATE_PATH, state)
        return canceled


@contextmanager
def analysis_maintenance_execution():
    """Acquire the analyzer lease for short non-scan maintenance actions."""
    lock_handle = open(ANALYSIS_EXECUTION_LOCK_PATH, "a+", encoding="utf-8")
    acquired = False
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as exc:
            state = _read_json(ANALYSIS_EXECUTION_STATE_PATH)
            owner = state.get("owner_pid")
            detail = f" by process {owner}" if owner else ""
            raise AnalysisAlreadyRunning(
                f"Another stream analysis is already running{detail}"
            ) from exc
        yield
    finally:
        if acquired:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        lock_handle.close()


def exclusive_analysis_execution(function):
    """Ensure every caller, including direct shell callers, shares one lease."""

    @wraps(function)
    def wrapped(*args, **kwargs):
        global _active_token

        lock_handle = open(ANALYSIS_EXECUTION_LOCK_PATH, "a+", encoding="utf-8")
        acquired = False
        token = None
        try:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError as exc:
                state = _read_json(ANALYSIS_EXECUTION_STATE_PATH)
                owner = state.get("owner_pid")
                detail = f" by process {owner}" if owner else ""
                raise AnalysisAlreadyRunning(
                    f"Another stream analysis is already running{detail}"
                ) from exc

            token = uuid.uuid4().hex
            _active_token = token
            with _control_lock():
                _write_json(
                    ANALYSIS_EXECUTION_STATE_PATH,
                    {
                        "token": token,
                        "owner_pid": os.getpid(),
                        "started_at": time.time(),
                        "accepting_cancel": True,
                    },
                )
            result = function(*args, **kwargs)
            raise_if_analysis_cancelled()
            return result
        finally:
            if acquired and token:
                with _control_lock():
                    _remove_if_token_matches(ANALYSIS_CANCEL_PATH, token)
                    _remove_if_token_matches(ANALYSIS_EXECUTION_STATE_PATH, token)
                if _active_token == token:
                    _active_token = None
            if acquired:
                try:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            lock_handle.close()

    return wrapped
