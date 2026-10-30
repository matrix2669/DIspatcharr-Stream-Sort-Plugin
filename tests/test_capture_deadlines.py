"""Actual killable acquisition tests against loopback fixtures only."""
from contextlib import contextmanager
import os
from pathlib import Path
import socketserver
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_sorter import incremental, throughput


@contextmanager
def drip_provider(mode):
    connected, disconnected, stop = threading.Event(), threading.Event(), threading.Event()
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(2)
            try:
                self.request.recv(8192)
                connected.set()
                if mode == 'headers':
                    prefix = b'HTTP/1.1 200 OK\r\nX-Slow: '
                    unit = b'x'
                elif mode == 'chunk':
                    prefix = b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'
                    unit = b'0'  # drip an unfinished chunk-size line
                else:
                    prefix = b'HTTP/1.1 200 OK\r\nContent-Type: video/MP2T\r\n\r\n'
                    unit = b'G' * 65536
                self.request.sendall(prefix)
                while not stop.is_set():
                    self.request.sendall(unit)
                    time.sleep(.01)
            except OSError:
                disconnected.set()
    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True
    server = Server(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_address[1]}/controlled', connected, disconnected
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        thread.join(2)


def track_children(monkeypatch):
    original = throughput.subprocess.Popen
    children = []
    def start(command, **kwargs):
        assert all('controlled-token' not in value for value in command)
        process = original(command, **kwargs)
        children.append(process)
        return process
    monkeypatch.setattr(throughput.subprocess, 'Popen', start)
    return children


@pytest.mark.parametrize('mode,cancel', [('headers', False), ('chunk', False), ('chunk', True)])
def test_total_deadline_or_cancel_closes_dripping_client_and_releases_exact_slot(tmp_path, monkeypatch, mode, cancel):
    monkeypatch.setenv('NO_PROXY', '127.0.0.1')
    monkeypatch.setattr(incremental, 'analysis_cancel_requested', lambda: False)
    children = track_children(monkeypatch)
    stop = threading.Event()
    with drip_provider(mode) as (url, connected, disconnected):
        def cancel_when_connected():
            if connected.wait(3):
                time.sleep(.1)
                stop.set()
        cancel_thread = threading.Thread(target=cancel_when_connected, daemon=True) if cancel else None
        if cancel_thread:
            cancel_thread.start()
        released = []
        def release(slot):
            assert slot == 'exact-slot'
            assert children and all(child.poll() is not None for child in children)
            released.append(slot)
        capacity = SimpleNamespace(try_acquire=Mock(return_value=(True, 'exact-slot')), release=release)
        def acquire(item):
            return throughput.capture_bounded_sample(url + '|Authorization=controlled-token',
                duration_seconds=1, timeout_seconds=5 if cancel else 1.3,
                temp_directory=str(tmp_path), stop_requested=stop.is_set)
        started = time.monotonic()
        completed = list(incremental._fair_account_futures(
            [{'id': 1, 'account_id': 7}], acquire, max_workers=1,
            thread_name_prefix='controlled-deadline', capacity_manager=capacity))
        elapsed = time.monotonic() - started
        measured, path = completed[0][1].result()
        assert elapsed < (2 if cancel else 3)
        assert connected.is_set()
        assert disconnected.wait(1.5), 'the killed acquisition must close its active provider socket'
        assert path is None and measured['complete'] is False
        assert measured['error_type'] == 'capture_incomplete'
        assert released == ['exact-slot']
        assert all(child.returncode is not None for child in children)
        assert not list(tmp_path.iterdir())
        if cancel_thread:
            cancel_thread.join(1)


@pytest.mark.parametrize('phase', ['dns', 'startup'])
def test_total_deadline_reaps_blocked_setup_without_contacting_provider(tmp_path, monkeypatch, phase):
    children = track_children(monkeypatch)
    real_worker = throughput._CAPTURE_WORKER_PATH
    blocked_worker = tmp_path / 'blocked_worker.py'
    if phase == 'dns':
        blocked_worker.write_text('import socket,time,runpy\n'
            'socket.getaddrinfo=lambda *a,**kw: time.sleep(60)\n'
            f'runpy.run_path({real_worker!r}, run_name="__main__")\n')
    else:
        blocked_worker.write_text('import time\ntime.sleep(60)\n')
    monkeypatch.setattr(throughput, '_CAPTURE_WORKER_PATH', str(blocked_worker))
    started = time.monotonic()
    measured, path = throughput.capture_bounded_sample('http://never-contacted.invalid/controlled-token',
        duration_seconds=1, timeout_seconds=1.3, temp_directory=str(tmp_path))
    assert time.monotonic() - started < 3
    assert path is None and measured['complete'] is False
    assert len(children) == 1 and children[0].poll() is not None
    assert list(tmp_path.iterdir()) == [blocked_worker]


def test_success_measures_live_capture_elapsed_not_shutdown_or_local_read_speed(tmp_path, monkeypatch):
    monkeypatch.setenv('NO_PROXY', '127.0.0.1')
    children = track_children(monkeypatch)
    with drip_provider('body') as (url, connected, disconnected):
        measured, path = throughput.capture_bounded_sample(url, duration_seconds=1,
            timeout_seconds=2, temp_directory=str(tmp_path), max_bytes=16 * 1024 * 1024)
        try:
            assert measured['complete'] is True
            assert 1 <= measured['elapsed_seconds'] < 2
            assert measured['bytes'] == os.path.getsize(path)
            assert measured['measured_mbps'] == measured['bytes'] * 8 / measured['elapsed_seconds'] / 1_000_000
            assert all(child.poll() is not None for child in children)
            assert disconnected.wait(1.5)
        finally:
            if path:
                Path(path).unlink()
