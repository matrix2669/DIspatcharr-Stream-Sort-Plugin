"""Stdlib-only raw acquisition child. No plugin/Django imports or credential logs.

The parent supplies private configuration on stdin, owns the temporary path,
AND kills/reaps this process on its total deadline or cooperative cancellation.
Socket inactivity timeouts alone do not bound DNS or trickling HTTP framing.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.request


def capture(config, *, now=None, opener=None):
    now = now or time.monotonic
    opener = opener or urllib.request.urlopen
    started = config['started']
    response = None
    total = 0
    code = 'capture_setup_failed'
    try:
        request = urllib.request.Request(config['url'], headers=config['headers'])
        code = 'capture_open_failed'
        response = opener(request, timeout=min(2.0, config['timeout_seconds']))
        if response.headers.get('Content-Length') or response.headers.get('Content-Range'):
            code = 'capture_finite_object'
            raise RuntimeError
        deadline = started + config['duration_seconds']
        code = 'capture_write_failed'
        with open(config['path'], 'wb') as output:
            while now() < deadline:
                remaining = config['max_bytes'] - total
                if remaining <= 0:
                    code = 'capture_byte_limit'
                    raise RuntimeError
                code = 'capture_read_failed'
                chunk = response.read1(min(65536, remaining))
                if not chunk:
                    code = 'capture_eof'
                    raise RuntimeError
                code = 'capture_write_failed'
                output.write(chunk)
                total += len(chunk)
        elapsed = max(now() - started, .001)
        if total < 65536:
            code = 'capture_too_little_data'
            raise RuntimeError
        result = {'complete': True, 'bytes': total, 'elapsed_seconds': elapsed,
                'capture_code': 'capture_ok'}
    except Exception:
        # Neither provider error messages nor configuration may reach stdout.
        result = {'complete': False, 'bytes': total, 'capture_code': code,
                'elapsed_seconds': max(now() - started, 0.0)}
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                # Preserve partial measurements without accepting a child that
                # could not close normally. Never expose the close exception.
                result.update(complete=False, capture_code='capture_close_failed')
    return result


def main():
    import sys
    timer = None
    try:
        config = json.load(sys.stdin)
        def expire():
            # Also bounds acquisition if the supervising process disappears.
            try:
                os.unlink(config['path'])
            except OSError:
                pass
            os._exit(124)
        remaining = config['started'] + min(30.0, config['timeout_seconds']) - time.monotonic()
        timer = threading.Timer(max(0, remaining), expire)
        timer.daemon = True
        timer.start()
        result = capture(config)
        print(json.dumps(result, separators=(',', ':')), flush=True)
    except Exception:
        print('{"complete":false,"bytes":0,"capture_code":"capture_setup_failed"}', flush=True)
    finally:
        if timer is not None:
            timer.cancel()


if __name__ == '__main__':
    main()
