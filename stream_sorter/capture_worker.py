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
    try:
        request = urllib.request.Request(config['url'], headers=config['headers'])
        response = opener(request, timeout=min(2.0, config['timeout_seconds']))
        if response.headers.get('Content-Length') or response.headers.get('Content-Range'):
            raise RuntimeError('Finite object requires direct validation')
        deadline = started + config['duration_seconds']
        with open(config['path'], 'wb') as output:
            while now() < deadline:
                remaining = config['max_bytes'] - total
                if remaining <= 0:
                    raise RuntimeError('Byte bound reached')
                chunk = response.read1(min(65536, remaining))
                if not chunk:
                    raise RuntimeError('Premature EOF')
                output.write(chunk)
                total += len(chunk)
        elapsed = max(now() - started, .001)
        if total < 65536:
            raise RuntimeError('Insufficient live data')
        return {'complete': True, 'bytes': total, 'elapsed_seconds': elapsed}
    except Exception:
        # Neither provider error messages nor configuration may reach stdout.
        return {'complete': False, 'bytes': total}
    finally:
        if response is not None:
            response.close()


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
        print('{"complete":false,"bytes":0}', flush=True)
    finally:
        if timer is not None:
            timer.cancel()


if __name__ == '__main__':
    main()
