"""
API smoke test (offline).
Starts uvicorn and hits a few endpoints using requests (no Dhan login required).
"""

import os
import socket
import subprocess
import sys
import time

import requests


def _get_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _wait_ok(url: str, timeout_s: float = 10.0) -> None:
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=1.0)
            if r.status_code == 200:
                return
        except Exception as e:
            last_err = e
        time.sleep(0.2)
    raise RuntimeError(f"Server did not become ready: {url} (last_err={last_err})")


def main():
    port = _get_free_port()
    base = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    # Keep output quieter during smoke test runs.
    env.setdefault("UVICORN_LOG_LEVEL", "warning")
    env.setdefault("DISABLE_AUTO_CONNECT", "1")

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=os.path.dirname(__file__),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_ok(f"{base}/api/health")

        r = requests.get(f"{base}/api/health", timeout=2.0)
        assert r.status_code == 200
        assert "status" in r.json()

        r = requests.get(f"{base}/api/status", timeout=2.0)
        assert r.status_code == 200
        body = r.json()
        assert "engine_running" in body
        assert "market_open" in body

        r = requests.get(f"{base}/api/metrics", timeout=2.0)
        assert r.status_code == 200
        body = r.json()
        assert body.get("status") == "success"
        metrics = body.get("metrics") or {}
        assert "tick_stats" in metrics
        assert "order_stats" in metrics

        # Without Dhan login, start should be blocked.
        r = requests.post(f"{base}/api/start", timeout=2.0)
        assert r.status_code == 200
        assert r.json().get("status") in {"error", "already_running", "armed"}

        r = requests.post(f"{base}/api/stop", timeout=2.0)
        assert r.status_code == 200
    finally:
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=5.0)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

        # If something went wrong, surface server output for debugging.
        if proc.returncode not in (0, None):
            try:
                out = (proc.stdout.read() if proc.stdout else "") or ""
            except Exception:
                out = ""
            if out.strip():
                print(out)


if __name__ == "__main__":
    main()
    print("OK")
