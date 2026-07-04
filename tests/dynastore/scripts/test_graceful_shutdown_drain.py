#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Subprocess regression test for the SIGTERM drain budget (#2925 / #2924).

Spawns a real gunicorn + ``DrainAwareUvicornWorker`` process (the exact
worker class ``start.sh`` now uses), holds a request open, sends SIGTERM,
and asserts:

1. A request that finishes within the drain budget still gets its
   response (the #2925 "dropped in-flight request" symptom).
2. ASGI lifespan shutdown always runs before the process exits — even
   when a connection is stalled past the drain budget — which is what
   lets the real app dispose DB engines / stop BackgroundSupervisor /
   close LISTEN connections instead of leaking them (#2924's app leg).

``gunicorn``/``uvicorn`` are Docker-build-time-only deps (not in
pyproject.toml), so the whole module is skipped when they are missing
from the local venv.
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from threading import Thread

import pytest

pytest.importorskip("gunicorn")
pytest.importorskip("uvicorn")

from tests._repo_paths import CORE_SRC  # noqa: E402

_FIXTURE_DIR = Path(__file__).resolve().parent
_CORE_SRC_PARENT = str(CORE_SRC.parent)  # .../packages/core/src


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise TimeoutError(f"gunicorn never opened port {port}")


def _wait_for_file(path: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise TimeoutError(f"{path} was never created")


def _spawn_gunicorn(
    port: int,
    drain_timeout: int,
    graceful_timeout: int,
    sentinel_file: Path,
    request_started_file: Path,
) -> subprocess.Popen:
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([_CORE_SRC_PARENT, str(_FIXTURE_DIR)]),
        "GUNICORN_DRAIN_TIMEOUT": str(drain_timeout),
        "LIFESPAN_SENTINEL_FILE": str(sentinel_file),
        "REQUEST_STARTED_SENTINEL_FILE": str(request_started_file),
    }
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "gunicorn",
            "--worker-class", "dynastore.scripts.gunicorn_worker.DrainAwareUvicornWorker",
            "_gunicorn_drain_fixture_app:app",
            "--bind", f"127.0.0.1:{port}",
            "--workers", "1",
            "--timeout", "30",
            "--graceful-timeout", str(graceful_timeout),
            "--log-level", "warning",
        ],
        cwd=_FIXTURE_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    _wait_for_port(port)
    return proc


def test_in_flight_request_survives_sigterm_within_drain_budget(tmp_path: Path) -> None:
    """A request that finishes inside the drain budget must still get its response."""
    port = _free_port()
    sentinel = tmp_path / "shutdown.marker"
    request_started = tmp_path / "request_started.marker"
    proc = _spawn_gunicorn(
        port, drain_timeout=6, graceful_timeout=8,
        sentinel_file=sentinel, request_started_file=request_started,
    )
    result: dict = {}
    try:
        def _do_request() -> None:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/slow?seconds=2", timeout=15) as resp:
                    result["status"] = resp.status
            except urllib.error.URLError as exc:
                result["error"] = str(exc)

        thread = Thread(target=_do_request)
        thread.start()
        # Synchronize on the handler actually having started -- a fixed
        # sleep is flaky under parallel test-worker CPU contention.
        _wait_for_file(request_started)
        proc.send_signal(signal.SIGTERM)
        thread.join(timeout=15)

        assert result.get("status") == 200, (
            f"in-flight request did not complete cleanly across SIGTERM: {result}\n"
            f"gunicorn output:\n{proc.stdout.read() if proc.stdout else ''}"
        )
    finally:
        proc.wait(timeout=15)


def test_lifespan_shutdown_runs_even_with_a_stalled_connection(tmp_path: Path) -> None:
    """Bounded drain must still reach ASGI lifespan shutdown (DB teardown)
    even when a connection outlives the drain budget (#2924's app leg).

    Without the bound, uvicorn waits forever for the stalled connection
    and lifespan shutdown -- and therefore DB engine dispose / background
    service stop / LISTEN teardown -- never runs until an external kill.
    """
    port = _free_port()
    sentinel = tmp_path / "shutdown.marker"
    request_started = tmp_path / "request_started.marker"
    proc = _spawn_gunicorn(
        port, drain_timeout=2, graceful_timeout=5,
        sentinel_file=sentinel, request_started_file=request_started,
    )
    try:
        def _do_stalled_request() -> None:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/slow?seconds=30", timeout=20)
            except (urllib.error.URLError, TimeoutError):
                pass  # expected — the connection is forcibly cancelled by the drain bound

        thread = Thread(target=_do_stalled_request, daemon=True)
        thread.start()
        _wait_for_file(request_started)
        proc.send_signal(signal.SIGTERM)

        returncode = proc.wait(timeout=15)
        assert returncode == 0, (
            f"gunicorn master did not exit cleanly (returncode={returncode})\n"
            f"output:\n{proc.stdout.read() if proc.stdout else ''}"
        )
        assert sentinel.exists(), (
            "ASGI lifespan shutdown never ran within the drain budget — "
            "DB engine dispose / BackgroundSupervisor.stop() would be skipped "
            "on a real recycle, leaking sessions/LISTEN connections (#2924)."
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
