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

"""Unit tests for ReadinessShedMiddleware (geoid#2946 / #2924)."""
from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from dynastore.extensions.tools.readiness_shed_middleware import ReadinessShedMiddleware
from dynastore.tools.memory_watchdog import MemoryWatchdogConfig
from dynastore.tools.serving_state import clear_draining, set_draining


async def _ok(request):
    return PlainTextResponse("ok")


def _make_app() -> Starlette:
    app = Starlette(
        routes=[
            Route("/health", _ok),
            Route("/ready", _ok),
            Route("/items", _ok),
        ]
    )
    app.add_middleware(ReadinessShedMiddleware)
    return app


@pytest.fixture(autouse=True)
def _reset():
    clear_draining()
    yield
    clear_draining()


def _patch_config(monkeypatch, cfg: MemoryWatchdogConfig) -> None:
    async def _load() -> MemoryWatchdogConfig:
        return cfg
    monkeypatch.setattr(
        "dynastore.extensions.tools.readiness_shed_middleware.load_memory_watchdog_config",
        _load,
    )


def test_passes_through_when_not_draining(monkeypatch) -> None:
    _patch_config(monkeypatch, MemoryWatchdogConfig(readiness_shed_enabled=True))
    client = TestClient(_make_app())
    resp = client.get("/items")
    assert resp.status_code == 200


def test_sheds_with_503_when_draining_and_enabled(monkeypatch) -> None:
    _patch_config(monkeypatch, MemoryWatchdogConfig(readiness_shed_enabled=True))
    set_draining()
    client = TestClient(_make_app())
    resp = client.get("/items")
    assert resp.status_code == 503
    assert "Retry-After" in resp.headers


def test_does_not_shed_when_draining_but_disabled(monkeypatch) -> None:
    _patch_config(monkeypatch, MemoryWatchdogConfig(readiness_shed_enabled=False))
    set_draining()
    client = TestClient(_make_app())
    resp = client.get("/items")
    assert resp.status_code == 200


@pytest.mark.parametrize("path", ["/health", "/ready"])
def test_excludes_health_and_ready_even_while_draining(monkeypatch, path: str) -> None:
    _patch_config(monkeypatch, MemoryWatchdogConfig(readiness_shed_enabled=True))
    set_draining()
    client = TestClient(_make_app())
    resp = client.get(path)
    assert resp.status_code == 200


def test_sheds_outside_an_inner_body_middleware(monkeypatch) -> None:
    """The shed must sit OUTSIDE body-handling middleware, so a draining worker
    rejects with 503 before anything inner touches the request body. Mirrors
    main.py, which registers ReadinessShedMiddleware AFTER
    SyncIngestBodyLimitMiddleware (last-added = outermost in Starlette), so the
    shed pre-empts the body-limit middleware's eager buffering rather than
    piling memory onto a worker already recycling for memory pressure.
    """
    reached_inner = {"value": False}

    class _InnerBodyMiddleware:
        def __init__(self, app):
            self.app = app

        async def __call__(self, scope, receive, send):
            reached_inner["value"] = True
            await self.app(scope, receive, send)

    _patch_config(monkeypatch, MemoryWatchdogConfig(readiness_shed_enabled=True))
    set_draining()

    app = Starlette(routes=[Route("/items", _ok, methods=["POST"])])
    # Same registration order as main.py: inner body middleware first, shed
    # after → shed is outermost of the two.
    app.add_middleware(_InnerBodyMiddleware)
    app.add_middleware(ReadinessShedMiddleware)

    resp = TestClient(app).post("/items", content=b"payload")
    assert resp.status_code == 503
    assert reached_inner["value"] is False


def test_fails_open_when_config_load_raises(monkeypatch) -> None:
    """A config-resolution error while draining must serve the request
    (fail-open), never 500 it — the serving path can't break on a config hiccup.
    """
    async def _boom() -> MemoryWatchdogConfig:
        raise RuntimeError("config store unreachable")

    monkeypatch.setattr(
        "dynastore.extensions.tools.readiness_shed_middleware.load_memory_watchdog_config",
        _boom,
    )
    set_draining()
    resp = TestClient(_make_app()).get("/items")
    assert resp.status_code == 200
