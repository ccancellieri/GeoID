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

"""Unit tests for :class:`SyncIngestBodyLimitMiddleware` (#2657) — the
pure-ASGI middleware bounding the inbound body of the synchronous bulk
item-POST before it is parsed.
"""

import pytest

import dynastore.tools.discovery as discovery
from dynastore.extensions.tools.body_size_limit import (
    _DEFAULT_MAX_BODY_BYTES,
    SyncIngestBodyLimitMiddleware,
)
from dynastore.tasks.ingestion.ingestion_config import IngestionPluginConfig

ITEMS_PATH = "/stac/catalogs/cat1/collections/col1/items"


def _scope(path=ITEMS_PATH, method="POST", content_length=None):
    headers = []
    if content_length is not None:
        headers.append((b"content-length", str(content_length).encode()))
    return {"type": "http", "method": method, "path": path, "headers": headers}


class _RecordingApp:
    """Downstream ASGI stand-in that drains ``receive`` like a body parser
    would, and records what it saw."""

    def __init__(self):
        self.called = False
        self.received_bodies: list[bytes] = []
        self.sent: list[dict] = []

    async def __call__(self, scope, receive, send):
        self.called = True
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            self.received_bodies.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})


def _chunk_receive(chunks: list[bytes]):
    """``receive`` yielding ``http.request`` messages for *chunks*, then
    ``http.disconnect``. Tracks how many times it was actually invoked."""
    state = {"i": 0, "calls": 0}

    async def receive():
        state["calls"] += 1
        i = state["i"]
        if i >= len(chunks):
            return {"type": "http.disconnect"}
        state["i"] = i + 1
        more = i < len(chunks) - 1
        return {"type": "http.request", "body": chunks[i], "more_body": more}

    receive.state = state
    return receive


def _never_receive():
    async def receive():
        raise AssertionError("receive() must not be called")

    return receive


class _Send:
    def __init__(self):
        self.messages: list[dict] = []

    async def __call__(self, message):
        self.messages.append(message)

    @property
    def status(self):
        for m in self.messages:
            if m["type"] == "http.response.start":
                return m["status"]
        return None

    @property
    def body(self) -> bytes:
        return b"".join(
            m.get("body", b"") for m in self.messages if m["type"] == "http.response.body"
        )


class _FakePlatformConfigs:
    """Minimal ``PlatformConfigsProtocol`` stand-in."""

    def __init__(self, max_body_mb: int):
        self.max_body_mb = max_body_mb

    @property
    def is_platform_manager(self) -> bool:
        return True

    async def get_config(self, config_cls, ctx=None):
        assert config_cls is IngestionPluginConfig
        return IngestionPluginConfig(sync_ingest_max_body_mb=self.max_body_mb)


@pytest.fixture(autouse=True)
def _no_platform_configs(monkeypatch):
    """Default: no config service registered — exercises the fallback."""
    monkeypatch.setattr(discovery, "get_protocol", lambda proto: None)


# ---------------------------------------------------------------------------
# Path / method gating
# ---------------------------------------------------------------------------


async def test_non_post_is_noop_even_when_oversize():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    scope = _scope(method="GET", content_length=_DEFAULT_MAX_BODY_BYTES + 1)
    send = _Send()
    receive = _chunk_receive([])  # GET has no body to drain
    await mw(scope, receive, send)
    # The middleware must forward straight to the app without inspecting
    # Content-Length at all for a non-POST request.
    assert app.called is True
    assert send.status == 200


async def test_non_item_path_not_clamped_even_when_oversize():
    """Asset upload / search routes must never be clamped by this
    middleware, however large their declared body."""
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    huge = _DEFAULT_MAX_BODY_BYTES * 4
    for path in (
        "/assets/catalogs/cat1/collections/col1/assets:bulk",
        "/assets/catalogs/cat1/collections/col1/assets/asset1",
        "/assets/catalogs/cat1/collections/col1/upload",
        "/stac/catalogs/cat1/collections/col1/search",
    ):
        app.called = False
        scope = _scope(path=path, content_length=huge)
        receive = _chunk_receive([b"x" * 10])
        send = _Send()
        await mw(scope, receive, send)
        # Forwarded straight through and handled by the app — the
        # middleware never intercepted Content-Length for this path.
        assert app.called is True, path
        assert send.status == 200, path


# ---------------------------------------------------------------------------
# Content-Length fast path
# ---------------------------------------------------------------------------


async def test_content_length_under_cap_passes_through_untouched():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    scope = _scope(content_length=1024)
    receive = _chunk_receive([b"{\"type\": \"Feature\"}"])
    send = _Send()
    await mw(scope, receive, send)
    assert app.called is True
    assert app.received_bodies == [b"{\"type\": \"Feature\"}"]
    assert send.status == 200


async def test_content_length_over_cap_rejects_without_reading_body():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    scope = _scope(content_length=_DEFAULT_MAX_BODY_BYTES + 1)
    receive = _never_receive()
    send = _Send()
    await mw(scope, receive, send)
    assert app.called is False
    assert send.status == 413
    body = send.body
    assert b"ingest-body-too-large" in body
    assert b"\"max_body_mb\":64" in body


# ---------------------------------------------------------------------------
# Streaming / chunked path (no Content-Length)
# ---------------------------------------------------------------------------


async def test_chunked_under_cap_replays_all_chunks_to_app():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    scope = _scope(content_length=None)
    chunks = [b"a" * 10, b"b" * 10, b"c" * 5]
    receive = _chunk_receive(chunks)
    send = _Send()
    await mw(scope, receive, send)
    assert app.called is True
    assert app.received_bodies == chunks
    assert send.status == 200


async def test_chunked_over_cap_rejects_and_stops_forwarding():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    scope = _scope(content_length=None)
    # Small cap via fallback default is 64 MiB — use a config override so
    # the test doesn't need to allocate that much memory.
    small_cap_mb = 1
    receive = _chunk_receive([b"x" * (1024 * 1024), b"y" * (1024 * 1024)])
    send = _Send()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(discovery, "get_protocol", lambda proto: _FakePlatformConfigs(small_cap_mb))
        await mw(scope, receive, send)

    assert app.called is False
    assert send.status == 413
    # Buffered at most a little over the 1 MiB cap (two 1 MiB chunks read
    # before the cumulative total was detected as over budget), never the
    # whole (unbounded) request.
    assert receive.state["calls"] <= 2


async def test_chunked_disconnect_before_cap_replays_disconnect():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    scope = _scope(content_length=None)
    receive = _chunk_receive([])  # immediate http.disconnect
    send = _Send()
    await mw(scope, receive, send)
    assert app.called is True
    assert app.received_bodies == []


# ---------------------------------------------------------------------------
# Config resolution: fallback + hot-reload
# ---------------------------------------------------------------------------


async def test_configs_unavailable_falls_back_to_default_cap():
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)
    # autouse fixture already patches get_protocol -> None
    scope = _scope(content_length=_DEFAULT_MAX_BODY_BYTES + 1)
    receive = _never_receive()
    send = _Send()
    await mw(scope, receive, send)
    assert send.status == 413
    assert b"\"max_body_mb\":64" in send.body


async def test_hot_reload_of_configured_cap_is_honoured(monkeypatch):
    app = _RecordingApp()
    mw = SyncIngestBodyLimitMiddleware(app)

    monkeypatch.setattr(discovery, "get_protocol", lambda proto: _FakePlatformConfigs(1))
    scope = _scope(content_length=2 * 1024 * 1024)
    send = _Send()
    await mw(scope, _never_receive(), send)
    assert send.status == 413
    assert b"\"max_body_mb\":1" in send.body

    # Reconfigure to a larger cap — no second cache should shadow the change.
    app.called = False
    monkeypatch.setattr(discovery, "get_protocol", lambda proto: _FakePlatformConfigs(4))
    scope2 = _scope(content_length=2 * 1024 * 1024)
    receive2 = _chunk_receive([b"z" * 10])
    send2 = _Send()
    await mw(scope2, receive2, send2)
    assert app.called is True
    assert send2.status == 200
