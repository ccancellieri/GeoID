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

"""Unit tests for the /ready readiness probe endpoint.

Tests the readiness_check handler in isolation by mocking the dependency
clients. DB-free — no PostgreSQL, Elasticsearch, or Valkey required.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


async def _call_readiness():
    """Import and call the readiness_check handler directly."""
    from dynastore.main import readiness_check
    return await readiness_check()


# ── PostgreSQL ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ready_503_when_pg_raises():
    """503 when DatabaseProtocol.async_engine.connect() raises."""
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = ConnectionRefusedError("PG unreachable")

    mock_db = MagicMock()
    mock_db.async_engine = mock_engine

    with (
        patch("dynastore.main.get_protocols", return_value=[mock_db]),
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=None),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
    ):
        resp = await _call_readiness()

    assert resp.status_code == 503
    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["postgres"]["status"] == "failed"


@pytest.mark.asyncio
async def test_ready_200_when_pg_ok():
    """200 when PG SELECT 1 succeeds and other deps are disabled."""
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn

    mock_db = MagicMock()
    mock_db.async_engine = mock_engine

    with (
        patch("dynastore.main.get_protocols", return_value=[mock_db]),
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=None),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
    ):
        resp = await _call_readiness()

    assert resp.status_code == 200
    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["postgres"]["status"] == "ok"
    assert body["dependencies"]["elasticsearch"]["status"] == "disabled"
    assert body["dependencies"]["valkey"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_ready_postgres_disabled_when_no_db_protocol():
    """postgres reported as disabled when DatabaseProtocol returns None."""
    with (
        patch("dynastore.main.get_protocols", return_value=[]),
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=None),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
    ):
        resp = await _call_readiness()

    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["postgres"]["status"] == "disabled"
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_ready_probes_the_provider_that_owns_the_async_engine():
    """The sync provider sorting first must not mask the async engine.

    Several modules implement DatabaseProtocol and `get_protocols` sorts them
    ascending by `priority`. On services that load DatastoreModule (priority 7)
    it precedes DBService (priority 10) and reports `async_engine is None`, so
    probing only the first provider reports postgres as "disabled" — and, since
    "disabled" leaves the status code at 200, a genuinely dead database would
    still be announced as ready.
    """
    sync_only = MagicMock()          # DatastoreModule: priority 7, sync engine only
    sync_only.async_engine = None

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn

    async_capable = MagicMock()      # DBService: priority 10, owns the async engine
    async_capable.async_engine = mock_engine

    with (
        patch("dynastore.main.get_protocols", return_value=[sync_only, async_capable]),
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=None),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
    ):
        resp = await _call_readiness()

    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["postgres"]["status"] == "ok"
    assert resp.status_code == 200
    mock_engine.connect.assert_called_once()


@pytest.mark.asyncio
async def test_ready_503_when_async_provider_is_down_behind_a_sync_provider():
    """A dead database must surface as 503 even when a sync-only provider leads."""
    sync_only = MagicMock()
    sync_only.async_engine = None

    dead_engine = MagicMock()
    dead_engine.connect.side_effect = ConnectionRefusedError("PG unreachable")

    async_capable = MagicMock()
    async_capable.async_engine = dead_engine

    with (
        patch("dynastore.main.get_protocols", return_value=[sync_only, async_capable]),
        patch("dynastore.modules.elasticsearch.client.get_client", return_value=None),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
    ):
        resp = await _call_readiness()

    assert resp.status_code == 503
    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["postgres"]["status"] == "failed"


# ── Elasticsearch ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ready_503_when_es_raises():
    """503 when ES ping raises."""
    mock_es = AsyncMock()
    mock_es.ping.side_effect = ConnectionError("ES down")

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn

    mock_db = MagicMock()
    mock_db.async_engine = mock_engine

    with (
        patch("dynastore.main.get_protocols", return_value=[mock_db]),
        patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=mock_es,
        ),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
    ):
        resp = await _call_readiness()

    assert resp.status_code == 503
    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["elasticsearch"]["status"] == "failed"


@pytest.mark.asyncio
async def test_ready_200_all_deps_ok():
    """200 when PG, ES, and Valkey all pass."""
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn

    mock_db = MagicMock()
    mock_db.async_engine = mock_engine

    mock_es = AsyncMock()
    mock_es.ping = AsyncMock(return_value=True)

    mock_valkey = AsyncMock()
    mock_valkey.ping = AsyncMock(return_value=True)

    mock_manager = MagicMock()
    mock_manager.get_async_backend.return_value = mock_valkey

    with (
        patch("dynastore.main.get_protocols", return_value=[mock_db]),
        patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=mock_es,
        ),
        patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", True),
        patch("dynastore.tools.cache.get_cache_manager", return_value=mock_manager),
        patch(
            "dynastore.tools.cache_valkey.ValkeyCacheBackend",
            type(mock_valkey),
        ),
    ):
        resp = await _call_readiness()

    assert resp.status_code == 200
    import json
    body = json.loads(resp.body)
    assert body["status"] == "ready"
    assert body["dependencies"]["postgres"]["status"] == "ok"
    assert body["dependencies"]["elasticsearch"]["status"] == "ok"


# ── Draining flag (Lever B self-recycle) ──────────────────────────────────────

@pytest.mark.asyncio
async def test_ready_503_when_draining():
    """/ready reports 503 when this worker has flagged itself as draining,
    regardless of every other dependency being healthy — unconditional, not
    gated by any config flag (unlike the readiness-shed middleware)."""
    from dynastore.tools.serving_state import clear_draining, set_draining

    set_draining()
    try:
        with (
            patch("dynastore.main.get_protocols", return_value=[]),
            patch("dynastore.modules.elasticsearch.client.get_client", return_value=None),
            patch("dynastore.tools.cache_valkey._CACHE_DEPS_OK", False),
        ):
            resp = await _call_readiness()
    finally:
        clear_draining()

    assert resp.status_code == 503
    import json
    body = json.loads(resp.body)
    assert body["dependencies"]["draining"]["status"] == "failed"
