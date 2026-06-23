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

"""Unit tests for the log writer's physical-id resolution path.

The log writer no longer owns a cache.  It resolves catalog->schema and
collection->physical-id through ``resolve_physical_schema`` /
``resolve_physical_id`` *without* a ``db_resource``, so the shared
``_physical_schema_cache`` / ``_collection_physical_id_cache`` accelerators
serve a batch of N same-catalog entries with one DB round-trip instead of N.

DB-free: patches CatalogsProtocol and the DB primitives so the flush path runs
without Postgres.  The key assertion is that the resolvers are called on the
cache-friendly path (no connection handed in, which would defeat the cache).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from typing import Optional

import pytest

from dynastore.modules.catalog.log_manager import LogService, LogEntryCreate


def _make_fake_catalogs(schema: Optional[str] = "s_cat1"):
    catalogs = MagicMock()
    catalogs.resolve_physical_schema = AsyncMock(return_value=schema)
    catalogs.resolve_physical_id = AsyncMock(return_value="c_phys")
    return catalogs


def _make_entries(catalog_id: str, n: int, *, collection_id: Optional[str] = None) -> list:
    return [
        LogEntryCreate(
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type="test_event",
            level="INFO",
            message=f"msg {i}",
        )
        for i in range(n)
    ]


class _FakeConn:
    pass


class _FakeTxCtx:
    async def __aenter__(self):
        return _FakeConn()

    async def __aexit__(self, *exc):
        return False


def _fake_managed_transaction(_engine):
    return _FakeTxCtx()


def _install_db_free(monkeypatch, fake_catalogs):
    """Patch discovery + DB primitives so the flush path runs without Postgres."""
    import dynastore.modules.catalog.log_manager as lm
    from dynastore.modules.db_config.query_executor import ResultHandler

    monkeypatch.setattr(lm, "get_protocol", lambda _proto: fake_catalogs)
    monkeypatch.setattr(lm, "managed_transaction", _fake_managed_transaction)
    import dynastore.modules.db_config.query_executor as qe
    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)

    class _FakeDQLQuery:
        def __init__(self, sql, *, result_handler=None):
            self.result_handler = result_handler

        async def execute(self, conn, **kwargs):
            if self.result_handler in (ResultHandler.SCALAR_ONE, ResultHandler.SCALAR):
                return 42
            return None

    monkeypatch.setattr(lm, "DQLQuery", _FakeDQLQuery)


@pytest.mark.asyncio
async def test_schema_resolution_uses_cache_friendly_path(monkeypatch):
    """_write_log_entry resolves the schema without handing in a connection.

    Passing a ``db_resource``/``ctx`` would bypass ``_physical_schema_cache``;
    the writer must not, so the shared cache can serve repeated lookups.
    """
    fake_catalogs = _make_fake_catalogs("s_cat1")
    _install_db_free(monkeypatch, fake_catalogs)

    service = LogService.__new__(LogService)
    service._engine = object()
    service._aggregator = None

    await service._flush_batch(_make_entries("cat1", 3))

    assert fake_catalogs.resolve_physical_schema.await_count == 3
    for c in fake_catalogs.resolve_physical_schema.call_args_list:
        # First positional arg is the catalog id; no ctx/db_resource handed in.
        assert c.args[0] == "cat1"
        assert "ctx" not in c.kwargs


@pytest.mark.asyncio
async def test_collection_resolution_uses_cache_friendly_path(monkeypatch):
    """Collection physical-id resolution also avoids the connection bypass."""
    fake_catalogs = _make_fake_catalogs("s_cat1")
    _install_db_free(monkeypatch, fake_catalogs)

    service = LogService.__new__(LogService)
    service._engine = object()
    service._aggregator = None

    await service._flush_batch(_make_entries("cat1", 2, collection_id="col1"))

    assert fake_catalogs.resolve_physical_id.await_count == 2
    for c in fake_catalogs.resolve_physical_id.call_args_list:
        assert c.args[0] == "cat1"
        assert c.args[1] == "col1"
        assert "ctx" not in c.kwargs
