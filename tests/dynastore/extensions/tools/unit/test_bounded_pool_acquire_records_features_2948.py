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

"""Extends the bounded fail-fast pool-acquire guard from #2933/#2947 to the
OGC Records and Features item-GET/search read surfaces (#2948).

``get_async_connection_bounded`` (``extensions/tools/db.py``) mirrors
``get_async_connection`` but threads ``acquire_timeout`` through
``managed_transaction`` so a saturated pool raises ``PoolSaturationError``
(mapped to a 503 + Retry-After by the existing
``PoolSaturationExceptionHandler``) instead of queuing for the engine's full
``pool_timeout`` — same guard STAC's item GET-by-id / item search already
got in #2947. Write routes (add/replace/update/delete) are left on the
plain, unbounded ``get_async_connection``, matching the read/write split
#2947 established for STAC.
"""
from __future__ import annotations

import inspect
import pathlib

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from dynastore.extensions.tools.db import get_async_connection_bounded
from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
from dynastore.modules.db_config.exceptions import PoolSaturationError


class _FakeTxnCm:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class _FakeConn(AsyncConnection):
    """``AsyncConnection`` subclass (instantiated via ``__new__``) so
    ``get_async_connection_bounded``'s ``isinstance`` check succeeds without
    a real DBAPI connection — mirrors ``_FakeEngine`` below."""

    def begin(self):
        return _FakeTxnCm()

    async def close(self) -> None:
        pass


class _FakeEngine(AsyncEngine):
    """See ``test_bounded_pool_acquire_2933.py`` — skips the real
    ``AsyncEngine.__init__`` since the acquire itself is mocked below."""


class _FakeManagedTransactionCm:
    def __init__(self, engine, *, acquire_timeout=None):
        self.engine = engine
        self.acquire_timeout = acquire_timeout

    async def __aenter__(self):
        return _FakeConn.__new__(_FakeConn)

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_get_async_connection_bounded_dispatches_through_managed_transaction(
    monkeypatch,
):
    """``get_async_connection_bounded`` passes a live ``acquire_timeout``
    into ``managed_transaction``, same as the STAC call sites (#2947)."""
    from dynastore.extensions.tools import db as db_module

    class _FakeRequest:
        class app:
            class state:
                engine = _FakeEngine.__new__(_FakeEngine)

    captured = {}

    def _fake_managed_transaction(engine, *, acquire_timeout=None):
        captured["engine"] = engine
        captured["acquire_timeout"] = acquire_timeout
        return _FakeManagedTransactionCm(engine, acquire_timeout=acquire_timeout)

    async def _fake_read_live_timeout():
        return 3.5

    monkeypatch.setattr(db_module, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(
        db_module, "_read_live_fg_acquire_timeout", _fake_read_live_timeout
    )

    gen = db_module.get_async_connection_bounded(_FakeRequest())  # type: ignore[arg-type]
    conn = await gen.__anext__()
    assert isinstance(conn, _FakeConn)
    assert captured["acquire_timeout"] == 3.5


def _read_source(*rel_parts: str) -> str:
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    return repo_root.joinpath(*rel_parts).read_text(encoding="utf-8")


def test_records_item_get_and_search_use_bounded_fail_fast_acquire():
    """Pin: Records ``get_record``/``get_records`` (item GET-by-id and item
    search/listing) depend on ``get_async_connection_bounded``, while the
    write routes keep the plain ``get_async_connection`` (#2948)."""
    source = _read_source(
        "packages", "extensions", "records", "src", "dynastore",
        "extensions", "records", "records_service.py",
    )
    assert source.count("Depends(get_async_connection_bounded)") == 2
    # Writes are unaffected.
    assert "add_records" in source
    assert source.count("Depends(get_async_connection)") >= 3


def test_features_item_get_and_search_use_bounded_fail_fast_acquire():
    """Pin: Features ``get_item``/``get_items`` (item GET-by-id and item
    search/listing) depend on ``get_async_connection_bounded``, while the
    write routes keep the plain ``get_async_connection`` (#2948)."""
    source = _read_source(
        "packages", "extensions", "features", "src", "dynastore",
        "extensions", "features", "features_service.py",
    )
    assert source.count("Depends(get_async_connection_bounded)") == 2
    assert source.count("Depends(get_async_connection)") >= 3


def test_get_item_signature_has_bounded_dependency():
    from dynastore.extensions.features.features_service import OGCFeaturesService

    sig = inspect.signature(OGCFeaturesService.get_item)
    assert sig.parameters["conn"].default.dependency is get_async_connection_bounded


def test_get_record_signature_has_bounded_dependency():
    from dynastore.extensions.records.records_service import RecordsService

    sig = inspect.signature(RecordsService.get_record)
    assert sig.parameters["conn"].default.dependency is get_async_connection_bounded


def test_features_get_item_returns_503_with_retry_after_on_pool_saturation():
    """End-to-end: a saturated pool on the Features item-GET path fails fast
    with 503 + Retry-After instead of a bare timeout/500 (#2948)."""
    from dynastore.extensions.features.features_service import OGCFeaturesService

    app = FastAPI()
    setup_exception_handlers(app)
    svc = OGCFeaturesService(app=app)
    app.include_router(svc.router)

    async def _saturated_pool():
        raise PoolSaturationError(
            "Database connection pool saturated after waiting 2.0s "
            "for a free connection (fail-fast bound).",
            retry_after=7,
        )
        yield  # pragma: no cover - unreachable, keeps this an async generator

    app.dependency_overrides[get_async_connection_bounded] = _saturated_pool

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/features/catalogs/cat1/collections/col1/items/item1")

    assert r.status_code == 503, r.text
    assert r.headers.get("Retry-After") == "7"
