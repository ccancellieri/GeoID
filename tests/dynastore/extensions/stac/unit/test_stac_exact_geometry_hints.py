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

"""STAC ``?hints=geometry_exact`` opt-in threads EXACT_READ_HINTS to stream_items.

The ``?hints=geometry_exact`` query parameter routes item reads to the
exact-geometry (PostgreSQL) tier instead of the simplified (Elasticsearch)
tier by passing ``hints=EXACT_READ_HINTS`` down to ``stream_items``.  These
tests pin the wiring at the handler boundary (``get_stac_collection_items``),
the generator boundary (``create_item_collection``), and the DB layer
(``get_stac_items_paginated``) so the hint is never silently dropped.

Paths covered:

* ``get_stac_collection_items`` with ``request_hints=EXACT_READ_HINTS`` →
  ``create_item_collection`` receives ``hints=EXACT_READ_HINTS``.
* ``get_stac_collection_items`` with ``request_hints=frozenset()`` (default) →
  ``create_item_collection`` receives ``hints=frozenset()`` (no hint injected,
  ES fast-path unchanged).
* ``create_item_collection(hints=EXACT_READ_HINTS, search_dispatch=None)`` →
  ``stac_db.get_stac_items_paginated`` receives
  ``hints=EXACT_READ_HINTS``.
* ``create_item_collection(hints=frozenset(), search_dispatch=None)`` →
  ``stac_db.get_stac_items_paginated`` receives ``hints=frozenset()``.
* ``get_stac_items_paginated(hints=EXACT_READ_HINTS)`` →
  ``items_svc.stream_items`` is called with ``hints=EXACT_READ_HINTS``.
* ``get_stac_items_paginated(hints=frozenset())`` →
  ``items_svc.stream_items`` is called with ``hints=frozenset()``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, FrozenSet
from unittest.mock import AsyncMock

import dynastore.extensions.stac.stac_db as stac_db_mod
import dynastore.extensions.stac.stac_generator as stac_generator_mod
import dynastore.extensions.stac.stac_service as stac_service_mod
import dynastore.extensions.tools.query as query_mod
from dynastore.extensions.stac.stac_service import STACService
from dynastore.modules.storage.hints import EXACT_READ_HINTS, Hint


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _request(path: str = "/stac/catalogs/cat/collections/col/items"):
    from starlette.datastructures import URL

    return SimpleNamespace(
        state=SimpleNamespace(
            principal="P",
            principal_id="user:alice",
            principal_role=["reader"],
        ),
        query_params={},
        url=URL(f"http://t{path}"),
    )


@asynccontextmanager
async def _fake_txn(_engine):
    yield None


def _acoro(value: Any):
    async def _coro(*args: Any, **kwargs: Any):
        return value

    return _coro


# ---------------------------------------------------------------------------
# Handler level: get_stac_collection_items forwards hints to create_item_collection
# ---------------------------------------------------------------------------


async def test_handler_passes_exact_hints_when_geometry_exact(monkeypatch):
    """``request_hints=EXACT_READ_HINTS`` → ``create_item_collection`` receives EXACT_READ_HINTS."""
    svc = STACService.__new__(STACService)

    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    # With GEOMETRY_EXACT hint, ES dispatch is skipped (wants_exact=True)
    async def _no_dispatch(**kwargs: Any):
        raise AssertionError("dispatch must not be called when GEOMETRY_EXACT hint is set")

    monkeypatch.setattr(
        query_mod, "maybe_dispatch_items_to_search_driver", _no_dispatch
    )

    seen: dict = {}

    async def _spy_collection(*args: Any, **kwargs: Any) -> dict:
        seen["hints"] = kwargs.get("hints")
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _spy_collection
    )

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request(),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        request_hints=EXACT_READ_HINTS,
    )

    assert seen["hints"] == EXACT_READ_HINTS, (
        f"Expected EXACT_READ_HINTS={EXACT_READ_HINTS!r}, got {seen['hints']!r}"
    )
    assert Hint.GEOMETRY_EXACT in seen["hints"]


async def test_handler_passes_empty_hints_by_default(monkeypatch):
    """No hints → ``create_item_collection`` receives empty frozenset."""
    svc = STACService.__new__(STACService)

    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    # Default path: dispatch may be called and returns a sentinel
    async def _dispatch(**kwargs: Any) -> str:
        return "QR"

    monkeypatch.setattr(
        query_mod, "maybe_dispatch_items_to_search_driver", _dispatch
    )

    seen: dict = {}

    async def _spy_collection(*args: Any, **kwargs: Any) -> dict:
        seen["hints"] = kwargs.get("hints")
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _spy_collection
    )

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request(),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        # request_hints not passed → default frozenset()
        request_hints=frozenset(),
    )

    assert seen["hints"] == frozenset(), (
        f"Default hints must be empty frozenset, got {seen['hints']!r}"
    )


# ---------------------------------------------------------------------------
# Generator level: create_item_collection forwards hints to get_stac_items_paginated
# ---------------------------------------------------------------------------


async def test_create_item_collection_forwards_exact_hints_to_db(monkeypatch):
    """With ``search_dispatch=None`` + ``hints=EXACT_READ_HINTS``, the hints
    reach ``get_stac_items_paginated``."""
    from starlette.datastructures import URL

    req = SimpleNamespace(
        query_params={},
        url=URL("http://t/stac/catalogs/cat/collections/col/items"),
    )

    seen: dict = {}

    async def _spy_paginated(
        conn: Any,
        catalog_id: str,
        collection_id: str,
        limit: int,
        offset: int,
        stac_config: Any = None,
        cql_filter: Any = None,
        request: Any = None,
        hints: FrozenSet = frozenset(),
    ):
        seen["hints"] = hints
        return [], 0

    monkeypatch.setattr(
        stac_generator_mod.stac_db,
        "get_stac_items_paginated",
        _spy_paginated,
    )

    stac_config = SimpleNamespace(simplification=None)
    await stac_generator_mod.create_item_collection(
        req,
        conn=None,
        schema="cat",
        table="col",
        limit=5,
        offset=0,
        stac_config=stac_config,
        search_dispatch=None,
        hints=EXACT_READ_HINTS,
    )

    assert seen["hints"] == EXACT_READ_HINTS, (
        f"Expected EXACT_READ_HINTS, got {seen['hints']!r}"
    )


async def test_create_item_collection_forwards_empty_hints_to_db(monkeypatch):
    """Default ``hints=frozenset()`` reaches ``get_stac_items_paginated`` unchanged."""
    from starlette.datastructures import URL

    req = SimpleNamespace(
        query_params={},
        url=URL("http://t/stac/catalogs/cat/collections/col/items"),
    )

    seen: dict = {}

    async def _spy_paginated(
        conn: Any,
        catalog_id: str,
        collection_id: str,
        limit: int,
        offset: int,
        stac_config: Any = None,
        cql_filter: Any = None,
        request: Any = None,
        hints: FrozenSet = frozenset(),
    ):
        seen["hints"] = hints
        return [], 0

    monkeypatch.setattr(
        stac_generator_mod.stac_db,
        "get_stac_items_paginated",
        _spy_paginated,
    )

    stac_config = SimpleNamespace(simplification=None)
    await stac_generator_mod.create_item_collection(
        req,
        conn=None,
        schema="cat",
        table="col",
        limit=5,
        offset=0,
        stac_config=stac_config,
        search_dispatch=None,
        hints=frozenset(),
    )

    assert seen["hints"] == frozenset(), (
        f"Default hints must be empty frozenset, got {seen['hints']!r}"
    )


# ---------------------------------------------------------------------------
# DB level: get_stac_items_paginated forwards hints to stream_items
# ---------------------------------------------------------------------------


def _make_query_response(catalog_id: str = "cat", collection_id: str = "col"):
    """Return an empty ``QueryResponse`` suitable for the stac_db async-for loop."""
    from dynastore.models.query_builder import QueryResponse

    async def _empty_gen():
        return
        yield  # pragma: no cover — makes it an async generator

    return QueryResponse(
        items=_empty_gen(),
        total_count=0,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )


async def test_get_stac_items_paginated_passes_exact_hints_to_stream_items(monkeypatch):
    """``hints=EXACT_READ_HINTS`` reaches ``items_svc.stream_items``."""
    import sys
    import unittest.mock as _mock

    seen: dict = {}

    class _FakeItemsSvc:
        async def stream_items(self, **kwargs: Any):
            seen["hints"] = kwargs.get("hints")
            return _make_query_response()

    async def _no_pg_envelope(catalog_id: str, collection_id: str) -> bool:
        return False

    fake_access_scope = SimpleNamespace(
        collection_uses_pg_access_envelope=_no_pg_envelope,
        compile_read_access_filter=AsyncMock(return_value=None),
        principals_from_request_state=lambda r: ([], None),
    )

    from starlette.datastructures import URL

    req = SimpleNamespace(
        state=SimpleNamespace(
            principal="P",
            principal_id="user:alice",
            principal_role=["reader"],
        ),
        url=URL("http://t/items"),
        query_params={},
    )

    with _mock.patch.dict(
        sys.modules,
        {"dynastore.modules.storage.access_scope": fake_access_scope},
    ):
        monkeypatch.setattr(stac_db_mod, "get_protocol", lambda _p: _FakeItemsSvc())
        await stac_db_mod.get_stac_items_paginated(
            conn=None,
            catalog_id="cat",
            collection_id="col",
            limit=10,
            offset=0,
            request=req,
            hints=EXACT_READ_HINTS,
        )

    assert seen.get("hints") == EXACT_READ_HINTS, (
        f"stream_items must receive EXACT_READ_HINTS, got {seen.get('hints')!r}"
    )


async def test_get_stac_items_paginated_passes_empty_hints_to_stream_items(monkeypatch):
    """Default ``hints=frozenset()`` reaches ``items_svc.stream_items`` unchanged."""
    import sys
    import unittest.mock as _mock

    seen: dict = {}

    class _FakeItemsSvc:
        async def stream_items(self, **kwargs: Any):
            seen["hints"] = kwargs.get("hints")
            return _make_query_response()

    async def _no_pg_envelope(catalog_id: str, collection_id: str) -> bool:
        return False

    fake_access_scope = SimpleNamespace(
        collection_uses_pg_access_envelope=_no_pg_envelope,
        compile_read_access_filter=AsyncMock(return_value=None),
        principals_from_request_state=lambda r: ([], None),
    )

    from starlette.datastructures import URL

    req = SimpleNamespace(
        state=SimpleNamespace(
            principal="P",
            principal_id="user:alice",
            principal_role=["reader"],
        ),
        url=URL("http://t/items"),
        query_params={},
    )

    with _mock.patch.dict(
        sys.modules,
        {"dynastore.modules.storage.access_scope": fake_access_scope},
    ):
        monkeypatch.setattr(stac_db_mod, "get_protocol", lambda _p: _FakeItemsSvc())
        await stac_db_mod.get_stac_items_paginated(
            conn=None,
            catalog_id="cat",
            collection_id="col",
            limit=10,
            offset=0,
            request=req,
            hints=frozenset(),
        )

    assert seen.get("hints") == frozenset(), (
        f"stream_items must receive empty frozenset, got {seen.get('hints')!r}"
    )
