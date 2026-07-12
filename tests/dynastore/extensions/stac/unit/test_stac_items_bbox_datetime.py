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

"""``get_stac_collection_items`` ``bbox=``/``datetime=`` handling (#3295).

Before this fix ``bbox``/``datetime`` were listed in ``OGC_RESERVED_QUERY_PARAMS``
(reserved so they don't fall into the ``?{property}={value}`` shorthand) but
nothing ever consumed them — the endpoint returned the full, unfiltered
collection regardless of the values supplied.

These tests pin, at the handler boundary:

* ``bbox=``/``datetime=`` reach the SEARCH-driver fast-path dispatch
  (``maybe_dispatch_items_to_search_driver``) as the structural ``bbox``/
  ``datetime`` kwargs it already understands.
* A malformed ``bbox=``/``datetime=`` value 400s before any driver dispatch.
* No ``bbox=``/``datetime=`` supplied → the dispatch call is unchanged
  (``bbox=None, datetime=None``), matching pre-fix behaviour.
* A CQL ``filter=`` present alongside ``bbox=``/``datetime=`` skips the
  fast-path dispatch (existing #1285/#1311 class) and instead threads a
  ``geom``/``validity`` ``FilterCondition`` list into
  ``create_item_collection`` for the PG fallback.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from starlette.datastructures import URL

import dynastore.extensions.stac.stac_service as stac_service_mod
import dynastore.extensions.tools.query as query_mod
from dynastore.extensions.stac.stac_service import STACService


def _request(query_params: dict, path: str = "/stac/catalogs/cat/collections/col/items"):
    qs = "&".join(f"{k}={v}" for k, v in query_params.items())
    return SimpleNamespace(
        state=SimpleNamespace(
            principal="P", principal_id="user:alice", principal_role=["reader"],
        ),
        query_params=query_params,
        url=URL(f"http://t{path}" + (f"?{qs}" if qs else "")),
    )


@asynccontextmanager
async def _fake_txn(_engine):
    yield None


def _acoro(value: Any):
    async def _coro(*args: Any, **kwargs: Any):
        return value

    return _coro


def _make_service(monkeypatch) -> STACService:
    svc = STACService.__new__(STACService)
    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)
    return svc


# ---------------------------------------------------------------------------
# bbox=/datetime= reach the SEARCH-driver dispatch fast path
# ---------------------------------------------------------------------------


async def test_bbox_reaches_search_driver_dispatch(monkeypatch):
    svc = _make_service(monkeypatch)
    seen: dict = {}

    async def _spy_dispatch(**kwargs: Any):
        seen["dispatch"] = kwargs
        return "QR"

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _spy_dispatch)

    seen_collection: dict = {}

    async def _spy_collection(*args: Any, **kwargs: Any) -> dict:
        seen_collection["kwargs"] = kwargs
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(stac_service_mod.stac_generator, "create_item_collection", _spy_collection)

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request({"bbox": "1,2,3,4"}),
        engine=object(),
        limit=10,
        offset=0,
        bbox="1,2,3,4",
        datetime_param=None,
        filter=None,
        language="en",
        request_hints=frozenset(),
    )

    assert seen["dispatch"]["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert seen["dispatch"]["datetime"] is None
    # The dispatch succeeded (non-None) → its result is used directly, and
    # numberMatched/features both come from the same driver call.
    assert seen_collection["kwargs"]["search_dispatch"] == "QR"


async def test_datetime_reaches_search_driver_dispatch(monkeypatch):
    svc = _make_service(monkeypatch)
    seen: dict = {}

    async def _spy_dispatch(**kwargs: Any):
        seen["dispatch"] = kwargs
        return "QR"

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _spy_dispatch)
    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _acoro({"type": "FeatureCollection", "features": []})
    )

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request({"datetime": "2020-01-01T00:00:00Z/2020-12-31T00:00:00Z"}),
        engine=object(),
        limit=10,
        offset=0,
        bbox=None,
        datetime_param="2020-01-01T00:00:00Z/2020-12-31T00:00:00Z",
        filter=None,
        language="en",
        request_hints=frozenset(),
    )

    assert seen["dispatch"]["bbox"] is None
    assert seen["dispatch"]["datetime"] == "2020-01-01T00:00:00Z/2020-12-31T00:00:00Z"


async def test_no_bbox_no_datetime_dispatch_unchanged(monkeypatch):
    """No bbox/datetime supplied → dispatch receives bbox=None, datetime=None,
    matching pre-fix behaviour byte-for-byte."""
    svc = _make_service(monkeypatch)
    seen: dict = {}

    async def _spy_dispatch(**kwargs: Any):
        seen["dispatch"] = kwargs
        return "QR"

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _spy_dispatch)
    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _acoro({"type": "FeatureCollection", "features": []})
    )

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request({}),
        engine=object(),
        limit=10,
        offset=0,
        bbox=None,
        datetime_param=None,
        filter=None,
        language="en",
        request_hints=frozenset(),
    )

    assert seen["dispatch"]["bbox"] is None
    assert seen["dispatch"]["datetime"] is None


# ---------------------------------------------------------------------------
# Malformed bbox/datetime → 400 before any driver dispatch
# ---------------------------------------------------------------------------


async def test_invalid_bbox_returns_400_before_dispatch(monkeypatch):
    svc = _make_service(monkeypatch)

    async def _no_dispatch(**kwargs: Any):
        raise AssertionError("dispatch must not be called for a malformed bbox")

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _no_dispatch)

    with pytest.raises(HTTPException) as excinfo:
        await svc.get_stac_collection_items(
            catalog_id="cat",
            collection_id="col_a",
            request=_request({"bbox": "1,2,3"}),
            engine=object(),
            limit=10,
            offset=0,
            bbox="1,2,3",
            datetime_param=None,
            filter=None,
            language="en",
            request_hints=frozenset(),
        )
    assert excinfo.value.status_code == 400


async def test_invalid_datetime_returns_400_before_dispatch(monkeypatch):
    svc = _make_service(monkeypatch)

    async def _no_dispatch(**kwargs: Any):
        raise AssertionError("dispatch must not be called for a malformed datetime")

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _no_dispatch)

    with pytest.raises(HTTPException) as excinfo:
        await svc.get_stac_collection_items(
            catalog_id="cat",
            collection_id="col_a",
            request=_request({"datetime": "not-a-date"}),
            engine=object(),
            limit=10,
            offset=0,
            bbox=None,
            datetime_param="not-a-date",
            filter=None,
            language="en",
            request_hints=frozenset(),
        )
    assert excinfo.value.status_code == 400


# ---------------------------------------------------------------------------
# bbox/datetime + an explicit CQL filter: fast path skipped (existing
# #1285/#1311 class), PG fallback receives structural_filters.
# ---------------------------------------------------------------------------


async def test_bbox_with_cql_filter_skips_dispatch_and_threads_structural_filters(monkeypatch):
    svc = _make_service(monkeypatch)

    async def _boom_dispatch(**kwargs: Any):
        raise AssertionError("dispatch must not be called when a CQL filter is present")

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _boom_dispatch)

    seen: dict = {}

    async def _spy_collection(*args: Any, **kwargs: Any) -> dict:
        seen["kwargs"] = kwargs
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(stac_service_mod.stac_generator, "create_item_collection", _spy_collection)

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request({"bbox": "1,2,3,4", "filter": "title = 'x'"}),
        engine=object(),
        limit=10,
        offset=0,
        bbox="1,2,3,4",
        datetime_param=None,
        filter="title = 'x'",
        language="en",
        request_hints=frozenset(),
    )

    assert seen["kwargs"]["search_dispatch"] is None
    structural_filters = seen["kwargs"]["structural_filters"]
    assert len(structural_filters) == 1
    assert structural_filters[0].field == "geom"
    assert structural_filters[0].spatial_op is True
    assert seen["kwargs"]["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert seen["kwargs"]["datetime_param"] is None
