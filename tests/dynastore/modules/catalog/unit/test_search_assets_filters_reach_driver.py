"""``AssetService.search_assets`` forwards filters to the routed driver.

History: the service used to flatten the filter list to a ``{field: value}``
equality dict and raise on any non-EQ operator, because the PG/ES asset drivers
only encoded equality. #1096 wired the full scalar operator set through to both
drivers (translation lives in ``dynastore.modules.tools.asset_filters``), so the
service now forwards the ``AssetFilter`` list verbatim — operator and all — and
no longer rejects non-EQ operators itself. Unsupported operators are rejected
deeper, in the shared builder.

These tests pin that the list reaches the driver unchanged (EQ and non-EQ
alike), so a future refactor cannot silently drop operator information on the
way down.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from dynastore.models.query_builder import AssetFilter, FilterOperator
from dynastore.modules.catalog.asset_service import AssetService


def _build_service() -> AssetService:
    """Construct the service with the engine left unset; these tests stub the
    driver resolver, so no live engine is needed."""
    svc = AssetService.__new__(AssetService)
    svc.engine = None  # type: ignore[attr-defined]
    return svc


def _patch_driver():
    """Return (context-manager, captured-dict) capturing the driver call."""
    captured: Dict[str, Any] = {}

    class _FakeDriver:
        async def search_assets(
            self,
            catalog_id: str,
            collection_id: Optional[str] = None,
            *,
            filters: Optional[List[AssetFilter]] = None,
            limit: int = 10,
            offset: int = 0,
            all_collections: bool = False,
            db_resource: Any = None,
        ) -> List[Dict[str, Any]]:
            captured["catalog_id"] = catalog_id
            captured["collection_id"] = collection_id
            captured["filters"] = filters
            captured["all_collections"] = all_collections
            return []

    async def _fake_get_driver(*_args, **_kwargs):
        return _FakeDriver()

    cm = patch(
        "dynastore.modules.storage.router.get_asset_search_driver",
        _fake_get_driver,
    )
    return cm, captured


def test_eq_filters_reach_driver_unchanged():
    """EQ filters are forwarded as an ``AssetFilter`` list, not flattened."""
    svc = _build_service()
    filters = [
        AssetFilter(field="provider", op=FilterOperator.EQ, value="ESA"),
        AssetFilter(field="metadata.sensor.name", op=FilterOperator.EQ, value="MSI"),
    ]
    cm, captured = _patch_driver()
    with cm:
        result = asyncio.run(svc.search_assets(catalog_id="cat-x", filters=filters))

    assert result == []
    assert captured["filters"] == filters


def test_non_eq_filters_reach_driver():
    """Non-EQ operators are no longer rejected by the service; they flow
    through to the driver, which encodes them per backend."""
    svc = _build_service()
    filters = [
        AssetFilter(field="size", op=FilterOperator.GTE, value=100),
        AssetFilter(field="name", op=FilterOperator.LIKE, value="%.tif"),
    ]
    cm, captured = _patch_driver()
    with cm:
        result = asyncio.run(svc.search_assets(catalog_id="cat-x", filters=filters))

    assert result == []
    forwarded = captured["filters"]
    assert forwarded is not None
    assert [(f.field, f.op, f.value) for f in forwarded] == [
        ("size", FilterOperator.GTE, 100),
        ("name", FilterOperator.LIKE, "%.tif"),
    ]


def test_empty_filters_forwarded_as_none():
    """An empty filter list collapses to ``None`` so the driver returns the
    unfiltered (scope-only) result set."""
    svc = _build_service()
    cm, captured = _patch_driver()
    with cm:
        asyncio.run(svc.search_assets(catalog_id="cat-x", filters=[]))
    assert captured["filters"] is None


def test_all_collections_threads_through():
    svc = _build_service()
    filters = [AssetFilter(field="provider", op=FilterOperator.EQ, value="ESA")]
    cm, captured = _patch_driver()
    with cm:
        asyncio.run(
            svc.search_assets(catalog_id="cat-x", filters=filters, all_collections=True)
        )
    assert captured["all_collections"] is True
