"""Regression test for ``AssetService.search_assets`` operator handling.

Background: the previous implementation routed EQ-only filter lists to
``driver.search_assets`` directly, and routed any list containing a
non-EQ filter through a "complex" branch that built an ``op_map`` of
``FilterOperator -> SQL operator``. The ``op_map`` was assigned but
never referenced — the branch then built ``pg_query[f.field] = f.value``
with no operator encoding and passed it to
``pg_asset_driver.search_assets``, whose WHERE-clause is hardcoded
``"<field>" = :val``. Net effect: ``POST /catalogs/{cat}/assets-search``
with ``filters=[{field: "size", op: "GTE", value: 100}]`` silently
returned the same rows as ``size == 100``.

Ruff F841 flagged the dead ``op_map`` for a long time. Fix:
``search_assets`` now raises ``ValueError`` listing the unsupported
operators, which the HTTP handler maps to 400.

This test pins the loud-raise behavior so a future "we should accept
non-EQ filters" change does not regress to silently-wrong results
without also wiring the operator through to the driver layer.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from dynastore.models.query_builder import FilterOperator
from dynastore.modules.catalog.asset_service import AssetFilter, AssetService


def _build_service() -> AssetService:
    """Construct the service with the engine left unset.

    The unsupported-operator check must fire before any driver-resolution
    or DB call, so we never need a live engine for these tests.
    """
    svc = AssetService.__new__(AssetService)
    svc.engine = None  # type: ignore[attr-defined]
    return svc


def test_non_eq_filter_raises_value_error_with_operator_list():
    """A single non-EQ filter must raise rather than silently match by
    equality. The error message lists the offending operators so the
    caller can correct the request."""
    svc = _build_service()
    filters = [AssetFilter(field="size", op=FilterOperator.GTE, value=100)]

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(svc.search_assets(catalog_id="cat-x", filters=filters))

    msg = str(exc_info.value)
    assert "EQ" in msg
    assert "gte" in msg.lower() or "GTE" in msg, (
        "error message must surface the unsupported operator(s) so the "
        f"caller knows what to fix; got {msg!r}"
    )


def test_mixed_eq_and_non_eq_filters_raise_listing_only_non_eq():
    """Lists mixing EQ and non-EQ must raise, and the message should
    only mention the non-EQ operators (EQ is supported)."""
    svc = _build_service()
    filters = [
        AssetFilter(field="provider", op=FilterOperator.EQ, value="ESA"),
        AssetFilter(field="size", op=FilterOperator.LT, value=1000),
        AssetFilter(field="name", op=FilterOperator.LIKE, value="%.tif"),
    ]

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(svc.search_assets(catalog_id="cat-x", filters=filters))

    msg = str(exc_info.value).lower()
    assert "eq" in msg  # mentioned as the only supported op
    # Both non-EQ operators must surface; EQ must NOT be listed as unsupported
    assert "lt" in msg
    assert "like" in msg


def test_eq_only_filters_reach_driver_unchanged():
    """The EQ-only path must still build the ``{field: value}`` dict the
    PG/ES drivers expect (i.e. the refactor that collapsed the two
    branches did not change the wire format)."""
    svc = _build_service()
    filters = [
        AssetFilter(field="provider", op=FilterOperator.EQ, value="ESA"),
        AssetFilter(field="metadata.sensor.name", op=FilterOperator.EQ, value="MSI"),
    ]

    captured: Dict[str, Any] = {}

    class _FakeDriver:
        async def search_assets(
            self,
            catalog_id: str,
            collection_id: Optional[str] = None,
            *,
            query: Optional[Dict[str, Any]] = None,
            limit: int = 10,
            offset: int = 0,
            all_collections: bool = False,
            db_resource: Any = None,
        ) -> List[Dict[str, Any]]:
            captured["catalog_id"] = catalog_id
            captured["collection_id"] = collection_id
            captured["query"] = query
            captured["all_collections"] = all_collections
            return []

    async def _fake_get_driver(*_args, **_kwargs):
        return _FakeDriver()

    with patch(
        "dynastore.modules.storage.router.get_asset_search_driver",
        _fake_get_driver,
    ):
        result = asyncio.run(svc.search_assets(catalog_id="cat-x", filters=filters))

    assert result == []
    assert captured["query"] == {
        "provider": "ESA",
        "metadata.sensor.name": "MSI",
    }, (
        "EQ-only path must pass the literal {field: value} dict the "
        "PG/ES drivers consume; metadata.* paths must remain dotted so "
        "pg_asset_driver can fold them into the JSONB containment "
        f"predicate. got {captured['query']!r}"
    )
