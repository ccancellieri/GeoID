"""Unit tests for ``is_query_fallback_driver`` — the shared item-listing gate.

The two item-listing dispatch paths (``_try_driver_dispatch`` and
``maybe_dispatch_items_to_search_driver``, both over the driver's streaming
``read_entities``) share exactly one thin step: deciding whether a resolved
items driver is the read-primary PG fallback, in which case both decline and
let the PostgreSQL ``stream_items`` path serve the listing.

These tests pin that shared semantics: ``None`` driver → fallback; a driver
advertising ``QUERY_FALLBACK_SOURCE`` → fallback; a non-PG search/read driver
→ not fallback; a driver missing ``capabilities`` → degrades to not-fallback
rather than raising.
"""
from __future__ import annotations

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.catalog.item_query import is_query_fallback_driver


def test_none_driver_is_fallback() -> None:
    assert is_query_fallback_driver(None) is True


def test_pg_query_fallback_source_driver_is_fallback() -> None:
    class _Pg:
        capabilities = frozenset({Capability.READ, Capability.QUERY_FALLBACK_SOURCE})

    assert is_query_fallback_driver(_Pg()) is True


def test_search_capable_driver_is_not_fallback() -> None:
    class _Search:
        capabilities = frozenset({Capability.READ})

    assert is_query_fallback_driver(_Search()) is False


def test_driver_without_capabilities_degrades_to_not_fallback() -> None:
    class _Bare:
        pass

    # A real driver always declares ``capabilities``; an absent attribute must
    # degrade to "not a fallback" (defer to the resolved driver) rather than
    # raising ``AttributeError``.
    assert is_query_fallback_driver(_Bare()) is False
