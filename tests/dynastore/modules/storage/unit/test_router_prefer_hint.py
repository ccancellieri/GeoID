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

"""Tests for the parametric ``prefer:<driver>`` request hint.

Covers:
- ``parse_request_hints`` retains prefer tokens verbatim; drops unknown non-prefer tokens.
- ``_resolve_driver_preferences`` resolves exact driver_ref and alias → substring.
- READ with ``prefer:es`` → ES first, PG as fallback tail.
- READ with ``prefer:pg`` → PG first, ES not ahead.
- SEARCH with ``prefer:es`` → matched-only (ES only, no tail).
- WRITE ignores prefer tokens (fan-out unchanged).
- Unknown prefer alias with no matching entry → no override, falls through to normal path.
- Collection and catalog routing config defaults: no hints → PG only;
  ``prefer:es`` → ES first.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.driver_registry import DriverRegistry
from dynastore.modules.storage.hints import (
    DRIVER_PREFER_ALIASES,
    PREFER_PREFIX,
    Hint,
    parse_request_hints,
)
from dynastore.modules.storage.router import (
    _resolve_driver_ids_cached,
    _resolve_driver_preferences,
)
from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    ItemsRoutingConfig,
)


# ---------------------------------------------------------------------------
# Helpers (mirrors test_router_hint_set.py)
# ---------------------------------------------------------------------------


def _make_routing(operations: dict) -> ItemsRoutingConfig:
    ops = {}
    for op, entries in operations.items():
        ops[op] = [
            OperationDriverEntry(
                driver_ref=e[0],
                hints=e[1] if len(e) > 1 else set(),
                on_failure=e[2] if len(e) > 2 else FailurePolicy.FATAL,
            )
            for e in entries
        ]
    return ItemsRoutingConfig(operations=ops)


def _make_collection_routing(operations: dict) -> CollectionRoutingConfig:
    ops = {}
    for op, entries in operations.items():
        ops[op] = [
            OperationDriverEntry(
                driver_ref=e[0],
                hints=e[1] if len(e) > 1 else set(),
                on_failure=e[2] if len(e) > 2 else FailurePolicy.FATAL,
            )
            for e in entries
        ]
    return CollectionRoutingConfig(operations=ops)


def _mock_configs_protocol(routing_config):
    mock = MagicMock()
    mock.get_config = AsyncMock(return_value=routing_config)
    return mock


def _mock_driver(driver_ref: str, supported_hints: frozenset = frozenset()):
    cls = type(driver_ref, (MagicMock,), {"supported_hints": supported_hints})
    return cls()


# ---------------------------------------------------------------------------
# parse_request_hints: prefer tokens retained, unknown non-prefer dropped
# ---------------------------------------------------------------------------


def test_parse_request_hints_prefer_token_retained():
    """prefer:es is retained verbatim as a plain string in the returned frozenset."""
    result = parse_request_hints(["prefer:es"])
    assert "prefer:es" in result


def test_parse_request_hints_prefer_token_exact_case_insensitive():
    """prefer tokens are lowercased before retention."""
    result = parse_request_hints(["PREFER:ES"])
    assert "prefer:es" in result


def test_parse_request_hints_prefer_and_canonical_combined():
    """prefer:pg can coexist with a canonical Hint token."""
    result = parse_request_hints(["prefer:pg,geometry_exact"])
    assert "prefer:pg" in result
    assert Hint.GEOMETRY_EXACT in result


def test_parse_request_hints_unknown_non_prefer_dropped():
    """An unrecognised non-prefer token is silently dropped."""
    result = parse_request_hints(["bogus"])
    assert len(result) == 0


def test_parse_request_hints_prefer_prefix_only_dropped():
    """A bare 'prefer:' with no driver name after the colon is dropped."""
    result = parse_request_hints(["prefer:"])
    assert len(result) == 0


def test_parse_request_hints_empty_returns_empty():
    result = parse_request_hints([])
    assert result == frozenset()


def test_parse_request_hints_none_returns_empty():
    result = parse_request_hints(None)
    assert result == frozenset()


# ---------------------------------------------------------------------------
# _resolve_driver_preferences: exact and alias resolution
# ---------------------------------------------------------------------------


def _make_entries(*refs):
    return [OperationDriverEntry(driver_ref=r) for r in refs]


def test_resolve_driver_preferences_exact_match():
    """Exact driver_ref match resolves correctly."""
    entries = _make_entries("collection_postgresql_driver", "collection_elasticsearch_driver")
    hints = frozenset({"prefer:collection_elasticsearch_driver"})
    result = _resolve_driver_preferences(hints, entries)
    assert result == ["collection_elasticsearch_driver"]


def test_resolve_driver_preferences_alias_es():
    """Short alias 'es' resolves to any driver_ref containing 'elasticsearch'."""
    entries = _make_entries("collection_postgresql_driver", "collection_elasticsearch_driver")
    hints = frozenset({"prefer:es"})
    result = _resolve_driver_preferences(hints, entries)
    assert result == ["collection_elasticsearch_driver"]


def test_resolve_driver_preferences_alias_pg():
    """Short alias 'pg' resolves to any driver_ref containing 'postgresql'."""
    entries = _make_entries("collection_postgresql_driver", "collection_elasticsearch_driver")
    hints = frozenset({"prefer:pg"})
    result = _resolve_driver_preferences(hints, entries)
    assert result == ["collection_postgresql_driver"]


def test_resolve_driver_preferences_alias_postgres():
    """'postgres' alias also resolves to postgresql drivers."""
    entries = _make_entries("collection_postgresql_driver", "collection_elasticsearch_driver")
    hints = frozenset({"prefer:postgres"})
    result = _resolve_driver_preferences(hints, entries)
    assert result == ["collection_postgresql_driver"]


def test_resolve_driver_preferences_unknown_alias_no_match():
    """An unknown alias with no matching entry returns an empty list."""
    entries = _make_entries("collection_postgresql_driver", "collection_elasticsearch_driver")
    hints = frozenset({"prefer:bq"})
    result = _resolve_driver_preferences(hints, entries)
    assert result == []


def test_resolve_driver_preferences_no_prefer_tokens():
    """When no prefer tokens are present, returns empty list."""
    entries = _make_entries("collection_postgresql_driver", "collection_elasticsearch_driver")
    hints = frozenset({Hint.GEOMETRY_SIMPLIFIED})
    result = _resolve_driver_preferences(hints, entries)
    assert result == []


def test_resolve_driver_preferences_deduplication():
    """Two prefer tokens resolving to the same driver produce one entry."""
    entries = _make_entries("collection_elasticsearch_driver")
    hints = frozenset({"prefer:es", "prefer:elasticsearch"})
    result = _resolve_driver_preferences(hints, entries)
    assert result == ["collection_elasticsearch_driver"]


# ---------------------------------------------------------------------------
# _resolve_driver_ids_cached: prefer READ — ES first, PG as fallback tail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefer_es_read_puts_es_first_pg_as_tail():
    """prefer:es on READ routes ES first; PG stays as ordered fallback tail."""
    routing = _make_routing({
        Operation.READ: [
            ("collection_postgresql_driver", set()),
            ("collection_elasticsearch_driver", {Hint.METADATA}),
        ],
    })
    mock_configs = _mock_configs_protocol(routing)
    pg = _mock_driver("collection_postgresql_driver", supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}))
    es = _mock_driver("collection_elasticsearch_driver", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}))
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            ItemsRoutingConfig, "cat1", "col1", Operation.READ,
            frozenset({"prefer:es"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs[0] == "collection_elasticsearch_driver", f"Expected ES first, got {refs}"
    assert "collection_postgresql_driver" in refs, "PG should be in the fallback tail"


@pytest.mark.asyncio
async def test_prefer_pg_read_puts_pg_first():
    """prefer:pg on READ keeps PG first; ES not placed ahead."""
    routing = _make_routing({
        Operation.READ: [
            ("collection_postgresql_driver", set()),
            ("collection_elasticsearch_driver", {Hint.METADATA}),
        ],
    })
    mock_configs = _mock_configs_protocol(routing)
    pg = _mock_driver("collection_postgresql_driver", supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}))
    es = _mock_driver("collection_elasticsearch_driver", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}))
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            ItemsRoutingConfig, "cat1", "col1", Operation.READ,
            frozenset({"prefer:pg"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs[0] == "collection_postgresql_driver", f"Expected PG first, got {refs}"


# ---------------------------------------------------------------------------
# _resolve_driver_ids_cached: prefer SEARCH — matched-only, no tail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefer_es_search_returns_es_only():
    """prefer:es on SEARCH returns only ES (matched-only, no fallback tail)."""
    routing = _make_routing({
        Operation.SEARCH: [
            ("collection_elasticsearch_driver", {Hint.GEOMETRY_SIMPLIFIED}),
            ("collection_postgresql_driver", {Hint.GEOMETRY_EXACT}),
        ],
    })
    mock_configs = _mock_configs_protocol(routing)
    pg = _mock_driver("collection_postgresql_driver", supported_hints=frozenset({Hint.GEOMETRY_EXACT}))
    es = _mock_driver("collection_elasticsearch_driver", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}))
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            ItemsRoutingConfig, "cat1", "col1", Operation.SEARCH,
            frozenset({"prefer:es"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs == ["collection_elasticsearch_driver"], f"Expected only ES, got {refs}"


# ---------------------------------------------------------------------------
# _resolve_driver_ids_cached: prefer WRITE — ignored, fan-out unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefer_ignored_for_write():
    """prefer:es on WRITE does not redirect the fan-out; all WRITE entries returned."""
    routing = _make_routing({
        Operation.WRITE: [
            ("collection_postgresql_driver", set()),
            ("collection_elasticsearch_driver", set()),
        ],
    })
    mock_configs = _mock_configs_protocol(routing)
    pg = _mock_driver("collection_postgresql_driver")
    es = _mock_driver("collection_elasticsearch_driver")
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            ItemsRoutingConfig, "cat1", "col1", Operation.WRITE,
            frozenset({"prefer:es"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    # Both WRITE entries must be present (fan-out unchanged)
    assert "collection_postgresql_driver" in refs
    assert "collection_elasticsearch_driver" in refs


# ---------------------------------------------------------------------------
# _resolve_driver_ids_cached: unknown prefer alias falls through to normal path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefer_unknown_alias_falls_through_to_normal_path():
    """prefer:bq with no BQ entry → no override; falls through to no-hint path."""
    routing = _make_routing({
        Operation.READ: [
            # PG untagged (no hints), ES tagged with METADATA
            ("collection_postgresql_driver", set()),
            ("collection_elasticsearch_driver", {Hint.METADATA}),
        ],
    })
    mock_configs = _mock_configs_protocol(routing)
    pg = _mock_driver("collection_postgresql_driver", supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}))
    es = _mock_driver("collection_elasticsearch_driver", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}))
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            ItemsRoutingConfig, "cat1", "col1", Operation.READ,
            frozenset({"prefer:bq"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    # prefer:bq had no match and was stripped; remaining hints empty → no-hint
    # READ path: untagged entries only (PG), since PG is untagged and ES is tagged.
    assert "collection_postgresql_driver" in refs


# ---------------------------------------------------------------------------
# Collection routing config semantics: no hints → PG only; prefer:es → ES first
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_routing_no_hints_resolves_pg_only():
    """Default collection routing: no hints → only PG (untagged entry).
    The METADATA-tagged ES entry is excluded by the no-hint READ filter."""
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig

    cfg = CollectionRoutingConfig()
    mock_configs = _mock_configs_protocol(cfg)
    pg = _mock_driver(
        "collection_postgresql_driver",
        supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}),
    )
    es = _mock_driver(
        "collection_elasticsearch_driver",
        supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}),
    )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            CollectionRoutingConfig, "cat1", None, Operation.READ,
            frozenset(),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs == ["collection_postgresql_driver"], (
        f"No-hint READ must resolve PG only; got {refs}"
    )


@pytest.mark.asyncio
async def test_collection_routing_prefer_es_puts_es_first_pg_as_tail():
    """Collection routing: prefer:es → ES first, PG as ordered fallback tail."""
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig

    cfg = CollectionRoutingConfig()
    mock_configs = _mock_configs_protocol(cfg)
    pg = _mock_driver(
        "collection_postgresql_driver",
        supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}),
    )
    es = _mock_driver(
        "collection_elasticsearch_driver",
        supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}),
    )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            CollectionRoutingConfig, "cat1", None, Operation.READ,
            frozenset({"prefer:es"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs[0] == "collection_elasticsearch_driver", (
        f"prefer:es READ must place ES first; got {refs}"
    )
    assert "collection_postgresql_driver" in refs, "PG must be in the fallback tail"


# ---------------------------------------------------------------------------
# Catalog routing config semantics: no hints → PG only; prefer:es → ES first
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_routing_no_hints_resolves_pg_only():
    """Default catalog routing: no hints → only PG (untagged entry)."""
    from dynastore.modules.storage.routing_config import CatalogRoutingConfig

    cfg = CatalogRoutingConfig()
    mock_configs = _mock_configs_protocol(cfg)
    pg = _mock_driver(
        "catalog_postgresql_driver",
        supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}),
    )
    es = _mock_driver(
        "catalog_elasticsearch_driver",
        supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}),
    )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            CatalogRoutingConfig, "cat1", None, Operation.READ,
            frozenset(),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs == ["catalog_postgresql_driver"], (
        f"No-hint catalog READ must resolve PG only; got {refs}"
    )


@pytest.mark.asyncio
async def test_catalog_routing_prefer_es_puts_es_first_pg_as_tail():
    """Catalog routing: prefer:es → ES first, PG as ordered fallback tail."""
    from dynastore.modules.storage.routing_config import CatalogRoutingConfig

    cfg = CatalogRoutingConfig()
    mock_configs = _mock_configs_protocol(cfg)
    pg = _mock_driver(
        "catalog_postgresql_driver",
        supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}),
    )
    es = _mock_driver(
        "catalog_elasticsearch_driver",
        supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}),
    )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
        patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
    ):
        out = await _resolve_driver_ids_cached(
            CatalogRoutingConfig, "cat1", None, Operation.READ,
            frozenset({"prefer:es"}),
        )
    DriverRegistry.clear()
    _resolve_driver_ids_cached.cache_clear()
    refs = [t[0] for t in out]
    assert refs[0] == "catalog_elasticsearch_driver", (
        f"prefer:es catalog READ must place ES first; got {refs}"
    )
    assert "catalog_postgresql_driver" in refs, "PG must be in the fallback tail"
