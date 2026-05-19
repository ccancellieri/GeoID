#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Hint-set dispatch on ``resolve_drivers`` (issue #995).

Pins the request-side composition rewrite that promotes the per-request
preference axis from a single ``hint=`` string to a typed
``hints=frozenset(Hint, ...)``. Covers the best-overlap matcher, the
empty-entry-hints fallback to ``driver.supported_hints``, the longest-
overlap + entry-order tiebreak, the legacy ``hint=`` back-compat shim,
and the frozenset cache-key contract on the L4 request cache.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.driver_registry import DriverRegistry
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.router import (
    _coerce_hints,
    _resolve_driver_ids_cached,
    get_driver,
    resolve_drivers,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    ItemsRoutingConfig,
    WriteMode,
)


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


def _mock_configs_protocol(routing_config):
    mock = MagicMock()
    mock.get_config = AsyncMock(return_value=routing_config)
    return mock


def _mock_driver(driver_ref: str, supported_hints: frozenset = frozenset()):
    """Create a mock driver whose class name equals ``driver_ref`` and
    whose class-level ``supported_hints`` carries the requested set."""
    cls = type(driver_ref, (MagicMock,), {"supported_hints": supported_hints})
    return cls()


# ---------------------------------------------------------------------------
# _coerce_hints — back-compat shim
# ---------------------------------------------------------------------------


class TestCoerceHints:
    def test_empty_passthrough(self):
        assert _coerce_hints(frozenset(), None) == frozenset()

    def test_hints_only_passthrough(self):
        hs = frozenset({Hint.GEOMETRY_EXACT})
        assert _coerce_hints(hs, None) is hs

    def test_legacy_hint_string_promoted(self):
        out = _coerce_hints(frozenset(), "geometry_exact")
        assert out == frozenset({Hint.GEOMETRY_EXACT})

    def test_legacy_hint_enum_promoted(self):
        out = _coerce_hints(frozenset(), Hint.TILES.value)
        assert out == frozenset({Hint.TILES})

    def test_both_supplied_raises(self):
        with pytest.raises(TypeError, match="either hints= .* or hint="):
            _coerce_hints(frozenset({Hint.GEOMETRY_EXACT}), "geometry_exact")

    def test_legacy_unknown_hint_raises(self):
        # Hint(...) raises ValueError for an unknown member — the shim does
        # not paper over typos, the StrEnum coerce is strict.
        with pytest.raises(ValueError):
            _coerce_hints(frozenset(), "absolutely_not_a_hint")


# ---------------------------------------------------------------------------
# _resolve_driver_ids_cached — best-overlap matcher
# ---------------------------------------------------------------------------


class TestBestOverlapMatcher:
    @pytest.mark.asyncio
    async def test_empty_hints_returns_all_entries_in_order(self):
        routing = _make_routing({
            Operation.READ: [
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
                ("postgresql", {Hint.GEOMETRY_EXACT}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        es = _mock_driver("elasticsearch")
        pg = _mock_driver("postgresql")
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[es, pg]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ, frozenset(),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert [t[0] for t in out] == ["elasticsearch", "postgresql"]

    @pytest.mark.asyncio
    async def test_superset_match_filters_non_matching_entries(self):
        routing = _make_routing({
            Operation.READ: [
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
                ("postgresql", {Hint.GEOMETRY_EXACT, Hint.FEATURES, Hint.TILES}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        es = _mock_driver("elasticsearch")
        pg = _mock_driver("postgresql")
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[es, pg]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset({Hint.FEATURES, Hint.GEOMETRY_EXACT}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        # ES only carries GEOMETRY_SIMPLIFIED → not a superset → excluded.
        assert [t[0] for t in out] == ["postgresql"]

    @pytest.mark.asyncio
    async def test_longest_effective_hints_wins_tiebreak(self):
        # Both entries match the request {FEATURES}; the entry whose effective
        # surface is longer wins. Final tiebreak (here irrelevant) is entry
        # order in the config.
        routing = _make_routing({
            Operation.READ: [
                ("driver_short", {Hint.FEATURES}),  # len=1
                ("driver_long", {Hint.FEATURES, Hint.GEOMETRY_EXACT, Hint.TILES}),  # len=3
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        d_short = _mock_driver("driver_short")
        d_long = _mock_driver("driver_long")
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[d_short, d_long]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset({Hint.FEATURES}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert [t[0] for t in out] == ["driver_long", "driver_short"]

    @pytest.mark.asyncio
    async def test_entry_order_breaks_ties_on_equal_length(self):
        routing = _make_routing({
            Operation.READ: [
                ("driver_first", {Hint.FEATURES}),
                ("driver_second", {Hint.FEATURES}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        d1 = _mock_driver("driver_first")
        d2 = _mock_driver("driver_second")
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[d1, d2]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset({Hint.FEATURES}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert [t[0] for t in out] == ["driver_first", "driver_second"]

    @pytest.mark.asyncio
    async def test_empty_entry_hints_fall_back_to_driver_supported_hints(self):
        routing = _make_routing({
            Operation.READ: [
                ("postgresql", set()),  # no entry hints → fall back to class-level
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql", supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.FEATURES}))
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[pg]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset({Hint.GEOMETRY_EXACT}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert [t[0] for t in out] == ["postgresql"]

    @pytest.mark.asyncio
    async def test_request_hint_unsatisfiable_returns_empty(self):
        routing = _make_routing({
            Operation.READ: [
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        es = _mock_driver("elasticsearch")
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[es]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset({Hint.GEOMETRY_EXACT}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert out == []


# ---------------------------------------------------------------------------
# resolve_drivers — public entry point + back-compat shim
# ---------------------------------------------------------------------------


class TestResolveDriversHintSet:
    @pytest.mark.asyncio
    async def test_legacy_hint_string_matches_typed_hints(self):
        pg = _mock_driver("postgresql")
        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)]),
            ) as mock_cached,
        ):
            legacy = await resolve_drivers("READ", "cat1", "col1", hint="geometry_exact")
            typed = await resolve_drivers(
                "READ", "cat1", "col1", hints=frozenset({Hint.GEOMETRY_EXACT}),
            )
            assert legacy[0].driver is typed[0].driver is pg
            # Both calls coerce to the same frozenset → same cache key.
            for call in mock_cached.await_args_list:
                # positional sig: (cls, cat, col, op, hints)
                assert call.args[4] == frozenset({Hint.GEOMETRY_EXACT})

    @pytest.mark.asyncio
    async def test_both_hint_and_hints_raises_typeerror(self):
        with pytest.raises(TypeError, match="either hints= .* or hint="):
            await resolve_drivers(
                "READ", "cat1", "col1",
                hints=frozenset({Hint.GEOMETRY_EXACT}),
                hint="geometry_exact",
            )

    @pytest.mark.asyncio
    async def test_l4_cache_key_uses_frozenset(self):
        # Regression pin: frozenset(...) must be hashable as a tuple member.
        # An accidental ``set(...)`` would raise ``TypeError: unhashable
        # type: 'set'`` on the ``if l4_key in l4`` line.
        pg = _mock_driver("postgresql")
        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)]),
            ),
        ):
            out = await resolve_drivers(
                "READ", "cat1", "col1",
                hints=frozenset({Hint.FEATURES, Hint.GEOMETRY_EXACT}),
            )
            assert out[0].driver is pg

    @pytest.mark.asyncio
    async def test_get_driver_passes_hints_to_resolve(self):
        pg = _mock_driver("postgresql")
        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)]),
            ),
        ):
            out = await get_driver(
                "READ", "cat1", "col1",
                hints=frozenset({Hint.GEOMETRY_EXACT}),
            )
            assert out is pg
