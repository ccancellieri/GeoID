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

"""Hint-set dispatch on ``resolve_drivers`` (issue #995).

Pins the request-side composition rewrite that promotes the per-request
preference axis to a typed ``hints=frozenset(Hint, ...)``. Covers the
best-overlap matcher, the empty-entry-hints fallback to
``driver.supported_hints``, the longest-overlap + entry-order tiebreak,
and the frozenset cache-key contract on the L4 request cache.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.driver_registry import DriverRegistry
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.router import (
    _resolve_driver_ids_cached,
    get_driver,
    resolve_drivers,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    ItemsRoutingConfig,
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
        # ES does not match (GEOMETRY_SIMPLIFIED ⊉ {FEATURES, GEOMETRY_EXACT}).
        # READ: matched entries first (PG), then unmatched entries as fallback
        # tail (ES) so a hint-preferred miss can still fall through to a reader.
        assert [t[0] for t in out] == ["postgresql", "elasticsearch"]

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
    async def test_join_hint_selects_pg_over_es_via_supported_hints(self):
        # Public default items READ lists ES + PG with no explicit entry hints,
        # so each entry's effective surface is its driver's ``supported_hints``.
        # Only PG advertises ``Hint.JOIN`` (ES tops out at GEOMETRY_SIMPLIFIED),
        # so the DWH-join / feature-export request ``{Hint.JOIN}`` resolves to PG
        # first — the full-precision read path, bypassing the ES-first default.
        # READ appends the non-matching ES entry as a fallback tail (declared order)
        # after PG, so a hinted read that PG misses can still try ES.
        routing = _make_routing({
            Operation.READ: [
                ("elasticsearch", set()),
                ("postgresql", set()),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        es = _mock_driver(
            "elasticsearch",
            supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.SEARCH}),
        )
        pg = _mock_driver(
            "postgresql",
            supported_hints=frozenset({Hint.JOIN, Hint.GEOMETRY_EXACT, Hint.FEATURES}),
        )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[es, pg]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "datamgr10", "region", Operation.READ,
                frozenset({Hint.JOIN}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        # PG matches (declares JOIN); ES does not → appended as fallback tail.
        assert [t[0] for t in out] == ["postgresql", "elasticsearch"]

    @pytest.mark.asyncio
    async def test_read_hint_unsatisfiable_relaxes_to_any_reader(self):
        # An ES-only catalog: the sole items driver serves GEOMETRY_SIMPLIFIED.
        # A READ asking for GEOMETRY_EXACT matches no driver, but for read paths
        # the hint is a preference, not a hard filter — the router relaxes it and
        # falls back to the available reader so the request returns data instead
        # of an empty result. This is what makes the OGC API Features /items list
        # non-empty on ES-only catalogs (it previously read the absent exact-
        # geometry PG tier and returned numberMatched=0).
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
        assert [t[0] for t in out] == ["elasticsearch"]

    @pytest.mark.asyncio
    async def test_index_hint_unsatisfiable_returns_empty_not_relaxed(self):
        """INDEX is never relaxed (unlike READ): an unsatisfiable hint must
        yield [] so the derived-search resolvers (``get_items_search_driver``)
        can tell "no INDEX entry satisfies this hint" apart from "an INDEX
        entry matched" and fall back to the READ lane with the SAME hints —
        e.g. a Hint.GROUP_BY search must resolve to the PG READ entry, not
        relax to an ES INDEX entry that doesn't support GROUP BY."""
        routing = _make_routing({
            Operation.INDEX: [
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
                ItemsRoutingConfig, "cat1", "col1", Operation.INDEX,
                frozenset({Hint.GEOMETRY_EXACT}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert out == []

    @pytest.mark.asyncio
    async def test_write_hint_unsatisfiable_returns_empty(self):
        # WRITE is NEVER relaxed: fanning a write to an unintended driver must
        # stay impossible, so an unsatisfiable hint set yields no driver.
        routing = _make_routing({
            Operation.WRITE: [
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
                ItemsRoutingConfig, "cat1", "col1", Operation.WRITE,
                frozenset({Hint.GEOMETRY_EXACT}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert out == []


# ---------------------------------------------------------------------------
# resolve_drivers — public entry point
# ---------------------------------------------------------------------------


class TestResolveDriversHintSet:
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
                new=AsyncMock(return_value=[("postgresql", FailurePolicy.FATAL)]),
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
                new=AsyncMock(return_value=[("postgresql", FailurePolicy.FATAL)]),
            ),
        ):
            out = await get_driver(
                "READ", "cat1", "col1",
                hints=frozenset({Hint.GEOMETRY_EXACT}),
            )
            assert out is pg


# ---------------------------------------------------------------------------
# Task D.1 — READ fallback tail, INDEX matched-only, WRITE strict empty
# ---------------------------------------------------------------------------


class TestReadFallbackTail:
    """Pins the hinted-READ tail-append behaviour introduced alongside the
    routing-config two-entry READ default (PG untagged + ES hinted)."""

    @pytest.mark.asyncio
    async def test_read_hinted_returns_matched_then_unmatched_tail(self):
        """READ with hints=[PG untagged, ES geometry_simplified]: hint=geometry_simplified
        resolves [ES, PG] — ES matched first (longest effective wins), PG appended as
        fallback tail."""
        routing = _make_routing({
            Operation.READ: [
                ("postgresql", set()),   # untagged → driver supported_hints used
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql", supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}))
        es = _mock_driver("elasticsearch", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}))
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset({Hint.GEOMETRY_SIMPLIFIED}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        # ES matched (carries GEOMETRY_SIMPLIFIED); PG did not match the hinted set
        # but is appended as fallback tail (declared order: PG was entry 0).
        assert [t[0] for t in out] == ["elasticsearch", "postgresql"]

    @pytest.mark.asyncio
    async def test_read_no_hints_excludes_opt_in_tagged_entry(self):
        """No-hint READ with an untagged default (PG) + a hint-tagged opt-in
        entry (ES) resolves PG ONLY. The tagged ES reader is opt-in: a plain
        read must not pull it into the default merge / first-non-None set, or a
        merge-all metadata router would overwrite PG's exact geometry with the
        ES simplified slice. This is the byte-identical-to-pre-hint guarantee
        for collection/catalog READ (which gained the ES entry)."""
        routing = _make_routing({
            Operation.READ: [
                ("postgresql", set()),
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql", supported_hints=frozenset({Hint.GEOMETRY_EXACT, Hint.METADATA}))
        es = _mock_driver("elasticsearch", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA}))
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset(),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert [t[0] for t in out] == ["postgresql"]

    @pytest.mark.asyncio
    async def test_read_no_hints_all_tagged_keeps_full_list(self):
        """No-hint READ where EVERY entry is tagged (the items-tier convention:
        ES-first + PG, both tagged) keeps the full list in declared order — the
        untagged-only filter must never strip readers to empty, so resolved[0]
        still decides (ES is the intentional items READ default)."""
        routing = _make_routing({
            Operation.READ: [
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
                ("postgresql", {Hint.GEOMETRY_EXACT}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        es = _mock_driver("elasticsearch", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}))
        pg = _mock_driver("postgresql", supported_hints=frozenset({Hint.GEOMETRY_EXACT}))
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.READ,
                frozenset(),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert [t[0] for t in out] == ["elasticsearch", "postgresql"]

    @pytest.mark.asyncio
    async def test_index_hinted_returns_matched_only_no_tail(self):
        """INDEX keeps matched-only (no unmatched fallback tail) — same as
        WRITE. A derived search picks one backend from this lane; there is
        no SoR chain to fall through to within the lane itself (fallback to
        READ happens one layer up, in ``get_items_search_driver``)."""
        routing = _make_routing({
            Operation.INDEX: [
                ("postgresql", set()),
                ("elasticsearch", {Hint.GEOMETRY_SIMPLIFIED}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql", supported_hints=frozenset({Hint.GEOMETRY_EXACT}))
        es = _mock_driver("elasticsearch", supported_hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}))
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[pg, es]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.INDEX,
                frozenset({Hint.GEOMETRY_SIMPLIFIED}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        # ES matched; PG did NOT match — INDEX must NOT append PG as tail.
        assert [t[0] for t in out] == ["elasticsearch"]

    @pytest.mark.asyncio
    async def test_write_unsatisfiable_hint_returns_empty(self):
        """WRITE with an unsatisfiable hint yields [] — no relaxation, no tail."""
        routing = _make_routing({
            Operation.WRITE: [
                ("postgresql", {Hint.GEOMETRY_EXACT}),
            ],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql")
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.tools.discovery.get_protocols", return_value=[pg]),
        ):
            out = await _resolve_driver_ids_cached(
                ItemsRoutingConfig, "cat1", "col1", Operation.WRITE,
                frozenset({Hint.GEOMETRY_SIMPLIFIED}),
            )
        DriverRegistry.clear()
        _resolve_driver_ids_cached.cache_clear()
        assert out == []
