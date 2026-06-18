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

"""Unit tests for geometry byte-budget threading through the envelope driver (Refs #1248).

Finding 1: envelope driver write_entities must pass max_bytes to maybe_simplify_for_es
           when simplify_target_bytes is set on the resolved config.

Finding 2: PrivateEntityTransformer._resolve_simplify_params must fail open
           (return True, DEFAULT_MAX_BYTES) when configs.get_config raises.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.tools.geometry_simplify import DEFAULT_MAX_BYTES


# ---------------------------------------------------------------------------
# Finding 1 — envelope driver write_entities threads max_bytes
# ---------------------------------------------------------------------------


class _FakeEnvelopeDriverConfig:
    """Resolved config with a 1 MB geometry budget."""

    simplify_geometry: bool = True
    simplify_target_bytes: int = 1_000_000


async def _async_resolve_simplify_geometry(self, *args, **kwargs) -> bool:
    return True


async def _async_resolve_simplify_max_bytes(self, *args, **kwargs) -> int:
    return 1_000_000


def test_write_entities_threads_max_bytes_to_maybe_simplify():
    """When _resolve_simplify_max_bytes returns 1_000_000, write_entities
    must call maybe_simplify_for_es with max_bytes=1_000_000."""
    from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
        ItemsElasticsearchEnvelopeDriver,
    )

    driver = ItemsElasticsearchEnvelopeDriver()

    # Track kwargs that maybe_simplify_for_es was called with.
    captured: list = []

    def fake_maybe_simplify(doc, *, simplify, max_bytes=DEFAULT_MAX_BYTES, **kwargs):
        captured.append({"simplify": simplify, "max_bytes": max_bytes})
        return doc, 1.0, "none"

    # Minimal feature with an id so _extract_item_id succeeds.
    feature = {"id": "geoid-1", "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {}}

    fake_es = AsyncMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.bulk = AsyncMock(return_value={"errors": False, "items": []})

    async def run():
        with (
            patch.object(
                type(driver),
                "_resolve_simplify_geometry",
                new=_async_resolve_simplify_geometry,
            ),
            patch.object(
                type(driver),
                "_resolve_simplify_max_bytes",
                new=_async_resolve_simplify_max_bytes,
            ),
            patch.object(driver, "_get_client", return_value=fake_es),
            patch.object(driver, "_ensure_index", AsyncMock()),
            patch(
                "dynastore.tools.geometry_simplify.maybe_simplify_for_es",
                side_effect=fake_maybe_simplify,
            ),
        ):
            await driver.write_entities(
                "cat1", "col1", [feature],
            )

    asyncio.get_event_loop().run_until_complete(run())

    assert captured, "maybe_simplify_for_es was never called"
    assert captured[0]["max_bytes"] == 1_000_000, (
        f"expected max_bytes=1_000_000, got {captured[0]['max_bytes']}"
    )


def test_index_threads_max_bytes_to_maybe_simplify():
    """envelope driver index() (single-op dispatcher path) must also thread max_bytes."""
    from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
        ItemsElasticsearchEnvelopeDriver,
    )

    driver = ItemsElasticsearchEnvelopeDriver()

    captured: list = []

    def fake_maybe_simplify(doc, *, simplify, max_bytes=DEFAULT_MAX_BYTES, **kwargs):
        captured.append({"simplify": simplify, "max_bytes": max_bytes})
        return doc, 1.0, "none"

    fake_es = AsyncMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.index = AsyncMock()

    ctx = MagicMock()
    ctx.catalog = "cat1"
    ctx.collection = "col1"

    op = MagicMock()
    op.entity_type = "item"
    op.op_type = "upsert"
    op.entity_id = "geoid-1"
    op.payload = {"id": "geoid-1", "geometry": {"type": "Point", "coordinates": [0, 0]},
                  "properties": {}}

    async def run():
        with (
            patch.object(
                type(driver),
                "_resolve_simplify_geometry",
                new=_async_resolve_simplify_geometry,
            ),
            patch.object(
                type(driver),
                "_resolve_simplify_max_bytes",
                new=_async_resolve_simplify_max_bytes,
            ),
            patch.object(driver, "_get_client", return_value=fake_es),
            patch.object(driver, "_ensure_index", AsyncMock()),
            patch(
                "dynastore.tools.geometry_simplify.maybe_simplify_for_es",
                side_effect=fake_maybe_simplify,
            ),
        ):
            await driver.index(ctx, op)

    asyncio.get_event_loop().run_until_complete(run())

    assert captured, "maybe_simplify_for_es was never called by index()"
    assert captured[0]["max_bytes"] == 1_000_000, (
        f"expected max_bytes=1_000_000, got {captured[0]['max_bytes']}"
    )


def test_index_bulk_threads_max_bytes_to_maybe_simplify():
    """envelope driver index_bulk() must also thread max_bytes."""
    from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
        ItemsElasticsearchEnvelopeDriver,
    )

    driver = ItemsElasticsearchEnvelopeDriver()

    captured: list = []

    def fake_maybe_simplify(doc, *, simplify, max_bytes=DEFAULT_MAX_BYTES, **kwargs):
        captured.append({"simplify": simplify, "max_bytes": max_bytes})
        return doc, 1.0, "none"

    fake_es = AsyncMock()
    fake_es.indices.exists = AsyncMock(return_value=True)
    fake_es.bulk = AsyncMock(return_value={"errors": False, "items": [
        {"index": {"_id": "geoid-1", "status": 200}},
    ]})

    ctx = MagicMock()
    ctx.catalog = "cat1"
    ctx.collection = "col1"

    op = MagicMock()
    op.entity_type = "item"
    op.op_type = "upsert"
    op.entity_id = "geoid-1"
    op.payload = {"id": "geoid-1", "geometry": {"type": "Point", "coordinates": [0, 0]},
                  "properties": {}}

    async def run():
        with (
            patch.object(
                type(driver),
                "_resolve_simplify_geometry",
                new=_async_resolve_simplify_geometry,
            ),
            patch.object(
                type(driver),
                "_resolve_simplify_max_bytes",
                new=_async_resolve_simplify_max_bytes,
            ),
            patch.object(driver, "_get_client", return_value=fake_es),
            patch.object(driver, "_ensure_index", AsyncMock()),
            patch(
                "dynastore.tools.geometry_simplify.maybe_simplify_for_es",
                side_effect=fake_maybe_simplify,
            ),
        ):
            await driver.index_bulk(ctx, [op])

    asyncio.get_event_loop().run_until_complete(run())

    assert captured, "maybe_simplify_for_es was never called by index_bulk()"
    assert captured[0]["max_bytes"] == 1_000_000, (
        f"expected max_bytes=1_000_000, got {captured[0]['max_bytes']}"
    )


# ---------------------------------------------------------------------------
# Finding 2 — PrivateEntityTransformer._resolve_simplify_params fails open
#             when configs.get_config raises
# ---------------------------------------------------------------------------


def test_transformer_resolver_fails_open_when_get_config_raises():
    """When configs.get_config raises, the resolver must return (True, DEFAULT_MAX_BYTES)."""
    from dynastore.modules.storage.drivers.elasticsearch_private.transformer import (
        PrivateEntityTransformer,
    )
    from dynastore.models.protocols.entity_transform import TransformChainContext

    async def _raise_get_config(*_a, **_kw):
        raise RuntimeError("simulated transient db error")

    fake_configs = type("FakeConfigs", (), {"get_config": _raise_get_config})()
    ctx = TransformChainContext()

    async def run():
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=fake_configs,
        ):
            result = await PrivateEntityTransformer._resolve_simplify_params(
                "cat1", "col1", ctx,
            )
        return result

    simplify, max_bytes = asyncio.get_event_loop().run_until_complete(run())
    assert simplify is True, f"expected True, got {simplify!r}"
    assert max_bytes == DEFAULT_MAX_BYTES, (
        f"expected DEFAULT_MAX_BYTES={DEFAULT_MAX_BYTES}, got {max_bytes}"
    )


def test_transformer_resolver_caches_fail_open_result():
    """The fail-open result from a get_config exception is memoized on ctx.cache."""
    from dynastore.modules.storage.drivers.elasticsearch_private.transformer import (
        PrivateEntityTransformer,
    )
    from dynastore.models.protocols.entity_transform import TransformChainContext

    call_count = 0

    async def _raise_get_config(*_a, **_kw):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("transient error")

    fake_configs = type("FakeConfigs", (), {"get_config": _raise_get_config})()
    ctx = TransformChainContext()

    async def run():
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=fake_configs,
        ):
            r1 = await PrivateEntityTransformer._resolve_simplify_params("cat1", "col1", ctx)
            r2 = await PrivateEntityTransformer._resolve_simplify_params("cat1", "col1", ctx)
        return r1, r2

    (s1, b1), (s2, b2) = asyncio.get_event_loop().run_until_complete(run())
    # Both calls return the fail-open defaults.
    assert s1 is True and b1 == DEFAULT_MAX_BYTES
    assert s2 is True and b2 == DEFAULT_MAX_BYTES
    # get_config only called once — second call hits the cache.
    assert call_count == 1, f"expected 1 get_config call, got {call_count}"
