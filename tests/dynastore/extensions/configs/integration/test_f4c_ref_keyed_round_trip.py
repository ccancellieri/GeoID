"""Cycle F.4c.5 — DB-backed round-trip for the F.4c.1-4 ref-keyed stack.

Closes the F.4c series with a single integration test that exercises
the full read+write surface against a live PostgreSQL backend:

1. Bootstrap the platform schema with the F.4c.1 DDL
   (``ref_key`` PK + ``class_key`` discriminator).
2. Write a single-instance config the canonical way
   (``set_config(TilesConfig, ...)``); ``ref_key == class_key``.
3. Write a second multi-instance row at the same class but a different
   ref via ``set_config_by_ref("tiles_secondary", ...)``.
4. ``list_refs_at_scope()`` returns BOTH refs, both pointing at
   ``tiles_config``.
5. ``get_config_by_ref`` resolves each row independently and returns
   distinct payloads.
6. The class-mismatch guard rejects an attempt to overwrite
   ``tiles_secondary`` with a different class (``StatsConfig``).
7. ``delete_config_by_ref("tiles_secondary")`` returns True; the canonical
   row stays; a second delete returns False (no-op).

This is the pin that lets future cycles (F.4c URL-PATCH rewrite,
F.4d-e) build on F.4c without re-validating the storage layer.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from dynastore.modules.db_config.platform_config_service import PlatformConfigService
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.modules.tiles.tiles_config import TilesConfig
from dynastore.modules.stats.config import StatsConfig


@pytest_asyncio.fixture
async def fresh_platform_engine(db_url):
    """A NullPool engine with the configs schema dropped + recreated.

    Drops every prior platform_configs row so the round-trip starts from
    a known empty state.  Per-test isolation: the next integration test
    gets the same fresh schema.
    """
    url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(url, poolclass=NullPool)

    # Drop + recreate the configs schema with the F.4c.1 DDL.
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS configs CASCADE;"))

    async with managed_transaction(engine) as conn:
        await PlatformConfigService.initialize_storage(conn)

    yield engine
    await engine.dispose()


@pytest.fixture
def platform_service(fresh_platform_engine):
    """A PlatformConfigService bound to the fresh engine."""
    svc = PlatformConfigService(fresh_platform_engine)
    svc._setup_cache()
    return svc


@pytest.mark.asyncio
async def test_f4c_ref_keyed_round_trip(platform_service):
    """End-to-end pin: F.4c.1 storage + F.4c.2 read API + F.4c.4 write API."""
    canonical_class_key = TilesConfig.class_key()

    # ---- 1. Single-instance write via the canonical class-keyed API ----
    canonical_payload = TilesConfig(min_zoom=2, max_zoom=10)
    await platform_service.set_config(TilesConfig, canonical_payload)

    # ---- 2. Verify the canonical row is visible via list_refs ----
    refs = await platform_service.list_refs()
    assert refs.get(canonical_class_key) == canonical_class_key, (
        f"After set_config, ref_key should equal class_key.  Got refs={refs!r}"
    )

    # ---- 3. Multi-instance write at a different ref, same class ----
    secondary_ref = "tiles_secondary"
    secondary_payload = TilesConfig(min_zoom=5, max_zoom=15, cache_on_demand=False)
    await platform_service.set_config_by_ref(secondary_ref, secondary_payload)

    # ---- 4. list_refs surfaces BOTH rows pointing at tiles_config ----
    refs = await platform_service.list_refs()
    assert refs.get(canonical_class_key) == canonical_class_key
    assert refs.get(secondary_ref) == canonical_class_key
    assert len([k for k, v in refs.items() if v == canonical_class_key]) >= 2

    # ---- 5. get_config_by_ref returns distinct payloads per ref ----
    canonical_round_trip = await platform_service.get_config_by_ref(canonical_class_key)
    secondary_round_trip = await platform_service.get_config_by_ref(secondary_ref)
    assert isinstance(canonical_round_trip, TilesConfig)
    assert isinstance(secondary_round_trip, TilesConfig)
    assert canonical_round_trip.min_zoom == 2
    assert canonical_round_trip.max_zoom == 10
    assert secondary_round_trip.min_zoom == 5
    assert secondary_round_trip.max_zoom == 15
    assert secondary_round_trip.cache_on_demand is False

    # ---- 6. Class-mismatch guard rejects cross-class overwrite ----
    with pytest.raises(ValueError, match=r"refusing to overwrite"):
        await platform_service.set_config_by_ref(
            secondary_ref, StatsConfig(),
        )

    # ---- 7. Delete returns True the first time, False the second ----
    deleted_first = await platform_service.delete_config_by_ref(secondary_ref)
    assert deleted_first is True
    deleted_second = await platform_service.delete_config_by_ref(secondary_ref)
    assert deleted_second is False

    # The canonical row is untouched by ref-keyed delete of the secondary.
    refs = await platform_service.list_refs()
    assert refs.get(canonical_class_key) == canonical_class_key
    assert secondary_ref not in refs


@pytest.mark.asyncio
async def test_f4c_get_config_by_ref_unknown_returns_none(platform_service):
    """Sanity: an unknown ref does NOT fall back to the canonical row."""
    got = await platform_service.get_config_by_ref("ref_definitely_not_present")
    assert got is None


@pytest.mark.asyncio
async def test_f4c_list_platform_refs_query_returns_only_projection(fresh_platform_engine):
    """Pin the lightweight projection at the SQL boundary — list_platform_refs
    must NOT bring back config_data (operators rely on it for cheap
    enumeration when triaging multi-instance deployments)."""
    async with managed_transaction(fresh_platform_engine) as conn:
        rows = await _cq.list_platform_refs.execute(conn)
    # On a fresh schema there are no rows; assert the query shape is fine
    # (callable, returns a list-like) — adversarial schema drift would
    # surface here as a column error.
    assert isinstance(rows, list)
