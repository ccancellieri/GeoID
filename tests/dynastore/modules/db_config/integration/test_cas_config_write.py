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

"""#2707 — DB-backed round-trip for the ``ConfigsProtocol`` compare-and-set
primitive at platform scope.

Exercises the full read+write surface against a live PostgreSQL backend:

1. ``get_config_versioned`` on a config with no persisted row yet returns
   the code default paired with ``version=None``.
2. A CAS write with ``expected_version=None`` behaves like a normal
   unconditional ``set_config`` — no row to conflict with.
3. The token round-trips: writing then reading back via
   ``get_config_versioned`` yields a fresh, non-``None`` version distinct
   from the previous one.
4. Two "writers" racing from the same base version: the first CAS write
   succeeds, the second (still holding the now-stale token) loses —
   raises ``ConfigVersionConflictError`` — and the row reflects only the
   winner's change.
5. A CAS write using a fresh, correct token succeeds after a conflict.
6. Genuinely concurrent writers (two ``asyncio.gather``-ed tasks, each on
   its own connection out of a ``NullPool`` engine) racing from the same
   base version: exactly one wins, the other gets a real
   ``ConfigVersionConflictError`` from an overlapping transaction, not
   just a sequential-call simulation.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from dynastore.modules.db_config.exceptions import ConfigVersionConflictError
from dynastore.modules.db_config.platform_config_service import PlatformConfigService
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.tiles.tiles_config import TilesConfig


@pytest_asyncio.fixture
async def fresh_platform_engine(db_url):
    """A NullPool engine with the configs schema dropped + recreated."""
    url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(url, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS configs CASCADE;"))

    async with managed_transaction(engine) as conn:
        await PlatformConfigService.initialize_storage(conn)

    yield engine
    await engine.dispose()


@pytest.fixture
def platform_service(fresh_platform_engine):
    svc = PlatformConfigService(fresh_platform_engine)
    svc._setup_cache()
    return svc


@pytest.mark.asyncio
async def test_get_config_versioned_no_row_returns_none_version(platform_service):
    cfg, version = await platform_service.get_config_versioned(TilesConfig)
    assert isinstance(cfg, TilesConfig)
    assert version is None


@pytest.mark.asyncio
async def test_cas_write_with_no_version_behaves_like_plain_set_config(platform_service):
    """No prior row, no version to assert against — writes unconditionally."""
    payload = TilesConfig(min_zoom=2, max_zoom=10)
    await platform_service.set_config(TilesConfig, payload, expected_version=None)

    round_tripped, version = await platform_service.get_config_versioned(TilesConfig)
    assert round_tripped.min_zoom == 2
    assert round_tripped.max_zoom == 10
    assert version is not None


@pytest.mark.asyncio
async def test_version_token_round_trips_and_changes_on_write(platform_service):
    await platform_service.set_config(TilesConfig, TilesConfig(min_zoom=2, max_zoom=10))
    _, v1 = await platform_service.get_config_versioned(TilesConfig)
    assert v1 is not None

    await platform_service.set_config(
        TilesConfig, TilesConfig(min_zoom=3, max_zoom=10), expected_version=v1,
    )
    cfg2, v2 = await platform_service.get_config_versioned(TilesConfig)
    assert cfg2.min_zoom == 3
    assert v2 is not None
    assert v2 != v1


@pytest.mark.asyncio
async def test_cas_success_then_conflict_for_the_loser(platform_service):
    """Two writers read the same base version; only the first CAS write
    lands — the second (stale token) is rejected, not silently applied."""
    await platform_service.set_config(TilesConfig, TilesConfig(min_zoom=1, max_zoom=10))
    base_cfg, base_version = await platform_service.get_config_versioned(TilesConfig)
    assert base_version is not None

    # Writer A wins the race.
    winner = base_cfg.model_copy(update={"min_zoom": 5})
    await platform_service.set_config(TilesConfig, winner, expected_version=base_version)

    # Writer B, still holding the stale base_version, loses.
    loser = base_cfg.model_copy(update={"max_zoom": 20})
    with pytest.raises(ConfigVersionConflictError):
        await platform_service.set_config(TilesConfig, loser, expected_version=base_version)

    # The stored row reflects only the winner's change — the loser's write
    # never landed, silently or otherwise.
    final_cfg, _ = await platform_service.get_config_versioned(TilesConfig)
    assert final_cfg.min_zoom == 5
    assert final_cfg.max_zoom == 10


@pytest.mark.asyncio
async def test_loser_retries_with_fresh_version_and_succeeds(platform_service):
    """The actuator retry pattern: on conflict, re-read via
    ``get_config_versioned`` and retry with the fresh token."""
    await platform_service.set_config(TilesConfig, TilesConfig(min_zoom=1, max_zoom=10))
    base_cfg, base_version = await platform_service.get_config_versioned(TilesConfig)

    winner = base_cfg.model_copy(update={"min_zoom": 5})
    await platform_service.set_config(TilesConfig, winner, expected_version=base_version)

    loser = base_cfg.model_copy(update={"max_zoom": 20})
    with pytest.raises(ConfigVersionConflictError):
        await platform_service.set_config(TilesConfig, loser, expected_version=base_version)

    # Retry: re-read, rebuild the update on the fresh value, write again.
    fresh_cfg, fresh_version = await platform_service.get_config_versioned(TilesConfig)
    retried = fresh_cfg.model_copy(update={"max_zoom": 20})
    await platform_service.set_config(TilesConfig, retried, expected_version=fresh_version)

    final_cfg, _ = await platform_service.get_config_versioned(TilesConfig)
    assert final_cfg.min_zoom == 5
    assert final_cfg.max_zoom == 20


@pytest.mark.asyncio
async def test_cas_write_against_stale_version_after_delete_conflicts(platform_service):
    """A version token from before a delete is stale — the row is absent,
    so the CAS predicate cannot match; must raise, not resurrect a row."""
    await platform_service.set_config(TilesConfig, TilesConfig(min_zoom=1, max_zoom=10))
    _, version = await platform_service.get_config_versioned(TilesConfig)

    await platform_service.delete_config(TilesConfig)

    with pytest.raises(ConfigVersionConflictError):
        await platform_service.set_config(
            TilesConfig, TilesConfig(min_zoom=9, max_zoom=9), expected_version=version,
        )


@pytest.mark.asyncio
async def test_two_genuinely_concurrent_writers_one_loses(fresh_platform_engine):
    """Real overlapping transactions, not a sequential simulation.

    Two ``PlatformConfigService`` instances share the same ``NullPool``
    engine (so each write gets its own physical connection) and both race
    ``set_config(..., expected_version=base_version)`` via
    ``asyncio.gather``. Postgres serializes the two ``UPDATE ... WHERE
    updated_at = :expected_version`` statements — the second to commit
    finds the row already moved and updates zero rows. Exactly one of the
    two must raise ``ConfigVersionConflictError``; the other lands, and
    the final row reflects only the winner's field.
    """
    svc = PlatformConfigService(fresh_platform_engine)
    svc._setup_cache()
    await svc.set_config(TilesConfig, TilesConfig(min_zoom=1, max_zoom=10))
    base_cfg, base_version = await svc.get_config_versioned(TilesConfig)
    assert base_version is not None

    writer_a = PlatformConfigService(fresh_platform_engine)
    writer_a._setup_cache()
    writer_b = PlatformConfigService(fresh_platform_engine)
    writer_b._setup_cache()

    payload_a = base_cfg.model_copy(update={"min_zoom": 5})
    payload_b = base_cfg.model_copy(update={"min_zoom": 7})

    results = await asyncio.gather(
        writer_a.set_config(TilesConfig, payload_a, expected_version=base_version),
        writer_b.set_config(TilesConfig, payload_b, expected_version=base_version),
        return_exceptions=True,
    )

    conflicts = [r for r in results if isinstance(r, ConfigVersionConflictError)]
    successes = [r for r in results if not isinstance(r, Exception)]
    other_errors = [r for r in results if isinstance(r, Exception) and r not in conflicts]

    assert not other_errors, f"unexpected exception(s): {other_errors!r}"
    assert len(conflicts) == 1, f"expected exactly one loser, got {results!r}"
    assert len(successes) == 1

    final_cfg, _ = await svc.get_config_versioned(TilesConfig)
    assert final_cfg.min_zoom in (5, 7)  # exactly one writer's value landed
