"""Live-PostgreSQL round-trip for the task-capability registry repository.

Lives under ``integration/`` (this repo's convention for DB-backed tests — see
the sibling ``tasks/integration/`` suite). It runs as part of the full,
DB-backed test session, NOT the fast unit runs. The session fixture clones and
cleans the master DB, so run this against the isolated Docker test database, not
a working ``gis_dev``.

Self-sufficient: it applies the registry DDL idempotently up front so it does
not depend on platform-config startup having created the table in the test DB.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from dynastore.modules.db_config.typed_store.ddl import TASK_CAPABILITY_REGISTRY_DDL
from dynastore.modules.tasks.registry import repository
from dynastore.modules.tasks.registry.model import CapabilityRow


async def _ensure_table(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS configs"))
        for stmt in (s.strip() for s in TASK_CAPABILITY_REGISTRY_DDL.split(";") if s.strip()):
            await conn.execute(text(stmt))


@pytest.mark.asyncio
async def test_upsert_then_list_roundtrip(db_engine):  # db_engine fixture from conftest
    await _ensure_table(db_engine)

    row = CapabilityRow(
        service="worker", task_key="gdal", kind="process", modes=["off_load", "async"],
        required_capability=None, mandatory=False, affinity_tier=None,
        service_version="1.0.0", service_commit="c1", version="c1",
    )
    await repository.upsert_rows(db_engine, [row])

    rows = await repository.list_all(db_engine)
    gdal = [r for r in rows if r["service"] == "worker" and r["task_key"] == "gdal"]
    assert len(gdal) == 1
    assert gdal[0]["modes"] == ["off_load", "async"]  # text[] round-trips intact

    # Idempotent re-upsert: same PK must not duplicate the row.
    await repository.upsert_rows(db_engine, [row])
    rows2 = await repository.list_all(db_engine)
    assert len([r for r in rows2 if r["task_key"] == "gdal" and r["service"] == "worker"]) == 1


@pytest.mark.asyncio
async def test_live_owners_for_respects_grace_window(db_engine):
    await _ensure_table(db_engine)

    row = CapabilityRow(
        service="worker", task_key="cascade_cleanup", kind="task", modes=["async"],
        required_capability=None, mandatory=True, affinity_tier="catalog",
        service_version="1.0.0", service_commit="c1", version="c1",
    )
    await repository.upsert_rows(db_engine, [row])
    await repository.heartbeat(db_engine, "worker")

    fresh = await repository.live_owners_for(db_engine, "cascade_cleanup", 3600.0)
    assert any(o["service"] == "worker" and o["affinity_tier"] == "catalog" for o in fresh)

    # A zero-second grace window excludes every row (last_seen is never > now()).
    stale = await repository.live_owners_for(db_engine, "cascade_cleanup", 0.0)
    assert stale == []
