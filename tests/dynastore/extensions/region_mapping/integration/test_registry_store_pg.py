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

"""Real-PostgreSQL smoke test for the ``region_mapping`` registry persistence
layer (un-fao/GeoID#2824, dynastore#2821/#2823).

``test_registry_store.py`` mocks ``registry_queries`` entirely, so
``ensure_mappings_table``'s DDL, the UPDATE-then-INSERT upsert dance, and the
``CAST(:keep_claim_ci AS TEXT[])`` array bind never touch a real asyncpg
connection. This module runs the same write paths against PostgreSQL
directly -- provision, apply, idempotent re-apply, cross-mapping conflict,
concurrent first-apply of the same mapping, and delete.

Uses the lightweight ``db_engine`` fixture (no full app bootstrap needed --
``apply_mapping``/``delete_mapping``/``ensure_mappings_table`` all take an
explicit engine argument), mirroring
``tests/dynastore/modules/db_config/integration/test_concurrency.py``.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from dynastore.extensions.region_mapping import registry_queries as rq
from dynastore.extensions.region_mapping import registry_store as store
from dynastore.modules.db_config.exceptions import UniqueViolationError
from dynastore.modules.db_config.query_executor import managed_transaction


def _unique_slug(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


async def _claims_for(engine, mapping_id: str) -> list[dict]:
    async with managed_transaction(engine) as conn:
        return await rq.SELECT_CLAIMS_BY_MAPPING_ID.execute(conn, mapping_id=mapping_id)


@pytest.mark.asyncio
async def test_apply_mapping_full_lifecycle_against_real_postgres(db_engine) -> None:
    """Provision, apply, idempotent re-apply, cross-mapping conflict, delete."""
    await rq.ensure_mappings_table(db_engine)

    catalog_id = _unique_slug("cat")
    collection_id = _unique_slug("coll")
    column = _unique_slug("region")

    mapping_id, rows = await store.apply_mapping(
        db_engine,
        catalog_id=catalog_id, collection_id=collection_id,
        column=column, alias=column, extra_aliases=[], title="Countries",
    )
    try:
        assert rows, "first apply must create at least one claim row"
        claim_cis = {row["claim_ci"] for row in rows}

        # Re-apply the identical mapping: idempotent, no exception, claims
        # unchanged.
        mapping_id_again, rows_again = await store.apply_mapping(
            db_engine,
            catalog_id=catalog_id, collection_id=collection_id,
            column=column, alias=column, extra_aliases=[], title="Countries",
        )
        assert mapping_id_again == mapping_id
        assert {row["claim_ci"] for row in rows_again} == claim_cis

        # A different mapping trying to claim the same column text is a
        # genuine cross-mapping collision -- must still surface 23505.
        other_collection_id = _unique_slug("coll")
        with pytest.raises(UniqueViolationError):
            await store.apply_mapping(
                db_engine,
                catalog_id=catalog_id, collection_id=other_collection_id,
                column=column, alias=column, extra_aliases=[], title="Conflict",
            )

        persisted = await _claims_for(db_engine, mapping_id)
        assert {row["claim_ci"] for row in persisted} == claim_cis
    finally:
        deleted = await store.delete_mapping(db_engine, mapping_id)
        assert deleted == len(claim_cis)
        assert await _claims_for(db_engine, mapping_id) == []


@pytest.mark.asyncio
async def test_apply_mapping_concurrent_first_apply_same_mapping_is_idempotent(
    db_url, db_engine,
) -> None:
    """Two racing first-applies of the SAME (never-applied) mapping on
    SEPARATE connections/engines must both succeed -- never a spurious 409
    (dynastore#2824). Each ``apply_mapping`` call owns its own engine, so
    this never shares one connection across ``asyncio.gather`` tasks."""
    await rq.ensure_mappings_table(db_engine)

    asyncpg_url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    engine_a = create_async_engine(asyncpg_url, poolclass=NullPool)
    engine_b = create_async_engine(asyncpg_url, poolclass=NullPool)

    catalog_id = _unique_slug("cat")
    collection_id = _unique_slug("coll")
    column = _unique_slug("region")
    mapping_id = None

    try:
        (mapping_id_a, rows_a), (mapping_id_b, rows_b) = await asyncio.gather(
            store.apply_mapping(
                engine_a,
                catalog_id=catalog_id, collection_id=collection_id,
                column=column, alias=column, extra_aliases=[], title="Race",
            ),
            store.apply_mapping(
                engine_b,
                catalog_id=catalog_id, collection_id=collection_id,
                column=column, alias=column, extra_aliases=[], title="Race",
            ),
        )
        assert mapping_id_a == mapping_id_b
        mapping_id = mapping_id_a
        assert rows_a and rows_b

        persisted = await _claims_for(db_engine, mapping_id)
        claim_cis = [row["claim_ci"] for row in persisted]
        assert len(claim_cis) == len(set(claim_cis)), (
            "concurrent first-apply of the same mapping must not leave "
            "duplicate claim_ci rows"
        )
    finally:
        await engine_a.dispose()
        await engine_b.dispose()
        if mapping_id is not None:
            deleted = await store.delete_mapping(db_engine, mapping_id)
            assert deleted == len(claim_cis)
            assert await _claims_for(db_engine, mapping_id) == []
