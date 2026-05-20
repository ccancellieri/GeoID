#    Copyright 2025 FAO
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

"""Integration (#1074): attribute-statistics DDL executes on real PostgreSQL and
a computed value round-trips through asyncpg.

The unit tests pin the generated SQL *strings* and the in-memory
payload/expose logic. This proves the bits unit tests cannot:

- the attributes-sidecar DDL is valid PostgreSQL — the COLUMNAR
  ``DOUBLE PRECISION`` stat column, its B-tree, and the shared
  ``attribute_stats`` JSONB blob all create;
- a value produced by ``prepare_upsert_payload`` actually persists into those
  columns through the asyncpg dialect and reads back via
  ``resolve_computed_value``.

The test owns a throwaway schema it creates and drops — it never touches the
shared ``configs``/master schemas, so it is safe to run serially (avoids the
f4c master-DB drop hazard).
"""

import uuid

import pytest
from sqlalchemy import text

from dynastore.modules.db_config.query_executor import DDLQuery
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)

_JSONB_COLS = {"attributes", "attribute_stats"}


def _value_placeholder(col: str) -> str:
    """Bind placeholder for a payload column (casts UUID / JSONB explicitly)."""
    if col == "geoid":
        return "CAST(:geoid AS uuid)"
    if col in _JSONB_COLS:
        return f"CAST(:{col} AS jsonb)"
    return f":{col}"


@pytest.mark.asyncio
async def test_attribute_stats_ddl_and_roundtrip_on_real_pg(app_lifespan):
    engine = app_lifespan.engine
    schema = f"tmp_attr_stats_{uuid.uuid4().hex[:8]}"
    phys = "items"
    attr_table = f"{phys}_attributes"

    sidecar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.ATTRIBUTE_STAT,
                    source="properties.population",
                    name="pop",
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    indexed=True,
                ),
                ComputedField(
                    kind=ComputedKind.ATTRIBUTE_STAT,
                    source="properties.gdp",
                    name="gdp",
                    storage_mode=StatisticStorageMode.JSONB,
                ),
            ],
        )
    )

    geoid = str(uuid.uuid4())
    payload = sidecar.prepare_upsert_payload(
        {"id": "f1", "properties": {"population": 500, "gdp": 1.5}},
        {"geoid": geoid},
    )
    # Sanity on the store-side payload before it touches the DB.
    assert payload["pop"] == 500
    assert "attribute_stats" in payload

    try:
        # --- DDL on the engine (own tx; handles ``::`` + multi-statement) ---
        await DDLQuery(f"CREATE SCHEMA IF NOT EXISTS {schema}").execute(engine)
        await DDLQuery("CREATE EXTENSION IF NOT EXISTS pgcrypto").execute(engine)
        await DDLQuery(
            f'CREATE TABLE {schema}."{phys}" (geoid UUID NOT NULL, PRIMARY KEY (geoid))'
        ).execute(engine)
        # The real attributes-sidecar DDL — proves it is valid PostgreSQL.
        await DDLQuery(sidecar.get_ddl(phys)).execute(engine, schema=schema)

        # --- Store the payload through the asyncpg dialect ---
        cols = list(payload.keys())
        col_sql = ", ".join(f'"{c}"' for c in cols)
        val_sql = ", ".join(_value_placeholder(c) for c in cols)
        async with engine.begin() as conn:
            await conn.execute(
                text(f'INSERT INTO {schema}."{phys}" (geoid) VALUES (CAST(:g AS uuid))'),
                {"g": geoid},
            )
            await conn.execute(
                text(f'INSERT INTO {schema}."{attr_table}" ({col_sql}) VALUES ({val_sql})'),
                payload,
            )

        # --- Read back ---
        async with engine.connect() as conn:
            res = await conn.execute(
                text(
                    f'SELECT "pop", attribute_stats FROM {schema}."{attr_table}" '
                    f"WHERE geoid = CAST(:g AS uuid)"
                ),
                {"g": geoid},
            )
            row = dict(res.mappings().one())

            idx = await conn.execute(
                text(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE schemaname = :s AND tablename = :t"
                ),
                {"s": schema, "t": attr_table},
            )
            index_names = {r[0] for r in idx}

        # COLUMNAR stat persisted as a real numeric column.
        assert row["pop"] == 500
        # indexed=True emitted a real B-tree on the stat column.
        assert any("pop" in name for name in index_names), index_names

        # The sidecar's read-extract logic resolves both layouts off the real row.
        assert sidecar.resolve_computed_value(row, "pop") == (True, 500.0)
        found_gdp, gdp = sidecar.resolve_computed_value(row, "gdp")
        assert found_gdp is True
        assert float(gdp) == 1.5
    finally:
        await DDLQuery(f"DROP SCHEMA IF EXISTS {schema} CASCADE").execute(engine)
