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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""DDL for the PostgreSQL :class:`TypedStore` backend.

Three scope-specific tables + one content-addressed schema registry:

* ``configs.schemas`` — global schema registry (content-addressed by sha256).
* ``configs.platform_configs`` — global, keyed by ``ref_key`` (Cycle F.4c.1).
* ``"<tenant_schema>".catalog_configs`` — per-tenant, keyed by ``ref_key``.
* ``"<tenant_schema>".collection_configs`` — per-tenant, keyed by
  ``(collection_id, ref_key)``.

Cycle F.4c.1 introduces ``ref_key`` as the operator-chosen instance name and
makes it part of the primary key.  ``class_key`` remains a NOT NULL
discriminator column so the dispatch class is recoverable from any row.
For single-instance configs (every config today) ``ref_key`` equals the
``class_key``; the multi-instance API extension that lets two rows share a
``class_key`` lands in F.4c.2.

Per-tenant tables live inside the tenant's own PG schema, matching
dynastore's physical tenant isolation — no ``catalog_id`` column needed.
"""

from __future__ import annotations

from dynastore.tools.db import validate_sql_identifier

CONFIGS_SCHEMA = "configs"

# Physical table names for the two per-tenant config stores.
# Referenced by both DDL (tenant_configs_ddl) and DML query factories
# (config_queries.py) so that renaming the tables requires a single edit here.
CATALOG_CONFIGS_TABLE = "catalog_configs"
COLLECTION_CONFIGS_TABLE = "collection_configs"

PLATFORM_SCHEMAS_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {CONFIGS_SCHEMA};

CREATE TABLE IF NOT EXISTS {CONFIGS_SCHEMA}.schemas (
    schema_id    TEXT        PRIMARY KEY,
    class_key    TEXT        NOT NULL,
    schema_json  JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by   TEXT
);

CREATE INDEX IF NOT EXISTS ix_schemas_class_key
    ON {CONFIGS_SCHEMA}.schemas (class_key);

CREATE TABLE IF NOT EXISTS {CONFIGS_SCHEMA}.platform_configs (
    ref_key     TEXT        PRIMARY KEY,
    class_key   TEXT        NOT NULL,
    schema_id   TEXT        NOT NULL REFERENCES {CONFIGS_SCHEMA}.schemas(schema_id),
    config_data JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_platform_configs_class_key
    ON {CONFIGS_SCHEMA}.platform_configs (class_key);
"""


def tenant_configs_ddl(tenant_schema: str) -> str:
    """Return idempotent DDL for the two per-tenant typed-config tables.

    ``tenant_schema`` is validated before interpolation to prevent SQL
    injection (asyncpg cannot bind identifiers).
    """
    validate_sql_identifier(tenant_schema)
    return f"""
    CREATE TABLE IF NOT EXISTS "{tenant_schema}".catalog_configs (
        ref_key     TEXT        PRIMARY KEY,
        class_key   TEXT        NOT NULL,
        schema_id   TEXT        NOT NULL REFERENCES {CONFIGS_SCHEMA}.schemas(schema_id),
        config_data JSONB       NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_catalog_configs_class_key
        ON "{tenant_schema}".catalog_configs (class_key);

    CREATE TABLE IF NOT EXISTS "{tenant_schema}".collection_configs (
        collection_id TEXT        NOT NULL,
        ref_key       TEXT        NOT NULL,
        class_key     TEXT        NOT NULL,
        schema_id     TEXT        NOT NULL REFERENCES {CONFIGS_SCHEMA}.schemas(schema_id),
        config_data   JSONB       NOT NULL,
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (collection_id, ref_key)
    );

    CREATE INDEX IF NOT EXISTS ix_collection_configs_class_key
        ON "{tenant_schema}".collection_configs (class_key);
    """
