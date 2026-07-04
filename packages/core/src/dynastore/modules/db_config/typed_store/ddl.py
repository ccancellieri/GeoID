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

"""DDL for the PostgreSQL :class:`TypedStore` backend.

Three scope-specific config tables. JSON schemas are not persisted — they are
generated on demand from the registered class (``cls.model_json_schema()``);
each config row carries the content-addressed ``schema_id`` (sha256) as a plain
version tag for drift detection.

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

CREATE TABLE IF NOT EXISTS {CONFIGS_SCHEMA}.platform_configs (
    ref_key     TEXT        PRIMARY KEY,
    class_key   TEXT        NOT NULL,
    schema_id   TEXT        NOT NULL,
    config_data JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_platform_configs_class_key
    ON {CONFIGS_SCHEMA}.platform_configs (class_key);
"""


# Durable task-capability registry. Platform-wide observed facts: one row per
# (service, task_key). Created idempotently alongside the platform config
# schemas; never ALTERed in place (hard invariant — new columns ship via a fresh
# CREATE on a clean pre-prod DB, not ADD COLUMN).
#
# Self-contained like PLATFORM_SCHEMAS_DDL: it re-asserts the configs schema so
# this table is never the lone victim when an earlier schema-create step is
# skipped (e.g. a multi-worker cold-start race on the existence check). The
# leading CREATE SCHEMA does not change the auto-inferred existence check, which
# keys on the first CREATE TABLE (configs.task_capability_registry).
TASK_CAPABILITY_REGISTRY_DDL = """
CREATE SCHEMA IF NOT EXISTS configs;
CREATE TABLE IF NOT EXISTS configs.task_capability_registry (
    service             text        NOT NULL,
    task_key            text        NOT NULL,
    kind                text        NOT NULL,
    required_capability text        NULL,
    mandatory           boolean     NOT NULL DEFAULT false,
    affinity_tier       text        NULL,
    service_version     text        NOT NULL DEFAULT 'unknown',
    service_commit      text        NOT NULL DEFAULT 'unknown',
    version             text        NOT NULL DEFAULT 'unknown',
    description         text        NOT NULL DEFAULT '',
    payload_schema      jsonb       NULL,
    last_seen           timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service, task_key)
);
CREATE INDEX IF NOT EXISTS task_capability_registry_task_key_idx
    ON configs.task_capability_registry (task_key);
CREATE INDEX IF NOT EXISTS task_capability_registry_mandatory_idx
    ON configs.task_capability_registry (task_key) WHERE mandatory;
"""


# Leader-lease table for transaction-mode-pooler-safe leader election.
# Must live in the configs schema alongside platform_configs.  Created
# idempotently at startup inside initialize_storage; never ALTER-ed in place.
# The leading CREATE SCHEMA IF NOT EXISTS makes the DDL self-contained so it
# survives a multi-worker cold-start race where the schema-create step is
# skipped (same reasoning as TASK_CAPABILITY_REGISTRY_DDL above).
LEADER_LEASE_DDL = """
CREATE SCHEMA IF NOT EXISTS configs;
CREATE TABLE IF NOT EXISTS configs.leader_lease (
    lock_key    BIGINT      PRIMARY KEY,
    lock_name   TEXT        NOT NULL,
    owner       TEXT        NOT NULL,
    epoch       BIGINT      NOT NULL DEFAULT 1,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    renewed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);
"""


# Per-instance liveness heartbeat (geoid#2924). One row per running process
# (keyed by the per-process instance id minted at import time — see
# dynastore.modules.db_config.instance.get_instance_id), renewed on a cheap
# cadence by every pod. configs.leader_lease only carries a row for whichever
# pod currently holds a given lease, so it cannot answer "is this specific
# instance alive" for a pod that never won an election; this table can. The
# zombie-session reaper (modules/db/zombie_session_reaper.py) uses "no row, or
# a row stale past a generous grace window" as proof an instance is gone.
# Created idempotently alongside the other configs-schema tables; never
# ALTER-ed in place.
INSTANCE_LIVENESS_DDL = """
CREATE SCHEMA IF NOT EXISTS configs;
CREATE TABLE IF NOT EXISTS configs.instance_liveness (
    instance_id TEXT        PRIMARY KEY,
    service     TEXT        NOT NULL,
    renewed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
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
        schema_id   TEXT        NOT NULL,
        config_data JSONB       NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_catalog_configs_class_key
        ON "{tenant_schema}".catalog_configs (class_key);

    CREATE TABLE IF NOT EXISTS "{tenant_schema}".collection_configs (
        collection_id TEXT        NOT NULL,
        ref_key       TEXT        NOT NULL,
        class_key     TEXT        NOT NULL,
        schema_id     TEXT        NOT NULL,
        config_data   JSONB       NOT NULL,
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (collection_id, ref_key)
    );

    CREATE INDEX IF NOT EXISTS ix_collection_configs_class_key
        ON "{tenant_schema}".collection_configs (class_key);
    """
