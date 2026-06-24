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

"""DQL query factories for the typed-config store tables.

All SQL that touches the config tables is defined here:

* ``configs.platform_configs`` — platform-level config store
* ``<tenant>.catalog_configs`` — per-tenant catalog config store
* ``<tenant>.collection_configs`` — per-tenant collection config store

**Platform queries** are module-level :class:`~dynastore.modules.db_config.query_executor.DQLQuery`
instances because the table path is static (always ``configs.*``).

**Tenant queries** are factory functions that accept ``phys_schema`` and return
a :class:`~dynastore.modules.db_config.query_executor.DQLQuery`.  The schema name is
validated via :func:`~dynastore.tools.db.validate_sql_identifier` before interpolation,
matching the same injection-prevention strategy used in :func:`~.ddl.tenant_configs_ddl`.

Cycle F.4c.1 keys all driver/engine/plugin rows by ``ref_key`` (PK).
``class_key`` remains a NOT NULL discriminator column so the dispatch class is
recoverable from any row.  Single-instance configs always have
``ref_key == class_key``; multi-instance support arrives with the F.4c.2 API
extension.

Usage example::

    from dynastore.modules.db_config.typed_store import config_queries as q

    # Platform — single-instance: ref_key equals class_key
    cfg = await q.get_platform_config.execute(conn, ref_key=cls.class_key())

    # Tenant
    data = await q.select_catalog_config(phys_schema).execute(conn, ref_key=key)
    await q.upsert_catalog_config(phys_schema).execute(conn, **params)
"""

from __future__ import annotations

from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
from dynastore.tools.db import validate_sql_identifier
from dynastore.modules.db_config.typed_store.ddl import (
    CONFIGS_SCHEMA,
    CATALOG_CONFIGS_TABLE,
    COLLECTION_CONFIGS_TABLE,
)

# ===========================================================================
#  Platform-level queries  (static table path → module-level DQLQuery)
# ===========================================================================

get_platform_config = DQLQuery(
    f"SELECT config_data FROM {CONFIGS_SCHEMA}.platform_configs WHERE ref_key = :ref_key;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

# F.4c.2 ref-keyed read API: surfaces class_key alongside config_data so callers
# can resolve the dispatch class from the row.  ``get_platform_config`` retains
# the SCALAR_ONE_OR_NONE semantics for class-keyed callers that already know
# the class statically.
get_platform_config_by_ref = DQLQuery(
    f"SELECT class_key, config_data FROM {CONFIGS_SCHEMA}.platform_configs WHERE ref_key = :ref_key;",
    result_handler=ResultHandler.ONE_DICT,
)

list_platform_refs = DQLQuery(
    f"SELECT ref_key, class_key FROM {CONFIGS_SCHEMA}.platform_configs ORDER BY ref_key;",
    result_handler=ResultHandler.ALL_DICTS,
)

upsert_platform_config = DQLQuery(
    f"""
    INSERT INTO {CONFIGS_SCHEMA}.platform_configs (ref_key, class_key, schema_id, config_data, updated_at)
    VALUES (:ref_key, :class_key, :schema_id, CAST(:config_data AS jsonb), NOW())
    ON CONFLICT (ref_key) DO UPDATE SET
        class_key   = EXCLUDED.class_key,
        schema_id   = EXCLUDED.schema_id,
        config_data = EXCLUDED.config_data,
        updated_at  = NOW();
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

list_platform_configs = DQLQuery(
    f"SELECT ref_key, class_key, config_data FROM {CONFIGS_SCHEMA}.platform_configs;",
    result_handler=ResultHandler.ALL_DICTS,
)

delete_platform_config = DQLQuery(
    f"DELETE FROM {CONFIGS_SCHEMA}.platform_configs WHERE ref_key = :ref_key;",
    result_handler=ResultHandler.ROWCOUNT,
)

# Distinct (class_key, schema_id) actually serialized in platform config rows.
# Schemas are not persisted in a registry table — they are generated on demand
# from the registered class (``cls.model_json_schema()``). This query backs the
# diagnostic audit of which schema versions live in real config rows.
list_platform_config_schema_ids = DQLQuery(
    f"SELECT DISTINCT class_key, schema_id FROM {CONFIGS_SCHEMA}.platform_configs ORDER BY class_key, schema_id;",
    result_handler=ResultHandler.ALL,
)


# ===========================================================================
#  Tenant-level queries  (dynamic schema → factory functions)
# ===========================================================================
#
# Every factory validates ``phys_schema`` before interpolation — identical to
# the strategy used in ``tenant_configs_ddl()``.  Never call these with
# untrusted user input that has not first been resolved to a physical schema
# from ``catalog.catalogs``.
# ===========================================================================


# --- catalog_configs ---------------------------------------------------------

def select_catalog_config(phys_schema: str) -> DQLQuery:
    """SELECT config_data for a single ref_key (read path, no lock)."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE ref_key = :ref_key;',
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    )


def select_catalog_config_by_ref(phys_schema: str) -> DQLQuery:
    """F.4c.2 ref-keyed read: returns class_key + config_data for a single ref_key."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT class_key, config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE ref_key = :ref_key;',
        result_handler=ResultHandler.ONE_DICT,
    )


def list_catalog_refs(phys_schema: str) -> DQLQuery:
    """F.4c.2 enumerate {ref_key: class_key} for the catalog scope."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT ref_key, class_key FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} ORDER BY ref_key;',
        result_handler=ResultHandler.ALL_DICTS,
    )


def select_catalog_config_for_update(phys_schema: str) -> DQLQuery:
    """SELECT config_data FOR UPDATE — used during immutability check before write."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE ref_key = :ref_key FOR UPDATE;',
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    )


def upsert_catalog_config(phys_schema: str) -> DQLQuery:
    """INSERT … ON CONFLICT DO UPDATE for catalog-level config."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f"""
        INSERT INTO "{phys_schema}".{CATALOG_CONFIGS_TABLE} (ref_key, class_key, schema_id, config_data, updated_at)
        VALUES (:ref_key, :class_key, :schema_id, CAST(:config_data AS jsonb), NOW())
        ON CONFLICT (ref_key) DO UPDATE SET
            class_key   = EXCLUDED.class_key,
            schema_id   = EXCLUDED.schema_id,
            config_data = EXCLUDED.config_data,
            updated_at  = NOW();
        """,
        result_handler=ResultHandler.ROWCOUNT,
    )


def delete_catalog_config(phys_schema: str) -> DQLQuery:
    """DELETE a single catalog-level config row."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'DELETE FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE ref_key = :ref_key;',
        result_handler=ResultHandler.ROWCOUNT,
    )


def list_catalog_configs(phys_schema: str) -> DQLQuery:
    """SELECT all ref_key / class_key / config_data rows (used for snapshots)."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT ref_key, class_key, config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE};',
        result_handler=ResultHandler.ALL_DICTS,
    )


def list_catalog_configs_paginated(phys_schema: str) -> DQLQuery:
    """SELECT with window COUNT + ORDER BY ref_key, LIMIT/OFFSET pagination."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f"""
        SELECT COUNT(*) OVER() AS total_count, ref_key, class_key, config_data
        FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE}
        ORDER BY ref_key
        LIMIT :limit OFFSET :offset;
        """,
        result_handler=ResultHandler.ALL_DICTS,
    )


# --- collection_configs -------------------------------------------------------

def select_collection_config(phys_schema: str) -> DQLQuery:
    """SELECT config_data for a single (collection_id, ref_key) pair (no lock)."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT config_data FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE collection_id = :collection_id AND ref_key = :ref_key;',
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    )


def select_collection_config_by_ref(phys_schema: str) -> DQLQuery:
    """F.4c.2 ref-keyed read: returns class_key + config_data for (collection_id, ref_key)."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT class_key, config_data FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE collection_id = :collection_id AND ref_key = :ref_key;',
        result_handler=ResultHandler.ONE_DICT,
    )


def list_collection_refs(phys_schema: str) -> DQLQuery:
    """F.4c.2 enumerate {ref_key: class_key} for a given collection_id."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT ref_key, class_key FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE collection_id = :collection_id ORDER BY ref_key;',
        result_handler=ResultHandler.ALL_DICTS,
    )


def select_collection_config_for_update(phys_schema: str) -> DQLQuery:
    """SELECT config_data FOR UPDATE — used during immutability check before write."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'SELECT config_data FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE collection_id = :collection_id AND ref_key = :ref_key FOR UPDATE;',
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    )


def upsert_collection_config(phys_schema: str) -> DQLQuery:
    """INSERT … ON CONFLICT DO UPDATE for collection-level config."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f"""
        INSERT INTO "{phys_schema}".{COLLECTION_CONFIGS_TABLE} (collection_id, ref_key, class_key, schema_id, config_data, updated_at)
        VALUES (:collection_id, :ref_key, :class_key, :schema_id, CAST(:config_data AS jsonb), NOW())
        ON CONFLICT (collection_id, ref_key) DO UPDATE SET
            class_key   = EXCLUDED.class_key,
            schema_id   = EXCLUDED.schema_id,
            config_data = EXCLUDED.config_data,
            updated_at  = NOW();
        """,
        result_handler=ResultHandler.ROWCOUNT,
    )


def delete_collection_config(phys_schema: str) -> DQLQuery:
    """DELETE a single collection-level config row."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f'DELETE FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE collection_id = :collection_id AND ref_key = :ref_key;',
        result_handler=ResultHandler.ROWCOUNT,
    )


def list_collection_configs_paginated(phys_schema: str) -> DQLQuery:
    """SELECT with window COUNT + ORDER BY ref_key for a given collection_id."""
    validate_sql_identifier(phys_schema)
    return DQLQuery(
        f"""
        SELECT COUNT(*) OVER() AS total_count, ref_key, class_key, config_data
        FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE}
        WHERE collection_id = :collection_id
        ORDER BY ref_key
        LIMIT :limit OFFSET :offset;
        """,
        result_handler=ResultHandler.ALL_DICTS,
    )
