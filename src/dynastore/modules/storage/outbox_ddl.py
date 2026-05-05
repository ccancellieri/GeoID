"""storage_outbox + index_failure_log DDL.

Created at per-catalog schema bootstrap (alongside other tenant DDLs).
The outbox table carries an AFTER INSERT trigger that emits
``pg_notify('outbox_<driver_id>_<schema>', json)`` so per-driver-per-tenant
drain workers wake on commit instead of polling.

DDL templates schema-qualify every object reference via
``{schema}.<name>`` and are rendered with ``str.format(schema=schema)`` at
call time. ``schema`` is validated through
:func:`dynastore.tools.db.validate_sql_identifier` before any string
formatting so a caller-controlled schema can never inject SQL.

Two entry points are exposed for each table:

* :func:`ensure_storage_outbox` / :func:`ensure_index_failure_log` —
  production path. Runs through :class:`DDLQuery` so the auto-inferred
  existence-check shortcut + advisory locking kick in on warm boots.
* :func:`ensure_storage_outbox_asyncpg` /
  :func:`ensure_index_failure_log_asyncpg` — test path. Executes the same
  rendered DDL on a raw ``asyncpg.Connection`` so LISTEN / NOTIFY can be
  exercised against the same physical session that runs the DDL. NOT for
  production catalog bootstrap.

The ``current_schema()`` reference inside ``storage_outbox_notify`` stays
runtime — it resolves to the schema the row was inserted into, which is
load-bearing for the NOTIFY channel name.
"""

from __future__ import annotations

from typing import Any

from dynastore.modules.db_config.query_executor import DDLQuery
from dynastore.tools.db import validate_sql_identifier


_STORAGE_OUTBOX_DDL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {schema}.storage_outbox (
    op_id              UUID         PRIMARY KEY,
    driver_id          TEXT         NOT NULL,
    driver_instance_id TEXT         NOT NULL,
    collection_id      TEXT         NOT NULL,
    op                 TEXT         NOT NULL,
    item_id            TEXT,
    payload            JSONB        NOT NULL,
    idempotency_key    TEXT         NOT NULL,
    status             TEXT         NOT NULL DEFAULT 'ready',
    attempts           INT          NOT NULL DEFAULT 0,
    last_error         TEXT,
    ready_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at         TIMESTAMPTZ,
    claimed_by         TEXT,
    finished_at        TIMESTAMPTZ,
    CONSTRAINT storage_outbox_status_chk
        CHECK (status IN ('ready', 'in_flight', 'done', 'failed'))
);

CREATE INDEX IF NOT EXISTS storage_outbox_drain_idx
    ON {schema}.storage_outbox (driver_id, status, ready_at)
    WHERE status IN ('ready', 'in_flight');

CREATE INDEX IF NOT EXISTS storage_outbox_done_idx
    ON {schema}.storage_outbox (finished_at)
    WHERE status = 'done';

CREATE OR REPLACE FUNCTION {schema}.storage_outbox_notify() RETURNS trigger AS $$
DECLARE
    cat TEXT := current_schema();
    payload TEXT;
BEGIN
    payload := json_build_object(
        'driver_id', NEW.driver_id,
        'catalog_id', cat,
        'op_id', NEW.op_id::text
    )::text;
    PERFORM pg_notify('outbox_' || NEW.driver_id || '_' || cat, payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS storage_outbox_notify_trg ON {schema}.storage_outbox;
CREATE TRIGGER storage_outbox_notify_trg
    AFTER INSERT ON {schema}.storage_outbox
    FOR EACH ROW EXECUTE FUNCTION {schema}.storage_outbox_notify();
"""


_INDEX_FAILURE_LOG_DDL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {schema}.index_failure_log (
    failure_id          UUID         PRIMARY KEY,
    occurred_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    collection_id       TEXT         NOT NULL,
    driver_id           TEXT         NOT NULL,
    driver_instance_id  TEXT         NOT NULL,
    op_id               UUID,
    item_id             TEXT,
    op                  TEXT         NOT NULL,
    attempts            INT          NOT NULL,
    error_class         TEXT         NOT NULL,
    error_message       TEXT         NOT NULL,
    status              TEXT         NOT NULL,
    correlation_id      TEXT,
    CONSTRAINT index_failure_log_status_chk
        CHECK (status IN ('retrying', 'failed'))
);

CREATE INDEX IF NOT EXISTS index_failure_log_browse_idx
    ON {schema}.index_failure_log (collection_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS index_failure_log_status_idx
    ON {schema}.index_failure_log (status, occurred_at DESC)
    WHERE status = 'failed';
"""


def _storage_outbox_ddl(schema: str) -> str:
    """Render the storage_outbox DDL with ``schema`` baked in.

    ``schema`` is validated up front so the subsequent
    ``str.format`` cannot interpolate an injection vector.
    """
    validate_sql_identifier(schema)
    return _STORAGE_OUTBOX_DDL_TEMPLATE.format(schema=f'"{schema}"')


def _index_failure_log_ddl(schema: str) -> str:
    """Render the index_failure_log DDL with ``schema`` baked in.

    ``schema`` is validated up front so the subsequent
    ``str.format`` cannot interpolate an injection vector.
    """
    validate_sql_identifier(schema)
    return _INDEX_FAILURE_LOG_DDL_TEMPLATE.format(schema=f'"{schema}"')


async def ensure_storage_outbox(db_resource: Any, schema: str) -> None:
    """Apply storage_outbox DDL via :class:`DDLQuery` (production path).

    Goes through :class:`DDLQuery` so the auto-inferred existence-check
    shortcut + advisory locking apply — second and subsequent calls for
    the same schema short-circuit on the cached sentinel without
    re-parsing the full multi-statement DDL.

    Idempotent. ``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT
    EXISTS`` plus ``DROP TRIGGER IF EXISTS`` then ``CREATE TRIGGER`` makes
    the trigger re-creation safe across calls; ``CREATE OR REPLACE
    FUNCTION`` keeps the handler in lockstep with the latest definition.
    """
    ddl = _storage_outbox_ddl(schema)
    await DDLQuery(ddl).execute(db_resource)


async def ensure_storage_outbox_asyncpg(conn: Any, schema: str) -> None:
    """Apply storage_outbox DDL on a raw ``asyncpg.Connection`` (test path).

    Used by ``test_pg_outbox.py`` to exercise LISTEN / NOTIFY on the same
    physical session that owns the DDL. NOT for production catalog
    bootstrap — production uses :func:`ensure_storage_outbox`, which goes
    through :class:`DDLQuery` for the warm-path shortcut and advisory
    locking.

    asyncpg's ``Connection.execute`` accepts multi-statement scripts when
    no parameters are bound, which is the case for these idempotent
    CREATE blocks; we hand the rendered script over as a single execute.
    """
    ddl = _storage_outbox_ddl(schema)
    await conn.execute(ddl)


async def ensure_index_failure_log(db_resource: Any, schema: str) -> None:
    """Apply index_failure_log DDL via :class:`DDLQuery` (production path).

    See :func:`ensure_storage_outbox` for the rationale on routing through
    :class:`DDLQuery`.
    """
    ddl = _index_failure_log_ddl(schema)
    await DDLQuery(ddl).execute(db_resource)


async def ensure_index_failure_log_asyncpg(conn: Any, schema: str) -> None:
    """Apply index_failure_log DDL on a raw ``asyncpg.Connection`` (test path).

    See :func:`ensure_storage_outbox_asyncpg` for the rationale on the raw
    asyncpg surface.
    """
    ddl = _index_failure_log_ddl(schema)
    await conn.execute(ddl)
