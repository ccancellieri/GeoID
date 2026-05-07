"""Full review-environment database reset via asyncpg (no psql required).

Reads reset policy from the sibling reset_policy.env file and DDL from
dynastore.modules.db_config.typed_store.ddl so there is a single source
of truth for both preserved-schema names and config table structure.

Modes:
  full    Drop all non-system schemas, wipe orphan cron jobs, recreate
          configs schema with current DDL.  Default.
  configs Drop and recreate only the configs schema.

Usage:
  DATABASE_URL=postgresql://... python -m dynastore.scripts.db_reset \\
      [--mode full|configs] [--dry-run]

Connection resolution (first match wins):
  1. DATABASE_URL env var
  2. /dynastore/env/.env (Secret Manager mount used by Cloud Run jobs)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shlex
import sys
from pathlib import Path

# ── Constants — duplicated by design ───────────────────────────────────────────
# This script is the ONE place destructive cleanup logic ships in the
# production wheel. The same constants exist in
# ``tests/dynastore/test_utils/db_cleanup.py`` (test-only, never in the
# production wheel); ``tests/.../test_db_cleanup_drift.py`` pins the two
# definitions byte-for-byte so they cannot diverge silently.
#
# Why duplicate instead of import? Importing from ``tests/`` is impossible
# at runtime (tests/ isn't packaged); importing the test-utils path into
# any production module would require shipping it in the wheel, which
# defeats the purpose of the separation.

_DEFAULT_PRESERVED_SCHEMAS = frozenset({
    "pg_catalog",
    "information_schema",
    "pg_toast",
    "cron",
    "public",
    "keycloak",
    # Cloud SQL ML extension schemas — owned by the cloud SA, not the app user.
    "ai",
    "google_ml",
    # PostGIS topology extension.
    "topology",
})

_DEFAULT_SYSTEM_CRON_JOBS = (
    "system_cleanup_orphaned_cron_jobs",
    "monthly_cleanup_system_logs",
)


def _load_reset_policy(
    policy_file: Path,
) -> tuple[frozenset[str], tuple[str, ...]]:
    """Read ``reset_policy.env`` overrides for preserved schemas and
    system cron jobs.  Returns the overridden values when the file
    declares them, falling back to the canonical defaults otherwise.

    File format is shell-style ``KEY=value``; lines starting with ``#``
    and blank lines are ignored; values are parsed with :mod:`shlex`.
    """
    preserved: frozenset[str] = _DEFAULT_PRESERVED_SCHEMAS
    system_cron: tuple[str, ...] = _DEFAULT_SYSTEM_CRON_JOBS

    if not policy_file.exists():
        return preserved, system_cron

    for raw in policy_file.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key == "PRESERVED_SCHEMAS":
            preserved = frozenset(shlex.split(val))
        elif key == "SYSTEM_CRON_JOBS":
            system_cron = tuple(shlex.split(val))

    return preserved, system_cron


PRESERVED_SCHEMAS, SYSTEM_CRON_JOBS = _load_reset_policy(
    Path(__file__).parent / "reset_policy.env"
)


# ── DDL ────────────────────────────────────────────────────────────────────────
# Keep in sync with dynastore.modules.db_config.typed_store.ddl.PLATFORM_SCHEMAS_DDL

def _get_platform_ddl() -> str:
    try:
        from dynastore.modules.db_config.typed_store.ddl import PLATFORM_SCHEMAS_DDL
        return PLATFORM_SCHEMAS_DDL
    except ImportError:
        pass
    return """
CREATE SCHEMA IF NOT EXISTS configs;

CREATE TABLE IF NOT EXISTS configs.schemas (
    schema_id    TEXT        PRIMARY KEY,
    class_key    TEXT        NOT NULL,
    schema_json  JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by   TEXT
);

CREATE INDEX IF NOT EXISTS ix_schemas_class_key
    ON configs.schemas (class_key);

CREATE TABLE IF NOT EXISTS configs.platform_configs (
    ref_key     TEXT        PRIMARY KEY,
    class_key   TEXT        NOT NULL,
    schema_id   TEXT        NOT NULL REFERENCES configs.schemas(schema_id),
    config_data JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_platform_configs_class_key
    ON configs.platform_configs (class_key);
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_url(url: str) -> dict:
    from urllib.parse import urlparse, unquote
    p = urlparse(url)
    kwargs: dict = {
        "host": p.hostname,
        "port": p.port or 5432,
        "user": unquote(p.username or ""),
        "password": unquote(p.password or ""),
        "database": (p.path or "/").lstrip("/"),
    }
    for part in (p.query or "").split("&"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k == "ssl":
            kwargs["ssl"] = v
    return kwargs


def _log(msg: str) -> None:
    print(msg, flush=True)


# ── Reset modes ────────────────────────────────────────────────────────────────

async def _reset_configs(conn, dry_run: bool) -> None:
    ddl = _get_platform_ddl()
    stmts = ["DROP SCHEMA IF EXISTS configs CASCADE;"] + [
        s.strip() + ";" for s in ddl.strip().split(";") if s.strip()
    ]
    if dry_run:
        _log("-- DRY RUN: configs schema statements --")
        for s in stmts:
            _log(s)
        return

    await conn.execute("DROP SCHEMA IF EXISTS configs CASCADE;")
    _log("configs schema dropped OK")
    for stmt in ddl.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            await conn.execute(stmt + ";")
    _log("configs schema DDL recreated OK")


async def _reset_full(conn, dry_run: bool) -> None:
    rows = await conn.fetch(
        "SELECT nspname FROM pg_namespace "
        "WHERE nspname != ALL($1::text[]) "
        "  AND nspname NOT LIKE 'pg_temp_%' "
        "  AND nspname NOT LIKE 'pg_toast_temp_%' "
        "ORDER BY nspname;",
        list(PRESERVED_SCHEMAS),
    )
    schemas = [r["nspname"] for r in rows]

    if not schemas:
        _log("No user schemas to drop.")
    else:
        _log(f"Schemas to drop ({len(schemas)}): {', '.join(schemas)}")

    if dry_run:
        _log("-- DRY RUN: no changes applied --")
        for s in schemas:
            _log(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;')
        _log(f"DELETE FROM cron.job WHERE jobname != ALL(ARRAY{list(SYSTEM_CRON_JOBS)});")
        _log(_get_platform_ddl())
        return

    for schema in schemas:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
            _log(f"  dropped schema: {schema}")
        except Exception as e:
            _log(f"  skipped schema {schema!r}: {e}")

    try:
        del_result = await conn.execute(
            "DELETE FROM cron.job WHERE jobname != ALL($1::text[]);",
            list(SYSTEM_CRON_JOBS),
        )
        _log(f"cron jobs wiped: {del_result}")
    except Exception as e:
        _log(f"cron wipe skipped (pg_cron not available?): {e}")

    await conn.execute("CREATE SCHEMA IF NOT EXISTS keycloak;")

    ddl = _get_platform_ddl()
    for stmt in ddl.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            await conn.execute(stmt + ";")
    _log("configs schema DDL recreated OK")
    _log("Reset complete. Restart dynastore to rebuild schemas.")


# ── Entry point ────────────────────────────────────────────────────────────────

async def _run(url: str, mode: str, dry_run: bool) -> None:
    import asyncpg
    kwargs = _parse_url(url)
    conn: asyncpg.Connection = await asyncpg.connect(**kwargs)
    try:
        if mode == "configs":
            await _reset_configs(conn, dry_run)
        else:
            await _reset_full(conn, dry_run)
    finally:
        await conn.close()


def _resolve_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    env_file = "/dynastore/env/.env"
    if os.path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("DATABASE_URL=") or line.startswith("export DATABASE_URL="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


_PRODUCTION_ENV_NAMES = frozenset({"prod", "production"})


def _refuse_in_production() -> None:
    """Defense-in-depth: refuse to run when the deployment env identifies
    itself as production. The Reset DB Cloud Run job is meant for review
    only; the workflow file enforces ``environment: review`` at the
    GitHub-Actions layer, but this guard runs INSIDE the container so
    even a manual ``gcloud run jobs execute`` invocation against a
    prod-tier deployment cannot accidentally drop schemas.

    The check honours two env vars in order:
      1. ``DYNASTORE_RESET_ALLOW_PRODUCTION=1`` — explicit opt-out for
         truly intentional production resets (must be set per invocation).
      2. ``DYNASTORE_ENV`` / ``ENVIRONMENT`` (case-insensitive) — when
         either equals ``prod`` or ``production``, the run is refused
         unless (1) is also set.
    """
    if os.environ.get("DYNASTORE_RESET_ALLOW_PRODUCTION") == "1":
        return
    env_label = (
        os.environ.get("DYNASTORE_ENV")
        or os.environ.get("ENVIRONMENT")
        or ""
    ).strip().lower()
    if env_label in _PRODUCTION_ENV_NAMES:
        print(
            f"REFUSED: db_reset detected DYNASTORE_ENV/ENVIRONMENT={env_label!r} "
            "(production tier). Set DYNASTORE_RESET_ALLOW_PRODUCTION=1 if this "
            "is genuinely intended.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["full", "configs"], default="full")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run:
        _refuse_in_production()

    url = _resolve_database_url()
    if not url:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr, flush=True)
        sys.exit(1)

    asyncio.run(_run(url, args.mode, args.dry_run))
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
