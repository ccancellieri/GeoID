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

"""Full review-environment database reset via asyncpg (no psql required).

Reads reset policy from the sibling reset_policy.env file and DDL from
dynastore.modules.db_config.typed_store.ddl so there is a single source
of truth for both preserved-schema names and config table structure.

Modes:
  full    Drop all non-system schemas, wipe orphan cron jobs, recreate
          configs schema with current DDL.  Default.
  configs Drop and recreate only the configs schema.

Selective component reset (``--components``, mutually exclusive with ``--mode``):
  Comma-separated set drawn from ``{configs, iam, tasks, catalog, cron}``.
  Each component drops only the artefacts it owns, so tenant data can be
  preserved when only IAM or only tasks have shifted shape. See dynastore#295.

Usage:
  DATABASE_URL=postgresql://... python -m dynastore.scripts.db_reset \\
      [--mode full|configs | --components configs,iam,tasks,catalog,cron] \\
      [--dry-run]

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

# ── Component dispatch (dynastore#295) ─────────────────────────────────────────
# Each entry maps a ``--components`` token to the fixed-name PG schema(s) it
# owns. ``catalog`` is special — it targets the per-tenant ``s_<base36>``
# schemas enumerated at runtime, not a fixed name.
_COMPONENT_SCHEMAS: dict[str, tuple[str, ...]] = {
    "iam": ("iam",),
    "tasks": ("tasks",),
}
_TENANT_SCHEMA_PATTERN = r"^s_[0-9a-z]{8}$"  # mirrors tests/dynastore/test_utils/db_cleanup.TENANT_SCHEMA_PATTERN
KNOWN_COMPONENTS: frozenset[str] = frozenset({"configs", "iam", "tasks", "catalog", "cron"})


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

CREATE TABLE IF NOT EXISTS configs.platform_configs (
    ref_key     TEXT        PRIMARY KEY,
    class_key   TEXT        NOT NULL,
    schema_id   TEXT        NOT NULL,
    config_data JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_platform_configs_class_key
    ON configs.platform_configs (class_key);
"""


# Keep in sync with dynastore.modules.db_config.typed_store.ddl.LEADER_LEASE_DDL
def _get_lease_ddl() -> str:
    try:
        from dynastore.modules.db_config.typed_store.ddl import LEADER_LEASE_DDL
        return LEADER_LEASE_DDL
    except ImportError:
        pass
    return """
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


# Keep in sync with dynastore.modules.db_config.typed_store.ddl.INSTANCE_LIVENESS_DDL
def _get_instance_liveness_ddl() -> str:
    try:
        from dynastore.modules.db_config.typed_store.ddl import INSTANCE_LIVENESS_DDL
        return INSTANCE_LIVENESS_DDL
    except ImportError:
        pass
    return """
CREATE SCHEMA IF NOT EXISTS configs;
CREATE TABLE IF NOT EXISTS configs.instance_liveness (
    instance_id TEXT        PRIMARY KEY,
    service     TEXT        NOT NULL,
    renewed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
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
    platform_ddl = _get_platform_ddl()
    lease_ddl = _get_lease_ddl()
    instance_liveness_ddl = _get_instance_liveness_ddl()
    all_ddl_stmts = [
        s.strip() + ";"
        for ddl in (platform_ddl, lease_ddl, instance_liveness_ddl)
        for s in ddl.strip().split(";")
        if s.strip()
    ]
    stmts = ["DROP SCHEMA IF EXISTS configs CASCADE;"] + all_ddl_stmts
    if dry_run:
        _log("-- DRY RUN: configs schema statements --")
        for s in stmts:
            _log(s)
        return

    await conn.execute("DROP SCHEMA IF EXISTS configs CASCADE;")
    _log("configs schema dropped OK")
    for stmt in all_ddl_stmts:
        await conn.execute(stmt)
    _log("configs schema DDL recreated OK")


# Roles that own Cloud SQL / AlloyDB system objects living in `public`.
# Anything they own is preserved by `_clean_public_schema`.
_SYSTEM_RELATION_OWNERS = (
    "cloudsqlsuperuser", "cloudsqladmin", "cloudsqlagent",
    "cloudsqliamuser", "cloudsqliamserviceaccount", "cloudsqlimportexport",
    "cloudsqlreplica",
    "alloydbsuperuser", "alloydbadmin", "alloydbagent",
    "alloydbiamuser", "alloydbimportexport", "alloydbreplica",
    "postgres",
)

_RELKIND_TO_DROP = {
    "r": "TABLE",
    "p": "TABLE",            # partitioned table
    "v": "VIEW",
    "m": "MATERIALIZED VIEW",
    "S": "SEQUENCE",
    "f": "FOREIGN TABLE",
}


async def _list_public_app_relations(conn) -> list[tuple[str, str]]:
    """Return [(relkind, relname)] for objects in `public` that are
    app-created — i.e. not owned by a Cloud SQL/AlloyDB system role and not
    bound to any installed extension via pg_depend (deptype='e')."""
    rows = await conn.fetch(
        """
        SELECT c.relkind::text AS relkind, c.relname AS relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_roles r     ON r.oid = c.relowner
        WHERE n.nspname = 'public'
          AND c.relkind = ANY($1::char[])
          AND r.rolname != ALL($2::text[])
          AND NOT EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.deptype = 'e'
          )
        ORDER BY c.relkind, c.relname;
        """,
        list(_RELKIND_TO_DROP.keys()),
        list(_SYSTEM_RELATION_OWNERS),
    )
    return [(r["relkind"], r["relname"]) for r in rows]


async def _clean_public_schema(conn, dry_run: bool) -> None:
    """Drop app-created relations inside `public`, preserving extension-owned
    objects (PostGIS, pg_cron, AlloyDB columnar views, etc.) and anything
    owned by a Cloud SQL/AlloyDB system role."""
    targets = await _list_public_app_relations(conn)
    if not targets:
        _log("public: no app-owned relations to drop.")
        return

    if dry_run:
        _log(f"-- DRY RUN: public app relations ({len(targets)}) --")
        for kind, name in targets:
            _log(f'DROP {_RELKIND_TO_DROP[kind]} IF EXISTS public."{name}" CASCADE;')
        return

    dropped = 0
    for kind, name in targets:
        sql_kind = _RELKIND_TO_DROP[kind]
        try:
            await conn.execute(f'DROP {sql_kind} IF EXISTS public."{name}" CASCADE;')
            dropped += 1
        except Exception as e:
            _log(f"  skipped public.{name} ({sql_kind}): {e}")
    _log(f"public: dropped {dropped}/{len(targets)} app-owned relations")


async def _wipe_cron_jobs(conn, dry_run: bool) -> None:
    """Remove non-system rows from `cron.job` via ``cron.unschedule(jobid)``.

    A bare ``DELETE FROM cron.job`` silently no-ops on Cloud SQL / AlloyDB
    when the connecting role does not own the row — pg_cron applies row-level
    security on `cron.job`. ``cron.unschedule`` is the documented API and is
    SECURITY DEFINER, so it works as long as the caller can call the
    function (granted by default to PUBLIC for the job owner; we additionally
    tolerate per-row failures so partial cleanup still progresses)."""
    try:
        rows = await conn.fetch(
            "SELECT jobid, jobname FROM cron.job "
            "WHERE jobname != ALL($1::text[]) "
            "ORDER BY jobid;",
            list(SYSTEM_CRON_JOBS),
        )
    except Exception as e:
        _log(f"cron wipe skipped (pg_cron not available?): {e}")
        return

    if not rows:
        _log("cron.job: nothing to wipe.")
        return

    if dry_run:
        _log(f"-- DRY RUN: cron jobs to unschedule ({len(rows)}) --")
        for r in rows:
            _log(f"SELECT cron.unschedule({r['jobid']}); -- {r['jobname']}")
        return

    ok = 0
    for r in rows:
        try:
            await conn.execute("SELECT cron.unschedule($1::bigint);", r["jobid"])
            ok += 1
        except Exception as e:
            _log(f"  cron.unschedule({r['jobid']}, {r['jobname']!r}) failed: {e}")
    _log(f"cron jobs unscheduled: {ok}/{len(rows)}")


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
        await _clean_public_schema(conn, dry_run=True)
        await _wipe_cron_jobs(conn, dry_run=True)
        _log(_get_platform_ddl())
        return

    for schema in schemas:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
            _log(f"  dropped schema: {schema}")
        except Exception as e:
            _log(f"  skipped schema {schema!r}: {e}")

    await _clean_public_schema(conn, dry_run=False)
    await _wipe_cron_jobs(conn, dry_run=False)

    await conn.execute("CREATE SCHEMA IF NOT EXISTS keycloak;")

    ddl = _get_platform_ddl()
    for stmt in ddl.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            await conn.execute(stmt + ";")
    _log("configs schema DDL recreated OK")
    _log("Reset complete. Restart dynastore to rebuild schemas.")


# ── Selective component reset (dynastore#295) ──────────────────────────────────

async def _drop_named_schemas(
    conn, schemas: tuple[str, ...], dry_run: bool, label: str
) -> None:
    """Drop fixed-name schemas (the ``iam`` / ``tasks`` cases).

    Modules recreate their own schema via ``CREATE SCHEMA IF NOT EXISTS`` at
    startup — no DDL needs to be re-applied here. Idempotent under partial
    failure: each schema is dropped independently.
    """
    if dry_run:
        _log(f"-- DRY RUN: {label} component — drop schemas {list(schemas)} --")
        for s in schemas:
            _log(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;')
        return
    for s in schemas:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;')
            _log(f"  dropped schema: {s}")
        except Exception as e:
            _log(f"  skipped schema {s!r}: {e}")


async def _list_tenant_schemas(conn) -> list[str]:
    rows = await conn.fetch(
        "SELECT nspname FROM pg_namespace "
        "WHERE nspname ~ $1 "
        "ORDER BY nspname;",
        _TENANT_SCHEMA_PATTERN,
    )
    return [r["nspname"] for r in rows]


async def _drop_tenant_schemas(conn, dry_run: bool) -> None:
    """Drop per-tenant ``s_<base36>`` schemas — the destructive piece
    previously bundled in ``--mode full``. Configs, IAM, tasks schemas are
    NOT touched (they have fixed names not matching the tenant pattern)."""
    schemas = await _list_tenant_schemas(conn)
    if not schemas:
        _log("catalog: no tenant schemas to drop.")
        return
    if dry_run:
        _log(f"-- DRY RUN: catalog component — drop {len(schemas)} tenant schema(s) --")
        for s in schemas:
            _log(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;')
        return
    dropped = 0
    for s in schemas:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;')
            dropped += 1
        except Exception as e:
            _log(f"  skipped tenant {s!r}: {e}")
    _log(f"catalog: dropped {dropped}/{len(schemas)} tenant schemas")


def _parse_components(raw: str) -> tuple[str, ...]:
    """Split ``--components`` value, validate, preserve declared order."""
    seen: set[str] = set()
    out: list[str] = []
    for token in raw.split(","):
        c = token.strip().lower()
        if not c:
            continue
        if c not in KNOWN_COMPONENTS:
            raise SystemExit(
                f"ERROR: --components: unknown token {c!r}. Allowed: "
                f"{sorted(KNOWN_COMPONENTS)}"
            )
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    if not out:
        raise SystemExit(
            "ERROR: --components requires at least one of "
            f"{sorted(KNOWN_COMPONENTS)}"
        )
    return tuple(out)


async def _reset_components(
    conn, components: tuple[str, ...], dry_run: bool
) -> None:
    """Dispatch each requested component in a fixed order independent of the
    caller's CLI ordering. ``cron`` last so any module that re-registers a
    cron job during a same-process reset is still wiped after."""
    _log(f"Components selected: {list(components)}")
    order = ("configs", "iam", "tasks", "catalog", "cron")
    requested = set(components)
    for c in order:
        if c not in requested:
            continue
        _log(f"-- component: {c} --")
        if c == "configs":
            await _reset_configs(conn, dry_run)
        elif c in _COMPONENT_SCHEMAS:
            await _drop_named_schemas(conn, _COMPONENT_SCHEMAS[c], dry_run, c)
        elif c == "catalog":
            await _drop_tenant_schemas(conn, dry_run)
        elif c == "cron":
            await _wipe_cron_jobs(conn, dry_run)
    if not dry_run:
        _log("Selective reset complete. Restart dynastore to rebuild schemas.")


# ── Entry point ────────────────────────────────────────────────────────────────

async def _run(
    url: str,
    mode: str,
    dry_run: bool,
    components: tuple[str, ...] | None = None,
) -> None:
    import asyncpg
    kwargs = _parse_url(url)
    conn: asyncpg.Connection = await asyncpg.connect(**kwargs)
    try:
        if components:
            await _reset_components(conn, components, dry_run)
        elif mode == "configs":
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
    parser.add_argument(
        "--components",
        default="",
        help=(
            "Comma-separated subset of "
            f"{sorted(KNOWN_COMPONENTS)}. When set, --mode is ignored. "
            "Example: --components configs,iam"
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    components: tuple[str, ...] | None = None
    if args.components:
        components = _parse_components(args.components)

    if not args.dry_run:
        _refuse_in_production()

    url = _resolve_database_url()
    if not url:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr, flush=True)
        sys.exit(1)

    asyncio.run(_run(url, args.mode, args.dry_run, components))
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
