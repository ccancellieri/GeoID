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

"""Operator tool to detect and tear down phantom catalogs, or all catalogs.

A phantom catalog is a row in ``catalog.catalogs`` whose ``external_id``
accidentally received an internal-id-shaped value (``c_<13 base32>``).
These rows were created by a now-fixed bug (the external-first
``resolve_catalog_id`` path that allowed an internal id to be used as a
lookup target).

Each phantom owns real GCP resources: a dedicated Postgres schema, a GCS
bucket, and a Pub/Sub topic + subscription.  They must be cleaned up at
the data layer, keyed on the internal ``id`` PK.

``--all`` mode
--------------
Pass ``--all`` to select ALL non-deleted catalogs (not just phantoms) and
tear them all down.  This is intended for DEV database resets.  A hard
environment guard refuses to run unless ``DYNASTORE_ENV`` (or ``ENVIRONMENT``)
is explicitly set to one of ``{dev, development, review}``.  Empty, unknown,
and production labels are always refused — there is no override flag.

Module stack
------------
When ``--execute`` is set without ``--allow-gcp-skip`` the script boots the
full module stack (StorageProtocol + EventingProtocol) so that GCP teardown
actually runs rather than being silently skipped.  The bootstrap is skipped
in dry-run mode for speed.

HARD SAFETY RULES
-----------------
* This tool keys on the internal ``id`` PK only — never on REST-accessible
  external ids.  The phantom's ``external_id`` collides with a real
  catalog's ``id``; routing teardown via that external id would destroy the
  real catalog.
* Detection via ``is_internal_physical_name`` is the principled phantom
  signature: a real catalog created through the API could never have an
  internal-shaped ``external_id`` (the guard rejects it at create time).
* Dry-run is the default.  Pass ``--execute`` to actually mutate anything.

Connection resolution (first match wins):
  1. DATABASE_URL env var
  2. /dynastore/env/.env (Secret Manager mount used by Cloud Run jobs)

Usage::

    DATABASE_URL=postgresql://... python -m dynastore.scripts.gc_phantom_catalogs \\
        [--ids id1 id2 ...] \\
        [--ids-file /path/to/ids.txt] \\
        [--execute]

    # Tear down ALL catalogs (DEV only):
    DATABASE_URL=postgresql://... DYNASTORE_ENV=dev \\
        python -m dynastore.scripts.gc_phantom_catalogs --all --execute
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Sequence

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phantom detection predicate
# ---------------------------------------------------------------------------

# Mirrors ``catalog_service._INTERNAL_NAME_SUFFIX`` — kept local so this
# script has no import dependency on the catalog module at detection time.
_INTERNAL_ID_PATTERN = re.compile(r"^c_[2-9a-x]{13}$")


def is_phantom_external_id(external_id: str) -> bool:
    """Return True when ``external_id`` matches the internal catalog-id shape.

    Real catalogs are rejected at create time when their ``external_id`` has
    this shape, so a matching row is definitively a phantom created by the bug.

    Shape: ``c_`` prefix + exactly 13 characters from the base32 alphabet
    ``[2-9a-x]`` (RFC 4648 without the ambiguous 0/1 symbols).
    """
    return bool(_INTERNAL_ID_PATTERN.match(external_id))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PhantomCatalog:
    """Metadata for one detected phantom (or targeted) catalog row."""

    internal_id: str          # PK of catalog.catalogs (= PG schema name)
    external_id: str          # the internal-shaped value used as external_id
    provisioning_status: str
    bucket_name: Optional[str]  # None when provisioning never completed


# ---------------------------------------------------------------------------
# Database helpers (asyncpg-based, no SQLAlchemy; mirrors db_reset.py)
# ---------------------------------------------------------------------------

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


async def _detect_phantoms(conn) -> List[PhantomCatalog]:
    """Select all non-deleted catalogs whose external_id matches the internal-id
    shape, then resolve each phantom's bucket_name from its own schema's
    catalog_configs table (where provisioning wrote it).
    """
    rows = await conn.fetch(
        """
        SELECT id, external_id, provisioning_status
        FROM catalog.catalogs
        WHERE deleted_at IS NULL
          AND external_id ~ $1
        ORDER BY id;
        """,
        r"^c_[2-9a-x]{13}$",
    )

    results: List[PhantomCatalog] = []
    for row in rows:
        internal_id = row["id"]
        bucket_name = await _resolve_bucket_name(conn, internal_id)
        results.append(PhantomCatalog(
            internal_id=internal_id,
            external_id=row["external_id"],
            provisioning_status=row["provisioning_status"],
            bucket_name=bucket_name,
        ))
    return results


async def _detect_all_catalogs(conn) -> List[PhantomCatalog]:
    """Select ALL non-deleted catalogs for bulk teardown (``--all`` mode).

    Returns the same ``PhantomCatalog`` dataclass as ``_detect_phantoms`` so
    the teardown loop is identical for both modes.
    """
    rows = await conn.fetch(
        """
        SELECT id, external_id, provisioning_status
        FROM catalog.catalogs
        WHERE deleted_at IS NULL
        ORDER BY id;
        """,
    )

    results: List[PhantomCatalog] = []
    for row in rows:
        internal_id = row["id"]
        bucket_name = await _resolve_bucket_name(conn, internal_id)
        results.append(PhantomCatalog(
            internal_id=internal_id,
            external_id=row["external_id"],
            provisioning_status=row["provisioning_status"],
            bucket_name=bucket_name,
        ))
    return results


async def _fetch_phantoms_by_ids(conn, ids: Sequence[str]) -> List[PhantomCatalog]:
    """Resolve a caller-supplied list of internal IDs into PhantomCatalog objects.

    Each id is validated to be internal-shaped before the DB lookup, so the
    tool cannot accidentally operate on a real catalog's external id.
    """
    results: List[PhantomCatalog] = []
    for internal_id in ids:
        if not is_phantom_external_id(internal_id):
            _log(f"  SKIP {internal_id!r}: not an internal-id shape — refusing to touch it")
            continue
        row = await conn.fetchrow(
            "SELECT id, external_id, provisioning_status "
            "FROM catalog.catalogs WHERE id = $1;",
            internal_id,
        )
        if row is None:
            _log(f"  SKIP {internal_id!r}: not found in catalog.catalogs (already deleted?)")
            continue
        bucket_name = await _resolve_bucket_name(conn, internal_id)
        results.append(PhantomCatalog(
            internal_id=row["id"],
            external_id=row["external_id"],
            provisioning_status=row["provisioning_status"],
            bucket_name=bucket_name,
        ))
    return results


async def _resolve_bucket_name(conn, internal_id: str) -> Optional[str]:
    """Read bucket_name from the phantom's own catalog_configs table.

    The bucket_name is stored as ``config_data->>'bucket_name'`` in the
    ``catalog_configs`` row whose ``class_key = 'gcp_catalog_bucket_config'``.
    The per-tenant table lives in the schema named after ``internal_id``.
    Returns None when the schema or config row does not exist (e.g. the
    phantom's provisioning never completed that step).
    """
    # Check the schema exists before querying — otherwise asyncpg raises a
    # PG error that aborts the current transaction.
    schema_exists = await conn.fetchval(
        "SELECT 1 FROM pg_namespace WHERE nspname = $1;",
        internal_id,
    )
    if not schema_exists:
        return None

    row = await conn.fetchrow(
        f'SELECT config_data FROM "{internal_id}".catalog_configs '
        "WHERE class_key = $1 LIMIT 1;",
        "gcp_catalog_bucket_config",
    )
    if row is None:
        return None
    data = row["config_data"]
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return None
    if isinstance(data, dict):
        return data.get("bucket_name")
    return None


# ---------------------------------------------------------------------------
# GCP teardown helper — shared by this script and consumable by the durable
# task without pulling in the full script module.
# ---------------------------------------------------------------------------

async def teardown_phantom_gcp_resources(
    catalog_id: str,
    bucket_name: Optional[str],
) -> dict:
    """Tear down GCP resources for one phantom catalog.

    Directly reuses the ``_cleanup_catalog`` logic from
    ``GcpCatalogCleanupTask`` without going through the durable-task
    dispatcher (the phantom's schema cannot safely host task rows because it
    is about to be dropped).

    This function is intentionally side-effect-free when GCP protocols are
    not registered (the modules may not be initialised in this operator
    context), in which case it returns ``{"status": "skipped_no_protocols"}``.
    Callers that need full GCP teardown must ensure the GCP module is
    initialised before calling (see the script's ``_run`` function).
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import StorageProtocol, EventingProtocol

    storage = get_protocol(StorageProtocol)
    eventing = get_protocol(EventingProtocol)

    if not storage and not eventing:
        logger.info(
            "teardown_phantom_gcp_resources: no GCP protocols registered for '%s'; "
            "skipping GCP teardown (bucket=%s).",
            catalog_id, bucket_name,
        )
        return {"catalog_id": catalog_id, "status": "skipped_no_protocols"}

    # Eventing teardown
    if eventing:
        from dynastore.modules import get_protocol as _gp
        from dynastore.models.protocols import ConfigsProtocol
        configs = _gp(ConfigsProtocol)
        try:
            if configs:
                from dynastore.modules.gcp.gcp_config import GcpEventingConfig
                eventing_config = await configs.get_config(GcpEventingConfig, catalog_id)
                await eventing.teardown_catalog_eventing(catalog_id, config=eventing_config)
            else:
                await eventing.teardown_catalog_eventing(catalog_id, config=None)
        except Exception as exc:
            logger.warning(
                "teardown_phantom_gcp_resources: eventing teardown for '%s' failed "
                "(may already be gone): %s", catalog_id, exc,
            )
            try:
                await eventing.teardown_catalog_eventing(catalog_id, config=None)
            except Exception as force_exc:
                logger.warning(
                    "teardown_phantom_gcp_resources: force eventing teardown for '%s' "
                    "also failed: %s", catalog_id, force_exc,
                )

    # Storage teardown
    if storage:
        resolved_bucket = bucket_name
        if not resolved_bucket:
            resolved_bucket = await storage.get_storage_identifier(catalog_id)
        if resolved_bucket:
            logger.info(
                "teardown_phantom_gcp_resources: deleting bucket '%s' for '%s'.",
                resolved_bucket, catalog_id,
            )
            try:
                from dynastore.modules.gcp.tools import bucket as bucket_tool
                await bucket_tool.delete_bucket(resolved_bucket, force=True, client=None)
            except Exception as exc:
                logger.warning(
                    "teardown_phantom_gcp_resources: failed to delete bucket '%s': %s",
                    resolved_bucket, exc,
                )
                raise
        else:
            logger.info(
                "teardown_phantom_gcp_resources: no bucket found for '%s'; skipping.",
                catalog_id,
            )

    return {"catalog_id": catalog_id, "status": "cleaned"}


# ---------------------------------------------------------------------------
# Per-phantom teardown (includes PG schema drop and row delete)
# ---------------------------------------------------------------------------

async def _teardown_phantom(
    conn,
    phantom: PhantomCatalog,
    *,
    allow_gcp_skip: bool = False,
) -> None:
    """Execute teardown for one phantom: GCP resources, schema drop, row delete.

    When GCP protocols are not registered, ``teardown_phantom_gcp_resources``
    returns ``{"status": "skipped_no_protocols"}``.  By default this is treated
    as an error: the schema and catalog row are NOT dropped so the phantom stays
    discoverable for a subsequent run inside a GCP-initialised context.

    Pass ``allow_gcp_skip=True`` only when GCP resources were already cleaned
    up manually and the operator wants the script to proceed with the PG cleanup
    regardless.  A prominent WARNING is emitted in that case.
    """
    _log(
        f"  Tearing down phantom {phantom.internal_id!r} "
        f"(bucket={phantom.bucket_name or 'unknown'}) …"
    )

    # 1. GCP resources (bucket + pubsub)
    result = await teardown_phantom_gcp_resources(phantom.internal_id, phantom.bucket_name)
    if result.get("status") == "skipped_no_protocols":
        if not allow_gcp_skip:
            raise RuntimeError(
                f"GCP teardown for {phantom.internal_id!r} was skipped because no "
                "StorageProtocol / EventingProtocol is registered in this process. "
                "The schema and catalog row have NOT been dropped to avoid orphaning "
                "the GCS bucket and Pub/Sub resources. "
                "Re-run inside a GCP-initialised context (e.g. a Cloud Run Job that "
                "boots the module stack), or pass --allow-gcp-skip if you have already "
                "deleted the GCP resources manually."
            )
        logger.warning(
            "WARNING: GCP teardown skipped for %r (--allow-gcp-skip set). "
            "The GCS bucket and Pub/Sub resources for this phantom were NOT deleted "
            "by this run. Proceeding with schema drop and row delete.",
            phantom.internal_id,
        )
        _log(
            f"  WARNING: GCP teardown waived for {phantom.internal_id!r} "
            f"(--allow-gcp-skip).  Bucket/topics NOT deleted by this run."
        )

    # 2. DROP SCHEMA (idempotent: IF EXISTS)
    _log(f"    DROP SCHEMA IF EXISTS \"{phantom.internal_id}\" CASCADE")
    await conn.execute(f'DROP SCHEMA IF EXISTS "{phantom.internal_id}" CASCADE;')

    # 3. Hard-delete the catalog row by internal PK
    _log(f"    DELETE FROM catalog.catalogs WHERE id = '{phantom.internal_id}'")
    await conn.execute(
        "DELETE FROM catalog.catalogs WHERE id = $1;",
        phantom.internal_id,
    )

    _log(f"  Done: {phantom.internal_id!r}")


# ---------------------------------------------------------------------------
# DEV-only guard for --all
# ---------------------------------------------------------------------------

# Environment labels that identify a non-production (dev/review) tier.
# Mirrors the idiom in db_reset._refuse_in_production but inverts the logic:
# --all requires an explicit dev label rather than merely blocking prod.
_DEV_ENV_NAMES = frozenset({"dev", "development", "review"})


def _require_dev_env() -> None:
    """Refuse to run when the environment is not an explicit dev/review tier.

    ``--all`` deletes every non-deleted catalog and its GCP resources.  This
    guard requires ``DYNASTORE_ENV`` or ``ENVIRONMENT`` (checked in that order,
    case-insensitive) to be one of ``{dev, development, review}``.

    An empty, unknown, or production label is always refused.  There is no
    override env var — ``--all`` must not run outside a dev environment.
    """
    env_label = (
        os.environ.get("DYNASTORE_ENV")
        or os.environ.get("ENVIRONMENT")
        or ""
    ).strip().lower()
    if env_label not in _DEV_ENV_NAMES:
        print(
            f"REFUSED: --all requires DYNASTORE_ENV or ENVIRONMENT to be one of "
            f"{sorted(_DEV_ENV_NAMES)}, got {env_label!r}. "
            "This guard has no override — --all must not run in non-dev environments.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(2)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    print(msg, flush=True)


def _print_phantom(p: PhantomCatalog) -> None:
    _log(
        f"  internal_id={p.internal_id!r}  "
        f"external_id={p.external_id!r}  "
        f"status={p.provisioning_status!r}  "
        f"bucket={p.bucket_name or '(none)'}"
    )


async def _run(
    url: str,
    *,
    execute: bool,
    allow_gcp_skip: bool,
    ids: Optional[List[str]],
    all_catalogs: bool = False,
) -> int:
    import asyncpg

    kwargs = _parse_url(url)

    # Boot the full module stack when executing with real GCP teardown so that
    # StorageProtocol / EventingProtocol are live inside teardown_phantom_gcp_resources.
    # Dry-run skips the boot for speed; --allow-gcp-skip implies GCP resources
    # were already cleaned manually so the protocols are not needed.
    # The boot is deferred until after target detection so that short-circuits
    # (empty id list, no matching rows) avoid an unnecessary stack startup.
    need_lifespan = execute and not allow_gcp_skip

    async def _execute_teardown_loop(conn, targets, mode_label) -> int:
        """Run the actual GCP + schema + row teardown for a non-empty target list."""
        _log(f"\nExecuting teardown for {len(targets)} {mode_label}(s) …")
        ok = 0
        failed = 0
        for p in targets:
            try:
                await _teardown_phantom(conn, p, allow_gcp_skip=allow_gcp_skip)
                ok += 1
            except Exception as exc:
                _log(f"  ERROR tearing down {p.internal_id!r}: {exc}")
                logger.exception("Teardown failed for %s", p.internal_id)
                failed += 1
        _log(f"\nSummary: {ok} torn down, {failed} failed out of {len(targets)} total.")
        return 0 if failed == 0 else 1

    conn: asyncpg.Connection = await asyncpg.connect(**kwargs)
    try:
        if all_catalogs:
            targets = await _detect_all_catalogs(conn)
            mode_label = "catalog"
        elif ids:
            targets = await _fetch_phantoms_by_ids(conn, ids)
            mode_label = "phantom catalog"
        else:
            targets = await _detect_phantoms(conn)
            mode_label = "phantom catalog"

        if not targets:
            _log(f"No {mode_label}s found.")
            return 0

        _log(f"\n{mode_label.capitalize()}s detected ({len(targets)}):")
        for p in targets:
            _print_phantom(p)

        if not execute:
            _log(
                f"\nDRY RUN — no changes applied.  "
                f"Actions that WOULD be taken per {mode_label}:"
            )
            for p in targets:
                _log(f"\n  [{p.internal_id}]")
                _log(f"    teardown_catalog_eventing('{p.internal_id}')")
                if p.bucket_name:
                    _log(f"    delete_bucket('{p.bucket_name}', force=True)")
                else:
                    _log("    (bucket unknown — would attempt live lookup via StorageProtocol)")
                _log(f"    DROP SCHEMA IF EXISTS \"{p.internal_id}\" CASCADE;")
                _log(f"    DELETE FROM catalog.catalogs WHERE id = '{p.internal_id}';")
            _log(
                "\nNOTE: --execute will refuse to drop schema/row if GCP protocols are "
                "not registered in this process (to avoid orphaning buckets/topics). "
                "Run inside a GCP-initialised context, or pass --allow-gcp-skip if "
                "GCP resources were already cleaned up manually."
            )
            _log("\nPass --execute to perform the teardown.")
            return 0

        # --- Execute mode: boot the module stack if GCP teardown is needed ---
        if need_lifespan:
            from types import SimpleNamespace
            from dynastore.tasks.bootstrap import bootstrap_task_env
            from dynastore.modules import lifespan as modules_lifespan

            app_state = SimpleNamespace()
            # Prevent background dispatcher loops from starting in this one-shot
            # operator context (mirrors main_task.py's ephemeral_job pattern).
            app_state.ephemeral_job = True
            bootstrap_task_env(app_state)
            async with modules_lifespan(app_state):
                return await _execute_teardown_loop(conn, targets, mode_label)
        else:
            return await _execute_teardown_loop(conn, targets, mode_label)

    finally:
        await conn.close()


def _load_ids_file(path: str) -> List[str]:
    ids = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                ids.append(line)
    return ids


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help=(
            "Actually perform teardown and deletion. "
            "Without this flag the script prints what it WOULD do and exits."
        ),
    )
    parser.add_argument(
        "--allow-gcp-skip",
        action="store_true",
        default=False,
        dest="allow_gcp_skip",
        help=(
            "Allow the schema drop and row delete to proceed even when GCP protocols "
            "(StorageProtocol / EventingProtocol) are not registered in this process. "
            "Use ONLY when GCP resources (bucket, Pub/Sub topic/subscription) have "
            "already been deleted manually. Without this flag, the script refuses to "
            "drop the schema or row when GCP teardown is skipped, preserving the row "
            "as a discovery handle for a later GCP-aware run."
        ),
    )
    id_group = parser.add_mutually_exclusive_group()
    id_group.add_argument(
        "--all",
        action="store_true",
        default=False,
        dest="all_catalogs",
        help=(
            "Select ALL non-deleted catalogs (not just phantoms) and tear them all "
            "down. Intended for DEV database resets. "
            "REQUIRES DYNASTORE_ENV or ENVIRONMENT to be one of "
            "{dev, development, review} — refused otherwise with no override. "
            "Mutually exclusive with --ids and --ids-file."
        ),
    )
    id_group.add_argument(
        "--ids",
        nargs="+",
        metavar="INTERNAL_ID",
        help=(
            "Explicit list of internal catalog IDs (c_<13 base32>) to target. "
            "Each must match the internal-id shape or it is skipped. "
            "Overrides auto-detection."
        ),
    )
    id_group.add_argument(
        "--ids-file",
        metavar="FILE",
        help=(
            "Path to a text file of internal catalog IDs, one per line "
            "(lines starting with # are ignored). Mutually exclusive with --ids."
        ),
    )
    args = parser.parse_args()

    # Hard dev-only guard: must be checked before any DB work.
    if args.all_catalogs:
        _require_dev_env()

    ids: Optional[List[str]] = None
    if args.ids:
        ids = args.ids
    elif args.ids_file:
        ids = _load_ids_file(args.ids_file)

    url = _resolve_database_url()
    if not url:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr, flush=True)
        sys.exit(1)

    rc = asyncio.run(
        _run(
            url,
            execute=args.execute,
            allow_gcp_skip=args.allow_gcp_skip,
            ids=ids,
            all_catalogs=args.all_catalogs,
        )
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
