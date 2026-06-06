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

import os
import sys

if "DYNASTORE_ENV" not in os.environ:
    os.environ["DYNASTORE_ENV"] = "test"

# Fallback for local testing: point to localhost instead of docker hostname 'db'
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = (
        "postgresql://testuser:testpassword@localhost:54320/gis_dev"
    )


def _worker_db_url(base_url: str) -> str:
    """
    Per-xdist-worker DB URL: ``gis_dev`` -> ``gis_dev_<worker_id>``.

    Returns base_url unchanged outside xdist. Each xdist worker uses its own
    Postgres database (cloned from the master at session start) so that DDL
    operations (CREATE/DROP SCHEMA, ALTER TABLE) on one worker cannot block
    queries on another. Eliminates the cross-worker pg_namespace lock
    contention that caused 180s pytest-timeout cascades on CI.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if not worker:
        return base_url
    from urllib.parse import urlparse, urlunparse

    p = urlparse(base_url)
    base_db = p.path.lstrip("/")
    if not base_db:
        return base_url
    return urlunparse(p._replace(path=f"/{base_db}_{worker}"))


# Apply worker-suffix at module import so any early reader (e.g. cleanup_db
# capturing DATABASE_URL at import) sees the per-worker DB.
os.environ["DATABASE_URL"] = _worker_db_url(os.environ["DATABASE_URL"])

import json
from tests.dynastore.test_utils import generate_test_id
import pytest
import pytest_asyncio
from typing import Any, Union


async def _create_db_template_with_retry(
    execute, source_db: str, worker_db: str, *, attempts: int = 5, backoff: float = 0.2
) -> None:
    """Run pg_terminate_backend + CREATE DATABASE TEMPLATE in a bounded retry.

    pg_terminate_backend is one-shot — any external client (dev stack, pgAdmin,
    idle sessions) that reconnects between the terminate and the CREATE will
    surface as asyncpg.ObjectInUseError. The retry loop re-terminates and
    re-attempts up to ``attempts`` times. If external clients are persistently
    connected the loop exhausts and raises a RuntimeError pointing the operator
    at the resolution path (clear-db workflow + stop the local/review stack).

    Terminate is strictly scoped by ``datname = source_db`` (the master
    ``gis_dev``) — sibling worker per-DBs (``gis_dev_gw*``) and any database
    not matching that name are never touched.  This is the load-bearing guard
    behind the per-worker DB drop race (#1027) — coupled with the file lock
    in ``_ensure_worker_db`` that prevents two workers from terminating +
    creating concurrently.
    """
    import asyncio
    import asyncpg

    last_exc: Exception | None = None
    for _ in range(attempts):
        # Strictly scoped to the source DB only — sibling workers' per-worker
        # DBs (``gis_dev_gw*``) match a DIFFERENT ``datname`` and are left
        # untouched.  ``pid <> pg_backend_pid()`` keeps us from killing our
        # own admin connection (which holds the advisory + file lock).
        await execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            source_db,
        )
        try:
            await execute(f'CREATE DATABASE "{worker_db}" TEMPLATE "{source_db}"')
            return
        except asyncpg.exceptions.ObjectInUseError as exc:
            last_exc = exc
            await asyncio.sleep(backoff)
    raise RuntimeError(
        f"Failed to clone {source_db} -> {worker_db} after {attempts} attempts; "
        f"last error: {last_exc}. External clients are connected to {source_db} "
        f"(check pg_stat_activity). Resolution: stop the dev stack (docker compose "
        f"down on the geoid app container) and/or reset the source DB before "
        f"re-running the test session — local: tests/dynastore/test_utils/cleanup_db.py "
        f"(application-aware Python cleanup), review env: "
        f"packages/core/src/dynastore/scripts/db_reset.sh reset --yes (boot-tier wipe)."
    )


async def _ensure_worker_db():
    """
    Create the per-xdist-worker DB by cloning the master ``gis_dev`` TEMPLATE.

    Serialized across workers via TWO layers (#1027):

    1. **OS-level file lock** acquired BEFORE opening any DB connection.  Without
       this, every worker simultaneously connects to ``/postgres`` to grab the
       Postgres advisory lock — under ``-n auto`` that's 12-16 concurrent admin
       connections and the ensuing terminate-backend storms (one per worker)
       inside ``_create_db_template_with_retry`` could collide with another
       worker's in-flight CREATE DATABASE TEMPLATE, leaving its target DB in a
       half-dropped state ("database gis_dev_gwN does not exist" cascades).
       The file lock ensures only ONE worker is inside this critical section
       at a time, so terminate-storms can never overlap with a peer's CREATE.

    2. **Postgres advisory lock** retained as belt-and-braces in case multiple
       pytest invocations run against the same Postgres on the same host (the
       file lock is per-host-tempdir, the advisory lock is per-cluster).

    External clients (dev stack, pgAdmin, leftover sessions) connected to the
    source DB are best-effort terminated (see ``_create_db_template_with_retry``);
    persistent external connections must be cleared by the operator before
    launching the session — the helper does not mutate source-DB settings (#894).
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if not worker:
        return
    import asyncio
    import asyncpg
    import fcntl
    import tempfile
    from urllib.parse import urlparse

    base = os.environ["DATABASE_URL"]
    # Strip the worker suffix to get the source DB name (we re-applied
    # _worker_db_url at import time, so DATABASE_URL ends with _gw0 etc.).
    p = urlparse(base)
    suffixed = p.path.lstrip("/")
    if not suffixed.endswith(f"_{worker}"):
        return
    source_db = suffixed[: -(len(worker) + 1)]
    worker_db = suffixed
    admin_url = base.replace(f"/{suffixed}", "/postgres")

    # Per-host file lock — serialize worker template-creation across xdist
    # workers in this pytest invocation BEFORE we ever touch Postgres.
    # The lock path is keyed on the source DB so that pytest runs against
    # different Postgres instances (different ports / clusters) do not block
    # each other unnecessarily.
    lock_path = os.path.join(
        tempfile.gettempdir(), f"dynastore_xdist_ensure_db_{source_db}.lock"
    )
    # Open with O_CREAT|O_RDWR so multiple workers can independently take
    # an exclusive lock on the same inode.
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        # Block until we hold the exclusive lock — this can take a few seconds
        # under -n auto while sibling workers finish their CREATE TEMPLATE.
        # Use a polled loop in the executor so we don't pin the event loop.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fcntl.flock, lock_fd, fcntl.LOCK_EX)
        conn = await asyncpg.connect(admin_url)
        try:
            # Belt-and-braces: serialize against any concurrent pytest
            # invocation that may be sharing the same Postgres cluster.
            await conn.execute("SELECT pg_advisory_lock(8472001)")
            try:
                await conn.execute(
                    f'DROP DATABASE IF EXISTS "{worker_db}" WITH (FORCE)'
                )
                await _create_db_template_with_retry(
                    conn.execute, source_db, worker_db,
                )
            finally:
                await conn.execute("SELECT pg_advisory_unlock(8472001)")
        finally:
            await conn.close()
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


async def _drop_worker_db():
    """No-op stub kept for backward compatibility (#1027).

    Per-worker DBs (``gis_dev_gw*``) are NOT dropped at session finish:

    * The next session's ``_ensure_worker_db`` already does
      ``DROP DATABASE IF EXISTS "{worker_db}" WITH (FORCE)`` immediately
      before re-cloning the template, so stale DBs are reaped on the
      following run.
    * Dropping at session-finish on the worker leaked through xdist's
      crashed-worker / replaced-worker lifecycle: when xdist replaced a
      crashed worker, the OLD process could still run ``_drop_worker_db``
      AFTER the REPLACEMENT process had successfully re-created the DB,
      yielding the "database gis_dev_gwN does not exist" cascades that
      flagged #1027.  Leaving the DB around between sessions sidesteps
      that race entirely.
    * The controller's session-finish hook still runs ``cleanup_db()`` on
      the master ``gis_dev`` so shared state is reset between sessions.
    """
    return


async def _bootstrap_foundational_schemas(engine) -> None:
    """Create foundational tables that must exist before xdist workers clone
    ``gis_dev`` as a per-worker template database.

    ``configs.platform_configs`` is owned by ``DBConfigModule``/
    ``PlatformConfigService``; ``iam.applied_presets`` is owned by
    ``IamModule``/``AppliedPresetsService``.  Both modules create these
    tables in their lifespan, but xdist workers clone the template
    *before* any lifespan runs — so the controller must bootstrap them
    explicitly here so every cloned worker DB already contains the rows.

    Called from the session-level ``db_reset_session`` fixture after the
    cleanup pass finishes.  Idempotent: all DDL uses CREATE … IF NOT EXISTS.
    DDL stays inside each owning module; this helper only invokes the
    existing idempotent bootstrap entry-points.
    """
    from dynastore.modules.db_config.platform_config_service import (
        PlatformConfigService,
    )
    from dynastore.modules.iam.applied_presets_service import AppliedPresetsService
    from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
    from dynastore.modules.db_config.query_executor import managed_transaction

    # configs schema + platform_configs table (delegates to PlatformConfigService
    # so the DDL stays inside the module, not here).
    await PlatformConfigService.initialize_storage(engine)

    # iam schema + applied_presets table.
    async with managed_transaction(engine) as conn:
        await ensure_schema_exists(conn, "iam")
        svc = AppliedPresetsService(engine)
        await svc.ensure_table(conn=conn)


async def _assert_foundational_schemas(engine) -> None:
    """Fail fast if foundational DB schemas are missing after module startup.

    ``DBConfigModule`` must create ``configs.platform_configs`` during its
    lifespan and ``IamModule`` must create ``iam.applied_presets``.  If
    either table is absent after ``modules.lifespan`` returns, every config
    read or preset-audit write will raise and every collection/catalog
    create will 500.  Asserting here surfaces the regression immediately
    with a clear message instead of burying it under hundreds of cascade 500s.
    """
    import pytest
    from sqlalchemy import text

    _REQUIRED: tuple[tuple[str, str], ...] = (
        ("configs", "platform_configs"),
        ("iam", "applied_presets"),
    )
    async with engine.connect() as conn:
        for schema, table in _REQUIRED:
            fq = f'"{schema}"."{table}"'
            row = await conn.execute(
                text("SELECT to_regclass(:fq)"), {"fq": fq}
            )
            if row.scalar() is None:
                pytest.fail(
                    f"Foundational schema regression: {fq} does not exist after "
                    f"module lifespan startup. Check module startup logs for "
                    f"CRITICAL errors."
                )


def pytest_sessionstart(session):
    """Per-worker DB provisioning. No-op outside xdist."""
    if not os.environ.get("PYTEST_XDIST_WORKER"):
        return
    import asyncio

    asyncio.run(_ensure_worker_db())


def pytest_sessionfinish(session, exitstatus):
    """
    Run database cleanup after the full test session.

    With xdist: workers do NOT drop their per-worker DBs (#1027 — the drop
    raced with replacement-worker startup when xdist restarted a crashed
    worker, leaving the recreated DB dropped just after creation). Worker
    DBs are reaped at the START of the next session by ``_ensure_worker_db``
    (DROP IF EXISTS + recreate from template). The controller still runs
    the final shared-state cleanup pass on the master gis_dev DB.

    Without xdist: called in the main process after all tests finish; runs
    cleanup_db on the single shared DB.
    """
    import asyncio

    if os.environ.get("PYTEST_XDIST_WORKER"):
        # Worker drop deliberately deferred to next session's
        # ``_ensure_worker_db`` (see #1027). Keep the call for the
        # no-op stub so callers / docstrings stay in sync.
        try:
            asyncio.run(_drop_worker_db())
        except Exception as e:
            print(f"[POST-SESSION] Worker DB drop warning (non-fatal): {e}")
        return

    try:
        from tests.dynastore.test_utils.cleanup_db import cleanup_db

        print("\n[POST-SESSION] Running database cleanup...")
        asyncio.run(cleanup_db())
        print("[POST-SESSION] Database cleanup complete.")
    except Exception as e:
        print(f"[POST-SESSION] Cleanup warning (non-fatal): {e}")


def pytest_load_initial_conftests(early_config, parser, args):
    """
    Ensures that if -n (xdist) is used but the plugin is mission, we don't crash.
    Also handles CI environment worker limits if not explicitly specified.
    """
    # Check if xdist is actually available
    try:
        import xdist  # noqa: F401
    except ImportError:
        # If xdist is missing, remove -n/--numprocesses from args to allow sequential run
        # instead of failing with 'unrecognized argument'
        i = 0
        while i < len(args):
            if args[i] == "-n" or args[i].startswith("--numprocesses"):
                # Remove the flag and its value
                args.pop(i)
                if i < len(args) and not args[i].startswith("-"):
                    args.pop(i)
            else:
                i += 1


def pytest_collection_modifyitems(items):
    """
    Patch pytest-asyncio Coroutine items so VS Code's test explorer recognises them.

    pytest-asyncio 1.x wraps async tests in a ``Coroutine`` subclass of
    ``pytest.Function``.  VS Code's Python extension only lists items whose
    ``__class__.__name__ == "Function"``, so async tests become invisible in the
    Testing panel.  Re-stamping the class name fixes discovery without changing
    runtime behaviour (``Coroutine`` already inherits from ``Function``).
    """
    try:
        from pytest_asyncio.plugin import Coroutine  # noqa: F401
    except ImportError:
        return
    for item in items:
        if isinstance(item, Coroutine):
            item.__class__.__name__ = "Function"


def pytest_runtest_logstart(nodeid, location):
    """Atomically write the current nodeid to ``$DYNASTORE_TEST_SENTINEL``.

    When CI cancels the integration-tests job at the 30-min ceiling (#541),
    pytest's stdout buffer is dropped and ``junitxml`` is never finalised,
    so the cancelled run leaves no record of which test was last running.
    A filesystem sentinel survives cancellation: the workflow uploads the
    file as an artifact, and the last-written nodeid identifies the
    offending test.

    No-op when the env var is unset (dev runs).  Atomic write via
    ``os.replace`` so a SIGTERM mid-flush leaves the previous nodeid
    intact rather than a half-written line.
    """
    path = os.environ.get("DYNASTORE_TEST_SENTINEL")
    if not path:
        return
    try:
        from datetime import datetime, timezone
        worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
        line = f"{datetime.now(timezone.utc).isoformat()} {worker} {nodeid}\n"
        tmp = f"{path}.tmp"
        with open(tmp, "w") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        # Sentinel writing must never break a test.
        pass


def pytest_runtest_setup(item):
    """
    Called before each test is executed.
    Handles marker-based skip logic:

    - ``local_only``: skipped when REMOTE_TEST_EXECUTION=true or CI env is detected.
    - ``gcp``: skipped when GCP Application Default Credentials are not available.
      This replaces the old pattern of stacking ``@local_only`` + ``@gcp`` — tests
      now run wherever valid credentials exist (local *or* CI).
    """
    if item.get_closest_marker("local_only"):
        is_remote = os.getenv("REMOTE_TEST_EXECUTION") == "true"
        is_ci = os.getenv("CI") or os.getenv("GITHUB_ACTIONS")

        if is_remote or is_ci:
            pytest.skip(
                f"Test marked as 'local_only' and running in a {'remote' if is_remote else 'CI'} environment."
            )

    if item.get_closest_marker("gcp"):
        # Attempt to resolve GCP credentials the same way the GCP module does.
        # If credentials are missing the test cannot interact with GCP APIs.
        try:
            from dynastore.modules.gcp.tools.service_account import get_credentials
            creds, _ = get_credentials()
            if not creds:
                pytest.skip("GCP credentials (ADC) not available — skipping GCP test.")
        except Exception as e:
            pytest.skip(f"GCP credentials not available ({e}) — skipping GCP test.")

    if item.get_closest_marker("elasticsearch"):
        # Opt-in via DYNASTORE_RUN_ELASTICSEARCH_TESTS=true. ES container being
        # reachable is not enough — the test app must explicitly enable the
        # ElasticsearchModule, which the default test fixture does not. Setting
        # the env var asserts the caller's stack is fully wired (ES + module).
        if os.getenv("DYNASTORE_RUN_ELASTICSEARCH_TESTS", "").lower() not in ("1", "true", "yes"):
            pytest.skip(
                "Elasticsearch tests require explicit opt-in: set "
                "DYNASTORE_RUN_ELASTICSEARCH_TESTS=true (and ensure the test "
                "stack enables ElasticsearchModule)."
            )

    if item.get_closest_marker("idp"):
        # Same opt-in pattern: an Identity Provider running on :8080 is not
        # enough; the test must wire IamModule against it.
        if os.getenv("DYNASTORE_RUN_IDP_TESTS", "").lower() not in ("1", "true", "yes"):
            pytest.skip(
                "Identity-provider tests require explicit opt-in: set "
                "DYNASTORE_RUN_IDP_TESTS=true."
            )


@pytest.fixture(scope="session", autouse=True)
def wait_for_db(request):
    """
    Block until PostgreSQL accepts real queries (not just TCP connections).

    pg_isready / TCP-level checks pass while the server is still in recovery
    mode. This fixture retries a SELECT 1 so tests only start when the DB is
    actually ready to serve queries.

    Skips the wait when no collected test requires a DB connection (e.g. pure
    unit test runs without app_lifespan or db_engine fixtures).
    """
    needs_db = any(
        "app_lifespan" in item.fixturenames or "db_engine" in item.fixturenames
        for item in request.session.items
    )
    if not needs_db:
        return

    import asyncio
    import time
    import asyncpg

    url = os.getenv("DATABASE_URL", "postgresql://testuser:testpassword@localhost:54320/gis_dev")
    asyncpg_url = url.replace("postgresql+asyncpg://", "postgresql://")

    async def _check():
        conn = await asyncpg.connect(asyncpg_url, timeout=5)
        try:
            await conn.execute("SELECT 1")
        finally:
            await conn.close()

    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            asyncio.run(_check())
            return
        except Exception as exc:
            if attempt < max_attempts - 1:
                time.sleep(2)
            else:
                raise RuntimeError(
                    f"Database at {asyncpg_url!r} did not become ready "
                    f"after {max_attempts * 2}s: {exc}"
                ) from exc


@pytest.fixture(scope="session")
def data_id():
    """Returns a unique session ID to avoid naming collisions in the database."""
    return generate_test_id(16)


@pytest.fixture
def dynastore_modules(request):
    """Default modules list. Resolution order:

    1. ``@pytest.mark.enable_modules(...)`` — per-test override.
    2. ``SCOPE`` env var (e.g. ``monkeypatch.setenv("SCOPE", "db_config,db,...")``)
       — matches the production-side narrowing knob. Without this,
       fixture-scoped ``setenv("SCOPE", ...)`` was a silent no-op
       (closes #206).
    3. Hard-coded default below.

    "gcp" is intentionally excluded from the default set: loading it hooks
    into catalog lifecycle events and makes real GCS API calls (bucket
    create/delete) on every catalog operation, adding ~30 s per test.
    Tests that require GCP behaviour must opt in explicitly:
       @pytest.mark.enable_modules("db_config", "db", "catalog", "gcp", ...)
    or via ``monkeypatch.setenv("SCOPE", "...,gcp,...")``.
    """
    marker = request.node.get_closest_marker("enable_modules")
    if marker:
        return list(marker.args)
    scope_env = os.environ.get("SCOPE", "").strip()
    if scope_env:
        return [name.strip() for name in scope_env.split(",") if name.strip()]
    return ["db_config", "db", "catalog", "stats", "iam", "stac", "collection_postgresql", "catalog_postgresql"]


@pytest.fixture
def dynastore_extensions(request):
    """Default extensions list. Can be overridden via @pytest.mark.enable_extensions."""
    marker = request.node.get_closest_marker("enable_extensions")
    if marker:
        return list(marker.args)
    return []


@pytest.fixture
def dynastore_tasks(request):
    """Default tasks list. Can be overridden via @pytest.mark.enable_tasks."""
    marker = request.node.get_closest_marker("enable_tasks")
    if marker:
        return list(marker.args)
    return []


@pytest.fixture(scope="session")
def base_url():
    """Returns the base URL for the DynaStore API."""
    # 1. Respect explicit env var (Full URL)
    url = os.getenv("DYNASTORE_API_URL")
    if url:
        return url.rstrip("/")

    # 2. Build from components (local dev fallback)
    # Support environment specific variable for API_HOST
    # Local: localhost, Docker dev: catalog, Pipeline test: catalog_test
    host = os.getenv("API_HOST", "127.0.0.1")
    port = os.getenv("TCP_PORT", "80")
    # Respect docker-compose port if present
    port = os.getenv("HOST_PORT_CATALOG", port)

    root_path = os.getenv("API_ROOT_PATH", "").rstrip("/")
    return f"http://{host}:{port}{root_path}"


async def _mint_test_jwt(iam_svc, roles: list[str], subject: str = "test-sysadmin") -> str:
    """Mint a short-lived HS256 JWT for test fixtures."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = await iam_svc.get_jwt_secret()
    payload = {
        "sub": subject,
        "roles": roles,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "dynastore-test",
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")


@pytest_asyncio.fixture(loop_scope="function")
async def in_process_client(app_lifespan):
    """
    In-process HTTP client for testing with specific extensions enabled.
    Depends on app_lifespan to start the app with configured modules/extensions.
    """
    from httpx import AsyncClient, ASGITransport

    # app_lifespan yields app.state, which has .app attached
    transport = ASGITransport(app=app_lifespan.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def sysadmin_in_process_client(app_lifespan):
    """
    In-process HTTP client with SYSADMIN privileges.
    Mints a JWT with sysadmin role via IamService.
    """
    from httpx import AsyncClient, ASGITransport
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol

    iam_svc = get_protocol(AuthenticatorProtocol)
    headers = {}

    if iam_svc:
        token = await _mint_test_jwt(iam_svc, roles=["sysadmin"])
        headers = {"Authorization": f"Bearer {token}"}

    transport = ASGITransport(app=app_lifespan.app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield client


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def in_process_client_module(app_lifespan_module):
    """Module-scoped HTTP client backed by app_lifespan_module (one bootstrap per file)."""
    from httpx import AsyncClient, ASGITransport

    transport = ASGITransport(app=app_lifespan_module.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def sysadmin_in_process_client_module(app_lifespan_module):
    """Module-scoped sysadmin HTTP client backed by app_lifespan_module."""
    from httpx import AsyncClient, ASGITransport
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol

    iam_svc = get_protocol(AuthenticatorProtocol)
    headers = {}

    if iam_svc:
        token = await _mint_test_jwt(iam_svc, roles=["sysadmin"])
        headers = {"Authorization": f"Bearer {token}"}

    transport = ASGITransport(app=app_lifespan_module.app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield client


async def reset_dynastore_state(engine=None):
    """Drops all singleton instances and resets registries to avoid cross-test contamination."""
    from dynastore import modules, extensions, tasks
    from dynastore.modules.db_config import query_executor
    from dynastore.modules.stats.service import STATS_SERVICE
    from dynastore.extensions import registry
    from dynastore.modules.catalog import catalog_module
    try:
        from dynastore.extensions.gcp import gcp_events
    except ImportError:
        gcp_events = None
    from dynastore.tools.async_utils import signal_bus
    from dynastore.tools.discovery import (
        get_protocol,
        get_protocols,
        _DYNASTORE_PLUGINS,
    )
    from collections import defaultdict
    import asyncio
    import logging

    logger = logging.getLogger(__name__)

    # --- Phase 1: Task Collection and Cancellation ---
    # We collect all background tasks that might be using the current loop/connections.
    pending_tasks = []

    # 4. Reset Query Executor Loop
    query_executor._main_app_loop = None
    query_executor._conn_locks.clear()
    from dynastore.modules.db_config.locking_tools import _StartupCoordinator

    _StartupCoordinator._tasks.clear()
    _StartupCoordinator._lock = asyncio.Lock()


    # 5. Reset Stats Service
    STATS_SERVICE._stats_driver = None
    STATS_SERVICE._drivers = []
    if hasattr(STATS_SERVICE, "_flush_task") and STATS_SERVICE._flush_task:
        if not STATS_SERVICE._flush_task.done():
            STATS_SERVICE._flush_task.cancel()
            pending_tasks.append(STATS_SERVICE._flush_task)
        STATS_SERVICE._flush_task = None

    # 6. Clear Event Listeners (CRITICAL for loop isolation)
    catalog_module._event_listeners = defaultdict(list)
    if gcp_events is not None:
        gcp_events._gcp_event_listeners = defaultdict(list)

    # 7. Clear Module Singletons (CRITICAL FIX)
    catalog_module._module_instance = None

    # 8. Clear Event Manager (CRITICAL for loop isolation)
    from dynastore.modules.catalog.event_service import event_service

    event_service._sync_listeners.clear()
    event_service._async_listeners.clear()
    
    # 8.1 Reset SignalBus (CRITICAL for async test isolation)
    if hasattr(signal_bus, "clear"):
        await signal_bus.clear()
    
    # 8.2 Clear Tasks Table (CRITICAL for test isolation)
    # Use DELETE (RowExclusiveLock) instead of TRUNCATE CASCADE (AccessExclusiveLock)
    # so concurrent SELECT queries from other workers are not blocked.
    #
    # Skip under xdist: the unscoped DELETE wipes rows belonging to OTHER
    # workers' in-flight tests, producing deterministic state corruption
    # (180s timeouts, 500s, "Event loop is closed"). The session-finish
    # cleanup hook in pytest_sessionfinish does the wholesale wipe when
    # all workers are done; intra-test isolation is achieved at the
    # catalog-scope level via generate_test_id-based unique catalog IDs.
    if engine and not os.environ.get("PYTEST_XDIST_WORKER"):
        from dynastore.modules.db_config.query_executor import DQLQuery, managed_transaction
        from dynastore.modules.db_config.locking_tools import check_table_exists
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.modules.db_config.query_executor import ResultHandler
        from sqlalchemy import text

        schema = get_task_schema()
        async with managed_transaction(engine) as conn:
            for table in ("tasks", "events"):
                if await check_table_exists(conn, table, schema):
                    try:
                        await conn.execute(text(f'DELETE FROM "{schema}".{table}'))
                        logger.info(f"Database: Deleted rows from '{schema}.{table}'.")
                    except Exception as e:
                        logger.warning(f"Database: Failed to delete rows from '{schema}.{table}': {e}")
    if hasattr(event_service, "aggregator") and event_service.aggregator:
        try:
            await event_service.aggregator.stop()
        except Exception as e:
            logger.debug(f"Failed to stop EventService aggregator: {e}")
    event_service._aggregator_started = False

    # 9. Clear Lifecycle Registry (Soft Reset)
    # NOTE: We use soft_clear() to preserve Framework/Module level hooks (decorators)
    # while clearing instance-specific dynamic hooks and cancelling pending tasks.
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
    
    # Track any tasks that are still running for diagnostics
    pending_tasks.extend(
        t for t in lifecycle_registry._active_tasks if not t.done()
    )
    
    lifecycle_registry.soft_clear()
    logger.info("LifecycleRegistry soft-cleared (framework hooks preserved).")

    # 10. Clear Log Service (Crucial for connection isolation)
    from dynastore.modules.catalog.log_manager import LOG_SERVICE

    if (
        hasattr(LOG_SERVICE, "_flush_task")
        and LOG_SERVICE._flush_task
        and not LOG_SERVICE._flush_task.done()
    ):
        LOG_SERVICE._flush_task.cancel()
        pending_tasks.append(LOG_SERVICE._flush_task)

    LOG_SERVICE._flush_task = None
    LOG_SERVICE._buffer = None
    LOG_SERVICE._engine = None

    # Await all cancelled tasks to ensure connections are released
    if pending_tasks:
        try:
            # We use a short timeout because these should cancel quickly
            await asyncio.wait_for(
                asyncio.gather(*pending_tasks, return_exceptions=True), timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for background tasks to cancel during state reset.")
        except Exception as e:
            logger.debug(f"Error while awaiting cancelled tasks: {e}")

    # --- Phase 5: Wiping Global Registries and Discovery (LIFESPAN IS OVER) ---
    # Now that all tasks ARE FINISHED, we can safely clear the framework state.
    
    # Discovery Cache
    get_protocol.cache_clear()
    get_protocols.cache_clear()
    _DYNASTORE_PLUGINS.clear()

    # Module & Task Instances (CRITICAL FIX)
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if hasattr(config, "instance"):
            config.instance = None
            
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if hasattr(config, "instance"):
            config.instance = None
            
    # Also clear the tasks module's internal version just in case
    from dynastore.modules import tasks as tasks_module
    if hasattr(tasks_module, "_DYNASTORE_TASKS"):
        for config in tasks_module._DYNASTORE_TASKS.values():
            if hasattr(config, "instance"):
                config.instance = None

    # Clear discovery cache AGAIN after wiping instances
    get_protocol.cache_clear()
    get_protocols.cache_clear()

    # Extension Instances
    for config in registry._DYNASTORE_EXTENSIONS.values():
        config.instance = None

    # Task Instances
    for config in tasks._DYNASTORE_TASKS.values():
        config.instance = None

    # 10.1 Clear Sidecar Registry (CRITICAL for extension isolation)
    from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
    SidecarRegistry.clear_registry()

    # 10.1b Drop the STAC items-sidecar module from ``sys.modules`` so that
    # a subsequent narrowed-SCOPE test (one that excludes ``stac``) doesn't
    # inherit a stale ``StacItemsSidecar`` registration left behind by
    # module-import side-effects (``stac_items_sidecar.py`` registers
    # ``StacItemsSidecar`` at import time, and ``clear_registry()`` only
    # wipes the registry dict — the cached module object survives).
    # Belt-and-braces: closes the gw2 cascade in CI (#206) where the
    # ingestion fixture inherited STAC sidecar state from a prior wide
    # test on the same xdist worker.
    sys.modules.pop("dynastore.extensions.stac.stac_items_sidecar", None)

    # 10.2 Clear metadata-tier sidecar registries via their public test-isolation
    # hooks. Both classes ship a ``.clear()`` classmethod (with the docstring
    # ``"""Test-isolation hook."""`` — explicitly designed for this fixture
    # path). Calling the public API survives any future internal field rename
    # and matches how the items-tier ``SidecarRegistry.clear_registry()`` is
    # invoked above.
    try:
        from dynastore.modules.storage.drivers.collection_postgresql import (
            CollectionPgSidecarRegistry,
        )
        CollectionPgSidecarRegistry.clear()
    except Exception as e:
        logger.debug(f"CollectionPgSidecarRegistry.clear() skipped: {e}")
    try:
        # Phase 3: the catalog-tier slice registry is folded into the single
        # SidecarRegistry; its test-isolation hook is clear_catalog_registry().
        SidecarRegistry.clear_catalog_registry()
    except Exception as e:
        logger.debug(f"SidecarRegistry.clear_catalog_registry() skipped: {e}")

    # 11. Clear Caches safely
    from dynastore.modules.catalog import config_service
    from dynastore.modules.tiles import tiles_module
    from dynastore.modules.catalog.catalog_module import CatalogModule
    from dynastore.models.protocols import CatalogsProtocol, AssetsProtocol
    try:
        from dynastore.modules.gcp.gcp_module import GCPModule
    except ImportError:
        GCPModule = None

    def clear_if_cached(obj, attr_name=None):
        target = getattr(obj, attr_name, None) if attr_name else obj
        if target and hasattr(target, "cache_clear"):
            try:
                target.cache_clear()
                # Reset alru_cache event loop binding so it works across
                # function-scoped event loops in pytest.
                # async_lru stores the first loop as a mangled attribute.
                _ALRU_LOOP_ATTR = "_LRUCacheWrapper__first_loop"
                if hasattr(target, _ALRU_LOOP_ATTR):
                    setattr(target, _ALRU_LOOP_ATTR, None)
            except Exception as e:
                logger.debug(f"Failed to clear cache for {attr_name}: {e}")

    # Clear Protocol instance caches
    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc:
        clear_if_cached(catalogs_svc, "_get_catalog_model_cached")
        clear_if_cached(catalogs_svc, "_get_catalog_query")
        
    assets_svc = get_protocol(AssetsProtocol)
    if assets_svc:
        clear_if_cached(assets_svc, "_check_asset_exists_query")

    from dynastore.models.protocols import CollectionsProtocol
    collections_svc = get_protocol(CollectionsProtocol)
    if collections_svc:
        clear_if_cached(collections_svc, "_get_collection_model_cached")
        clear_if_cached(collections_svc, "_resolve_physical_table_cached")
        
    # Config service caches
    clear_if_cached(config_service, "_catalog_config_cache")
    clear_if_cached(config_service, "_collection_config_cache")

    # Tiles module cleanup
    clear_if_cached(tiles_module, "get_tile_resolution_params")
    clear_if_cached(tiles_module, "get_collection_source_srid")
    clear_if_cached(tiles_module, "get_custom_tms")
    clear_if_cached(tiles_module, "list_custom_tms")
    clear_if_cached(tiles_module, "resolve_srid")
    clear_if_cached(tiles_module.TilePGPreseedStorage, "check_tile_exists")

    # GCP module caches
    if GCPModule:
        clear_if_cached(GCPModule, "get_self_url")


@pytest.fixture
def db_url():
    """Returns the database URL for the test environment.

    Under xdist this resolves to a per-worker DB (gis_dev_<worker>) so each
    worker's DDL operations are isolated.
    """
    # DATABASE_URL was worker-suffixed at module import via _worker_db_url(),
    # so this returns gis_dev_gw0 etc. under xdist, gis_dev otherwise.
    default_url = "postgresql://testuser:testpassword@localhost:54320/gis_dev"
    return os.getenv("DATABASE_URL", default_url)


@pytest_asyncio.fixture(loop_scope="function")
async def db_engine(db_url):
    """Provides a fresh SQLAlchemy engine for each test."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(
        db_url.replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool
    )
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="function")
async def app_lifespan(
    db_url, db_engine, request, dynastore_modules, dynastore_extensions, dynastore_tasks
):
    """
    Fixture that accurately replicates the DynaStore application startup sequence.
    Uses the bootstrap_app function from main.py for perfect synchronization.
    """
    import asyncio
    import os
    import sys
    from unittest.mock import MagicMock, AsyncMock
    from types import SimpleNamespace
    from contextlib import AsyncExitStack
    from fastapi import FastAPI
    from starlette.datastructures import State
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.tools.protocol_helpers import resolve
    from dynastore.modules.gcp.gcp_config import GcpModuleConfig

    # 0. Isolation Reset
    await reset_dynastore_state(engine=db_engine)

    # 1. Set Environment (MUST be done before bootstrap for discovery)
    os.environ["DATABASE_URL"] = db_url
    os.environ["DYNASTORE_TESTING"] = "true"  # Ensure GCP/Cloud Run mocks work

    # When the Keycloak integration flag is set, point the IAM module at the
    # local sidecar issuer/JWKS so that Bearer tokens issued by that sidecar
    # are trusted by the in-process test app.  We only write these vars when
    # they are not already set, so an operator-supplied override is honoured.
    # The vars are cleared on fixture teardown to avoid cross-test pollution.
    _keycloak_env_keys_injected: list = []
    if os.getenv("KEYCLOAK_INTEGRATION", "").strip() in {"1", "true", "True"}:
        from tests.dynastore.test_utils.keycloak import (
            _DEFAULT_ISSUER as _KC_ISSUER,
            _DEFAULT_CLIENT_ID as _KC_CLIENT_ID,
            _DEFAULT_CLIENT_SECRET as _KC_CLIENT_SECRET,
        )
        for _key, _val in (
            ("KEYCLOAK_ISSUER_URL", _KC_ISSUER),
            ("KEYCLOAK_CLIENT_ID", _KC_CLIENT_ID),
            ("KEYCLOAK_CLIENT_SECRET", _KC_CLIENT_SECRET),
        ):
            if _key not in os.environ:
                os.environ[_key] = _val
                _keycloak_env_keys_injected.append(_key)

    # 2. Mock App Instance (Production-like)
    app = FastAPI()
    app.state = State()  # Fresh state
    
    # Enable global exception handling for test app
    from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
    setup_exception_handlers(app)

    # Set the main loop for query_executor bridge
    from dynastore.modules.db_config.query_executor import set_main_app_loop
    set_main_app_loop(asyncio.get_running_loop())

    # INJECT THE TEST ENGINE BEFORE BOOTSTRAP
    app.state.engine = db_engine

    # 3. Bootstrap Discovery
    from dynastore.extensions.bootstrap import bootstrap_app
    from dynastore import modules, extensions, tasks

    # Define explicit lists for control
    modules_marker = request.node.get_closest_marker("enable_modules")
    if modules_marker:
        modules_list = list(modules_marker.args)
    else:
        # Priority: Fixture -> Env -> Default
        modules_list = dynastore_modules

    extensions_marker = request.node.get_closest_marker("enable_extensions")
    if extensions_marker:
        extensions_list = list(extensions_marker.args)
    else:
        extensions_list = dynastore_extensions

    tasks_marker = request.node.get_closest_marker("enable_tasks")
    if tasks_marker:
        tasks_list = list(tasks_marker.args)
        # Ensure 'tasks' module is enabled if any task is requested
        if "tasks" not in modules_list:
            modules_list.append("tasks")
    else:
        tasks_list = dynastore_tasks

    if modules_list is None:
        modules_list = []
    if extensions_list is None:
        extensions_list = []
        
    # Ensure 'processes' module is enabled if 'processes' extension is enabled
    if "processes" in extensions_list and "processes" not in modules_list:
        modules_list.append("processes")

    # Bootstrap using the explicit lists if provided (or fallback to env).
    # Discovery loads every installed entry-point; these lists narrow
    # *instantiation* only (test isolation).  Tasks have no instantiate step,
    # so ``tasks_list`` is collected above for future test-side isolation
    # helpers but is NOT passed here.
    _ = tasks_list  # intentionally unused post SCOPE-filter removal
    bootstrap_app(
        app,
        include_modules=modules_list,
        include_extensions=extensions_list,
    )

    # Ensure db_config has the engine attached if it was created by bootstrap
    if hasattr(app.state, "db_config"):
        app.state.db_config.engine = db_engine

    # 4. Orchestrate Lifespans
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(
            modules.lifespan(app.state)
        )

        # Regression guard: foundational schemas must exist after module
        # lifespan startup or every subsequent create will 500-cascade.
        await _assert_foundational_schemas(db_engine)

        # 5. Apply Global GCP Config for Tests (Faster visibility / reliable cleanup)
        try:
            import logging
            test_logger = logging.getLogger(__name__)
            configs_svc = resolve(ConfigsProtocol)
            if configs_svc:
                gcp_test_config = GcpModuleConfig(
                    project_id="test-project",
                    region="europe-west1",
                    catalog_visibility_max_retries=30,
                    catalog_visibility_retry_interval=1.0
                )
                await configs_svc.set_config(GcpModuleConfig, gcp_test_config)
                test_logger.info("Tests: Applied GCP Module config overrides.")
        except Exception as e:
             import logging
             logging.getLogger(__name__).warning(f"Tests: Could not apply GCP Module config overrides: {e}")

        # tasks.manage_tasks is now handled via TasksModule.lifespan
        # which is triggered by modules.lifespan(app.state) above.

        try:
            from dynastore.extensions.lifespan import lifespan as _ext_lifespan
            await stack.enter_async_context(_ext_lifespan(app))
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(
                f"Extension lifespan partially failed: {e}"
            )

        # Apply foundational presets then all PolicyContributorPresets so that
        # platform policies (sysadmin_full_access, role seeds, extension ACLs)
        # land in the test DB — equivalent to the platform_demo apply that
        # would run on a real deployment.
        try:
            import logging as _logging
            from dynastore.modules.storage.presets.registry import find_preset, list_presets
            from dynastore.modules.storage.presets.policy_contributor_adapter import PolicyContributorPreset
            from dynastore.modules.storage.presets.preset import NoParams
            from dynastore.modules.storage.presets.lifecycle import _build_context
            from dynastore.models.protocols import DatabaseProtocol

            _db = modules.get_protocol(DatabaseProtocol)
            _engine = _db.engine if _db else None
            _ctx = _build_context(_engine, None, "platform")

            # Apply foundational IAM presets first so sysadmin_full_access and
            # default roles exist before extension contributor presets run.
            for _foundation_name in ("default_roles_baseline", "iam_baseline"):
                try:
                    _fp = find_preset(_foundation_name)
                    if _fp is not None:
                        await _fp.apply(NoParams(), "platform", _ctx)
                except Exception as _pe:
                    _logging.getLogger(__name__).warning(
                        "Test fixture: preset '%s' apply failed: %s", _foundation_name, _pe
                    )

            for _preset_name in list_presets():
                try:
                    _preset = find_preset(_preset_name)
                    if isinstance(_preset, PolicyContributorPreset):
                        await _preset.apply(NoParams(), "platform", _ctx)
                except Exception as _pe:
                    _logging.getLogger(__name__).warning(
                        "Test fixture: preset '%s' apply failed: %s", _preset_name, _pe
                    )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                f"Test fixture: PolicyContributorPreset apply loop failed: {e}"
            )

        # Attach app to state for easy access in tests (e.g. for in-process AsyncClient)
        app.state.app = app
        # Yield app state to the test
        yield app.state

    # Teardown: remove any Keycloak env vars we injected so they don't bleed
    # into subsequent test functions within the same process.
    for _key in _keycloak_env_keys_injected:
        os.environ.pop(_key, None)


_DEFAULT_MODULE_LIST = [
    "db_config", "db", "catalog", "stats", "iam", "stac",
    "collection_postgresql",
    "catalog_postgresql",
]
_DEFAULT_EXTENSION_LIST: list = []
_DEFAULT_TASK_LIST: list = []


def _resolve_module_lifespan_markers(request):
    """Collect enable_modules / enable_extensions / enable_tasks markers across
    every test in the current pytest module, returning the UNION as the bootstrap set.

    Module-level ``pytestmark`` markers contribute to every test; function/class
    markers contribute only to that test. We take the union so a module-scoped
    bootstrap satisfies the strictest test in the file.

    Returns (modules_list, extensions_list, tasks_list).
    """
    mod_set: set = set()
    ext_set: set = set()
    task_set: set = set()
    have_modules_marker = False
    have_extensions_marker = False
    have_tasks_marker = False

    for item in request.session.items:
        if getattr(item, "module", None) is not request.module:
            continue
        for marker_name, target_set, flag_name in (
            ("enable_modules", mod_set, "have_modules_marker"),
            ("enable_extensions", ext_set, "have_extensions_marker"),
            ("enable_tasks", task_set, "have_tasks_marker"),
        ):
            for marker in item.iter_markers(marker_name):
                target_set.update(marker.args)
                if marker_name == "enable_modules":
                    have_modules_marker = True
                elif marker_name == "enable_extensions":
                    have_extensions_marker = True
                else:
                    have_tasks_marker = True

    modules_list = sorted(mod_set) if have_modules_marker else list(_DEFAULT_MODULE_LIST)
    extensions_list = sorted(ext_set) if have_extensions_marker else list(_DEFAULT_EXTENSION_LIST)
    tasks_list = sorted(task_set) if have_tasks_marker else list(_DEFAULT_TASK_LIST)
    # `processes` extension hard-requires the `processes` module — match
    # app_lifespan's behaviour at line 715.
    if "processes" in extensions_list and "processes" not in modules_list:
        modules_list.append("processes")
    return modules_list, extensions_list, tasks_list


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def app_lifespan_module(request):
    """
    Module-scoped variant of ``app_lifespan``.

    Bootstraps the application ONCE per test file (module) instead of once per
    test function — eliminates ~3-5s × N redundant ``reset_dynastore_state`` +
    module-discovery + lifespan-entry cycles per file.

    Module/extension/task selection: walks every test in the module and unions
    their ``enable_modules`` / ``enable_extensions`` / ``enable_tasks`` markers
    (function-, class-, and module-level all contribute). When NO marker for a
    category is present anywhere in the module, falls back to the defaults
    used by ``dynastore_modules`` / ``dynastore_extensions`` / ``dynastore_tasks``.

    Test isolation is achieved at the DB level via unique catalog/collection IDs
    (``generate_test_id()`` in ``tests/dynastore/test_utils``); the convention is
    already enforced across the suite, so cross-test pollution within a module
    is benign.

    Usage:
        Replace ``app_lifespan`` with ``app_lifespan_module`` in test signatures.
        File-level ``pytestmark = [pytest.mark.enable_modules(...), ...]``
        is the cleanest expression of the bootstrap set; per-test markers also
        work (their args are unioned).
    """
    import asyncio
    import os
    from contextlib import AsyncExitStack
    from fastapi import FastAPI
    from starlette.datastructures import State
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.tools.protocol_helpers import resolve
    from dynastore.modules.gcp.gcp_config import GcpModuleConfig
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    db_url_val = os.getenv(
        "DATABASE_URL", "postgresql://testuser:testpassword@localhost:54320/gis_dev"
    )

    engine = create_async_engine(
        db_url_val.replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool
    )

    # 0. Isolation Reset (once per module)
    await reset_dynastore_state(engine=engine)

    os.environ["DATABASE_URL"] = db_url_val
    os.environ["DYNASTORE_TESTING"] = "true"

    # When the Keycloak integration flag is set, point the IAM module at the
    # local sidecar issuer/JWKS so Bearer tokens from the sidecar are trusted.
    _keycloak_env_keys_injected_mod: list = []
    if os.getenv("KEYCLOAK_INTEGRATION", "").strip() in {"1", "true", "True"}:
        from tests.dynastore.test_utils.keycloak import (
            _DEFAULT_ISSUER as _KC_ISSUER_MOD,
            _DEFAULT_CLIENT_ID as _KC_CLIENT_ID_MOD,
            _DEFAULT_CLIENT_SECRET as _KC_CLIENT_SECRET_MOD,
        )
        for _key_m, _val_m in (
            ("KEYCLOAK_ISSUER_URL", _KC_ISSUER_MOD),
            ("KEYCLOAK_CLIENT_ID", _KC_CLIENT_ID_MOD),
            ("KEYCLOAK_CLIENT_SECRET", _KC_CLIENT_SECRET_MOD),
        ):
            if _key_m not in os.environ:
                os.environ[_key_m] = _val_m
                _keycloak_env_keys_injected_mod.append(_key_m)

    app = FastAPI()
    app.state = State()

    from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
    setup_exception_handlers(app)

    from dynastore.modules.db_config.query_executor import set_main_app_loop
    set_main_app_loop(asyncio.get_running_loop())

    app.state.engine = engine

    from dynastore.extensions.bootstrap import bootstrap_app
    from dynastore import modules, extensions, tasks

    modules_list, extensions_list, tasks_list = _resolve_module_lifespan_markers(request)
    _ = tasks_list  # reserved for future test-side task-isolation helper

    bootstrap_app(
        app,
        include_modules=modules_list,
        include_extensions=extensions_list,
    )

    if hasattr(app.state, "db_config"):
        app.state.db_config.engine = engine

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(modules.lifespan(app.state))

        # Regression guard: foundational schemas must exist after module
        # lifespan startup or every subsequent create will 500-cascade.
        await _assert_foundational_schemas(engine)

        try:
            import logging
            _log = logging.getLogger(__name__)
            configs_svc = resolve(ConfigsProtocol)
            if configs_svc:
                gcp_cfg = GcpModuleConfig(
                    project_id="test-project",
                    region="europe-west1",
                    catalog_visibility_max_retries=30,
                    catalog_visibility_retry_interval=1.0,
                )
                await configs_svc.set_config(GcpModuleConfig, gcp_cfg)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Tests: Could not apply GCP config: {e}")

        try:
            from dynastore.extensions.lifespan import lifespan as _ext_lifespan
            await stack.enter_async_context(_ext_lifespan(app))
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Extension lifespan partially failed: {e}")

        # Apply foundational presets then all PolicyContributorPresets so that
        # platform policies (sysadmin_full_access, role seeds, extension ACLs)
        # land in the test DB — equivalent to the platform_demo apply that
        # would run on a real deployment.
        try:
            import logging as _logging
            from dynastore.modules.storage.presets.registry import find_preset, list_presets
            from dynastore.modules.storage.presets.policy_contributor_adapter import PolicyContributorPreset
            from dynastore.modules.storage.presets.preset import NoParams
            from dynastore.modules.storage.presets.lifecycle import _build_context
            from dynastore.models.protocols import DatabaseProtocol

            _db = modules.get_protocol(DatabaseProtocol)
            _engine = _db.engine if _db else None
            _ctx = _build_context(_engine, None, "platform")

            # Apply foundational IAM presets first so sysadmin_full_access and
            # default roles exist before extension contributor presets run.
            for _foundation_name in ("default_roles_baseline", "iam_baseline"):
                try:
                    _fp = find_preset(_foundation_name)
                    if _fp is not None:
                        await _fp.apply(NoParams(), "platform", _ctx)
                except Exception as _pe:
                    _logging.getLogger(__name__).warning(
                        "Test fixture: preset '%s' apply failed: %s", _foundation_name, _pe
                    )

            for _preset_name in list_presets():
                try:
                    _preset = find_preset(_preset_name)
                    if isinstance(_preset, PolicyContributorPreset):
                        await _preset.apply(NoParams(), "platform", _ctx)
                except Exception as _pe:
                    _logging.getLogger(__name__).warning(
                        "Test fixture: preset '%s' apply failed: %s", _preset_name, _pe
                    )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                f"Test fixture: PolicyContributorPreset apply loop failed: {e}"
            )

        app.state.app = app
        yield app.state

    # Module-level teardown
    await reset_dynastore_state(engine=engine)
    await engine.dispose()

    # Remove any Keycloak env vars injected for integration mode.
    for _key_m in _keycloak_env_keys_injected_mod:
        os.environ.pop(_key_m, None)


@pytest_asyncio.fixture(loop_scope="function")
async def task_app_state(db_url, db_engine, dynastore_modules, dynastore_tasks):
    """
    Fixture that replicates the DynaStore generic task environment startup.
    Uses the bootstrap_task_env function from main_task.py.
    """
    import asyncio
    import os
    import sys
    from unittest.mock import MagicMock
    from types import SimpleNamespace
    from contextlib import AsyncExitStack
    from dynastore.tasks.bootstrap import bootstrap_task_env
    from dynastore import modules, tasks
    from dynastore.modules.db_config.db_config import DBConfig
    from dynastore.modules.db_config.query_executor import set_main_app_loop

    # 0. Isolation Reset
    await reset_dynastore_state(engine=db_engine)

    # 1. Set Environment
    os.environ["DATABASE_URL"] = db_url

    # Set the main loop for query_executor bridge
    set_main_app_loop(asyncio.get_running_loop())

    # 2. Setup State
    app_state = SimpleNamespace()
    app_state.engine = db_engine
    DBConfig.database_url = db_url
    app_state.db_config = DBConfig()

    # 3. Bootstrap: discovery loads everything installed; ``include_modules``
    # narrows instantiation only.  Task isolation happens post-discovery if
    # needed.
    _ = dynastore_tasks
    bootstrap_task_env(app_state, include_modules=dynastore_modules)

    # 4. Orchestrate Lifespans
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(modules.lifespan(app_state))
        await stack.enter_async_context(tasks.manage_tasks(app_state))

        yield app_state


@pytest.fixture
def test_data_loader(request):
    """
    Fixture that returns a function to load JSON data.
    It searches in:
    1. A 'data' directory relative to the test module.
    2. A global 'tests/data' directory.
    """

    def _loader(filename):
        local_dir = os.path.join(os.path.dirname(request.module.__file__), "data")
        local_path = os.path.join(local_dir, filename)

        if os.path.exists(local_path):
            with open(local_path, "r") as f:
                return json.load(f)

        # Global data directory (tests/data)
        # We find the 'tests' directory by looking up from this file's directory
        tests_dir = os.path.dirname(os.path.abspath(__file__))
        global_path = os.path.join(tests_dir, "data", filename)

        if os.path.exists(global_path):
            with open(global_path, "r") as f:
                return json.load(f)

        raise FileNotFoundError(
            f"Test data file '{filename}' not found in local ({local_dir}) "
            f"or global ({os.path.join(tests_dir, 'data')}) folders."
        )

    return _loader
