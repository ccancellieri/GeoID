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

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Optional
import asyncio
import json
import uuid
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
import sys
import os
from contextlib import asynccontextmanager
from dynastore.modules import (
    lifespan as modules_lifespan,
    discover_modules,
    instantiate_modules,
    get_protocol,
    get_protocols,
)
from dynastore.extensions.lifespan import lifespan as extensions_lifespan
from dynastore._version import VERSION, get_build_info
from dynastore.extensions.tools.fast_api import ORJSONResponse
from dynastore.extensions.bootstrap import bootstrap_app
from dynastore.modules.concurrency import set_concurrency_backend
from dynastore.tools.background_service import (
    BackgroundSupervisor,
    Leadership,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.correlation import _correlation_id_var, set_correlation_id
from dynastore.tools.memory_watchdog import (
    build_memory_watchdog_service,
    load_memory_watchdog_config,
    read_process_rss_bytes,
    resolve_watchdog_budget_mb,
)
from dynastore.tools.serving_state import is_draining

# Register the scaling PluginConfig at the composition root so its class_key is
# known to the config seeder. The seeder runs inside the TasksModule lifespan
# and resolves only already-imported PluginConfig subclasses; scaling is
# otherwise first imported when CatalogModule's lifespan starts its signal
# publisher — one step too late, so a scaling-policy seed is silently skipped
# as "unknown class_key". Importing here (main is imported before any lifespan)
# guarantees registration in time — same pattern as the memory watchdog above.
from dynastore.modules.scaling import config as _scaling_config  # noqa: F401
from fastapi.concurrency import run_in_threadpool

# --- Initialize Concurrency Backend ---
# Since this is the FastAPI entry point, we use FastAPI's threadpool runner.
set_concurrency_backend(run_in_threadpool)


class _CorrelationFilter(logging.Filter):
    """Add correlation_id from context to all log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _correlation_id_var.get(None)
        return True


class _JsonFormatter(logging.Formatter):
    """Format log records as JSON."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        # logging.Formatter.formatTime delegates to time.strftime, which has no
        # microsecond directive — a "%f" in datefmt would be emitted literally.
        # Build the timestamp from datetime instead so sub-second precision works.
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime(datefmt) if datefmt else dt.isoformat()

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%f"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": os.getenv("SERVICE_NAME", "dynastore"),
            "correlation_id": getattr(record, "correlation_id", None),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Propagate X-Request-ID header as correlation ID through context."""

    async def dispatch(self, request: Request, call_next):
        cid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        token = set_correlation_id(cid)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = cid
            return response
        finally:
            _correlation_id_var.reset(token)


# --- Logging Configuration ---
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_JsonFormatter())
_handler.addFilter(_CorrelationFilter())
logging.root.setLevel(log_level)
logging.root.handlers = [_handler]

# Cap noisy third-party library loggers regardless of root LOG_LEVEL.
# opensearch-py logs every HTTP exchange at DEBUG (full request/response
# bodies) and every 4xx at WARNING — including idempotent 404s that
# driver-level delete paths catch and treat as success. Without this cap,
# a dev run with LOG_LEVEL=DEBUG produces hundreds of
# index_not_found_exception body dumps during bulk delete flows. Set
# OPENSEARCH_LOG_LEVEL=WARNING (or INFO) to tune; default ERROR hides
# routine 404s but still surfaces auth / connectivity failures.
_os_log_level_name = os.getenv("OPENSEARCH_LOG_LEVEL", "ERROR").upper()
_os_log_level = getattr(logging, _os_log_level_name, logging.ERROR)
for _lib in ("opensearch", "elasticsearch", "elastic_transport"):
    logging.getLogger(_lib).setLevel(_os_log_level)

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("Invalid %s=%r; using %.1f.", name, raw, default)
        return default


class _ColdBootReconciliationService:
    """Runs the cold-boot contributor pipeline off the startup-probe path (#3002).

    ``run_cold_boot`` iterates every registered ``ColdBootContributor`` —
    force=True IAM/preset self-heal, the file-backed preset seeder (which can
    provision demo catalogs/collections), and similar idempotent reconciliation
    work. None of it gates ``/health`` or ``/ready`` (neither route checks IAM
    or preset state), so running it synchronously before ``yield`` only made
    boot time scale with catalog count for no correctness benefit — a
    full-fleet deploy against a DB with many catalogs could grind past the
    Cloud Run startup-probe window entirely.

    Submitted as a delayed one-shot background task instead: each process
    reaches readiness before this work starts, then checks a per-service /
    per-revision shared-property marker and makes one fleet-level lease
    attempt. Only the first winner for the current service revision runs the
    pipeline; later workers see the marker and return. The contributor-local
    advisory locks still provide per-contributor single-flight safety.
    """

    name = "cold_boot_reconciliation"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.ALL
    lock_key: Optional[str] = None
    initial_delay_seconds = _env_float(
        "DYNASTORE_COLD_BOOT_INITIAL_DELAY_SECONDS",
        30.0,
    )

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    async def run(self, ctx: ServiceContext) -> None:
        from dynastore.modules.db_config.locking_tools import lease_leadership
        from dynastore.modules.presets.cold_boot import run_cold_boot

        engine = self._engine if self._engine is not None else ctx.engine
        if engine is None:
            logger.info(
                "Cold-boot reconciliation skipped; no database engine is available."
            )
            return

        marker_key = _cold_boot_revision_marker_key()
        if marker_key and await _cold_boot_revision_completed(engine, marker_key):
            logger.info(
                "Cold-boot reconciliation skipped; revision marker %r already set.",
                marker_key,
            )
            return

        try:
            async with lease_leadership(
                engine,
                "dynastore.cold_boot_reconciliation",
                name=self.name,
            ) as (is_leader, _lock_conn):
                if not is_leader:
                    logger.info(
                        "Cold-boot reconciliation skipped; another process "
                        "holds the fleet lease."
                    )
                    return
                if marker_key and await _cold_boot_revision_completed(
                    engine,
                    marker_key,
                ):
                    logger.info(
                        "Cold-boot reconciliation skipped after lease; "
                        "revision marker %r already set.",
                        marker_key,
                    )
                    return
                await run_cold_boot(engine, probe=_ColdBootMemoryProbe())
                if marker_key:
                    await _mark_cold_boot_revision_completed(engine, marker_key)
        except Exception:
            logger.error(
                "Cold-boot reconciliation failed; some presets or IdP config "
                "may not be seeded.",
                exc_info=True,
            )
        else:
            logger.info(
                "--- [main.py] Cold-boot reconciliation complete (background). ---"
            )


def _cold_boot_revision_marker_key() -> Optional[str]:
    """Return the shared-property key that makes cold boot once-per-revision.

    Cloud Run provides ``K_SERVICE`` and ``K_REVISION``. Outside Cloud Run we do
    not set a durable marker: without a revision identity, a DB marker could
    incorrectly suppress cold-boot self-heal across ordinary local/on-prem
    restarts.
    """
    service = os.getenv("K_SERVICE")
    revision = os.getenv("K_REVISION")
    if not service or not revision:
        return None
    return f"platform.cold_boot_reconciliation.completed.{service}.{revision}"


async def _cold_boot_revision_completed(engine: Any, marker_key: str) -> bool:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    try:
        async with managed_transaction(engine) as conn:
            value = await DQLQuery(
                "SELECT key_value FROM catalog.shared_properties "
                "WHERE key_name = :key_name",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, key_name=marker_key)
            return value == "true"
    except Exception as exc:
        logger.debug(
            "Cold-boot revision marker read failed for %r (%s); proceeding.",
            marker_key,
            exc,
        )
        return False


async def _mark_cold_boot_revision_completed(engine: Any, marker_key: str) -> None:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        await DQLQuery(
            """
            INSERT INTO catalog.shared_properties (key_name, key_value, owner_code)
            VALUES (:key_name, 'true', 'cold_boot_reconciliation')
            ON CONFLICT (key_name) DO UPDATE SET
                key_value = EXCLUDED.key_value,
                owner_code = EXCLUDED.owner_code;
            """,
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(conn, key_name=marker_key)


class _ColdBootMemoryProbe:
    """Emit per-contributor RSS diagnostics when watchdog diagnostics are enabled."""

    def __init__(self) -> None:
        self._rss_before: dict[str, Optional[int]] = {}

    async def __call__(
        self,
        event: str,
        contributor: Any,
        elapsed_seconds: Optional[float],
        error: Optional[BaseException],
    ) -> None:
        name = getattr(contributor, "name", "<unknown>")
        before = self._rss_before.pop(name, None) if event == "after" else None
        try:
            config = await load_memory_watchdog_config()
        except Exception:
            return
        if not config.diagnostic_tracemalloc_enabled:
            return

        rss_bytes = read_process_rss_bytes()
        if event == "before":
            self._rss_before[name] = rss_bytes
            logger.info(
                "cold_boot[diagnostic]: contributor %r starting "
                "(priority=%s, rss=%s)",
                name,
                getattr(contributor, "priority", "<unknown>"),
                _format_mib(rss_bytes),
            )
            return

        budget_bytes = _cold_boot_budget_bytes(config)
        ratio = (rss_bytes / budget_bytes) if rss_bytes is not None and budget_bytes else None
        delta = (
            rss_bytes - before
            if rss_bytes is not None and before is not None
            else None
        )
        level = logging.WARNING
        if error is not None or (
            ratio is not None and ratio >= config.critical_ratio
        ):
            level = logging.ERROR

        logger.log(
            level,
            "cold_boot[diagnostic]: contributor %r finished "
            "elapsed=%.3fs rss_before=%s rss_after=%s delta=%s budget=%s "
            "ratio=%s error=%s",
            name,
            elapsed_seconds if elapsed_seconds is not None else 0.0,
            _format_mib(before),
            _format_mib(rss_bytes),
            _format_signed_mib(delta),
            _format_mib(budget_bytes),
            _format_ratio(ratio),
            type(error).__name__ if error is not None else "none",
        )


def _cold_boot_budget_bytes(config: Any) -> Optional[int]:
    if getattr(config, "limit_mb", None) is not None:
        return int(config.limit_mb) * 1024 * 1024
    budget_mb = resolve_watchdog_budget_mb()
    if budget_mb is None:
        return None
    return budget_mb * 1024 * 1024


def _format_mib(value: Optional[int]) -> str:
    if value is None:
        return "unknown"
    return f"{value / (1024 * 1024):.0f}MiB"


def _format_signed_mib(value: Optional[int]) -> str:
    if value is None:
        return "unknown"
    return f"{value / (1024 * 1024):.0f}MiB"


def _format_ratio(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    return f"{value * 100:.0f}%"


# --- Combined Application Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the complete application lifecycle in the correct order:
    1. Start Modules (e.g., connect to DB).
    2. Start Web Extensions (e.g., mount API routers).
    3. The application runs.
    4. Shutdown Web Extensions.
    5. Shutdown Modules.
    """
    # The outer context manager initializes foundational modules.
    # They populate `app.state` with core services. TasksModule.lifespan
    # already manages task singletons and the dispatcher internally.
    # Expose the FastAPI app on app.state so modules that legitimately
    # need to register top-level HTTP routes (e.g. LocalUploadModule's
    # /local-upload, /local-download endpoints) can reach it without
    # being promoted to extensions.
    app.state.app = app

    # Memory watchdog (#2946): an OOM kill sends SIGKILL straight to the
    # process, so nothing here can catch or drain it. This proactive
    # RSS poll is the only way to turn the climb leading up to a kill into
    # a monitored log-based error before it happens. Enabled by default via
    # MemoryWatchdogConfig; started outside (and independent of) module/
    # extension lifespans so it observes memory pressure from the very
    # start of the process, not just once modules finish booting (the
    # platform config store is not yet reachable at this point either way,
    # so this always resolves the config's defaults). The effective memory
    # budget (explicit limit_mb, else cgroup auto-detection, else inert) is
    # resolved lazily on the service's own first tick, once the config store
    # is reachable — see tools/memory_watchdog.py.
    _mem_watchdog_service = await build_memory_watchdog_service()
    _mem_watchdog_shutdown = asyncio.Event()
    _mem_watchdog_supervisor = BackgroundSupervisor()
    if _mem_watchdog_service is not None:
        _mem_watchdog_supervisor.register(_mem_watchdog_service)
        _mem_watchdog_supervisor.start(
            ServiceContext(
                engine=None,
                shutdown=_mem_watchdog_shutdown,
                is_ephemeral=False,
                name=os.environ.get("SERVICE_NAME", "dynastore"),
            )
        )

    # Cold-boot reconciliation supervisor (#3002): started once the DB engine
    # is available below, stopped alongside the memory watchdog in `finally`.
    _cold_boot_shutdown = asyncio.Event()
    _cold_boot_supervisor = BackgroundSupervisor()

    try:
        async with modules_lifespan(app.state):
            logger.info("--- [main.py] Modules are active. ---")
            # Extensions can now reliably access services from modules and task instances.
            async with extensions_lifespan(app):
                # Flush any pending policy/role registrations from extensions
                from dynastore.models.protocols.policies import PermissionProtocol
                pm = get_protocol(PermissionProtocol)
                # Announce the authorization posture exactly once at startup. The
                # IAM extension is always_on whenever its wheel is installed, so a
                # service meant to be open (e.g. a SCOPE that excludes the iam
                # extension) can silently flip to deny-by-default if it runs a
                # stale or wrong-SCOPE image that still carries the wheel. Surfacing
                # the posture here turns that into an obvious log line instead of
                # mysterious "Deny by Default" 403s on every non-public route.
                _scope = os.environ.get("SCOPE", "<unset>")
                if pm is None:
                    logger.warning(
                        "Authorization DISABLED - no PermissionProtocol registered; "
                        "all requests run unauthenticated (open access) [SCOPE=%s]. "
                        "Expected for open scopes built without the iam extension.",
                        _scope,
                    )
                else:
                    logger.warning(
                        "Authorization ENFORCED (deny-by-default) via %s "
                        "[SCOPE=%s, IdP=%s]. For an open/no-auth deployment, build "
                        "WITHOUT the iam extension (e.g. a catalog-only scope).",
                        type(pm).__name__,
                        _scope,
                        os.environ.get("IDP_ISSUER_URL") or "<none>",
                    )
                # Run all registered cold-boot contributors in descending priority
                # order, off the critical path (#3002). This work is idempotent
                # self-heal/reconciliation — it does not gate /health or /ready —
                # so it is submitted as a background task rather than awaited
                # here. Each contributor is fail-soft — a failure does not abort
                # the pipeline. Fully agnostic: no module-specific (iam/web/auth)
                # names here.
                from dynastore.models.protocols import DatabaseProtocol
                _db = get_protocol(DatabaseProtocol)
                _engine = _db.engine if _db else None
                _cold_boot_supervisor.register(_ColdBootReconciliationService(_engine))
                _cold_boot_supervisor.start(
                    ServiceContext(
                        engine=_engine,
                        shutdown=_cold_boot_shutdown,
                        is_ephemeral=False,
                        name=os.environ.get("SERVICE_NAME", "dynastore"),
                    )
                )
                logger.info("--- [main.py] Web Extensions are active. Application is running. ---")
                yield

        logger.info("--- [main.py] Application shutdown complete. ---")
    finally:
        _cold_boot_shutdown.set()
        await _cold_boot_supervisor.stop()
        if _mem_watchdog_service is not None:
            _mem_watchdog_shutdown.set()
            await _mem_watchdog_supervisor.stop()

# --- Main Application Creation ---

app = FastAPI(
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    root_path=os.getenv("API_ROOT_PATH", "/"),
    title=os.getenv("TITLE", "Agro-Informatics Platform - Catalog Services API"),
    description=os.getenv(
        "DESCRIPTION",
        "Agro-Informatics Platform - Catalog Services",
    ),
    version=VERSION,
    docs_url=None, # We will serve custom docs
    redoc_url=None, # We will serve custom redoc
    swagger_ui_parameters={"defaultModelsExpandDepth": -1} # Optional: hide models by default
)

@app.get("/api", include_in_schema=False)
async def get_api_document(f: Optional[str] = None, request: Request = None):  # type: ignore[assignment]
    """OGC API - Common canonical API document.

    Returns the OpenAPI schema with OAS 3.0 by default (what FastAPI emits
    natively). Callers can opt into OAS 3.1 with ``?f=oas31`` or
    ``Accept: application/vnd.oai.openapi+json;version=3.1``.

    OAS 3.1 upgrades just the ``openapi`` field — the underlying schema
    structures are 3.0-compatible. Callers needing strict 3.1 constructs
    (e.g. ``type: ["x","null"]`` over 3.0's ``nullable: true``) should
    treat this as a best-effort compatibility profile until FastAPI emits
    native 3.1.
    """
    accept = ""
    if request is not None:
        accept = request.headers.get("accept", "")
    want_oas31 = (
        (f or "").lower() == "oas31"
        or "application/vnd.oai.openapi+json;version=3.1" in accept.lower()
    )
    schema = app.openapi()
    if want_oas31:
        schema = {**schema, "openapi": "3.1.0"}
    return ORJSONResponse(content=schema)


@app.get("/health", tags=["Web Health"])
async def health_check():
    info = get_build_info()
    return {
        "name": app.title,
        "description": app.description,
        "version": info["version"],
        "commit": info["commit"],
        "build_time": info["build_time"],
        "status": "ok",
    }


@app.get("/ready", tags=["Web Health"])
async def readiness_check():
    """Deep readiness probe — checks every required backing service.

    Returns 200 with per-dependency status JSON when all configured
    required dependencies are reachable; 503 with the same structure
    when any required dependency is down.

    Dependencies that are not loaded in this deployment are reported as
    ``"disabled"`` rather than ``"failed"`` and do not affect the HTTP
    status code.
    """
    deps: dict = {}
    all_ok = True

    # --- PostgreSQL ---
    try:
        from dynastore.models.protocols.database import DatabaseProtocol

        # Providers are sorted ascending by `priority`, so the first one does
        # not necessarily hold the async engine: on services that load the sync
        # DatastoreModule (priority 7) it precedes DBService (priority 10) and
        # always reports `async_engine` as None. Probing only that leader would
        # report "disabled" — which leaves the status code at 200 — and a dead
        # database would be announced as ready. Walk the providers for the async
        # engine, as protocol_helpers.get_engine() does for the same reason.
        async_engine = None
        for provider in get_protocols(DatabaseProtocol):
            async_engine = provider.async_engine
            if async_engine is not None:
                break

        if async_engine is None:
            deps["postgres"] = {"status": "disabled"}
        else:
            from sqlalchemy import text as sa_text
            async with asyncio.timeout(2):
                async with async_engine.connect() as conn:
                    await conn.execute(sa_text("SELECT 1"))
            deps["postgres"] = {"status": "ok"}
    except TimeoutError as exc:
        all_ok = False
        deps["postgres"] = {"status": "failed", "detail": "timed out"}
        logger.warning("readiness: postgres timeout: %s", exc)
    except Exception as exc:
        all_ok = False
        deps["postgres"] = {"status": "failed", "detail": str(exc)}
        logger.warning("readiness: postgres error: %s", exc)

    # --- Elasticsearch / OpenSearch ---
    try:
        from dynastore.modules.elasticsearch.client import get_client as _get_es
        es = _get_es()
        if es is None:
            deps["elasticsearch"] = {"status": "disabled"}
        else:
            async with asyncio.timeout(2):
                reachable = await es.ping()
            if reachable:
                deps["elasticsearch"] = {"status": "ok"}
            else:
                all_ok = False
                deps["elasticsearch"] = {"status": "failed", "detail": "ping returned false"}
    except TimeoutError as exc:
        all_ok = False
        deps["elasticsearch"] = {"status": "failed", "detail": "timed out"}
        logger.warning("readiness: elasticsearch timeout: %s", exc)
    except ImportError:
        deps["elasticsearch"] = {"status": "disabled"}
    except Exception as exc:
        all_ok = False
        deps["elasticsearch"] = {"status": "failed", "detail": str(exc)}
        logger.warning("readiness: elasticsearch error: %s", exc)

    # --- Valkey ---
    try:
        from dynastore.tools.cache import get_cache_manager
        from dynastore.tools.cache_valkey import ValkeyCacheBackend, _CACHE_DEPS_OK
        if not _CACHE_DEPS_OK:
            deps["valkey"] = {"status": "disabled"}
        else:
            manager = get_cache_manager()
            valkey_backend = None
            try:
                backend = manager.get_async_backend()
                if isinstance(backend, ValkeyCacheBackend):
                    valkey_backend = backend
            except RuntimeError:
                pass  # no backends registered
            if valkey_backend is None:
                deps["valkey"] = {"status": "disabled"}
            else:
                async with asyncio.timeout(2):
                    ok = await valkey_backend.ping()
                if ok:
                    deps["valkey"] = {"status": "ok"}
                else:
                    all_ok = False
                    deps["valkey"] = {"status": "failed", "detail": "ping returned false"}
    except TimeoutError as exc:
        all_ok = False
        deps["valkey"] = {"status": "failed", "detail": "timed out"}
        logger.warning("readiness: valkey timeout: %s", exc)
    except ImportError:
        deps["valkey"] = {"status": "disabled"}
    except Exception as exc:
        all_ok = False
        deps["valkey"] = {"status": "failed", "detail": str(exc)}
        logger.warning("readiness: valkey error: %s", exc)

    # --- Self-recycle draining flag (geoid#2946 / #2924) ---
    # Set by the memory watchdog's self-recycle lever when this worker has
    # decided to gracefully SIGTERM itself ahead of an OOM kill. Always
    # folded into the real readiness signal (not gated by any config flag —
    # unlike the readiness-shed middleware, which IS opt-in) since this is
    # the platform's own probe and must reflect true state.
    if is_draining():
        all_ok = False
        deps["draining"] = {"status": "failed", "detail": "worker is draining for self-recycle"}

    payload = {"status": "ready" if all_ok else "not_ready", "dependencies": deps}
    from fastapi.responses import JSONResponse
    return JSONResponse(
        content=payload,
        status_code=200 if all_ok else 503,
    )


# /docs is registered later by ``documentation.service.configure_swagger_ui``,
# which builds the custom Swagger UI (theme + OAuth2 redirect handler).
# /redoc is currently not exposed; if reintroduced it should also live in the
# documentation extension so all docs-rendering routes share one owner.

# Extensions register their own middleware (IamMiddleware, TenantScopeMiddleware,
# SessionMiddleware, CORS, GZip, proxy-headers, slash-redirect, ...) inside
# bootstrap_app. Starlette makes the *last*-added middleware the *outermost*
# one, so bootstrap_app must run before we add the correlation-id and global
# exception-handling middleware below — otherwise an exception raised inside
# one of those extension middlewares would bypass both and hit Starlette's
# bare ServerErrorMiddleware instead of the platform JSON error shape.
bootstrap_app(app)

from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
setup_exception_handlers(app)

# Bounds the inbound body of the synchronous bulk item-POST before it is
# parsed (#2657). Must sit inside (added before) CorrelationIdMiddleware so
# a rejected request still gets stamped with X-Request-ID, and outside the
# routing layer so it can reject a request before the route ever reads its
# body.
from dynastore.extensions.tools.body_size_limit import SyncIngestBodyLimitMiddleware
app.add_middleware(SyncIngestBodyLimitMiddleware)

# Sheds new requests with 503 while this worker is draining for a memory-
# watchdog self-recycle (Lever B, geoid#2946 / #2924). Added AFTER (so it
# sits OUTSIDE) SyncIngestBodyLimitMiddleware — under Starlette's insert-at-
# front semantics the last-added middleware runs first — so a draining worker
# sheds with a cheap 503 before SyncIngestBodyLimitMiddleware eagerly buffers
# a (chunked) body it will refuse anyway, which would only pile memory onto a
# worker recycling precisely because of memory pressure. Still added before
# CorrelationIdMiddleware, so it stays inside it and shed 503s keep X-Request-ID.
from dynastore.extensions.tools.readiness_shed_middleware import ReadinessShedMiddleware
app.add_middleware(ReadinessShedMiddleware)

# Correlation ID middleware must be the outermost middleware so it stamps
# X-Request-ID on every response — including error responses produced by
# GlobalExceptionHandlingMiddleware for exceptions raised inside any
# extension-registered middleware.
app.add_middleware(CorrelationIdMiddleware)

logger.info("--- [main.py] FastAPI application instance created. ---")


async def run_worker():
    """
    Initializes the application's modules via their lifespans and runs as a
    long-lived worker process.

    The TasksModule lifespan is responsible for starting the dispatcher and
    queue listener internally — no schema or dispatcher knowledge is needed here.
    Dispatcher concurrency is configured by the TasksModule (dispatcher batch
    size); the worker itself is a single process running one event loop.
    """
    logger.info("--- [main.py] Initializing worker context... ---")

    app_state = SimpleNamespace()

    # 1. Discover all modules based on SCOPE environment variable.
    discover_modules()

    # 2. Instantiate all discovered modules using the shared state object.
    instantiate_modules(app_state)

    # 3. Run module lifespans — TasksModule will start the dispatcher and queue listener.
    async with modules_lifespan(app_state):
        logger.info("--- [main.py] Worker running. Dispatcher managed by TasksModule. ---")
        # Block until manually terminated (SIGTERM will set the shutdown event
        # inside TasksModule and clean up gracefully).
        shutdown_event = asyncio.Event()
        await shutdown_event.wait()

    logger.info("--- [main.py] Worker shut down cleanly. ---")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="DynaStore Application Entry Point")
    parser.add_argument("--worker", action="store_true", help="Run as a background worker instead of API server")
    args = parser.parse_args()

    if args.worker:
        asyncio.run(run_worker())
    else:
        print("This script is intended to be imported by an ASGI server (for API) or run with --worker (for Worker).")
