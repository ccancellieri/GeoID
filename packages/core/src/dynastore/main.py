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
from typing import Optional
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
)
from dynastore.extensions.lifespan import lifespan as extensions_lifespan
from dynastore._version import VERSION, get_build_info
from dynastore.extensions.tools.fast_api import ORJSONResponse
from dynastore.extensions.bootstrap import bootstrap_app
from dynastore.modules.concurrency import set_concurrency_backend
from dynastore.tools.background_service import BackgroundSupervisor, ServiceContext
from dynastore.tools.correlation import _correlation_id_var, set_correlation_id
from dynastore.tools.memory_watchdog import build_memory_watchdog_service_from_env
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
    # a monitored log-based error before it happens. Disabled unless an
    # operator opts in via MEMORY_WATCHDOG_LIMIT_MB; started outside (and
    # independent of) module/extension lifespans so it observes memory
    # pressure from the very start of the process, not just once modules
    # finish booting.
    _mem_watchdog_service = build_memory_watchdog_service_from_env()
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
                # order. Each contributor is fail-soft — a failure does not abort
                # startup. Fully agnostic: no module-specific (iam/web/auth) names here.
                try:
                    from dynastore.modules.presets.cold_boot import run_cold_boot
                    from dynastore.models.protocols import DatabaseProtocol
                    _db = get_protocol(DatabaseProtocol)
                    _engine = _db.engine if _db else None
                    await run_cold_boot(_engine)
                except Exception:
                    logger.error(
                        "Cold-boot orchestrator raised an unexpected error; "
                        "some presets or IdP config may not be seeded.",
                        exc_info=True,
                    )
                logger.info("--- [main.py] Web Extensions are active. Application is running. ---")
                yield

        logger.info("--- [main.py] Application shutdown complete. ---")
    finally:
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
        db = get_protocol(DatabaseProtocol)
        if db is None or db.async_engine is None:
            deps["postgres"] = {"status": "disabled"}
        else:
            from sqlalchemy import text as sa_text
            async with asyncio.timeout(2):
                async with db.async_engine.connect() as conn:
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