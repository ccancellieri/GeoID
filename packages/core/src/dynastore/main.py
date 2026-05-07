
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

import logging
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
from dynastore import modules, extensions, tasks
from dynastore._version import VERSION, get_build_info
from dynastore.extensions.tools.fast_api import ORJSONResponse
from dynastore.extensions.bootstrap import bootstrap_app
from dynastore.modules.concurrency import set_concurrency_backend
from dynastore.tools.correlation import _correlation_id_var, set_correlation_id
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
# bodies) and every 4xx at WARNING — including idempotent 404s that our
# task code catches and treats as success (see
# tasks/elasticsearch/tasks.py:84-87). Without this cap, a dev run with
# LOG_LEVEL=DEBUG produces hundreds of index_not_found_exception body
# dumps during bulk delete flows. Set OPENSEARCH_LOG_LEVEL=WARNING (or
# INFO) to tune; default ERROR hides routine 404s but still surfaces
# auth / connectivity failures.
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
    async with modules.lifespan(app.state):
        logger.info("--- [main.py] Modules are active. ---")
        # Extensions can now reliably access services from modules and task instances.
        async with extensions.lifespan(app):
            # Flush any pending policy/role registrations from extensions
            from dynastore.models.protocols.policies import PermissionProtocol
            pm = modules.get_protocol(PermissionProtocol)
            flush = getattr(pm, "flush_pending_registrations", None)
            if flush is not None:
                await flush()
            logger.info("--- [main.py] Web Extensions are active. Application is running. ---")
            yield

    logger.info("--- [main.py] Application shutdown complete. ---")

# --- Main Application Creation ---

app = FastAPI(
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    root_path=os.getenv("API_ROOT_PATH", "/"),
    title=os.getenv("TITLE", "Agro-Informatics Platform - Catalog Services API"),
    description=os.getenv(
        "DESCRIPTION",
        "Multi-tenant, OGC-compliant geospatial data platform implementing OGC API Features (Parts 1-4), "
        "STAC API 1.0.0, Processes, Records, Tiles, Maps, Coverages, and Dimensions with CQL2 filtering, "
        "multi-CRS support, and transaction capabilities.",
    ),
    version=VERSION,
    docs_url=None, # We will serve custom docs
    redoc_url=None, # We will serve custom redoc
    swagger_ui_parameters={"defaultModelsExpandDepth": -1} # Optional: hide models by default
)

# Add correlation ID middleware
app.add_middleware(CorrelationIdMiddleware)

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

# Custom Swagger UI to include logo
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    from fastapi.openapi.docs import get_swagger_ui_html
    return get_swagger_ui_html(
        openapi_url=app.openapi_url or "/openapi.json",
        title=app.title + " - Swagger UI",
        swagger_favicon_url="/web/static/dynastore.png"
    )

# For ReDoc we can also customize similarly.
@app.get("/redoc", include_in_schema=False)
async def custom_redoc_html():
    from fastapi.openapi.docs import get_redoc_html
    return get_redoc_html(
        openapi_url=app.openapi_url or "/openapi.json",
        title=app.title + " - ReDoc",
        redoc_favicon_url="/web/static/dynastore.png"
    )


from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
setup_exception_handlers(app)
bootstrap_app(app)

logger.info("--- [main.py] FastAPI application instance created. ---")


async def run_worker(concurrency: int = 1):
    """
    Initializes the application's modules via their lifespans and runs as a
    long-lived worker process.

    The TasksModule lifespan is responsible for starting the dispatcher and
    queue listener internally — no schema or dispatcher knowledge is needed here.

    Args:
        concurrency: Reserved for future use. The dispatcher concurrency is
                     configured via the TasksModule.
    """
    logger.info("--- [main.py] Initializing worker context (concurrency=%d)... ---", concurrency)

    app_state = SimpleNamespace()

    # 1. Discover all modules based on SCOPE environment variable.
    modules.discover_modules()

    # 2. Instantiate all discovered modules using the shared state object.
    modules.instantiate_modules(app_state)

    # 3. Run module lifespans — TasksModule will start the dispatcher and queue listener.
    async with modules.lifespan(app_state):
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
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Number of concurrent worker processes (worker mode only, default: 1)")
    args = parser.parse_args()

    if args.worker:
        asyncio.run(run_worker(concurrency=args.concurrency))
    else:
        print("This script is intended to be imported by an ASGI server (for API) or run with --worker (for Worker).")