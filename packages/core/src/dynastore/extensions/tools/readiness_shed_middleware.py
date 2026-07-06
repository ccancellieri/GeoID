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

"""Pure-ASGI middleware that sheds new requests while this worker is
draining ahead of a graceful self-recycle (geoid#2946 / #2924).

Cloud Run exposes no readiness gate for HTTP services — only a startup
probe and (optionally) a liveness probe — so flipping ``/ready`` to 503
does not, by itself, stop new requests from being routed to a worker that
has decided to recycle itself (see ``tools/memory_watchdog.py``'s
self-recycle lever) and is about to receive ``SIGTERM``. This middleware is
the actual traffic-shedding lever on Cloud Run: while
``MemoryWatchdogConfig.readiness_shed_enabled`` is on AND this worker's
draining flag (``tools/serving_state.py``) is set, every ordinary request
is answered 503 + ``Retry-After`` without ever reaching the app, so a
well-behaved client backs off and retries — landing, on a multi-instance
deployment, against a different (non-draining) instance or worker instead
of racing the drain window. ``/health`` and ``/ready`` are excluded: the
platform's own probes must always see the true state, draining or not.

Deliberately NOT a ``starlette.middleware.base.BaseHTTPMiddleware``
subclass, mirroring ``body_size_limit.py`` — this only ever needs to
inspect the path up front and, in the common (non-draining) case, do
nothing at all, so a pure-ASGI callable avoids that base class's
whole-response buffering for no benefit here.
"""

from __future__ import annotations

from fastapi import status
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from dynastore.tools.memory_watchdog import load_memory_watchdog_config
from dynastore.tools.serving_state import is_draining

# The platform's own probes must always see the true state, draining or
# not — never shed these paths regardless of readiness_shed_enabled.
_EXCLUDED_PATHS = frozenset({"/health", "/ready"})

_RETRY_AFTER_SECONDS = "5"


def _shedding_response() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        headers={"Retry-After": _RETRY_AFTER_SECONDS},
        content={
            "detail": {
                "title": "Worker draining",
                "status": status.HTTP_503_SERVICE_UNAVAILABLE,
                "detail": (
                    "This worker is gracefully recycling ahead of a memory "
                    "limit and is shedding new requests; retry shortly."
                ),
            }
        },
    )


class ReadinessShedMiddleware:
    """Sheds ordinary requests with 503 while this worker is draining."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path", "") in _EXCLUDED_PATHS:
            await self.app(scope, receive, send)
            return

        # Common case: not draining — no config lookup needed at all.
        if not is_draining():
            await self.app(scope, receive, send)
            return

        # Fail-open: never let this middleware turn a draining worker into a
        # source of 500s on the serving path. load_memory_watchdog_config
        # already swallows its own errors and returns defaults today, but
        # keeping the guarantee self-contained here means a future change
        # there cannot regress into fail-closed shedding.
        try:
            config = await load_memory_watchdog_config()
            shed = config.readiness_shed_enabled
        except Exception:  # noqa: BLE001 — deliberate serving-path fail-open
            shed = False

        if not shed:
            await self.app(scope, receive, send)
            return

        response = _shedding_response()
        await response(scope, receive, send)
