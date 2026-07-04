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

"""Pure-ASGI middleware bounding the inbound body of the synchronous bulk
item-POST endpoint (#2657).

STAC, OGC Features and Records item-POST handlers declare a typed Pydantic
body parameter, so FastAPI fully parses and validates the whole request
body before ``solve_dependencies`` runs — a ``Depends``-based size guard
would only see the payload after it has already been materialized in
memory. This middleware wraps ``receive`` instead, so an oversize body is
rejected with 413 before the route ever reads it, before it is parsed, and
before the caller's connection is asked to send any more of it.

Deliberately NOT a ``starlette.middleware.base.BaseHTTPMiddleware``
subclass — that base class buffers the whole request body into memory
before ``dispatch`` runs, which would defeat the purpose here.

Scope note: this bounds peak *memory* (buffered bytes never exceed the
cap), not connection *duration* — a client trickling bytes slowly while
staying under the cap is not stopped here; per-request/slow-client
timeouts remain the ingress/proxy layer's responsibility.
"""

import re
from typing import List, Optional

from fastapi import status
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_MIB = 1024 * 1024

# Fallback cap (MiB) used when the platform configs service can't be
# reached (e.g. test/stub contexts) — mirrors ``IngestionPluginConfig.
# sync_ingest_max_body_mb``'s own default so behaviour is unchanged when
# the config simply hasn't been customised.
_DEFAULT_MAX_BODY_BYTES = 64 * _MIB

# Matches the synchronous bulk item-ingest endpoint across every OGC
# protocol extension mounted under its own prefix — e.g.
# ``/stac/catalogs/{catalog_id}/collections/{collection_id}/items`` or
# ``/features/catalogs/{catalog_id}/collections/{collection_id}/items`` —
# and nothing else. The path must end exactly in
# ``/collections/<id>/items`` (optional trailing slash), so asset-binary
# upload routes (``.../assets/...``, ``.../upload``) and other collection
# sub-resources (``.../search``, ``.../assets:bulk``, ...) never match.
_ITEMS_PATH_RE = re.compile(r"^/[^/]+/catalogs/[^/]+/collections/[^/]+/items/?$")

_ERROR_TYPE = "https://docs.dynastore.io/errors/ingest-body-too-large"


def _too_large_response(max_body_mb: int) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        content={
            "detail": {
                "type": _ERROR_TYPE,
                "title": "Request body too large for synchronous ingest",
                "status": status.HTTP_413_CONTENT_TOO_LARGE,
                "detail": (
                    f"The request body exceeds the {max_body_mb} MiB limit "
                    "accepted by the synchronous bulk item-POST endpoint. "
                    "Submit very large FeatureCollections to the "
                    "asynchronous ingestion task instead, which streams "
                    "items from source without materializing the whole "
                    "payload in memory."
                ),
                "max_body_mb": max_body_mb,
            }
        },
    )


def _content_length(scope: Scope) -> Optional[int]:
    for key, value in scope.get("headers", []):
        if key == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


async def _resolve_max_body_bytes() -> int:
    """Resolve the configured cap, in bytes, from ``IngestionPluginConfig``.

    Falls back to ``_DEFAULT_MAX_BODY_BYTES`` when the platform configs
    service is unavailable — mirrors the defensive ``except: return
    config_cls()`` fallback used by other plugin config reads, so the
    bound still applies with a safe default in test / stub contexts.
    Reads through the platform configs service's own cache; no second
    cache is kept here.
    """
    try:
        from dynastore.models.protocols.platform_configs import (
            PlatformConfigsProtocol,
        )
        from dynastore.tasks.ingestion.ingestion_config import (
            IngestionPluginConfig,
        )
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is None:
            return _DEFAULT_MAX_BODY_BYTES
        cfg = await config_mgr.get_config(IngestionPluginConfig)
        assert isinstance(cfg, IngestionPluginConfig)
        return max(1, cfg.sync_ingest_max_body_mb) * _MIB
    except Exception:  # pragma: no cover - defensive fallback
        return _DEFAULT_MAX_BODY_BYTES


class SyncIngestBodyLimitMiddleware:
    """Rejects oversize bodies on the synchronous bulk item-POST before
    they are parsed, steering large loads to the async ingestion task.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http"
            or scope.get("method") != "POST"
            or not _ITEMS_PATH_RE.match(scope.get("path", ""))
        ):
            await self.app(scope, receive, send)
            return

        max_body_bytes = await _resolve_max_body_bytes()

        # Fast path: a declared Content-Length lets us reject before
        # reading a single body byte.
        content_length = _content_length(scope)
        if content_length is not None:
            if content_length > max_body_bytes:
                response = _too_large_response(max_body_bytes // _MIB)
                await response(scope, receive, send)
                return
            # Within budget — forward untouched, no need to intercept.
            await self.app(scope, receive, send)
            return

        # No (or chunked) Content-Length: pre-drain ``receive`` ourselves,
        # summing bytes across ``http.request`` messages. The instant the
        # cumulative total exceeds the cap we stop draining and answer
        # 413 directly without ever forwarding control to the app, so
        # peak buffered memory stays bounded to ~max_body_bytes. Under
        # budget, the drained messages are replayed to the app so its
        # behaviour is otherwise unchanged.
        buffered: List[Message] = []
        total = 0
        over_limit = False
        while True:
            message = await receive()
            buffered.append(message)
            if message["type"] != "http.request":
                # e.g. http.disconnect — nothing more to drain.
                break
            total += len(message.get("body") or b"")
            if total > max_body_bytes:
                over_limit = True
                break
            if not message.get("more_body", False):
                break

        if over_limit:
            response = _too_large_response(max_body_bytes // _MIB)
            await response(scope, receive, send)
            return

        await self.app(scope, _replay_receive(buffered, receive), send)


def _replay_receive(buffered: List[Message], receive: Receive) -> Receive:
    """Build a ``receive`` callable that first yields *buffered* messages,
    then falls through to the original *receive* for anything further."""
    index: List[int] = [0]

    async def _receive() -> Message:
        i = index[0]
        if i < len(buffered):
            index[0] = i + 1
            return buffered[i]
        return await receive()

    return _receive
