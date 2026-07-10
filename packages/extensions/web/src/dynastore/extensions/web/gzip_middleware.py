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

"""GZip middleware that leaves already-compressed image tiles alone.

Raster tile bodies (``image/png``, ``image/jpeg``, ``image/tiff``) are
already deflate-compressed at the format level; re-running them through
gzip at Starlette's default compresslevel 9 burns CPU and briefly doubles
the response in memory for a payload that does not shrink. MVT
(``application/vnd.mapbox-vector-tile``) and JSON responses compress well
and keep going through gzip, just at level 6 instead of 9 -- close to the
same ratio for a fraction of the CPU.
"""

from starlette.datastructures import Headers
from starlette.middleware.gzip import (
    DEFAULT_EXCLUDED_CONTENT_TYPES,
    GZipMiddleware,
    GZipResponder,
    IdentityResponder,
)
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# Starlette already skips text/event-stream (SSE must not be buffered);
# images are excluded for the same reason gzip would be wasted work here.
_EXCLUDED_CONTENT_TYPES = DEFAULT_EXCLUDED_CONTENT_TYPES + ("image/",)


def _is_excluded(headers: Headers) -> bool:
    return headers.get("content-type", "").startswith(_EXCLUDED_CONTENT_TYPES)


class _SkipImagesIdentityResponder(IdentityResponder):
    """Same as Starlette's IdentityResponder, with image/* also excluded."""

    async def send_with_compression(self, message: Message) -> None:
        if message["type"] == "http.response.start":
            self.initial_message = message
            headers = Headers(raw=self.initial_message["headers"])
            self.content_encoding_set = "content-encoding" in headers
            self.content_type_is_excluded = _is_excluded(headers)
            return
        await super().send_with_compression(message)


class _SkipImagesGZipResponder(GZipResponder):
    """Same as Starlette's GZipResponder, with image/* also excluded.

    All the actual compression/streaming state handling (multi-chunk
    bodies, empty bodies, headers) is inherited unchanged; only the
    per-response content-type classification is widened.
    """

    async def send_with_compression(self, message: Message) -> None:
        if message["type"] == "http.response.start":
            self.initial_message = message
            headers = Headers(raw=self.initial_message["headers"])
            self.content_encoding_set = "content-encoding" in headers
            self.content_type_is_excluded = _is_excluded(headers)
            return
        await super().send_with_compression(message)


class TileAwareGZipMiddleware(GZipMiddleware):
    """GZipMiddleware variant that never compresses image/* responses."""

    def __init__(self, app: ASGIApp, minimum_size: int = 500, compresslevel: int = 6) -> None:
        super().__init__(app, minimum_size=minimum_size, compresslevel=compresslevel)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":  # pragma: no cover
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        responder: ASGIApp
        if "gzip" in headers.get("Accept-Encoding", ""):
            responder = _SkipImagesGZipResponder(self.app, self.minimum_size, compresslevel=self.compresslevel)
        else:
            responder = _SkipImagesIdentityResponder(self.app, self.minimum_size)

        await responder(scope, receive, send)
