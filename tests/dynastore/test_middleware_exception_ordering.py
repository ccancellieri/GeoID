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

"""Regression test for #2830 — middleware assembly order in ``main.py``.

Starlette makes the *last*-added middleware the *outermost* one. Extension
middleware (``IamMiddleware``, ``TenantScopeMiddleware``, ``SessionMiddleware``,
CORS, GZip, proxy-headers, slash-redirect, ...) is registered inside
``bootstrap_app``. Correlation-id propagation and global JSON exception
handling must wrap *around* all of that, or an exception raised inside an
extension middleware bypasses both and falls through to Starlette's bare
``ServerErrorMiddleware`` — no platform error shape, no ``X-Request-ID``.

This test forces an exception inside ``IamMiddleware`` (a real
bootstrap-registered, always-on extension middleware) through the actual
application assembled by ``dynastore.main`` and asserts the response still
carries the platform JSON error shape and the ``X-Request-ID`` header.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from dynastore.main import app
from dynastore.extensions.iam.middleware import IamMiddleware


def test_exception_inside_extension_middleware_reaches_platform_error_handler(monkeypatch):
    async def _raise(self, request, call_next):
        raise RuntimeError("forced failure inside IamMiddleware")

    monkeypatch.setattr(IamMiddleware, "dispatch", _raise)

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/health")

    assert resp.status_code == 500
    body = resp.json()
    assert isinstance(body.get("detail"), str) and body["detail"]

    assert "X-Request-ID" in resp.headers
