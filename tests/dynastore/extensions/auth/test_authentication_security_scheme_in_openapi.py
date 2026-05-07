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

"""Unit tests verifying ``/auth/userinfo``, ``/auth/me``, and ``/auth/debug``
declare ``HTTPBearer`` as their security scheme in the generated OpenAPI
document.

The previous implementation used ``authorization: str = Header(None)`` to
read the bearer token. That keeps the runtime check working (the
middleware enforces the actual authorization) but FastAPI does not infer
a security scheme from a bare ``Header`` parameter, so Swagger UI never
renders a lock icon and the operation has no link to the
``securitySchemes.HTTPBearer`` entry that ``build_iam_openapi_schema``
adds. Switching the dependency to ``HTTPBearer`` ties them together.
"""

from __future__ import annotations

from typing import List

from fastapi import FastAPI

from dynastore.extensions.auth.authentication import Authentication
from dynastore.extensions.iam.service import build_iam_openapi_schema


def _security_for_path(schema: dict, path: str, method: str = "get") -> List[dict]:
    """Return the *effective* security requirement list for an operation.

    Per OpenAPI 3.0 §4.7.5, an operation inherits the top-level
    ``security`` field unless it overrides it. Tests assert the request
    requires ``HTTPBearer`` either way — both sources are valid.
    """
    op = schema["paths"][path][method]
    if "security" in op:
        return op["security"]
    return schema.get("security", [])


def _assert_httpbearer_required(schema: dict, path: str, method: str = "get") -> None:
    security = _security_for_path(schema, path, method)
    schemes_in_or = {key for entry in security for key in entry.keys()}
    assert "HTTPBearer" in schemes_in_or, (
        f"{method.upper()} {path}: expected ``HTTPBearer`` in the security "
        f"requirement list (operation-level or inherited from top-level), "
        f"got {security!r}"
    )


def _build_app() -> FastAPI:
    app = FastAPI(title="t", version="0.0.0", description="d")
    auth_ext = Authentication()
    auth_ext._setup_routes()
    app.include_router(auth_ext.router)
    return app


def test_userinfo_carries_httpbearer_security(monkeypatch):
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_build_app())
    _assert_httpbearer_required(schema, "/auth/userinfo", "get")


def test_me_carries_httpbearer_security(monkeypatch):
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_build_app())
    _assert_httpbearer_required(schema, "/auth/me", "get")


def test_debug_carries_httpbearer_security(monkeypatch):
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_build_app())
    _assert_httpbearer_required(schema, "/auth/debug", "get")
