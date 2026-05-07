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

"""Unit tests for the unified ``Authentication & Authorization`` OpenAPI tag.

The IAM, auth, and admin extensions previously each declared their own
OpenAPI tag (``Authentication``, ``IAM Governance``, ``Authorization
Management``, ``Admin``, ...). Swagger UI groups operations by tag, so the
auth/identity surface ended up scattered across many sections. The fix
hoists a single shared tag — ``Authentication & Authorization`` — to every
router under ``/auth``, ``/iam``, and ``/admin/users``, and adds a matching
top-level ``tags`` entry inside ``build_iam_openapi_schema``.
"""

from __future__ import annotations

import re

from fastapi import FastAPI

from dynastore.extensions.iam.service import build_iam_openapi_schema
from dynastore.extensions.auth.authentication import Authentication
from dynastore.extensions.iam.authorization_api import (
    router as iam_admin_router,
    me_router as iam_me_router,
)
from dynastore.extensions.admin.admin_service import AdminService


_AUTHN_AUTHZ_TAG = "Authentication & Authorization"
_AUTHN_AUTHZ_PATH_RE = re.compile(r"^/(auth|iam|admin/users)(/|$)")


def _build_app_with_auth_routers() -> FastAPI:
    """Mount every auth/iam/admin router on a fresh FastAPI app.

    The routers are imported directly (not via the lifespan-driven
    extension loader) so the test exercises the metadata that ships
    with the routers themselves — exactly what FastAPI feeds to
    OpenAPI generation in production.
    """

    app = FastAPI(title="t", version="0.0.0", description="d")

    # Auth extension owns /auth.
    auth_ext = Authentication()
    auth_ext._setup_routes()
    app.include_router(auth_ext.router)

    # IAM extension's authorization_api routers ship as module-level
    # singletons — include them under the /iam prefix that the
    # IamExtension would otherwise mount them at.
    app.include_router(iam_admin_router, prefix="/iam")
    app.include_router(iam_me_router, prefix="/iam")

    # Admin extension owns /admin (including /admin/users).
    admin_ext = AdminService()
    app.include_router(admin_ext.router)

    return app


def test_auth_routes_carry_authn_tag(monkeypatch):
    """Every operation under /auth, /iam, or /admin/users carries exactly
    the shared tag — never the legacy per-extension tag, never two tags."""
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    app = _build_app_with_auth_routers()
    schema = build_iam_openapi_schema(app)

    paths = schema.get("paths", {})
    matched_any = False
    for path, ops in paths.items():
        if not _AUTHN_AUTHZ_PATH_RE.match(path):
            continue
        matched_any = True
        for method, op in ops.items():
            if method.lower() not in {"get", "post", "put", "delete", "patch"}:
                continue
            tags = op.get("tags") or []
            assert tags == [_AUTHN_AUTHZ_TAG], (
                f"{method.upper()} {path} has tags {tags!r}, "
                f"expected exactly [{_AUTHN_AUTHZ_TAG!r}]"
            )

    assert matched_any, (
        "No paths matched /auth, /iam, or /admin/users — the test fixture "
        "did not mount the routers correctly."
    )


def test_openapi_tags_array_has_authn_entry(monkeypatch):
    """The top-level ``tags`` array advertises the shared tag with a
    non-empty description so Swagger UI can render it."""
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_build_app_with_auth_routers())
    tags = schema.get("tags", [])
    matches = [t for t in tags if t.get("name") == _AUTHN_AUTHZ_TAG]
    assert len(matches) == 1, (
        f"Expected exactly one {_AUTHN_AUTHZ_TAG!r} entry in openapi['tags'], "
        f"got {len(matches)}: {tags!r}"
    )
    assert matches[0].get("description"), (
        "The shared tag entry must have a non-empty description."
    )


def test_no_orphan_old_tags(monkeypatch):
    """The old per-extension tag names are removed from the top-level
    ``tags`` array so Swagger UI doesn't render dangling sections."""
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_build_app_with_auth_routers())
    tag_names = {t.get("name") for t in schema.get("tags", [])}

    for orphan in ("auth", "iam", "admin"):
        assert orphan not in tag_names, (
            f"Orphan tag {orphan!r} still present in openapi['tags']: {tag_names!r}"
        )
