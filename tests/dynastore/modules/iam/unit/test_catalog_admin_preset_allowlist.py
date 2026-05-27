"""Unit tests for ``CatalogAdminHandler.allowed_preset_names`` (#1426).

Verifies the safe-subset allowlist gate on ``/admin/catalogs/{cat}/presets/{name}``:

- None (default) preserves legacy role-only behaviour.
- Set + path matches + name in allowlist → allow.
- Set + path matches + name NOT in allowlist → deny.
- Set but path is NOT a preset path → pass through (don't apply guard).
- Sysadmin / platform-grant principals bypass the allowlist.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from dynastore.models.auth import Principal
from dynastore.modules.iam.conditions import (
    CatalogAdminHandler,
    EvaluationContext,
)
from dynastore.tools.discovery import register_plugin, unregister_plugin


pytestmark = pytest.mark.asyncio


class _StubIamQuery:
    def __init__(
        self,
        catalog_roles: Optional[Dict[str, List[str]]] = None,
        platform: bool = False,
    ):
        self._catalog_roles = catalog_roles or {}
        self._platform = platform

    async def list_catalog_memberships(
        self, provider: str, subject_id: str
    ) -> Dict[str, Any]:
        return {
            "platform": self._platform,
            "catalogs": list(self._catalog_roles.keys()),
            "catalog_roles": dict(self._catalog_roles),
            "total": len(self._catalog_roles),
        }


@pytest.fixture
def handler() -> CatalogAdminHandler:
    return CatalogAdminHandler()


_SUBJECT_COUNTER = 0


def _principal(roles: Optional[List[str]] = None) -> Principal:
    """Unique-subject Principal so the per-pod membership cache can't bleed."""
    global _SUBJECT_COUNTER
    _SUBJECT_COUNTER += 1
    sub = f"preset-allowlist-{_SUBJECT_COUNTER}"
    return Principal(
        provider="internal",
        subject_id=sub,
        display_name=sub,
        roles=roles or ["admin"],
        is_active=True,
    )


def _make_ctx(catalog_id: str, principal: Principal, path: str) -> EvaluationContext:
    return EvaluationContext(
        request=None,
        storage=None,  # type: ignore[arg-type]
        catalog_id=catalog_id,
        path=path,
        method="POST",
        extras={"principal_obj": principal},
    )


# ---------------------------------------------------------------------------
# allowlist absent (None) → legacy role-only behaviour
# ---------------------------------------------------------------------------

async def test_no_allowlist_is_legacy_role_only(handler):
    """When allowed_preset_names is absent, the handler ignores the path
    and gates purely on role match (existing behaviour).
    """
    stub = _StubIamQuery(catalog_roles={"acme": ["admin"]})
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/presets/items_es_private",
        )
        assert await handler.evaluate({"required_roles": ["admin"]}, ctx) is True
    finally:
        unregister_plugin(stub)


# ---------------------------------------------------------------------------
# allowlist present → safe-subset gate on preset paths
# ---------------------------------------------------------------------------

async def test_allowlist_permits_named_preset(handler):
    stub = _StubIamQuery(catalog_roles={"acme": ["admin"]})
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/presets/public_catalog",
        )
        verdict = await handler.evaluate(
            {
                "required_roles": ["admin"],
                "allowed_preset_names": ["public_catalog", "private_catalog"],
            },
            ctx,
        )
        assert verdict is True
    finally:
        unregister_plugin(stub)


async def test_allowlist_denies_unlisted_preset(handler):
    """Role match alone is no longer enough — the preset name must be allowed."""
    stub = _StubIamQuery(catalog_roles={"acme": ["admin"]})
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/presets/items_es_private",
        )
        verdict = await handler.evaluate(
            {
                "required_roles": ["admin"],
                "allowed_preset_names": ["public_catalog", "private_catalog"],
            },
            ctx,
        )
        assert verdict is False
    finally:
        unregister_plugin(stub)


async def test_empty_allowlist_denies_all_presets(handler):
    stub = _StubIamQuery(catalog_roles={"acme": ["admin"]})
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/presets/public_catalog",
        )
        verdict = await handler.evaluate(
            {"required_roles": ["admin"], "allowed_preset_names": []},
            ctx,
        )
        assert verdict is False
    finally:
        unregister_plugin(stub)


# ---------------------------------------------------------------------------
# allowlist present but path is NOT a preset path → pass through unchanged
# ---------------------------------------------------------------------------

async def test_allowlist_does_not_affect_non_preset_paths(handler):
    """Same condition config attached to a broader policy must still allow
    other catalog admin routes — the gate is preset-path-scoped.
    """
    stub = _StubIamQuery(catalog_roles={"acme": ["admin"]})
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/collections/some-coll",
        )
        verdict = await handler.evaluate(
            {
                "required_roles": ["admin"],
                "allowed_preset_names": ["public_catalog"],
            },
            ctx,
        )
        assert verdict is True
    finally:
        unregister_plugin(stub)


# ---------------------------------------------------------------------------
# Sysadmin / platform bypass remain unaffected by the allowlist
# ---------------------------------------------------------------------------

async def test_sysadmin_bypass_skips_allowlist(handler):
    """Sysadmins keep blanket access — the allowlist is a delegation guard."""
    # NOTE: no stub registered — sysadmin must short-circuit without IAM call.
    ctx = _make_ctx(
        "acme",
        _principal(roles=["sysadmin"]),
        "/admin/catalogs/acme/presets/items_es_private",
    )
    verdict = await handler.evaluate(
        {"required_roles": ["admin"], "allowed_preset_names": ["public_catalog"]},
        ctx,
    )
    assert verdict is True


async def test_platform_grant_bypass_skips_allowlist(handler):
    stub = _StubIamQuery(catalog_roles={}, platform=True)
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/presets/items_es_private",
        )
        verdict = await handler.evaluate(
            {
                "required_roles": ["admin"],
                "allowed_preset_names": ["public_catalog"],
            },
            ctx,
        )
        assert verdict is True
    finally:
        unregister_plugin(stub)


# ---------------------------------------------------------------------------
# Negative case: role miss still denies regardless of allowlist
# ---------------------------------------------------------------------------

async def test_role_miss_denies_even_for_allowed_preset(handler):
    stub = _StubIamQuery(catalog_roles={"acme": ["viewer"]})  # not admin
    register_plugin(stub)
    try:
        ctx = _make_ctx(
            "acme",
            _principal(roles=[]),
            "/admin/catalogs/acme/presets/public_catalog",
        )
        verdict = await handler.evaluate(
            {
                "required_roles": ["admin"],
                "allowed_preset_names": ["public_catalog"],
            },
            ctx,
        )
        assert verdict is False
    finally:
        unregister_plugin(stub)
