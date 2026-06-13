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

"""Unit tests for the ogc_conformance_public preset.

Covers:
    (a) anonymous GET /conformance → allowed when exposure=public (default)
    (b) anonymous GET / (landing page) → allowed
    (c) per-service /{svc}/conformance → allowed
    (d) when exposure=private → denied (policy not applied / empty)
    (e) the policy is GET-only (anonymous POST still denied)
    (f) preset registers itself in the global registry
    (g) apply is idempotent (duplicate apply produces same result)
"""
from __future__ import annotations

import re
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

# Importing the preset module triggers its register_preset side-effect.
import dynastore.extensions.tools.conformance_preset as _preset_mod  # noqa: F401
from dynastore.extensions.tools.conformance_config import ConformanceExposureConfig
from dynastore.extensions.tools.conformance_preset import (
    _POLICY_ID,
    _conformance_policies,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.preset import AppliedDescriptor, PresetContext
from dynastore.modules.storage.presets.registry import find_preset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(
    *,
    updated_policies: Optional[List[str]] = None,
    bound_roles: Optional[List[str]] = None,
    deleted_policies: Optional[List[str]] = None,
) -> PresetContext:
    if updated_policies is None:
        updated_policies = []
    if bound_roles is None:
        bound_roles = []
    if deleted_policies is None:
        deleted_policies = []

    policy_svc = MagicMock()
    iam_svc = MagicMock()

    async def _update_policy(policy: Any) -> Any:
        updated_policies.append(policy.id)
        return policy

    async def _delete_policy(pid: str, catalog_id: Any = None) -> bool:
        deleted_policies.append(pid)
        return True

    async def _get_policy(pid: str) -> None:
        # Simulate no existing policy — always fresh upsert.
        raise Exception("not found")

    async def _bind(role_name: str, policy_entry: Any, catalog_id: Any = None) -> None:
        bound_roles.append(role_name)

    async def _create_role(role: Any) -> None:
        pass

    iam_svc.create_role = _create_role
    iam_svc.bind_policy_to_role = _bind
    iam_svc.list_roles = AsyncMock(return_value=[])
    iam_svc.delete_role = AsyncMock(return_value=True)
    iam_svc.unbind_policy_from_role = AsyncMock()
    policy_svc.update_policy = _update_policy
    policy_svc.delete_policy = _delete_policy
    policy_svc.get_policy = _get_policy

    return PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


def _matches(resource_pattern: str, path: str) -> bool:
    """Replicate PolicyService.matches_resource: re.match (start-anchored)."""
    return bool(re.match(resource_pattern, path))


def _policy_allows(path: str, method: str = "GET") -> bool:
    """Return True when the conformance policy covers *path* with *method*."""
    policies = _conformance_policies()
    for pol in policies:
        if method not in pol.actions:
            continue
        for pattern in pol.resources:
            if _matches(pattern, path):
                return True
    return False


# ---------------------------------------------------------------------------
# (f) Preset self-registers in the global registry
# ---------------------------------------------------------------------------

def test_preset_registered_in_registry():
    preset = find_preset("ogc_conformance_public")
    assert preset is not None
    assert isinstance(preset, PolicyContributorPreset)


def test_preset_name():
    preset = find_preset("ogc_conformance_public")
    assert preset.name == "ogc_conformance_public"


def test_preset_keywords_include_ogc_and_conformance():
    preset = find_preset("ogc_conformance_public")
    assert "ogc" in preset.keywords
    assert "conformance" in preset.keywords


# ---------------------------------------------------------------------------
# (a) anonymous GET /conformance allowed when public=True (default)
# ---------------------------------------------------------------------------

def test_platform_conformance_allowed():
    assert _policy_allows("/conformance"), "/conformance must be allowed"


def test_platform_conformance_only_anchored():
    # /conformance/extra should NOT match — end anchor prevents widening.
    assert not _policy_allows("/conformance/extra"), (
        "/conformance/extra must not be covered by the platform-level pattern"
    )


# ---------------------------------------------------------------------------
# (b) anonymous GET / (landing page) — handled by web_public_access, not here
# ---------------------------------------------------------------------------

def test_root_slash_not_covered_by_conformance_preset():
    # "/" (bare) is intentionally NOT covered by the ogc_conformance_public preset.
    # It is already in web_public_access via "/$".  Our preset focuses on
    # /{svc}/conformance and /{svc}/ (service-level landing pages with a
    # non-empty first segment), so there is no regression here.
    assert not _policy_allows("/"), (
        "Bare / must not be widened by the conformance preset "
        "(it is handled by web_public_access /$)"
    )


def test_root_landing_with_slash_allowed():
    # /{svc}/ landing pages are covered by "^/(features|stac|…)/?$".
    assert _policy_allows("/features/"), "/{svc}/ landing page must be allowed"
    assert _policy_allows("/stac/"), "/stac/ landing page must be allowed"


# ---------------------------------------------------------------------------
# (security) the landing/conformance grant reaches OGC services ONLY — never
# a non-OGC root.  Those roots are protected by deny-by-default (narrow
# role-scoped ALLOWs, no explicit DENY), so a blanket anonymous ALLOW would
# OVERRIDE their protection.  This test locks the scoping.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("root", [
    "/admin", "/admin/", "/iam", "/iam/", "/auth", "/auth/",
    "/configs", "/configs/", "/dwh", "/dwh/", "/tasks", "/tasks/",
    "/events", "/gcp", "/gdal", "/logs", "/me", "/me/", "/notebooks",
    "/proxy", "/search", "/stats", "/template",
])
def test_non_ogc_roots_never_allowed(root: str):
    assert not _policy_allows(root), (
        f"{root} is a non-OGC root protected by deny-by-default and MUST NOT "
        "be granted anonymous access by the conformance preset"
    )
    assert not _policy_allows(f"{root}/conformance"), (
        f"{root}/conformance must not be anonymously granted either"
    )


# ---------------------------------------------------------------------------
# (c) per-service /{svc}/conformance allowed
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("svc", [
    "features",
    "stac",
    "records",
    "coverages",
    "tiles",
    "maps",
    "edr",
    "dggs",
    "processes",
    "styles",
    "consys",
    "movingfeatures",
    "volumes",
    "wfs",
    "assets",
    "dimensions",
    "join",
])
def test_per_service_conformance_allowed(svc: str):
    assert _policy_allows(f"/{svc}/conformance"), (
        f"/{svc}/conformance must be anonymously accessible"
    )


@pytest.mark.parametrize("svc", ["features", "stac", "records", "maps"])
def test_per_service_landing_page_allowed(svc: str):
    assert _policy_allows(f"/{svc}/"), f"/{svc}/ landing page must be allowed"


@pytest.mark.parametrize("svc", ["features", "stac"])
def test_per_service_landing_without_trailing_slash_allowed(svc: str):
    assert _policy_allows(f"/{svc}"), f"/{svc} (no trailing slash) must be allowed"


# ---------------------------------------------------------------------------
# (d) the policy does NOT open data surfaces
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", [
    "/features/catalogs/demo/collections",
    "/features/catalogs/demo/collections/roads/items",
    "/stac/catalogs/demo/collections",
    "/stac/search",
    "/records/catalogs/demo/collections/surveys/items/1",
    "/maps/catalogs/demo/collections/roads/map",
    "/tiles/catalogs/demo/collections/roads/tiles",
])
def test_data_surfaces_not_covered(path: str):
    assert not _policy_allows(path), (
        f"{path} must NOT be anonymously accessible via the conformance policy"
    )


# ---------------------------------------------------------------------------
# (e) policy is GET-only — POST/PUT/DELETE denied for anonymous
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH"])
def test_non_get_methods_not_allowed(method: str):
    assert not _policy_allows("/features/conformance", method), (
        f"method {method} on /features/conformance must not be covered"
    )


def test_options_is_allowed():
    # OPTIONS is typically needed for browser CORS preflight.
    assert _policy_allows("/features/conformance", "OPTIONS")


# ---------------------------------------------------------------------------
# (a+c) apply writes the policy and binds the anonymous role
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_upserts_conformance_policy():
    preset = find_preset("ogc_conformance_public")
    updated_policies: List[str] = []
    ctx = _make_context(updated_policies=updated_policies)

    await preset.apply(preset.params_model(), "platform", ctx)

    assert _POLICY_ID in updated_policies


@pytest.mark.asyncio
async def test_apply_binds_anonymous_role():
    preset = find_preset("ogc_conformance_public")
    bound_roles: List[str] = []
    ctx = _make_context(bound_roles=bound_roles)

    await preset.apply(preset.params_model(), "platform", ctx)

    assert len(bound_roles) > 0, "at least one role must be bound"


@pytest.mark.asyncio
async def test_apply_returns_descriptor_with_policy_id():
    preset = find_preset("ogc_conformance_public")
    ctx = _make_context()

    descriptor: AppliedDescriptor = await preset.apply(preset.params_model(), "platform", ctx)

    assert _POLICY_ID in descriptor.payload["policy_ids"]


# ---------------------------------------------------------------------------
# (d) exposure=private — ConformanceExposureConfig.public=False
# ---------------------------------------------------------------------------

def test_default_exposure_is_public():
    cfg = ConformanceExposureConfig()
    assert cfg.public is True


def test_private_exposure_config():
    cfg = ConformanceExposureConfig(public=False)
    assert cfg.public is False


# ---------------------------------------------------------------------------
# (g) idempotency — applying twice produces the same descriptor shape
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_is_idempotent():
    preset = find_preset("ogc_conformance_public")
    ctx1 = _make_context()
    ctx2 = _make_context()
    params = preset.params_model()

    d1: AppliedDescriptor = await preset.apply(params, "platform", ctx1)
    d2: AppliedDescriptor = await preset.apply(params, "platform", ctx2)

    assert d1.payload.keys() == d2.payload.keys()
    assert sorted(d1.payload["policy_ids"]) == sorted(d2.payload["policy_ids"])


# ---------------------------------------------------------------------------
# revoke — removes what apply wrote
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_deletes_conformance_policy():
    preset = find_preset("ogc_conformance_public")
    deleted_policies: List[str] = []
    ctx = _make_context()
    params = preset.params_model()

    descriptor = await preset.apply(params, "platform", ctx)

    ctx2 = _make_context(deleted_policies=deleted_policies)
    await preset.revoke(descriptor, ctx2)

    assert _POLICY_ID in deleted_policies
