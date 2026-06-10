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

"""Regression guard: per-extension static prefixes must be in web_public_access.

Anonymous GET /web/{prefix}/… returns 403 with IAM loaded when the prefix is
absent from the policy, because PolicyService denies by default.  These tests
pin the 9 known extension prefixes (literal baseline) and verify the dynamic
derivation path (Layer 2) so future extensions are covered automatically.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import pytest

from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.web.web import _web_policies
from dynastore.extensions.tools.web_collect import collect_static_assets
from dynastore.models.protocols.web import WebModuleProtocol
from dynastore.models.protocols.web_ui import StaticAsset
from dynastore.tools.discovery import register_plugin, unregister_plugin


# ---------------------------------------------------------------------------
# Layer 1 — literal baseline
# ---------------------------------------------------------------------------

_REQUIRED_PREFIXES = [
    "stac",
    "records",
    "features",
    "assets",
    "edr",
    "movingfeatures",
    "tiles",
    "auth",
    "coverages",
]


def _public_resources() -> List[str]:
    policies = {p.id: p for p in _web_policies()}
    return policies["web_public_access"].resources


@pytest.mark.parametrize("prefix", _REQUIRED_PREFIXES)
def test_literal_extension_prefix_in_web_public_access(prefix: str) -> None:
    """Every known extension static prefix must appear in web_public_access."""
    resources = _public_resources()
    expected = f"/web/{prefix}/.*"
    assert expected in resources, (
        f"web_public_access is missing {expected!r}. "
        f"Anonymous GET /web/{prefix}/ returns 403 with IAM loaded."
    )


def test_no_duplicate_resources_in_web_public_access() -> None:
    """web_public_access must not list the same pattern twice."""
    resources = _public_resources()
    seen: set = set()
    dupes = []
    for r in resources:
        if r in seen:
            dupes.append(r)
        seen.add(r)
    assert not dupes, f"Duplicate entries in web_public_access resources: {dupes}"


# ---------------------------------------------------------------------------
# Layer 2 — dynamic derivation
# ---------------------------------------------------------------------------

class _FakeWebModule(WebModuleProtocol):
    """Minimal WebModuleProtocol stand-in that exposes controlled prefix meta."""

    def __init__(self, meta: Dict[str, Any]) -> None:
        self._meta = meta

    def get_static_prefix_meta(self) -> Dict[str, Any]:
        return dict(self._meta)

    # Satisfy the rest of the protocol surface expected by isinstance checks.
    web_pages: Dict[str, Any] = {}
    static_providers: Dict[str, Any] = {}
    static_prefix_meta: Dict[str, Any] = {}

    def register_web_page(self, config: Any, provider: Any) -> None: ...
    def register_static_provider(self, *a: Any, **kw: Any) -> None: ...
    async def get_web_pages_config(self, language: str = "en") -> List[Any]:
        return []
    def get_web_page_content(self, *a: Any, **kw: Any) -> Any: ...
    def get_docs_manifest(self) -> Any: return {}
    def get_doc_path(self, doc_id: str) -> Optional[str]: return None
    def generate_etag(self, content: Any) -> str: return ""
    def get_cache_headers(self, max_age: int = 3600) -> Any: return {}
    def list_static_prefix_info(self) -> List[Any]: return []
    def list_page_providers(self, page_id: str) -> List[Any]: return []


@pytest.fixture()
def fake_web_module():
    """Register a fake WebModule with controlled prefix meta, clean up after."""
    module = _FakeWebModule(
        meta={
            "newext": {"public": True, "owner": "newext", "description": ""},
            "secretext": {"public": False, "owner": "secretext", "description": ""},
        }
    )
    register_plugin(module)
    yield module
    unregister_plugin(module)


def test_dynamic_public_prefix_included(fake_web_module: _FakeWebModule) -> None:
    """A prefix with public=True added via WebModule appears in web_public_access."""
    resources = _public_resources()
    assert "/web/newext/.*" in resources, (
        "Dynamic public prefix 'newext' must be included in web_public_access "
        "when WebModule reports it as public=True."
    )


def test_dynamic_non_public_prefix_excluded(fake_web_module: _FakeWebModule) -> None:
    """A prefix with public=False must NOT appear in web_public_access."""
    resources = _public_resources()
    assert "/web/secretext/.*" not in resources, (
        "Dynamic prefix 'secretext' with public=False must not appear in "
        "web_public_access; gated prefixes must stay protected."
    )


def test_no_duplicate_when_dynamic_prefix_matches_literal() -> None:
    """When a dynamic prefix coincides with a literal, no duplicate is emitted."""

    class _OverlapModule(_FakeWebModule):
        def get_static_prefix_meta(self) -> Dict[str, Any]:
            # 'stac' is already in the literal list
            return {"stac": {"public": True, "owner": "stac-ext", "description": ""}}

    module = _OverlapModule(meta={})
    register_plugin(module)
    try:
        resources = _public_resources()
        stac_count = resources.count("/web/stac/.*")
        assert stac_count == 1, (
            f"Expected exactly 1 occurrence of /web/stac/.* but found {stac_count}; "
            "deduplication must prevent double-listing when dynamic equals literal."
        )
    finally:
        unregister_plugin(module)


def test_dynamic_derivation_survives_missing_webmodule() -> None:
    """_web_policies() must not raise when WebModule is absent (startup ordering)."""
    # By default no WebModule is registered in unit-test isolation.
    # The call must succeed and return the literal list.
    resources = _public_resources()
    assert "/web/stac/.*" in resources
    assert isinstance(resources, list)


# ---------------------------------------------------------------------------
# expose_static public= flag: decorator + collect_static_assets
# ---------------------------------------------------------------------------


def test_expose_static_public_true_default() -> None:
    """@expose_static defaults to public=True."""
    @expose_static("mypfx")
    def _provider() -> List[str]:
        return []

    assert getattr(_provider, "_web_static_public", True) is True


def test_expose_static_public_false_stored() -> None:
    """@expose_static(public=False) stores the flag as False."""
    @expose_static("secretpfx", public=False)
    def _provider() -> List[str]:
        return []

    assert getattr(_provider, "_web_static_public") is False


def test_collect_static_assets_propagates_public_true() -> None:
    class _Ext:
        @expose_static("pub", owner="e", description="")
        def _s(self) -> List[str]:
            return []

    assets = collect_static_assets(_Ext())
    assert len(assets) == 1
    assert assets[0].public is True


def test_collect_static_assets_propagates_public_false() -> None:
    class _Ext:
        @expose_static("priv", owner="e", description="", public=False)
        def _s(self) -> List[str]:
            return []

    assets = collect_static_assets(_Ext())
    assert len(assets) == 1
    assert assets[0].public is False


def test_static_asset_public_default() -> None:
    """StaticAsset.public defaults to True when not supplied."""
    asset = StaticAsset(prefix="x", files_provider=lambda: [])
    assert asset.public is True


def test_static_asset_public_false() -> None:
    asset = StaticAsset(prefix="x", files_provider=lambda: [], public=False)
    assert asset.public is False


# ---------------------------------------------------------------------------
# Security regression: dashboard prefix must never be emitted by the dynamic
# block, even when registered with public=True in metadata.
# ---------------------------------------------------------------------------


def test_dynamic_block_does_not_emit_dashboard_pattern() -> None:
    """A WebModule reporting dashboard as public=True must NOT cause the dynamic
    block to append /web/dashboard/.*, because the literal list already
    references /web/dashboard/?$ — the literal is authoritative and the
    dynamic block must skip the prefix entirely.

    This prevents the gated data endpoints (/web/dashboard/catalogs/…,
    /web/dashboard/(stats|tasks)) from being anonymously opened.
    """

    class _DashboardModule(_FakeWebModule):
        def get_static_prefix_meta(self) -> Dict[str, Any]:
            # Simulate a misconfigured or legacy extension that reports
            # dashboard as public.
            return {"dashboard": {"public": True, "owner": "test", "description": ""}}

    module = _DashboardModule(meta={})
    register_plugin(module)
    try:
        resources = _public_resources()
        assert "/web/dashboard/.*" not in resources, (
            "/web/dashboard/.* must not be emitted by the dynamic block; "
            "the literal /web/dashboard/?$ is authoritative and the dynamic "
            "block must skip any prefix already referenced by the literal list."
        )
    finally:
        unregister_plugin(module)


def test_web_extension_dashboard_static_is_public_false() -> None:
    """Web._provide_dashboard_static must be decorated with public=False.

    The dashboard static prefix serves the HTML shell (reachable via the
    existing /web/dashboard/?$ literal entry) but the gated data endpoints
    under /web/dashboard/ must not be opened anonymously.  Marking the
    decorator public=False prevents the dynamic derivation block from ever
    widening the policy for this prefix.
    """
    import inspect
    from dynastore.extensions.web.web import Web

    method = None
    for _, member in inspect.getmembers(Web):
        if callable(member) and getattr(member, "_web_static_prefix", None) == "dashboard":
            method = member
            break

    assert method is not None, (
        "Could not find a method on Web decorated with @expose_static('dashboard')"
    )
    assert getattr(method, "_web_static_public", True) is False, (
        "@expose_static('dashboard') on Web must be declared public=False so the "
        "dynamic derivation block never emits /web/dashboard/.* and the gated "
        "dashboard data endpoints stay protected."
    )
