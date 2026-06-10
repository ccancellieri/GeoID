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

"""Unit tests for the web pluggability seams (issue #1921).

Covers:
- StaticAsset now carries owner + description fields
- expose_static decorator forwards owner/description metadata
- collect_static_assets picks up owner/description from decorated methods
- WebModule.register_static_provider stores owner/description in static_prefix_meta
- WebModule.list_static_prefix_info returns correct entries
- WebModule.list_page_providers returns correct handler introspection
- Policy: sysadmin has access to the provider introspection resource pattern
- Policy: static-prefix endpoint is covered by the existing web_public_access pattern
"""
from __future__ import annotations

import re
from typing import List

from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.web.web import _web_policies
from dynastore.extensions.tools.web_collect import collect_static_assets
from dynastore.models.protocols.web_ui import StaticAsset
from dynastore.modules.web.web_module import WebModule  # type: ignore[import-untyped]


# ---------------------------------------------------------------------------
# StaticAsset fields
# ---------------------------------------------------------------------------


def test_static_asset_default_owner_and_description() -> None:
    asset = StaticAsset(prefix="foo", files_provider=lambda: [])
    assert asset.owner == ""
    assert asset.description == ""


def test_static_asset_carries_owner_description() -> None:
    asset = StaticAsset(
        prefix="geoid",
        files_provider=lambda: [],
        owner="geoid",
        description="GeoID extension static assets.",
    )
    assert asset.owner == "geoid"
    assert asset.description == "GeoID extension static assets."


# ---------------------------------------------------------------------------
# expose_static decorator
# ---------------------------------------------------------------------------


def test_expose_static_stores_metadata() -> None:
    @expose_static("mypfx", owner="myext", description="My extension assets.")
    def _provider() -> List[str]:
        return []

    assert getattr(_provider, "_web_static_prefix") == "mypfx"
    assert getattr(_provider, "_web_static_owner") == "myext"
    assert getattr(_provider, "_web_static_description") == "My extension assets."


def test_expose_static_defaults_empty_strings() -> None:
    @expose_static("bare")
    def _provider() -> List[str]:
        return []

    assert getattr(_provider, "_web_static_owner", "") == ""
    assert getattr(_provider, "_web_static_description", "") == ""


# ---------------------------------------------------------------------------
# collect_static_assets
# ---------------------------------------------------------------------------


def test_collect_static_assets_propagates_metadata() -> None:
    class FakeExtension:
        @expose_static("pfx", owner="ext-a", description="Desc A.")
        def _my_static(self) -> List[str]:
            return ["/fake/file.js"]

    assets = collect_static_assets(FakeExtension())
    assert len(assets) == 1
    a = assets[0]
    assert a.prefix == "pfx"
    assert a.owner == "ext-a"
    assert a.description == "Desc A."


def test_collect_static_assets_no_metadata_defaults() -> None:
    class FakeExtension:
        @expose_static("pfx2")
        def _my_static(self) -> List[str]:
            return []

    assets = collect_static_assets(FakeExtension())
    assert len(assets) == 1
    a = assets[0]
    assert a.owner == ""
    assert a.description == ""


# ---------------------------------------------------------------------------
# WebModule.register_static_provider / list_static_prefix_info
# ---------------------------------------------------------------------------


def test_webmodule_stores_static_prefix_meta() -> None:
    module = WebModule()
    module.register_static_provider(
        "geoid",
        lambda: [],
        owner="geoid-ext",
        description="GeoID static files.",
    )
    assert "geoid" in module.static_providers
    meta = module.static_prefix_meta.get("geoid", {})
    assert meta["owner"] == "geoid-ext"
    assert meta["description"] == "GeoID static files."
    # public defaults to True when not explicitly passed
    assert meta.get("public", True) is True


def test_webmodule_stores_static_prefix_meta_public_false() -> None:
    """register_static_provider must store public=False when passed explicitly."""
    module = WebModule()
    module.register_static_provider(
        "private-pfx",
        lambda: [],
        owner="myext",
        description="Gated assets.",
        public=False,
    )
    meta = module.static_prefix_meta.get("private-pfx", {})
    assert meta.get("public") is False, (
        "register_static_provider must store the public flag so _web_policies() "
        "can skip non-public prefixes in the dynamic derivation block."
    )


def test_webmodule_list_static_prefix_info_sorted() -> None:
    module = WebModule()
    module.register_static_provider("z-pfx", lambda: [], owner="z", description="Z desc.")
    module.register_static_provider("a-pfx", lambda: [], owner="a", description="A desc.")

    info = module.list_static_prefix_info()
    prefixes = [entry["prefix"] for entry in info]
    assert prefixes == sorted(prefixes), "list_static_prefix_info must return entries in alphabetical order"
    assert prefixes[0] == "a-pfx"
    assert info[0]["owner"] == "a"
    assert info[0]["description"] == "A desc."


def test_webmodule_list_static_prefix_info_empty() -> None:
    module = WebModule()
    assert module.list_static_prefix_info() == []


# ---------------------------------------------------------------------------
# WebModule.list_page_providers
# ---------------------------------------------------------------------------


def test_webmodule_list_page_providers_returns_empty_for_unknown() -> None:
    module = WebModule()
    result = module.list_page_providers("nonexistent")
    assert result == []


def test_webmodule_list_page_providers_returns_handler_info() -> None:
    module = WebModule()

    def _primary_handler() -> str:
        return "<p>Primary</p>"

    def _embed_handler() -> str:
        return "<p>Embed</p>"

    module.register_web_page(
        {"id": "home", "title": "Home", "priority": -100, "is_embed": False, "enabled": True},
        _primary_handler,
    )
    module.register_web_page(
        {"id": "home", "title": "Home", "priority": 10, "is_embed": True, "enabled": True},
        _embed_handler,
    )

    providers = module.list_page_providers("home")
    assert len(providers) == 2

    # Providers are stored sorted by priority
    primary = next(p for p in providers if not p["is_embed"])
    embed = next(p for p in providers if p["is_embed"])

    assert primary["priority"] == -100
    assert embed["priority"] == 10
    assert "_primary_handler" in primary["handler"]
    assert "_embed_handler" in embed["handler"]


# ---------------------------------------------------------------------------
# Policy: sysadmin access to provider introspection
# ---------------------------------------------------------------------------


def test_sysadmin_policy_covers_provider_introspection_route() -> None:
    policies = {p.id: p for p in _web_policies()}
    sysadmin = policies["web_sysadmin_access"]

    pattern_str = r"^/web/admin/pages/[^/]+/providers$"
    assert pattern_str in sysadmin.resources, (
        "web_sysadmin_access must include the provider introspection route pattern"
    )

    compiled = re.compile(pattern_str)
    assert compiled.match("/web/admin/pages/home/providers")
    assert compiled.match("/web/admin/pages/stac_browser/providers")
    assert not compiled.match("/web/admin/pages/home/providers/extra")


# ---------------------------------------------------------------------------
# Policy: static-prefix registry is anonymously accessible
# ---------------------------------------------------------------------------


def test_web_public_access_covers_static_prefixes_route() -> None:
    """GET /web/config/static-prefixes must be reachable by anonymous users.

    The route falls under /web/config/.* which is already in web_public_access.
    """
    policies = {p.id: p for p in _web_policies()}
    public = policies["web_public_access"]

    # Find a pattern that matches /web/config/static-prefixes
    target = "/web/config/static-prefixes"
    matched = any(re.match(p, target) for p in public.resources)
    assert matched, (
        f"web_public_access must cover {target!r} so anonymous callers can "
        "discover registered static prefixes."
    )
