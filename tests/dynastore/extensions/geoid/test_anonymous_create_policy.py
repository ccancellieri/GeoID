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

"""Unit tests for the anonymous-create policy registered by the geoid extension.

Policy registration lives in the per-catalog ``register_geoid_policies_for_catalog()``,
invoked from the geoid preset's on-applied hook (CATALOG_CREATION). These tests
pin the per-catalog contract: the entry point emits
``geoid_anonymous_create_per_collection`` as an ALLOW on POST to the STAC/Features
item-intake paths.

Priority bands for the same policy set are pinned separately in
``test_catalog_policy_priorities.py``; here we focus on the create policy's
effect / action / resource shape. All DB access is stubbed — no Postgres.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.extensions.geoid import catalog_policies as cp
from dynastore.models.auth import Policy


def _run(coro: Any) -> Any:
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_env(catalog_id: str):
    """Mock PermissionProtocol / Database / Catalogs with no DB access."""
    mock_pm = MagicMock()
    mock_pm.get_policy = AsyncMock(return_value=None)
    mock_pm.create_policy = AsyncMock(return_value=MagicMock())

    mock_policy_storage = MagicMock()
    mock_policy_storage.ensure_policy_partition = AsyncMock()

    mock_db = MagicMock()
    mock_db.engine = MagicMock()

    mock_catalogs = MagicMock()
    mock_catalogs.resolve_physical_schema = AsyncMock(return_value=f"tenant_{catalog_id}")

    return mock_pm, mock_policy_storage, mock_db, mock_catalogs


def _fake_get_protocol(mock_pm: Any, mock_db: Any, mock_catalogs: Any):
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol

    def _fake(proto: Any) -> Any:
        if proto is PermissionProtocol:
            return mock_pm
        if proto is DatabaseProtocol:
            return mock_db
        if proto is CatalogsProtocol:
            return mock_catalogs
        return None

    return _fake


def _register_for_catalog(monkeypatch: Any, catalog_id: str) -> Dict[str, Policy]:
    """Run ``register_geoid_policies_for_catalog`` with DB access stubbed and
    return ``{policy_id: Policy}`` for every ``create_policy`` call."""
    mock_pm, mock_policy_storage, mock_db, mock_catalogs = _make_env(catalog_id)
    monkeypatch.setattr(cp, "get_protocol", _fake_get_protocol(mock_pm, mock_db, mock_catalogs))
    with patch(
        "dynastore.extensions.geoid.catalog_policies.PostgresPolicyStorage",
        return_value=mock_policy_storage,
    ), patch(
        "dynastore.extensions.geoid.catalog_policies.managed_transaction",
    ), patch(
        "dynastore.extensions.geoid.catalog_policies.DriverContext",
        return_value=MagicMock(),
    ):
        _run(cp.register_geoid_policies_for_catalog(catalog_id))
    return {call.args[0].id: call.args[0] for call in mock_pm.create_policy.call_args_list}


# ---------------------------------------------------------------------------
# Per-catalog registration — the create policy's shape
# ---------------------------------------------------------------------------

def test_anonymous_create_policy_registered_with_allow_post(monkeypatch):
    registered = _register_for_catalog(monkeypatch, "cat_anon_create")

    assert "geoid_anonymous_create_per_collection" in registered
    p = registered["geoid_anonymous_create_per_collection"]
    assert p.effect == "ALLOW"
    assert "POST" in p.actions


def test_anonymous_create_policy_covers_stac_and_features_item_paths(monkeypatch):
    registered = _register_for_catalog(monkeypatch, "cat_anon_create")
    p = registered["geoid_anonymous_create_per_collection"]

    assert any(
        "/stac/catalogs/" in r and "/collections/" in r and "/items" in r
        for r in p.resources
    )
    assert any(
        "/features/catalogs/" in r and "/collections/" in r and "/items" in r
        for r in p.resources
    )


def test_anonymous_create_policy_is_among_the_registered_anon_set(monkeypatch):
    """The create policy registers alongside the anonymous lookup/deny policies
    (the four-policy anonymous posture), not in isolation."""
    registered = _register_for_catalog(monkeypatch, "cat_anon_create")

    assert {
        "geoid_anonymous_lookup",
        "geoid_anonymous_stac_deny_lookup_only",
        "geoid_anonymous_features_deny_lookup_only",
        "geoid_anonymous_create_per_collection",
    }.issubset(registered.keys())
