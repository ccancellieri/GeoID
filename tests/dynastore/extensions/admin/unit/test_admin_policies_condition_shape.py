#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Lock in that ``POST /admin/policies?catalog_id=...`` accepts the full
Policy + Condition shape used by the lookup-only-anonymous-write demo.

The demo notebook (``uc_lookup_only_anonymous_write``) POSTs a catalog-scoped
policy bundle built by ``deny_all_except_lookup``. Each policy in that bundle
exercises a different corner of the wire schema:

- multiple ``actions`` per policy (e.g. ``["GET", "POST"]``),
- ``resources`` as a regex list,
- ``conditions: [{type: "lookup_only_search", config: {}}, ...]`` — where the
  ``config`` key may be present-and-empty or omitted entirely,
- both ``effect: "DENY"`` and ``effect: "ALLOW"``,
- catalog-scoped POST (``catalog_id`` forwarded to the policy manager).

These tests pin that the wire DTO (:class:`PolicyCreate`), the domain
:class:`Policy`, the :class:`Condition` sub-model, and the ``create_policy``
route handler all accept the bundle without drift. They are pure-unit: the
policy manager is stubbed, so no DB / app lifespan is touched.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.auth import Condition, Policy
from dynastore.models.protocols.policies import PolicyCreate


# ---------------------------------------------------------------------------
# The demo bundle, reproduced verbatim from the notebook's
# ``deny_all_except_lookup`` helper (one DENY-all block per service prefix
# plus the three ALLOW carve-outs). Kept as raw dicts so the test validates
# exactly what an HTTP client would send.
# ---------------------------------------------------------------------------

_DEMO_CATALOG = "demo-cat"
_COLL_IDS = ["coll1", "coll2"]
_SERVICE_PREFIXES = [
    "stac", "features", "coverages", "records", "tiles", "processes",
    "edr", "maps", "styles", "dggs", "consys", "moving_features",
]


def _demo_policy_bundle(cat_id: str) -> list[dict]:
    """Mirror of the notebook's ``deny_all_except_lookup(cat_id)``."""
    policies: list[dict] = []

    # 1. DENY everything at catalog scope — multiple actions, regex resource.
    for svc in _SERVICE_PREFIXES:
        policies.append({
            "id": f"{cat_id}-deny-{svc}",
            "effect": "DENY",
            "actions": ["GET", "POST", "PUT", "PATCH", "DELETE"],
            "resources": [rf"^/{svc}/catalogs/{cat_id}(/.*)?$"],
        })

    # 2. ALLOW anonymous POST to specific collections via Features API —
    #    condition with the ``config`` key omitted entirely.
    feature_resources = [
        rf"^/features/catalogs/{cat_id}/collections/{coll}/items/?$"
        for coll in _COLL_IDS
    ]
    policies.append({
        "id": f"{cat_id}-allow-anon-features-write",
        "effect": "ALLOW",
        "actions": ["POST"],
        "resources": feature_resources,
        "conditions": [{"type": "collection_write_anonymous_allowed"}],
    })

    # 3a. ALLOW catalog-scoped lookup — two conditions, multiple actions.
    policies.append({
        "id": f"{cat_id}-allow-anon-search-lookup-scoped",
        "effect": "ALLOW",
        "actions": ["GET", "POST"],
        "resources": [rf"^/search/catalogs/{cat_id}/?$"],
        "conditions": [
            {"type": "lookup_only_search"},
            {"type": "catalog_lookup_public_allowed"},
        ],
    })

    # 3b. ALLOW unscoped lookup — and here we add the explicit empty
    #     ``config: {}`` form from issue #285 to prove both shapes parse.
    policies.append({
        "id": f"{cat_id}-allow-anon-search-lookup-unscoped",
        "effect": "ALLOW",
        "actions": ["GET", "POST"],
        "resources": [r"^/search/?$"],
        "conditions": [
            {"type": "lookup_only_search", "config": {}},
            {"type": "catalog_lookup_public_allowed", "config": {}},
        ],
    })

    return policies


# ---------------------------------------------------------------------------
# Wire-DTO validation (what FastAPI does before the handler body runs).
# ---------------------------------------------------------------------------

def test_demo_bundle_parses_as_policy_create():
    """Every policy in the demo bundle must validate as :class:`PolicyCreate`
    — multiple actions, regex resources, both effects, condition lists with
    and without an explicit ``config`` key."""
    bundle = _demo_policy_bundle(_DEMO_CATALOG)
    assert len(bundle) == len(_SERVICE_PREFIXES) + 3

    parsed = [PolicyCreate(**raw) for raw in bundle]

    # Both effects are present and preserved.
    effects = {p.effect for p in parsed}
    assert effects == {"ALLOW", "DENY"}

    # Multiple actions survive untouched on a DENY policy.
    deny = next(p for p in parsed if p.id == f"{_DEMO_CATALOG}-deny-stac")
    assert deny.effect == "DENY"
    assert deny.actions == ["GET", "POST", "PUT", "PATCH", "DELETE"]

    # Regex resource list is carried verbatim (only the literal "*" wildcard
    # is rewritten; these patterns contain none).
    assert deny.resources == [rf"^/stac/catalogs/{_DEMO_CATALOG}(/.*)?$"]


def test_condition_config_optional_defaults_to_empty_dict():
    """``conditions: [{type: ...}]`` with the ``config`` key omitted must
    default to an empty dict, matching the notebook's payloads."""
    pc = PolicyCreate(
        id="cond-no-config",
        effect="ALLOW",
        actions=["GET"],
        resources=[r"^/search/?$"],
        conditions=[{"type": "lookup_only_search"}],
    )
    assert len(pc.conditions) == 1
    cond = pc.conditions[0]
    assert isinstance(cond, Condition)
    assert cond.type == "lookup_only_search"
    assert cond.config == {}


def test_condition_explicit_empty_config_accepted():
    """The explicit ``config: {}`` form from issue #285 parses identically."""
    pc = PolicyCreate(
        id="cond-empty-config",
        effect="ALLOW",
        actions=["GET", "POST"],
        resources=[r"^/search/?$"],
        conditions=[{"type": "lookup_only_search", "config": {}}],
    )
    assert pc.conditions[0].config == {}


def test_multiple_conditions_preserved_and_ordered():
    pc = PolicyCreate(**next(
        raw for raw in _demo_policy_bundle(_DEMO_CATALOG)
        if raw["id"] == f"{_DEMO_CATALOG}-allow-anon-search-lookup-scoped"
    ))
    assert [c.type for c in pc.conditions] == [
        "lookup_only_search",
        "catalog_lookup_public_allowed",
    ]
    assert pc.actions == ["GET", "POST"]
    assert pc.effect == "ALLOW"


# ---------------------------------------------------------------------------
# Handler round-trip: PolicyCreate -> Policy -> create_policy -> PolicyResponse.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_policy_handler_round_trips_full_shape():
    """``AdminService.create_policy`` must map the wire DTO onto a domain
    :class:`Policy`, forward the ``catalog_id`` to the policy manager, and
    project the result back without dropping conditions or flipping effect.
    """
    from dynastore.extensions.admin.admin_service import AdminService

    body = PolicyCreate(
        id=f"{_DEMO_CATALOG}-allow-anon-search-lookup-scoped",
        effect="ALLOW",
        actions=["GET", "POST"],
        resources=[rf"^/search/catalogs/{_DEMO_CATALOG}/?$"],
        conditions=[
            {"type": "lookup_only_search", "config": {}},
            {"type": "catalog_lookup_public_allowed"},
        ],
    )

    captured: dict = {}

    async def _fake_create_policy(policy: Policy, catalog_id=None):
        captured["policy"] = policy
        captured["catalog_id"] = catalog_id
        return policy  # the manager normally returns the stored Policy

    fake_pm = MagicMock()
    fake_pm.create_policy = AsyncMock(side_effect=_fake_create_policy)
    fake_iam = MagicMock()
    fake_iam.get_policy_service = MagicMock(return_value=fake_pm)

    with patch(
        "dynastore.extensions.admin.admin_service._iam",
        return_value=fake_iam,
    ):
        resp = await AdminService.create_policy(body=body, catalog_id=_DEMO_CATALOG)

    # catalog-scoped POST: catalog_id forwarded verbatim to the manager.
    assert captured["catalog_id"] == _DEMO_CATALOG

    # The domain Policy carries the full shape across the DTO boundary.
    domain: Policy = captured["policy"]
    assert domain.effect == "ALLOW"
    assert domain.actions == ["GET", "POST"]
    assert domain.resources == [rf"^/search/catalogs/{_DEMO_CATALOG}/?$"]
    assert [c.type for c in domain.conditions] == [
        "lookup_only_search",
        "catalog_lookup_public_allowed",
    ]

    # The response projection preserves conditions + effect for the client.
    assert resp.id == body.id
    assert resp.effect == "ALLOW"
    assert resp.actions == ["GET", "POST"]
    assert [c.type for c in resp.conditions] == [
        "lookup_only_search",
        "catalog_lookup_public_allowed",
    ]


@pytest.mark.asyncio
async def test_create_policy_handler_accepts_deny_effect():
    """The DENY-all half of the bundle must round-trip with effect intact."""
    from dynastore.extensions.admin.admin_service import AdminService

    body = PolicyCreate(
        id=f"{_DEMO_CATALOG}-deny-stac",
        effect="DENY",
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=[rf"^/stac/catalogs/{_DEMO_CATALOG}(/.*)?$"],
    )

    fake_pm = MagicMock()
    fake_pm.create_policy = AsyncMock(side_effect=lambda policy, catalog_id=None: policy)
    fake_iam = MagicMock()
    fake_iam.get_policy_service = MagicMock(return_value=fake_pm)

    with patch(
        "dynastore.extensions.admin.admin_service._iam",
        return_value=fake_iam,
    ):
        resp = await AdminService.create_policy(body=body, catalog_id=_DEMO_CATALOG)

    assert resp.effect == "DENY"
    assert resp.actions == ["GET", "POST", "PUT", "PATCH", "DELETE"]
    assert resp.conditions == []
