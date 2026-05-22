"""Unit tests for the geoid extension's anonymous lookup policy registration:

- ALLOW anonymous on POST /search/catalogs/{cat}/items-search when catalog opts in.
- DENY anonymous on /stac/catalogs/{cat}/* and /features/catalogs/{cat}/*
  when the same catalog opts in (lockdown to lookup-only for anonymous).
"""
from unittest.mock import MagicMock

from dynastore.models.protocols.authorization import IamRolesConfig

_ANON = IamRolesConfig().anonymous_role_name


def test_register_geoid_policies_registers_anonymous_lookup_allow(monkeypatch):
    """Registers ALLOW policy on POST /search/catalogs/.../items-search with the
    lookup-public condition (#1210)."""
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )

    geoid_policies.register_geoid_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "geoid_anonymous_lookup_per_catalog" in registered
    p = registered["geoid_anonymous_lookup_per_catalog"]
    assert p.effect == "ALLOW"
    assert p.actions == ["POST"]
    # Every opened resource is the items-search sub-path — the ALLOW must NOT
    # leak onto the broad catalog-scoped item search (POST /search/catalogs/{cat}).
    assert p.resources
    assert all("items-search" in r for r in p.resources)
    assert any(c.type == "catalog_lookup_public_allowed" for c in p.conditions)


def test_register_geoid_policies_registers_stac_anon_deny(monkeypatch):
    """Registers DENY policy on /stac/catalogs/{cat}/* anonymous when catalog opted in."""
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )
    geoid_policies.register_geoid_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "geoid_anonymous_stac_deny_lookup_only" in registered
    p = registered["geoid_anonymous_stac_deny_lookup_only"]
    assert p.effect == "DENY"
    assert any("/stac/catalogs/" in r for r in p.resources)
    assert any(c.type == "catalog_lookup_public_allowed" for c in p.conditions)


def test_register_geoid_policies_registers_features_anon_deny(monkeypatch):
    """Registers DENY policy on /features/catalogs/{cat}/* anonymous when catalog opted in."""
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )
    geoid_policies.register_geoid_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "geoid_anonymous_features_deny_lookup_only" in registered
    p = registered["geoid_anonymous_features_deny_lookup_only"]
    assert p.effect == "DENY"
    assert any("/features/catalogs/" in r for r in p.resources)
    assert any(c.type == "catalog_lookup_public_allowed" for c in p.conditions)


def test_anonymous_role_grants_all_three_lookup_policies(monkeypatch):
    """The ANONYMOUS role aggregates the ALLOW + 2 DENY policies."""
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )
    geoid_policies.register_geoid_policies()

    anon_grants = [
        call.args[0] for call in pm.register_role.call_args_list
        if call.args[0].name == _ANON
    ]
    assert anon_grants, "ANONYMOUS role grant missing"
    granted_policies = set()
    for g in anon_grants:
        granted_policies.update(g.policies)
    assert "geoid_anonymous_lookup_per_catalog" in granted_policies
    assert "geoid_anonymous_stac_deny_lookup_only" in granted_policies
    assert "geoid_anonymous_features_deny_lookup_only" in granted_policies


def test_register_geoid_policies_no_op_when_permission_protocol_missing(monkeypatch, caplog):
    from dynastore.extensions.geoid import policies as geoid_policies

    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: None,
    )

    with caplog.at_level("WARNING"):
        geoid_policies.register_geoid_policies()

    assert any("PermissionProtocol not available" in rec.message for rec in caplog.records)
