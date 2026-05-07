"""Unit tests for the anonymous-create policy registered by the geoid extension."""
from unittest.mock import MagicMock

from dynastore.models.protocols.authorization import DefaultRole


def test_register_geoid_policies_registers_anonymous_create_allow(monkeypatch):
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )

    geoid_policies.register_geoid_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "geoid_anonymous_create_per_collection" in registered
    p = registered["geoid_anonymous_create_per_collection"]
    assert p.effect == "ALLOW"
    assert "POST" in p.actions
    assert any(
        "/stac/catalogs/" in r and "/collections/" in r and "/items" in r
        for r in p.resources
    )
    assert any(
        c.type == "collection_write_anonymous_allowed"
        for c in p.conditions
    )


def test_anonymous_role_grant_includes_create_policy(monkeypatch):
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )
    geoid_policies.register_geoid_policies()

    granted = set()
    for call in pm.register_role.call_args_list:
        r = call.args[0]
        if r.name == DefaultRole.ANONYMOUS.value:
            granted.update(r.policies)
    assert "geoid_anonymous_create_per_collection" in granted


def test_existing_lookup_and_deny_policies_still_register(monkeypatch):
    """Phase A's policies must still register after Phase B added the create one."""
    from dynastore.extensions.geoid import policies as geoid_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: pm,
    )
    geoid_policies.register_geoid_policies()

    registered_ids = {call.args[0].id for call in pm.register_policy.call_args_list}
    assert {
        "geoid_anonymous_lookup_per_catalog",
        "geoid_anonymous_stac_deny_lookup_only",
        "geoid_anonymous_features_deny_lookup_only",
        "geoid_anonymous_create_per_collection",
    }.issubset(registered_ids)


def test_no_op_when_permission_protocol_missing(monkeypatch, caplog):
    from dynastore.extensions.geoid import policies as geoid_policies

    monkeypatch.setattr(
        "dynastore.extensions.geoid.policies.get_protocol",
        lambda _proto: None,
    )
    with caplog.at_level("WARNING"):
        geoid_policies.register_geoid_policies()

    assert any("PermissionProtocol not available" in rec.message for rec in caplog.records)
