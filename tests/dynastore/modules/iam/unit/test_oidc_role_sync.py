"""Pure unit tests for the OIDC -> internal role reconciler.

The reconciler in ``dynastore.modules.iam.oidc_role_sync`` is intentionally
I/O-free so its diff and policy logic can be exercised without a database,
identity provider, or PluginConfig table. These tests pin the contract
that wraps the wider IamService.authenticate flow:

- only mapped roles drive grant/revoke decisions;
- unrelated internal roles are never proposed for revocation;
- the issuer whitelist gates assignment;
- the ``enabled=False`` default short-circuits the overlay used at
  first-time auto-registration.
"""

from dynastore.modules.iam.oidc_role_sync import (
    RoleAction,
    diff,
    initial_role_overlay,
    is_issuer_allowed,
)
from dynastore.modules.iam.oidc_role_sync_config import OidcRoleSyncConfig


MAPPING = {"geoid.sysadmin": "sysadmin"}


def test_diff_grants_when_oidc_has_role_and_internal_does_not():
    actions = diff(
        oidc_roles=["geoid.sysadmin"],
        current_internal_roles=[],
        role_mapping=MAPPING,
    )
    assert actions == [RoleAction(role_name="sysadmin", action="grant")]


def test_diff_revokes_when_internal_has_mapped_role_and_oidc_does_not():
    actions = diff(
        oidc_roles=[],
        current_internal_roles=["sysadmin"],
        role_mapping=MAPPING,
    )
    assert actions == [RoleAction(role_name="sysadmin", action="revoke")]


def test_diff_noop_when_both_sides_match():
    actions = diff(
        oidc_roles=["geoid.sysadmin"],
        current_internal_roles=["sysadmin"],
        role_mapping=MAPPING,
    )
    assert actions == []


def test_diff_ignores_unmapped_oidc_roles():
    actions = diff(
        oidc_roles=["geoid.editor", "some.other.role"],
        current_internal_roles=[],
        role_mapping=MAPPING,
    )
    assert actions == []


def test_diff_ignores_unmapped_internal_roles():
    # 'admin' is not in the mapping values; it must NOT be revoked even
    # if the OIDC token doesn't list a corresponding role.
    actions = diff(
        oidc_roles=[],
        current_internal_roles=["admin", "viewer"],
        role_mapping=MAPPING,
    )
    assert actions == []


def test_diff_handles_multiple_mapped_roles():
    mapping = {"kc.admin": "admin", "kc.sysadmin": "sysadmin"}
    actions = diff(
        oidc_roles=["kc.admin"],
        current_internal_roles=["sysadmin"],
        role_mapping=mapping,
    )
    assert RoleAction("admin", "grant") in actions
    assert RoleAction("sysadmin", "revoke") in actions
    assert len(actions) == 2


def test_is_issuer_allowed_no_whitelist_means_allowed():
    assert is_issuer_allowed("https://anything", None) is True
    assert is_issuer_allowed(None, None) is True


def test_is_issuer_allowed_whitelist_match():
    assert is_issuer_allowed("https://kc/realms/x", ["https://kc/realms/x"]) is True


def test_is_issuer_allowed_whitelist_miss():
    assert is_issuer_allowed("https://kc/realms/y", ["https://kc/realms/x"]) is False
    assert is_issuer_allowed(None, ["https://kc/realms/x"]) is False


def test_initial_overlay_disabled_returns_base():
    cfg = OidcRoleSyncConfig(enabled=False)
    out = initial_role_overlay(
        oidc_roles=["geoid.sysadmin"],
        base_roles=["user"],
        config=cfg,
        issuer="https://kc/realms/r",
    )
    assert out == ["user"]


def test_initial_overlay_grants_mapped_role_when_enabled():
    cfg = OidcRoleSyncConfig(enabled=True)
    out = initial_role_overlay(
        oidc_roles=["geoid.sysadmin"],
        base_roles=["user"],
        config=cfg,
        issuer="https://kc/realms/r",
    )
    assert out == ["user", "sysadmin"]


def test_initial_overlay_replaces_existing_mapped_internal_role():
    # If the base already lists 'sysadmin' but the token doesn't carry
    # the OIDC role, the overlay drops it. Internal-only roles like
    # 'user' survive.
    cfg = OidcRoleSyncConfig(enabled=True)
    out = initial_role_overlay(
        oidc_roles=[],
        base_roles=["user", "sysadmin"],
        config=cfg,
        issuer="https://kc/realms/r",
    )
    assert out == ["user"]


def test_initial_overlay_blocked_by_issuer_whitelist():
    cfg = OidcRoleSyncConfig(
        enabled=True, issuer_whitelist=["https://trusted/realms/x"]
    )
    out = initial_role_overlay(
        oidc_roles=["geoid.sysadmin"],
        base_roles=["user"],
        config=cfg,
        issuer="https://untrusted/realms/y",
    )
    assert out == ["user"]


def test_config_defaults_off():
    cfg = OidcRoleSyncConfig()
    assert cfg.enabled is False
    assert cfg.role_mapping == {"geoid.sysadmin": "sysadmin"}
    assert cfg.ttl_seconds == 60
    assert cfg.issuer_whitelist is None


def test_config_address_is_platform_iam_oidc_role_sync():
    assert OidcRoleSyncConfig._address == ("platform", "iam", "oidc_role_sync")
