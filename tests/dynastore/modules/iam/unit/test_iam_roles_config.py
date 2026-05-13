"""Unit tests for the ``IamRolesConfig`` PluginConfig.

Pins the shape that replaces the old fixed-field-per-role ``IamRoleConfig``:

- defaults seed 5 roles (sysadmin/admin/editor/user/anonymous; no `viewer`);
- hierarchy default is the linear sysadmin > admin > editor > user > anonymous chain;
- named slots (sysadmin/admin/editor/default_user/anonymous role names) must
  point at a real entry in `roles`;
- `admin_tier_role_names` entries must be present in `roles`;
- cycles in `hierarchy` are rejected at validation time.
"""

import pytest

from dynastore.models.protocols.authorization import (
    IamRolesConfig,
    RoleSeed,
)


def test_defaults_seed_five_platform_tier_roles_without_viewer() -> None:
    cfg = IamRolesConfig()
    names = [r.name for r in cfg.roles]
    assert names == ["sysadmin", "admin", "editor", "user", "anonymous"]
    assert all(r.is_platform_tier for r in cfg.roles)
    # `viewer` was dropped from the default seed (geoid#643).
    assert "viewer" not in names


def test_default_named_slots() -> None:
    cfg = IamRolesConfig()
    assert cfg.sysadmin_role_name == "sysadmin"
    assert cfg.admin_role_name == "admin"
    assert cfg.editor_role_name == "editor"
    assert cfg.default_user_role_name == "user"
    assert cfg.anonymous_role_name == "anonymous"


def test_admin_role_set_property_derives_from_admin_tier_role_names() -> None:
    cfg = IamRolesConfig()
    assert cfg.admin_role_set == frozenset({"admin", "sysadmin"})


def test_role_names_property() -> None:
    cfg = IamRolesConfig()
    assert set(cfg.role_names) == {
        "sysadmin", "admin", "editor", "user", "anonymous",
    }


def test_hierarchy_default_is_linear_chain_minus_viewer() -> None:
    cfg = IamRolesConfig()
    assert cfg.hierarchy == [
        ("sysadmin", "admin"),
        ("admin",    "editor"),
        ("editor",   "user"),
        ("user",     "anonymous"),
    ]


def test_operator_added_role_flows_through() -> None:
    """An operator can append a 6th role via PATCH — verify it shows up
    in `role_names` and `_get_default_roles` semantics."""
    cfg = IamRolesConfig(
        roles=[
            *IamRolesConfig().roles,
            RoleSeed(name="reviewer", description="External reviewer", policies=[]),
        ],
        hierarchy=[
            *IamRolesConfig().hierarchy,
            ("editor", "reviewer"),
        ],
    )
    assert "reviewer" in cfg.role_names


def test_slot_referencing_unknown_role_rejected() -> None:
    with pytest.raises(ValueError, match="sysadmin_role_name"):
        IamRolesConfig(sysadmin_role_name="missing_role")


def test_admin_tier_role_names_must_be_in_roles() -> None:
    with pytest.raises(ValueError, match="admin_tier_role_names"):
        IamRolesConfig(admin_tier_role_names=["admin", "sysadmin", "ghost"])


def test_hierarchy_cycle_rejected() -> None:
    with pytest.raises(ValueError, match="cycle"):
        IamRolesConfig(
            hierarchy=[
                ("sysadmin", "admin"),
                ("admin",    "sysadmin"),
            ],
        )


def test_config_address_is_platform_iam_roles() -> None:
    assert IamRolesConfig._address == ("platform", "iam", "roles")
