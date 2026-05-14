"""Unit tests for the ``IamRolesConfig`` PluginConfig (geoid#643 slice 2).

Pins the tier-split shape that replaces the single-list ``roles`` field:

- defaults seed ``platform_roles=[sysadmin]`` and
  ``catalog_roles=[admin, editor, user, unauthenticated]``;
- role inheritance is encoded on ``RoleSeed.parent``, not a separate
  ``hierarchy`` list (catalog chain: admin → editor → user → unauthenticated);
- named slots resolve to the correct tier — ``sysadmin_role_name`` must
  be in ``platform_roles``; ``admin/editor/default_user/anonymous`` slots
  must be in ``catalog_roles``;
- ``admin_tier_role_names`` may span tiers (default: ``[sysadmin, admin]``);
- cycles in the parent links are rejected;
- cross-tier parents are rejected (a catalog role cannot inherit from a
  platform role and vice-versa);
- role names are globally unique across tiers.
"""

import pytest

from dynastore.models.protocols.authorization import (
    IamRolesConfig,
    RoleSeed,
)


def test_defaults_seed_platform_sysadmin_only() -> None:
    cfg = IamRolesConfig()
    assert [r.name for r in cfg.platform_roles] == ["sysadmin"]


def test_defaults_seed_catalog_tier_four_roles() -> None:
    cfg = IamRolesConfig()
    assert [r.name for r in cfg.catalog_roles] == [
        "admin",
        "editor",
        "user",
        "unauthenticated",
    ]
    # `viewer` was dropped from the default seed (geoid#643 slice 1).
    # `anonymous` was renamed to `unauthenticated` (geoid#643 slice 2).
    for forbidden in ("viewer", "anonymous"):
        assert forbidden not in [r.name for r in cfg.catalog_roles]


def test_catalog_chain_parent_links() -> None:
    """Catalog tier inheritance: admin > editor > user > unauthenticated."""
    cfg = IamRolesConfig()
    parents = {r.name: r.parent for r in cfg.catalog_roles}
    assert parents == {
        "admin": None,
        "editor": "admin",
        "user": "editor",
        "unauthenticated": "user",
    }


def test_platform_sysadmin_has_no_parent() -> None:
    cfg = IamRolesConfig()
    assert cfg.platform_roles[0].parent is None


def test_default_named_slots() -> None:
    cfg = IamRolesConfig()
    assert cfg.sysadmin_role_name == "sysadmin"
    assert cfg.admin_role_name == "admin"
    assert cfg.editor_role_name == "editor"
    assert cfg.default_user_role_name == "user"
    # Renamed default per slice 2.
    assert cfg.anonymous_role_name == "unauthenticated"


def test_admin_role_set_spans_tiers() -> None:
    """``admin_tier_role_names`` default spans tiers by design:
    sysadmin (platform) + admin (catalog)."""
    cfg = IamRolesConfig()
    assert cfg.admin_role_set == frozenset({"sysadmin", "admin"})


def test_role_names_property_unions_both_tiers() -> None:
    cfg = IamRolesConfig()
    assert set(cfg.role_names) == {
        "sysadmin",
        "admin",
        "editor",
        "user",
        "unauthenticated",
    }


def test_platform_role_names_property_returns_platform_tier_only() -> None:
    cfg = IamRolesConfig()
    assert cfg.platform_role_names == frozenset({"sysadmin"})


def test_operator_added_catalog_role_flows_through() -> None:
    """An operator can append a 5th catalog role via PATCH — verify it
    shows up in role_names and the catalog set."""
    cfg = IamRolesConfig(
        catalog_roles=[
            *IamRolesConfig().catalog_roles,
            RoleSeed(name="reviewer", description="External reviewer",
                     policies=[], level=40, parent="editor"),
        ],
    )
    assert "reviewer" in cfg.role_names
    assert "reviewer" not in cfg.platform_role_names


def test_sysadmin_slot_must_be_in_platform_roles() -> None:
    with pytest.raises(ValueError, match="sysadmin_role_name"):
        IamRolesConfig(sysadmin_role_name="admin")  # admin is catalog-tier


def test_admin_slot_must_be_in_catalog_roles() -> None:
    with pytest.raises(ValueError, match="admin_role_name"):
        IamRolesConfig(admin_role_name="sysadmin")  # sysadmin is platform-tier


def test_anonymous_slot_must_be_in_catalog_roles() -> None:
    with pytest.raises(ValueError, match="anonymous_role_name"):
        IamRolesConfig(anonymous_role_name="sysadmin")


def test_admin_tier_role_names_must_be_known() -> None:
    with pytest.raises(ValueError, match="admin_tier_role_names"):
        IamRolesConfig(admin_tier_role_names=["sysadmin", "admin", "ghost"])


def test_parent_cycle_rejected() -> None:
    """A catalog-tier role cannot have a parent chain that loops."""
    with pytest.raises(ValueError, match="cycle"):
        IamRolesConfig(
            catalog_roles=[
                RoleSeed(name="admin", level=100, parent="editor"),
                RoleSeed(name="editor", level=50, parent="admin"),
                RoleSeed(name="user", level=10, parent="editor"),
                RoleSeed(name="unauthenticated", level=0, parent="user"),
            ],
        )


def test_cross_tier_parent_rejected() -> None:
    """A catalog role cannot point at a platform-tier role via `parent`."""
    with pytest.raises(ValueError, match="parent=|not in"):
        IamRolesConfig(
            catalog_roles=[
                RoleSeed(name="admin", level=100, parent="sysadmin"),
                RoleSeed(name="editor", level=50, parent="admin"),
                RoleSeed(name="user", level=10, parent="editor"),
                RoleSeed(name="unauthenticated", level=0, parent="user"),
            ],
        )


def test_role_name_uniqueness_across_tiers() -> None:
    """A name appearing in both tiers is rejected — names must be unique
    globally so grant-resolution is unambiguous."""
    with pytest.raises(ValueError, match="appear in BOTH"):
        IamRolesConfig(
            platform_roles=[
                RoleSeed(name="sysadmin", level=100),
                RoleSeed(name="admin", level=90),  # also in catalog tier
            ],
            catalog_roles=[
                RoleSeed(name="admin", level=100),
                RoleSeed(name="editor", level=50, parent="admin"),
                RoleSeed(name="user", level=10, parent="editor"),
                RoleSeed(name="unauthenticated", level=0, parent="user"),
            ],
        )


def test_config_address_is_platform_iam_roles() -> None:
    assert IamRolesConfig._address == ("platform", "iam", "roles")
