"""Parametrised coverage test: every togglable extension must inherit ExposableConfigMixin."""

# Importing each extension/module package (not the .config submodule directly) is
# sufficient for registration: each __init__.py now carries `from . import config`
# which triggers auto-registration via PersistentModel.__init_subclass__.
import dynastore.extensions.features  # noqa: F401
import dynastore.extensions.wfs  # noqa: F401
import dynastore.modules.tiles  # noqa: F401
import dynastore.modules.stac  # noqa: F401
import dynastore.modules.gcp  # noqa: F401
import dynastore.extensions.maps  # noqa: F401
import dynastore.extensions.coverages  # noqa: F401
import dynastore.extensions.records  # noqa: F401
import dynastore.extensions.processes  # noqa: F401
import dynastore.extensions.dimensions  # noqa: F401
import dynastore.extensions.dwh  # noqa: F401
import dynastore.extensions.search  # noqa: F401
import dynastore.modules.stats  # noqa: F401
import dynastore.extensions.logs  # noqa: F401
import dynastore.extensions.notebooks  # noqa: F401
import dynastore.extensions.crs  # noqa: F401
import dynastore.extensions.gdal  # noqa: F401
import dynastore.extensions.assets  # noqa: F401
import dynastore.extensions.styles  # noqa: F401
import dynastore.extensions.dggs  # noqa: F401
import dynastore.extensions.edr  # noqa: F401
import dynastore.extensions.joins  # noqa: F401
import dynastore.extensions.moving_features  # noqa: F401
import dynastore.extensions.connected_systems  # noqa: F401
import dynastore.extensions.volumes  # noqa: F401

import pytest
from dynastore.extensions.tools.exposure_mixin import (
    ExposableConfigMixin,
    KNOWN_EXTENSION_IDS,
    ALWAYS_ON_EXTENSIONS,
    find_dead_exposable_configs,
)
from dynastore.modules.db_config.platform_config_service import list_registered_configs


TOGGLABLE = KNOWN_EXTENSION_IDS - ALWAYS_ON_EXTENSIONS


@pytest.mark.parametrize("ext_id", sorted(TOGGLABLE))
def test_togglable_has_exposable_config(ext_id):
    """Every togglable extension MUST register at least one PluginConfig inheriting the mixin."""
    classes = list_registered_configs().values()
    matching = [
        c for c in classes
        if c.__module__.startswith(f"dynastore.extensions.{ext_id}")
        or c.__module__.startswith(f"dynastore.modules.{ext_id}")
    ]
    assert matching, f"Extension '{ext_id}' registers no PluginConfig; add one (atomic design, no always-on hedge)."
    assert any(issubclass(c, ExposableConfigMixin) for c in matching), (
        f"Extension '{ext_id}' has no PluginConfig inheriting ExposableConfigMixin."
    )


def test_no_extension_redeclares_enabled_field():
    """Inherited field only — do not shadow the mixin."""
    for cls in list_registered_configs().values():
        if not issubclass(cls, ExposableConfigMixin):
            continue
        own_fields = set(cls.__dict__.get("__annotations__", {}))
        assert "enabled" not in own_fields, (
            f"{cls.__name__} shadows ExposableConfigMixin.enabled — remove the local field."
        )


def test_no_dead_exposable_configs():
    """Regression guard for #853/#854: every ExposableConfigMixin subclass
    must be visible to ExposureMatrix at lifespan startup.

    If this fails, a config has been added (or moved) such that its
    `enabled` field appears in /configs/ responses but flipping it does
    nothing. Either strip the mixin or register the extension name in
    KNOWN_EXTENSION_IDS — see find_dead_exposable_configs() docstring.
    """
    dead = find_dead_exposable_configs()
    assert not dead, "Dead ExposableConfigMixin uses:\n" + "\n".join(
        f"  - {cls.__module__}.{cls.__name__}: {reason}"
        for cls, reason in dead
    )
