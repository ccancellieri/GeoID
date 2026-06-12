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

from unittest.mock import patch

import pytest


def test_register_sidecar_bounds_source_calls_register_plugin():
    """Confirms both a BoundsSourceProtocol and a GeometryFetcherProtocol
    are registered against the platform's discovery layer.

    End-to-end DB test is deferred to integration fixtures; this unit
    test ensures the factory call succeeds without runtime errors.
    """
    import dynastore.extensions.volumes.platform_bounds_source as mod
    registered = []
    with patch.object(
        mod, "register_plugin", side_effect=lambda obj: registered.append(obj),
    ):
        mod.register_sidecar_bounds_source()

    assert len(registered) == 2

    from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
    from dynastore.models.protocols.geometry_fetcher import GeometryFetcherProtocol

    assert any(isinstance(r, BoundsSourceProtocol) for r in registered)
    assert any(isinstance(r, GeometryFetcherProtocol) for r in registered)


def _register_and_capture_source():
    """Register the source/fetcher and return the BoundsSourceProtocol one."""
    import dynastore.extensions.volumes.platform_bounds_source as mod
    from dynastore.models.protocols.bounds_source import BoundsSourceProtocol

    registered = []
    with patch.object(
        mod, "register_plugin", side_effect=lambda obj: registered.append(obj),
    ):
        mod.register_sidecar_bounds_source()
    return next(r for r in registered if isinstance(r, BoundsSourceProtocol))


class _FakeCatalogs:
    async def resolve_physical_schema(self, catalog_id, *a, **k):
        return "tenant_schema"


class _FakeDriverWithConfig:
    """Stands in for ItemsPostgresqlDriver: resolves a machine-assigned
    physical table distinct from the collection_id, plus a sidecar config
    whose geom column is non-default."""

    def __init__(self, physical_table, geom_column):
        self._physical_table = physical_table
        self._geom_column = geom_column

    async def resolve_physical_table(self, catalog_id, collection_id, **k):
        return self._physical_table

    async def get_driver_config(self, catalog_id, collection_id=None, **k):
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
        )

        class _Cfg:
            sidecars = [GeometriesSidecarConfig(geom_column=self._geom_column)]

        return _Cfg()


@pytest.mark.asyncio
async def test_layout_resolver_reads_physical_table_and_geom_column():
    """The resolver must surface the driver's physical table + geom column,
    not the hardcoded collection_id / 'geom' conventions."""
    import dynastore.extensions.volumes.platform_bounds_source as mod

    source = _register_and_capture_source()

    driver = _FakeDriverWithConfig(physical_table="phys_7c2", geom_column="footprint")

    async def _fake_get_driver(op, cat, col):
        return driver

    with patch.object(mod, "get_protocol", return_value=_FakeCatalogs()), \
            patch("dynastore.modules.storage.router.get_driver", _fake_get_driver):
        layout = await source._layout_resolver("demo-3d", "denhaag")

    assert layout.schema == "tenant_schema"
    assert layout.hub_table == "phys_7c2"
    assert layout.geometries_table == "phys_7c2_geometries"
    assert layout.geom_column == "footprint"
    assert layout.feature_id_column == "geoid"


@pytest.mark.asyncio
async def test_layout_resolver_degrades_to_defaults_when_driver_unavailable():
    """A driver-resolution miss must fall back to the standard layout
    (collection_id hub + fallback geom column) rather than raising — a
    raise would silently empty the tileset."""
    import dynastore.extensions.volumes.platform_bounds_source as mod

    source = _register_and_capture_source()

    async def _boom_get_driver(op, cat, col):
        raise RuntimeError("no driver for this collection")

    with patch.object(mod, "get_protocol", return_value=_FakeCatalogs()), \
            patch("dynastore.modules.storage.router.get_driver", _boom_get_driver):
        layout = await source._layout_resolver("demo-3d", "denhaag")

    assert layout.schema == "tenant_schema"
    assert layout.hub_table == "denhaag"
    assert layout.geometries_table == "denhaag_geometries"
    assert layout.geom_column == "geom"
    assert layout.feature_id_column == "geoid"
