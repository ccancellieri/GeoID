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

"""Regression: ``maps_service._resolve_target_srid`` must not read the
legacy ``geometry_storage`` field.

``ItemsPostgresqlDriverConfig`` dropped its ``geometry_storage`` field when
sidecar tables became a derived (Computed) ``sidecars`` list; ``target_srid``
now lives on the geometries sidecar entry within that list. GeoID #2744:
``layer_config.geometry_storage`` raised ``AttributeError`` and 500'd every
styled map render once the #2722 SQL fix started reaching this code path.
"""

from __future__ import annotations

import sys
import types


def _install_osgeo_stubs() -> bool:
    if "osgeo" in sys.modules:
        return False
    osgeo = types.ModuleType("osgeo")
    osgeo.__version__ = "3.x.stub"  # type: ignore[attr-defined]

    gdal = types.ModuleType("osgeo.gdal")
    gdal.Dataset = object  # type: ignore[attr-defined]
    gdal.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    gdal.VSIFOpenL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFSeekL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFTellL = lambda *a: 0  # type: ignore[attr-defined]
    gdal.VSIFReadL = lambda *a: b""  # type: ignore[attr-defined]
    gdal.VSIFCloseL = lambda *a: None  # type: ignore[attr-defined]
    gdal.Unlink = lambda *a: None  # type: ignore[attr-defined]
    gdal.RasterizeLayer = lambda *a, **kw: None  # type: ignore[attr-defined]

    ogr = types.ModuleType("osgeo.ogr")
    ogr.Geometry = object  # type: ignore[attr-defined]
    ogr.Layer = object  # type: ignore[attr-defined]
    ogr.Feature = lambda *a: None  # type: ignore[attr-defined]
    ogr.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    ogr.wkbLineString = 2  # type: ignore[attr-defined]
    ogr.wkbMultiPolygon = 6  # type: ignore[attr-defined]

    osr = types.ModuleType("osgeo.osr")
    osr.SpatialReference = object  # type: ignore[attr-defined]

    sys.modules.update({
        "osgeo": osgeo,
        "osgeo.gdal": gdal,
        "osgeo.ogr": ogr,
        "osgeo.osr": osr,
    })
    return True


def _uninstall_osgeo_stubs() -> None:
    for name in ("osgeo.osr", "osgeo.ogr", "osgeo.gdal", "osgeo"):
        sys.modules.pop(name, None)


_we_installed_osgeo_stubs = _install_osgeo_stubs()

from dynastore.extensions.maps import maps_service as ms  # noqa: E402
from dynastore.modules.storage.driver_config import (  # noqa: E402
    ItemsPostgresqlDriverConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (  # noqa: E402
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (  # noqa: E402
    GeometriesSidecarConfig,
)

if _we_installed_osgeo_stubs:
    _uninstall_osgeo_stubs()


def test_explicit_target_srid_read_from_geometries_sidecar():
    """A materialised collection with a geometries sidecar returns that
    sidecar's ``target_srid``, not the default."""
    cfg = ItemsPostgresqlDriverConfig()
    object.__setattr__(
        cfg,
        "sidecars",
        [
            FeatureAttributeSidecarConfig(),
            GeometriesSidecarConfig(target_srid=3857),
        ],
    )

    assert ms._resolve_target_srid(cfg) == 3857


def test_empty_sidecars_falls_back_to_default_without_raising():
    """Empty ``sidecars`` (pre-materialisation FEATURES collection, or any
    RECORDS collection per #2655) must not raise and must fall back to the
    geometries sidecar's own default SRID."""
    cfg = ItemsPostgresqlDriverConfig()
    assert cfg.sidecars == []

    assert ms._resolve_target_srid(cfg) == 4326


def test_no_attribute_error_on_legacy_geometry_storage_path():
    """The legacy field is gone; resolving the SRID must never touch
    ``geometry_storage`` (GeoID #2744 regression)."""
    cfg = ItemsPostgresqlDriverConfig()
    assert not hasattr(cfg, "geometry_storage")

    # Must not raise AttributeError.
    ms._resolve_target_srid(cfg)


def test_non_pg_config_degrades_to_default():
    """A resolved layer config without a ``sidecars`` attribute at all
    (e.g. a non-PG driver config) degrades cleanly via ``driver_sidecars``."""

    class _NonPgConfig:
        pass

    assert ms._resolve_target_srid(_NonPgConfig()) == 4326
