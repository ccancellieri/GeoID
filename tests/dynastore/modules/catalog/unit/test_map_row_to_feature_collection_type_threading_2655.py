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

"""#2655: ``ItemService.map_row_to_feature`` optionally threads the real
``CollectionInfo.kind`` (+ ``allow_geometry``) into its internal
``_effective_sidecars`` resolution — the same resolution
``collection_has_geometry()`` / ``ItemsPostgresqlDriver.ensure_storage``
already use — so a RECORDS row-mapping pipeline no longer resolves a
geometries sidecar it will never have data for.

The parameters are additive and default to ``None``: every existing caller
that omits them gets byte-identical resolution to before this change
(``_effective_sidecars`` falls back to its own ``"VECTOR"`` default).
"""

from typing import Any, Dict
from unittest.mock import MagicMock

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


def _svc() -> ItemService:
    return ItemService(engine=MagicMock())


def _row() -> dict:
    return {"geoid": "g-1", "attributes": "{}"}


def _install_effective_sidecars_spy():
    """Wrap the real ``_effective_sidecars`` and raise with its result so the
    test can assert on the resolved sidecar list and the kwargs
    ``map_row_to_feature`` passed in, without mocking the sidecar registry.

    Mirrors ``TestEnsureStorageCollectionTypeThreading`` in
    ``test_postgresql_driver.py``.
    """
    from dynastore.modules.storage.drivers.pg_sidecars import (
        _effective_sidecars as _real_effective_sidecars,
    )

    captured: Dict[str, Any] = {}

    class _StopAfterSidecars(Exception):
        pass

    def _spy(*args, **kwargs):
        captured["collection_type"] = kwargs.get("collection_type")
        captured["context"] = kwargs.get("context")
        captured["sidecars"] = _real_effective_sidecars(*args, **kwargs)
        raise _StopAfterSidecars

    from unittest.mock import patch

    return (
        patch(
            "dynastore.modules.storage.drivers.pg_sidecars._effective_sidecars",
            side_effect=_spy,
        ),
        captured,
        _StopAfterSidecars,
    )


def test_records_collection_type_omits_geometry_sidecar():
    """Passing collection_type="RECORDS" resolves only the attributes
    sidecar — the geometries sidecar is never instantiated for the row."""
    spy_patch, captured, stop_exc = _install_effective_sidecars_spy()

    import pytest

    with spy_patch:
        with pytest.raises(stop_exc):
            _svc().map_row_to_feature(
                _row(), ItemsPostgresqlDriverConfig(), collection_type="RECORDS",
            )

    assert captured["collection_type"] == "RECORDS"
    sidecar_types = [s.sidecar_type for s in captured["sidecars"]]
    assert "geometries" not in sidecar_types
    assert "attributes" in sidecar_types


def test_vector_collection_type_keeps_geometry_sidecar():
    """Passing collection_type="VECTOR" explicitly still resolves both
    geometries and attributes — unaffected by the new optional param."""
    spy_patch, captured, stop_exc = _install_effective_sidecars_spy()

    import pytest

    with spy_patch:
        with pytest.raises(stop_exc):
            _svc().map_row_to_feature(
                _row(), ItemsPostgresqlDriverConfig(), collection_type="VECTOR",
            )

    assert captured["collection_type"] == "VECTOR"
    sidecar_types = [s.sidecar_type for s in captured["sidecars"]]
    assert "geometries" in sidecar_types
    assert "attributes" in sidecar_types


def test_omitted_collection_type_is_byte_identical_to_pre_2655():
    """No collection_type/allow_geometry passed (every pre-existing caller)
    → _effective_sidecars is invoked exactly as before this change, with no
    collection_type/context kwargs at all."""
    spy_patch, captured, stop_exc = _install_effective_sidecars_spy()

    import pytest

    with spy_patch:
        with pytest.raises(stop_exc):
            _svc().map_row_to_feature(_row(), ItemsPostgresqlDriverConfig())

    assert captured["collection_type"] is None
    assert captured["context"] is None
    # Falls back to _effective_sidecars' own "VECTOR" default.
    sidecar_types = [s.sidecar_type for s in captured["sidecars"]]
    assert "geometries" in sidecar_types
    assert "attributes" in sidecar_types


def test_allow_geometry_true_overrides_records_stripping():
    """RFC #2550: an explicit allow_geometry=True keeps the geometry sidecar
    for a RECORDS collection with an explicit geometry sidecar config."""
    import pytest
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
    )

    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[
            FeatureAttributeSidecarConfig(sidecar_type="attributes"),
            GeometriesSidecarConfig(sidecar_type="geometries"),
        ]
    )

    spy_patch, captured, stop_exc = _install_effective_sidecars_spy()

    with spy_patch:
        with pytest.raises(stop_exc):
            _svc().map_row_to_feature(
                _row(), col_config, collection_type="RECORDS", allow_geometry=True,
            )

    sidecar_types = [s.sidecar_type for s in captured["sidecars"]]
    assert "geometries" in sidecar_types
