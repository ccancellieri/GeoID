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

"""Boundary validator tests for sidecar config column-name fields (#2314).

Column names are interpolated into SQL as identifiers (they cannot be bound
parameters), so each sidecar config class validates its column-name fields at
parse time via a Pydantic ``field_validator``.  These tests pin that contract:
an injection-shaped value must be rejected with ``ValidationError`` at config
construction time, before it can ever reach an f-string query.

Covered config classes:
  - ``FeatureAttributeSidecarConfig`` — external_id_field, asset_id_field,
    validity_column
  - ``GeometriesSidecarConfig`` — geom_column, bbox_column
  - ``AccessEnvelopeSidecarConfig`` — column_name
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)

# Representative injection payloads — any one of these reaching an f-string
# would close the quoted identifier and open an injection channel.
_INJECTION_CASES = [
    "bad'; DROP TABLE items; --",
    'col; DROP TABLE items --',
    'a"b',
    "col WITH spaces",
    "1leading_digit",
    "",          # empty string
    "x" * 64,   # exceeds 63-char PG limit
    "select",   # SQL reserved word
]


# ---------------------------------------------------------------------------
# FeatureAttributeSidecarConfig
# ---------------------------------------------------------------------------

class TestFeatureAttributeSidecarConfigValidators:
    """Column-name fields on FeatureAttributeSidecarConfig reject injection values."""

    @pytest.mark.parametrize("bad", _INJECTION_CASES)
    def test_external_id_field_rejects_injection(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            FeatureAttributeSidecarConfig(external_id_field=bad)

    @pytest.mark.parametrize("bad", _INJECTION_CASES)
    def test_asset_id_field_rejects_injection(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            FeatureAttributeSidecarConfig(asset_id_field=bad)

    @pytest.mark.parametrize("bad", _INJECTION_CASES)
    def test_validity_column_rejects_injection(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            FeatureAttributeSidecarConfig(validity_column=bad)

    def test_external_id_field_none_is_valid(self) -> None:
        """None disables the column — it must always be accepted."""
        cfg = FeatureAttributeSidecarConfig(external_id_field=None)
        assert cfg.external_id_field is None

    def test_asset_id_field_none_is_valid(self) -> None:
        cfg = FeatureAttributeSidecarConfig(asset_id_field=None)
        assert cfg.asset_id_field is None

    def test_validity_column_none_is_valid(self) -> None:
        cfg = FeatureAttributeSidecarConfig(validity_column=None)
        assert cfg.validity_column is None

    @pytest.mark.parametrize("name", ["external_id", "fid", "_id_col", "CityID"])
    def test_valid_external_id_field_accepted(self, name: str) -> None:
        cfg = FeatureAttributeSidecarConfig(external_id_field=name)
        assert cfg.external_id_field == name

    @pytest.mark.parametrize("name", ["asset_id", "_asset", "AssetID"])
    def test_valid_asset_id_field_accepted(self, name: str) -> None:
        cfg = FeatureAttributeSidecarConfig(asset_id_field=name)
        assert cfg.asset_id_field == name

    @pytest.mark.parametrize("name", ["validity", "_valid_at", "ValidOn"])
    def test_valid_validity_column_accepted(self, name: str) -> None:
        cfg = FeatureAttributeSidecarConfig(validity_column=name)
        assert cfg.validity_column == name

    def test_feature_id_field_name_property_reflects_external_id_field(self) -> None:
        """The property used at SQL interpolation points returns the validated value."""
        cfg = FeatureAttributeSidecarConfig(external_id_field="my_fid")
        assert cfg.feature_id_field_name == "my_fid"

    def test_feature_id_field_name_property_returns_none_when_disabled(self) -> None:
        cfg = FeatureAttributeSidecarConfig(external_id_field=None)
        assert cfg.feature_id_field_name is None


# ---------------------------------------------------------------------------
# GeometriesSidecarConfig
# ---------------------------------------------------------------------------

class TestGeometriesSidecarConfigValidators:
    """Column-name fields on GeometriesSidecarConfig reject injection values."""

    @pytest.mark.parametrize("bad", _INJECTION_CASES)
    def test_geom_column_rejects_injection(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            GeometriesSidecarConfig(geom_column=bad)

    @pytest.mark.parametrize("bad", _INJECTION_CASES)
    def test_bbox_column_rejects_injection(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            GeometriesSidecarConfig(bbox_column=bad)

    def test_bbox_column_none_is_valid(self) -> None:
        """None disables the bbox column — it must always be accepted."""
        cfg = GeometriesSidecarConfig(bbox_column=None)
        assert cfg.bbox_column is None
        assert cfg.write_bbox is False

    @pytest.mark.parametrize("name", ["geom", "geometry", "the_geom", "wkb_geom", "Geom4326"])
    def test_valid_geom_column_accepted(self, name: str) -> None:
        cfg = GeometriesSidecarConfig(geom_column=name)
        assert cfg.geom_column == name

    @pytest.mark.parametrize("name", ["bbox_geom", "bbox", "BBox"])
    def test_valid_bbox_column_accepted(self, name: str) -> None:
        cfg = GeometriesSidecarConfig(bbox_column=name)
        assert cfg.bbox_column == name


# ---------------------------------------------------------------------------
# AccessEnvelopeSidecarConfig
# ---------------------------------------------------------------------------

class TestAccessEnvelopeSidecarConfigValidator:
    """column_name on AccessEnvelopeSidecarConfig rejects injection values."""

    @pytest.mark.parametrize("bad", _INJECTION_CASES)
    def test_column_name_rejects_injection(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            AccessEnvelopeSidecarConfig(column_name=bad)

    @pytest.mark.parametrize("name", ["access_envelope", "acl", "_envelope", "MyEnv"])
    def test_valid_column_name_accepted(self, name: str) -> None:
        cfg = AccessEnvelopeSidecarConfig(column_name=name)
        assert cfg.column_name == name

    def test_default_column_name_is_valid_identifier(self) -> None:
        """The hard-coded default must itself satisfy the validator."""
        cfg = AccessEnvelopeSidecarConfig()
        # If the default were invalid the constructor above would have raised.
        assert cfg.column_name == "access_envelope"


# ---------------------------------------------------------------------------
# Round-trip: invalid value is caught even when embedded inside driver config
# ---------------------------------------------------------------------------

class TestInjectionCaughtThroughDriverConfig:
    """An injection value nested inside ItemsPostgresqlDriverConfig is caught.

    This mirrors the real insertion path: operator API → driver config →
    sidecar config.  The Pydantic discriminated-union resolution calls each
    sidecar class' validators, so the injection is rejected before the payload
    can be persisted.
    """

    def test_bad_external_id_field_rejected_via_driver_config(self) -> None:
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        with pytest.raises(ValidationError):
            ItemsPostgresqlDriverConfig(
                sidecars=[
                    {
                        "sidecar_type": "attributes",
                        "external_id_field": "bad'; DROP TABLE items; --",
                    }
                ]
            )

    def test_bad_geom_column_rejected_via_driver_config(self) -> None:
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        with pytest.raises(ValidationError):
            ItemsPostgresqlDriverConfig(
                sidecars=[
                    {
                        "sidecar_type": "geometries",
                        "geom_column": "bad'; DROP TABLE items; --",
                    }
                ]
            )

    def test_bad_envelope_column_rejected_via_driver_config(self) -> None:
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        with pytest.raises(ValidationError):
            ItemsPostgresqlDriverConfig(
                sidecars=[
                    {
                        "sidecar_type": "access_envelope",
                        "column_name": "bad'; DROP TABLE items; --",
                    }
                ]
            )
