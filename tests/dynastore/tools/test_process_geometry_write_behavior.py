"""Regression coverage for #941 — `process_geometry` reads write-time
behaviour off :class:`GeometriesWriteBehavior` (on
``ItemsWritePolicy.geometries``), not off the sidecar config.

Pins:
- Behaviour defaults match the prior sidecar defaults so collections
  upgrading from pre-#941 see no semantic shift.
- ``invalid_geom_policy=REJECT`` on the behaviour block raises even
  when the sidecar config carries no such field anymore.
- ``simplification_algorithm`` + ``simplification_tolerance`` on the
  behaviour block actually simplify; absence is a no-op.
"""
from __future__ import annotations

import pytest
from shapely import wkb as _wkb
from shapely.geometry import Polygon

from dynastore.modules.storage.driver_config import (
    GeometriesWriteBehavior,
    ItemsWritePolicy,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    InvalidGeometryPolicy,
    SimplificationAlgorithm,
    TargetDimension,
)
from dynastore.tools.geospatial import process_geometry


def _wkb_hex(geom) -> str:
    return geom.wkb.hex()


def _sidecar() -> GeometriesSidecarConfig:
    return GeometriesSidecarConfig(
        sidecar_type="geometries",
        target_srid=4326,
        target_dimension=TargetDimension.FORCE_2D,
    )


class TestSidecarConfigHasNoBehaviorFields:
    """The 6 behaviour knobs MUST live on `GeometriesWriteBehavior`,
    not on `GeometriesSidecarConfig` — sidecar config is storage-shape
    only."""

    @pytest.mark.parametrize(
        "field",
        [
            "invalid_geom_policy",
            "srid_mismatch_policy",
            "allowed_geometry_types",
            "simplification_algorithm",
            "simplification_tolerance",
            "remove_redundant_vertices",
        ],
    )
    def test_field_removed_from_sidecar(self, field):
        assert field not in GeometriesSidecarConfig.model_fields, (
            f"{field} still on GeometriesSidecarConfig — should live on "
            f"GeometriesWriteBehavior under ItemsWritePolicy.geometries"
        )

    @pytest.mark.parametrize(
        "field",
        [
            "invalid_geom_policy",
            "srid_mismatch_policy",
            "allowed_geometry_types",
            "simplification_algorithm",
            "simplification_tolerance",
            "remove_redundant_vertices",
        ],
    )
    def test_field_present_on_behavior(self, field):
        assert field in GeometriesWriteBehavior.model_fields

    def test_items_write_policy_exposes_geometries_block(self):
        policy = ItemsWritePolicy()
        assert isinstance(policy.geometries, GeometriesWriteBehavior)


class TestProcessGeometryUsesBehavior:
    def test_default_behavior_preserves_valid_geometry(self):
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        out = process_geometry(_wkb_hex(poly), _sidecar(), source_srid=4326)
        assert out["wkb_hex_processed"] is not None
        roundtrip = _wkb.loads(bytes.fromhex(out["wkb_hex_processed"]))
        assert roundtrip.is_valid

    def test_reject_invalid_geometry_via_behavior(self):
        # bowtie self-intersection — invalid
        bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1)])
        behavior = GeometriesWriteBehavior(
            invalid_geom_policy=InvalidGeometryPolicy.REJECT,
        )
        with pytest.raises(Exception) as exc_info:
            process_geometry(
                _wkb_hex(bowtie),
                _sidecar(),
                source_srid=4326,
                write_behavior=behavior,
            )
        # InvalidGeometryError from tools.geospatial
        assert "invalid" in str(exc_info.value).lower() or "policy is reject" in str(exc_info.value).lower()

    def test_simplification_via_behavior_reduces_vertex_count(self):
        # high-vertex polygon — many redundant points along straight edges
        coords = [(x / 100, 0) for x in range(101)]  # 101 collinear points
        coords += [(1, 1), (0, 1)]
        poly = Polygon(coords)
        behavior = GeometriesWriteBehavior(
            simplification_algorithm=SimplificationAlgorithm.DOUGLAS_PEUCKER,
            simplification_tolerance=0.5,
        )
        out = process_geometry(
            _wkb_hex(poly),
            _sidecar(),
            source_srid=4326,
            write_behavior=behavior,
        )
        result = _wkb.loads(bytes.fromhex(out["wkb_hex_processed"]))
        # Douglas-Peucker at 0.5 tolerance on these inputs collapses
        # the 100 collinear bottom-edge points to 2.
        assert len(result.exterior.coords) < len(poly.exterior.coords)

    def test_allowed_types_rejects_disallowed(self):
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        behavior = GeometriesWriteBehavior(
            allowed_geometry_types=["Point"],
        )
        with pytest.raises(Exception) as exc_info:
            process_geometry(
                _wkb_hex(poly),
                _sidecar(),
                source_srid=4326,
                write_behavior=behavior,
            )
        assert "not allowed" in str(exc_info.value).lower()

    def test_no_write_behavior_uses_documented_defaults(self):
        """When the sidecar receives no policy on context (legacy/test
        paths), the function must still fall back to behaviour defaults
        (TRANSFORM/ATTEMPT_FIX) — i.e. zero behaviour change from the
        old sidecar defaults."""
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        out_explicit = process_geometry(
            _wkb_hex(poly),
            _sidecar(),
            source_srid=4326,
            write_behavior=GeometriesWriteBehavior(),
        )
        out_default = process_geometry(
            _wkb_hex(poly),
            _sidecar(),
            source_srid=4326,
            write_behavior=None,
        )
        assert out_explicit["wkb_hex_processed"] == out_default["wkb_hex_processed"]
