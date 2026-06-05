"""Read-policy gate: geometry ``stats`` + the pagination total must never leak
onto the Feature root as foreign members.

A COLUMNAR collection projects geometry statistics (``area`` / ``perimeter`` /
``length`` …) and the pagination total (``COUNT(*) OVER() AS _total_count``) as
plain row columns. The attributes sidecar republishes the *entire* raw row into
the pipeline context for inter-sidecar use, while a STRICT attribute schema keeps
those non-attribute columns out of ``feature.properties``. Before this fix the
``map_row_to_feature`` context bridge then emitted them as top-level GeoJSON
foreign members beside ``properties`` — e.g.::

    {"type":"Feature","properties":{"CODE":"IT",...},
     "area":2.2e10,"perimeter":1.2e6,"length":1.2e6,"_total_count":20,...}

— regardless of the read policy, which never requested them. These tests pin the
gate: derived/computed values are governed solely by the read policy (exposed via
``feature_type.expose`` / folded into the gated ``stats``/``system`` sections) and
``_total_count`` is pure pagination transport — neither may surface implicitly.
"""

from unittest.mock import MagicMock

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)


def _svc() -> ItemService:
    return ItemService(engine=MagicMock())


def _col_config() -> ItemsPostgresqlDriverConfig:
    """COLUMNAR collection: geometry sidecar producing area/perimeter/length +
    an attributes sidecar with a STRICT schema of user attributes only.

    This is the production shape behind the leak: the geometry stats and the
    user attributes share one wide row, but only the schema attributes belong
    in ``properties``.
    """
    geom_fields = [
        ComputedField(kind=k, storage_mode=StatisticStorageMode.COLUMNAR)
        for k in (ComputedKind.AREA, ComputedKind.PERIMETER, ComputedKind.LENGTH)
    ]
    geom = GeometriesSidecarConfig(
        sidecar_type="geometries",
        target_srid=4326,
        target_dimension=TargetDimension.FORCE_2D,
        partition_strategy=None,
        partition_resolution=0,
        statistics=None,
        compute_fields_overlay=geom_fields,
    )
    attrs = FeatureAttributeSidecarConfig(
        sidecar_type="attributes",
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[
            AttributeSchemaEntry(name="CODE"),
            AttributeSchemaEntry(name="NAME"),
        ],
    )
    return ItemsPostgresqlDriverConfig(sidecars=[attrs, geom])


def _row() -> dict:
    # A wide read row: user attributes (schema), geometry stats (computed),
    # and the pagination total — exactly as the COLUMNAR optimizer streams it.
    return {
        "geoid": "g-1",
        "external_id": "ext-1",
        "CODE": "IT",
        "NAME": "Italy",
        "area": 22160448264.83,
        "perimeter": 1211527.55,
        "length": 1211527.55,
        "_total_count": 20,
    }


def test_geometry_stats_do_not_leak_as_foreign_members() -> None:
    """area/perimeter/length stay out of ``properties`` AND off the Feature root
    when the read policy did not request them."""
    feat = _svc().map_row_to_feature(_row(), _col_config())

    # User attributes only — the stats are NOT folded into properties.
    assert feat.properties == {"CODE": "IT", "NAME": "Italy"}
    # No top-level foreign-member leak.
    extra = feat.model_extra or {}
    for stat in ("area", "perimeter", "length"):
        assert stat not in extra

    dumped = feat.model_dump(exclude_none=True)
    assert set(dumped) == {"id", "type", "properties"}


def test_total_count_pagination_column_never_surfaces() -> None:
    """``_total_count`` is pagination transport: it rides every row but must
    never reach the wire (neither ``properties`` nor a foreign member)."""
    feat = _svc().map_row_to_feature(_row(), _col_config())

    assert "_total_count" not in (feat.properties or {})
    assert "_total_count" not in (feat.model_extra or {})
    assert "_total_count" not in feat.model_dump(exclude_none=True)


def test_explicitly_requested_stat_surfaces_in_gated_section() -> None:
    """The gate is policy-driven, not a blanket suppression: a computed value
    named explicitly in the request surfaces — but in the gated ``stats``
    section (#1827 partial mode), never as a bare flat foreign member."""
    ctx = FeaturePipelineContext(lang="en", requested_fields={"area"})
    feat = _svc().map_row_to_feature(_row(), _col_config(), context=ctx)

    extra = feat.model_extra or {}
    assert extra.get("stats", {}).get("area") == 22160448264.83
    # Still not flat on the root, and properties stays user-only.
    assert "area" not in {k for k in extra if k != "stats"}
    assert feat.properties == {"CODE": "IT", "NAME": "Italy"}
