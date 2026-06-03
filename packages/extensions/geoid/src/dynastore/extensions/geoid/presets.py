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

"""``geoid`` routing preset (#847) — flagship FAO GeoID profile.

Composes the ``private_catalog`` bundle (PG-first storage, per-tenant
private ES indexers on the items tier, IAM DENY on the catalog's routes)
and opts the catalog in to a single anonymous audience:

  * ``CatalogLookupAudience.is_public=True`` — opens the anonymous
    lookup-only needle endpoint ``POST /search/catalogs/{cat}/geoid-search``
    (resolve one item by exact ``geoid`` / ``external_id``, gated by the
    ``lookup_only_search`` handler so a broadening request never matches),
    and arms the geoid extension's anonymous STAC/Features enumeration DENY
    so the catalog is reachable only by exact lookup — never by browsing,
    listing, or filterable search.

``on_applied`` installs the per-catalog IAM policies in the catalog's
tenant schema (see ``catalog_policies.register_geoid_policies_for_catalog``):
the lookup ALLOW, the STAC + Features enumeration DENYs, and a dormant
per-collection anonymous-create ALLOW.

The bundle also carries a catalog-tier ``ItemsWritePolicy`` (see
``_build_registry_write_policy``) so the catalog's collections inherit
GeoID **registry** write semantics: geometry is the identity. An identical
geometry returns the geoid it is already registered under; a duplicate id is
refused (409); input is reprojected to 4326; any geometry type is accepted;
invalid geometry is rejected with a reason rather than silently repaired; and
geohash + S2 spatial cells plus geometry statistics are materialised for
lookup.

Anonymous *writes* are off by default: no collection opts in, so the catalog
refuses anonymous inserts (un-fao/GeoID#1204 — public users must not insert
by default). A catalog admin turns a single intake collection into a "blind
dropbox" by setting ``CollectionWriteAudience.allow_anonymous_create=True``
on it (PUT ``/configs/catalogs/{cat}/collections/{col}/plugins/collection_write_audience``).
For that collection only, anonymous ``POST .../items`` is then allowed: the
create ALLOW carries a higher priority than the enumeration DENY, so it wins
the #915 ranking (highest priority wins; DENY only wins on a tie) — while
every browse / list / search verb stays denied because the ALLOW is
POST-only. The two halves compose on one catalog: contribute an item and
resolve it later by a known ``geoid``, but never enumerate what is there.

Auto-registers on extension import.
"""
from __future__ import annotations

from dynastore.modules.iam.audience_configs import CatalogLookupAudience
from typing import ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import ItemsWritePolicy

from dynastore.modules.storage.presets import BundlePreset, get_preset, register_preset
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetBundleEntry, PresetTier


def _build_registry_write_policy() -> "ItemsWritePolicy":
    """Registry write semantics for a GeoID catalog's collections.

    Geometry is the registry's identity. The ``identity`` chain is evaluated
    top-down, first match wins:

      1. ``geometry_hash`` → ``REFUSE_RETURN`` — an incoming feature whose
         byte-exact geometry (SHA256 of ``ST_AsBinary(geom)``, taken after
         the 4326 reproject) already exists short-circuits the write and
         echoes the stored record, so the caller gets back the geoid the
         shape is already registered under. Idempotent: re-submitting the
         same shape — even under a different id — never mints a second geoid.
      2. ``external_id`` → ``REFUSE_FAIL`` — a *new* geometry submitted under
         an id that is already a geoid raises ``ConflictError`` (HTTP 409):
         a geoid is never registered twice.

    This deliberately inverts the platform default (external_id first): for a
    registry, collapsing identical geometries to one geoid regardless of the
    submitted id is the whole point.

    Geometry handling: every input is reprojected to EPSG:4326 (the geometries
    sidecar's default ``target_srid`` + the default TRANSFORM policy); any
    geometry type is accepted (``allowed_geometry_types=[]``); invalid geometry
    is REJECTed rather than silently repaired — the rejection message names the
    defect (``explain_validity``).

    ``content_hashes=["geometry"]`` arms the ``geometry_hash`` derivation the
    first rule references. ``spatial_cells`` materialise indexed geohash + S2
    keys for spatial lookup; ``geometry_stats`` precompute area / perimeter /
    centroid / bbox / vertex-count into a shared JSONB stats column (no
    per-stat column, no attribute index — the registry is queried by geoid and
    geohash, never by attribute).

    Set at the CATALOG tier by the preset; collections created later inherit
    it through the config waterfall unless they carry their own policy.
    """
    from dynastore.modules.storage.driver_config import (
        GeometriesWriteBehavior,
        ItemsWritePolicy,
        WriteConflictPolicy,
    )
    from dynastore.modules.storage.computed_fields import (
        ComputedKind,
        DeriveSpec,
        GeometryStat,
        IdentityRule,
        SpatialCell,
        StatisticStorageMode,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        InvalidGeometryPolicy,
    )

    _jsonb = StatisticStorageMode.JSONB
    return ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.REFUSE_RETURN,
        geometries=GeometriesWriteBehavior(
            invalid_geom_policy=InvalidGeometryPolicy.REJECT,
        ),
        derive=DeriveSpec(
            content_hashes=["geometry"],
            spatial_cells=[
                SpatialCell(grid="geohash", resolution=12),
                SpatialCell(grid="s2", resolution=12),
            ],
            geometry_stats=[
                GeometryStat(stat=ComputedKind.AREA, store=_jsonb),
                GeometryStat(stat=ComputedKind.PERIMETER, store=_jsonb),
                GeometryStat(stat=ComputedKind.CENTROID, store=_jsonb),
                GeometryStat(stat=ComputedKind.BBOX, store=_jsonb),
                GeometryStat(stat=ComputedKind.VERTEX_COUNT, store=_jsonb),
            ],
        ),
        identity=[
            IdentityRule(
                match_on=["geometry_hash"],
                on_match=WriteConflictPolicy.REFUSE_RETURN,
            ),
            IdentityRule(
                match_on=["external_id"],
                on_match=WriteConflictPolicy.REFUSE_FAIL,
            ),
        ],
    )


class GeoidPreset(BundlePreset):
    """Flagship FAO GeoID profile: private storage + anonymous lookup-only."""

    name = "geoid"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    description = (
        "FAO GeoID flagship profile. Composes the private_catalog bundle "
        "(PG-first storage + per-tenant private ES indexers, IAM DENY on "
        "default routes) with one anonymous opt-in: catalog-level "
        "lookup_audience.is_public=True. That arms lookup-only mode — "
        "anonymous callers can resolve a single record by geoid or "
        "external_id but cannot enumerate. Anonymous insert stays off unless "
        "a catalog admin opts a specific collection in via "
        "CollectionWriteAudience.allow_anonymous_create (per-collection "
        "blind-dropbox intake). Use for catalogs that serve anonymous lookup "
        "— and optionally anonymous contribution to chosen collections — "
        "while keeping the read/index/browse surface private. "
        "Collections also inherit registry write semantics (ItemsWritePolicy): "
        "an identical geometry returns the geoid it is already registered "
        "under (geometry_hash → REFUSE_RETURN), a duplicate id is refused with "
        "409 (external_id → REFUSE_FAIL), geometry is reprojected to 4326, any "
        "type is accepted, invalid geometry is rejected (not repaired) with a "
        "reason, and geohash + S2 cells plus geometry stats are materialised."
    )

    def build(self, catalog_id: str, **_scope: str) -> PresetBundle:
        from dynastore.modules.storage.driver_config import ItemsWritePolicy

        base = get_preset("private_catalog").build(catalog_id=catalog_id)
        return PresetBundle(
            entries=(
                *base.entries,
                PresetBundleEntry(
                    slot="audience:catalog_lookup_audience",
                    config_cls=CatalogLookupAudience,
                    instance=CatalogLookupAudience(is_public=True),
                    rollback_priority=100,
                ),
                # Registry write semantics, applied at the catalog tier so the
                # catalog's collections inherit it (geometry_hash → return the
                # existing geoid; duplicate external_id → 409; reproject 4326;
                # reject-not-fix invalid geometry; geohash + S2 cells; geometry
                # stats). See ``_build_registry_write_policy``.
                PresetBundleEntry(
                    slot="items_write_policy",
                    config_cls=ItemsWritePolicy,
                    instance=_build_registry_write_policy(),
                    rollback_priority=100,
                ),
            )
        )

    async def on_applied(self, catalog_id: str, **_scope: str) -> None:
        """Called after the preset bundle is applied to a catalog.
        
        Registers per-catalog geoid IAM policies for the lookup-only access model.
        """
        from .catalog_policies import register_geoid_policies_for_catalog
        
        await register_geoid_policies_for_catalog(catalog_id)


register_preset(GeoidPreset())
