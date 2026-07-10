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

"""``vector_ingest_demo`` preset — parameterizable vector ingestion (PLATFORM tier).

Applies at platform scope (``POST /configs/presets/vector_ingest_demo``):

  1. Creates the target catalog and collection (skip-if-exists). The
     collection is configured with PG-primary + Elasticsearch-secondary
     routing so items ingested by the background task appear in STAC search.

  2. Submits an async ``ingestion`` job that reads the source vector file
     and upserts every feature into the collection. Re-running the preset
     against the SAME source converges instead of duplicating: pass
     ``id_field`` to key identity on a stable source column/property (e.g.
     GAUL's ``GAUL1_CODE``); without it, identity falls back to the
     source's own OGR feature id (stable across re-reads of an unmodified
     file) and finally to a content hash of the feature (see #2709).

**Default behavior (no body / empty body)**:

  Reads the Natural Earth 110m admin-0 country polygons GeoJSON (~180
  features, ~200 KB) from the nvkelso/natural-earth-vector GitHub mirror
  into ``demo_vector_catalog / demo_vector_collection``.

**Parameterized behavior**:

  POST a JSON body with any subset of these fields to ingest an arbitrary
  vector file into an arbitrary catalog/collection::

      {
        "source_uri":    "gs://my-bucket/data/regions.gpkg",
        "catalog_id":    "my_catalog",
        "collection_id": "my_collection",
        "source_format": "application/geopackage+sqlite3"
      }

  All fields are optional. An absent field falls back to the Natural Earth
  default. The ``source_format`` hint is only required when the URI
  extension is ambiguous; the reader infers format from ``.gpkg``,
  ``.geojson``, ``.parquet``, ``.zip`` etc. automatically.

All source columns are mapped automatically (``attributes_source_type=all``).

Revoke: removes items, collection, and catalog in order (collection and
catalog are left if they still hold other data not introduced by this preset).
"""
from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple, Type

from pydantic import BaseModel, Field

from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)

from .preset import (
    AppliedDescriptor,
    DataSeed,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
    TaskSeed,
)
from .protocol import PresetTier

_CATALOG_ID = "demo_vector_catalog"
_COLLECTION_ID = "demo_vector_collection"

_CATALOG_DATA: Dict[str, Any] = {
    "id": _CATALOG_ID,
    "title": {"en": "Vector Ingestion Demo"},
    "description": {
        "en": (
            "Demo catalog populated by the vector_ingest_demo preset. "
            "Contains country polygon features ingested from Natural Earth."
        ),
    },
    "keywords": ["demo", "vector", "ingestion", "platform"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA: Dict[str, Any] = {
    "id": _COLLECTION_ID,
    "title": {"en": "Natural Earth Countries (110m)"},
    "description": {
        "en": (
            "Country polygon features at 1:110m scale sourced from Natural Earth "
            "and ingested via the vector_ingest_demo preset."
        ),
    },
}

# Natural Earth 110m admin-0 countries — ~180 polygon features, ~200 KB.
# Hosted on GitHub raw CDN; reliable and publicly accessible without auth.
_NE_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_110m_admin_0_countries.geojson"
)


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------

class VectorIngestParams(BaseModel):
    """Optional overrides for the ``vector_ingest_demo`` preset.

    All fields default to ``None``; an absent field falls back to the
    preset's Natural Earth defaults. Defaults reproduce the original
    param-less behavior exactly.

    Minimal call to ingest a GeoPackage from GCS into a custom target::

        POST /configs/presets/vector_ingest_demo
        {"source_uri": "gs://my-bucket/data/roads.gpkg",
         "catalog_id": "roads_catalog", "collection_id": "roads"}
    """

    model_config = {"extra": "ignore"}

    source_uri: Optional[str] = Field(
        default=None,
        description=(
            "URI of the vector source file to ingest. Accepts gs:// and "
            "https:// URIs. The format is inferred from the URI extension "
            "(.gpkg, .geojson, .parquet, .zip for Shapefiles, …). "
            "When absent the Natural Earth 110m countries GeoJSON is used."
        ),
        examples=[
            "gs://fao-aip-geospatial-review-data/demo_data/ph4_sc7ao_network_smooth.gpkg",
            "https://example.com/data/admin_boundaries.geojson",
        ],
    )
    catalog_id: Optional[str] = Field(
        default=None,
        description=(
            "Target catalog identifier. The catalog is created if it does not "
            "already exist. Defaults to demo_vector_catalog."
        ),
        examples=["my_catalog", "roads_catalog"],
    )
    collection_id: Optional[str] = Field(
        default=None,
        description=(
            "Target collection identifier within the catalog. The collection is "
            "created if it does not already exist. Defaults to "
            "demo_vector_collection."
        ),
        examples=["my_collection", "roads_network"],
    )
    source_format: Optional[str] = Field(
        default=None,
        description=(
            "Optional MIME content-type hint passed as asset.metadata['content_type'] "
            "to help the ingestion reader when the URI extension is ambiguous. "
            "When absent the reader infers format from the URI extension alone. "
            "For .gpkg files this field is never required."
        ),
        examples=["application/geopackage+sqlite3", "application/geo+json"],
    )
    id_field: Optional[str] = Field(
        default=None,
        description=(
            "Optional source column/property that uniquely identifies each "
            "feature (e.g. 'GAUL1_CODE' for GAUL administrative boundaries). "
            "Forwarded as ingestion_request.column_mapping.external_id. "
            "Re-running the preset against the SAME source converges on this "
            "field instead of appending duplicate rows (#2709). When absent, "
            "identity falls back to the source's own OGR feature id (FID) "
            "and finally to a content hash of the feature — both are still "
            "deterministic across re-runs of an unmodified source file, so "
            "an id_field is recommended but not required for idempotent "
            "re-ingestion."
        ),
        examples=["GAUL1_CODE", "ADM2_PCODE"],
    )
    preseed_tiles: bool = Field(
        default=True,
        description=(
            "When True (default) a tile preseed is chained after the ingestion "
            "job commits, so the collection's tiles are cached and served fast. "
            "Set False to skip preseeding — tiles still render on-the-fly from "
            "PostGIS on demand. Tiles cache in the catalog's own bucket, or in "
            "the bucket resolved from cache_bucket (see below)."
        ),
    )
    tile_format: str = Field(
        default="mvt",
        description=(
            "Output format for the post-ingestion tile preseed: 'mvt' stores "
            "individual vector tiles, 'pmtiles' builds a single archive. Ignored "
            "when preseed_tiles is False."
        ),
        examples=["mvt", "pmtiles"],
    )
    cache_bucket: Optional[str] = Field(
        default=None,
        description=(
            "Optional destination GCS bucket for the preseeded tile cache, pinned "
            "to this catalog (written as a catalog-scoped GcpTileCacheConfig). "
            "When None (default) and the source is a gs:// URI, the cache defaults "
            "to the source file's OWN bucket, in a folder named after the source "
            "file alongside it — e.g. gs://bucket/dir/file.gpkg caches at "
            "gs://bucket/dir/file/{catalog_id}/{collection}/{tms}/{z}/{x}/{y}."
            "{format} (the catalog_id keeps two catalogs fed the same file "
            "isolated). When "
            "None and the source is not gs:// (e.g. an https download), tiles "
            "cache in the catalog's own provisioned bucket. The bucket must "
            "already exist and be writable by the service account (geoid does not "
            "provision it). Has no effect when preseed_tiles is False or the GCP "
            "module is not installed (on-prem caches on local disk)."
        ),
        examples=["fao-aip-geospatial-review-data", "my-tile-cache-bucket"],
    )


# ---------------------------------------------------------------------------
# Items routing (PG-primary + Elasticsearch-secondary)
# ---------------------------------------------------------------------------

def _vector_items_routing() -> ItemsRoutingConfig:
    """PG-primary + Elasticsearch-INDEX routing for the demo collection.

    Items written by the ingestion task land in Postgres first; the async
    INDEX-lane fan-out indexes them in Elasticsearch so STAC search returns
    them. Mirrors the demo_data routing pattern so the preset is immediately
    searchable after the ingestion job completes.
    """
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


# ---------------------------------------------------------------------------
# Per-request contributor — constructed fresh with resolved params
# ---------------------------------------------------------------------------

@dataclass
class _VectorIngestDemoContributor:
    """Data + task contributor for one vector_ingest_demo apply call.

    Constructed per-call with the resolved parameter values so the preset
    singleton remains stateless and safe under concurrent requests.
    """

    catalog_id: str
    collection_id: str
    source_uri: str
    source_format: Optional[str]
    id_field: Optional[str] = None
    preseed_tiles: bool = True
    tile_format: str = "mvt"
    cache_bucket: Optional[str] = None

    def _resolve_cache_location(self) -> "Tuple[Optional[str], Optional[str]]":
        """Resolve ``(cache_bucket, cache_prefix)`` for the tile cache, or
        ``(None, None)`` to leave tiles in the catalog's own managed bucket.

        Precedence:

        1. Explicit ``cache_bucket`` param → that bucket, default prefix (None →
           storage folds ``{key_prefix}/{catalog_id}`` for isolation).
        2. No param + ``gs://`` source → the source file's OWN bucket, with a
           prefix that is the source path minus its extension (a folder named
           after the source file in the same folder) PLUS a ``catalog_id``
           segment, so two catalogs fed the same source file stay isolated and
           dropping one never wipes the other's tiles
           (``gs://bkt/dir/file.gpkg`` → ``("bkt", "dir/file/<catalog_id>")``).
        3. Otherwise (https / non-gs source) → ``(None, None)`` (managed bucket).
        """
        if self.cache_bucket:
            return self.cache_bucket, None
        if self.source_uri.startswith("gs://"):
            rest = self.source_uri[len("gs://"):]
            bucket, _, path = rest.partition("/")
            if bucket and path:
                stem = posixpath.splitext(path)[0].strip("/")
                if stem:
                    return bucket, f"{stem}/{self.catalog_id}"
        return None, None

    def get_configs(self) -> "List[Any]":
        """Emit a catalog-scoped ``GcpTileCacheConfig`` pinning this catalog's
        tile cache to the resolved ``(bucket, prefix)``.

        Returns an empty list when there is nothing to pin — preseeding off, the
        cache stays in the managed bucket, or the GCP module is not installed
        (on-prem caches on local disk via a different backend). Importing the GCP
        config lazily keeps this preset usable in non-GCP deployments.
        """
        if not self.preseed_tiles:
            return []
        bucket, prefix = self._resolve_cache_location()
        if not bucket:
            return []
        try:
            from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig
        except Exception:
            return []
        return [GcpTileCacheConfig(cache_bucket=bucket, cache_prefix=prefix)]

    def get_data(self) -> Iterable[DataSeed]:
        # Build catalog/collection payloads with the actual (possibly overridden)
        # IDs so _apply_data_kind creates the right resources.
        catalog_data = {**_CATALOG_DATA, "id": self.catalog_id}
        collection_data = {**_COLLECTION_DATA, "id": self.collection_id}
        yield DataSeed(
            catalog_id=self.catalog_id,
            collection_id=self.collection_id,
            catalog_data=catalog_data,
            collection_data=collection_data,
            items=(),
            manage_catalog=True,
            manage_collection=True,
            items_routing=_vector_items_routing(),
            # Vector ingestion is PG-primary (features land in Postgres + an ES
            # secondary index, never as GCS asset bytes), so a provisioned asset
            # bucket would be dead weight. Create the catalog bucket-free: defer
            # holds back the deferrable GCP storage provisioners so the catalog
            # reaches ``ready`` core-only. Scoped to this preset — the platform
            # default (always provision a bucket) is unchanged.
            #
            # Tiles preseeded after ingestion do NOT require this catalog to own
            # a bucket: if the operator points the GCS tile cache at an external
            # bucket (``GcpTileCacheConfig.cache_bucket``) all tile I/O targets
            # that bucket, keyed by catalog_id; otherwise the catalog's own
            # bucket is provisioned lazily on first tile write.
            defer_provisioning=True,
        )

    def get_tasks(self) -> Iterable[TaskSeed]:
        # Build the ingestion_request asset dict; include content_type hint
        # only when source_format was explicitly supplied.
        asset: Dict[str, Any] = {"uri": self.source_uri}
        if self.source_format:
            asset["metadata"] = {"content_type": self.source_format}

        # The dedup_key incorporates URI + target so concurrent applies to
        # different collections are not collapsed onto the same job.
        dedup_key = (
            f"preset:vector_ingest_demo:"
            f"{self.catalog_id}:{self.collection_id}:{self.source_uri}"
        )

        column_mapping: Dict[str, Any] = {"attributes_source_type": "all"}
        if self.id_field:
            # Deterministic per-source identity (#2709): keys the upsert on
            # this stable field so re-applying the preset against the same
            # source converges instead of appending duplicate features.
            column_mapping["external_id"] = self.id_field

        inputs: Dict[str, Any] = {
            "catalog_id": self.catalog_id,
            "collection_id": self.collection_id,
            "ingestion_request": {
                "asset": asset,
                "column_mapping": column_mapping,
            },
        }
        if self.preseed_tiles:
            # Chain a tile preseed once ingestion succeeds. The ingestion task
            # enqueues a ``tiles_preseed`` job for this collection only after the
            # features are committed, so the render has real data (a preseed
            # submitted up-front would race the async ingestion and tile an empty
            # collection). Where the tiles land is governed by the catalog-scoped
            # GcpTileCacheConfig this preset writes (see get_configs): the
            # catalog's own bucket by default, an explicit cache_bucket, or — for
            # a gs:// source with no override — a folder named after the source
            # file alongside it.
            inputs["preseed_on_success"] = {"output_format": self.tile_format}

        yield TaskSeed(
            process_id="ingestion",
            inputs=inputs,
            async_mode=True,
            dedup_key=dedup_key,
        )


# ---------------------------------------------------------------------------
# Preset class — stateless, safe under concurrency
# ---------------------------------------------------------------------------

class _VectorIngestDemoPreset:
    """Parameterizable vector ingestion demo preset.

    Implements the Preset protocol without subclassing MultiContributorPreset
    so that per-request params can be threaded to the contributor without
    mutating shared singleton state.
    """

    name: ClassVar[str] = "vector_ingest_demo"
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = VectorIngestParams
    keywords: ClassVar[Tuple[str, ...]] = (
        "demo", "data", "platform", "vector", "ingestion",
    )
    description: ClassVar[str] = (
        "Create the target catalog/collection and submit an async ingestion job "
        "that reads a vector source file and upserts every feature — re-applying "
        "the preset against the same source converges instead of duplicating "
        "features (#2709). Without params ingests the Natural Earth 110m country "
        "polygons GeoJSON into demo_vector_catalog/demo_vector_collection. Pass "
        "source_uri, catalog_id, and/or collection_id to ingest any gs:// or "
        "https:// vector file into a custom target. Pass id_field to key identity "
        "on a stable source column (e.g. 'GAUL1_CODE'); without it identity falls "
        "back to the source's own OGR feature id, then a content hash of the "
        "feature. Items are indexed in Elasticsearch via PG-primary + async-"
        "secondary routing so STAC search returns them after the job completes."
    )

    def _resolve(self, params: BaseModel) -> _VectorIngestDemoContributor:
        """Coerce params and resolve defaults — returns a per-call contributor."""
        p = (
            params
            if isinstance(params, VectorIngestParams)
            else VectorIngestParams.model_validate(params.model_dump())
        )
        return _VectorIngestDemoContributor(
            catalog_id=p.catalog_id or _CATALOG_ID,
            collection_id=p.collection_id or _COLLECTION_ID,
            source_uri=p.source_uri or _NE_COUNTRIES_URL,
            source_format=p.source_format,
            id_field=p.id_field,
            preseed_tiles=p.preseed_tiles,
            tile_format=p.tile_format,
            cache_bucket=p.cache_bucket,
        )

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        c = self._resolve(params)
        entries: list[PresetPlanEntry] = []
        for seed in c.get_data():
            entries.append(PresetPlanEntry(
                kind="seed_data",
                target=f"{seed.catalog_id}/{seed.collection_id}",
                detail={"items": len(seed.items)},
            ))
        for tseed in c.get_tasks():
            entries.append(PresetPlanEntry(
                kind="trigger_task",
                target=tseed.process_id,
                detail={"async": tseed.async_mode, "inputs": dict(tseed.inputs)},
            ))
        for cfg in c.get_configs():
            cfg_cls = type(cfg)
            entries.append(PresetPlanEntry(
                kind="set_config",
                target=f"{cfg_cls.__qualname__}@catalog:{c.catalog_id}",
                detail=cfg.model_dump(mode="json"),
            ))
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(entries),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        from .multi_contributor import _apply_data_kind, _apply_task_kind

        c = self._resolve(params)
        applied_data: list[dict] = []
        applied_tasks: list[dict] = []
        await _apply_data_kind(self.name, c, ctx, applied_data)
        await _apply_task_kind(self.name, c, ctx, applied_tasks)

        # Pin the tile cache to the resolved bucket/prefix as a CATALOG-scoped
        # config (not the preset's platform scope), so each ingested catalog
        # keeps its own cache target and the preset is safely reusable for many
        # catalogs. Emits nothing when the cache stays in the managed bucket or
        # GCP is absent (see _VectorIngestDemoContributor.get_configs).
        config_qualnames: list[str] = []
        if ctx.config is not None:
            for cfg in c.get_configs():
                cfg_cls = type(cfg)
                await ctx.config.set_config(
                    config_cls=cfg_cls,
                    config=cfg,
                    catalog_id=c.catalog_id,
                    check_immutability=False,
                )
                config_qualnames.append(f"{cfg_cls.__module__}.{cfg_cls.__qualname__}")

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "policy_ids": [],
            "role_names": [],
            "config_qualnames": config_qualnames,
            "config_catalog_id": c.catalog_id if config_qualnames else None,
            "data": applied_data,
            "tasks": applied_tasks,
            "scope": scope,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        from .multi_contributor import _revoke_data_kind, _revoke_task_kind

        payload = applied_descriptor.payload

        # Drop the catalog-scoped tile cache config this preset wrote (if any),
        # before removing the catalog itself.
        cfg_catalog_id = payload.get("config_catalog_id")
        if cfg_catalog_id and ctx.config is not None:
            try:
                from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig
                await ctx.config.delete_config(
                    config_cls=GcpTileCacheConfig,
                    catalog_id=cfg_catalog_id,
                )
            except Exception:
                pass  # best-effort: GCP absent or already gone

        await _revoke_task_kind(self.name, payload.get("tasks", []))
        await _revoke_data_kind(self.name, ctx, payload.get("data", []))


VECTOR_INGEST_DEMO_PRESET = _VectorIngestDemoPreset()
