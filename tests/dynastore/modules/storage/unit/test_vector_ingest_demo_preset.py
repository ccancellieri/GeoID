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

"""Unit tests for the vector_ingest_demo preset parameterization.

Pure-Python tests — no DB, no FastAPI, no network. Suite covers:

- VectorIngestParams: defaults produce the Natural Earth / demo-catalog values.
- VectorIngestParams: overrides change source_uri / catalog_id / collection_id /
  source_format independently.
- _VectorIngestDemoContributor.get_data() yields a DataSeed with PG+ES routing.
- _VectorIngestDemoContributor.get_tasks() yields a TaskSeed whose
  ingestion_request.asset.uri matches the resolved URI.
- param-less contributor still uses Natural Earth URI (backward compat).
- source_format is forwarded as asset.metadata['content_type'].
- VECTOR_INGEST_DEMO_PRESET metadata: name, tier, params_model.
- Registry: preset registered as "vector_ingest_demo" at PLATFORM tier.
"""
from __future__ import annotations


from dynastore.modules.storage.presets.vector_ingest_demo import (
    VECTOR_INGEST_DEMO_PRESET,
    VectorIngestParams,
    _VectorIngestDemoContributor,
    _NE_COUNTRIES_URL,
    _CATALOG_ID,
    _COLLECTION_ID,
)
from dynastore.modules.storage.presets.preset import DataSeed, TaskSeed
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import find_preset


# ---------------------------------------------------------------------------
# VectorIngestParams — defaults
# ---------------------------------------------------------------------------

def test_params_all_none_by_default() -> None:
    p = VectorIngestParams()
    assert p.source_uri is None
    assert p.catalog_id is None
    assert p.collection_id is None
    assert p.source_format is None
    assert p.id_field is None


def test_params_extra_fields_ignored() -> None:
    """Extra keys in the JSON body are silently ignored (model_config extra=ignore)."""
    p = VectorIngestParams.model_validate({"source_uri": "gs://x/y.gpkg", "unknown_key": 99})
    assert p.source_uri == "gs://x/y.gpkg"
    assert not hasattr(p, "unknown_key")


def test_params_partial_override() -> None:
    """Only the provided fields are overridden; others stay None."""
    p = VectorIngestParams(catalog_id="my_catalog")
    assert p.catalog_id == "my_catalog"
    assert p.source_uri is None
    assert p.collection_id is None


# ---------------------------------------------------------------------------
# _VectorIngestDemoContributor — param-less (Natural Earth defaults)
# ---------------------------------------------------------------------------

def _default_contributor() -> _VectorIngestDemoContributor:
    """Contributor built with default params — mirrors what the preset does."""
    p = VectorIngestParams()
    return _VectorIngestDemoContributor(
        catalog_id=p.catalog_id or _CATALOG_ID,
        collection_id=p.collection_id or _COLLECTION_ID,
        source_uri=p.source_uri or _NE_COUNTRIES_URL,
        source_format=p.source_format,
    )


def test_default_contributor_catalog_id() -> None:
    c = _default_contributor()
    assert c.catalog_id == "demo_vector_catalog"


def test_default_contributor_collection_id() -> None:
    c = _default_contributor()
    assert c.collection_id == "demo_vector_collection"


def test_default_contributor_source_uri_is_natural_earth() -> None:
    c = _default_contributor()
    assert "nvkelso/natural-earth-vector" in c.source_uri
    assert c.source_uri.endswith(".geojson")


def test_default_contributor_source_format_is_none() -> None:
    c = _default_contributor()
    assert c.source_format is None


# ---------------------------------------------------------------------------
# _VectorIngestDemoContributor.get_data()
# ---------------------------------------------------------------------------

def test_get_data_yields_one_seed() -> None:
    seeds = list(_default_contributor().get_data())
    assert len(seeds) == 1


def test_get_data_seed_catalog_and_collection() -> None:
    seed: DataSeed = list(_default_contributor().get_data())[0]
    assert seed.catalog_id == "demo_vector_catalog"
    assert seed.collection_id == "demo_vector_collection"


def test_get_data_seed_items_empty() -> None:
    """No inline items — the ingestion task writes them asynchronously."""
    seed = list(_default_contributor().get_data())[0]
    assert seed.items == ()


def test_get_data_seed_manage_catalog_true() -> None:
    seed = list(_default_contributor().get_data())[0]
    assert seed.manage_catalog is True


def test_get_data_seed_manage_collection_true() -> None:
    seed = list(_default_contributor().get_data())[0]
    assert seed.manage_collection is True


def test_get_data_seed_declares_pg_primary_routing() -> None:
    from dynastore.modules.storage.routing_config import Operation

    seed = list(_default_contributor().get_data())[0]
    assert seed.items_routing is not None
    write_entries = seed.items_routing.operations[Operation.WRITE]
    pg = [e for e in write_entries if e.driver_ref == "items_postgresql_driver"]
    assert pg, "PG primary write entry must be present"


def test_get_data_seed_declares_es_index_routing() -> None:
    from dynastore.modules.storage.routing_config import Operation

    seed = list(_default_contributor().get_data())[0]
    assert seed.items_routing is not None
    index_entries = seed.items_routing.operations[Operation.INDEX]
    es = [e for e in index_entries if e.driver_ref == "items_elasticsearch_driver"]
    assert es, "ES INDEX-lane entry must be present"
    assert es[0].source == "auto"


def test_get_data_seed_catalog_data_id_matches_catalog_id() -> None:
    """catalog_data['id'] must match seed.catalog_id so create_catalog uses the right id."""
    seed = list(_default_contributor().get_data())[0]
    assert seed.catalog_data["id"] == seed.catalog_id


def test_get_data_seed_collection_data_id_matches_collection_id() -> None:
    seed = list(_default_contributor().get_data())[0]
    assert seed.collection_data["id"] == seed.collection_id


# ---------------------------------------------------------------------------
# _VectorIngestDemoContributor.get_tasks()
# ---------------------------------------------------------------------------

def test_get_tasks_yields_one_task() -> None:
    tasks = list(_default_contributor().get_tasks())
    assert len(tasks) == 1


def test_get_tasks_process_id_is_ingestion() -> None:
    tseed: TaskSeed = list(_default_contributor().get_tasks())[0]
    assert tseed.process_id == "ingestion"


def test_get_tasks_async_mode() -> None:
    tseed = list(_default_contributor().get_tasks())[0]
    assert tseed.async_mode is True


def test_get_tasks_inputs_catalog_and_collection() -> None:
    tseed = list(_default_contributor().get_tasks())[0]
    assert tseed.inputs["catalog_id"] == "demo_vector_catalog"
    assert tseed.inputs["collection_id"] == "demo_vector_collection"


def test_get_tasks_ingestion_request_asset_uri_default() -> None:
    """Default URI must point at the Natural Earth GeoJSON."""
    tseed = list(_default_contributor().get_tasks())[0]
    asset = tseed.inputs["ingestion_request"]["asset"]
    assert "nvkelso/natural-earth-vector" in asset["uri"]


def test_get_tasks_ingestion_request_column_mapping() -> None:
    tseed = list(_default_contributor().get_tasks())[0]
    mapping = tseed.inputs["ingestion_request"]["column_mapping"]
    assert mapping["attributes_source_type"] == "all"


def test_get_tasks_no_metadata_when_no_source_format() -> None:
    """Without source_format the asset dict must not include a metadata key."""
    tseed = list(_default_contributor().get_tasks())[0]
    asset = tseed.inputs["ingestion_request"]["asset"]
    assert "metadata" not in asset


def test_get_tasks_dedup_key_contains_catalog_collection_uri() -> None:
    c = _default_contributor()
    tseed = list(c.get_tasks())[0]
    assert tseed.dedup_key is not None
    assert c.catalog_id in tseed.dedup_key
    assert c.collection_id in tseed.dedup_key
    assert c.source_uri in tseed.dedup_key


# ---------------------------------------------------------------------------
# Parameterized overrides — source_uri
# ---------------------------------------------------------------------------

_GPKG_URI = "gs://fao-aip-geospatial-review-data/demo_data/ph4_sc7ao_network_smooth.gpkg"


def _custom_contributor(
    source_uri=_GPKG_URI,
    catalog_id="ph4_catalog",
    collection_id="ph4_network",
    source_format=None,
) -> _VectorIngestDemoContributor:
    return _VectorIngestDemoContributor(
        catalog_id=catalog_id,
        collection_id=collection_id,
        source_uri=source_uri,
        source_format=source_format,
    )


def test_custom_uri_overrides_asset_uri() -> None:
    tseed = list(_custom_contributor().get_tasks())[0]
    asset = tseed.inputs["ingestion_request"]["asset"]
    assert asset["uri"] == _GPKG_URI


def test_custom_catalog_id_propagates_to_inputs() -> None:
    tseed = list(_custom_contributor(catalog_id="ph4_catalog").get_tasks())[0]
    assert tseed.inputs["catalog_id"] == "ph4_catalog"


def test_custom_collection_id_propagates_to_inputs() -> None:
    tseed = list(_custom_contributor(collection_id="ph4_network").get_tasks())[0]
    assert tseed.inputs["collection_id"] == "ph4_network"


def test_custom_catalog_id_in_data_seed() -> None:
    seed = list(_custom_contributor(catalog_id="ph4_catalog").get_data())[0]
    assert seed.catalog_id == "ph4_catalog"
    assert seed.catalog_data["id"] == "ph4_catalog"


def test_custom_collection_id_in_data_seed() -> None:
    seed = list(_custom_contributor(collection_id="ph4_network").get_data())[0]
    assert seed.collection_id == "ph4_network"
    assert seed.collection_data["id"] == "ph4_network"


def test_source_format_forwarded_as_metadata() -> None:
    """source_format must appear as asset.metadata['content_type']."""
    c = _custom_contributor(source_format="application/geopackage+sqlite3")
    tseed = list(c.get_tasks())[0]
    asset = tseed.inputs["ingestion_request"]["asset"]
    assert asset.get("metadata", {}).get("content_type") == "application/geopackage+sqlite3"


def test_source_format_none_means_no_metadata() -> None:
    c = _custom_contributor(source_format=None)
    tseed = list(c.get_tasks())[0]
    asset = tseed.inputs["ingestion_request"]["asset"]
    assert "metadata" not in asset


# ---------------------------------------------------------------------------
# id_field — deterministic per-source identity (#2709)
# ---------------------------------------------------------------------------


def test_id_field_forwarded_as_column_mapping_external_id() -> None:
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
        id_field="GAUL1_CODE",
    )
    tseed = list(c.get_tasks())[0]
    mapping = tseed.inputs["ingestion_request"]["column_mapping"]
    assert mapping["external_id"] == "GAUL1_CODE"


def test_id_field_none_omits_external_id_from_mapping() -> None:
    """Without id_field the mapping still carries attributes_source_type but
    no external_id key — identity falls back to OGR FID / content hash."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
    )
    tseed = list(c.get_tasks())[0]
    mapping = tseed.inputs["ingestion_request"]["column_mapping"]
    assert "external_id" not in mapping
    assert mapping["attributes_source_type"] == "all"


def test_resolve_forwards_id_field_to_contributor() -> None:
    p = VectorIngestParams(id_field="GAUL1_CODE")
    c = VECTOR_INGEST_DEMO_PRESET._resolve(p)
    assert c.id_field == "GAUL1_CODE"


def test_different_targets_have_different_dedup_keys() -> None:
    c1 = _custom_contributor(catalog_id="cat1", collection_id="col1")
    c2 = _custom_contributor(catalog_id="cat2", collection_id="col2")
    k1 = list(c1.get_tasks())[0].dedup_key
    k2 = list(c2.get_tasks())[0].dedup_key
    assert k1 != k2


# ---------------------------------------------------------------------------
# VECTOR_INGEST_DEMO_PRESET — metadata
# ---------------------------------------------------------------------------

def test_preset_name() -> None:
    assert VECTOR_INGEST_DEMO_PRESET.name == "vector_ingest_demo"


def test_preset_tier_is_platform() -> None:
    assert VECTOR_INGEST_DEMO_PRESET.tier == PresetTier.PLATFORM


def test_preset_params_model_is_vector_ingest_params() -> None:
    assert VECTOR_INGEST_DEMO_PRESET.params_model is VectorIngestParams


def test_preset_params_model_instantiable_without_args() -> None:
    """Param-less POST must still work: params_model() must succeed."""
    p = VECTOR_INGEST_DEMO_PRESET.params_model()
    assert isinstance(p, VectorIngestParams)
    assert p.source_uri is None


def test_preset_description_non_empty() -> None:
    assert VECTOR_INGEST_DEMO_PRESET.description


def test_preset_keywords_contain_expected() -> None:
    kws = set(VECTOR_INGEST_DEMO_PRESET.keywords)
    assert {"demo", "vector", "ingestion"} <= kws


def test_preset_resolve_defaults_with_no_params() -> None:
    """_resolve(NoParams-equivalent) must return the Natural Earth contributor."""
    p = VectorIngestParams()
    c = VECTOR_INGEST_DEMO_PRESET._resolve(p)
    assert c.catalog_id == _CATALOG_ID
    assert c.collection_id == _COLLECTION_ID
    assert c.source_uri == _NE_COUNTRIES_URL
    assert c.source_format is None


def test_preset_resolve_with_overrides() -> None:
    p = VectorIngestParams(
        source_uri=_GPKG_URI,
        catalog_id="ph4_catalog",
        collection_id="ph4_network",
    )
    c = VECTOR_INGEST_DEMO_PRESET._resolve(p)
    assert c.source_uri == _GPKG_URI
    assert c.catalog_id == "ph4_catalog"
    assert c.collection_id == "ph4_network"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_preset_registered_in_registry() -> None:
    import dynastore.modules.storage.presets  # noqa: F401 — side-effect registers all

    preset = find_preset("vector_ingest_demo")
    assert preset.name == "vector_ingest_demo"


def test_preset_registry_tier_is_platform() -> None:
    import dynastore.modules.storage.presets  # noqa: F401

    preset = find_preset("vector_ingest_demo")
    assert preset.tier == PresetTier.PLATFORM


def test_preset_registry_params_model_is_vector_ingest_params() -> None:
    import dynastore.modules.storage.presets  # noqa: F401

    preset = find_preset("vector_ingest_demo")
    assert preset.params_model is VectorIngestParams


# ---------------------------------------------------------------------------
# Bucket-free + MVT preseed wiring
# ---------------------------------------------------------------------------

def test_data_seed_is_bucket_free() -> None:
    """Vector ingestion is PG-primary, so the catalog is created bucket-free
    (defer_provisioning=True) — no GCS bucket is provisioned for this preset."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
    )
    seed = next(iter(c.get_data()))
    assert seed.defer_provisioning is True


def test_task_seed_requests_mvt_preseed_on_success() -> None:
    """The ingestion TaskSeed carries a preseed_on_success hint so an MVT tile
    preseed is chained after the features are committed."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
    )
    seed = next(iter(c.get_tasks()))
    assert seed.inputs["preseed_on_success"] == {"output_format": "mvt"}


def test_preseed_disabled_omits_preseed_on_success() -> None:
    """preseed_tiles=False drops the chain hint entirely — ingestion only."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
        preseed_tiles=False,
    )
    seed = next(iter(c.get_tasks()))
    assert "preseed_on_success" not in seed.inputs


def test_custom_tile_format_propagates_to_preseed_hint() -> None:
    """tile_format overrides the chained preseed output_format."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
        tile_format="geojson",
    )
    seed = next(iter(c.get_tasks()))
    assert seed.inputs["preseed_on_success"] == {"output_format": "geojson"}


def test_params_preseed_defaults() -> None:
    """Defaults: preseed on, MVT format."""
    p = VectorIngestParams()
    assert p.preseed_tiles is True
    assert p.tile_format == "mvt"


def test_resolve_forwards_preseed_params_to_contributor() -> None:
    """_resolve must thread preseed_tiles / tile_format onto the contributor."""
    p = VectorIngestParams(preseed_tiles=False, tile_format="geojson")
    c = VECTOR_INGEST_DEMO_PRESET._resolve(p)
    assert c.preseed_tiles is False
    assert c.tile_format == "geojson"


# ---------------------------------------------------------------------------
# cache_bucket param + tile-cache-location derivation
# ---------------------------------------------------------------------------

def test_params_cache_bucket_default_none() -> None:
    assert VectorIngestParams().cache_bucket is None


def test_resolve_cache_location_explicit_bucket_default_prefix() -> None:
    """Explicit cache_bucket → that bucket, prefix None (storage folds catalog_id)."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_GPKG_URI, source_format=None,
        cache_bucket="my-cache-bucket",
    )
    assert c._resolve_cache_location() == ("my-cache-bucket", None)


def test_resolve_cache_location_derives_from_gs_source() -> None:
    """No override + gs:// source → source's own bucket, folder named after file,
    with a catalog_id segment for isolation."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri="gs://fao-aip-geospatial-review-data/demo_data/ph4_net.gpkg",
        source_format=None,
    )
    assert c._resolve_cache_location() == (
        "fao-aip-geospatial-review-data", "demo_data/ph4_net/cc"
    )


def test_resolve_cache_location_isolates_same_source_different_catalogs() -> None:
    """Two catalogs fed the SAME source file must get distinct cache prefixes,
    so dropping one never wipes the other's tiles."""
    src = "gs://bkt/dir/file.gpkg"
    a = _VectorIngestDemoContributor(
        catalog_id="catA", collection_id="lyr", source_uri=src, source_format=None,
    )
    b = _VectorIngestDemoContributor(
        catalog_id="catB", collection_id="lyr", source_uri=src, source_format=None,
    )
    pa = a._resolve_cache_location()
    pb = b._resolve_cache_location()
    assert pa == ("bkt", "dir/file/catA")
    assert pb == ("bkt", "dir/file/catB")
    assert pa[1] != pb[1]


def test_resolve_cache_location_https_source_is_managed() -> None:
    """Non-gs source with no override → managed bucket (None, None)."""
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_NE_COUNTRIES_URL, source_format=None,
    )
    assert c._resolve_cache_location() == (None, None)


def test_get_configs_emits_catalog_scoped_gcp_config_for_gs_source() -> None:
    """gs:// source yields a GcpTileCacheConfig pinned to the derived location."""
    from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig

    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri="gs://bkt/dir/file.gpkg", source_format=None,
    )
    cfgs = c.get_configs()
    assert len(cfgs) == 1
    cfg = cfgs[0]
    assert isinstance(cfg, GcpTileCacheConfig)
    assert cfg.cache_bucket == "bkt"
    assert cfg.cache_prefix == "dir/file/cc"


def test_get_configs_empty_when_preseed_disabled() -> None:
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri="gs://bkt/dir/file.gpkg", source_format=None,
        preseed_tiles=False,
    )
    assert c.get_configs() == []


def test_get_configs_empty_for_https_managed_source() -> None:
    c = _VectorIngestDemoContributor(
        catalog_id="cc", collection_id="lyr",
        source_uri=_NE_COUNTRIES_URL, source_format=None,
    )
    assert c.get_configs() == []


def test_resolve_forwards_cache_bucket_to_contributor() -> None:
    p = VectorIngestParams(cache_bucket="explicit-bucket")
    c = VECTOR_INGEST_DEMO_PRESET._resolve(p)
    assert c.cache_bucket == "explicit-bucket"
