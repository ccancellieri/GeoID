"""Platform notebook registrations for the catalog module.

Registers collection configuration notebooks:
  - items_write_policy            — write policy configuration
  - collection_feature_type            — feature type definition
  - collection_metadata_enrichment     — metadata enrichment / METADATA storage routing
  - collection_shapefile_strict_schema — end-to-end strict-schema demo (PRs #178/179/180/181)

Import this module during CatalogModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.catalog"

register_platform_notebook(
    notebook_id="items_write_policy",
    registered_by=_REG,
    notebook_path=_HERE / "items_write_policy.ipynb",
    title={"en": "Items Write Policy"},
    description={"en": "Configure write-once semantics, external ID tracking, and temporal validity for items."},
    tags=["items", "write-policy", "configuration"],
)

register_platform_notebook(
    notebook_id="collection_feature_type",
    registered_by=_REG,
    notebook_path=_HERE / "feature_type.ipynb",
    title={"en": "Collection Feature Type Definition"},
    description={"en": "Define the feature type schema (field names, types, geometry) for a collection."},
    tags=["collection", "feature-type", "schema", "configuration"],
)

register_platform_notebook(
    notebook_id="collection_metadata_enrichment",
    registered_by=_REG,
    notebook_path=_HERE / "metadata_enrichment.ipynb",
    title={"en": "Collection Metadata Enrichment"},
    description={"en": "Configure METADATA storage routing — which storage drivers supply collection metadata at read time."},
    tags=["collection", "metadata", "enrichment", "configuration"],
)

register_platform_notebook(
    notebook_id="collection_shapefile_strict_schema",
    registered_by=_REG,
    notebook_path=_HERE / "shapefile_strict_schema.ipynb",
    title={"en": "Strict-Schema Collection from a Shapefile"},
    description={"en": "End-to-end demo: upload shapefile → derive schema via OGR → enforce strict schema → optimize storage by columns → refuse duplicate identity. Combines PRs #178 (strict_unknown_fields), #179 (materialize_fields_as_columns), #180 (post-ingest ANALYZE), #181 (extract_ogr_schema)."},
    tags=["collection", "schema", "shapefile", "strict-mode", "ogr", "ingestion", "demo"],
)
