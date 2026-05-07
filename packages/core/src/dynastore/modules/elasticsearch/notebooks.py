"""Platform notebook registrations for the elasticsearch module.

Registers Elasticsearch-specific pattern notebooks:
  - storage_combo_private_es_plus_pg     — PG primary + private ES index
  - storage_combo_private_es_only        — private ES index only
  - collection_vault_geoid_only          — vault collection: write-only + geoid-keyed retrieval
  - catalog_metadata_search              — ES/PG collection metadata search

Import this module during ElasticsearchModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.elasticsearch"

register_platform_notebook(
    notebook_id="storage_combo_private_es_plus_pg",
    registered_by=_REG,
    notebook_path=_HERE / "private_es_plus_pg.ipynb",
    title={"en": "PG Primary + Private ES Index"},
    description={"en": "Route WRITE to both PostgreSQL and a private ES index; READ from PG; SEARCH via the private ES index (geoid tokens only)."},
    tags=["storage", "postgresql", "elasticsearch", "private", "routing"],
)

register_platform_notebook(
    notebook_id="storage_combo_private_es_only",
    registered_by=_REG,
    notebook_path=_HERE / "private_es_only.ipynb",
    title={"en": "Private ES Index Only"},
    description={"en": "All operations routed to a private Elasticsearch index — only geoid tokens are stored, no item attributes persisted."},
    tags=["storage", "elasticsearch", "private"],
)

register_platform_notebook(
    notebook_id="collection_vault_geoid_only",
    registered_by=_REG,
    notebook_path=_HERE / "collection_vault_geoid_only.ipynb",
    title={"en": "Vault Collection — geoid-only retrieval"},
    description={
        "en": (
            "Lock down a collection so the only retrieval path is by geoid. WRITE fans out to "
            "PostgreSQL (authoritative geometry) + private ES (geoid-keyed index); READ pinned to "
            "PG with the geometry_exact hint; SEARCH pinned to private ES (no filter capability). "
            "Discovery surfaces (features list, STAC search, tiles, EDR, coverages, connected_systems) "
            "are blocked by a catalog-scoped IAM DENY bundle. Demonstrates the two-call accurate-geometry "
            "round-trip (ES geoid lookup -> PG items-by-id) and the capability-token model."
        )
    },
    tags=["storage", "elasticsearch", "private", "postgresql", "iam", "vault", "geoid", "routing"],
)

register_platform_notebook(
    notebook_id="catalog_metadata_search",
    registered_by=_REG,
    notebook_path=_HERE / "catalog_metadata_search.ipynb",
    title={"en": "Catalog Metadata Search"},
    description={
        "en": (
            "Configure and query the collection metadata search backend — "
            "Elasticsearch or PostgreSQL via the metadata.override routing. "
            "Covers auto-discovery, explicit config, multilanguage search, spatial bbox, "
            "CQL2 filter, aggregations, and PG fallback."
        )
    },
    tags=["catalog", "metadata", "elasticsearch", "search", "configuration"],
)
