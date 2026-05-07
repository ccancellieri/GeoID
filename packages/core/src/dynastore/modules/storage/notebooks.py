"""Platform notebook registrations for the storage module.

Registers multi-driver routing pattern notebooks:
  - storage_combo_duckdb_plus_es    — DuckDB write + ES search
  - storage_combo_iceberg_plus_es   — Iceberg lakehouse + ES search
  - storage_combo_iceberg_plus_pg   — Iceberg write + PG read/search

Import this module during StorageModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.storage"

register_platform_notebook(
    notebook_id="storage_combo_duckdb_plus_es",
    registered_by=_REG,
    notebook_path=_HERE / "duckdb_plus_es.ipynb",
    title={"en": "DuckDB Analytics + ES Search"},
    description={"en": "Route WRITE to DuckDB for analytical queries; SEARCH to Elasticsearch for full-text."},
    tags=["storage", "duckdb", "elasticsearch", "routing", "analytics"],
)

register_platform_notebook(
    notebook_id="storage_combo_iceberg_plus_es",
    registered_by=_REG,
    notebook_path=_HERE / "iceberg_plus_es.ipynb",
    title={"en": "Iceberg Lakehouse + ES Search"},
    description={"en": "Route WRITE to Apache Iceberg for lakehouse storage; SEARCH to Elasticsearch."},
    tags=["storage", "iceberg", "elasticsearch", "routing", "lakehouse"],
)

register_platform_notebook(
    notebook_id="storage_combo_iceberg_plus_pg",
    registered_by=_REG,
    notebook_path=_HERE / "iceberg_plus_pg.ipynb",
    title={"en": "Iceberg Write + PG Read/Search"},
    description={"en": "Route WRITE to Apache Iceberg; READ and SEARCH to PostgreSQL."},
    tags=["storage", "iceberg", "postgresql", "routing"],
)
