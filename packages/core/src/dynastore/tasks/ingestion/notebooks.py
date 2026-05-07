"""Platform notebook registrations for the ingestion task.

Registers ingestion workflow notebooks:
  - ingestion_basic      -- core ingestion pipeline (CSV, GeoJSON, column mapping)
  - ingestion_operations -- pre/post operations (encoding detection, asset download)
  - ingestion_advanced   -- write policies, reporters, multi-driver, temporal validity

Import this module during NotebooksModule lifespan (before seeding).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.tasks.ingestion"

register_platform_notebook(
    notebook_id="ingestion_basic",
    registered_by=_REG,
    notebook_path=_HERE / "basic_ingestion.ipynb",
    title={"en": "Data Ingestion \u2014 Getting Started"},
    description={"en": "Ingest CSV, GeoJSON, and Shapefile data into collections via the OGC Processes API."},
    tags=["ingestion", "csv", "geojson", "shapefile", "processes"],
)

register_platform_notebook(
    notebook_id="ingestion_operations",
    registered_by=_REG,
    notebook_path=_HERE / "ingestion_operations.ipynb",
    title={"en": "Ingestion Pre/Post Operations"},
    description={"en": "Configure pre-ingestion and post-ingestion operations: asset download, encoding detection, custom hooks."},
    tags=["ingestion", "operations", "pre-processing", "post-processing"],
)

register_platform_notebook(
    notebook_id="ingestion_advanced",
    registered_by=_REG,
    notebook_path=_HERE / "ingestion_advanced.ipynb",
    title={"en": "Advanced Ingestion Patterns"},
    description={"en": "Write policies, reporters, multi-driver ingestion, and temporal validity during data ingestion."},
    tags=["ingestion", "write-policy", "reporters", "multi-driver", "temporal"],
)
