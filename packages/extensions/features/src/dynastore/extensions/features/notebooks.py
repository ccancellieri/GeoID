"""Platform notebook registrations for the OGC Features extension.

Imported during OGCFeaturesService lifespan so the showcase notebooks land
in the platform notebook table before NotebooksModule seeds them.
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.extensions.features"


register_platform_notebook(
    notebook_id="features_ingestion_and_diagnostics",
    registered_by=_REG,
    notebook_path=_HERE / "ingestion_and_diagnostics.ipynb",
    title={"en": "Features — Ingestion & Diagnostics"},
    description={
        "en": (
            "Walks the waterfall-driven ingestion path: zero-config "
            "collection accepts items via code defaults, policy-driven "
            "rejections return an IngestionReport (HTTP 207 partial / "
            "200 fully accepted) with diagnostic links."
        )
    },
    tags=["features", "ingestion", "ogc", "demo"],
)
