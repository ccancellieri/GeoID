"""Platform notebook registrations for the STAC extension.

Imported during STACService lifespan so the showcase notebooks land in
the platform notebook table before NotebooksModule seeds them.
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.extensions.stac"


register_platform_notebook(
    notebook_id="stac_catalog_collection_lifecycle",
    registered_by=_REG,
    notebook_path=_HERE / "catalog_collection_lifecycle.ipynb",
    title={"en": "Catalog / Collection Lifecycle — STAC"},
    description={
        "en": (
            "Walks the full STAC lifecycle: create catalog, create "
            "collection with inline schema/layer_config/write_policy, "
            "localized update (en + es), round-trip, soft-delete, and "
            "the zero-config variant where every config falls back to "
            "code defaults."
        )
    },
    tags=["stac", "catalog", "collection", "lifecycle", "demo"],
)

register_platform_notebook(
    notebook_id="stac_virtual_asset_collections",
    registered_by=_REG,
    notebook_path=_HERE / "virtual_asset_collections.ipynb",
    title={"en": "Virtual Asset Collections — STAC"},
    description={
        "en": (
            "End-to-end loop using the /virtual/assets/... STAC endpoints "
            "to expose a single uploaded asset across every collection it "
            "belongs to. Demonstrates registering one asset_id under two "
            "collections and listing membership via the new virtual route."
        )
    },
    tags=["stac", "virtual", "assets", "demo"],
)
