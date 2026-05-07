"""Demo data populate/cleanup CLI.

Invocation:
    python -m dynastore.tools.demo.populate populate
    python -m dynastore.tools.demo.populate cleanup

TODO: expose as an admin-gated `POST /tools/demo/populate` endpoint so the
      web demo page can trigger population without shelling into a container.
"""

import asyncio
import logging
import os
import sys
from contextlib import AsyncExitStack  # noqa: F401 — reserved for future nested lifespans
from types import SimpleNamespace

from dynastore import extensions, modules  # noqa: F401 — import side-effects register plugins
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://testuser:testpassword@localhost:54320/gis_dev",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("populate_demo_data")


def _get_or_register_catalogs_service():
    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc is None:
        raise RuntimeError(
            "CatalogsProtocol not registered. Ensure modules.discover_modules() "
            "and modules.instantiate_modules() ran before calling this helper."
        )
    return catalogs_svc


async def populate():
    if not os.getenv("DATABASE_URL"):
        logger.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    app_state = SimpleNamespace()
    modules.discover_modules()
    modules.instantiate_modules(app_state)

    logger.info("Starting DynaStore modules and extensions...")
    async with modules.lifespan(app_state):
        catalogs_svc = _get_or_register_catalogs_service()
        if not catalogs_svc:
            logger.error("CatalogsProtocol not available.")
            return

        catalog_id = "demo_catalog"
        logger.info(f"Ensuring catalog '{catalog_id}' exists...")
        await catalogs_svc.delete_catalog(catalog_id, force=True)

        catalog_data = {
            "id": catalog_id,
            "title": {"en": "Demo Catalog", "it": "Catalogo Demo"},
            "description": {"en": "A catalog for demonstration purposes.", "it": "Un catalogo per scopi dimostrativi."},
            "keywords": ["demo", "dynastore", "geospatial"],
            "license": "CC-BY-4.0",
        }
        await catalogs_svc.create_catalog(catalog_data, lang="*")
        logger.info(f"Catalog '{catalog_id}' created.")

        collection_id = "demo_collection"
        logger.info(f"Ensuring collection '{collection_id}' exists in '{catalog_id}'...")
        collection_data = {
            "id": collection_id,
            "title": {"en": "Demo Collection"},
            "description": {"en": "A demo collection of points."},
            "type": "Feature",
        }
        await catalogs_svc.create_collection(catalog_id, collection_data, lang="*")
        logger.info(f"Collection '{collection_id}' created.")

        logger.info(f"Ingesting demo items into '{catalog_id}:{collection_id}'...")
        demo_items = [
            {
                "id": "item_1",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.4964, 41.9028]},
                "properties": {"name": "Rome", "description": "The capital of Italy"},
            },
            {
                "id": "item_2",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.8952, 52.3702]},
                "properties": {"name": "Amsterdam", "description": "The capital of the Netherlands"},
            },
        ]

        result = await catalogs_svc.upsert(catalog_id, collection_id, demo_items)
        logger.info(f"Successfully ingested {len(result)} items.")
        logger.info("Demo data population complete.")


async def cleanup():
    if not os.getenv("DATABASE_URL"):
        return

    app_state = SimpleNamespace()
    modules.discover_modules()
    modules.instantiate_modules(app_state)

    async with modules.lifespan(app_state):
        catalogs_svc = _get_or_register_catalogs_service()
        if not catalogs_svc:
            return
        catalog_id = "demo_catalog"
        logger.info(f"Cleaning up catalog '{catalog_id}'...")
        await catalogs_svc.delete_catalog(catalog_id, force=True)
        logger.info(f"Catalog '{catalog_id}' deleted.")


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "populate"
    if command == "populate":
        asyncio.run(populate())
    elif command == "cleanup":
        asyncio.run(cleanup())
    else:
        print(f"Unknown command: {command}. Use 'populate' or 'cleanup'.")
        sys.exit(2)


if __name__ == "__main__":
    main()
