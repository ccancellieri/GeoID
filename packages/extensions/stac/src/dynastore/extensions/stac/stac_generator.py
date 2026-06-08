#    Copyright 2025 FAO
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

# dynastore/extensions/stac/stac_generator.py
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Union, cast
from dynastore.models.shared_models import Feature
from dynastore.models.ogc import Feature as OGCFeature

import pystac
from fastapi import HTTPException, Request, status
from dynastore.modules.elasticsearch.items_projection import strip_reserved_members
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.extensions.tools.url import (
    get_parent_url,
    get_root_url,
    get_url,
)
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import GeometriesSidecarConfig
from dynastore.modules.stac.stac_config import (
    StacPluginConfig,
    HierarchyStrategy,
)
from dynastore.tools.language_utils import resolve_localized_field
from dynastore.extensions.tools.conformance import get_active_conformance
from dynastore.models.localization import (
    STAC_LANGUAGE_EXTENSION_URI,
    get_language_object,
)
from .stac_models import stac_localize
from dynastore.tools.discovery import get_protocol, get_protocols
from .stac_extension_protocol import StacExtensionProtocol, StacExtensionContext
from .metadata_helpers import merge_stac_metadata

logger = logging.getLogger(__name__)

from . import stac_db, asset_factory

SUPPORTED_STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/datacube/v2.3.0/schema.json",
    "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
    STAC_LANGUAGE_EXTENSION_URI,
]


def _apply_extra_metadata_fallbacks(
    collection: "pystac.Collection",
    merged_summaries: Dict[str, Any],
) -> None:
    """Resolve providers/summaries from extra_fields when the collection_stac
    PG sidecar is not active.

    Write-path: ``_pack_stac_extras`` (stac_service) folds them into
    ``extra_metadata``.  Read-path: the generator copies ``extra_metadata``
    content into ``collection.extra_fields``; this helper then promotes the
    typed pystac fields and removes the duplicates so they are not
    double-serialized by the ``stac_top_level`` cleanup that follows.

    When ``collection.providers`` / ``collection.summaries`` are already set
    (from the ``collection_stac`` sidecar) the fallback is skipped and the
    duplicate key is still removed from ``extra_fields``.
    """
    if collection.providers is None:
        extra_providers = collection.extra_fields.pop("providers", None)
        if extra_providers and isinstance(extra_providers, list):
            collection.providers = [
                pystac.Provider(**p) if isinstance(p, dict) else p
                for p in extra_providers
            ]
    else:
        collection.extra_fields.pop("providers", None)

    # pystac always initialises summaries to an empty Summaries(), never None.
    # Use is_empty() to distinguish "no data yet" from "already populated".
    summaries_empty = collection.summaries is None or collection.summaries.is_empty()
    if summaries_empty:
        extra_summaries = collection.extra_fields.pop("summaries", None)
        if extra_summaries and isinstance(extra_summaries, dict):
            if not merged_summaries:
                collection.summaries = pystac.Summaries(extra_summaries)
            else:
                merged_summaries.update(extra_summaries)
                collection.summaries = pystac.Summaries(merged_summaries)
    else:
        collection.extra_fields.pop("summaries", None)


async def create_root_catalog(request: Request, lang: str = "en") -> Dict[str, Any]:
    """Generates the root STAC Catalog."""
    # NOTE: derive from get_root_url (includes ``root_path``) rather than
    # get_url (which strips it).  Behind a reverse proxy with a non-empty
    # mount prefix, get_url(request) drops the prefix and the resulting
    # self/root/search/conformance links 404.
    base_url = f"{get_root_url(request)}/stac"
    root_catalog = pystac.Catalog(
        id="dynastore-stac-root",
        description="Multi-tenant OGC-compliant geospatial data platform implementing STAC API 1.0.0, OGC API Features (Parts 1-4), Processes, Records, Tiles, Maps, Coverages, and Dimensions.",
        title="DynaStore OGC STAC API",
    )

    # Dynamically inject the current server's conformance list
    root_catalog.extra_fields["conformsTo"] = get_active_conformance().conformsTo
    root_catalog.stac_extensions.append(STAC_LANGUAGE_EXTENSION_URI)

    # Note: Root catalog metadata is currently static/hardcoded in English.
    # In a full implementation, this could be localized if stored in DB.
    # For now, we manually inject the language block for consistency if requested.
    from dynastore.models.localization import get_language_object

    root_catalog.extra_fields["language"] = get_language_object(lang).model_dump(
        exclude_none=True
    )

    root_catalog.set_self_href(base_url)
    root_catalog.add_link(
        pystac.Link(
            rel="root", target=root_catalog.get_self_href() or "", title="Root Catalog"
        )
    )
    root_catalog.add_link(
        pystac.Link(rel="search", target=f"{base_url}/search", title="STAC Search")
    )
    root_catalog.add_link(
        pystac.Link(
            rel="conformance",
            target=f"{base_url}/conformance",
            media_type="application/json",
            title="API Conformance",
        )
    )

    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        raise RuntimeError("CatalogsProtocol not available")
    all_catalogs = await cast(CatalogsProtocol, catalogs_svc).list_catalogs(lang=lang, limit=1000)
    for cat in all_catalogs:
        # Localize catalog summary for the link title
        catalog_id = cat.id
        child_href = f"{base_url}/catalogs/{catalog_id}"
        root_catalog.add_link(
            pystac.Link(
                rel="child",
                target=child_href,
                media_type="application/json",
                title=str(cat.title.resolve(lang) or catalog_id) if cat.title else catalog_id,
            )
        )

    root_catalog.set_root(root_catalog)
    return root_catalog.to_dict()


def create_catalog_summary(
    request: Request, catalog_model: Any, lang: str = "en"
) -> Dict[str, Any]:
    """Generates a lightweight STAC Catalog summary (no child links)."""

    # Localize metadata
    meta_dict, available_langs = stac_localize(catalog_model, lang)

    catalog = pystac.Catalog(
        id=catalog_model.id,
        description=meta_dict.get("description")
        or f"STAC Catalog for the '{catalog_model.id}' database schema.",
        title=meta_dict.get("title") or f"Catalog: {catalog_model.id}",
    )

    # Inject language metadata
    if (
        "language" in meta_dict
        and STAC_LANGUAGE_EXTENSION_URI not in catalog.stac_extensions
    ):
        catalog.stac_extensions.append(STAC_LANGUAGE_EXTENSION_URI)
    if "language" in meta_dict:
        catalog.extra_fields["language"] = meta_dict["language"]
    if "languages" in meta_dict:
        catalog.extra_fields["languages"] = meta_dict["languages"]

    # Merge localized extra metadata into catalog extra fields
    # Use explicit variable to assist debugging if needed
    extra_meta = meta_dict.get("extra_metadata")
    if extra_meta and isinstance(extra_meta, dict):
        catalog.extra_fields.update(extra_meta)

    # Use dynamic exclusion from model and add STAC-specific top-level exclusions
    stac_top_level = {"stac_version", "stac_extensions", "links", "conformsTo", "id", "title", "description"}
    internal_cols = catalog_model.get_internal_columns() | stac_top_level
    for internal in internal_cols:
        catalog.extra_fields.pop(internal, None)

    # Set Links
    # Self link needs to point to the specific catalog endpoint
    self_href = f"{get_root_url(request)}/stac/catalogs/{catalog_model.id}"
    catalog.add_link(
        pystac.Link(rel="self", target=self_href, media_type="application/json")
    )

    # Root link
    catalog.add_link(
        pystac.Link(
            rel="root", target=f"{get_root_url(request)}/stac", title="Root Catalog"
        )
    )

    # Collections link (convenience)
    catalog.add_link(
        pystac.Link(
            rel="data",
            target=f"{self_href}/collections",
            media_type="application/json",
            title="Collections",
        )
    )

    return catalog.to_dict()


async def create_catalog(
    request: Request, catalog_id: str, lang: str = "en"
) -> Dict[str, Any]:
    """Generates a STAC Catalog for a specific catalog ID."""
    base_url = get_url(request)
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        raise RuntimeError("CatalogsProtocol not available")
    catalog_metadata_model = await cast(CatalogsProtocol, catalogs_svc).get_catalog_model(catalog_id)
    if not catalog_metadata_model:
        return {}

    # Localize metadata before using it in PySTAC
    meta_dict, available_langs = stac_localize(catalog_metadata_model, lang)

    catalog = pystac.Catalog(
        id=catalog_id,
        description=meta_dict.get("description")
        or f"STAC Catalog for the '{catalog_id}' database schema.",
        title=meta_dict.get("title") or f"Catalog: {catalog_id}",
    )

    # Inject language metadata
    if (
        "language" in meta_dict
        and STAC_LANGUAGE_EXTENSION_URI not in catalog.stac_extensions
    ):
        catalog.stac_extensions.append(STAC_LANGUAGE_EXTENSION_URI)
    if "language" in meta_dict:
        catalog.extra_fields["language"] = meta_dict["language"]
    if "languages" in meta_dict:
        catalog.extra_fields["languages"] = meta_dict["languages"]

    # Merge localized extra metadata into catalog extra fields
    if "extra_metadata" in meta_dict and isinstance(meta_dict["extra_metadata"], dict):
        extra = meta_dict["extra_metadata"]
        for k, v in extra.items():
            if k not in ["language", "languages"]:
                catalog.extra_fields[k] = v

    # Use dynamic exclusion based on the model's protocol + STAC top level
    stac_top_level = {"stac_version", "stac_extensions", "links", "conformsTo", "id", "title", "description"}
    internal_cols = catalog_metadata_model.get_internal_columns() | stac_top_level
    for internal in internal_cols:
        catalog.extra_fields.pop(internal, None)

    catalog.set_self_href(base_url)
    if lang != "*":
        catalog.get_links("self")[0].extra_fields["hreflang"] = lang

    root_link = pystac.Link(
        rel="root", target=get_parent_url(request, 2), title="Root Catalog"
    )
    parent_link = pystac.Link(
        rel="parent", target=get_parent_url(request, 1), title="Parent Catalog"
    )

    if lang != "*":
        root_link.extra_fields["hreflang"] = lang
        parent_link.extra_fields["hreflang"] = lang

    catalog.add_link(root_link)
    catalog.add_link(parent_link)

    # Add alternate links for other languages
    if lang != "*" and available_langs:
        for other_lang in sorted(available_langs - {lang}):
            alt_url = str(request.url.replace_query_params(lang=other_lang))
            lang_meta = get_language_object(other_lang)
            catalog.add_link(
                pystac.Link(
                    rel="alternate",
                    target=alt_url,
                    media_type="application/json",
                    title=lang_meta.name,
                    extra_fields={"hreflang": other_lang},
                )
            )

    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        raise RuntimeError("CatalogsProtocol not available")
    collections = await cast(CatalogsProtocol, catalogs_svc).list_collections(catalog_id, lang=lang, limit=1000)
    for coll in collections:
        # Localize collection summary for the link title
        collection_id = coll.id
        collection_href = f"{base_url}/collections/{collection_id}"
        catalog.add_link(
            pystac.Link(
                rel="child",
                target=collection_href,
                title=coll.title.resolve(lang) if coll.title else collection_id,
                media_type="application/json",
            )
        )
    catalog.set_root(catalog)
    return catalog.to_dict()


async def create_collections_catalog(
    request: Request, catalog_id: str, lang: str = "en"
) -> Dict[str, Any]:
    """Generates the collections list for a specific catalog."""
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        raise RuntimeError("CatalogsProtocol not available")
    collections = await cast(CatalogsProtocol, catalogs_svc).list_collections(catalog_id, lang=lang, limit=1000)

    stac_collections = []
    for coll in collections:
        stac_coll = await create_collection(request, catalog_id, coll.id, lang=lang)
        if stac_coll:
            stac_collections.append(stac_coll.to_dict())

    root_url = get_root_url(request)
    links = [
        {
            "rel": "self",
            "type": "application/json",
            "href": f"{root_url}/stac/catalogs/{catalog_id}/collections",
        },
        {
            "rel": "parent",
            "type": "application/json",
            "href": f"{root_url}/stac/catalogs/{catalog_id}",
        },
        {"rel": "root", "type": "application/json", "href": f"{root_url}/stac"},
    ]

    return {"collections": stac_collections, "links": links}


async def create_collection(
    request: Request, catalog_id: str, collection_id: str, lang: str = "en"
) -> Optional[pystac.Collection]:
    """Generates a full STAC Collection for a specific database table."""
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc:
        raise RuntimeError("CatalogsProtocol not available")
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    driver = await get_driver(Operation.READ, catalog_id, collection_id)
    metadata_model, layer_config = await asyncio.gather(
        catalogs_svc.get_collection_model(catalog_id, collection_id),  # type: ignore[attr-defined]
        driver.get_driver_config(catalog_id, collection_id),
    )
    if not metadata_model:
        return None

    # Localize metadata
    meta_dict, available_langs = stac_localize(metadata_model, lang)

    # Fetch STAC-specific config to inject extensions like datacube
    config_manager = get_protocol(ConfigsProtocol)
    if not config_manager:
        raise RuntimeError("ConfigsProtocol not available")
    stac_config = await config_manager.get_config(
        StacPluginConfig, catalog_id, collection_id
    )

    # Correctly handle the Extent object and its attributes
    spatial_bbox = [0, 0, 0, 0]
    if (
        metadata_model.extent
        and metadata_model.extent.spatial
        and metadata_model.extent.spatial.bbox
    ):
        # The model stores bbox as a list of tuples, get the first one.
        spatial_bbox = metadata_model.extent.spatial.bbox[0]

    spatial_extent = pystac.SpatialExtent([list(spatial_bbox)])

    temporal_interval_dates: List[Optional[datetime]] = [None, None]
    if (
        metadata_model.extent
        and metadata_model.extent.temporal
        and metadata_model.extent.temporal.interval
    ):
        # The model stores interval as a list of tuples, get the first one.
        start_dt, end_dt = metadata_model.extent.temporal.interval[0]
        if start_dt and end_dt:
            temporal_interval_dates = [
                dt.replace(tzinfo=timezone.utc) for dt in (start_dt, end_dt)
            ]

    temporal_extent = pystac.TemporalExtent(intervals=[temporal_interval_dates])
    extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)

    stac_extensions_to_add = [STAC_LANGUAGE_EXTENSION_URI]
    # Merge extension URIs declared in config (any-extension support)
    for ext_uri in stac_config.enabled_extensions:
        if ext_uri not in stac_extensions_to_add:
            stac_extensions_to_add.append(ext_uri)
    # Add extensions based on config
    if stac_config.cube_dimensions or stac_config.cube_variables:
        if SUPPORTED_STAC_EXTENSIONS[0] not in stac_extensions_to_add:
            stac_extensions_to_add.append(SUPPORTED_STAC_EXTENSIONS[0])

    # Cast layer_config components to expected types to handle Immutable wrappers.
    # Sidecars are PG-driver-internal — driver_sidecars() returns [] for any
    # non-PG resolved driver and the geometries-sidecar fast path is skipped.
    from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars

    storage_config = None
    if layer_config:
        geom_sidecar = next(
            (
                sc
                for sc in driver_sidecars(layer_config)
                if getattr(sc, "sidecar_type", None) == "geometries"
            ),
            None,
        )
        if geom_sidecar:
            storage_config = cast(GeometriesSidecarConfig, geom_sidecar)
            if storage_config.target_srid != 4326:
                stac_extensions_to_add.append(SUPPORTED_STAC_EXTENSIONS[1])

    collection = pystac.Collection(
        id=collection_id,
        description=meta_dict.get("description")
        or f"Data from '{catalog_id}:{collection_id}'",
        extent=extent,
        title=meta_dict.get("title") or collection_id,
        stac_extensions=stac_extensions_to_add,
        keywords=meta_dict.get("keywords"),
        license=meta_dict.get("license") or "proprietary",
    )

    # --- Providers (DB model takes precedence, config as fallback) ---
    providers_list = meta_dict.get("providers") or []
    if not providers_list and stac_config.providers:
        providers_list = stac_config.providers
    if providers_list:
        collection.providers = [
            pystac.Provider(**p) if isinstance(p, dict) else p
            for p in providers_list
        ]

    # --- Summaries (merge: config base + DB overrides) ---
    merged_summaries: Dict[str, Any] = {}
    if stac_config.summaries:
        from pydantic import BaseModel
        for k, v in stac_config.summaries.items():
            merged_summaries[k] = v.model_dump(exclude_none=True) if isinstance(v, BaseModel) else v
    db_summaries = meta_dict.get("summaries")
    if db_summaries and isinstance(db_summaries, dict):
        merged_summaries.update(db_summaries)
    if merged_summaries:
        collection.summaries = pystac.Summaries(merged_summaries)

    # --- Collection-level assets (config base + DB overrides) ---
    merged_assets: Dict[str, pystac.Asset] = {}
    # Config assets (static definitions with multilanguage resolution)
    for asset_id, asset_def in stac_config.assets.items():
        asset_dict = asset_def.model_dump(exclude_none=True)
        title = resolve_localized_field(asset_def.title, lang)
        desc = resolve_localized_field(asset_def.description, lang)
        merged_assets[asset_id] = pystac.Asset(
            href=asset_dict.get("href", ""),
            title=title if isinstance(title, str) else None,
            description=desc if isinstance(desc, str) else None,
            media_type=asset_dict.get("type"),
            roles=asset_dict.get("roles"),
            extra_fields={
                k: v for k, v in asset_dict.items()
                if k not in {"href", "title", "description", "type", "roles"}
            },
        )
    # DB-level assets override config
    db_assets = meta_dict.get("assets")
    if db_assets and isinstance(db_assets, dict):
        for asset_id, asset_data in db_assets.items():
            if isinstance(asset_data, dict):
                merged_assets[asset_id] = pystac.Asset(
                    href=asset_data.get("href", ""),
                    title=asset_data.get("title"),
                    description=asset_data.get("description"),
                    media_type=asset_data.get("type"),
                    roles=asset_data.get("roles"),
                    extra_fields={
                        k: v for k, v in asset_data.items()
                        if k not in {"href", "title", "description", "type", "roles"}
                    },
                )
    if merged_assets:
        collection.assets = merged_assets

    # --- Item assets templates (config base + DB overrides) ---
    merged_item_assets: Dict[str, Any] = {}
    for asset_id, asset_def in stac_config.item_assets.items():
        asset_dict = asset_def.model_dump(exclude_none=True)
        asset_dict.pop("href", None)  # item_assets must NOT have href
        title = resolve_localized_field(asset_def.title, lang)
        desc = resolve_localized_field(asset_def.description, lang)
        if title:
            asset_dict["title"] = title
        if desc:
            asset_dict["description"] = desc
        merged_item_assets[asset_id] = asset_dict
    db_item_assets = meta_dict.get("item_assets")
    if db_item_assets and isinstance(db_item_assets, dict):
        merged_item_assets.update(db_item_assets)
    if merged_item_assets:
        if "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json" not in collection.stac_extensions:
            collection.stac_extensions.append(
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
            )
        collection.extra_fields["item_assets"] = merged_item_assets

    # Inject language metadata
    if "language" in meta_dict:
        collection.extra_fields["language"] = meta_dict["language"]
    if "languages" in meta_dict:
        collection.extra_fields["languages"] = meta_dict["languages"]

    # Merge localized extra metadata into collection extra fields.
    # This path surfaces STAC extras (cube:dimensions, themes, sci:citation, …)
    # stored by _pack_stac_extras at write time, as well as any other custom
    # metadata the operator placed in extra_metadata directly.
    if "extra_metadata" in meta_dict and isinstance(meta_dict["extra_metadata"], dict):
        extra = meta_dict["extra_metadata"]
        for k, v in extra.items():
            if k not in ["language", "languages"]:
                 collection.extra_fields[k] = v

    # Fallback: resolve providers / summaries from extra_metadata when the
    # collection_stac PG sidecar is not active.  The helper also handles the
    # case where they ARE set (sidecar active) by dropping the duplicate key.
    _apply_extra_metadata_fallbacks(collection, merged_summaries)

    # Add datacube dimensions and variables from config
    if stac_config.cube_dimensions:
        collection.extra_fields["cube:dimensions"] = {
            k: v.model_dump(exclude_none=True)
            for k, v in stac_config.cube_dimensions.items()
        }

    if stac_config.cube_variables:
        collection.extra_fields["cube:variables"] = {
            k: v.model_dump(exclude_none=True)
            for k, v in stac_config.cube_variables.items()
        }

    if (
        SUPPORTED_STAC_EXTENSIONS[1] in stac_extensions_to_add
        and storage_config
        and "proj" in stac_config.auto_render_extensions
    ):
        collection.extra_fields["proj:epsg"] = storage_config.target_srid
        collection.extra_fields["proj:wkt2"] = None

    # Use dynamic exclusion based on the model's protocol + STAC top level
    stac_top_level = {"stac_version", "stac_extensions", "links", "conformsTo", "id", "title", "description", "extent", "keywords", "license", "providers", "summaries"}
    internal_cols = metadata_model.get_internal_columns() | stac_top_level
    for internal in internal_cols:
        collection.extra_fields.pop(internal, None)

    # Set the root catalog to avoid pystac trying to resolve it via network
    root_catalog = pystac.Catalog(
        id="root", description="Root Catalog", href=get_root_url(request)
    )
    collection.set_root(root_catalog)

    root_url = get_root_url(request)

    # Ensure correct Self and Items URLs regardless of calling endpoint
    collection_self_href = (
        f"{root_url}/stac/catalogs/{catalog_id}/collections/{collection_id}"
    )
    collection.set_self_href(collection_self_href)

    if lang != "*":
        collection.get_links("self")[0].extra_fields["hreflang"] = lang

    root_link = pystac.Link(rel="root", target=f"{root_url}/stac", title="Root Catalog")
    parent_link = pystac.Link(
        rel="parent",
        target=f"{root_url}/stac/catalogs/{catalog_id}",
        title="Parent Catalog",
    )

    items_href = f"{collection_self_href}/items"
    items_link = pystac.Link(
        rel="items",
        target=items_href,
        media_type="application/geo+json",
        title="Items in this Collection",
    )

    if lang != "*":
        root_link.extra_fields["hreflang"] = lang
        parent_link.extra_fields["hreflang"] = lang
        items_link.extra_fields["hreflang"] = lang

    collection.add_links([root_link, parent_link, items_link])

    # Add alternate links for other languages
    if lang != "*" and available_langs:
        for other_lang in sorted(available_langs - {lang}):
            alt_url = str(request.url.replace_query_params(lang=other_lang))
            lang_meta = get_language_object(other_lang)
            collection.add_link(
                pystac.Link(
                    rel="alternate",
                    target=alt_url,
                    media_type="application/json",
                    title=lang_meta.name,
                    extra_fields={"hreflang": other_lang},
                )
            )

    return collection


def apply_hierarchy_links(
    item: pystac.Item,
    feature_properties: Dict[str, Any],
    asset_id: str,
    config: StacPluginConfig,
    catalog_id: str,
    collection_id: str,
    collection_url: str,
    source_collection_url: str,
    view_mode: str = "standard",
) -> pystac.Item:
    """
    Dynamically applies parent, child, and source links to a STAC Item based on configuration and View Mode.

    Args:
        item: The pystac.Item object to modify.
        feature_properties: The properties of the feature, used to evaluate hierarchy rules.
        asset_id: The ID linking this feature to its source file item.
        config: The StacPluginConfig for the collection.
        collection_url: The base URL of the current collection.
        source_collection_url: The base URL of the shadow `_sources` collection.
        view_mode: "standard" or "virtual".

    Returns:
        The modified pystac.Item with added links.
    """
    # 1. Virtual View Check
    if view_mode.startswith("virtual"):
        # The virtual URL structure:
        # /stac/virtual/assets/{asset_id}/catalogs/{id}/collections/{coll_id}
        # /stac/virtual/hierarchy/{hierarchy_id}/catalogs/{id}/collections/{coll_id}

        # Extract root URL from the item's root catalog or derive from collection_url
        root_catalog = item.get_root()
        if root_catalog and hasattr(root_catalog, "get_self_href"):
            root_href = root_catalog.get_self_href()
            if root_href:
                root_url = root_href
            else:
                # Fallback: derive from collection_url
                root_url = (
                    collection_url.rsplit("/catalogs/", 1)[0]
                    if "/catalogs/" in collection_url
                    else collection_url
                )
        else:
            # Fallback: derive from collection_url
            root_url = (
                collection_url.rsplit("/catalogs/", 1)[0]
                if "/catalogs/" in collection_url
                else collection_url
            )

        if view_mode == "virtual-asset" and asset_id:
            virtual_coll_url = f"{root_url}/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
            item.add_link(
                pystac.Link(
                    rel="parent",
                    target=virtual_coll_url,
                    media_type="application/json",
                    title=f"Source Asset: {asset_id}",
                )
            )
            item.add_link(
                pystac.Link(
                    rel="collection",
                    target=virtual_coll_url,
                    media_type="application/json",
                    title=f"Source Asset: {asset_id}",
                )
            )
            return item

        if view_mode == "virtual-hierarchy":
            # For hierarchy, we need to know WHICH hierarchy_id we are in.
            # We can try to find the matching rule.
            from dynastore.tools.expression import evaluate_sql_condition
            if config.hierarchy:
                for rule in config.hierarchy.rules.values():
                    if rule.condition and evaluate_sql_condition(rule.condition, feature_properties):
                        # Link to the virtual collection for this hierarchy level
                        hier_coll_url = f"{root_url}/stac/virtual/hierarchy/{rule.hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}"

                        # If this is a child level, we can scope it to the parent value
                        if rule.parent_code_field:
                            parent_val = feature_properties.get(rule.parent_code_field)
                            if parent_val:
                                hier_coll_url += f"?parent_value={parent_val}"

                        item.add_link(
                            pystac.Link(
                                rel="parent",
                                target=hier_coll_url,
                                title=f"Level: {rule.level_name or rule.hierarchy_id}",
                            )
                        )
                        item.add_link(
                            pystac.Link(
                                rel="collection",
                                target=hier_coll_url,
                                title=f"Level: {rule.level_name or rule.hierarchy_id}",
                            )
                        )
                        break
            return item

    # Note: Asset lineage linking is now handled dynamically in asset_factory.

    # 3. Standard View: Dynamic Hierarchy Rules
    if config.hierarchy and config.hierarchy.enabled and config.hierarchy.rules:
        for rule in config.hierarchy.rules.values():
            # Recursive Strategy
            if rule.strategy == HierarchyStrategy.RECURSIVE:
                # Check if this item is a root in the recursive graph
                is_root = False
                if rule.root_condition:
                    from dynastore.tools.expression import evaluate_sql_condition
                    is_root = evaluate_sql_condition(
                        rule.root_condition, feature_properties
                    )
                elif rule.parent_code_field:
                    # Implicit root if parent field is null
                    parent_val = feature_properties.get(rule.parent_code_field)
                    is_root = parent_val is None

                if is_root:
                    # It's a root item, so it links to the Collection (already handled by default)
                    # Or we can explicitly tag it.
                    pass
                else:
                    # It's a child node. Parent is the item with ID specified in parent_code_field
                    if rule.parent_code_field:
                        parent_code = feature_properties.get(rule.parent_code_field)
                        if parent_code:
                            parent_item_url = f"{collection_url}/items/{parent_code}"
                            item.add_link(
                                pystac.Link(
                                    rel="parent",
                                    target=parent_item_url,
                                    media_type="application/geo+json",
                                )
                            )

            # Fixed Strategy (Default)
            else:
                # Evaluate condition to see if this rule applies to the current item
                from dynastore.tools.expression import evaluate_sql_condition
                if rule.condition and evaluate_sql_condition(rule.condition, feature_properties):
                    # Rule Matches: This item belongs to this level.
                    # Find its parent using parent_code_field
                    if rule.parent_code_field:
                        parent_code = feature_properties.get(rule.parent_code_field)
                        if parent_code:
                            parent_item_url = f"{collection_url}/items/{parent_code}"
                            item.add_link(
                                pystac.Link(
                                    rel="parent",
                                    target=parent_item_url,
                                    media_type="application/geo+json",
                                )
                            )
                    break  # Stop after the first matching rule for Fixed strategy

    return item



def resolve_item_datetime(properties: Dict[str, Any]) -> Optional[datetime]:
    """Resolve a STAC item's primary datetime from feature properties.

    STAC requires every item to carry a ``datetime``. Resolution order, most
    specific first: ``datetime`` → ``start_datetime`` → ``valid_from`` →
    ``created`` → ``transaction_time``. ``start_datetime`` is how a validity
    backed temporal column round-trips — ``lower(validity)`` is projected back
    as ``start_datetime`` on read — and ``created`` / ``transaction_time`` carry
    the ingestion timestamp.

    Returns a timezone-aware ``datetime`` (naive values are assumed UTC), or
    ``None`` when no parseable temporal value is present. A ``None`` result is
    the #1253 trigger: a COLUMNAR collection without a validity sink drops the
    item's datetime on write, so the stored row echoes back with no temporal
    field. The caller stamps an ingestion-timestamp fallback in that case so the
    echo stays valid STAC instead of 500-ing; enabling validity on the
    collection preserves the item's own datetime.
    """
    for dt_field in (
        "datetime",
        "start_datetime",
        "valid_from",
        "created",
        "transaction_time",
    ):
        val = properties.get(dt_field)
        if not val:
            continue
        try:
            dt = (
                val
                if isinstance(val, datetime)
                else datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            )
        except (ValueError, TypeError):
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


async def create_item_from_feature(
    request: Request,
    catalog_id: str,
    collection_id: str,
    feature: Union[Feature, OGCFeature],
    stac_config: Optional[StacPluginConfig] = None,
    view_mode: str = "standard",
    lang: str = "en",
    collection_url_override: Optional[str] = None,
) -> Optional[pystac.Item]:
    """Generates a STAC Item from a mapped Feature."""
    from dynastore.models.protocols.configs import ConfigsProtocol

    # 1. Resolve Configs
    if not stac_config:
        config_manager = get_protocol(ConfigsProtocol)
        if not config_manager:
            raise RuntimeError("ConfigsProtocol not available")
        stac_config = await config_manager.get_config(
            StacPluginConfig, catalog_id, collection_id
        )

    if feature is None:
        return None

    # 3. Detect available languages
    available_langs = set()
    
    # Check context metadata first (e.g. from StacItemsSidecar)
    # Sidecars publish to context.metadata, which the ItemMapper attaches to feature.model_extra
    if hasattr(feature, "model_extra") and feature.model_extra:
        ctx_langs = (
            feature.model_extra.get("item_metadata_available_langs")
            or feature.model_extra.get("stac_available_langs")
        )
        if ctx_langs:
            available_langs.update(ctx_langs)
    
    # helper for manual extraction if context is missing or incomplete
    def _extract_langs_from_dict(source: dict):
        if not isinstance(source, dict):
            return
        for val in source.values():
            if isinstance(val, dict):
                for k in val.keys():
                    if isinstance(k, str) and (len(k) == 2 or (len(k) == 5 and k[2] == "-")):
                        available_langs.add(k)

    # 4. Extract from feature itself if no context langs (best effort)
    if not available_langs:
        if hasattr(feature, "properties") and feature.properties:
            _extract_langs_from_dict(feature.properties)
            _extract_langs_from_dict(feature.properties.get("stac_extra_fields", {}))
            
        if hasattr(feature, "model_extra") and feature.model_extra:
            _extract_langs_from_dict(feature.model_extra)
            _extract_langs_from_dict(feature.model_extra.get("stac_extra_fields", {}))

    # 5. Geometry and BBox
    geometry = feature.geometry.model_dump() if feature.geometry else None

    # 6. Datetimes handling
    # Strip GeoJSON/STAC structural members (id, geometry, …) that can ride into
    # stored properties (the PG ``feature_id_expr AS id`` alias via the
    # attributes sidecar, or an ES write echo). pystac stores ``properties``
    # verbatim, so a leaked ``id`` would surface in the STAC echo; the canonical
    # identity already lives at ``feature.id`` (-> ``item.id``). Keeps the
    # POST/PUT echo aligned with the GET read contract (#1232).
    properties = strip_reserved_members(feature.properties or {})
    # Strip cross-search routing hints injected by the search path
    # (_inject_search_hints / search_items) that must never appear in the
    # serialized STAC item.  They are read by the caller before this function
    # is invoked and are not part of the STAC item's authored properties.
    properties.pop("_catalog_id", None)
    properties.pop("_collection_id", None)

    # Resolve i18n dicts in properties down to a single string for the
    # requested language. Internal storage keeps Internationalized fields
    # as ``{"en": "...", "it": "..."}`` so multiple consumers can pick
    # their language; STAC clients receive a mono-lingual document. Reuse
    # the same coercion the validator already applies on the write path so
    # write/read shapes stay symmetrical (an item that round-trips through
    # POST → GET ?lang=it returns the Italian string, not the dict).
    if lang != "*":
        from .stac_validator import _coerce_for_stac_validation
        properties = _coerce_for_stac_validation(properties, lang=lang)

    item_dt = resolve_item_datetime(properties)
    if item_dt is None:
        # STAC requires a datetime. A COLUMNAR collection without a validity
        # sink drops the item's datetime on write (#1253), so the echoed row
        # carries no temporal value. Rather than 500 on an invalid item, stamp
        # the ingestion timestamp; enabling validity on the collection routes
        # the item's own datetime into the validity range and preserves it.
        item_dt = datetime.now(timezone.utc)
        logger.warning(
            "STAC item '%s' (%s/%s) has no stored temporal value; using the "
            "ingestion timestamp as datetime. Enable validity on the collection "
            "to preserve item datetimes (#1253).",
            getattr(feature, "id", None),
            catalog_id,
            collection_id,
        )

    # 7. Create PySTAC Item
    item = pystac.Item(
        id=str(feature.id) if feature.id is not None else "",
        geometry=geometry,
        bbox=getattr(feature, "bbox", None),
        datetime=item_dt,
        properties=properties,
        collection=collection_id,
        stac_extensions=getattr(feature, "stac_extensions", []),
        extra_fields=getattr(feature, "extra_fields", {}),
    )

    if available_langs:
        from .stac_models import inject_stac_language_fields

        item_dict = item.to_dict()
        inject_stac_language_fields(item_dict, available_langs, lang)
        item.stac_extensions = item_dict.get("stac_extensions", item.stac_extensions)
        item.extra_fields.update(
            {k: v for k, v in item_dict.items() if k in ["language", "languages"]}
        )
        logger.debug(
            f"Injected language extension. available_langs={available_langs}. extensions={item.stac_extensions}"
        )

    # Add hreflang to standard links
    if lang != "*":
        for link in item.links:
            if link.rel in ["self", "root", "parent", "collection", "items"]:
                link.extra_fields["hreflang"] = lang

    # Add alternate links for other languages
    if lang != "*" and available_langs:
        for other_lang in sorted(available_langs - {lang}):
            alt_url = str(request.url.replace_query_params(lang=other_lang))
            lang_meta = get_language_object(other_lang)
            item.add_link(
                pystac.Link(
                    rel="alternate",
                    target=alt_url,
                    media_type="application/geo+json",
                    title=lang_meta.name,
                    extra_fields={"hreflang": other_lang},
                )
            )

    # 4. Finalize Item Links (STAC Compliance)
    # Use manual links to avoid pystac graph resolution issues in single-item output
    root_url = get_root_url(request)
    collection_url = (
        collection_url_override
        or f"{root_url}/stac/catalogs/{catalog_id}/collections/{collection_id}"
    )
    
    item.set_self_href(f"{collection_url}/items/{item.id}")
    
    # Set root and collection for hierarchical context (prevents network resolution)
    root_catalog = pystac.Catalog(
        id="root", description="Root Catalog", href=f"{root_url}/stac"
    )
    item.set_root(root_catalog)

    # Use a dummy Collection object to satisfy PySTAC's link management
    collection_obj = pystac.Collection(
        id=collection_id,
        description="Collection",
        extent=pystac.Extent(
            pystac.SpatialExtent([[-180, -90, 180, 90]]),
            pystac.TemporalExtent([[None, None]]),
        ),
        href=collection_url,
    )
    item.set_collection(collection_obj)
    
    # Manually inject parent link if missing (PySTAC often drops it if duplicate of collection)
    if not item.get_links("parent"):
        item.add_link(
            pystac.Link(
                rel="parent",
                target=collection_url,
                media_type="application/json",
                title="Collection",
            )
        )

    # Add dynamic assets and merge external metadata
    # Extract asset_id securely from feature model_extra
    if hasattr(feature, "model_extra") and feature.model_extra and "asset_id" in feature.model_extra:
        feat_asset_id = feature.model_extra["asset_id"]
    else:
        feat_asset_id = properties.get("asset_id")

    # Extract geoid similarly
    feat_geoid = feature.id if hasattr(feature, "id") else properties.get("geoid")

    # 4. Extract external_metadata from sidecar columns
    # StacItemsSidecar.map_row_to_feature already handles merging title,
    # description, etc into `feature.properties` but for extensions and assets
    # `merge_stac_metadata` still expects them in `external_metadata`.
    #
    # For default (no-schema/JSONB) catalogs without a stac_metadata sidecar,
    # `assets` and `stac_extensions` are stored inside the attributes JSONB blob
    # (keyed as "assets" / "stac_extensions") so they survive the PG round-trip.
    # strip_reserved_members() above removed them from the working `properties`
    # dict, but they are still accessible via the raw feature.properties dict.
    # The fallback reads from there so the round-trip is complete without
    # requiring the stac_metadata sidecar.
    _raw_feature_props = feature.properties or {} if hasattr(feature, "properties") else {}
    external_metadata = {}
    feat_assets = getattr(feature, "assets", None)
    if feat_assets:
        external_metadata["external_assets"] = feat_assets
    elif "assets" in properties:
        external_metadata["external_assets"] = properties["assets"]
    elif "assets" in _raw_feature_props:
        external_metadata["external_assets"] = _raw_feature_props["assets"]

    feat_stac_extensions = getattr(feature, "stac_extensions", None)
    if feat_stac_extensions:
        external_metadata["external_extensions"] = feat_stac_extensions
    elif "stac_extensions" in properties:
        external_metadata["external_extensions"] = properties["stac_extensions"]
    elif "stac_extensions" in _raw_feature_props:
        external_metadata["external_extensions"] = _raw_feature_props["stac_extensions"]

    extension_context = StacExtensionContext(
        base_url=root_url,
        catalog_id=catalog_id,
        collection_id=collection_id,
        item_id=item.id,
        geoid=str(feat_geoid) if feat_geoid else "",
        lang=lang,
    )

    # 3. Get all STAC extension providers, filtered by the collection's
    # ``auto_render_extensions`` flag.  Empty list = passthrough; only
    # externally-supplied content from the stac_metadata sidecar flows
    # through, no auto-generation runs.
    from dynastore.extensions.stac.metadata_helpers import filter_providers_by_short_names
    providers = filter_providers_by_short_names(
        get_protocols(StacExtensionProtocol),
        stac_config.auto_render_extensions,
    )

    # 4. Merge external + managed metadata
    await merge_stac_metadata(item, external_metadata, providers, extension_context)

    # 5. Legacy dynamic assets (temporary until all providers migrate to Protocol)
    asset_context = asset_factory.AssetContext(
        base_url=root_url,
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        stac_config=stac_config,
        asset_id=feat_asset_id,
    )
    await asset_factory.add_dynamic_assets_and_links(item, asset_context)

    # Hierarchy and source links apply to all feature items.
    apply_hierarchy_links(
        item=item,
        feature_properties=properties,
        asset_id=feat_asset_id or "",
        config=stac_config,
        catalog_id=catalog_id,
        collection_id=collection_id,
        collection_url=collection_url,
        source_collection_url="",  # Deprecated
        view_mode=view_mode,
    )

    return item


async def create_item_collection(
    request: Request,
    conn: Any,
    schema: str,
    table: str,
    limit: int,
    offset: int,
    stac_config: StacPluginConfig,
    view_mode: str = "standard",
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    lang: str = "en",
    cql_filter: Optional[str] = None,
    search_dispatch: Optional[Any] = None,
    hints: FrozenSet = frozenset(),
) -> Dict[str, Any]:
    """Generates a STAC ItemCollection for a single collection.

    ``search_dispatch`` (#1311): when the caller has already dispatched the
    listing to the routing-pinned items SEARCH driver (see
    ``maybe_dispatch_items_to_search_driver``), it passes the resulting
    ``QueryResponse`` here so this builder uses the driver-streamed features +
    ``total_count`` instead of running the PostgreSQL ``get_stac_items_paginated``
    fallback. ``None`` (the default) keeps the existing PG path byte-for-byte.

    ``hints``: per-request routing preferences forwarded to
    ``get_stac_items_paginated`` when the PG fallback path is taken (i.e. when
    ``search_dispatch`` is ``None``). Pass ``EXACT_READ_HINTS`` to force the
    exact-geometry driver; the default ``frozenset()`` preserves the existing
    routing behaviour unchanged.
    """
    # Ensure logical IDs are available for row-to-feature conversion
    catalog_id = catalog_id or schema
    collection_id = collection_id or table

    if search_dispatch is not None:
        # Driver-streamed page: the SEARCH driver already applied row-level
        # access scoping (envelope driver) and reports numberMatched via
        # ``count_entities``. Materialize the bounded page for item rendering.
        items_rows = [feature async for feature in search_dispatch.items]
        item_count = search_dispatch.total_count or 0
    else:
        items_rows, item_count = await stac_db.get_stac_items_paginated(
            conn, catalog_id, collection_id, limit, offset, stac_config,
            cql_filter=cql_filter,
            request=request,
            hints=hints,
        )

    stac_items_tasks = [
        create_item_from_feature(
            request,
            catalog_id,
            collection_id,
            feature,
            stac_config,
            view_mode=view_mode,
            lang=lang,
        )
        for feature in items_rows
    ]
    stac_items = await asyncio.gather(*stac_items_tasks)

    item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
    collection_dict = item_collection.to_dict()

    self_href = get_url(request, remove_qp=True)
    # Preserve any filter query params (filter, filter-lang, ?{property}={value}
    # shorthand, …) on the pagination links so paging through a filtered result
    # set keeps the filter. ``limit``/``offset`` are set explicitly per link.
    from urllib.parse import urlencode

    preserved = {
        k: v
        for k, v in request.query_params.items()
        if k not in ("limit", "offset")
    }

    def _page_href(page_offset: int) -> str:
        params = {**preserved, "limit": limit, "offset": page_offset}
        return f"{self_href}?{urlencode(params)}"

    collection_dict.setdefault("links", []).append(
        {
            "rel": "self",
            "href": _page_href(offset),
            "type": "application/geo+json",
        }
    )

    if (offset + limit) < item_count:
        collection_dict["links"].append(
            {
                "rel": "next",
                "href": _page_href(offset + limit),
                "type": "application/geo+json",
                "title": "Next page",
            }
        )

    if offset > 0:
        collection_dict["links"].append(
            {
                "rel": "prev",
                "href": _page_href(max(0, offset - limit)),
                "type": "application/geo+json",
                "title": "Previous page",
            }
        )

    collection_dict["numberMatched"] = item_count
    collection_dict["numberReturned"] = len(stac_items)
    return collection_dict


def create_empty_item_collection(
    request: Request, limit: int, offset: int
) -> Dict[str, Any]:
    """Generates a valid but empty STAC ItemCollection."""
    item_collection = pystac.ItemCollection(items=[])
    collection_dict = item_collection.to_dict()

    self_href = get_url(request, remove_qp=True)
    collection_dict["links"] = [
        {
            "rel": "self",
            "href": f"{self_href}?limit={limit}&offset={offset}",
            "type": "application/geo+json",
            "title": "Self",
        }
    ]
    collection_dict["numberMatched"] = 0
    collection_dict["numberReturned"] = 0
    return collection_dict


async def create_search_results_collection(
    request: Request,
    features: List[Feature],
    total_count: int,
    limit: int,
    offset: int,
    stac_config: StacPluginConfig,
    lang: str = "en",
) -> Dict[str, Any]:
    """
    Generates a STAC ItemCollection from a cross-collection search result.
    This function is now a thin presentation layer over the generic search module.
    """

    stac_items_tasks = []
    for feature in features:
        # Cross-collection tracking injected by search.py
        cid = feature.properties.get("_catalog_id") or ""
        tid = feature.properties.get("_collection_id") or ""

        stac_items_tasks.append(
            create_item_from_feature(request, cid, tid, feature=feature, stac_config=stac_config, lang=lang)
        )

    stac_items = await asyncio.gather(*stac_items_tasks)

    item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
    collection_dict = item_collection.to_dict()

    # Per STAC spec, POST search responses should not have next/prev links
    # but should provide context for the client to form the next request.
    # The 'context' object is deprecated in STAC API 1.0.0 for ItemCollection
    # in favor of numberMatched/numberReturned at the top level.
    # We will add them here.
    collection_dict["numberMatched"] = total_count
    collection_dict["numberReturned"] = len(stac_items)

    # Add a 'self' link to allow clients to repeat the search
    # (Note: GET request with parameters would be more standard here)
    self_href = get_url(request)
    collection_dict.setdefault("links", []).append(
        pystac.Link(
            rel="self", target=self_href, media_type="application/geo+json"
        ).to_dict()
    )

    return collection_dict


async def _process_stac_item_for_db(
    item: pystac.Item, layer_config: ItemsPostgresqlDriverConfig
) -> Dict[str, Any]:
    """Validates and prepares a STAC Item for database insertion."""
    if not item.geometry:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="STAC Item must have a valid geometry.",
        )

    try:
        # Return the item as a GeoJSON dictionary
        return item.to_dict()

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
