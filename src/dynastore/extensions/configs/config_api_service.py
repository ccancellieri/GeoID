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

"""Composer for the centralised composed-config API.

Response shape: ``configs[scope][topic][(sub)?][ClassName] -> payload``.
Class name IS the identity — no ``class_key`` field inside the payload.
Routing configs' ``operations[OP]`` become slim ``DriverRef`` dicts
pointing at sibling driver configs in ``configs.storage.drivers.*``.

Placement is read from each ``PluginConfig`` subclass's mandatory
``_address: ClassVar[Tuple[Optional[str], ...]]`` ClassVar (variable-length
post Cycle D.0; subclass annotations may stay narrower as 3-tuples and
migrate incrementally during D.2).
Scope filtering is read from the optional ``_visibility`` ClassVar
(``"collection"`` / ``"catalog"`` / ``None`` = visible everywhere).
The composer no longer owns a placement heuristic — there is exactly
one source of truth per class.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Dict, List, Optional, Tuple, Type

from dynastore.extensions.configs.config_api_dto import (
    CatalogConfigResponse,
    CollectionConfigResponse,
    DriverRef,
    Link,
    PlatformConfigResponse,
)
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    list_registered_configs,
)

logger = logging.getLogger(__name__)


def _routing_config_keys() -> frozenset[str]:
    """Routing config wire keys (snake_case via ``class_key()``).

    Derived once on first call so renaming any routing class flows through
    without touching the composer.  No hardcoded PascalCase strings.
    """
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    )
    return frozenset({
        cls.class_key() for cls in (
            ItemsRoutingConfig, CollectionRoutingConfig,
            AssetRoutingConfig, CatalogRoutingConfig,
        )
    })


def _place(
    cls: Type[PluginConfig], active_scope: str,
) -> Optional[Tuple[Optional[str], ...]]:
    """Return ``cls._address`` if visible at ``active_scope``, else ``None``.

    Variable-length post Cycle D.1 — accepts today's 3-tuples
    (e.g. ``("storage","drivers","items")`` / ``("platform","gcp",None)``)
    and the post-D.2 tier-first shape (e.g.
    ``("platform","catalog","collection","items","policy")``).  The
    composer walks the tuple recursively in ``_place_at``.

    Filters:
    - Abstract bases (``is_abstract_base = True``) → dropped.
    - ``_visibility = "collection"`` and ``active_scope != "collection"`` → dropped.
    - ``_visibility = "catalog"`` and ``active_scope == "collection"`` → dropped.
    """
    if cls.__dict__.get("is_abstract_base", False):
        return None
    visibility = getattr(cls, "_visibility", None)
    if visibility == "collection" and active_scope != "collection":
        return None
    address = getattr(cls, "_address", None)
    if not address:
        # Defensive — concrete subclasses must declare _address (enforced in
        # PluginConfig.__init_subclass__) but skip rather than crash if a
        # malformed entry slips through.  ``not address`` catches both the
        # inherited-empty-tuple base sentinel (post Cycle D.0) and the absent
        # attribute case in one check; sentinel-shape independent.
        logger.warning(
            "PluginConfig %s.%s has no _address; skipping placement.",
            cls.__module__, cls.__name__,
        )
        return None
    return address


class ConfigApiService:
    """Composes the scope→topic→class-name configuration tree."""

    def __init__(
        self,
        config_service: ConfigsProtocol,
        assets_service: Optional[Any] = None,
        catalogs_service: Optional[Any] = None,
    ):
        self._config_service = config_service
        self._assets_service = assets_service
        self._catalogs_service = catalogs_service

    # --- Effective-value resolution (unchanged shape). ---

    async def _get_effective_configs(
        self,
        catalog_id: Optional[str],
        collection_id: Optional[str],
        resolved: bool,
    ) -> Tuple[
        Dict[str, Dict[str, Any]],
        Dict[str, str],
        Dict[str, Dict[str, Dict[str, Any]]],
    ]:
        """Return ``(by_class, sources, tier_data)`` for every registered config class.

        * ``by_class[ClassName]`` — effective payload (waterfall-resolved
          when ``resolved``).
        * ``sources[ClassName]`` — ``"collection" | "catalog" | "platform" | "default"``.
        * ``tier_data[tier]`` — ``{class_key: raw_delta}`` for each tier loaded
          (``platform``/``catalog``/``collection``).  Empty dict for tiers
          not loaded for this scope (catalog tier missing at platform-scope
          requests, collection tier missing at catalog-scope, etc.).
          Used by the composer to build the per-class layer trace
          (``meta.<class>.layers``) without re-fetching.  Always returned
          (also under ``resolved=False`` — the single loaded tier appears
          alone, the others as empty dicts).
        """
        svc = self._config_service
        all_classes = list_registered_configs()

        def _to_data_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
            return {it["plugin_id"]: (it.get("config_data") or {}) for it in items}

        if not resolved:
            tier_data: Dict[str, Dict[str, Dict[str, Any]]] = {
                "platform": {}, "catalog": {}, "collection": {},
            }
            if catalog_id and collection_id:
                result = await svc.list_configs(
                    catalog_id=catalog_id, collection_id=collection_id,
                    limit=1000, offset=0,
                )
                source = "collection"
                tier_data["collection"] = _to_data_map(result.get("items", []))
            elif catalog_id:
                result = await svc.list_configs(catalog_id=catalog_id, limit=1000, offset=0)
                source = "catalog"
                tier_data["catalog"] = _to_data_map(result.get("items", []))
            else:
                result = await svc.list_configs(limit=1000, offset=0)
                source = "platform"
                tier_data["platform"] = _to_data_map(result.get("items", []))

            by_class: Dict[str, Dict[str, Any]] = {}
            sources: Dict[str, str] = {}
            for item in result.get("items", []):
                class_key: str = item["plugin_id"]
                cls = all_classes.get(class_key)
                raw: Dict[str, Any] = item.get("config_data") or {}
                try:
                    by_class[class_key] = (
                        cls.model_validate(raw).model_dump() if cls else raw
                    )
                except Exception:
                    by_class[class_key] = raw
                sources[class_key] = source
            return by_class, sources, tier_data

        # Resolved path — pull each tier's per-class delta dict ONCE (3
        # SELECTs total) then merge in memory, mirroring the waterfall in
        # ``ConfigService.get_config`` (code defaults > platform > catalog
        # > collection, right-most wins).  Replaces the previous per-class
        # ``await svc.get_config(cls, ...)`` loop which fired ~N awaits per
        # request — large win on the deep view that the dashboard hits.
        collection_data: Dict[str, Dict[str, Any]] = {}
        catalog_data: Dict[str, Dict[str, Any]] = {}
        if catalog_id and collection_id:
            r = await svc.list_configs(
                catalog_id=catalog_id, collection_id=collection_id,
                limit=1000, offset=0,
            )
            collection_data = _to_data_map(r.get("items", []))
        if catalog_id:
            r = await svc.list_configs(catalog_id=catalog_id, limit=1000, offset=0)
            catalog_data = _to_data_map(r.get("items", []))
        r = await svc.list_configs(limit=1000, offset=0)
        platform_data = _to_data_map(r.get("items", []))

        by_class: Dict[str, Dict[str, Any]] = {}
        sources: Dict[str, str] = {}
        for class_key, cls in all_classes.items():
            if class_key in collection_data:
                sources[class_key] = "collection"
            elif class_key in catalog_data:
                sources[class_key] = "catalog"
            elif class_key in platform_data:
                sources[class_key] = "platform"
            else:
                sources[class_key] = "default"

            # Build the effective payload by merging deltas onto the code
            # default model.  Tier rows are stored as deltas (only fields
            # the caller explicitly set), so dict-update with last-wins
            # mirrors ``ConfigService.get_config``.
            try:
                merged: Dict[str, Any] = cls().model_dump(mode="python")
            except Exception:
                merged = {}
            for tier_dict in (platform_data, catalog_data, collection_data):
                delta = tier_dict.get(class_key)
                if delta:
                    merged.update(delta)

            # Round-trip through the model so defaults / coercions are applied
            # consistently with the single-class read path.
            try:
                by_class[class_key] = cls.model_validate(merged).model_dump()
            except Exception:
                by_class[class_key] = merged or {}
        return by_class, sources, {
            "platform":   platform_data,
            "catalog":    catalog_data,
            "collection": collection_data,
        }

    # --- Tree + meta in one pass. ---

    # NOTE: ``_build_meta_entry`` was retired in Cycle B.  The
    # waterfall-trace meta (source + layers) is gone; tier-of-origin
    # information now lives in the top-level ``inherited`` map.

    @staticmethod
    @functools.lru_cache(maxsize=256)
    def _extract_field_docs(model_cls: Type[PluginConfig]) -> Dict[str, str]:
        """Extract ``{field_name: description}`` from a Pydantic class's JSON schema.

        Lightweight alternative to attaching the full ``model_json_schema()``:
        each class declares ``Field(..., description=...)`` per field; this
        helper strips out just the description map. Cached per class since
        the schema is static.
        """
        try:
            schema = model_cls.model_json_schema()
        except Exception:
            return {}
        props = schema.get("properties", {})
        out: Dict[str, str] = {}
        for name, spec in props.items():
            if not isinstance(spec, dict):
                continue
            desc = spec.get("description")
            if isinstance(desc, str) and desc:
                out[name] = desc
        return out

    @staticmethod
    def _compose_tree(
        by_class: Dict[str, Dict[str, Any]],
        sources: Dict[str, str],
        active_scope: str,
        meta_mode: str = "none",
        include_mode: str = "scope",
    ) -> Tuple[
        Dict[str, Any],
        Optional[Dict[str, Any]],
        Optional[Dict[str, Any]],
    ]:
        """Bucket visible classes into a tier-first tree and optionally build hierarchical meta.

        ``meta_mode`` controls per-class field documentation in the
        hierarchical ``meta`` tree:

        - ``"none"`` — no docs; ``meta`` returned as ``None``.
        - ``"field"`` (default) — leaf is ``{"field_docs": {field_name:
          description}}``, extracted from the class JSON Schema.
          Lightweight, suitable for dashboards.
        - ``"schema"`` — leaf is ``{"json_schema": <full Pydantic schema>}``.
          Heavier; suitable for form-builders.

        The ``meta`` tree mirrors the ``configs`` tree shape exactly:
        the same path that produces a payload in ``configs`` produces
        the docs leaf in ``meta``.

        ``include_mode`` controls scope-vs-waterfall payload rendering:

        - ``"scope"`` (default) — body shows configs whose ``_visibility``
          declares the active scope as their owner OR whose stored row
          lives at the active scope.  Upstream-tier configs (catalog and
          platform from collection's POV; platform from catalog's POV) go
          into the hierarchical ``inherited`` tree, NOT inlined.
        - ``"upstream"`` — every visible class is rendered with its
          waterfall-resolved value.  Returned ``inherited`` is None.

        Cycle D.3: ``inherited`` is a hierarchical tree mirroring the
        ``configs`` tree shape.  Each leaf carries ``{"source": <tier>}``
        where ``<tier>`` is ``"platform"`` / ``"catalog"`` / ``"default"``
        — telling operators WHERE the resolved value comes from at the
        same path the value would land at if rendered.  The previous
        ``inherited_from_catalog`` sibling block is gone; catalog-tier
        configs at collection scope flow through the same ``inherited``
        tree as platform-tier ones — single uniform mechanism.

        Returns ``(tree, meta, inherited)``.  ``meta`` is None unless
        ``meta_mode != "none"``.  ``inherited`` is None unless
        ``include_mode == "scope"`` and at least one upstream class was
        filtered out.
        """
        wants_docs = meta_mode in ("field", "schema")
        slim = include_mode == "scope"
        all_classes = list_registered_configs()
        tree: Dict[str, Any] = {}
        meta_tree: Dict[str, Any] = {}
        inherited_tree: Dict[str, Any] = {}
        inherited_has_entries = False

        def _is_in_scope(cls: Type[PluginConfig], class_key: str) -> bool:
            """Slim-mode filter: keep only configs owned by ``active_scope``."""
            if active_scope == "platform":
                return True
            visibility = getattr(cls, "_visibility", None)
            if visibility == active_scope:
                return True
            if sources.get(class_key) == active_scope:
                return True
            return False

        def _doc_leaf(cls: Type[PluginConfig]) -> Dict[str, Any]:
            """Build the meta-leaf payload for a class according to ``meta_mode``."""
            if meta_mode == "schema":
                return {"json_schema": cls.model_json_schema()}
            return {"field_docs": ConfigApiService._extract_field_docs(cls)}

        def _place_at(
            target: Dict[str, Any],
            address: Tuple[Optional[str], ...],
            class_key: str,
            value: Any,
        ) -> None:
            """Walk the variable-length address tuple to the leaf and set value.

            Cycle D.1: walks the tuple recursively instead of unpacking
            ``(scope, topic, sub)``.  Skips ``None`` segments so today's
            3-tuples with trailing ``None`` (e.g. ``("platform","gcp",None)``)
            land at the same depth as before — zero behaviour change for
            existing addresses.  Variable-length tuples (post Cycle D.2)
            land at any depth: ``("platform","catalog","collection","items",
            "policy")`` produces a 5-deep nested path before the class_key
            leaf.
            """
            node = target
            for seg in address:
                if seg is None:
                    continue
                node = node.setdefault(seg, {})
            node[class_key] = value

        for class_key, payload in by_class.items():
            cls = all_classes.get(class_key)
            if cls is None:
                continue

            placed = _place(cls, active_scope)
            if placed is None:
                continue

            # Slim mode: defer upstream-tier configs (platform OR catalog
            # from collection's POV) to the hierarchical ``inherited``
            # tree instead of inlining their bodies.  Catalog-tier configs
            # flow through here too — no separate sibling block (Cycle D.3
            # dropped ``inherited_from_catalog``).
            if slim and not _is_in_scope(cls, class_key):
                source = sources.get(class_key, "default")
                _place_at(inherited_tree, placed, class_key, {"source": source})
                inherited_has_entries = True
                # NB: meta entries for inherited (slim-suppressed) classes
                # are intentionally absent — the inherited tree is a
                # breadcrumb, not a documentation surface.
                continue

            _place_at(tree, placed, class_key, payload)
            if wants_docs:
                _place_at(meta_tree, placed, class_key, _doc_leaf(cls))
        return (
            tree,
            (meta_tree if wants_docs else None),
            (inherited_tree if (slim and inherited_has_entries) else None),
        )

    # --- Routing-ref rewrite. ---

    @staticmethod
    def _build_routing_refs(by_class: Dict[str, Dict[str, Any]]) -> None:
        """Rewrite ``operations[OP]`` in routing configs as slim ``DriverRef``s.

        Driver → config-class lookup uses the registry directly: post-TypedDriver
        bind, ``class_key()`` for ``XDriverConfig`` returns the snake_case form
        of the bound driver class (e.g. ``"items_postgresql_driver"``) — the
        same string used as ``driver_id`` in routing entries.  ``driver_id`` IS
        the lookup key into ``list_registered_configs()``.
        """
        all_classes = list_registered_configs()
        for class_key in _routing_config_keys():
            routing = by_class.get(class_key)
            if not routing:
                continue
            operations = routing.get("operations")
            if not isinstance(operations, dict):
                continue
            rewritten: Dict[str, List[Dict[str, Any]]] = {}
            for op, entries in operations.items():
                refs: List[Dict[str, Any]] = []
                for entry in entries or []:
                    if not isinstance(entry, dict):
                        continue
                    driver_id = entry.get("driver_id", "")
                    config_ref = driver_id if driver_id in all_classes else None
                    refs.append(DriverRef(
                        driver_id=driver_id,
                        config_ref=config_ref,
                        on_failure=entry.get("on_failure", "fatal"),
                        write_mode=entry.get("write_mode", "sync"),
                    ).model_dump())
                rewritten[op] = refs
            routing["operations"] = rewritten

    # --- Compose endpoints. ---

    # NOTE: ``_build_routing_resolution`` was retired in Cycle C.  The
    # synthetic resolver hard-coded ``items_elasticsearch_driver`` /
    # ``items_elasticsearch_private_driver`` per op without consulting
    # the actual ``ItemsRoutingConfig``, which made it lie post PR #254
    # (PG is the WRITE primary, not ES).  Operators read the truth from
    # the routing tree under ``configs.platform.catalog.collection.
    # storage.routing`` directly.

    # --- JSON Hyper-Schema link assembly. ---

    @staticmethod
    def _query_param_schema(scope: str) -> Dict[str, Any]:
        """JSON Schema 2020-12 describing the query params for ``scope``.

        Used as ``hrefSchema`` on the ``self`` link so operators discover
        supported query parameters with descriptions and examples — without
        scanning the OpenAPI document. Returned schema is JSON Schema, not
        OpenAPI Parameter Object, by design (the user asked for alignment
        with general standards, not just OpenAPI/FastAPI).
        """
        common: Dict[str, Any] = {
            "resolved": {
                "type": "boolean", "default": True,
                "description": (
                    "When true (default): all registered configs with "
                    "waterfall-resolved values; the top-level ``inherited`` "
                    "tree (hierarchical, mirrors ``configs`` shape) tells "
                    "you which tier provided each upstream value. "
                    "When false: only configs explicitly stored at this "
                    "scope (delta-only, safe for read-modify-write flows)."
                ),
                "examples": [True, False],
            },
            "meta": {
                "type": "string", "enum": ["none", "field", "schema"],
                "default": "field",
                "description": (
                    "Documentation mode for the hierarchical ``meta`` tree. "
                    "``none`` — ``meta`` returned as null. ``field`` "
                    "(default) — leaf at the configs path is "
                    "``{field_docs: {field_name: description}}`` per class. "
                    "``schema`` — leaf is ``{json_schema: <full Pydantic "
                    "schema 2020-12>}`` per class (heavier, form-builder "
                    "ready)."
                ),
                "examples": ["field", "schema", "none"],
            },
            "include": {
                "type": "string", "enum": ["scope", "upstream"],
                "default": "scope",
                "description": (
                    "Body-rendering mode. ``scope`` (default) — body lists "
                    "only configs owned by the active scope; upstream-tier "
                    "configs are summarised in the hierarchical "
                    "``inherited`` tree (mirrors ``configs`` shape; leaves "
                    "carry ``{source: <tier>}``). ``upstream`` — every "
                    "visible class rendered with its waterfall-resolved "
                    "value (today's verbose default; useful when you want "
                    "the full payload)."
                ),
                "examples": ["scope", "upstream"],
            },
        }
        # NOTE: per-scope ``*_page`` and ``page_size`` slots were retired
        # in Cycle C alongside the ``categories`` paginated-children field
        # and the depth-expansion machinery.  Operators discover children
        # via the existing list endpoints (``GET /catalogs``,
        # ``GET /catalogs/{cat}/collections``, ``GET .../assets``).
        properties = dict(common)
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": properties,
            "additionalProperties": False,
        }

    @staticmethod
    def _build_links(
        scope: str,
        base_url: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> List[Link]:
        """Assemble the response-level ``_links`` block.

        Includes:
        - ``self`` with ``hrefSchema`` describing all query parameters.
        - ``alternate`` representations (other ``docs=`` modes).
        - ``edit`` (templated) for per-class PATCH at this scope.

        ``base_url`` is the URL without query string; the per-class CRUD
        endpoint convention is ``<base_url>/plugins/{class_key}``.
        """
        links: List[Link] = [
            Link(
                rel="self",
                href=base_url,
                method="GET",
                title=f"This {scope} config view (default mode)",
                hrefSchema=ConfigApiService._query_param_schema(scope),
            ),
            Link(
                rel="alternate",
                href=f"{base_url}?meta=schema",
                method="GET",
                title="Same view with full JSON Schema per class (form-builder mode)",
            ),
            Link(
                rel="alternate",
                href=f"{base_url}?meta=none",
                method="GET",
                title="Same view without field documentation (lean mode)",
            ),
            Link(
                rel="alternate",
                href=f"{base_url}?resolved=false",
                method="GET",
                title=(
                    "Delta-only: configs explicitly stored at this scope "
                    "(safe for read-modify-write flows)"
                ),
            ),
            Link(
                rel="alternate",
                href=f"{base_url}?include=upstream",
                method="GET",
                title=(
                    "Full waterfall: every visible class rendered with its "
                    "resolved value (verbose; today's pre-slim default)"
                ),
            ),
            Link(
                rel="edit",
                href=f"{base_url}/plugins/{{class_key}}",
                method="PATCH",
                title="Modify a single config class at this scope",
                templated=True,
            ),
        ]
        return links

    async def compose_collection_config(
        self,
        base_url: str,
        catalog_id: str,
        collection_id: str,
        resolved: bool = True,
        meta: str = "field",
        include: str = "scope",
    ) -> CollectionConfigResponse:
        by_class, sources, _tier_data = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=collection_id, resolved=resolved,
        )
        self._build_routing_refs(by_class)
        tree, meta_dict, inherited = self._compose_tree(
            by_class, sources, "collection",
            meta_mode=meta, include_mode=include,
        )
        return CollectionConfigResponse(
            links=self._build_links(
                "collection", base_url,
                catalog_id=catalog_id, collection_id=collection_id,
            ),
            collection_id=collection_id, catalog_id=catalog_id,
            inherited=inherited,
            configs=tree, meta=meta_dict,
        )

    async def compose_catalog_config(
        self,
        base_url: str,
        catalog_id: str,
        resolved: bool = True,
        meta: str = "field",
        include: str = "scope",
    ) -> CatalogConfigResponse:
        by_class, sources, _tier_data = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=None, resolved=resolved,
        )
        self._build_routing_refs(by_class)
        tree, meta_dict, inherited = self._compose_tree(
            by_class, sources, "catalog",
            meta_mode=meta, include_mode=include,
        )
        return CatalogConfigResponse(
            links=self._build_links("catalog", base_url, catalog_id=catalog_id),
            catalog_id=catalog_id, inherited=inherited,
            configs=tree, meta=meta_dict,
        )

    async def compose_platform_config(
        self,
        base_url: str,
        resolved: bool = True,
        meta: str = "field",
        include: str = "scope",
    ) -> PlatformConfigResponse:
        by_class, sources, _tier_data = await self._get_effective_configs(
            catalog_id=None, collection_id=None, resolved=resolved,
        )
        self._build_routing_refs(by_class)
        tree, meta_dict, _inherited = self._compose_tree(
            by_class, sources, "platform",
            meta_mode=meta, include_mode=include,
        )
        return PlatformConfigResponse(
            links=self._build_links("platform", base_url),
            scope="platform", configs=tree, meta=meta_dict,
        )

    # --- PATCH. ---

    async def patch_config(
        self,
        *,
        catalog_id: Optional[str],
        body: Dict[str, Optional[Dict[str, Any]]],
        collection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate-then-write partial update of one or more configs at a scope.

        Body is RFC 7396 merge-patch over the scope's plugin set: each top-level
        key is a ``plugin_id``; value is the new payload, or ``null`` to delete
        the override.  Atomic at the scope level — the validation pass runs to
        completion before any write fires.
        """
        all_classes = list_registered_configs()
        prepared: List[Tuple[str, Type[PluginConfig], Optional[Dict[str, Any]]]] = []

        for plugin_id, value in body.items():
            cls = all_classes.get(plugin_id)
            if cls is None:
                raise ValueError(f"Unknown config class '{plugin_id}'")
            if value is None:
                prepared.append((plugin_id, cls, None))
                continue
            current = (await self._config_service.get_persisted_config(
                cls, catalog_id=catalog_id, collection_id=collection_id,
            )) or {}
            merged = {**current, **value}
            cls.model_validate(merged)  # raises on bad data
            prepared.append((plugin_id, cls, merged))

        for plugin_id, cls, merged in prepared:
            if merged is None:
                await self._config_service.delete_config(
                    cls, catalog_id=catalog_id, collection_id=collection_id,
                )
            else:
                validated = cls.model_validate(merged)
                await self._config_service.set_config(
                    cls, validated, catalog_id=catalog_id, collection_id=collection_id,
                )
        return {"updated": [p for p, _, _ in prepared]}

    # --- Pagination helpers. ---

    # NOTE: Cycle C retired ``_build_config_page``, ``_list_assets``,
    # ``_list_collections``, and the entire ``categories`` /
    # ``ConfigPage`` paginated-children machinery (~150 lines).
    # Operators discover children via the existing list endpoints
    # (``GET /catalogs``, ``GET /catalogs/{cat}/collections``,
    # ``GET .../assets``).  The composed-config response is now scoped
    # to a single tier — siblings/children are discovered separately.
