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

"""Composer for the centralised composed-config API.

Response shape: ``configs[scope][topic][(sub)?][ClassName] -> payload``.
Class name IS the identity — no ``class_key`` field inside the payload.
Routing configs' ``operations[OP]`` become slim ``DriverRef`` dicts
pointing at sibling driver configs in ``configs.platform.catalog.{tier}.drivers.*``.

Placement is read from each ``PluginConfig`` subclass's mandatory
``_address: ClassVar[Tuple[Optional[str], ...]]`` ClassVar (variable-length
post Cycle D.0; subclass annotations may stay narrower as 3-tuples and
migrate incrementally during D.2).
Scope placement is read from ``PluginConfig.effective_tiers()`` — the set
of scopes a config renders at, taken from the explicit ``_tiers`` ClassVar
when set and otherwise derived from ``_address``.  ``_tiers`` is decoupled
from ``_freeze_at`` (the immutability-gate tier) so a config gated for
immutability at one tier can still be viewed/edited at others — routing
configs rely on this to surface catalog-tier defaults that cascade to
collections.  The composer no longer owns a placement heuristic — there is
exactly one source of truth per class.
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
from dynastore.models.plugin_config import PluginConfig, list_registered_configs

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


def _view_scopes_for(cls: Type[PluginConfig]) -> frozenset[str]:
    """Effective scopes at which ``cls`` renders in the composed view.

    Single source: ``PluginConfig.effective_tiers()`` (explicit ``_tiers``
    else address-derived). Read defensively so synthetic test stubs (plain
    classes, not PluginConfig subclasses) still resolve.
    """
    fn = getattr(cls, "effective_tiers", None)
    if callable(fn):
        tiers: Any = fn()
        return frozenset(tiers)
    explicit = getattr(cls, "_tiers", None)
    if explicit is not None:
        return frozenset(explicit)
    return frozenset({"platform", "catalog", "collection"})


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
    - ``active_scope`` not in the config's effective view scopes
      (explicit ``_tiers``, else derived from ``_address`` via
      ``effective_tiers``) → dropped.
    """
    if cls.__dict__.get("is_abstract_base", False):
        return None
    if active_scope not in _view_scopes_for(cls):
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

    async def _get_extra_refs(
        self,
        catalog_id: Optional[str],
        collection_id: Optional[str],
    ) -> Dict[str, Tuple[str, Dict[str, Any]]]:
        """F.4d.1 — surface multi-instance refs (rows where ``ref_key !=
        class_key``) at the active scope.

        Returns ``{ref_key: (class_key, payload)}``.  The class-keyed canonical
        rows (``ref_key == class_key``) are already covered by the existing
        :meth:`_get_effective_configs` waterfall — only the *extra* refs flow
        through this helper, so no payload is read twice.

        Uses the F.4c.2 ``list_refs_at_scope`` + ``get_config_by_ref`` API
        added on ``ConfigsProtocol``; degrades to ``{}`` when the bound
        service does not implement them (older deployments / mocks).
        """
        svc = self._config_service
        list_refs = getattr(svc, "list_refs_at_scope", None)
        get_by_ref = getattr(svc, "get_config_by_ref", None)
        if list_refs is None or get_by_ref is None:
            return {}
        ref_map: Dict[str, str] = await list_refs(
            catalog_id=catalog_id, collection_id=collection_id,
        )
        if not ref_map:
            return {}
        all_classes = list_registered_configs()
        out: Dict[str, Tuple[str, Dict[str, Any]]] = {}
        for ref_key, class_key in ref_map.items():
            if ref_key == class_key:
                continue  # canonical row — already in by_class
            cls = all_classes.get(class_key)
            if cls is None:
                # Stored row references an unregistered class — skip
                # (matches the warning logged inside get_config_by_ref).
                continue
            cfg = await get_by_ref(
                ref_key, catalog_id=catalog_id, collection_id=collection_id,
            )
            if cfg is None:
                continue
            try:
                payload = cfg.model_dump()
            except Exception:
                payload = {}
            out[ref_key] = (class_key, payload)
        return out

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
            # ConfigService.list_configs returns rows shaped as
            # {"plugin_id": <class_key>, "config": <delta-dict>}. Earlier
            # drafts of this loader read "config_data" / "items" — that
            # contract never matched, so every composed view silently
            # rendered code defaults regardless of stored tier rows.
            return {it["plugin_id"]: (it.get("config") or {}) for it in items}

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
                tier_data["collection"] = _to_data_map(result.get("results", []))
            elif catalog_id:
                result = await svc.list_configs(catalog_id=catalog_id, limit=1000, offset=0)
                source = "catalog"
                tier_data["catalog"] = _to_data_map(result.get("results", []))
            else:
                result = await svc.list_configs(limit=1000, offset=0)
                source = "platform"
                tier_data["platform"] = _to_data_map(result.get("results", []))

            by_class: Dict[str, Dict[str, Any]] = {}
            sources: Dict[str, str] = {}
            for item in result.get("results", []):
                class_key: str = item["plugin_id"]
                cls = all_classes.get(class_key)
                raw: Dict[str, Any] = item.get("config") or {}
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
            collection_data = _to_data_map(r.get("results", []))
        if catalog_id:
            r = await svc.list_configs(catalog_id=catalog_id, limit=1000, offset=0)
            catalog_data = _to_data_map(r.get("results", []))
        r = await svc.list_configs(limit=1000, offset=0)
        platform_data = _to_data_map(r.get("results", []))

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
    def _extract_docs(model_cls: Type[PluginConfig]) -> Dict[str, str]:
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
    @functools.lru_cache(maxsize=256)
    def _extract_mutability(model_cls: Type[PluginConfig]) -> Dict[str, str]:
        """Return ``{field_name: kind}`` for a PluginConfig class.

        Delegates to the Protocol-based introspection API
        (``MutabilityIntrospectionProtocol.mutability_map``) so the
        renderer stays decoupled from the concrete ``PluginConfig`` —
        any class that implements the Protocol surface works.
        """
        from dynastore.models.mutability import (
            MutabilityIntrospectionProtocol,
            mutability_map,
        )
        if isinstance(model_cls, type) and issubclass(
            model_cls, MutabilityIntrospectionProtocol  # type: ignore[arg-type]
        ):
            try:
                return dict(model_cls.mutability_map())
            except Exception:
                pass
        # Fallback: the standalone helper handles any class with
        # Pydantic ``model_fields`` even if it doesn't formally implement
        # the Protocol.
        try:
            return mutability_map(model_cls)
        except Exception:
            return {}

    @staticmethod
    def _physical_projection(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build the read-only PHYSICAL projection for a PG items driver leaf.

        ``sidecars`` is a Computed (derived, read-only) realization of the
        items policy. This surfaces the resolved plan so operators can SEE
        how their policy is physically realized — answering "where are the
        sidecars / why is the list empty". Returns ``None`` for any leaf that
        isn't the PG items driver (no ``sidecars`` key), so non-PG nodes are
        untouched.

        Read-only by construction (``readOnly: true`` envelope): the source
        ``sidecars`` field is Computed, so PUTting the leaf back strips it and
        this projection is never round-tripped.
        """
        sidecars = payload.get("sidecars")
        if not sidecars or not isinstance(sidecars, list):
            # Only the PG items driver carries ``sidecars``; an empty list is
            # the unmaterialised default-fast state (nothing to project yet).
            return None

        resolved: List[Dict[str, Any]] = []
        for sc in sidecars:
            if not isinstance(sc, dict):
                continue
            sc_type = sc.get("sidecar_type")
            entry: Dict[str, Any] = {"sidecar_type": sc_type}
            # Surface the policy-derived storage shape per sidecar family so an
            # operator sees the concrete physical realization, not just names.
            if sc_type == "geometries":
                cells = sc.get("spatial_cells_overlay") or []
                geohash_prec = None
                h3_res: List[int] = []
                s2_res: List[int] = []
                for cf in cells:
                    if not isinstance(cf, dict):
                        continue
                    kind = cf.get("kind")
                    res = cf.get("resolution")
                    if kind == "geohash" and res is not None:
                        geohash_prec = res
                    elif kind == "h3" and res is not None:
                        h3_res.append(res)
                    elif kind == "s2" and res is not None:
                        s2_res.append(res)
                entry["derived"] = {
                    "geohash_precision": geohash_prec,
                    "h3_resolutions": h3_res,
                    "s2_resolutions": s2_res,
                    "geom_column": sc.get("geom_column"),
                    "bbox_column": sc.get("bbox_column"),
                    "statistics": [
                        cf.get("name") or cf.get("kind")
                        for cf in (sc.get("compute_fields_overlay") or [])
                        if isinstance(cf, dict)
                    ],
                }
            elif sc_type == "attributes":
                entry["derived"] = {
                    "storage_mode": sc.get("storage_mode"),
                    "external_id_field": sc.get("external_id_field"),
                    "asset_id_field": sc.get("asset_id_field"),
                    "validity_column": sc.get("validity_column"),
                    "statistics": [
                        cf.get("name") or cf.get("kind")
                        for cf in (sc.get("compute_fields_overlay") or [])
                        if isinstance(cf, dict)
                    ],
                }
            resolved.append(entry)

        return {
            "readOnly": True,
            "description": (
                "DERIVED physical realization of the items policy (read-only). "
                "Sidecars are computed by the PG driver at materialization; "
                "they cannot be authored — shape them via items_write_policy."
            ),
            "sidecars": resolved,
        }

    @staticmethod
    def _compose_tree(
        by_class: Dict[str, Dict[str, Any]],
        sources: Dict[str, str],
        active_scope: str,
        meta_mode: str = "none",
        include_mode: str = "scope",
        strict: bool = True,
        extra_refs: Optional[Dict[str, Tuple[str, Dict[str, Any]]]] = None,
        *,
        view_mode: str = "effective",
        links_mode: str = "none",
        base_url: str = "",
        configs_root_url: str = "",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bucket visible classes into a tier-first tree.

        ``meta_mode`` controls whether provenance + docs are inlined on
        each in-scope leaf as a ``_meta`` sibling.  #665 slice 3 had
        every leaf carry ``_meta = {tier, source}`` unconditionally
        (the parallel top-level ``inherited`` tree was retired in the
        same change).  #946 reversed that "always-on" contract: #918's
        ``extra="forbid"`` on ``PersistentModel`` makes any round-trip
        of a GET response into a PUT/PATCH body 422 on the envelope
        keys, so operators were forced to scrub manually.

        - ``"none"`` — no ``_meta`` on the leaf.  Payload is a clean
          delta safe to copy verbatim into a PATCH body.  Provenance
          is unavailable here; callers that need it pass ``"field"``
          or ``"schema"``.
        - ``"field"`` (default) — ``_meta = {tier, source, docs:
          {field_name: description}}`` from the class JSON Schema.
          Lightweight, suitable for dashboards.
        - ``"schema"`` — ``_meta = {tier, source, json_schema: <full
          Pydantic schema>}``.  Heavier; suitable for form-builders.

        Per #517: the field docs / schema are inlined on the leaf
        (alongside ``_links``) instead of mirroring the tree shape in a
        parallel top-level ``meta`` field.  Single source of truth, no
        cross-walk required.

        ``links_mode`` controls per-leaf HATEOAS edit affordances injected
        inline as a ``_links`` sibling (see ``_leaf_links``):

        - ``"none"`` (default) — no ``_links`` on any leaf.
        - ``"minimal"`` — ``rel``/``href``/``method`` per link, no titles.
        - ``"full"`` — adds a contextual ``title`` per link naming the
          class key and tier (catalog/collection ids included).

        ``include_mode`` controls scope-vs-waterfall payload rendering at
        PLATFORM scope only.  Post-#761 the catalog/collection responses
        surface the full configurable surface regardless of include mode —
        slim-filtering hid module/extension/task/engine leaves and broke
        the "complete the configuration" contract.

        - ``"scope"`` (default) — at PLATFORM scope under ``strict=True``
          drops catalog-tier templates (``_freeze_at="catalog"``) from
          the body.  At catalog and collection scope this mode is identical
          to ``"upstream"``: every leaf placed by ``_place()`` lands in
          the tree with its waterfall-resolved value.
        - ``"upstream"`` — every class that passes ``_place()`` is rendered
          with its waterfall-resolved value at every scope.

        Each leaf's ``_meta.source`` reports the effective tier
        (``"platform"`` / ``"catalog"`` / ``"collection"`` / ``"default"``)
        so operators see what they inherit vs override.  ``_freeze_at``
        still gates writability via the service layer; it no longer hides
        the leaf from a sub-platform read view.

        ``strict`` (default True, Cycle F.7d.2) tightens platform-scope
        visibility: when True, platform-scope view drops configs declared
        with ``_freeze_at="catalog"`` from the body.  This matches the
        user-mental-model that platform scope shows only platform-
        intrinsic configs (modules / extensions / tasks / engines), with
        catalog-tier templates surfaced only on demand.  When False, the
        previous always-true platform-scope filter is restored —
        catalog-tier defaults appear inline in the body.  Has no effect
        at catalog or collection scope (the per-tier ``_tiers`` placement
        filter already runs there).

        ``view_mode`` applies a provenance-based post-filter on the
        decorated leaf set AFTER all placement and slim-mode filters:

        - ``"effective"`` (default) — every leaf that passes placement
          and slim-mode is rendered; behaviour is byte-for-byte identical
          to the pre-#947 default.  No breaking change.
        - ``"delta"`` — only leaves whose ``_meta.source`` equals the
          active scope are included.  Equivalent to "what has THIS tier
          explicitly overridden?"  At platform scope this returns leaves
          whose source is ``"platform"`` (code-default leaves are
          suppressed).  Useful for read-modify-write flows — combine
          with ``resolved=true`` to see overrides rendered with their
          full effective values.
        - ``"inherited"`` — only leaves whose ``_meta.source`` differs
          from the active scope are included.  Returns everything the
          tier is inheriting from upstream tiers (plus ``"default"``
          values at any scope).

        Returns the composed ``configs`` tree.  Provenance lives on each
        leaf as ``_meta.source`` (``"platform"`` / ``"catalog"`` /
        ``"collection"`` / ``"default"``) — no parallel tree.
        """
        wants_links = links_mode in ("minimal", "full")
        slim = include_mode == "scope"
        all_classes = list_registered_configs()
        tree: Dict[str, Any] = {}
        # ``active_scope`` is the source-of-truth tier label (the URL may
        # be empty under unit tests; ``_scope_label_from_url`` only feeds
        # link titles in ``_leaf_links``).
        scope_label = active_scope

        def _is_in_scope(cls: Type[PluginConfig], class_key: str) -> bool:
            """Slim-mode filter: keep only configs visible at ``active_scope``.

            At platform scope under ``strict=True`` (Cycle F.7d.2), drop
            configs declared with ``_freeze_at="catalog"`` from the body
            (they're catalog-tier templates stored at platform scope —
            visible only via ``strict=False`` / ``include=upstream``).

            ``_freeze_at="platform"`` (engine configs, etc.) is treated
            as platform-intrinsic — kept in body at platform scope strict
            mode.  Engines ARE platform-tier resources by definition.

            An explicit ``_tiers`` is authoritative and overrides the
            ``_freeze_at`` template-slim: a config that opts into the
            platform view (e.g. routing defaults, which cascade from the
            platform base tier) stays in the body even under strict mode,
            so it is visible at the tier it was applied at.  Configs without
            ``_tiers`` keep the ``_freeze_at`` slimming behaviour.

            At catalog/collection scope: include every config that passed
            ``_place()`` — the resolved config response must surface the full
            configurable surface (modules, extensions, tasks, engines,
            drivers) per #761.  ``_meta.source`` on each leaf reports the
            effective tier (``platform`` / ``catalog`` / ``collection`` /
            ``default``); ``_freeze_at`` continues to gate writability via
            the service layer but does not hide the leaf from the read view.

            Side-effect: at catalog/collection scope ``include=scope`` and
            ``include=upstream`` currently return the same body (#947).  An
            explicit delta/inherited rendering knob is tracked there
            (proposed ``view=delta|effective|inherited``) so callers that
            want round-trip-into-PATCH semantics get a real filter rather
            than relying on ``resolved=false`` to drop inherited fields.
            """
            if active_scope == "platform":
                if not strict:
                    return True
                # Explicit ``_tiers`` wins over the ``_freeze_at``
                # template-slim: a config that opts into the platform view
                # is shown even under strict mode (consistent with
                # ``_place``, which placed it here for the same reason).
                explicit_tiers = getattr(cls, "_tiers", None)
                if explicit_tiers is not None:
                    return "platform" in explicit_tiers
                # Strict fallback: platform-intrinsic configs stay in body.
                # Both ``_freeze_at=None`` (default; gated at the platform
                # tier) AND ``_freeze_at="platform"`` (engines + other
                # platform-exclusive configs) qualify.  Catalog-/collection-
                # tier templates (``_freeze_at="catalog"`` / ``"collection"``)
                # are filtered out.
                freeze_at = getattr(cls, "_freeze_at", None)
                return freeze_at is None or freeze_at == "platform"
            del class_key
            return True

        def _doc_extras(cls: Type[PluginConfig]) -> Dict[str, Any]:
            """Optional ``_meta`` extras gated by ``meta_mode``.

            Returns the field-level extras to merge into the always-on
            ``_meta = {tier, source}`` envelope.  Empty when
            ``meta_mode == "none"``.
            """
            if meta_mode == "schema":
                # Markers contribute ``readOnly`` + ``x-mutability`` to
                # each property's JSON Schema via their
                # ``__get_pydantic_json_schema__`` hook — no
                # post-processing here.
                return {"json_schema": cls.model_json_schema()}
            if meta_mode == "field":
                out: Dict[str, Any] = {"docs": ConfigApiService._extract_docs(cls)}
                # Per #665 slice 4: ``mutability`` is a sibling of
                # ``docs`` covering every field of the class.
                mutability = ConfigApiService._extract_mutability(cls)
                if mutability:
                    out["mutability"] = mutability
                return out
            return {}

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

        def _decorate(
            payload: Dict[str, Any],
            cls: Type[PluginConfig],
            class_key: str,
            ref_key: str,
        ) -> Dict[str, Any]:
            """Inject ``_meta`` and ``_links`` siblings into an in-scope leaf.

            Mutates ``payload`` and returns it (cheaper than copying; the
            dict comes from ``_get_effective_configs`` and has no other
            consumer past this point).

            ``_meta = {tier, source[, docs|json_schema|mutability]}`` is
            injected when ``meta_mode != "none"``.  Under ``meta_mode="none"``
            no ``_meta`` is written so the payload is a clean delta safe to
            copy verbatim into a PATCH body (#946).  The structural-provenance
            envelope from #665 slice 3 is now opt-in via the explicit
            ``meta=field`` (default) / ``meta=schema`` modes; the admin UI
            and any dashboard that wants provenance badges passes one of
            those.  Overwrites any pre-existing ``_meta`` / ``_links`` so
            the composer is idempotent under repeated calls on the same
            payload dict.
            """
            if meta_mode != "none":
                payload["_meta"] = {
                    "tier": scope_label,
                    "source": sources.get(class_key, "default"),
                    **_doc_extras(cls),
                }
            if wants_links:
                engine_ref = payload.get("engine_ref") if isinstance(payload, dict) else None
                if not isinstance(engine_ref, str) or not engine_ref:
                    engine_ref = None
                payload["_links"] = ConfigApiService._leaf_links(
                    class_key=class_key,
                    ref_key=ref_key,
                    base_url=base_url,
                    configs_root_url=configs_root_url,
                    scope_label=scope_label,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    mode=links_mode,
                    engine_ref=engine_ref,
                )
            # #1016: drop empty ``input_transformers`` / ``output_transformers``
            # arrays from any leaf payload — they carry zero signal in the
            # default state (Pydantic default factory yields an empty list).
            # Catches ``ItemsReadPolicy.output_transformers`` and any future
            # config that adopts the same field name; routing-entry leaks are
            # handled separately in ``_build_routing_refs``.  Round-trip safe:
            # PUTting the payload back rehydrates the field to its empty
            # default — ``OperationDriverEntry`` and ``ItemsReadPolicy`` both
            # default to empty.
            if isinstance(payload, dict):
                if payload.get("input_transformers") == []:
                    payload.pop("input_transformers", None)
                if payload.get("output_transformers") == []:
                    payload.pop("output_transformers", None)

            # Phase 3 Decision 4 — PHYSICAL projection. ``sidecars`` is a
            # Computed (read-only) field derived from the items policy at
            # ensure_storage time. Surface the resolved plan + each sidecar's
            # derived fields as a clearly read-only ``physical`` envelope under
            # the PG driver node, so an operator can SEE how their policy is
            # physically realized even though they cannot author it. This
            # directly answers the "where are the sidecars / why is the list
            # empty" confusion. The projection is read-only (no PUT round-trip):
            # PUTting the leaf back strips ``sidecars`` (Computed) and the
            # ``physical`` key is dropped by the config-write strip path.
            # Skipped under ``meta=none`` so clean delta payloads stay
            # byte-for-byte round-trippable into a PATCH body (#946).
            if meta_mode != "none" and isinstance(payload, dict):
                projection = ConfigApiService._physical_projection(payload)
                if projection is not None:
                    payload["physical"] = projection
            return payload

        def _passes_view_filter(class_key: str) -> bool:
            """Provenance-based leaf filter driven by ``view_mode``.

            Applied after slim-mode and placement filters.  Uses the
            ``sources`` map built by ``_get_effective_configs``.

            - ``"effective"`` — every leaf passes; default, byte-for-byte
              identical to the pre-#947 behaviour.
            - ``"delta"`` — only leaves whose source equals the active
              scope pass.  At platform scope "delta" keeps platform-
              stored leaves and drops code defaults.
            - ``"inherited"`` — only leaves whose source differs from the
              active scope pass (includes ``"default"``-sourced leaves at
              any scope).
            """
            if view_mode == "effective":
                return True
            leaf_source = sources.get(class_key, "default")
            if view_mode == "delta":
                return leaf_source == active_scope
            # "inherited"
            return leaf_source != active_scope

        for class_key, payload in by_class.items():
            cls = all_classes.get(class_key)
            if cls is None:
                continue

            placed = _place(cls, active_scope)
            if placed is None:
                continue

            # Slim mode (default ``include=scope``): at platform scope
            # combined with ``strict=True``, ``_is_in_scope`` drops
            # ``_freeze_at=catalog``/``collection`` templates.  At
            # catalog/collection scope ``_is_in_scope`` unconditionally
            # returns True per the post-#761 'complete the configurable
            # surface' contract — every config visible at the tier is
            # rendered with ``_meta.source`` reporting the effective tier.
            # Provenance for any rendered leaf is on its own ``_meta.source``
            # *when* ``meta_mode != "none"`` (#946); callers that need
            # provenance and pass ``meta=none`` will find none — that's an
            # intentional trade-off for clean round-trippable payloads.
            # The parallel ``inherited`` tree was retired in #665 slice 3.
            if slim and not _is_in_scope(cls, class_key):
                continue

            # Provenance filter (#947): ``view=delta`` keeps only leaves
            # the active scope owns; ``view=inherited`` keeps only upstream
            # leaves.  ``view=effective`` (default) is a no-op.
            if not _passes_view_filter(class_key):
                continue

            _place_at(tree, placed, class_key, _decorate(payload, cls, class_key, class_key))

        # F.4d.1 — multi-instance ref surfacing.
        # Each entry in ``extra_refs`` is ``ref_key → (class_key, payload)``
        # where ``ref_key != class_key`` (canonical rows are already in
        # ``by_class``).  Place the ref leaf under the parent class's
        # ``_address`` so operators see ``platform.modules.tiles.tiles_secondary``
        # alongside the canonical ``platform.modules.tiles.tiles_config``.
        # Multi-instance leaves use ``ref_key`` for self/edit (the
        # per-instance CRUD URL) and the canonical ``class_key`` for
        # ``describedby`` (schema is per-class, not per-instance).
        if extra_refs:
            for ref_key, (class_key, payload) in extra_refs.items():
                cls = all_classes.get(class_key)
                if cls is None:
                    continue
                placed = _place(cls, active_scope)
                if placed is None:
                    continue
                if slim and not _is_in_scope(cls, class_key):
                    continue
                if not _passes_view_filter(class_key):
                    continue
                _place_at(tree, placed, ref_key, _decorate(payload, cls, class_key, ref_key))
        return tree

    # --- Routing-ref rewrite. ---

    @staticmethod
    def _scope_label_from_url(base_url: str) -> str:
        """Infer a human label for the active scope from the request URL.

        Cycle F.7d.3-fixup: routing-entry HATEOAS link titles need to
        tell operators which tier they are about to PATCH, so a
        collection-scope ``driver-config`` link doesn't get mistaken
        for a platform default override.
        """
        if "/collections/" in base_url:
            return "collection"
        if "/catalogs/" in base_url:
            return "catalog"
        return "platform"

    @staticmethod
    def _configs_root_url(base_url: str) -> str:
        """Strip ``/catalogs/{x}[/collections/{y}]`` from a composed-config URL.

        The registry endpoint (``/registry/{plugin_id}``) is scope-agnostic
        and lives at the configs router root.  Per-leaf ``describedby``
        links must therefore use the root URL even when the surrounding
        request is scoped.  Drops a trailing slash for consistent joining.
        """
        url = base_url.rstrip("/")
        # Order matters: ``/collections/`` is always nested under
        # ``/catalogs/`` so cut the deeper segment first.
        idx = url.find("/catalogs/")
        if idx >= 0:
            url = url[:idx]
        return url

    @staticmethod
    def _leaf_links(
        *,
        class_key: str,
        ref_key: str,
        base_url: str,
        configs_root_url: str,
        scope_label: str,
        catalog_id: Optional[str],
        collection_id: Optional[str],
        mode: str,
        engine_ref: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Build the per-leaf HATEOAS ``_links`` array.

        Always emits four affordances:

        - ``rel="self"`` ``GET {base_url}/plugins/{ref_key}`` — read the
          effective plugin config at the active scope.
        - ``rel="edit"`` ``PUT {base_url}/plugins/{ref_key}`` — replace
          the scope-tier override (full body required).
        - ``rel="edit"`` ``DELETE {base_url}/plugins/{ref_key}`` — clear
          the scope-tier override; falls back to the upstream waterfall.
        - ``rel="describedby"`` ``GET {configs_root_url}/registry/{class_key}``
          — Registry wrapper response carrying JSON Schema + description +
          scope.  Scope-agnostic; uses the canonical ``class_key`` (not
          ``ref_key``) because multi-instance refs share the canonical
          class's schema.

        ``mode == "full"`` adds two cross-link affordances:

        - ``rel="schema"`` ``GET {configs_root_url}/registry/{class_key}?meta=schema``
          — raw JSON Schema 2020-12 (no wrapper) — feed straight to a
          form-builder.
        - ``rel="engine"`` ``GET {platform_configs_root}/plugins/{engine_ref}``
          — only emitted when the leaf carries a non-null ``engine_ref``.
          Points at the platform-tier ``PluginConfig`` instance backing
          this driver's pool binding.  Same target whether ``engine_ref``
          is the engine's class_key (single-instance) or an operator-
          chosen ref name (multi-instance).

        ``mode == "full"`` also adds a contextual ``title`` per link
        naming the class key and tier (including catalog/collection ids
        when present).  ``mode == "minimal"`` emits rel/href/method only
        and skips the two cross-link affordances above.
        """
        base = base_url.rstrip("/")
        platform_root = configs_root_url.rstrip("/")
        plugin_href = f"{base}/plugins/{ref_key}"
        registry_href = f"{platform_root}/registry/{class_key}"

        if scope_label == "collection" and catalog_id and collection_id:
            tier_phrase = f"collection '{catalog_id}/{collection_id}'"
        elif scope_label == "catalog" and catalog_id:
            tier_phrase = f"catalog '{catalog_id}'"
        else:
            tier_phrase = "platform scope"

        def _title(text: str) -> Optional[str]:
            return text if mode == "full" else None

        links: List[Link] = [
            Link(
                rel="self",
                href=plugin_href,
                method="GET",
                title=_title(f"Read {ref_key} at {tier_phrase}"),
            ),
            Link(
                rel="edit",
                href=plugin_href,
                method="PUT",
                title=_title(
                    f"Replace {ref_key} at {tier_phrase} (full body required)"
                ),
            ),
            Link(
                rel="edit",
                href=plugin_href,
                method="DELETE",
                title=_title(
                    f"Clear {ref_key} override at {tier_phrase}; "
                    "falls back to the upstream waterfall"
                ),
            ),
            Link(
                rel="describedby",
                href=registry_href,
                method="GET",
                title=_title(f"Registry entry for {class_key}"),
            ),
        ]
        if mode == "full":
            links.append(Link(
                rel="schema",
                href=f"{registry_href}?meta=schema",
                method="GET",
                title=f"Raw JSON Schema 2020-12 for {class_key}",
            ))
            if engine_ref:
                links.append(Link(
                    rel="engine",
                    href=f"{platform_root}/plugins/{engine_ref}",
                    method="GET",
                    title=f"Platform engine binding for {ref_key}: {engine_ref}",
                ))
        return [lk.model_dump(by_alias=True, exclude_none=True) for lk in links]

    @staticmethod
    def _build_routing_refs(
        by_class: Dict[str, Dict[str, Any]],
        base_url: str,
        *,
        meta_mode: str = "field",
        links_mode: str = "minimal",
    ) -> None:
        """Rewrite ``operations[OP]`` in routing configs as slim ``DriverRef``s.

        Driver → config-class lookup uses the registry directly: post-TypedDriver
        bind, ``class_key()`` for ``XDriverConfig`` returns the snake_case form
        of the bound driver class (e.g. ``"items_postgresql_driver"``) — the
        same string used as ``driver_ref`` in routing entries.  ``driver_ref``
        IS the lookup key into ``list_registered_configs()``.

        Cycle F.7d.3: when the driver_ref binds to a registered config class,
        emit a single HATEOAS ``rel="driver-config"`` link pointing at the
        PATCH endpoint at the active scope (``base_url`` carries the scope
        path).  Composition sub-drivers (no registered config) emit no link
        — operators see only what's actionable.  Replaces the old
        ``config_ref: Optional[str]`` scalar (``null`` was confusing).

        Routing refs honor the same ``meta_mode`` / ``links_mode`` knobs as
        the composed-tree decorator (#946).  When ``meta_mode == "none"`` no
        ``meta`` field is written into each DriverRef; when
        ``links_mode == "none"`` no ``links`` are attached.  Without this
        gate the routing-config leaves leaked ``meta`` and a ``driver-config``
        link even when the caller asked for a clean round-trippable payload
        — every other leaf went through ``_decorate`` (gated), only routing
        refs were ungated.
        """
        all_classes = list_registered_configs()
        scope_label = ConfigApiService._scope_label_from_url(base_url)
        want_meta = meta_mode != "none"
        want_links = links_mode in ("minimal", "full")
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
                    driver_ref = entry.get("driver_ref", "")
                    links: List[Link] = []
                    if want_links and driver_ref in all_classes:
                        links.append(Link(
                            rel="driver-config",
                            href=f"{base_url.rstrip('/')}/plugins/{driver_ref}",
                            method="PUT",
                            title=f"PUT this driver's config at {scope_label} scope",
                        ))
                    entry_meta: Dict[str, Any] = {}
                    if want_meta:
                        entry_meta["tier"] = scope_label
                        src = entry.get("source")
                        if src is not None:
                            entry_meta["source"] = src
                    dumped = DriverRef(
                        driver_ref=driver_ref,
                        hints=[str(h) for h in (entry.get("hints") or [])],
                        on_failure=entry.get("on_failure", "fatal"),
                        write_mode=entry.get("write_mode", "sync"),
                        input_transformers=[
                            str(t) for t in (entry.get("input_transformers") or ())
                        ],
                        output_transformers=[
                            str(t) for t in (entry.get("output_transformers") or ())
                        ],
                        meta=entry_meta,
                        links=links,
                    ).model_dump(by_alias=True, exclude_none=True)
                    # #946: drop empty envelopes so the routing payload
                    # matches the rest of the tree under ``meta=none`` /
                    # ``links=none`` — no ``_meta: {}`` or ``_links: []``
                    # left behind for clients to filter.
                    if not dumped.get("_meta"):
                        dumped.pop("_meta", None)
                    if not dumped.get("_links"):
                        dumped.pop("_links", None)
                    # #1016: drop empty transformer arrays and empty
                    # ``hints`` on egress.  Empty list == none configured
                    # (Pydantic default), so the field carries zero signal
                    # in that state.  Round-trips remain safe because
                    # ``OperationDriverEntry``'s defaults are the empty
                    # tuple / empty frozenset — PUT bodies that omit the
                    # keys rehydrate to the same state on the server side.
                    if not dumped.get("input_transformers"):
                        dumped.pop("input_transformers", None)
                    if not dumped.get("output_transformers"):
                        dumped.pop("output_transformers", None)
                    if not dumped.get("hints"):
                        dumped.pop("hints", None)
                    refs.append(dumped)
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
    def _query_param_schema() -> Dict[str, Any]:
        """JSON Schema 2020-12 describing the composed-config query params.

        Used as ``hrefSchema`` on the ``self`` link so operators discover
        supported query parameters with descriptions and examples without
        scanning the OpenAPI document.  Sourced from
        ``_composed_query_params.QUERY_PARAM_SCHEMA`` so the description /
        enum / default text is single-sourced with the FastAPI handler
        signatures.
        """
        from dynastore.extensions.configs._composed_query_params import (
            QUERY_PARAM_SCHEMA,
        )

        return QUERY_PARAM_SCHEMA

    async def compose_collection_config(
        self,
        base_url: str,
        catalog_id: str,
        collection_id: str,
        resolved: bool = True,
        meta: str = "field",
        include: str = "scope",
        strict: bool = True,
        links: str = "minimal",
        view: str = "effective",
    ) -> CollectionConfigResponse:
        by_class, sources, _tier_data = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=collection_id, resolved=resolved,
        )
        extra_refs = await self._get_extra_refs(
            catalog_id=catalog_id, collection_id=collection_id,
        )
        self._build_routing_refs(
            by_class, base_url, meta_mode=meta, links_mode=links,
        )
        configs_root_url = self._configs_root_url(base_url)
        tree = self._compose_tree(
            by_class, sources, "collection",
            meta_mode=meta, include_mode=include, strict=strict,
            extra_refs=extra_refs,
            view_mode=view,
            links_mode=links, base_url=base_url,
            configs_root_url=configs_root_url,
            catalog_id=catalog_id, collection_id=collection_id,
        )
        return CollectionConfigResponse(
            collection_id=collection_id, catalog_id=catalog_id,
            configs=tree,
        )

    async def compose_catalog_config(
        self,
        base_url: str,
        catalog_id: str,
        resolved: bool = True,
        meta: str = "field",
        include: str = "scope",
        strict: bool = True,
        links: str = "minimal",
        view: str = "effective",
    ) -> CatalogConfigResponse:
        by_class, sources, _tier_data = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=None, resolved=resolved,
        )
        extra_refs = await self._get_extra_refs(
            catalog_id=catalog_id, collection_id=None,
        )
        self._build_routing_refs(
            by_class, base_url, meta_mode=meta, links_mode=links,
        )
        configs_root_url = self._configs_root_url(base_url)
        tree = self._compose_tree(
            by_class, sources, "catalog",
            meta_mode=meta, include_mode=include, strict=strict,
            extra_refs=extra_refs,
            view_mode=view,
            links_mode=links, base_url=base_url,
            configs_root_url=configs_root_url,
            catalog_id=catalog_id, collection_id=None,
        )
        return CatalogConfigResponse(
            catalog_id=catalog_id,
            configs=tree,
        )

    async def compose_platform_config(
        self,
        base_url: str,
        resolved: bool = True,
        meta: str = "field",
        include: str = "scope",
        strict: bool = True,
        links: str = "minimal",
        view: str = "effective",
    ) -> PlatformConfigResponse:
        by_class, sources, _tier_data = await self._get_effective_configs(
            catalog_id=None, collection_id=None, resolved=resolved,
        )
        extra_refs = await self._get_extra_refs(
            catalog_id=None, collection_id=None,
        )
        self._build_routing_refs(
            by_class, base_url, meta_mode=meta, links_mode=links,
        )
        configs_root_url = self._configs_root_url(base_url)
        tree = self._compose_tree(
            by_class, sources, "platform",
            meta_mode=meta, include_mode=include, strict=strict,
            extra_refs=extra_refs,
            view_mode=view,
            links_mode=links, base_url=base_url,
            configs_root_url=configs_root_url,
            catalog_id=None, collection_id=None,
        )
        return PlatformConfigResponse(
            scope="platform", configs=tree,
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
        key is either a ``class_key`` (single-instance, class-as-identity path)
        or a multi-instance ``ref_key`` (Cycle F.4d.2).  Value is the new
        payload, or ``null`` to delete the override.  Atomic at the scope
        level — the validation pass runs to completion before any write fires.

        Multi-instance ref entries (key not in
        :func:`list_registered_configs`) require a ``class_key`` (or
        ``driver_class``) discriminator inside the body so the composer can
        resolve the dispatch class and validate the payload.  An unregistered
        key whose body carries *no* discriminator is treated as an unknown
        single-instance config class (404 "Unknown config class"), not as a
        ref-create attempt — only a body with a discriminator is interpreted
        as a multi-instance ref-create.  Deletes (``value is None``) for an
        unknown key are allowed without a discriminator — the F.4c.4
        ``delete_config_by_ref`` returns False for no-op without surfacing an
        error.

        Set-by-ref dispatches to :meth:`ConfigsProtocol.set_config_by_ref`
        (refusing to overwrite a stored row whose ``class_key`` differs
        from the discriminator).  Class-keyed entries keep the existing
        :meth:`ConfigsProtocol.set_config` path so single-instance writes
        retain their cache invalidation + apply-handler shape.
        """
        all_classes = list_registered_configs()
        # Prepared per-entry: (key, cls_or_None, merged_or_None, is_ref).
        # ``is_ref`` selects the by-ref dispatch on the write phase.
        prepared: List[
            Tuple[str, Optional[Type[PluginConfig]], Optional[Dict[str, Any]], bool]
        ] = []

        for plugin_id, value in body.items():
            cls = all_classes.get(plugin_id)
            if cls is not None:
                # Class-keyed path (existing semantics).
                if value is None:
                    prepared.append((plugin_id, cls, None, False))
                    continue
                # #946: strip response-only envelopes so payloads fetched
                # via GET (with any ``meta`` / ``links`` mode) round-trip
                # cleanly into PATCH.  Without this, ``extra="forbid"`` on
                # ``PersistentModel`` (#918) rejects ``_meta`` / ``_links``
                # with a 422.
                value = {
                    k: v for k, v in value.items()
                    if k not in ("_meta", "_links")
                }
                current = (await self._config_service.get_persisted_config(
                    cls, catalog_id=catalog_id, collection_id=collection_id,
                )) or {}
                merged = {**current, **value}
                cls.model_validate(merged)  # raises on bad data
                prepared.append((plugin_id, cls, merged, False))
                continue

            # Multi-instance ref path (F.4d.2).  ``plugin_id`` is a ref_key.
            if value is None:
                # Delete an unknown ref — defer existence check to the
                # service-layer delete which returns False for no-op.
                prepared.append((plugin_id, None, None, True))
                continue
            # Body must carry the dispatch discriminator.  Accept either
            # ``class_key`` or ``driver_class`` (Cycle F.2 alias for the
            # driver subset of PluginConfigs); strip from the merged
            # payload so it doesn't reach Pydantic validation.  Also strip
            # response-only envelopes (#946) for the same reason as the
            # class-keyed path above.
            value = {
                k: v for k, v in dict(value).items()
                if k not in ("_meta", "_links")
            }
            class_key = value.pop("class_key", None) or value.pop("driver_class", None)
            if not class_key:
                # Without a ``class_key``/``driver_class`` discriminator the
                # body is not a multi-instance ref-create attempt, so the
                # unregistered key is simply an unknown single-instance config
                # class (e.g. a typo) — surface it as a clean 404 rather than
                # implying the caller meant to create a ref.  A genuine
                # ref-create (discriminator present) falls through below.
                raise ValueError(
                    f"Unknown config class '{plugin_id}'. It is neither a "
                    f"registered single-instance config nor a multi-instance "
                    f"ref-create (add a 'class_key' or 'driver_class' "
                    f"discriminator to create a multi-instance row). "
                    f"Existing config classes are: {sorted(all_classes)[:5]}..."
                )
            cls = all_classes.get(class_key)
            if cls is None:
                raise ValueError(
                    f"ref '{plugin_id}': body discriminator class_key="
                    f"{class_key!r} is not a registered config class."
                )
            # Try to merge against an existing row at this ref (F.4c.2
            # ``get_config_by_ref``).  Service that doesn't implement the
            # F.4c read API → treat as a fresh write (no merge base).
            get_by_ref = getattr(self._config_service, "get_config_by_ref", None)
            current_dict: Dict[str, Any] = {}
            if get_by_ref is not None:
                existing = await get_by_ref(
                    plugin_id, catalog_id=catalog_id, collection_id=collection_id,
                )
                if existing is not None:
                    try:
                        current_dict = existing.model_dump(exclude_unset=True)
                    except Exception:
                        current_dict = {}
            merged = {**current_dict, **value}
            cls.model_validate(merged)  # raises on bad data
            prepared.append((plugin_id, cls, merged, True))

        for plugin_id, cls, merged, is_ref in prepared:
            if is_ref:
                if merged is None:
                    deleter = getattr(
                        self._config_service, "delete_config_by_ref", None,
                    )
                    if deleter is not None:
                        await deleter(
                            plugin_id, catalog_id=catalog_id,
                            collection_id=collection_id,
                        )
                else:
                    assert cls is not None  # set guarded above
                    setter = getattr(
                        self._config_service, "set_config_by_ref", None,
                    )
                    if setter is None:
                        raise RuntimeError(
                            "config service lacks set_config_by_ref; "
                            "F.4c.4 service-layer write API not available"
                        )
                    validated = cls.model_validate(merged)
                    await setter(
                        plugin_id, validated, catalog_id=catalog_id,
                        collection_id=collection_id,
                    )
                continue

            assert cls is not None  # class-keyed path always carries cls
            if merged is None:
                await self._config_service.delete_config(
                    cls, catalog_id=catalog_id, collection_id=collection_id,
                )
            else:
                validated = cls.model_validate(merged)
                await self._config_service.set_config(
                    cls, validated, catalog_id=catalog_id, collection_id=collection_id,
                )
        return {"updated": [p for p, _, _, _ in prepared]}

    # --- Pagination helpers. ---

    # NOTE: Cycle C retired ``_build_config_page``, ``_list_assets``,
    # ``_list_collections``, and the entire ``categories`` /
    # ``ConfigPage`` paginated-children machinery (~150 lines).
    # Operators discover children via the existing list endpoints
    # (``GET /catalogs``, ``GET /catalogs/{cat}/collections``,
    # ``GET .../assets``).  The composed-config response is now scoped
    # to a single tier — siblings/children are discovered separately.
