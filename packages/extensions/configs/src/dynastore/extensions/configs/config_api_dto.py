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

"""DTOs for the centralised composed-config API.

The response is a tier-first tree (platform/catalog/collection/items
with assets forks at each tier) where the leaves are raw PluginConfig
payloads keyed by class_key.  No wrapper envelopes, no duplicated
driver configs inline under routing entries, no ``class_key`` field
(the map key IS the class_key).

Tier-of-origin information lives in the top-level ``inherited`` tree,
which mirrors the ``configs`` shape — each leaf carries
``{"source": <tier>}`` at the same address the resolved value would
land at if rendered.

Field-level docs and per-class HATEOAS edit affordances live INLINE on
each plugin leaf:

- ``_meta`` — ``{"docs": {field: description}}`` (default
  ``?meta=field``) or ``{"json_schema": {...}}`` (``?meta=schema``).
  Suppressed entirely with ``?meta=none``.
- ``_links`` — per-leaf HATEOAS edit affordances pointing at the active
  scope's per-plugin CRUD endpoint and at the registry's JSON Schema
  entry.  Emitted by default (``?links=minimal``); opt-out with
  ``?links=none``; opt-in to verbose ``?links=full``.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, RootModel


class DriverRef(BaseModel):
    """Slim reference to a driver configured under a routing operation.

    Cycle F.7d.3 replaced the ``config_ref: Optional[str]`` scalar with
    a HATEOAS ``_links`` array.  When the driver class binds to a
    registered config, a single ``rel="driver-config"`` link points at
    the PATCHable endpoint under ``configs.platform.catalog.{tier}.drivers.*``.
    Composition sub-drivers (no registered config) emit no link at all
    — operators see only what's actionable.
    """

    driver_ref: str = Field(
        ..., description="Driver reference (snake_case, e.g. 'items_postgresql_driver')."
    )
    hints: List[str] = Field(
        default_factory=list,
        description=(
            "Selectivity tags from the routing entry's ``hints`` set "
            "(e.g. ``geometry_simplified``, ``geometry_exact``).  Empty "
            "list means the entry responds to every hint the driver "
            "class supports.  Lets operators distinguish multiple entries "
            "that share the same ``driver_ref`` under one operation."
        ),
    )
    on_failure: str = Field(
        "fatal",
        description=(
            "Failure policy: ``fatal`` (raise — default), ``outbox`` "
            "(defer to drain task; standard for ES INDEX), ``warn`` "
            "(log + continue), ``ignore``."
        ),
    )
    write_mode: str = Field("sync", description="Write mode: sync | async.")
    input_transformers: List[str] = Field(
        default_factory=list,
        description=(
            "Ordered transformer ``driver_ref``s applied to entities going "
            "INTO this driver call. Wired hops in this release: ``INDEX``. "
            "Declaring this on other operations emits a one-time WARN at "
            "load time. Mirrors ``OperationDriverEntry.input_transformers`` "
            "(#501); accepted on PUT/PATCH request bodies."
        ),
    )
    output_transformers: List[str] = Field(
        default_factory=list,
        description=(
            "Ordered transformer ``driver_ref``s applied to entities coming "
            "OUT of this driver call. Wired hops in this release: ``SEARCH``. "
            "Mirrors ``OperationDriverEntry.output_transformers`` (#501)."
        ),
    )
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        serialization_alias="_meta",
        description=(
            "Response-only provenance block, mirroring the post-#518 inline "
            "``_meta`` pattern used on plugin leaves.  Carries ``source`` "
            "(``operator`` | ``auto``) and ``tier`` (``platform`` | "
            "``catalog`` | ``collection``) of the active composed scope.  "
            "Not accepted on PUT/PATCH request bodies — strip before round-tripping."
        ),
    )
    links: List["Link"] = Field(
        default_factory=list,
        serialization_alias="_links",
        description=(
            "HATEOAS links for this routing entry.  Cycle F.7d.3: when the "
            "driver_ref binds to a registered config class, a single "
            "``rel=\"driver-config\"`` link points at the PATCHable "
            "endpoint.  Composition sub-drivers emit no link."
        ),
    )


class Link(BaseModel):
    """A JSON Hyper-Schema (draft 2019-09) link descriptor.

    Surfaces hypermedia at the response root so operators can discover
    related endpoints, alternate representations of the same resource,
    and edit affordances without consulting OpenAPI separately.

    See https://json-schema.org/draft/2019-09/json-schema-hypermedia.html
    """

    rel: str = Field(
        ...,
        description=(
            "Standard relation name per IANA Link Relations registry "
            "(``self`` | ``alternate`` | ``edit`` | ``related`` | ``next`` "
            "| ``prev`` | ``successor-version`` | ``documentation``)."
        ),
    )
    href: str = Field(
        ...,
        description=(
            "Target URI. May be a URI Template (RFC 6570) when "
            "``templated`` is true — e.g. ``/configs/catalogs/{cat}/"
            "collections/{coll}/plugins/{class_key}``."
        ),
    )
    title: Optional[str] = Field(
        None, description="Human-readable label for the link target."
    )
    method: Optional[str] = Field(
        None,
        description=(
            "HTTP method used to follow the link (``GET`` | ``PATCH`` | "
            "``PUT`` | ``DELETE``). Defaults to ``GET`` when omitted. "
            "Strict JSON Hyper-Schema does not include ``method``; widely "
            "accepted HAL-style extension carried here for operator clarity."
        ),
    )
    templated: bool = Field(
        default=False,
        description=(
            "When true, ``href`` is a URI Template per RFC 6570 (placeholders "
            "in ``{...}`` MUST be expanded by the client before dereferencing)."
        ),
    )
    hrefSchema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "JSON Schema 2020-12 describing query-string parameters accepted "
            "by this link. Each property carries ``description`` and "
            "``examples``. Drives self-documenting parameter discovery — "
            "operators read the schema to learn what query params do and what "
            "values they take, without scanning OpenAPI separately."
        ),
    )


# NOTE: ConfigLayer and ConfigMeta were retired in Cycle B of the
# config-API restructure (2026-05-05).  The waterfall-trace meta
# (``source`` + ``layers``) was dropped — operators didn't find the
# per-tier presence breadcrumb interesting; the tier-of-origin
# information now lives in the top-level ``inherited`` map.
#
# The top-level ``meta`` response field (a parallel tree mirroring
# ``configs``) was retired in #517: per-class field docs and JSON
# Schema now live INLINE as ``_meta`` siblings on each plugin leaf,
# alongside the new ``_links`` array.  Single source of truth, no
# cross-walk required when staring at a leaf.


# NOTE: ``ConfigPage`` (paginated child resources) was retired in
# Cycle C alongside the ``categories`` field on every response.
# Operators discover children via the existing list endpoints
# (``GET /catalogs``, ``GET /catalogs/{cat}/collections``,
# ``GET .../assets``).  The composed-config response is now scoped to a
# single tier — siblings/children are discovered separately.


class CollectionConfigResponse(BaseModel):
    """Composed view of all effective configs at a single collection scope."""

    links: List[Link] = Field(
        default_factory=list,
        serialization_alias="_links",
        description=(
            "Response-level JSON Hyper-Schema links.  Holds only ``self`` "
            "(carries ``hrefSchema`` advertising supported query params).  "
            "Per-plugin edit affordances live inline as ``_links`` on each "
            "leaf when ``?links=minimal|full`` (see ``configs`` description)."
        ),
    )
    collection_id: str
    catalog_id: str
    inherited: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Hierarchical breadcrumb tree mirroring the ``configs`` shape "
            "for configs that resolved at this scope but are NOT rendered "
            "in ``configs`` (default ``?include=scope`` mode). Each leaf "
            "carries ``{\"source\": <tier>}`` — ``platform`` / ``catalog`` / "
            "``default`` — at the same path the resolved value would land "
            "at if it were inlined. Operators see WHICH upstream-tier "
            "configs influence this collection AND at which natural "
            "address, without the full payloads flooding the body. Set "
            "``?include=upstream`` to render the resolved bodies inline "
            "in ``configs`` (today's verbose mode); ``inherited`` is "
            "``null`` in that mode."
        ),
    )
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective configs at this collection scope, nested as a "
            "tier-first tree (platform/catalog/collection/items + assets "
            "forks at each tier).  Each leaf is the raw plugin payload "
            "keyed by ``class_key``.  When ``?meta=field|schema`` is set "
            "(default ``field``), each leaf carries a ``_meta`` sibling "
            "(``{docs: {...}}`` or ``{json_schema: {...}}``).  "
            "When ``?links=minimal|full`` is set, each leaf also carries "
            "a ``_links`` sibling — a HATEOAS array with ``self`` (GET), "
            "``edit`` (PUT — replace), ``edit`` (DELETE — clear override) "
            "and ``describedby`` (GET registry/{class_key}).  "
            "``?links=full`` adds contextual ``title`` per link naming "
            "the class_key and scope."
        ),
    )


class CatalogConfigResponse(BaseModel):
    """Composed view of all effective configs at a single catalog scope."""

    links: List[Link] = Field(
        default_factory=list,
        serialization_alias="_links",
        description=(
            "JSON Hyper-Schema link descriptors. See "
            "``CollectionConfigResponse._links`` for the link semantics."
        ),
    )
    catalog_id: str
    inherited: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Hierarchical breadcrumb tree mirroring the ``configs`` shape "
            "for configs that resolved at this scope but are NOT rendered "
            "in ``configs`` (default ``?include=scope`` mode). See "
            "``CollectionConfigResponse.inherited`` for full semantics."
        ),
    )
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective configs at this catalog scope, nested as a "
            "tier-first tree (platform/catalog + assets fork).  See "
            "``CollectionConfigResponse.configs`` for ``_meta`` / "
            "``_links`` per-leaf semantics."
        ),
    )


class PlatformConfigResponse(BaseModel):
    """Composed view of all effective configs at the platform scope."""

    links: List[Link] = Field(
        default_factory=list,
        serialization_alias="_links",
        description=(
            "JSON Hyper-Schema link descriptors. See "
            "``CollectionConfigResponse._links`` for the link semantics."
        ),
    )
    scope: str = Field("platform", frozen=True)
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective platform-level configs, nested as a tier-first "
            "tree (platform tier with the catalog/collection/items "
            "scaffold rendered when waterfall-resolved).  See "
            "``CollectionConfigResponse.configs`` for ``_meta`` / "
            "``_links`` per-leaf semantics."
        ),
    )
    inherited: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Hierarchical breadcrumb tree mirroring the ``configs`` shape "
            "for catalog-/collection-tier templates filtered out under "
            "``?strict=true`` (Cycle F.7d.2).  None when no entries were "
            "filtered (e.g. ``?strict=false`` restores the previous "
            "always-true platform-scope inclusion).  Each leaf carries "
            "``{\"source\": <tier>}`` indicating where the resolved value "
            "comes from."
        ),
    )
class PatchConfigBody(RootModel[Dict[str, Optional[Dict[str, Any]]]]):
    """Partial composed configuration.

    Keys: class name.  Values:

    * non-null dict — partial merge into the stored config at this scope.
    * null          — delete the stored record at this scope (revert to
                      inherit / class default).
    """
    pass


# Resolve forward reference DriverRef.links: List["Link"] now that Link
# is defined.  Cycle F.7d.3 introduced this self-reference to surface
# the routing-entry HATEOAS link.
DriverRef.model_rebuild()
