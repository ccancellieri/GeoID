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

Provenance + HATEOAS edit affordances live INLINE on each plugin leaf:

- ``_meta`` — always carries ``{"tier": <active_scope>, "source":
  <originating_tier>}``.  ``?meta=field`` (default) adds ``"docs":
  {field: description}``; ``?meta=schema`` adds ``"json_schema":
  {...}``; ``?meta=none`` strips the extras but keeps ``tier``+
  ``source`` (per #665 slice 3 — provenance is structural).
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


# NOTE: ConfigLayer / ConfigMeta (Cycle B, 2026-05-05) and the top-level
# parallel ``meta`` tree (#517) were retired upstream.  The parallel
# ``inherited`` tree was retired in #665 slice 3 — tier-of-origin lives
# inline on each leaf's ``_meta.source``.  Per-class field docs and JSON
# Schema also live INLINE as ``_meta`` siblings on each plugin leaf,
# alongside the ``_links`` array.  Single source of truth, no
# cross-walk required when staring at a leaf.


# NOTE: ``ConfigPage`` (paginated child resources) was retired in
# Cycle C alongside the ``categories`` field on every response.
# Operators discover children via the existing list endpoints
# (``GET /catalogs``, ``GET /catalogs/{cat}/collections``,
# ``GET .../assets``).  The composed-config response is now scoped to a
# single tier — siblings/children are discovered separately.


class CollectionConfigResponse(BaseModel):
    """Composed view of all effective configs at a single collection scope."""

    collection_id: str
    catalog_id: str
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective configs at this collection scope, nested as a "
            "tier-first tree (platform/catalog/collection/items + assets "
            "forks at each tier).  Each leaf is the raw plugin payload "
            "keyed by ``class_key``.  Every leaf carries a ``_meta`` "
            "sibling with ``{tier, source}``; ``?meta=field`` (default) "
            "adds ``docs`` and ``?meta=schema`` adds ``json_schema``.  "
            "When ``?links=minimal|full`` is set, each leaf also carries "
            "a ``_links`` sibling — a HATEOAS array with ``self`` (GET), "
            "``edit`` (PUT — replace), ``edit`` (DELETE — clear override) "
            "and ``describedby`` (GET registry/{class_key}).  "
            "``?links=full`` additionally adds ``rel=schema`` and "
            "(when ``engine_ref`` is bound) ``rel=engine``."
        ),
    )


class CatalogConfigResponse(BaseModel):
    """Composed view of all effective configs at a single catalog scope."""

    catalog_id: str
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
