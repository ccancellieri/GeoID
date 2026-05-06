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
land at if rendered.  The ``meta`` field carries field-level docs or
full JSON Schema per class, also hierarchical and mirroring the
``configs`` tree shape; mode is selected via ``?meta=none|field|schema``
(default ``field``).
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, RootModel


class DriverRef(BaseModel):
    """Slim reference to a driver configured under a routing operation.

    ``config_ref`` is the class name the UI follows to look up the
    driver's full config under the sibling ``storage.drivers.*`` topic
    in the same response — avoiding per-routing-entry payload
    duplication.
    """

    driver_id: str = Field(
        ..., description="Driver class name, e.g. 'ItemsPostgresqlDriver'."
    )
    config_ref: Optional[str] = Field(
        None,
        description=(
            "Class name of the driver's config (sibling lookup under "
            "configs.storage.drivers.*).  Null when the driver has no "
            "registered config."
        ),
    )
    on_failure: str = Field(
        "fatal", description="Failure policy: fatal | warn | ignore."
    )
    write_mode: str = Field("sync", description="Write mode: sync | async.")


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
# information now lives in the top-level ``inherited`` map.  ``entity``
# (the items/assets/collection bucket) was dropped along with the
# composer's ``_build_meta_entry`` helper.
#
# The ``meta`` response field now carries field-level docs OR full JSON
# Schema per class, hierarchical to mirror the ``configs`` tree shape.
# Each leaf is a plain dict (``{field_docs: {...}}`` or
# ``{json_schema: {...}}``), so ``meta: Optional[Dict[str, Any]]`` on
# the response models suffices — no per-class DTO needed.


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
            "JSON Hyper-Schema link descriptors for this resource. Always "
            "populated. Includes ``self``, ``alternate`` representations "
            "(other ``?meta=`` modes), and ``edit`` (templated "
            "PATCH against per-class plugin endpoints). Operators read "
            "``hrefSchema`` on each link to discover supported query "
            "parameters with descriptions and examples."
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
            "forks at each tier).  Each leaf is "
            "``{class_key: payload}``."
        ),
    )
    meta: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Per-class field-level docs OR full JSON Schema, hierarchical "
            "and mirroring the ``configs`` tree shape — the same path that "
            "produces the resolved payload in ``configs`` produces a "
            "``{field_docs: {...}}`` (default ``?meta=field``) or "
            "``{json_schema: {...}}`` (when ``?meta=schema``) leaf in "
            "``meta``.  Set ``?meta=none`` to suppress entirely.  The "
            "older waterfall trace (``source`` + ``layers``) was retired in "
            "Cycle B — tier-of-origin breadcrumbs live in ``inherited``."
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
            "tier-first tree (platform/catalog + assets fork)."
        ),
    )
    meta: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Per-class field-level docs OR full JSON Schema, hierarchical "
            "and mirroring the ``configs`` tree shape — the same path that "
            "produces the resolved payload in ``configs`` produces a "
            "``{field_docs: {...}}`` (default ``?meta=field``) or "
            "``{json_schema: {...}}`` (when ``?meta=schema``) leaf in "
            "``meta``.  Set ``?meta=none`` to suppress entirely.  The "
            "older waterfall trace (``source`` + ``layers``) was retired in "
            "Cycle B — tier-of-origin breadcrumbs live in ``inherited``."
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
            "scaffold rendered when waterfall-resolved)."
        ),
    )
    meta: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Per-class field-level docs OR full JSON Schema, hierarchical "
            "and mirroring the ``configs`` tree shape — the same path that "
            "produces the resolved payload in ``configs`` produces a "
            "``{field_docs: {...}}`` (default ``?meta=field``) or "
            "``{json_schema: {...}}`` (when ``?meta=schema``) leaf in "
            "``meta``.  Set ``?meta=none`` to suppress entirely.  The "
            "older waterfall trace (``source`` + ``layers``) was retired in "
            "Cycle B — tier-of-origin breadcrumbs live in ``inherited``."
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
