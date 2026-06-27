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

"""Generalised Preset protocol and supporting types.

A ``Preset`` is a named, parameterised, reversible platform action. It can
grant IAM policies, write config, seed data, spawn tasks, or register cron
jobs — anything reachable through the ``PresetContext``.

Key types:

* ``Preset``          — structural protocol every preset must satisfy.
* ``CompositePreset`` — base class for presets that delegate to child presets.
* ``PresetContext``   — the narrow surface a preset is allowed to touch.
* ``AppliedDescriptor`` — opaque JSON payload stored in the audit row; used
  by ``revoke`` to undo exactly what ``apply`` introduced.
* ``PresetPlan``      — dry-run output describing what ``apply`` would do.
* ``NoParams``        — default empty Pydantic params model.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Optional,
    Protocol,
    Tuple,
    Type,
    runtime_checkable,
)

from pydantic import BaseModel

from .examples import PresetExample
from .protocol import PresetTier

if TYPE_CHECKING:
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig


# ---------------------------------------------------------------------------
# Parameter model default
# ---------------------------------------------------------------------------

class NoParams(BaseModel):
    """Empty Pydantic model used as the default ``params_model`` for presets
    that accept no structured parameters."""


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AppliedDescriptor:
    """Opaque payload produced by a synchronous ``apply`` call.

    Stored verbatim in ``iam.applied_presets.revoke_descriptor``; handed
    back to ``revoke`` to undo exactly what ``apply`` introduced.
    ``payload`` is arbitrary JSON-serialisable data — the shape is private
    to each preset implementation.
    """

    payload: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return self.payload

    @classmethod
    def from_json(cls, data: Optional[Dict[str, Any]]) -> "AppliedDescriptor":
        return cls(payload=data or {})


@dataclass(frozen=True)
class DataSeed:
    """One unit of seed data a *data contributor* asks a preset to apply.

    A data contributor exposes ``get_data() -> Iterable[DataSeed]`` (see
    ``MultiContributorPreset``). At apply time the preset ensures the catalog
    and collection exist — creating each only if absent — then upserts
    ``items``. ``revoke`` removes exactly what apply created: items first, then
    the collection, then the catalog, but only the ones this preset itself
    created. A catalog or collection that pre-existed (``manage_catalog`` /
    ``manage_collection`` resolved to a no-op because it was already there), or
    that is explicitly flagged shared, is never deleted on revoke — only the
    items this preset upserted are pulled back out. This mirrors the
    "never delete what the operator owns" rule the routing and policy
    contributors already follow.

    ``catalog_data`` / ``collection_data`` are the create payloads handed to
    ``CatalogsProtocol.create_catalog`` / ``create_collection``; they are
    ignored when the target already exists.
    """

    catalog_id: str
    collection_id: str
    catalog_data: Dict[str, Any] = field(default_factory=dict)
    collection_data: Dict[str, Any] = field(default_factory=dict)
    items: Tuple[Dict[str, Any], ...] = ()
    # When False, revoke leaves a shared/operator-owned catalog or collection
    # in place and only removes the items this preset upserted.
    manage_catalog: bool = True
    manage_collection: bool = True
    lang: str = "*"
    # Optional per-collection routing applied at the seed's own
    # catalog/collection scope before the items are upserted. A seed that needs
    # its items to be searchable (e.g. an Elasticsearch secondary index) declares
    # it here so the collection is searchable *by construction* — the
    # MultiContributorPreset otherwise applies at platform scope and cannot infer
    # per-collection routing. When None the collection inherits the ambient
    # catalog/platform routing (#1285 / #2241).
    items_routing: Optional["ItemsRoutingConfig"] = None
    # When True the catalog is created with Hint.DEFER so its deferrable
    # storage-backend (GCP bucket/eventing) provisioning is held back: a
    # records/feature-only seed catalog reaches ``ready`` bucket-free. Storage,
    # if ever needed, is provisioned later via an explicit catalog_provision
    # task. Defaults to False (catalog provisions normally at creation).
    defer_provisioning: bool = False


@dataclass(frozen=True)
class TaskSeed:
    """One background job a *task contributor* asks a preset to trigger.

    A task contributor exposes ``get_tasks() -> Iterable[TaskSeed]`` (see
    ``MultiContributorPreset``). At apply time — after every synchronous
    contribution (policies, configs, seed data) is in place — the preset
    submits each ``process_id`` to the OGC Process execution engine and records
    the returned job id in its ``AppliedDescriptor`` so callers can poll the
    job's status.

    This is how a *synchronous* preset delegates heavy work to the *existing*
    async OGC Process machinery without itself becoming an async preset: it
    kicks the job off, records the reference, and returns ``applied``. The job
    runs in the background with its own status lifecycle, queryable through the
    normal task-status endpoint.

    ``revoke`` does NOT cancel or undo a triggered job — a job that has already
    run cannot be un-run. Any data the job produced is governed by the
    data-seed revoke rules (a collection that still holds items is never
    deleted; see ``DataSeed``).

    ``async_mode`` picks ``ASYNC_EXECUTE`` (default) vs ``SYNC_EXECUTE`` as the
    preferred execution mode. ``dedup_key`` collapses redelivered triggers onto
    a single in-flight job (the execution engine returns no new job on a hit).
    """

    process_id: str
    inputs: Dict[str, Any] = field(default_factory=dict)
    async_mode: bool = True
    dedup_key: Optional[str] = None


# ---------------------------------------------------------------------------
# Dry-run plan
# ---------------------------------------------------------------------------

@dataclass
class PresetPlanEntry:
    """One proposed change in a ``PresetPlan``."""

    kind: str          # e.g. "create_policy", "delete_role", "set_config"
    target: str        # human-readable identity of the thing being changed
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PresetPlan:
    """Dry-run output — what ``apply`` would do without any writes."""

    preset_name: str
    scope_key: str
    entries: Tuple[PresetPlanEntry, ...] = field(default_factory=tuple)
    warnings: Tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Preset context
# ---------------------------------------------------------------------------

@dataclass
class PresetContext:
    """Narrow surface a preset is allowed to touch.

    All fields are injected by the lifecycle layer; presets receive this
    context on every ``dry_run`` / ``apply`` / ``revoke`` call.

    In unit tests the fields can be mocked freely — no preset should reach
    into globals.
    """

    # DB engine reference — use with managed_transaction / DDLQuery / DQLQuery.
    db: Any  # AsyncEngine or connection-compatible resource

    # IamService — roles + hierarchy.
    iam: Any  # IamService

    # PolicyService — policy CRUD.
    policy: Any  # PolicyService

    # ConfigsProtocol reader + writer.
    config: Any  # ConfigsProtocol

    # Task creation wrapper — auto-fills caller_id.
    tasks: Any  # _TaskCtx (see below)

    # Cron helpers — require an active conn from ctx.db.
    cron: Any  # _CronCtx (see below)

    # Curated library handles (opt-in per preset; not used by core IAM presets).
    libs: Any  # Optional dict / namespace of library handles

    # The caller's principal — used for audit and the self-lockout check.
    principal: Any  # Principal or None for system calls

    # Resolved scope string, e.g. "platform", "catalog:cat-7",
    # "catalog:cat-7/collection:coll-3".
    scope: str

    # CatalogsProtocol — the data-contributor surface (seed catalogs /
    # collections / items). Optional with a ``None`` default so the dozens of
    # existing positional/keyword constructors (mostly IAM unit tests) keep
    # working unchanged; only ``_build_context`` and data-seeding presets pass
    # it. Dataclass default-ordering forces a defaulted field to the end, which
    # is why this sits after ``scope`` rather than next to ``config``.
    catalogs: Any = None  # CatalogsProtocol or None


# ---------------------------------------------------------------------------
# Preset protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class Preset(Protocol):
    """Structural protocol every generalised preset must satisfy.

    Class variables are the static metadata; instance methods perform the
    work. All methods are async to allow DB I/O inside them without thread
    pinning.

    ``"iam" in keywords`` triggers the self-lockout guard on DELETE. No
    separate flag is needed — keywords are the classifier.
    """

    name: ClassVar[str]
    description: ClassVar[str]
    keywords: ClassVar[Tuple[str, ...]]
    tier: ClassVar[PresetTier]
    catalog_scopable: ClassVar[bool]
    params_model: ClassVar[Type[BaseModel]]

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan: ...

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor: ...

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None: ...


# ---------------------------------------------------------------------------
# Composite preset
# ---------------------------------------------------------------------------

class CompositePreset:
    """Base class for presets that delegate to child presets.

    Subclasses declare ``compose`` with an ordered tuple of child preset
    names. The base implementation:

    * ``apply``:  iterates children forward; rolls back prior children on
      sync failure.
    * ``revoke``: iterates children in reverse.
    * ``dry_run``: collects child plans without writing.

    ``compose`` references are validated at registration time by the
    registry — an unknown child name raises ``ValueError``.
    """

    name: ClassVar[str]
    description: ClassVar[str]
    keywords: ClassVar[Tuple[str, ...]] = ()
    tier: ClassVar[PresetTier]
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    examples: ClassVar[Tuple[PresetExample, ...]] = ()
    compose: ClassVar[Tuple[str, ...]]

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        from .registry import find_preset

        all_entries: list[PresetPlanEntry] = []
        warnings: list[str] = []

        for child_name in self.compose:
            child = find_preset(child_name)
            child_plan = await child.dry_run(params, scope, ctx)
            all_entries.extend(child_plan.entries)
            warnings.extend(child_plan.warnings)

        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(all_entries),
            warnings=tuple(warnings),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        from .registry import find_preset

        applied_children: list[tuple[str, AppliedDescriptor]] = []

        for child_name in self.compose:
            child = find_preset(child_name)
            try:
                result = await child.apply(params, scope, ctx)
                applied_children.append((child_name, result))
            except Exception as exc:
                # On failure: reverse-rollback all prior children (best-effort).
                for prior_name, prior_result in reversed(applied_children):
                    prior_child = find_preset(prior_name)
                    try:
                        await prior_child.revoke(prior_result, ctx)
                    except Exception:
                        pass  # best-effort
                raise exc

        # Build composite descriptor listing successful children.
        child_pairs = [
            {"name": name, "descriptor": res.payload}
            for name, res in applied_children
        ]
        return AppliedDescriptor(payload={"children": child_pairs, "scope": scope})

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        from .registry import find_preset

        children = applied_descriptor.payload.get("children", [])
        for child_entry in reversed(children):
            child_name = child_entry["name"]
            child_descriptor = AppliedDescriptor(
                payload=child_entry.get("descriptor", {})
            )
            child = find_preset(child_name)
            await child.revoke(child_descriptor, ctx)
        return None
