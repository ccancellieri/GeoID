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

"""Asset-side write-policy types.

Mirrors :mod:`dynastore.modules.storage.driver_config` /
:mod:`dynastore.modules.storage.computed_fields` (items side) so operators can
reason about both surfaces with one mental model.

Two layers live here, exactly like the items side:

**Authoring layer (the wire/config shape).** :class:`AssetDeriveSpec` declares
which asset identity dimensions are *available* (``asset_id`` / ``filename`` /
``url`` / ``content_hash`` self-describing booleans, plus ``metadata_fields`` —
a list of dotted JSON paths into the asset ``metadata`` column, each producing
one named derivation). :class:`AssetIdentityRule` references those declared
dimensions **by name** (``match_on: List[str]``) instead of re-declaring them.
This is what an operator authors and what :class:`AssetsWritePolicy` persists.

**Engine layer (the internal value type).** :class:`AssetIdentityField` is the
flat per-dimension value the chain runner consumes — it carries the concrete
:class:`AssetIdentityKind` plus the optional metadata ``path``. It is produced
from the authored names + :class:`AssetDeriveSpec` via
:meth:`AssetsWritePolicy.resolved_identity`; nothing outside this bridge needs
to know the authoring buckets exist.

Public surface:

* :class:`AssetWriteConflictPolicy` — per-asset conflict action enum.
* :class:`AssetIdentityKind` — internal engine enum of identity dimensions.
* :class:`AssetIdentityField` — internal engine value type (kind + path).
* :class:`AssetDeriveSpec` — the authored "which dimensions are available"
  buckets + the bridge to derivation names.
* :class:`AssetIdentityRule` — an AND-composition over declared dimension
  *names* used to resolve "is this incoming asset already known?". The
  :attr:`AssetsWritePolicy.identity` slot is an ordered list of these rules;
  the first rule whose conjunction matches wins (OR across rules).
* :class:`ResolvedAssetIdentityRule` — an :class:`AssetIdentityRule` with its
  names resolved back to :class:`AssetIdentityField` objects for the runner.
* :class:`AssetsWritePolicy` — the collection-tier ``PluginConfig`` evaluated
  by the chain runner in :mod:`dynastore.modules.catalog.asset_distributed`.
* :class:`AssetWritePolicyDefaults` — posture-only defaults at the platform /
  catalog tiers (no field-name bindings).

The default is intentionally strict (``REFUSE_FAIL`` on asset_id-then-filename)
so silent overwrites of existing rows are no longer the path of least
resistance — operators must opt into ``UPDATE`` / ``NEW_VERSION`` deliberately.

Cross-link: the items side at :mod:`dynastore.modules.storage.computed_fields`
uses :class:`DeriveSpec` / :class:`IdentityRule` / :class:`ComputedField` for
the same purpose. The two are deliberately NOT a shared abstraction — items
dimensions are spatial (geohash / h3 / s2 / geometry-hash / statistics) while
asset dimensions are identifier / file / hash / metadata-path. The shape is
mirrored so operators can reason about both surfaces with one mental model.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class AssetWriteConflictPolicy(StrEnum):
    """Asset-level conflict policy — applied per row when identity matches.

    Mirrors :class:`dynastore.modules.storage.driver_config.WriteConflictPolicy`
    (items side) so operators can reason about both surfaces with one model.

    Actions:
      - ``UPDATE``         — overwrite ``metadata`` / mutable fields in place
      - ``NEW_VERSION``    — soft-delete the existing row (status='deleted')
                             and INSERT a fresh one with the same ``asset_id``.
                             Permitted because the ``assets_uq_*`` partial
                             unique indexes exclude ``status='deleted'`` rows.
      - ``REFUSE``         — skip silently, return the existing row
      - ``REFUSE_RETURN``  — idempotent return-existing (caller-visible no-op)
      - ``REFUSE_FAIL``    — raise :class:`AssetSidecarRejectedError` →
                             surfaces as 409 / 207 ``IngestionReport`` entry.
                             **Default.**
    """

    UPDATE = "update"
    NEW_VERSION = "new_version"
    REFUSE = "refuse"
    REFUSE_RETURN = "refuse_return"
    REFUSE_FAIL = "refuse_fail"


class AssetIdentityKind(StrEnum):
    """Kinds of identity dimension the chain runner can probe.

    INTERNAL ENGINE TYPE — not part of the authoring surface. Operators author
    :class:`AssetDeriveSpec` buckets + name references; this enum is what those
    names resolve back to for the SQL probe dispatcher in
    :mod:`dynastore.modules.catalog.asset_distributed`. Mirrors the items side,
    where :class:`dynastore.modules.storage.computed_fields.ComputedKind` is the
    engine enum behind :class:`DeriveSpec`.

    The chain runner dispatches one SQL probe per kind:

    - ``ASSET_ID``       — the canonical (catalog_id, collection_id, asset_id)
                           lookup. Cheapest matcher; should usually lead the
                           chain.
    - ``FILENAME``       — match by ``filename`` within the same collection
                           scope (``kind='physical'`` only). Detects "same file
                           re-uploaded under a different asset_id".
    - ``URL``            — match by ``uri`` (physical+active) or ``href``
                           (virtual). Detects re-registrations of the same
                           external URL and re-uploads of the same storage
                           blob.
    - ``METADATA_FIELD`` — JSON-path lookup against the ``metadata`` column.
                           Requires :attr:`AssetIdentityField.path`
                           (dot-notation, e.g. ``"iso19115.fileIdentifier"``).
    - ``CONTENT_HASH``   — match by SHA-256 ``content_hash``. Only meaningful
                           after the finalize event has populated the column,
                           so this kind will simply miss for ``PENDING``
                           rows — keep it after ``ASSET_ID`` / ``FILENAME``
                           in the chain so the cheap probes win first.
    """

    ASSET_ID = "asset_id"
    FILENAME = "filename"
    URL = "url"
    METADATA_FIELD = "metadata_field"
    CONTENT_HASH = "content_hash"


class AssetIdentityField(BaseModel):
    """One identity dimension resolved for the chain runner.

    INTERNAL ENGINE TYPE — produced by
    :meth:`AssetsWritePolicy.resolved_identity` from the authored name
    references, NOT authored directly. Mirrors
    :class:`dynastore.modules.storage.computed_fields.ComputedField` (the items
    engine type behind :class:`DeriveSpec`).

    Most kinds are self-describing — the kind is the entire spec. The
    exception is :attr:`AssetIdentityKind.METADATA_FIELD`, which carries a
    dot-notation JSON path into the asset ``metadata`` column so the runner
    knows where to look.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: AssetIdentityKind
    path: Optional[str] = Field(
        default=None,
        description=(
            "Dot-notation JSON path into the asset ``metadata`` column. "
            "Set when ``kind == METADATA_FIELD`` "
            "(e.g. ``iso19115.fileIdentifier``); ``None`` for every other "
            "kind so the field stays self-describing."
        ),
    )

    @model_validator(mode="after")
    def _check_path(self) -> "AssetIdentityField":
        if self.kind == AssetIdentityKind.METADATA_FIELD and not self.path:
            raise ValueError(
                "AssetIdentityField(kind=METADATA_FIELD) requires `path` "
                "(dot-notation JSON path into the asset metadata column)."
            )
        if self.kind != AssetIdentityKind.METADATA_FIELD and self.path:
            raise ValueError(
                f"AssetIdentityField(kind={self.kind.value}) does not accept "
                "`path` — only METADATA_FIELD uses it."
            )
        return self

    @property
    def resolved_name(self) -> str:
        """Stable identifier used to reference this dimension from a rule.

        For the self-describing kinds the name is the kind's value
        (``"asset_id"``, ``"filename"``, ``"url"``, ``"content_hash"``). For
        ``METADATA_FIELD`` the name is the dotted path's final segment
        (``"iso19115.fileIdentifier"`` -> ``"fileIdentifier"``) — mirrors how
        the items side derives an attribute-stat name from its source path.
        """
        if self.kind == AssetIdentityKind.METADATA_FIELD:
            return (self.path or "").rsplit(".", 1)[-1] or "metadata_field"
        return self.kind.value


class AssetDeriveSpec(BaseModel):
    """Authored declaration of which asset identity dimensions are available.

    Replaces the old polymorphic ``match_on: List[AssetIdentityField]``
    authoring surface. Each self-describing dimension is a boolean (the cheap
    ones default on); ``metadata_fields`` lists dotted JSON paths into the
    asset ``metadata`` column, each becoming one named derivation.

    Derivation NAMES (referenceable from :attr:`AssetIdentityRule.match_on`):

    - ``"asset_id"``     — enabled by :attr:`asset_id` (default ``True``).
    - ``"filename"``     — enabled by :attr:`filename` (default ``True``).
    - ``"url"``          — enabled by :attr:`url` (default ``False``).
    - ``"content_hash"`` — enabled by :attr:`content_hash` (default ``False``;
      only meaningful post-finalize).
    - one name per ``metadata_fields`` entry — the path's **final segment**
      (e.g. ``"iso19115.fileIdentifier"`` -> ``"fileIdentifier"``). Leaf names
      must be unique across the list (collisions are rejected at validation).

    Mirrors :class:`dynastore.modules.storage.computed_fields.DeriveSpec`: the
    authored buckets declare the available derivations; identity rules
    reference them by name.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    asset_id: bool = Field(
        default=True,
        description=(
            "Expose the canonical (catalog_id, collection_id, asset_id) lookup "
            "as the ``asset_id`` derivation. Cheapest matcher; on by default."
        ),
    )
    filename: bool = Field(
        default=True,
        description=(
            "Expose the per-scope ``filename`` lookup (physical assets) as the "
            "``filename`` derivation. On by default — catches the same file "
            "re-uploaded under a different asset_id."
        ),
    )
    url: bool = Field(
        default=False,
        description=(
            "Expose the ``uri`` (physical/active) / ``href`` (virtual) lookup "
            "as the ``url`` derivation. Off by default — opt in for "
            "URL-dedup collections."
        ),
    )
    content_hash: bool = Field(
        default=False,
        description=(
            "Expose the SHA-256 ``content_hash`` lookup as the "
            "``content_hash`` derivation. Off by default — only matches "
            "post-finalize (PENDING rows have no hash yet)."
        ),
    )
    metadata_fields: List[str] = Field(
        default_factory=list,
        examples=[["iso19115.fileIdentifier"], ["external.urn", "doi"]],
        description=(
            "Dotted JSON paths into the asset ``metadata`` column. Each path "
            "becomes one named derivation referenceable from an identity "
            "rule; the name is the path's final segment "
            "(``iso19115.fileIdentifier`` -> ``fileIdentifier``). Leaf names "
            "must be unique across the list."
        ),
    )

    @model_validator(mode="after")
    def _check_metadata_field_names(self) -> "AssetDeriveSpec":
        leaves: List[str] = []
        for path in self.metadata_fields:
            if not path:
                raise ValueError(
                    "AssetDeriveSpec.metadata_fields entries must be non-empty "
                    "dotted JSON paths."
                )
            leaf = path.rsplit(".", 1)[-1]
            if not leaf:
                raise ValueError(
                    f"AssetDeriveSpec.metadata_fields path {path!r} has an "
                    "empty final segment; supply a proper dotted path."
                )
            leaves.append(leaf)
        dupes = sorted({n for n in leaves if leaves.count(n) > 1})
        if dupes:
            raise ValueError(
                "AssetDeriveSpec.metadata_fields leaf names must be unique; "
                f"colliding name(s): {dupes}. Disambiguate the paths or rename "
                "the leaf segment."
            )
        return self

    def metadata_path_for(self, name: str) -> Optional[str]:
        """Return the dotted path whose leaf segment is ``name`` (or ``None``)."""
        for path in self.metadata_fields:
            if path.rsplit(".", 1)[-1] == name:
                return path
        return None

    def produced_names(self) -> set[str]:
        """Set of derivation names this spec exposes.

        Mirrors :meth:`DeriveSpec`-derived name resolution on the items side:
        every name an identity rule may reference must appear here.
        """
        names: set[str] = set()
        if self.asset_id:
            names.add(AssetIdentityKind.ASSET_ID.value)
        if self.filename:
            names.add(AssetIdentityKind.FILENAME.value)
        if self.url:
            names.add(AssetIdentityKind.URL.value)
        if self.content_hash:
            names.add(AssetIdentityKind.CONTENT_HASH.value)
        for path in self.metadata_fields:
            names.add(path.rsplit(".", 1)[-1])
        return names

    def to_field(self, name: str) -> AssetIdentityField:
        """Resolve a derivation ``name`` to its engine :class:`AssetIdentityField`.

        Raises ``ValueError`` when the name is not produced by this spec —
        the policy-level validator guards against this at config-save, this is
        the defensive re-check used by the runner-facing resolver.
        """
        if name == AssetIdentityKind.ASSET_ID.value and self.asset_id:
            return AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)
        if name == AssetIdentityKind.FILENAME.value and self.filename:
            return AssetIdentityField(kind=AssetIdentityKind.FILENAME)
        if name == AssetIdentityKind.URL.value and self.url:
            return AssetIdentityField(kind=AssetIdentityKind.URL)
        if name == AssetIdentityKind.CONTENT_HASH.value and self.content_hash:
            return AssetIdentityField(kind=AssetIdentityKind.CONTENT_HASH)
        path = self.metadata_path_for(name)
        if path is not None:
            return AssetIdentityField(
                kind=AssetIdentityKind.METADATA_FIELD, path=path
            )
        raise ValueError(
            f"AssetIdentityRule.match_on references derivation {name!r} not "
            f"produced by AssetDeriveSpec; declared: {sorted(self.produced_names())}."
        )


class AssetIdentityRule(BaseModel):
    """One AND-composition over declared derivations for identity resolution.

    ``match_on`` lists derivation **names** (the values
    :meth:`AssetDeriveSpec.produced_names` exposes — e.g. ``"asset_id"``,
    ``"filename"``, ``"fileIdentifier"``) whose values together identify an
    asset. The dimensions are *referenced*, not re-declared: every name must
    resolve to an :class:`AssetDeriveSpec` output. All listed names must match
    the same existing row for the rule to fire (AND).
    :attr:`AssetsWritePolicy.identity` is an ordered list of these rules; the
    first whose conjunction matches wins (OR across rules, first-match-wins).

    ``on_match`` lets a rule override the policy-level
    :class:`AssetWriteConflictPolicy` — useful for "match by asset_id →
    UPDATE; match by content_hash → REFUSE_RETURN".

    Mirrors :class:`dynastore.modules.storage.computed_fields.IdentityRule`.
    """

    model_config = ConfigDict(extra="forbid")

    match_on: List[str] = Field(min_length=1)
    on_match: Optional[AssetWriteConflictPolicy] = None


@dataclass(frozen=True)
class ResolvedAssetIdentityRule:
    """An :class:`AssetIdentityRule` with its ``match_on`` names resolved to the
    engine's :class:`AssetIdentityField` objects.

    Produced by :meth:`AssetsWritePolicy.resolved_identity`; consumed by the
    write-path identity matcher (``asset_distributed``) which needs each field's
    ``kind`` (and optional metadata ``path``) to drive the SQL probes. Not an
    authored/config type. Mirrors
    :class:`dynastore.modules.storage.driver_config.ResolvedIdentityRule`.
    """

    match_on: List[AssetIdentityField]
    on_match: Optional[AssetWriteConflictPolicy] = None


# Default identity chain shipped with AssetsWritePolicy(). Two single-name
# rules — ``asset_id`` first (cheap canonical lookup), then ``filename``
# (catches "same file re-uploaded under a different asset_id"). Both names are
# produced by the default AssetDeriveSpec (asset_id + filename on).
def _default_identity() -> List[AssetIdentityRule]:
    return [
        AssetIdentityRule(match_on=[AssetIdentityKind.ASSET_ID.value]),
        AssetIdentityRule(match_on=[AssetIdentityKind.FILENAME.value]),
    ]


class AssetsWritePolicy(PluginConfig):
    """Asset-level write behaviour, applied at upload-create / virtual-create.

    Registered as ``AssetsWritePolicy`` in the config waterfall (identity:
    ``class_key``, ``"assets_write_policy"``) — collection > catalog >
    platform > code defaults.

    Read by :func:`dynastore.modules.catalog.asset_distributed.upsert_asset`
    via the standard waterfall::

        configs = get_protocol(ConfigsProtocol)
        policy = await configs.get_config(
            AssetsWritePolicy, catalog_id=catalog_id, collection_id=collection_id
        )

    Identity is authored as buckets + names, mirroring the items side
    (:class:`dynastore.modules.storage.driver_config.ItemsWritePolicy`):

    - :attr:`derive` — an :class:`AssetDeriveSpec` declaring which identity
      dimensions are available (``asset_id`` / ``filename`` / ``url`` /
      ``content_hash`` booleans + ``metadata_fields`` dotted paths).
    - :attr:`identity` — an ordered list of :class:`AssetIdentityRule`. Each
      rule ANDs the derivation *names* in its ``match_on`` (referencing
      ``derive`` outputs); rules OR across the list (first match wins).
      Per-rule ``on_match`` overrides :attr:`on_conflict`.

    The defaults are deliberately strict: ``REFUSE_FAIL`` on the
    asset_id-then-filename chain. Operators who want overwrite-on-collision or
    new-version semantics opt in explicitly at the platform / catalog /
    collection tier.

    Layering vs :class:`AssetWritePolicyDefaults`:
      - ``AssetsWritePolicy`` is the collection-INTRINSIC config — also valid
        at platform / catalog tiers as the operator-facing surface.
      - ``AssetWritePolicyDefaults`` (sibling class) is the
        platform/catalog-tier POSTURE config — no field-name bindings.
      - When both are present, the closer-tier ``AssetsWritePolicy`` wins;
        ``AssetWritePolicyDefaults`` at upstream tiers fills any gap.

    Worked scenarios:

    1. **Default strict mode** (no silent overwrites)::

           AssetsWritePolicy()  # REFUSE_FAIL on [asset_id, filename]

       Re-uploading the same asset_id OR the same filename surfaces as a 409
       (or 207 ``IngestionReport`` entry) instead of silently mutating the
       existing row.

    2. **Idempotent re-upload** (unchanged content → no-op return-existing)::

           AssetsWritePolicy(
               on_conflict=AssetWriteConflictPolicy.UPDATE,
               derive=AssetDeriveSpec(
                   asset_id=True, filename=True, content_hash=True
               ),
               identity=[
                   AssetIdentityRule(
                       match_on=["content_hash"],
                       on_match=AssetWriteConflictPolicy.REFUSE_RETURN,
                   ),
                   AssetIdentityRule(match_on=["asset_id"]),
               ],
           )

       The ``content_hash`` rule is the unified replacement for the old
       ``skip_if_unchanged_content_hash`` boolean: matching on
       ``content_hash`` *is* the "incoming hash equals an existing row's
       hash" condition, and ``on_match=REFUSE_RETURN`` makes that an
       idempotent no-op that echoes the existing row. Put the rule ahead of
       the mutating rules so an unchanged re-upload short-circuits before
       ``UPDATE`` / ``NEW_VERSION`` fire. ``content_hash`` only matches
       post-finalize (PENDING upload-create rows have no hash yet), so the
       upload-create path falls through to the cheaper rules unchanged.

    3. **External-id dedup via metadata** (e.g. ISO19115 fileIdentifier)::

           AssetsWritePolicy(
               on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
               derive=AssetDeriveSpec(
                   metadata_fields=["iso19115.fileIdentifier"],
               ),
               identity=[
                   AssetIdentityRule(match_on=["fileIdentifier"]),
               ],
           )
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "collection",
        "assets",
        "policy",
    )
    _freeze_at: ClassVar[Optional[str]] = "collection"

    on_conflict: Mutable[AssetWriteConflictPolicy] = Field(
        default=AssetWriteConflictPolicy.REFUSE_FAIL,
        examples=["refuse_fail", "update", "new_version", "refuse_return"],
        description=(
            "Action when an incoming asset matches an existing row via the "
            "configured identity chain. ``refuse_fail`` (default) raises a "
            "structured rejection that surfaces as a 409 (single ingest) or a "
            "207 ``IngestionReport`` entry (bulk). ``update`` mutates the "
            "matched row's ``metadata``. ``new_version`` soft-deletes the old "
            "row and inserts a fresh one with the same ``asset_id``. "
            "``refuse`` / ``refuse_return`` skip the write — the latter "
            "returns the existing row so the caller observes idempotence. "
            "An individual :class:`AssetIdentityRule` may override this via "
            "``on_match``."
        ),
    )
    derive: Mutable[AssetDeriveSpec] = Field(
        default_factory=AssetDeriveSpec,
        description=(
            "Declares which identity dimensions are available "
            "(:class:`AssetDeriveSpec`): ``asset_id`` / ``filename`` (cheap, "
            "default on), ``url`` / ``content_hash`` (opt-in), and "
            "``metadata_fields`` (dotted JSON paths, each a named derivation). "
            "Identity rules reference these by name. The default exposes "
            "``asset_id`` + ``filename`` so the default chain resolves."
        ),
    )
    identity: Mutable[List[AssetIdentityRule]] = Field(
        default_factory=_default_identity,
        examples=[
            [
                {"match_on": ["asset_id"]},
            ],
            [
                {"match_on": ["asset_id"]},
                {"match_on": ["filename"]},
            ],
            [
                {"match_on": ["asset_id"]},
                {"match_on": ["content_hash"]},
            ],
            [
                {"match_on": ["fileIdentifier"]},
            ],
        ],
        description=(
            "Ordered identity rules — first rule returning a match wins (OR "
            "across rules). Each rule's ``match_on`` is an AND-composition "
            "over derivation **names** declared by ``derive`` (e.g. "
            "``asset_id``, ``filename``, ``content_hash``, or a metadata "
            "field's leaf name). Every name must resolve to a ``derive`` "
            "output (validated at config-save). ``asset_id`` and ``filename`` "
            "are the cheap default chain; ``content_hash`` only matches "
            "post-finalize (PENDING rows have no hash yet) so put it after "
            "the cheap names."
        ),
    )
    require_filename: Mutable[bool] = Field(
        default=True,
        examples=[True, False],
        description=(
            "Service-layer pre-check: if True, a physical-asset create with "
            "``filename`` empty / missing is rejected (422) before the "
            "identity chain runs. The DB CHECK constraint already enforces "
            "NOT NULL for physical rows; this flag surfaces the rule earlier "
            "with a typed error. Orthogonal to identity resolution — the "
            "``filename`` identity probe simply returns no hit when the "
            "incoming payload has no filename."
        ),
    )
    def requires_driver_versioning(self) -> bool:
        """True iff the policy needs an asset driver that supports versioning."""
        return self.on_conflict == AssetWriteConflictPolicy.NEW_VERSION

    # ------------------------------------------------------------------
    # Helpers — resolve authored names back to engine fields
    # ------------------------------------------------------------------

    def _produced_names(self) -> set[str]:
        """Set of derivation names :attr:`derive` exposes."""
        return self.derive.produced_names()

    def resolved_identity(self) -> List["ResolvedAssetIdentityRule"]:
        """Resolve each identity rule's ``match_on`` names to engine fields.

        Every name must be a declared :attr:`derive` output — enforced at
        config-save by :meth:`_validate_identity_refs`, re-checked here
        defensively. Consumed by the write-path identity matcher.
        """
        rules: List[ResolvedAssetIdentityRule] = []
        for rule in self.identity:
            fields = [self.derive.to_field(name) for name in rule.match_on]
            rules.append(
                ResolvedAssetIdentityRule(
                    match_on=fields, on_match=rule.on_match
                )
            )
        return rules

    @model_validator(mode="after")
    def _validate_identity_refs(self) -> "AssetsWritePolicy":
        """Fail-fast: every identity ``match_on`` name must resolve to a
        ``derive`` output."""
        allowed = self._produced_names()
        for rule in self.identity:
            unknown = [n for n in rule.match_on if n not in allowed]
            if unknown:
                raise ValueError(
                    "AssetIdentityRule.match_on references name(s) not "
                    f"produced by AssetsWritePolicy.derive: {unknown}. "
                    f"Available: {sorted(allowed)}."
                )
        return self


class AssetWritePolicyDefaults(PluginConfig):
    """Posture-only defaults for the platform / catalog write-policy waterfall.

    Carries only posture flags — never references specific field names or JSON
    paths.  Use this at the platform / catalog tier to enforce a default
    strictness across all owned collections without binding to any single
    metadata schema.

    Layering vs :class:`AssetsWritePolicy`:
      - At platform / catalog tiers: prefer ``AssetWritePolicyDefaults`` for
        posture.
      - At collection tier: use ``AssetsWritePolicy`` for the full surface
        (derive buckets, identity rules — including a ``content_hash`` rule
        for idempotent re-upload).
      - When both are present, ``AssetsWritePolicy`` at the closer tier wins
        for the fields it owns; ``AssetWritePolicyDefaults`` at upstream
        tiers fills the gap via the standard waterfall.

    Worked scenarios:

    1. **Platform-wide strict mode**::

           # at platform tier:
           AssetWritePolicyDefaults(
               on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
               require_filename=True,
           )

       Every collection without an explicit ``AssetsWritePolicy`` rejects
       duplicates with a 409 and demands ``filename`` on physical assets.

    2. **Per-catalog defaults** (loose at catalog A, strict at catalog B)::

           # at catalog A scope (landing zone):
           AssetWritePolicyDefaults(on_conflict=AssetWriteConflictPolicy.UPDATE)

           # at catalog B scope (authoritative master):
           AssetWritePolicyDefaults(
               on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
           )
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "assets",
        "policy_defaults",
    )

    on_conflict: Mutable[AssetWriteConflictPolicy] = Field(
        default=AssetWriteConflictPolicy.REFUSE_FAIL,
        examples=["refuse_fail", "update", "new_version", "refuse_return"],
        description=(
            "Default conflict action when no closer-tier ``AssetsWritePolicy`` "
            "overrides it. Cascades platform → catalog → collection."
        ),
    )
    require_filename: Mutable[bool] = Field(
        default=True,
        examples=[True, False],
        description=(
            "Default for the ``require_filename`` posture. If True, physical "
            "asset creates with no ``filename`` are rejected (422) at the "
            "service layer."
        ),
    )


__all__ = [
    "AssetWriteConflictPolicy",
    "AssetIdentityKind",
    "AssetIdentityField",
    "AssetDeriveSpec",
    "AssetIdentityRule",
    "ResolvedAssetIdentityRule",
    "AssetsWritePolicy",
    "AssetWritePolicyDefaults",
]
