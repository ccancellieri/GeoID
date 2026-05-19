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

"""Asset-side write-policy types.

Mirrors :mod:`dynastore.modules.storage.driver_config` (items side):

* :class:`AssetWriteConflictPolicy` — per-asset conflict action enum.
* :class:`AssetIdentityKind` — kinds of identity a rule may evaluate.
* :class:`AssetIdentityField` — one identity dimension (kind + optional path).
* :class:`AssetIdentityRule` — an AND-composition over fields used to resolve
  "is this incoming asset already known?". The
  :attr:`AssetsWritePolicy.identity` slot is an ordered list of these rules;
  the first rule whose conjunction matches wins (OR across rules).
* :class:`AssetsWritePolicy` — the collection-tier ``PluginConfig`` evaluated
  by the chain runner in :mod:`dynastore.modules.catalog.asset_distributed`.
* :class:`AssetWritePolicyDefaults` — posture-only defaults at the platform /
  catalog tiers (no field-name bindings).

The default is intentionally strict (``REFUSE_FAIL`` on ASSET_ID-then-FILENAME)
so silent overwrites of existing rows are no longer the path of least
resistance — operators must opt into ``UPDATE`` / ``NEW_VERSION`` deliberately.

Cross-link: the items side at :mod:`dynastore.modules.storage.computed_fields`
uses :class:`ComputedField` / :class:`IdentityRule` for the same purpose. The
two are deliberately NOT a shared abstraction — items dimensions are spatial
(geohash / h3 / s2 / geometry-hash / statistics) while asset dimensions are
identifier / file / hash / metadata-path. The shape is mirrored so operators
can reason about both surfaces with one mental model.
"""

from enum import StrEnum
from typing import ClassVar, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig


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
    """Kinds of identity dimension an :class:`AssetIdentityField` may carry.

    The chain runner in :mod:`dynastore.modules.catalog.asset_distributed`
    dispatches one SQL probe per kind:

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
    """One identity dimension declared on an :class:`AssetIdentityRule`.

    Most kinds are self-describing — the kind is the entire spec. The
    exception is :attr:`AssetIdentityKind.METADATA_FIELD`, which needs a
    dot-notation JSON path into the asset ``metadata`` column to know where
    to look.

    The mirror on the items side is
    :class:`dynastore.modules.storage.computed_fields.ComputedField`;
    different dimensions, same shape.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: AssetIdentityKind
    path: Optional[str] = Field(
        default=None,
        description=(
            "Dot-notation JSON path into the asset ``metadata`` column. "
            "Required when ``kind == METADATA_FIELD`` "
            "(e.g. ``iso19115.fileIdentifier``); forbidden for every other "
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


class AssetIdentityRule(BaseModel):
    """One AND-composition over identity fields used to resolve an asset.

    ``match_on`` lists the fields whose values together identify an asset.
    All listed fields must match an existing row for the rule to fire (AND).
    :attr:`AssetsWritePolicy.identity` is an ordered list of these rules;
    the first one whose conjunction matches wins (OR across rules,
    first-match-wins).

    For Stage-1 simplicity the runner currently treats each rule as a
    single-field probe — i.e. ``match_on`` is expected to carry exactly one
    field per rule. Multi-field AND-composition over assets is left as a
    follow-up (no live use case yet); the shape is set up to accept it
    without another model migration.

    ``on_match`` lets a rule override the policy-level
    :class:`AssetWriteConflictPolicy` — useful for "match by ASSET_ID →
    UPDATE; match by CONTENT_HASH → REFUSE_RETURN" semantics. Defaults to
    ``None`` (use the policy-level action).
    """

    model_config = ConfigDict(extra="forbid")

    match_on: List[AssetIdentityField] = Field(min_length=1)
    on_match: Optional[AssetWriteConflictPolicy] = None


# Default identity chain shipped with AssetsWritePolicy() / defaults class.
# Two single-field rules — ASSET_ID first (cheap canonical lookup), then
# FILENAME (catches "same file re-uploaded under a different asset_id").
def _default_identity() -> List[AssetIdentityRule]:
    return [
        AssetIdentityRule(
            match_on=[AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)],
        ),
        AssetIdentityRule(
            match_on=[AssetIdentityField(kind=AssetIdentityKind.FILENAME)],
        ),
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

    The defaults are deliberately strict: ``REFUSE_FAIL`` on the
    ASSET_ID-then-FILENAME chain.  Operators who want overwrite-on-collision
    or new-version semantics opt in explicitly at the platform / catalog /
    collection tier.

    Layering vs :class:`AssetWritePolicyDefaults`:
      - ``AssetsWritePolicy`` is the collection-INTRINSIC config — also valid
        at platform / catalog tiers as the operator-facing surface.
      - ``AssetWritePolicyDefaults`` (sibling class) is the
        platform/catalog-tier POSTURE config — no field-name references.
      - When both are present, the closer-tier ``AssetsWritePolicy`` wins;
        ``AssetWritePolicyDefaults`` at upstream tiers fills any gap.

    Worked scenarios:

    1. **Default strict mode** (no silent overwrites)::

           AssetsWritePolicy()  # REFUSE_FAIL on [ASSET_ID, FILENAME]

       Re-uploading the same asset_id OR the same filename surfaces as a 409
       (or 207 ``IngestionReport`` entry) instead of silently mutating the
       existing row.

    2. **Idempotent re-upload** (same content, same key, no error)::

           AssetsWritePolicy(
               on_conflict=AssetWriteConflictPolicy.REFUSE_RETURN,
               identity=[
                   AssetIdentityRule(
                       match_on=[AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)]
                   ),
                   AssetIdentityRule(
                       match_on=[AssetIdentityField(kind=AssetIdentityKind.CONTENT_HASH)]
                   ),
               ],
           )

    3. **External-id dedup via metadata** (e.g. ISO19115 fileIdentifier)::

           AssetsWritePolicy(
               on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
               identity=[
                   AssetIdentityRule(
                       match_on=[
                           AssetIdentityField(
                               kind=AssetIdentityKind.METADATA_FIELD,
                               path="iso19115.fileIdentifier",
                           ),
                       ],
                   ),
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
    _visibility: ClassVar[Optional[str]] = "collection"

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
    identity: Mutable[List[AssetIdentityRule]] = Field(
        default_factory=_default_identity,
        examples=[
            [
                {"match_on": [{"kind": "asset_id"}]},
            ],
            [
                {"match_on": [{"kind": "asset_id"}]},
                {"match_on": [{"kind": "filename"}]},
            ],
            [
                {"match_on": [{"kind": "asset_id"}]},
                {"match_on": [{"kind": "content_hash"}]},
            ],
            [
                {
                    "match_on": [
                        {"kind": "metadata_field", "path": "iso19115.fileIdentifier"}
                    ],
                },
            ],
        ],
        description=(
            "Ordered list of identity rules — first rule returning a match "
            "wins (OR across rules). Each rule's ``match_on`` is an "
            "AND-composition over identity fields. ``asset_id`` and "
            "``filename`` are the cheap default chain. ``content_hash`` only "
            "matches post-finalize (PENDING rows have no hash yet) so put it "
            "after the cheap fields. ``metadata_field`` requires ``path``."
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
            "FILENAME identity probe simply returns no hit when the incoming "
            "payload has no filename."
        ),
    )
    skip_if_unchanged_content_hash: Mutable[bool] = Field(
        default=False,
        examples=[False, True],
        description=(
            "Post-finalize idempotence shortcut. When True, a matched row "
            "whose ``content_hash`` equals the incoming hash short-circuits "
            "the conflict action: ``NEW_VERSION`` becomes a no-op, "
            "``UPDATE`` collapses to ``REFUSE_RETURN``. Has no effect during "
            "upload-create (where the row is PENDING and has no hash yet)."
        ),
    )

    def requires_driver_versioning(self) -> bool:
        """True iff the policy needs an asset driver that supports versioning."""
        return self.on_conflict == AssetWriteConflictPolicy.NEW_VERSION

    @model_validator(mode="after")
    def _validate_identity(self) -> "AssetsWritePolicy":
        # The AssetIdentityField validator already enforces the
        # METADATA_FIELD ↔ path constraint at the field level; this hook is
        # the place for any cross-rule invariant that should fail loudly.
        # Currently there is none beyond the field-level rules.
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
        (identity rules, metadata path, content-hash gating).
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
