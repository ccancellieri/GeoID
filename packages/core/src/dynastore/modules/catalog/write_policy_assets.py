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
* :class:`AssetIdentityMatcher` — strategies for resolving "is this incoming
  asset already known?".
* :class:`AssetsWritePolicy` — the collection-tier ``PluginConfig`` evaluated
  by the chain runner in :mod:`dynastore.modules.catalog.asset_distributed`.
* :class:`AssetWritePolicyDefaults` — posture-only defaults at the platform /
  catalog tiers (no field-name bindings).

The default is intentionally strict (``REFUSE_FAIL`` on
``[ASSET_ID, FILENAME]``) so silent overwrites of existing rows are no longer
the path of least resistance — operators must opt into ``UPDATE`` /
``NEW_VERSION`` deliberately.
"""

from enum import StrEnum
from typing import ClassVar, List, Optional, Tuple

from pydantic import Field, model_validator

from dynastore.modules.db_config.platform_config_service import Mutable, PluginConfig


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


class AssetIdentityMatcher(StrEnum):
    """Strategies for deciding whether an incoming asset matches an existing one.

    Matchers are evaluated in the order declared on
    :attr:`AssetsWritePolicy.identity_matchers`; the first matcher that
    resolves a row wins.

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
                           Requires :attr:`AssetsWritePolicy.metadata_match_path`
                           (dot-notation, e.g. ``"iso19115.fileIdentifier"``).
    - ``CONTENT_HASH``   — match by SHA-256 ``content_hash``. Only meaningful
                           after the finalize event has populated the column,
                           so this matcher will simply miss for ``PENDING``
                           rows — keep it after ``ASSET_ID`` / ``FILENAME``
                           in the chain so the cheap probes win first.
    """

    ASSET_ID = "asset_id"
    FILENAME = "filename"
    URL = "url"
    METADATA_FIELD = "metadata_field"
    CONTENT_HASH = "content_hash"


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
    ``[ASSET_ID, FILENAME]`` chain.  Operators who want overwrite-on-collision
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
               identity_matchers=[
                   AssetIdentityMatcher.ASSET_ID,
                   AssetIdentityMatcher.CONTENT_HASH,
               ],
           )

    3. **External-id dedup via metadata** (e.g. ISO19115 fileIdentifier)::

           AssetsWritePolicy(
               on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
               identity_matchers=[AssetIdentityMatcher.METADATA_FIELD],
               metadata_match_path="iso19115.fileIdentifier",
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
            "configured matcher chain. ``refuse_fail`` (default) raises a "
            "structured rejection that surfaces as a 409 (single ingest) or a "
            "207 ``IngestionReport`` entry (bulk). ``update`` mutates the "
            "matched row's ``metadata``. ``new_version`` soft-deletes the old "
            "row and inserts a fresh one with the same ``asset_id``. "
            "``refuse`` / ``refuse_return`` skip the write — the latter "
            "returns the existing row so the caller observes idempotence."
        ),
    )
    identity_matchers: Mutable[List[AssetIdentityMatcher]] = Field(
        default_factory=lambda: [
            AssetIdentityMatcher.ASSET_ID,
            AssetIdentityMatcher.FILENAME,
        ],
        examples=[
            ["asset_id"],
            ["asset_id", "filename"],
            ["asset_id", "content_hash"],
            ["metadata_field"],
        ],
        description=(
            "Ordered matcher chain — first matcher returning a row wins. "
            "``asset_id`` and ``filename`` are the cheap default chain. "
            "``content_hash`` only matches post-finalize (PENDING rows have "
            "no hash yet) so put it after the cheap matchers. "
            "``metadata_field`` requires ``metadata_match_path``."
        ),
    )
    metadata_match_path: Mutable[Optional[str]] = Field(
        default=None,
        examples=[None, "iso19115.fileIdentifier", "external.urn"],
        description=(
            "Dot-notation JSON path into the ``metadata`` column used by the "
            "``metadata_field`` matcher (e.g. ``iso19115.fileIdentifier``). "
            "Required when ``metadata_field`` appears in ``identity_matchers``."
        ),
    )
    require_filename: Mutable[bool] = Field(
        default=True,
        examples=[True, False],
        description=(
            "If True, a physical-asset create with ``filename`` empty / "
            "missing is rejected (422). The DB CHECK constraint already "
            "enforces NOT NULL for physical rows; this flag surfaces the "
            "rule earlier (service layer) with a typed error."
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
    def _validate_metadata_path(self) -> "AssetsWritePolicy":
        if (
            AssetIdentityMatcher.METADATA_FIELD in self.identity_matchers
            and not self.metadata_match_path
        ):
            raise ValueError(
                "AssetIdentityMatcher.METADATA_FIELD requires "
                "``metadata_match_path`` to be set (dot-notation JSON path "
                "into the asset metadata column)."
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
        (matchers, metadata path, content-hash gating).
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
