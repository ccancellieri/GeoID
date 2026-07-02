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

"""Preset lifecycle REST surface — mounted under the Configuration API.

Presets are a configuration concern, not an admin one: applying a preset
walks a bundle of validated configs through the standard
``ConfigsProtocol.set_config`` lifecycle.  This sub-router is mounted onto
``ConfigsService.router`` (prefix ``/configs``), so the three URL families
that encode the apply scope read:

* ``PLATFORM``   — ``/configs/presets/{name}`` (no scope params; ``build()``)
* ``CATALOG``    — ``/configs/catalogs/{cat}/presets/{name}``
* ``COLLECTION`` — ``/configs/catalogs/{cat}/collections/{col}/presets/{name}``

``ITEMS`` / ``ASSETS`` presets bind to the collection family always, and to
the catalog family when ``catalog_scopable=True``.

Authorization: the whole ``/configs/.*`` surface is sysadmin-only via the
``configs_access`` policy.  Catalog-scoped preset routes are additionally
reachable by a catalog admin through the central catalog-scoped grant
(``ALLOW {protocol}/catalogs/{cat}/*``) — a catalog admin is admin under
their catalog across every protocol, so no preset-specific or per-service
delegation policy is involved.  ``DELETE`` is the symmetric unapply path on
the same resource URL.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError

from dynastore.modules import get_protocol
from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.iam.iam_service import IamService
from dynastore.extensions.tools.resolvers import (
    resolve_catalog_or_404,
    resolve_collection_or_404,
)

# Single sub-router with no prefix of its own; ``ConfigsService`` mounts it
# via ``include_router`` so every path here inherits the ``/configs`` prefix
# and the shared ``Configuration API`` tag.
router: APIRouter = APIRouter()


# --- Applied-presets bulk list DTOs (#1425) -------------------------------


class AppliedRowResponse(BaseModel):
    """One row from ``iam.applied_presets`` for a given scope."""

    preset_name: str
    scope_key: str
    state: str
    applied_at: Optional[str] = Field(
        default=None,
        description="ISO 8601 timestamp when the preset reached 'applied' state.",
    )
    applied_by: Optional[str] = Field(
        default=None, description="UUID of the principal that triggered the apply."
    )
    params_snapshot: Optional[dict] = Field(
        default=None, description="Parameter snapshot captured at apply time."
    )
    last_error: Optional[str] = Field(
        default=None, description="Last error message when state is 'failed' or 'revoke_failed'."
    )
    updated_at: Optional[str] = Field(
        default=None, description="ISO 8601 timestamp of the last state-machine transition."
    )


class AppliedPresetsPage(BaseModel):
    """Paginated response for ``GET /configs/presets/applied``."""

    rows: "list[AppliedRowResponse]"
    next_cursor: Optional[str] = Field(
        default=None,
        description=(
            "Opaque keyset cursor. Pass as ``cursor`` on the next request "
            "to retrieve the following page. ``null`` when this is the last page."
        ),
    )


# --- Scope-resolution helpers ---------------------------------------------


async def _assert_catalog_exists(catalog_id: str) -> None:
    """Raise 404 if ``catalog_id`` does not resolve to a known catalog.

    Collection-scope preset endpoints write catalog/collection-tier config
    rows; an operator typo for the catalog segment must fail loudly rather
    than seed orphan config under a non-existent catalog.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return
    await resolve_catalog_or_404(catalogs, catalog_id, use_model=True)


async def _assert_collection_exists(catalog_id: str, collection_id: str) -> None:
    """Raise 404 if ``collection_id`` does not exist under ``catalog_id``.

    Collection-scope preset endpoints write collection-tier config rows; an
    operator typo for the collection segment must fail loudly rather than
    seed orphan config under a non-existent collection.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return
    await resolve_collection_or_404(
        catalogs.collections, catalog_id, collection_id,
        detail=f"Collection '{collection_id}' not found in catalog '{catalog_id}'.",
    )


def _preset_reachable_at(preset, url_tier: "PresetTier") -> bool:  # noqa: F821
    """Whether ``preset`` may be applied at the ``url_tier`` URL family.

    Single-family tiers (``PLATFORM`` / ``CATALOG`` / ``COLLECTION``) match
    their own URL family. ``ITEMS`` / ``ASSETS`` presets bind to the
    collection family always and additionally to the catalog family when
    ``catalog_scopable`` is set. A ``PLATFORM`` preset with
    ``catalog_scopable=True`` (the composite bundles) is additionally
    reachable from the catalog family, scoping its writes to that catalog.
    Mismatches are surfaced as HTTP 409 by the caller — the preset exists
    but is not valid at that scope.
    """
    from dynastore.modules.storage.presets import PresetTier

    preset_tier = getattr(preset, "tier", None)
    if preset_tier == url_tier:
        return True
    if preset_tier in (PresetTier.ITEMS, PresetTier.ASSETS):
        if url_tier == PresetTier.COLLECTION:
            return True
        if url_tier == PresetTier.CATALOG:
            return bool(getattr(preset, "catalog_scopable", False))
    if preset_tier == PresetTier.PLATFORM and url_tier == PresetTier.CATALOG:
        return bool(getattr(preset, "catalog_scopable", False))
    return False


def _resolve_preset_for_scope(preset_name: str, url_tier: "PresetTier"):  # noqa: F821
    """Look the preset up and verify it is reachable at ``url_tier``.

    Returns the preset on success. Raises 404 when unknown, 409 when the
    URL scope does not match the preset's declared tier.
    """
    from dynastore.modules.storage.presets import PresetTier, get_preset

    try:
        preset = get_preset(preset_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not _preset_reachable_at(preset, url_tier):
        preset_tier = getattr(preset, "tier", None)
        tier_label = preset_tier.value if isinstance(preset_tier, PresetTier) else str(preset_tier)
        raise HTTPException(
            status_code=409,
            detail=(
                f"Preset '{preset_name}' is a {tier_label}-tier preset and "
                f"cannot be applied at the {url_tier.value} URL scope."
            ),
        )
    return preset


def _coerce_params(preset, params_body: Optional[Dict[str, Any]]):
    """Validate an optional request body against the preset's params model.

    Returns ``None`` when no body (or an empty body) is supplied — the
    lifecycle then applies the preset's default params — or a validated
    params instance built from ``preset.params_model``. Raises 422 when the
    body does not match that model, so a caller choosing e.g.
    ``{"stac_level": "items", "stac_storage": "ES"}`` for the ``stac_storage``
    preset gets a clear validation error instead of a silent default.
    """
    if not params_body:
        return None
    model = getattr(preset, "params_model", None)
    if model is None:
        return None
    try:
        return model.model_validate(params_body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc


async def _apply_preset_bundle(
    preset,
    base_scope: dict,
    *,
    force: bool = False,
    params: Optional[BaseModel] = None,
) -> dict:
    """Delegate to :func:`lifecycle.dispatch_preset`.

    The dispatcher picks the right path for ``preset`` — routing-config
    bundle (``hasattr(preset, "build")``) or generalised ``Preset``
    (``apply``/``revoke``/``dry_run``).

    ``params`` carries an optional caller-supplied params model (from the
    request body); when ``None`` the lifecycle falls back to the preset's
    default params. ``force`` carries the ``?force=true`` REST flag through to
    :func:`apply_preset`, letting an operator replace an already-applied
    preset whose stored params snapshot differs from the request.
    """
    from dynastore.modules.storage.presets.lifecycle import dispatch_preset

    return await dispatch_preset(
        preset, "apply", base_scope=base_scope, params=params, force=force
    )


async def _unapply_preset_bundle(preset, base_scope: dict) -> dict:
    """Delegate to :func:`lifecycle.dispatch_preset` for revoke."""
    from dynastore.modules.storage.presets.lifecycle import dispatch_preset

    return await dispatch_preset(preset, "unapply", base_scope=base_scope)


# -------------------------------------------------------------------------
# Presets — named configuration actions applied/revoked via the Config API.
#
# Three URL families encode the apply scope:
#   /configs/presets/{name}                                  platform
#   /configs/catalogs/{cat}/presets/{name}                   catalog
#   /configs/catalogs/{cat}/collections/{col}/presets/{name} collection
#
# Routing presets (``public_catalog``, ``private_catalog``, etc.) apply
# their bundle through the standard ``set_config`` lifecycle. Generalised
# presets additionally record an audit row in ``iam.applied_presets`` when
# the lifecycle layer is available.
# -------------------------------------------------------------------------


@router.get("/presets", summary="Search and list registered presets")
async def list_presets(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    tier: Optional[str] = Query(
        None,
        description=(
            "Filter to presets of this tier "
            "(platform / catalog / collection / items / assets)."
        ),
    ),
    q: Optional[str] = Query(None, description="Full-text search on name, description, keywords."),
    name: Optional[str] = Query(None, description="Exact or prefix match on preset name."),
    keywords: Optional[str] = Query(None, description="Comma-separated AND match on keywords."),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None, description="Keyset pagination cursor (preset name)."),
):
    from dynastore.modules.storage.presets import (
        PresetTier,
        search_presets,
    )

    tier_filter: Optional[PresetTier] = None
    if tier is not None:
        try:
            tier_filter = PresetTier(tier)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unknown tier {tier!r}. Known: "
                    f"{[t.value for t in PresetTier]}"
                ),
            ) from exc

    kw_list = [k.strip() for k in keywords.split(",")] if keywords else None

    result = search_presets(
        q=q,
        name=name,
        tier=tier_filter,
        keywords=kw_list,
        limit=limit,
        cursor=cursor,
    )
    # Legacy shape: also expose as "presets" list for back-compat.
    return {"presets": result["items"], "next_cursor": result["next_cursor"]}


@router.get(
    "/presets/applied",
    summary="List applied-presets audit rows for a given scope (#1425)",
    response_model=AppliedPresetsPage,
)
async def list_applied_presets(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    scope_key: str = Query(
        ...,
        description=(
            "Exact scope key to query. Accepted shapes: "
            "``platform`` | ``catalog:<cat_id>`` | ``catalog:<cat_id>/collection:<coll_id>``"
        ),
    ),
    state: str = Query(
        "applied",
        description="State filter. One of: applied, revoked, pending, failed, partial, …",
    ),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None, description="Opaque keyset pagination cursor."),
):
    """Return paginated ``iam.applied_presets`` rows for an exact ``scope_key``.

    Degrades cleanly to 503 when the IAM extension is not loaded (the
    ``iam.applied_presets`` table does not exist without IAM).
    """
    import re

    _SCOPE_RE = re.compile(
        r"^(platform|catalog:[^/]+((/collection:[^/]+)?))$"
    )
    if not _SCOPE_RE.match(scope_key):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid scope_key {scope_key!r}. "
                "Expected: 'platform', 'catalog:<id>', or 'catalog:<id>/collection:<id>'."
            ),
        )

    # IAM-optional: the iam.applied_presets table only exists when the
    # IAM extension is installed.  Fail with 503 rather than 500 so the
    # frontend can surface a clear "IAM not installed" message.
    if get_protocol(IamService) is None:
        raise HTTPException(
            status_code=503,
            detail="IAM extension not loaded; applied_presets history is unavailable.",
        )

    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.modules.iam.applied_presets_service import (
        AppliedPresetsService,
        _VALID_STATES,
        _decode_cursor,
    )

    if state not in _VALID_STATES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown state {state!r}. Allowed: {sorted(_VALID_STATES)}",
        )

    if cursor is not None:
        try:
            _decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    db_proto = get_protocol(DatabaseProtocol)
    if db_proto is None:
        raise HTTPException(
            status_code=503,
            detail="Database protocol unavailable.",
        )
    engine = db_proto.engine

    svc = AppliedPresetsService(engine)
    rows, next_cursor = await svc.list_for_scope(
        scope_key=scope_key,
        state=state,
        cursor=cursor,
        limit=limit,
    )

    def _ts_iso(val: object) -> Optional[str]:  # type: ignore[reportReturnType]
        """Convert a datetime-like or string timestamp to ISO 8601 string."""
        if val is None:
            return None
        if isinstance(val, str):
            return val
        iso = getattr(val, "isoformat", None)
        if callable(iso):
            return str(iso())
        return str(val)

    def _row_response(r: dict) -> AppliedRowResponse:
        import json as _json

        snap = r.get("params_snapshot")
        if isinstance(snap, str):
            try:
                snap = _json.loads(snap)
            except Exception:
                snap = None
        return AppliedRowResponse(
            preset_name=r["preset_name"],
            scope_key=r["scope_key"],
            state=r["state"],
            applied_at=_ts_iso(r.get("applied_at")),
            applied_by=str(r["applied_by"]) if r.get("applied_by") else None,
            params_snapshot=snap,
            last_error=r.get("last_error"),
            updated_at=_ts_iso(r.get("updated_at")),
        )

    return AppliedPresetsPage(
        rows=[_row_response(r) for r in rows],
        next_cursor=next_cursor,
    )


@router.get("/presets/{preset_name}", summary="Get a single preset definition")
async def get_preset_detail(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    preset_name: str,
):
    from dynastore.modules.storage.presets import get_preset, search_presets

    try:
        get_preset(preset_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    result = search_presets(name=preset_name, limit=1)
    items = result.get("items", [])
    return items[0] if items else {}


@router.get(
    "/presets/{preset_name}/schema",
    summary="JSON Schema for a preset's params model (A7)",
)
async def get_preset_params_schema(preset_name: str) -> Dict[str, Any]:
    """Return the JSON Schema for ``preset.params_model``.

    * A preset with a real params model → ``model_json_schema()``
    * ``NoParams`` or absent ``params_model`` → ``{"type": "object", "properties": {}}``
    * Unknown preset name → 404
    """
    from dynastore.modules.storage.presets import get_preset
    from dynastore.modules.storage.presets.bundle_preset import NoParams

    try:
        preset = get_preset(preset_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    params_model = getattr(preset, "params_model", None)
    _empty: Dict[str, Any] = {"type": "object", "properties": {}}
    if params_model is None or params_model is NoParams:
        return _empty
    try:
        return params_model.model_json_schema()
    except Exception:
        return _empty


# ----- Platform tier: /configs/presets/{name} ---------------------------


@router.post(
    "/presets/{preset_name}",
    summary="Apply a platform-tier preset (#972)",
)
async def apply_platform_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    preset_name: str,
    force: bool = Query(
        False,
        description=(
            "Replace an already-applied preset whose stored params "
            "snapshot differs from this request (otherwise a mismatch "
            "returns 409)."
        ),
    ),
    params_body: Optional[Dict[str, Any]] = Body(
        default=None,
        description=(
            "Optional preset params, validated against the preset's "
            "params_model. Omit (or send an empty body) to apply the "
            "preset's defaults."
        ),
    ),
):
    """Apply a ``PLATFORM``-tier preset. Returns 409 if ``preset_name``
    declares a non-platform tier."""
    from dynastore.modules.storage.presets import PresetTier

    preset = _resolve_preset_for_scope(preset_name, PresetTier.PLATFORM)
    params = _coerce_params(preset, params_body)
    return await _apply_preset_bundle(preset, {}, force=force, params=params)


@router.delete(
    "/presets/{preset_name}",
    summary="Rollback a platform-tier preset (#972)",
)
async def unapply_platform_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    preset_name: str,
):
    from dynastore.modules.storage.presets import PresetTier

    preset = _resolve_preset_for_scope(preset_name, PresetTier.PLATFORM)
    return await _unapply_preset_bundle(preset, {})


# ----- Catalog tier: /configs/catalogs/{cat}/presets/{name} -------------
# Existing #847/#971 contract. Also reachable by items/assets presets
# that declare ``catalog_scopable``.


@router.post(
    "/catalogs/{catalog_id}/presets/{preset_name}",
    summary="Apply a preset to a catalog (#847)",
)
async def apply_catalog_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    catalog_id: str,
    preset_name: str,
    force: bool = Query(
        False,
        description=(
            "Replace an already-applied preset whose stored params "
            "snapshot differs from this request (otherwise a mismatch "
            "returns 409)."
        ),
    ),
    params_body: Optional[Dict[str, Any]] = Body(
        default=None,
        description=(
            "Optional preset params, validated against the preset's "
            "params_model. Omit (or send an empty body) to apply the "
            "preset's defaults."
        ),
    ),
):
    """Apply ``preset_name`` to ``catalog_id`` by walking the bundle
    through the standard ``ConfigsProtocol.set_config`` lifecycle.

    Each slot is applied at the catalog tier; the cascade validators
    (#960 scope 4 / items + collection) catch mixed public/private
    combos and the per-config validators run via ``set_config``. The
    endpoint does not bypass any validation — a preset is just a named
    bundle of standard ``set_config`` calls. Returns 409 if the preset
    is not reachable at the catalog scope (e.g. a collection-only
    preset).

    ``params_body`` lets a caller drive the preset (e.g. the FAO STAC
    migration POSTing ``{"stac_level": "items", "stac_storage": "ES"}`` to
    the ``stac_storage`` preset); omitted, the preset's defaults apply.
    """
    from dynastore.modules.storage.presets import PresetTier

    await _assert_catalog_exists(catalog_id)
    preset = _resolve_preset_for_scope(preset_name, PresetTier.CATALOG)
    params = _coerce_params(preset, params_body)
    return await _apply_preset_bundle(
        preset, {"catalog_id": catalog_id}, force=force, params=params
    )


@router.delete(
    "/catalogs/{catalog_id}/presets/{preset_name}",
    summary="Rollback a preset (#971)",
)
async def unapply_catalog_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    catalog_id: str,
    preset_name: str,
):
    """Rollback ``preset_name`` from ``catalog_id`` leniently per slot.

    Walks slots leaf-first (items template → collection template →
    catalog routing → audiences). For each slot:
    - matches preset emission → deleted, listed in ``deleted``.
    - diverged from preset emission → left in place, listed in
      ``skipped`` with ``reason: diverged`` plus the persisted/expected
      payload so operators can audit.
    - missing → listed in ``skipped`` with ``reason: missing``.

    Operators who edited a slot via REST after apply keep their edits;
    revoke removes only what still matches what the preset wrote.
    """
    from dynastore.modules.storage.presets import PresetTier

    await _assert_catalog_exists(catalog_id)
    preset = _resolve_preset_for_scope(preset_name, PresetTier.CATALOG)
    return await _unapply_preset_bundle(preset, {"catalog_id": catalog_id})


# ----- Collection tier: /configs/catalogs/{cat}/collections/{col}/... ---
# Reachable by COLLECTION-tier presets and by items/assets presets at
# collection scope (#972).


@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/presets/{preset_name}",
    summary="Apply a preset to a collection (#972)",
)
async def apply_collection_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    catalog_id: str,
    collection_id: str,
    preset_name: str,
    force: bool = Query(
        False,
        description=(
            "Replace an already-applied preset whose stored params "
            "snapshot differs from this request (otherwise a mismatch "
            "returns 409)."
        ),
    ),
    params_body: Optional[Dict[str, Any]] = Body(
        default=None,
        description=(
            "Optional preset params, validated against the preset's "
            "params_model. Omit (or send an empty body) to apply the "
            "preset's defaults."
        ),
    ),
):
    """Apply ``preset_name`` at the collection scope. Returns 409 if the
    preset is not reachable at the collection scope (e.g. a catalog-tier
    preset). ``params_body`` drives the preset; omitted, defaults apply."""
    from dynastore.modules.storage.presets import PresetTier

    await _assert_catalog_exists(catalog_id)
    await _assert_collection_exists(catalog_id, collection_id)
    preset = _resolve_preset_for_scope(preset_name, PresetTier.COLLECTION)
    params = _coerce_params(preset, params_body)
    return await _apply_preset_bundle(
        preset,
        {"catalog_id": catalog_id, "collection_id": collection_id},
        force=force,
        params=params,
    )


@router.delete(
    "/catalogs/{catalog_id}/collections/{collection_id}/presets/{preset_name}",
    summary="Rollback a collection preset (#972)",
)
async def unapply_collection_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    catalog_id: str,
    collection_id: str,
    preset_name: str,
):
    from dynastore.modules.storage.presets import PresetTier

    await _assert_catalog_exists(catalog_id)
    await _assert_collection_exists(catalog_id, collection_id)
    preset = _resolve_preset_for_scope(preset_name, PresetTier.COLLECTION)
    return await _unapply_preset_bundle(
        preset, {"catalog_id": catalog_id, "collection_id": collection_id}
    )


# ----- Dry-run endpoints (all three scopes) ------------------------------


@router.post(
    "/presets/{preset_name}/dry-run",
    summary="Dry-run a platform-tier preset — returns plan without writes",
)
async def dry_run_platform_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    preset_name: str,
    params_body: Optional[Dict[str, Any]] = Body(
        default=None,
        description=(
            "Optional preset params, validated against the preset's "
            "params_model. Omit (or send an empty body) to preview the "
            "preset's defaults."
        ),
    ),
):
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.modules.storage.presets import PresetTier
    from dynastore.modules.storage.presets.lifecycle import _build_context, dry_run_preset as _dry_run

    preset = _resolve_preset_for_scope(preset_name, PresetTier.PLATFORM)
    params = _coerce_params(preset, params_body) or preset.params_model()
    db_proto = get_protocol(DatabaseProtocol)
    engine = db_proto.engine if db_proto else None
    ctx = _build_context(engine, principal=None, scope="platform")
    plan = await _dry_run(preset_name, "platform", params, ctx)
    return {
        "preset_name": plan.preset_name,
        "scope_key": plan.scope_key,
        "entries": [
            {"kind": e.kind, "target": e.target, "detail": e.detail}
            for e in plan.entries
        ],
        "warnings": list(plan.warnings),
    }


@router.post(
    "/catalogs/{catalog_id}/presets/{preset_name}/dry-run",
    summary="Dry-run a catalog-tier preset — returns plan without writes",
)
async def dry_run_catalog_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    catalog_id: str,
    preset_name: str,
    params_body: Optional[Dict[str, Any]] = Body(
        default=None,
        description=(
            "Optional preset params, validated against the preset's "
            "params_model. Omit (or send an empty body) to preview the "
            "preset's defaults."
        ),
    ),
):
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.modules.storage.presets import PresetTier
    from dynastore.modules.storage.presets.lifecycle import _build_context, dry_run_preset as _dry_run

    await _assert_catalog_exists(catalog_id)
    preset = _resolve_preset_for_scope(preset_name, PresetTier.CATALOG)
    params = _coerce_params(preset, params_body) or preset.params_model()
    scope_key = f"catalog:{catalog_id}"
    db_proto = get_protocol(DatabaseProtocol)
    engine = db_proto.engine if db_proto else None
    ctx = _build_context(engine, principal=None, scope=scope_key)
    plan = await _dry_run(preset_name, scope_key, params, ctx)
    return {
        "preset_name": plan.preset_name,
        "scope_key": plan.scope_key,
        "entries": [
            {"kind": e.kind, "target": e.target, "detail": e.detail}
            for e in plan.entries
        ],
        "warnings": list(plan.warnings),
    }


@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/presets/{preset_name}/dry-run",
    summary="Dry-run a collection-tier preset — returns plan without writes",
)
async def dry_run_collection_preset(
    request: Request,  # type: ignore[reportGeneralTypeIssues]
    catalog_id: str,
    collection_id: str,
    preset_name: str,
    params_body: Optional[Dict[str, Any]] = Body(
        default=None,
        description=(
            "Optional preset params, validated against the preset's "
            "params_model. Omit (or send an empty body) to preview the "
            "preset's defaults."
        ),
    ),
):
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.modules.storage.presets import PresetTier
    from dynastore.modules.storage.presets.lifecycle import _build_context, dry_run_preset as _dry_run

    await _assert_catalog_exists(catalog_id)
    await _assert_collection_exists(catalog_id, collection_id)
    preset = _resolve_preset_for_scope(preset_name, PresetTier.COLLECTION)
    params = _coerce_params(preset, params_body) or preset.params_model()
    scope_key = f"catalog:{catalog_id}/collection:{collection_id}"
    db_proto = get_protocol(DatabaseProtocol)
    engine = db_proto.engine if db_proto else None
    ctx = _build_context(engine, principal=None, scope=scope_key)
    plan = await _dry_run(preset_name, scope_key, params, ctx)
    return {
        "preset_name": plan.preset_name,
        "scope_key": plan.scope_key,
        "entries": [
            {"kind": e.kind, "target": e.target, "detail": e.detail}
            for e in plan.entries
        ],
        "warnings": list(plan.warnings),
    }
