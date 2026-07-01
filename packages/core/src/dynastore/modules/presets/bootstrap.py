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

"""Cold-boot single-application helper for platform presets.

``bootstrap_preset_if_absent`` applies a named preset exactly once per
DB cluster on cold-boot, guarded by:

1. The platform bootstrap guard (``catalog.shared_properties`` key
   ``platform.bootstrap_initialized``) — skips all non-force presets on
   subsequent restarts after the first successful boot sequence.

2. A sentinel row in ``iam.applied_presets`` — idempotency guard for the
   preset itself, separate from the bootstrap guard so individual presets
   can self-heal independently.

3. A PostgreSQL advisory lock (``acquire_startup_lock``) — prevents two
   pods racing to first-boot from both applying the preset.

When ``force=True`` both guards are bypassed: the preset re-applies on
every call.  This is the ``public_access_baseline`` self-heal path that
keeps the Cloud Run ``/health`` startup probe alive without the sentinel
blocking re-application.

This module has no IAM or storage-driver imports.  It depends only on:
- ``modules/db_config/query_executor`` (DQLQuery, ResultHandler)
- ``modules/db_config/locking_tools`` (acquire_startup_lock)
- ``modules/catalog/bootstrap_guard`` (is_initialized)
- ``modules/storage/presets/registry`` (find_preset)
- stdlib / pydantic

Import rule: nothing in this module may import from ``modules/iam`` or
introduce new circular dependencies.  The sentinel table it reads is
``iam.applied_presets``; the physical table stays in the ``iam`` schema
because every deployed cluster already holds sentinel rows there and a
rename buys nothing — the ``configs.applied_presets`` view provides the
schema-neutral name for future migrations.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sentinel queries — read from the existing iam.applied_presets table.
# Table stays in the iam schema; a forward-compatibility view
# (configs.applied_presets) is created by the IAM bootstrap DDL alongside
# the physical table so that future schema migrations have a clean migration
# target without needing data movement now.
# ---------------------------------------------------------------------------

_SELECT_SENTINEL = DQLQuery(
    """
    SELECT 1 FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

_INSERT_SENTINEL = DQLQuery(
    """
    INSERT INTO iam.applied_presets
        (preset_name, scope_key, state, applied_at, applied_by,
         params_snapshot, revoke_descriptor, updated_at)
    VALUES
        (:preset_name, :scope_key, 'applied', NOW(), NULL,
         :params_snapshot, :revoke_descriptor, NOW())
    ON CONFLICT (preset_name, scope_key) DO NOTHING
    """,
    # INSERT … ON CONFLICT DO NOTHING returns no rows under ONE_OR_NONE.
    # ROWCOUNT is the write-query convention when no RETURNING clause is used.
    result_handler=ResultHandler.ROWCOUNT,
)


async def bootstrap_preset_if_absent(
    engine: Any,
    *,
    preset_name: str,
    scope_key: str = "platform",
    lock_key: Optional[str] = None,
    force: bool = False,
    params: Optional[Dict[str, Any]] = None,
) -> bool:
    """Apply a preset once per DB on cold-boot if no sentinel row exists.

    Uses the single platform bootstrap guard (``catalog.shared_properties``
    key ``platform.bootstrap_initialized``) to skip all non-force presets on
    subsequent restarts.  When ``force`` is ``True`` the guard is bypassed and
    the preset re-applies on every call — this is the ``public_access_baseline``
    self-heal path that gates the Cloud Run ``/health`` probe.

    Inside the advisory lock the guard is re-checked (double-checked locking)
    before skipping, so two pods racing to first-boot never both skip.

    Parameter resolution order for the params passed to ``preset.apply``:

    1. The explicit ``params`` keyword argument, if provided and non-None.
    2. ``load_preset_params(preset_name)`` from the file-based param loader.
    3. An empty dict ``{}``.

    The resolved dict is validated against ``preset.params_model`` before
    ``apply`` is called.  When validation fails, an ERROR is logged and
    ``False`` is returned — a bad seed file must not crash the platform.
    The validated model's JSON dump is stored in the sentinel row so that
    audit rows always reflect the actual params used.

    Returns ``True`` if the preset was applied this call, ``False`` if it was
    skipped (guard already set and ``force`` is False, lock timed out, preset
    not registered, or params validation failed).
    """
    from pydantic import ValidationError

    from dynastore.modules.db_config.locking_tools import acquire_startup_lock
    from dynastore.modules.db_config.query_executor import is_lock_not_available_error
    from dynastore.modules.storage.presets.preset import NoParams
    from dynastore.modules.storage.presets.registry import find_preset

    # Build the context here to avoid a circular import with lifecycle.py.
    def _build_ctx(eng: Any) -> Any:
        from dynastore.modules.storage.presets.lifecycle import _build_context
        return _build_context(eng, principal=None, scope=scope_key)

    _lock_key = lock_key or f"iam_seed:{preset_name}:{scope_key}"

    # A lock-timeout (PG 55P03) raises out of ``acquire_startup_lock`` rather
    # than yielding None, so it's caught here and folded into the same
    # skip-and-return-False path as the "already applied" / "conn is None"
    # cases below — a peer pod racing to first-boot is expected, not fatal
    # (#2625).
    try:
        async with acquire_startup_lock(engine, _lock_key) as conn:
            if conn is None:
                return False

            # Re-check bootstrap guard inside the lock (double-checked locking).
            # force=True bypasses the guard — load-bearing self-heal presets must
            # always run regardless of the guard state.
            if not force:
                from dynastore.modules.catalog.bootstrap_guard import is_initialized
                if await is_initialized(db_resource=conn):
                    logger.debug(
                        "bootstrap_preset_if_absent: bootstrap guard set — "
                        "skipping %r (force=False).",
                        preset_name,
                    )
                    return False

            row = await _SELECT_SENTINEL.execute(conn, preset_name=preset_name, scope_key=scope_key)
            if row is not None and not force:
                logger.debug(
                    "bootstrap_preset_if_absent: sentinel present for %r at %r — skipping",
                    preset_name,
                    scope_key,
                )
                return False
            if row is not None and force:
                logger.debug(
                    "bootstrap_preset_if_absent: sentinel present for %r at %r — "
                    "re-applying (force=True) to self-heal",
                    preset_name,
                    scope_key,
                )

            preset = find_preset(preset_name)
            if preset is None:
                logger.warning(
                    "bootstrap_preset_if_absent: preset %r not registered — skipping",
                    preset_name,
                )
                return False

            # Resolve params: explicit arg → file loader → empty dict.
            if params is not None:
                resolved_raw = params
            else:
                from dynastore.modules.presets.param_loader import load_preset_params
                file_params = load_preset_params(preset_name)
                resolved_raw = file_params if file_params is not None else {}

            # Validate against the preset's own params_model.
            params_model = getattr(preset, "params_model", None)
            try:
                if callable(params_model):
                    validated = params_model.model_validate(resolved_raw)
                else:
                    validated = NoParams.model_validate(resolved_raw)
            except ValidationError as exc:
                logger.error(
                    "bootstrap_preset_if_absent: params validation failed for %r: %s — skipping",
                    preset_name,
                    exc,
                )
                return False

            ctx = _build_ctx(engine)
            descriptor = await preset.apply(validated, scope_key, ctx)

            payload = descriptor.payload if hasattr(descriptor, "payload") else {}
            await _INSERT_SENTINEL.execute(
                conn,
                preset_name=preset_name,
                scope_key=scope_key,
                params_snapshot=json.dumps(validated.model_dump(mode="json")),
                revoke_descriptor=json.dumps(payload),
            )
            logger.info(
                "bootstrap_preset_if_absent: preset %r applied at scope %r on cold-boot",
                preset_name,
                scope_key,
            )
            return True
    except Exception as exc:
        if not is_lock_not_available_error(exc):
            raise
        logger.info(
            "bootstrap_preset_if_absent: %s timed out (another process is "
            "likely still applying it) — skipping %r: %s",
            _lock_key, preset_name, exc,
        )
        return False


async def bootstrap_presets(
    engine: Any,
    payloads: Any,
    *,
    default_scope_key: str = "platform",
) -> Dict[str, bool]:
    """Apply an ordered sequence of presets, continuing on per-preset failure.

    *payloads* is coerced via ``coerce_payloads`` — it accepts any shape
    that function accepts (single dict, chain dict, bare list, or already-built
    ``PresetPayload`` / ``PresetChainPayload`` instances).

    Each preset in the resolved list is applied sequentially via
    ``bootstrap_preset_if_absent``.  A failed or skipped preset is logged and
    the chain continues — partial application is preferred over full abort
    because boot resilience matters more than atomicity here.  Callers that
    need atomicity should use ``CompositePreset`` instead.

    ``payload.scope_key`` overrides ``default_scope_key`` per entry.

    Returns a dict mapping ``preset_name`` to the applied bool from
    ``bootstrap_preset_if_absent``.
    """
    from dynastore.modules.presets.payload import coerce_payloads

    preset_list = coerce_payloads(payloads)
    results: Dict[str, bool] = {}

    for entry in preset_list:
        scope = entry.scope_key if entry.scope_key != "platform" else default_scope_key
        try:
            applied = await bootstrap_preset_if_absent(
                engine,
                preset_name=entry.preset_name,
                scope_key=scope,
                force=entry.force,
                params=entry.params or None,
            )
        except Exception as exc:
            logger.error(
                "bootstrap_presets: preset %r raised unexpectedly — continuing chain: %s",
                entry.preset_name,
                exc,
            )
            applied = False
        results[entry.preset_name] = applied

    return results
