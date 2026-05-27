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

"""Adapter that wraps a legacy ``RoutingPreset`` as a new-style ``Preset``.

Every existing routing preset — ``public_catalog``, ``private_catalog``,
``defaults_postgres``, ``private_collection``, ``items_es_private``, and
``geoid`` — is a pure config-factory with no async lifecycle. The adapter
maps their ``build()`` → ``set_config()`` flow onto ``apply`` / ``revoke``
without touching any of the preset's own logic.

The adapter supports two optional hooks that compose symmetrically with the
existing ``on_applied`` hook some routing presets implement:

* ``on_applied`` — the existing hook on the wrapped preset is still called
  after ``set_config`` entries are applied. No change to existing behaviour.
* ``on_revoked`` — a new optional async callable ``(** scope_kwargs) → None``
  that is called after the config entries are deleted. The ``geoid`` preset
  registers per-catalog IAM policies via ``on_applied``; the adapter's
  ``on_revoked`` support lets the lifecycle layer pass a callable that
  reverses those policies on ``revoke``.

Revoke uses the byte-comparison guard from the existing
``_unapply_preset_bundle`` implementation: a slot whose persisted row
diverges from the expected shape is skipped on revoke (it was changed by
the operator after the preset was applied; the adapter records the
divergence in the ``AppliedDescriptor`` so the lifecycle layer can surface
it). This keeps existing rollback semantics unchanged.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable, ClassVar, Dict, Optional, Tuple, Type, cast

from pydantic import BaseModel

from .preset import AppliedDescriptor, NoParams, PresetContext, PresetPlan, PresetPlanEntry, TaskHandle
from .protocol import PresetTier, RoutingPreset

logger = logging.getLogger(__name__)

_Scope = Dict[str, str]


def _scope_to_kwargs(scope: str) -> _Scope:
    """Convert a scope string like ``"catalog:cat-7"`` to kwargs dict."""
    if scope == "platform":
        return {}
    parts = scope.split("/")
    kwargs: _Scope = {}
    for part in parts:
        if part.startswith("catalog:"):
            kwargs["catalog_id"] = part[len("catalog:"):]
        elif part.startswith("collection:"):
            kwargs["collection_id"] = part[len("collection:"):]
    return kwargs


def _scope_to_string(scope_kwargs: _Scope) -> str:
    """Inverse of ``_scope_to_kwargs``."""
    if not scope_kwargs:
        return "platform"
    parts: list[str] = []
    if "catalog_id" in scope_kwargs:
        parts.append(f"catalog:{scope_kwargs['catalog_id']}")
    if "collection_id" in scope_kwargs:
        parts.append(f"collection:{scope_kwargs['collection_id']}")
    return "/".join(parts)


class RoutingPresetAdapter:
    """Wraps a ``RoutingPreset`` instance as a new-style ``Preset``.

    The adapter is created once per routing preset during registry
    migration. Existing code that calls ``get_preset(name)`` and then
    ``preset.build(...)`` / ``preset.on_applied(...)`` continues to work
    because the adapter exposes ``build`` and ``on_applied`` by delegation.
    """

    is_async: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams

    def __init__(
        self,
        routing_preset: RoutingPreset,
        on_revoked: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._rp = routing_preset
        self._on_revoked = on_revoked

    # ------------------------------------------------------------------
    # Delegation of RoutingPreset fields / methods (back-compat surface)
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._rp.name

    @property
    def description(self) -> str:
        return self._rp.description  # type: ignore[return-value]

    @property
    def tier(self) -> PresetTier:
        return self._rp.tier  # type: ignore[return-value]

    @property
    def catalog_scopable(self) -> bool:
        return bool(getattr(self._rp, "catalog_scopable", False))

    @property
    def keywords(self) -> Tuple[str, ...]:
        return getattr(self._rp, "keywords", ("routing",))

    def build(self, **scope: str):  # type: ignore[override]
        return self._rp.build(**scope)

    async def on_applied(self, **scope_kwargs: str) -> None:
        hook = getattr(self._rp, "on_applied", None)
        if callable(hook):
            try:
                result = hook(**scope_kwargs)
                if isinstance(result, Awaitable):
                    await result
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "on_applied hook error preset=%s scope=%s: %s",
                    self.name, scope_kwargs, exc, exc_info=True,
                )

    # ------------------------------------------------------------------
    # New-style Preset interface
    # ------------------------------------------------------------------

    async def dry_run(
        self,
        params: BaseModel,  # noqa: ARG002 — routing presets take no params
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        scope_kwargs = _scope_to_kwargs(scope)
        bundle = self._rp.build(**scope_kwargs)
        entries = [
            PresetPlanEntry(
                kind="set_config",
                target=f"{entry.config_cls.__name__}[{entry.slot}]",
                detail={"scope": scope_kwargs, "rollback_priority": entry.rollback_priority},
            )
            for entry in bundle.iter_apply()
        ]
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(entries),
        )

    async def apply(
        self,
        params: BaseModel,  # noqa: ARG002
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor | TaskHandle:
        from dynastore.modules import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        scope_kwargs = _scope_to_kwargs(scope)
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            raise RuntimeError("ConfigsProtocol not available.")

        bundle = self._rp.build(**scope_kwargs)
        applied_slots: list[str] = []

        for entry in bundle.iter_apply():
            entry_scope = {**scope_kwargs, **dict(entry.scope)}
            configs_any = cast(Any, configs)
            await configs_any.set_config(entry.config_cls, entry.instance, **entry_scope)
            applied_slots.append(entry.slot)

        # Call the wrapped preset's on_applied hook if present.
        hook = getattr(self._rp, "on_applied", None)
        if callable(hook):
            try:
                result = hook(**scope_kwargs)
                if isinstance(result, Awaitable):
                    await result
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "on_applied hook error preset=%s scope=%s: %s",
                    self.name, scope_kwargs, exc, exc_info=True,
                )

        # Snapshot the bundle for revoke — store entry data by slot.
        snapshot: Dict[str, Any] = {}
        for entry in bundle.iter_apply():
            snapshot[entry.slot] = {
                "config_cls": entry.config_cls.__name__,
                "instance": json.loads(entry.instance.model_dump_json()),
                "scope": dict(entry.scope),
                "rollback_priority": entry.rollback_priority,
            }

        return AppliedDescriptor(payload={"slots": snapshot, "scope": scope})

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None | TaskHandle:
        from dynastore.modules import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        scope_kwargs = _scope_to_kwargs(applied_descriptor.payload.get("scope", "platform"))
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            raise RuntimeError("ConfigsProtocol not available.")

        bundle = self._rp.build(**scope_kwargs)
        diverged: list[dict] = []
        to_delete: list[tuple] = []

        configs_any = cast(Any, configs)
        for entry in bundle.iter_rollback():
            entry_scope = {**scope_kwargs, **dict(entry.scope)}
            persisted = await configs_any.get_persisted_config(entry.config_cls, **entry_scope)
            if persisted is None:
                continue

            try:
                persisted_norm = entry.config_cls.model_validate(persisted).model_dump(mode="json")
            except Exception:  # noqa: BLE001
                persisted_norm = persisted

            expected_norm = entry.instance.model_dump(mode="json")

            if persisted_norm == expected_norm:
                to_delete.append((entry.slot, entry.config_cls, entry_scope))
            else:
                diverged.append({
                    "slot": entry.slot,
                    "persisted": persisted_norm,
                    "expected": expected_norm,
                })
                logger.warning(
                    "preset=%s scope=%s slot=%s diverged on revoke — skipping",
                    self.name, scope_kwargs, entry.slot,
                )

        for _slot, cfg_cls, entry_scope in to_delete:
            await configs_any.delete_config(cfg_cls, **entry_scope)

        # Call the on_revoked hook if provided.
        if self._on_revoked is not None:
            try:
                result = self._on_revoked(**scope_kwargs)
                if isinstance(result, Awaitable):
                    await result
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "on_revoked hook error preset=%s scope=%s: %s",
                    self.name, scope_kwargs, exc, exc_info=True,
                )

        return None
