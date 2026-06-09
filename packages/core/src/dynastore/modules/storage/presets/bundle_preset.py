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

"""``BundlePreset`` — base class for build-based routing presets.

A ``BundlePreset`` subclass implements a single ``build(**scope) ->
PresetBundle`` factory; the base class supplies the generalised
``apply`` / ``revoke`` / ``dry_run`` lifecycle on top of it. ``apply``
walks the bundle and writes each entry through
``ConfigsProtocol.set_config``; ``revoke`` reverses it with a
byte-comparison divergence guard — an entry whose persisted row was
changed by the operator after apply is skipped on revoke (and a warning
is logged) so the operator's edit is never silently clobbered.

Two optional hooks compose with the lifecycle:

* ``on_applied(**scope_kwargs)`` — called after the bundle entries are set.
* ``on_revoked(**scope_kwargs)`` — called after the bundle entries are
  deleted.

Both default to no-ops; subclasses override either as needed (e.g. the
``geoid`` preset registers per-catalog IAM policies in ``on_applied``).
Hooks are best-effort: an error is logged, never raised, so a hook failure
never aborts the surrounding apply/revoke. Both sync and async overrides
are accepted.

This base class replaces the former ``RoutingPreset`` protocol +
``RoutingPresetAdapter`` wrapper: presets subclass ``BundlePreset``
directly and expose the unified ``Preset`` interface natively, so the
registry stores them as-is with no auto-wrap step.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, ClassVar, Dict, List, Tuple, Type, cast

from pydantic import BaseModel

from .examples import PresetExample
from .preset import AppliedDescriptor, NoParams, PresetContext, PresetPlan, PresetPlanEntry
from .protocol import PresetBundle, PresetTier

logger = logging.getLogger(__name__)

_Scope = Dict[str, str]


def _scope_to_kwargs(scope: str) -> _Scope:
    """Convert a scope string like ``"catalog:cat-7"`` to a kwargs dict."""
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
    parts: List[str] = []
    if "catalog_id" in scope_kwargs:
        parts.append(f"catalog:{scope_kwargs['catalog_id']}")
    if "collection_id" in scope_kwargs:
        parts.append(f"collection:{scope_kwargs['collection_id']}")
    return "/".join(parts)


class BundlePreset:
    """Base class for presets defined by a ``build() -> PresetBundle`` factory.

    Subclasses set the class-level metadata (``name`` / ``description`` /
    ``tier`` / ``catalog_scopable``) and implement ``build``. The base
    supplies the generalised ``Preset`` interface — ``dry_run`` /
    ``apply`` / ``revoke`` — so a ``BundlePreset`` subclass is registered
    and dispatched exactly like any other preset.
    """

    name: ClassVar[str]
    description: ClassVar[str]
    keywords: ClassVar[Tuple[str, ...]] = ("routing",)
    tier: ClassVar[PresetTier]
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    examples: ClassVar[Tuple[PresetExample, ...]] = ()

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    def build(self, **scope: str) -> PresetBundle:
        """Return the bundle of config entries this preset applies.

        Subclasses MUST override. Accept ``**scope`` (or the precise
        keywords your tier needs) and ignore what you do not consume.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.build must be overridden"
        )

    def _build_bundle(self, params: BaseModel, scope_kwargs: _Scope) -> PresetBundle:
        """Resolve the bundle for ``params`` + scope.

        Default ignores ``params`` (param-less presets) and delegates to
        :meth:`build`. Parameterised presets override this to thread their
        validated params into the bundle factory; the base
        ``dry_run`` / ``apply`` / ``revoke`` then carry the params through
        uniformly. On revoke the params are reconstructed from the persisted
        descriptor (see ``apply`` / ``revoke``) so a preset applied with
        non-default params is revoked against the SAME bundle it created.
        """
        return self.build(**scope_kwargs)

    async def on_applied(self, **scope_kwargs: str) -> None:
        """Hook called after the bundle entries are applied. No-op by default."""

    async def on_revoked(self, **scope_kwargs: str) -> None:
        """Hook called after the bundle entries are revoked. No-op by default."""

    async def _fire_hook(self, hook_name: str, scope_kwargs: _Scope) -> None:
        """Call ``on_applied`` / ``on_revoked`` best-effort.

        Errors are logged, never raised — a hook failure must not abort the
        surrounding apply/revoke. Both sync and async overrides are accepted.
        """
        hook = getattr(self, hook_name)
        try:
            result = hook(**scope_kwargs)
            if isinstance(result, Awaitable):
                await result
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "%s hook error preset=%s scope=%s: %s",
                hook_name, self.name, scope_kwargs, exc, exc_info=True,
            )

    # ------------------------------------------------------------------
    # Generalised Preset interface
    # ------------------------------------------------------------------

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,  # noqa: ARG002
    ) -> PresetPlan:
        scope_kwargs = _scope_to_kwargs(scope)
        bundle = self._build_bundle(params, scope_kwargs)
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
        params: BaseModel,
        scope: str,
        ctx: PresetContext,  # noqa: ARG002
    ) -> AppliedDescriptor:
        from dynastore.modules import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        scope_kwargs = _scope_to_kwargs(scope)
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            raise RuntimeError("ConfigsProtocol not available.")

        bundle = self._build_bundle(params, scope_kwargs)
        configs_any = cast(Any, configs)

        for entry in bundle.iter_apply():
            entry_scope = {**scope_kwargs, **dict(entry.scope)}
            await configs_any.set_config(entry.config_cls, entry.instance, **entry_scope)

        await self._fire_hook("on_applied", scope_kwargs)

        # Snapshot the bundle for revoke — store entry data by slot.
        snapshot: Dict[str, Any] = {}
        for entry in bundle.iter_apply():
            snapshot[entry.slot] = {
                "config_cls": entry.config_cls.__name__,
                "instance": json.loads(entry.instance.model_dump_json()),
                "scope": dict(entry.scope),
                "rollback_priority": entry.rollback_priority,
            }

        # Persist the validated params so revoke reconstructs the SAME bundle
        # (parameterised presets — e.g. the STAC preset — emit different
        # entries per params; a param-less preset stores ``{}`` and is
        # unaffected).
        return AppliedDescriptor(
            payload={
                "slots": snapshot,
                "scope": scope,
                "params": params.model_dump(mode="json"),
            }
        )

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,  # noqa: ARG002
    ) -> None:
        from dynastore.modules import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        scope_kwargs = _scope_to_kwargs(applied_descriptor.payload.get("scope", "platform"))
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            raise RuntimeError("ConfigsProtocol not available.")

        # Reconstruct the params used at apply so a parameterised preset is
        # revoked against the same entries it created (param-less presets
        # validate to an empty params model and rebuild deterministically).
        params = self.params_model.model_validate(
            applied_descriptor.payload.get("params") or {}
        )
        bundle = self._build_bundle(params, scope_kwargs)
        diverged: List[dict] = []
        to_delete: List[tuple] = []

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

        await self._fire_hook("on_revoked", scope_kwargs)
        return None
