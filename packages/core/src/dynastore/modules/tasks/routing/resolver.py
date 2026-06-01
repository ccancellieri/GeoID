"""Single entry points the claim path and execution engine consult.

Fail-open: any failure to load the routing config returns an empty target
list ("no opinion"), and the caller falls back to capable-set derivation.
A fresh or degraded registry can never make a pod refuse to claim.

``select_target`` is a pure function with no I/O: it accepts the preloaded
target list (from ``resolved_targets``) plus caller-supplied predicates and
returns the best-matching ``RunnerTarget`` or ``None`` when no viable entry
exists.  The injected ``can_handle`` callable keeps the function testable in
isolation -- no runner-registry import required here.
"""
from __future__ import annotations

import logging
from typing import Callable, FrozenSet, List, Optional

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.model import RunnerTarget, TaskRoutingConfig

logger = logging.getLogger(__name__)


async def _load_config() -> TaskRoutingConfig:
    """Load the platform-tier TaskRoutingConfig, fail-open to an empty default.

    Uses PlatformConfigsProtocol when available; falls back to
    TaskRoutingConfig() (which fills the registry-derived defaults via
    _materialize_if_empty) when the protocol is absent or returns an
    unexpected type.
    """
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    config_mgr = get_protocol(PlatformConfigsProtocol)
    if config_mgr is None:
        return TaskRoutingConfig()
    cfg = await config_mgr.get_config(TaskRoutingConfig)
    return cfg if isinstance(cfg, TaskRoutingConfig) else TaskRoutingConfig()


async def resolved_targets(task_key: str) -> List[RunnerTarget]:
    """Return the ordered RunnerTarget list for task_key, fail-open to [].

    A failure to load the config (network error, DB unavailable, corrupt config)
    returns [] and logs a WARNING so the caller can degrade gracefully --
    a fail-open contract: a degraded registry never makes a pod refuse to claim.
    """
    try:
        cfg = await _load_config()
        return cfg.resolved_targets(task_key)
    except Exception:
        logger.warning(
            "task routing: resolver failed for %r -- failing open",
            task_key,
            exc_info=True,
        )
        return []


def select_target(
    targets: List[RunnerTarget],
    request_hints: FrozenSet[ExecHint],
    this_service: str,
    can_handle: Callable[[str], bool],
) -> Optional[RunnerTarget]:
    """Select the best RunnerTarget from an ordered candidate list.

    Mirrors storage/router.py::_resolve_driver_ids_cached hint-matching
    semantics:

    * A target matches iff:
        1. not target.consumers (any service) OR this_service in target.consumers
        2. can_handle(target.runner) returns True
        3. request_hints.issubset(effective_hints(target))
    * Effective hints: target.hints when non-empty; otherwise the full
      ExecHint universe (target claims to serve any hint -- same semantics as
      an empty OperationDriverEntry.hints).
    * Empty request_hints: skip the superset check (all viable targets match).
    * Tie-break: sort by (-len(effective_hints), position) so the most
      specific target (longest declared hint surface) wins; position is the
      final deterministic tiebreaker.
    * Returns None when no target matches (fail-open: caller decides the
      fallback).

    Args:
        targets:       Ordered list from resolved_targets or a config entry.
        request_hints: Frozenset of ExecHint values the caller requested.
        this_service:  Logical service name of the calling pod (e.g. "catalog").
        can_handle:    Injected callable (runner_type: str) -> bool; no
                       runner-registry import required in this module.
    """
    _all_hints: FrozenSet[ExecHint] = frozenset(ExecHint)

    def _effective_hints(t: RunnerTarget) -> FrozenSet[ExecHint]:
        return frozenset(t.hints) if t.hints else _all_hints

    def _matches(t: RunnerTarget) -> bool:
        if t.consumers and this_service not in t.consumers:
            return False
        if not can_handle(t.runner):
            return False
        if request_hints:
            return request_hints.issubset(_effective_hints(t))
        return True

    matched = [
        (i, t, _effective_hints(t))
        for i, t in enumerate(targets)
        if _matches(t)
    ]
    if not matched:
        return None

    # Sort: longest effective-hint surface first, original position as tiebreaker.
    matched.sort(key=lambda triple: (-len(triple[2]), triple[0]))
    return matched[0][1]
