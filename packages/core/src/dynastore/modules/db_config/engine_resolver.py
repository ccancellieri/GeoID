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

"""Engine snapshot resolver — sync ``engine_ref → EngineConfig`` lookup.

:class:`~dynastore.modules.db_config.engine_instance_cache.EngineInstanceCache`
takes a synchronous ``engine_resolver`` callable.
:class:`~dynastore.modules.db_config.platform_config_service.PlatformConfigService.get_config`
is async — so we build a *snapshot* of the platform-tier engine configs at
:meth:`DBConfigModule.lifespan` start and serve from that.

Snapshot semantics (Cycle F.6):

- Walks every concrete :class:`EngineConfig` subclass via
  :func:`~dynastore.modules.db_config.engine_registry.list_registered_engines`.
- Calls ``await pcfg.get_config(cls)`` once per kind; in the F.1
  single-instance-per-kind contract, that's all the engines that exist.
- Indexes the result both by ``class_key`` (e.g. ``postgresql_engine_config``)
  and by ``engine_class`` discriminator (e.g. ``postgresql_engine``) so
  default-deployment ref-naming conventions both resolve.
- F.4c will add per-scope ref-keyed storage (operator-chosen ref names like
  ``pg_main`` + ``pg_secondary``); this resolver is the seam where that
  widening happens.

Until F.4c lands no driver actually consumes the cache in production, but
the lifespan still constructs the cache so tests + admin tooling can
exercise the contract end-to-end.
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, Optional, TYPE_CHECKING

from dynastore.modules.db_config.engine_config import EngineConfig
from dynastore.modules.db_config.engine_registry import list_registered_engines

if TYPE_CHECKING:
    from dynastore.modules.db_config.platform_config_service import (
        PlatformConfigService,
    )

logger = logging.getLogger(__name__)


async def build_engine_snapshot(
    pcfg: "PlatformConfigService",
) -> Dict[str, EngineConfig]:
    """Snapshot the platform-tier engine configs into a ``ref → instance`` map.

    For every concrete :class:`EngineConfig` subclass, fetch the stored
    configuration (or zero-arg default if none stored) and index it under
    BOTH the ``class_key()`` and the ``engine_class`` discriminator so a
    driver referencing either form resolves correctly.

    Errors fetching a single engine config are logged + skipped — the cache
    serves what it can; missing entries surface as ``KeyError`` from
    :meth:`EngineInstanceCache.get` at first dispatch.
    """
    snapshot: Dict[str, EngineConfig] = {}
    for class_key, cls in list_registered_engines().items():
        try:
            config = await pcfg.get_config(cls)
        except Exception as exc:  # noqa: BLE001 — best-effort snapshot
            logger.warning(
                "build_engine_snapshot: failed to load %s (%s); skipping.",
                class_key, exc,
            )
            continue
        if not isinstance(config, EngineConfig):
            logger.warning(
                "build_engine_snapshot: %s returned non-EngineConfig %r; "
                "skipping.",
                class_key, type(config).__name__,
            )
            continue
        snapshot[class_key] = config
        # Mirror under the engine_class discriminator (e.g.
        # ``postgresql_engine``) so drivers that reference the kind rather
        # than the class_key resolve to the same instance.
        if config.engine_class and config.engine_class != class_key:
            snapshot[config.engine_class] = config
    return snapshot


def make_resolver(
    snapshot: Dict[str, EngineConfig],
) -> Callable[[str], Optional[EngineConfig]]:
    """Return a sync resolver closure over a snapshot dict.

    The returned callable matches the
    :class:`~dynastore.modules.db_config.engine_instance_cache.EngineInstanceCache`
    contract (``engine_ref → Optional[EngineConfig]``).  Treating unknown
    refs as ``None`` lets the cache surface a clear ``KeyError`` to callers
    rather than papering over typos with a default engine.
    """

    def _resolve(engine_ref: str) -> Optional[EngineConfig]:
        return snapshot.get(engine_ref)

    return _resolve


__all__ = [
    "build_engine_snapshot",
    "make_resolver",
]
