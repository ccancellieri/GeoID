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

"""Shared helpers for provisioning task implementations.

Thin wrappers over protocol lookups and hook invocation used by
``gcp_provision`` and ``catalog_provision``.
"""

from __future__ import annotations

import inspect
from typing import Any, Optional

from dynastore.modules import get_protocol
from dynastore.modules.concurrency import run_in_thread
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.protocol_helpers import resolve


def _get_catalog_protocol() -> CatalogsProtocol:
    """Return the live CatalogsProtocol instance or raise RuntimeError.

    Named, mockable seam for the provisioning tasks; delegates to the generic
    fail-fast ``resolve`` so the not-available error path lives in one place.
    """
    return resolve(CatalogsProtocol)


async def call_hook(fn: Any, **kwargs: Any) -> Any:
    """Invoke a provisioner hook, awaiting it if it is a coroutine function.

    Sync hooks run off the event loop via ``run_in_thread`` — provisioning
    tasks (``catalog_provision``) share the dispatcher's event loop with
    ``BatchedHeartbeat._beat_loop``, so a blocking sync hook (e.g. a sync GCP
    SDK call) would otherwise starve heartbeats and let the lease lapse mid-run.

    Args:
        fn:      Callable to invoke.  May be sync or async.
        **kwargs: Keyword arguments forwarded to ``fn``.

    Returns:
        The return value of ``fn``.
    """
    if inspect.iscoroutinefunction(fn):
        return await fn(**kwargs)
    return await run_in_thread(fn, **kwargs)


async def get_tasks_config() -> Optional[Any]:
    """Return the live ``TasksPluginConfig`` instance, or ``None`` on failure.

    Fail-open: any exception (protocol not registered, config absent) returns
    ``None`` so callers can apply their own defaults without surfacing an error.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig

        mgr = get_protocol(PlatformConfigsProtocol)
        if mgr is not None:
            cfg = await mgr.get_config(TasksPluginConfig)
            if isinstance(cfg, TasksPluginConfig):
                return cfg
    except Exception:  # noqa: BLE001 — best-effort; callers use defaults
        pass
    return None
