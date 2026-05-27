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

"""Curated composite presets — auto-register on import.

Each composite is registered in a try/except block so that a missing child
preset (e.g. because the IAM extension is not installed) prevents only that
composite from registering.  The remaining composites continue to register
normally.  A single info-level log line is emitted per skipped composite,
naming the missing child preset(s), so operators can diagnose the gap.

Extensions that register the last dependency of a composite (e.g. the IAM
extension registering ``iam_baseline``) should call
``retry_composite_registration()`` after their own preset registrations so
that composites which failed at initial import time get a second attempt.

PR-4 of umbrella #1412.
"""
from __future__ import annotations

import logging
from typing import List, Tuple, Type

from dynastore.modules.storage.presets.preset import CompositePreset
from dynastore.modules.storage.presets.registry import _REGISTRY, register_preset

from .platform_demo import PlatformDemo
from .private_tenant import PrivateTenant
from .public_open_data import PublicOpenData

logger = logging.getLogger(__name__)

_COMPOSITES: Tuple[Type[CompositePreset], ...] = (
    PlatformDemo,
    PublicOpenData,
    PrivateTenant,
)


def _try_register(cls: Type[CompositePreset]) -> bool:
    """Attempt to register a composite; return True on success.

    Skips silently if already registered (idempotent retry path).
    Logs at info level and returns False when a child preset is missing.
    """
    if cls.name in _REGISTRY:
        return True
    try:
        register_preset(cls())
        return True
    except ValueError as exc:
        logger.info("composite preset %r not registered: %s", cls.name, exc)
        return False


def retry_composite_registration() -> List[str]:
    """Retry registration for any composite that was not registered on import.

    Call this after registering extension presets that composites depend on
    (e.g. after the IAM extension registers ``iam_baseline``).  Already-
    registered composites are skipped.

    Returns the list of composite names that are now registered.
    """
    registered = []
    for cls in _COMPOSITES:
        if _try_register(cls):
            registered.append(cls.name)
    return registered


# Initial registration attempt at import time.  Composites whose children
# are not yet in the registry (e.g. IAM extension not yet imported) will
# log an info line and skip — call retry_composite_registration() later.
for _cls in _COMPOSITES:
    _try_register(_cls)

__all__ = [
    "PlatformDemo",
    "PrivateTenant",
    "PublicOpenData",
    "retry_composite_registration",
]
