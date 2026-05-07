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

"""Per-request driver cache (L4 context var).

FastAPI middleware (or a dependency) should call ``init_request_driver_cache()``
at request start and ``clear_request_driver_cache(token)`` at response end.
The dict is GC-eligible as soon as the context var token is reset.

Cross-worker cache coherence is handled by the tiered @cached decorator (L1+L2).

Usage::

    # In FastAPI middleware / dependency:
    token = init_request_driver_cache()
    try:
        yield
    finally:
        clear_request_driver_cache(token)

    # In router (inside get_driver / resolve_drivers):
    cache = get_request_driver_cache()
    key = (operation, catalog_id, collection_id, hint)
    if key not in cache:
        cache[key] = await _resolve(...)
    return cache[key]
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any, Dict, Optional, Tuple

# Maps (operation, catalog_id, collection_id, hint) -> List[ResolvedDriver]
_REQUEST_DRIVER_CACHE: ContextVar[Optional[Dict[Tuple, Any]]] = ContextVar(
    "request_driver_cache", default=None
)


def init_request_driver_cache() -> Token:
    """Initialise a fresh per-request driver cache for the current task context.

    Returns the ``Token`` needed to reset the context var; pass it to
    :func:`clear_request_driver_cache` in the response teardown path.
    """
    return _REQUEST_DRIVER_CACHE.set({})


def clear_request_driver_cache(token: Token) -> None:
    """Reset the per-request cache to its pre-request state."""
    _REQUEST_DRIVER_CACHE.reset(token)


def get_request_driver_cache() -> Dict[Tuple, Any]:
    """Return the per-request driver cache dict, or an empty sentinel if not initialised.

    The returned dict is mutable; callers may store resolved drivers in it.
    If middleware has not initialised the cache, returns a standalone empty dict
    (hits are still correct, but are not shared across calls in the same request).
    """
    cache = _REQUEST_DRIVER_CACHE.get()
    if cache is None:
        return {}
    return cache
