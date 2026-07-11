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

"""Process-global cache warm-up readiness gate (geoid#3207).

A cold-boot autoscale instance joins with empty shared config caches
exactly when load is highest, so its first requests would pay the full
cache-rebuild cost synchronously. An extension that wants ``/ready`` to
hold off until its hot caches are warm calls :func:`run_warmup` as a
background task from its lifespan; :func:`is_warm` folds into the
aggregate readiness signal the same way ``dynastore.main.readiness_check``
already folds in ``dynastore.tools.serving_state.is_draining``.

Opt-in and best-effort by design:

- :func:`is_warm` defaults to ``True`` (never gates) until something
  actually registers a warm-up via :func:`run_warmup` -- deployments that
  don't use this stay unaffected.
- :func:`run_warmup` always resolves within its ``timeout``: an
  individual warm-up's own failure or timeout is caught and logged, never
  propagated, so a broken or slow cache leaves this instance un-warm for
  one probe cycle at most, never un-ready forever.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Set

logger = logging.getLogger(__name__)

_pending: Set[str] = set()


def is_warm() -> bool:
    """Return whether every registered warm-up has finished (or given up)."""
    return not _pending


async def run_warmup(name: str, awaitable: "Awaitable[None]", *, timeout: float) -> None:
    """Run one named warm-up best-effort, gating :func:`is_warm` meanwhile.

    Marks ``name`` pending synchronously before the first ``await`` so a
    concurrent ``/ready`` call can never observe a warm-up that has
    started but is not yet tracked. Always clears it in a ``finally`` --
    success, a raised exception, and a timeout all still resolve, so this
    worker can always reach readiness.
    """
    _pending.add(name)
    try:
        await asyncio.wait_for(awaitable, timeout=timeout)
    except TimeoutError:
        logger.warning(
            "readiness warm-up %r did not finish within %.1fs; "
            "reporting ready anyway (best-effort).",
            name, timeout,
        )
    except Exception:
        logger.warning(
            "readiness warm-up %r failed; reporting ready anyway (best-effort).",
            name, exc_info=True,
        )
    finally:
        _pending.discard(name)


def reset_for_testing() -> None:
    """Clear every pending warm-up. Test-only escape hatch."""
    _pending.clear()


__all__ = ["is_warm", "run_warmup", "reset_for_testing"]
