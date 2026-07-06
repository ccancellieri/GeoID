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

"""Process-global draining flag for this worker (geoid#2946 / #2924).

Set when this worker process has decided to gracefully recycle itself ahead
of an OOM kill (the memory watchdog's self-recycle lever — see
``tools/memory_watchdog.py``): ``/ready`` and the readiness-shed middleware
both consult it to steer new traffic away from a worker that is about to
receive ``SIGTERM``.

Deliberately a plain module-level flag, not an ``asyncio.Event`` — it needs
to be read from the ordinary synchronous ``/ready`` handler code path just
as readily as from async code, with no event loop or ``await`` required.
Scoped to this process only: each gunicorn worker is its own process with
its own copy of this module's state, so setting it never affects sibling
workers or other instances.
"""

from __future__ import annotations

_is_draining = False


def is_draining() -> bool:
    """Return whether this worker has flagged itself as draining."""
    return _is_draining


def set_draining() -> None:
    """Flag this worker as draining (about to gracefully recycle)."""
    global _is_draining
    _is_draining = True


def clear_draining() -> None:
    """Clear the draining flag (e.g. a recycle attempt was aborted)."""
    global _is_draining
    _is_draining = False


__all__ = ["is_draining", "set_draining", "clear_draining"]
