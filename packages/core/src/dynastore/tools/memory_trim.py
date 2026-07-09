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

# dynastore/tools/memory_trim.py

"""Return freed heap pages to the OS after large allocation bursts.

glibc's allocator keeps pages freed by a large, short-lived allocation
burst (e.g. a storage-drain run's JSONB/GeoJSON decode transients, #3121)
cached in its malloc arenas instead of returning them to the kernel, so the
worker's RSS stays pinned at the burst peak — and the next burst stacks on
top of whatever the allocator retained. ``malloc_trim(0)`` walks the arenas
and releases whatever can be handed back.

glibc-only by nature: a no-op (``False``) on non-Linux platforms and on
Linux libcs without ``malloc_trim`` (musl).
"""
from __future__ import annotations

import ctypes
import logging
import sys

logger = logging.getLogger(__name__)


def trim_malloc_arenas() -> bool:
    """Best-effort ``malloc_trim(0)``.

    Returns True if the allocator reported memory was released, False on
    any failure or unsupported platform — never raises.
    """
    if not sys.platform.startswith("linux"):
        return False
    try:
        libc = ctypes.CDLL("libc.so.6")
        released = bool(libc.malloc_trim(0))
    except Exception as exc:  # noqa: BLE001 — memory hygiene must never break a run
        logger.debug("malloc_trim unavailable: %s", exc)
        return False
    if released:
        logger.debug("malloc_trim(0) released retained heap pages to the OS")
    return released
