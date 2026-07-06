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

"""Bounded, byte-budget async tile-cache writer.

Interactive (non-preseeded) tile rendering persists the rendered bytes via a
best-effort background write. Handing that write to FastAPI's
``BackgroundTasks`` directly is unbounded: concurrent bucket PUTs and pinned
``tile_bytes`` in RAM grow without limit under load, and any write still
in flight at SIGTERM is simply killed (a silent cache miss, self-healed by
the next request's re-render).

``TileCacheWriter`` replaces that raw enqueue with a single process-wide
worker pool gated by a running byte budget rather than a queue-size cap —
tile sizes vary from ~2 KB (MVT) to ~300 KB (PNG), so only a byte budget
actually bounds RAM. Submission (``submit_nowait``) never awaits and never
raises: the request path stays fully insulated from write pressure.

Durability is explicitly out of scope here (Tier A). A write that overflows
the byte budget, or is still queued when the process drains at shutdown, is
simply dropped — the tile re-renders on the next request, which is the
retry. ``_on_overflow`` is the single seam a future durable backstop (a
queued task instead of a drop) would replace.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Minimum gap between repeated drop warnings (overflow or write error), so a
# sustained storm logs at a trickle instead of once per dropped tile.
_WARN_THROTTLE_INTERVAL_SECONDS = 30.0


@dataclass(frozen=True)
class _TileWriteJob:
    provider: Any
    catalog_id: str
    cache_key: str
    tms_id: str
    z: int
    x: int
    y: int
    tile_bytes: bytes
    fmt: str


class TileCacheWriter:
    """Bounded async writer draining tile-cache persistence off the request path.

    Construct one instance per process, ``start()`` it during app startup and
    ``stop()`` it during shutdown. ``submit_nowait`` is the only method meant
    to be called from a request handler.
    """

    def __init__(self, *, buffer_max_bytes: int, workers: int) -> None:
        self._buffer_max_bytes = buffer_max_bytes
        self._workers = workers
        self._queue: "asyncio.Queue[_TileWriteJob]" = asyncio.Queue()
        self._inflight_bytes = 0
        self._pending_keys: set[str] = set()
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._last_overflow_warn = 0.0
        self._last_error_warn = 0.0

        self.enqueued = 0
        self.written = 0
        self.dropped_overflow = 0
        self.dropped_error = 0
        self.coalesced = 0

    @property
    def inflight_bytes(self) -> int:
        return self._inflight_bytes

    def start(self) -> None:
        """Spawn the worker pool. Call once, from a running event loop."""
        self._worker_tasks = [
            asyncio.ensure_future(self._worker())
            for _ in range(self._workers)
        ]

    async def stop(self, *, drain_timeout: float) -> None:
        """Best-effort drain, then cancel any stragglers.

        Waits up to ``drain_timeout`` seconds for the queue to empty, then
        cancels the worker tasks regardless of outcome. Never raises and
        never blocks past ``drain_timeout`` plus cancellation teardown, so
        shutdown always completes.
        """
        if not self._worker_tasks:
            return
        try:
            await asyncio.wait_for(self._queue.join(), timeout=drain_timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "TileCacheWriter: drain timed out after %.1fs with "
                "%d job(s) still queued/in-flight (%d bytes) — cancelling workers.",
                drain_timeout, self._queue.qsize(), self._inflight_bytes,
            )
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks = []

    def submit_nowait(
        self,
        provider: Any,
        catalog_id: str,
        cache_key: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        tile_bytes: bytes,
        fmt: str,
    ) -> None:
        """Admit a tile write if the byte budget allows; O(1), never awaits or raises.

        Coalesces with an already-queued/in-flight write for the same
        ``cache_key`` (skips the duplicate). Overflowing the byte budget
        routes to :meth:`_on_overflow` instead of admitting.
        """
        if cache_key in self._pending_keys:
            self.coalesced += 1
            return
        n = len(tile_bytes)
        job = _TileWriteJob(
            provider=provider, catalog_id=catalog_id, cache_key=cache_key,
            tms_id=tms_id, z=z, x=x, y=y, tile_bytes=tile_bytes, fmt=fmt,
        )
        if self._inflight_bytes + n > self._buffer_max_bytes:
            self._on_overflow(job)
            return
        self._inflight_bytes += n
        self._pending_keys.add(cache_key)
        self._queue.put_nowait(job)
        self.enqueued += 1

    def _on_overflow(self, job: _TileWriteJob) -> None:
        """Policy seam for a write that exceeds the byte budget.

        Tier A: count and drop, with a throttled warning. A durable backstop
        (queueing a recoverable task instead) would replace only this method.
        """
        self.dropped_overflow += 1
        now = time.monotonic()
        if now - self._last_overflow_warn >= _WARN_THROTTLE_INTERVAL_SECONDS:
            self._last_overflow_warn = now
            logger.warning(
                "TileCacheWriter: byte budget exceeded (inflight=%d + %d > "
                "max=%d); dropping tile-cache write for catalog=%s key=%s "
                "z=%s x=%s y=%s (dropped_overflow=%d total). Tile re-renders "
                "on next request.",
                self._inflight_bytes, len(job.tile_bytes), self._buffer_max_bytes,
                job.catalog_id, job.cache_key, job.z, job.x, job.y,
                self.dropped_overflow,
            )

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                await job.provider.save_tile(
                    job.catalog_id, job.cache_key, job.tms_id,
                    job.z, job.x, job.y, job.tile_bytes, job.fmt,
                )
                self.written += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.dropped_error += 1
                now = time.monotonic()
                if now - self._last_error_warn >= _WARN_THROTTLE_INTERVAL_SECONDS:
                    self._last_error_warn = now
                    logger.warning(
                        "TileCacheWriter: save_tile failed for catalog=%s "
                        "key=%s z=%s x=%s y=%s: %s (dropped_error=%d total). "
                        "Tile re-renders on next request.",
                        job.catalog_id, job.cache_key, job.z, job.x, job.y,
                        exc, self.dropped_error,
                    )
            finally:
                self._inflight_bytes -= len(job.tile_bytes)
                self._pending_keys.discard(job.cache_key)
                self._queue.task_done()
