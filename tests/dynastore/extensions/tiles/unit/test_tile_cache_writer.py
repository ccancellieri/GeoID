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

"""Unit tests for the bounded byte-budget tile-cache writer.

Covers the writer's own contract in isolation (no FastAPI/HTTP involved):
submission never blocks or raises, byte-budget admission and overflow
counting, in-flight coalescing, worker resilience to a failing write, and a
clean bounded shutdown. The three call-site wirings in ``TilesService`` are
covered separately in the map/vector-tile dispatch test suites.
"""
from __future__ import annotations

import asyncio

import pytest

from dynastore.extensions.tiles.tile_cache_writer import TileCacheWriter


class _FakeProvider:
    """Minimal stand-in for ``TileStorageProtocol``: an async ``save_tile``
    whose behavior each test controls via injected callables/events."""

    def __init__(self, *, on_save=None):
        self._on_save = on_save
        self.calls: list[tuple] = []

    async def save_tile(self, catalog_id, cache_key, tms_id, z, x, y, tile_bytes, fmt):
        self.calls.append((catalog_id, cache_key, tms_id, z, x, y, tile_bytes, fmt))
        if self._on_save is not None:
            await self._on_save()


async def _drain(writer: TileCacheWriter) -> None:
    """Wait for the queue to fully empty (all submitted jobs processed)."""
    await writer._queue.join()


class TestSubmitNowaitNeverBlocks:
    def test_admits_until_budget_then_drops_overflow(self):
        writer = TileCacheWriter(buffer_max_bytes=10, workers=1)
        provider = _FakeProvider()

        # Never started (`start()` not called) — submit_nowait must still be
        # a synchronous, non-raising O(1) admission decision.
        writer.submit_nowait(provider, "cat", "key-1", "WebMercatorQuad", 0, 0, 0, b"12345", "mvt")
        assert writer.enqueued == 1
        assert writer.inflight_bytes == 5

        writer.submit_nowait(provider, "cat", "key-2", "WebMercatorQuad", 0, 0, 0, b"12345", "mvt")
        assert writer.enqueued == 2
        assert writer.inflight_bytes == 10

        # A third write would exceed the 10-byte budget — dropped, not queued.
        writer.submit_nowait(provider, "cat", "key-3", "WebMercatorQuad", 0, 0, 0, b"123456", "mvt")
        assert writer.dropped_overflow == 1
        assert writer.enqueued == 2
        assert writer.inflight_bytes == 10


class TestByteAccountingReturnsToZero:
    @pytest.mark.asyncio
    async def test_inflight_bytes_zero_after_success_and_error(self):
        writer = TileCacheWriter(buffer_max_bytes=1_000_000, workers=2)
        writer.start()
        try:
            ok_provider = _FakeProvider()

            async def _boom():
                raise RuntimeError("bucket unavailable")

            failing_provider = _FakeProvider(on_save=_boom)

            writer.submit_nowait(ok_provider, "cat", "ok-key", "WebMercatorQuad", 0, 0, 0, b"data", "mvt")
            writer.submit_nowait(failing_provider, "cat", "err-key", "WebMercatorQuad", 0, 0, 0, b"data", "mvt")

            await _drain(writer)

            assert writer.inflight_bytes == 0
            assert writer.written == 1
            assert writer.dropped_error == 1
            assert ok_provider.calls
        finally:
            await writer.stop(drain_timeout=1.0)


class TestCoalescing:
    @pytest.mark.asyncio
    async def test_duplicate_pending_key_is_skipped_and_counted(self):
        writer = TileCacheWriter(buffer_max_bytes=1_000_000, workers=1)
        release = asyncio.Event()

        async def _wait_for_release():
            await release.wait()

        provider = _FakeProvider(on_save=_wait_for_release)
        writer.start()
        try:
            writer.submit_nowait(provider, "cat", "same-key", "WebMercatorQuad", 0, 0, 0, b"first", "mvt")
            # Give the worker a chance to pick up the first job so the key is
            # genuinely pending (queued or in-flight) when the duplicate arrives.
            await asyncio.sleep(0.01)

            writer.submit_nowait(provider, "cat", "same-key", "WebMercatorQuad", 0, 0, 0, b"second", "mvt")

            assert writer.coalesced == 1
            assert writer.enqueued == 1

            release.set()
            await _drain(writer)
            assert writer.written == 1
            assert len(provider.calls) == 1
        finally:
            await writer.stop(drain_timeout=1.0)


class TestWorkerSurvivesError:
    @pytest.mark.asyncio
    async def test_failed_write_does_not_stop_the_worker(self):
        writer = TileCacheWriter(buffer_max_bytes=1_000_000, workers=1)
        writer.start()
        try:
            async def _boom():
                raise ValueError("write failed")

            failing_provider = _FakeProvider(on_save=_boom)
            ok_provider = _FakeProvider()

            writer.submit_nowait(failing_provider, "cat", "key-a", "WebMercatorQuad", 0, 0, 0, b"x", "mvt")
            writer.submit_nowait(ok_provider, "cat", "key-b", "WebMercatorQuad", 0, 0, 0, b"y", "mvt")

            await _drain(writer)

            assert writer.dropped_error == 1
            assert writer.written == 1
            assert ok_provider.calls
        finally:
            await writer.stop(drain_timeout=1.0)


class TestShutdownDrainsThenCancels:
    @pytest.mark.asyncio
    async def test_stop_waits_for_in_flight_then_returns(self):
        writer = TileCacheWriter(buffer_max_bytes=1_000_000, workers=1)
        provider = _FakeProvider()
        writer.start()

        writer.submit_nowait(provider, "cat", "key", "WebMercatorQuad", 0, 0, 0, b"data", "mvt")

        await writer.stop(drain_timeout=2.0)

        assert writer.written == 1
        assert all(t.done() for t in writer._worker_tasks) or writer._worker_tasks == []

    @pytest.mark.asyncio
    async def test_stop_cancels_stragglers_after_timeout(self):
        writer = TileCacheWriter(buffer_max_bytes=1_000_000, workers=1)
        stuck = asyncio.Event()

        async def _hang_forever():
            await stuck.wait()

        provider = _FakeProvider(on_save=_hang_forever)
        writer.start()

        writer.submit_nowait(provider, "cat", "key", "WebMercatorQuad", 0, 0, 0, b"data", "mvt")
        # Let the worker actually pick up the job before stopping.
        await asyncio.sleep(0.01)

        await writer.stop(drain_timeout=0.05)

        assert writer._worker_tasks == []
