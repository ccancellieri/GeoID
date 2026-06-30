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

"""Unit tests for the ingestion→tiles_preseed completion chain.

Covers ``_maybe_enqueue_tile_preseed`` in isolation: it submits a
``tiles_preseed`` job through the canonical OGC ``execute_process`` path once
ingestion has committed, but only when ``preseed_on_success`` is set. All I/O
(the short-lived async engine, ``execute_process``) is mocked.

``ingestion_task`` hard-imports ``geopandas`` as a capability gate; the module
never *uses* it at import time, so we stub it to let the test run regardless of
whether the geospatial extra is installed in this environment.
"""
from __future__ import annotations

import sys
import types

sys.modules.setdefault("geopandas", types.ModuleType("geopandas"))

from unittest.mock import AsyncMock, MagicMock, patch  # noqa: E402

import pytest  # noqa: E402

from dynastore.tasks.ingestion.ingestion_task import (  # noqa: E402
    _maybe_enqueue_tile_preseed,
)


async def _run(spec):
    """Invoke the helper with mocked engine + execute_process; return the mock."""
    engine = MagicMock()
    engine.dispose = AsyncMock()
    exec_proc = AsyncMock(return_value=None)
    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=engine),
        patch(
            "dynastore.modules.db_config.tools.normalize_db_url",
            return_value="postgresql+asyncpg://x/y",
        ),
        patch(
            "dynastore.modules.processes.processes_module.execute_process",
            exec_proc,
        ),
    ):
        await _maybe_enqueue_tile_preseed("cat", "lyr", spec)
    return exec_proc, engine


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", [None, False, {}, ""])
async def test_no_preseed_when_spec_falsy(spec):
    # Ordinary ingestions carry no (or a falsy) preseed_on_success → no chain.
    exec_proc, _engine = await _run(spec)
    exec_proc.assert_not_awaited()


@pytest.mark.asyncio
async def test_preseed_dict_requests_mvt_seed():
    exec_proc, engine = await _run({"output_format": "mvt"})
    exec_proc.assert_awaited_once()
    args, kwargs = exec_proc.await_args
    assert args[0] == "tiles_preseed"
    inputs = args[1].inputs
    assert inputs["catalog_id"] == "cat"
    assert inputs["collection_id"] == "lyr"
    assert inputs["output_format"] == "mvt"
    assert inputs["operation"] == "seed"
    assert kwargs["dedup_key"] == "preseed_on_ingest:cat:lyr"
    # Short-lived engine is always disposed.
    engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_preseed_bool_true_defaults_to_mvt():
    exec_proc, _engine = await _run(True)
    exec_proc.assert_awaited_once()
    assert exec_proc.await_args.args[1].inputs["output_format"] == "mvt"


@pytest.mark.asyncio
async def test_tms_ids_forwarded_when_supplied():
    exec_proc, _engine = await _run(
        {"output_format": "mvt", "tms_ids": ["WebMercatorQuad"]}
    )
    inputs = exec_proc.await_args.args[1].inputs
    assert inputs["tms_ids"] == ["WebMercatorQuad"]


@pytest.mark.asyncio
async def test_engine_disposed_even_when_execute_raises():
    # A failure inside execute_process must still dispose the engine; the
    # exception propagates to the task, which swallows it (best-effort chain).
    engine = MagicMock()
    engine.dispose = AsyncMock()
    exec_proc = AsyncMock(side_effect=RuntimeError("boom"))
    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=engine),
        patch(
            "dynastore.modules.db_config.tools.normalize_db_url",
            return_value="postgresql+asyncpg://x/y",
        ),
        patch(
            "dynastore.modules.processes.processes_module.execute_process",
            exec_proc,
        ),
    ):
        with pytest.raises(RuntimeError):
            await _maybe_enqueue_tile_preseed("cat", "lyr", {"output_format": "mvt"})
    engine.dispose.assert_awaited_once()
