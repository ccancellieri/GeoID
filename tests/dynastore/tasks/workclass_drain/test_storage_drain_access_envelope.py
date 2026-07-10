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

"""Unit tests for the drain's access-aware (envelope) driver support (#2687).

Pure unit tests — no PG / no DB fixture, mirrors
``test_resolve_indexer_registered_driver_resolves_via_registry`` in
``test_storage_drain.py``.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class _StubCanonicalInput:
    """Minimal stand-in for ``CanonicalIndexInput``."""

    def __init__(self, geoid: str = "g1", access=None) -> None:
        self.row = {"geoid": geoid}
        self.resolved_sidecars = []
        self.geometry = None
        self.bbox = None
        self.user_properties = None
        self.access = access
        self.stac_reserved_members = None


@pytest.mark.asyncio
async def test_resolve_indexer_envelope_driver_resolves_and_is_tracked():
    """The envelope driver's ``driver_id`` resolves to an ``ESBulkIndexer``
    (same adapter as the standard driver, #2687) and is recorded in
    ``_envelope_driver_ids`` so ``_build_canonical_doc`` picks the
    envelope-shaped builder."""
    from dynastore.modules.storage.driver_registry import DriverRegistry
    from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
        ItemsElasticsearchEnvelopeDriver,
    )
    from dynastore.tasks.workclass_drain.es_indexer_adapter import ESBulkIndexer
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    fake_driver = MagicMock(spec=ItemsElasticsearchEnvelopeDriver)
    fake_driver.is_available.return_value = True

    task = StorageDrainTask()
    with (
        patch.object(
            DriverRegistry, "collection_index",
            return_value={"items_elasticsearch_envelope_driver": fake_driver},
        ),
        patch.object(DriverRegistry, "asset_index", return_value={}),
    ):
        indexer = await task._resolve_indexer("items_elasticsearch_envelope_driver")

    assert isinstance(indexer, ESBulkIndexer)
    assert "items_elasticsearch_envelope_driver" in task._envelope_driver_ids


@pytest.mark.asyncio
async def test_build_canonical_doc_envelope_path_uses_envelope_builder():
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask()
    task._envelope_driver_ids.add("items_elasticsearch_envelope_driver")
    ci = _StubCanonicalInput(access={"_visibility": "private", "_owner": "alice"})

    with patch(
        "dynastore.modules.storage.drivers.elasticsearch_envelope.doc_builder."
        "build_envelope_feature_doc",
    ) as mock_build:
        mock_build.return_value = {"geoid": "g1", "visibility": "private", "owner": "alice"}
        doc = await task._build_canonical_doc(
            catalog_id="cat1", collection_id="col1", ci=ci,
            driver_id="items_elasticsearch_envelope_driver",
        )

    mock_build.assert_called_once()
    assert mock_build.call_args.args[0] is ci
    assert doc == {"geoid": "g1", "visibility": "private", "owner": "alice"}


@pytest.mark.asyncio
async def test_build_canonical_doc_envelope_path_fails_closed_without_envelope():
    """#2687 hard invariant: an access-aware driver's doc must never be built
    without its ABAC envelope — this must raise (→ retry), never silently
    build a doc-less-of-envelope."""
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask()
    task._envelope_driver_ids.add("items_elasticsearch_envelope_driver")
    ci = _StubCanonicalInput(access=None)

    with pytest.raises(RuntimeError):
        await task._build_canonical_doc(
            catalog_id="cat1", collection_id="col1", ci=ci,
            driver_id="items_elasticsearch_envelope_driver",
        )


@pytest.mark.asyncio
async def test_build_canonical_doc_non_envelope_driver_uses_standard_builder():
    """A driver_id not tracked as access-aware (or ``None``) keeps building
    the standard canonical doc — zero behaviour change."""
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask()
    ci = _StubCanonicalInput(access=None)

    with (
        patch(
            "dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc",
        ) as mock_build,
        patch(
            "dynastore.modules.elasticsearch.items_projection.resolve_catalog_known_fields",
        ) as mock_known_fields,
    ):
        mock_build.return_value = {"id": "g1"}

        async def _known_fields(*_a, **_kw):
            return {}

        mock_known_fields.side_effect = _known_fields
        doc = await task._build_canonical_doc(
            catalog_id="cat1", collection_id="col1", ci=ci,
            driver_id="items_elasticsearch_driver",
        )

    mock_build.assert_called_once()
    assert doc == {"id": "g1"}


@pytest.mark.asyncio
async def test_build_canonical_doc_no_driver_id_uses_standard_builder():
    """The write-id primary-batch call site may omit ``driver_id`` for a
    driver ``_resolve_indexer`` never ran for (e.g. a direct test seam
    call) — must degrade to the standard builder, not raise."""
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask()
    ci = _StubCanonicalInput(access=None)

    with (
        patch(
            "dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc",
        ) as mock_build,
        patch(
            "dynastore.modules.elasticsearch.items_projection.resolve_catalog_known_fields",
        ) as mock_known_fields,
    ):
        mock_build.return_value = {"id": "g1"}

        async def _known_fields(*_a, **_kw):
            return {}

        mock_known_fields.side_effect = _known_fields
        doc = await task._build_canonical_doc(
            catalog_id="cat1", collection_id="col1", ci=ci,
        )

    mock_build.assert_called_once()
    assert doc == {"id": "g1"}
