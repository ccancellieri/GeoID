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

"""Regression test: item_service's per-request index-result merge stays
bounded (#2657).

``ItemService.upsert_items`` accumulates per-indexer ``BulkResult``s into
``ctx.extensions["_index_results"]`` across every write folded into one
request (e.g. the ingestion task's chunked inline dispatch). Before this fix
the merge concatenated ``failures`` with no bound. This tests the extracted
merge helper directly — it is the exact code the write path calls — without
needing a live DB/driver stack.
"""
from __future__ import annotations

from typing import Any, Dict

from dynastore.models.protocols.indexer import (
    MAX_ACCUMULATED_FAILURE_SAMPLES,
    BulkResult,
)
from dynastore.modules.catalog.item_service import _merge_index_results_into


def test_merge_bounds_failures_across_many_writes() -> None:
    existing: Dict[str, Any] = {}

    n_writes = 500
    per_write_failed = 5
    for i in range(n_writes):
        batch = {
            "es": BulkResult(
                total=10,
                succeeded=5,
                failed=per_write_failed,
                failures=[
                    {"id": f"feat-{i}-{j}", "error": "x" * 32}
                    for j in range(per_write_failed)
                ],
            )
        }
        _merge_index_results_into(existing, batch)

    merged = existing["es"]

    # Counts stay exact regardless of the sample cap.
    assert merged.total == 10 * n_writes
    assert merged.succeeded == 5 * n_writes
    assert merged.failed == per_write_failed * n_writes

    # The detail sample is bounded — not the 2500 failures a naive concat
    # across every write folded into this request would hold.
    assert len(merged.failures) <= MAX_ACCUMULATED_FAILURE_SAMPLES

    # The bound keeps the most-recent failures.
    last_ids = {f"feat-{n_writes - 1}-{j}" for j in range(per_write_failed)}
    assert last_ids.issubset({f["id"] for f in merged.failures})


def test_merge_first_write_passes_through_unmodified() -> None:
    existing: Dict[str, Any] = {}
    _merge_index_results_into(
        existing,
        {"es": BulkResult(total=3, succeeded=2, failed=1, failures=[{"id": "a"}])},
    )
    assert existing["es"].failed == 1
    assert existing["es"].failures == [{"id": "a"}]
