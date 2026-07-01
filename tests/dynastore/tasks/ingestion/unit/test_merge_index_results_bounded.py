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

"""Regression test: accumulated secondary-index failure sample stays bounded.

Contract verified:
  Across an arbitrarily large number of batches, _merge_index_results keeps the
  per-indexer ``failures`` detail list bounded to _MAX_ACCUMULATED_FAILURE_SAMPLES
  while the integer counts (``total``/``succeeded``/``failed``) remain exact.

  Before this fix the ``failures`` lists were concatenated unbounded across every
  batch; on a multi-million-feature ingest against a degraded secondary index the
  echoed payloads grew to gigabytes and OOM-killed the Cloud Run job.
"""
from __future__ import annotations

from typing import Any, Dict

from dynastore.models.protocols.indexer import BulkResult
from dynastore.tasks.ingestion.main_ingestion import (
    _MAX_ACCUMULATED_FAILURE_SAMPLES,
    _merge_index_results,
)


def test_accumulated_failures_are_bounded_but_counts_are_exact() -> None:
    accumulated: Dict[str, Any] = {}

    n_batches = 500
    per_batch_failed = 5
    for i in range(n_batches):
        batch = {
            "es": BulkResult(
                total=10,
                succeeded=5,
                failed=per_batch_failed,
                failures=[
                    {"id": f"feat-{i}-{j}", "error": "x" * 32}
                    for j in range(per_batch_failed)
                ],
            )
        }
        _merge_index_results(accumulated, batch)

    merged = accumulated["es"]

    # Counts stay exact regardless of the sample cap.
    assert merged.total == 10 * n_batches
    assert merged.succeeded == 5 * n_batches
    assert merged.failed == per_batch_failed * n_batches

    # The detail sample is bounded — not the ~2500 failures a naive concat holds.
    assert len(merged.failures) <= _MAX_ACCUMULATED_FAILURE_SAMPLES

    # The bound keeps the most-recent failures (last batch's ids survive).
    last_ids = {f"feat-{n_batches - 1}-{j}" for j in range(per_batch_failed)}
    assert last_ids.issubset({f["id"] for f in merged.failures})


def test_single_batch_passthrough_preserves_failures() -> None:
    accumulated: Dict[str, Any] = {}
    _merge_index_results(
        accumulated,
        {"es": BulkResult(total=3, succeeded=2, failed=1, failures=[{"id": "a"}])},
    )
    assert accumulated["es"].failed == 1
    assert accumulated["es"].failures == [{"id": "a"}]
