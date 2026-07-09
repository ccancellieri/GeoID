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

"""Unit tests for the shared byte-adaptive chunk sizing helpers (#3154).

``estimate_doc_bytes`` / ``next_adaptive_chunk_rows`` were factored out of
``StorageDrainTask`` (#3121) so other apply loops — the item-write chunk
loop in ``ItemService.upsert()`` — can reuse the same estimator instead of
inventing a second one. These tests cover the pure math; the storage-drain
call site keeps its own tests in
``tests/dynastore/tasks/workclass_drain/test_storage_drain.py``.
"""
from __future__ import annotations

from dynastore.tools.adaptive_chunk_sizing import (
    DEFAULT_UNESTIMATED_DOC_BYTES,
    estimate_doc_bytes,
    next_adaptive_chunk_rows,
)


class TestEstimateDocBytes:
    def test_measures_json_encoded_size(self) -> None:
        doc = {"a": 1}
        assert estimate_doc_bytes(doc) == len(b'{"a": 1}')

    def test_unserializable_value_falls_back(self) -> None:
        class _Unserializable:
            def __str__(self) -> str:  # even the str() fallback fails
                raise RuntimeError("nope")

        assert estimate_doc_bytes({"bad": _Unserializable()}) == (
            DEFAULT_UNESTIMATED_DOC_BYTES
        )

    def test_custom_fallback_bytes_is_honoured(self) -> None:
        class _Unserializable:
            def __str__(self) -> str:
                raise RuntimeError("nope")

        assert estimate_doc_bytes({"bad": _Unserializable()}, fallback_bytes=42) == 42


class TestNextAdaptiveChunkRows:
    def test_no_rows_read_carries_current_forward(self) -> None:
        assert next_adaptive_chunk_rows(
            chunk_bytes=0, rows_read=0, byte_budget=1000, current=7, ceiling=50,
        ) == 7

    def test_zero_measured_bytes_carries_current_forward(self) -> None:
        assert next_adaptive_chunk_rows(
            chunk_bytes=0, rows_read=5, byte_budget=1000, current=3, ceiling=50,
        ) == 3

    def test_light_rows_grow_to_ceiling(self) -> None:
        # 1 byte/row average against a 1000-byte budget would fit 1000 rows —
        # clamped to the caller's ceiling.
        assert next_adaptive_chunk_rows(
            chunk_bytes=1, rows_read=1, byte_budget=1000, current=1, ceiling=50,
        ) == 50

    def test_heavy_rows_shrink_below_ceiling(self) -> None:
        # 2 MiB/row average against a 16 MiB budget fits 8 rows — well under
        # a 50-row ceiling.
        two_mib = 2 * 1024 * 1024
        assert next_adaptive_chunk_rows(
            chunk_bytes=two_mib, rows_read=1,
            byte_budget=16 * 1024 * 1024, current=1, ceiling=50,
        ) == 8

    def test_oversized_single_row_floors_at_one(self) -> None:
        # A single row heavier than the whole byte budget is still fetched
        # alone rather than split (there is nothing smaller to split into).
        assert next_adaptive_chunk_rows(
            chunk_bytes=100 * 1024 * 1024, rows_read=1,
            byte_budget=16 * 1024 * 1024, current=1, ceiling=50,
        ) == 1

    def test_ceiling_caps_result_even_with_huge_budget(self) -> None:
        assert next_adaptive_chunk_rows(
            chunk_bytes=1, rows_read=1, byte_budget=10**12, current=1, ceiling=10,
        ) == 10
