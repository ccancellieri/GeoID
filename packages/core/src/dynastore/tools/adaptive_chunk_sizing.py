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

# dynastore/tools/adaptive_chunk_sizing.py

"""Byte-adaptive chunk sizing for apply loops over hydrated/decoded rows.

Extracted from ``StorageDrainTask``'s id-only re-read chunking (#3121:
``_next_id_only_chunk_rows`` / ``_estimate_doc_bytes``) so every apply loop
that materializes hydrated documents in memory-bounded chunks shares one
estimator instead of each caller guessing its own row count.

The mechanism: a fixed row-count chunk applies the same size to a chunk of
lightweight documents as it does to a chunk of multi-MB ones. Sizing the
*next* chunk from the *previous* chunk's measured JSON-encoded byte cost
keeps the in-flight materialization roughly constant regardless of
per-document size — the same fix ``StorageDrainTask`` applies to its
canonical re-read groups, generalized for reuse.
"""
from __future__ import annotations

import json
from typing import Any, Dict

# Fallback size (bytes) attributed to a document that cannot be
# JSON-estimated (e.g. a non-JSON-serializable value slipped through).
# Large enough to force an isolating flush rather than silently
# accumulating an unmeasured object indefinitely.
DEFAULT_UNESTIMATED_DOC_BYTES: int = 8 * 1024 * 1024  # 8 MiB


def estimate_doc_bytes(
    doc: Dict[str, Any], *, fallback_bytes: int = DEFAULT_UNESTIMATED_DOC_BYTES,
) -> int:
    """Estimate a document's wire size via JSON encoding.

    Used only to decide chunk/flush boundaries — not for exact accounting.
    """
    try:
        return len(json.dumps(doc, default=str).encode("utf-8"))
    except Exception:  # noqa: BLE001 — an unestimable doc still forces a flush
        return fallback_bytes


def next_adaptive_chunk_rows(
    *, chunk_bytes: int, rows_read: int, byte_budget: int, current: int, ceiling: int,
) -> int:
    """Size the next chunk from the previous chunk's measured byte cost (#3121).

    Fits ``byte_budget`` worth of rows using the previous chunk's measured
    per-row average, floor 1 (an oversized single row is processed alone
    rather than split), ceiling ``ceiling`` (the caller's pre-existing fixed
    chunk size, kept as an upper bound rather than removed).

    A chunk with no measurable cost (nothing read, or a zero-byte
    measurement) carries ``current`` forward unchanged rather than guessing.
    """
    if rows_read <= 0 or chunk_bytes <= 0:
        return current
    avg_row_bytes = max(1, chunk_bytes // rows_read)
    return max(1, min(ceiling, byte_budget // avg_row_bytes))
