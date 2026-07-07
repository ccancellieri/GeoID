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

"""Regression tests for ES bulk-error surfacing (#1753).

Covers:
  1. :func:`classify_bulk_response` — shared classifier (Task 1).
  2. Inline sync ``write_entities`` raises :class:`EsBulkWriteError` and
     ERROR-logs on per-doc rejections for the public items driver (Task 2).
  3. Same for the private items driver (Task 2).
  4. Same for the envelope items driver (Task 2).
  5. Same for the asset driver (Task 2).
  6. :func:`raise_on_bulk_errors` defers to
     :func:`maybe_raise_bulk_mapping_mismatch` first so
     ``illegal_argument_exception`` still surfaces as
     :class:`IndexMappingMismatchError` (existing contract preserved).
  7. Circuit-breaker open path enqueues OUTBOX instead of silently
     discarding ops (Task 4).

All tests are pure-unit — no live ES cluster required.
"""
from __future__ import annotations

import logging
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers / fixtures shared by multiple tests
# ---------------------------------------------------------------------------

def _bulk_error_response(
    error_type: str = "mapper_parsing_exception",
    reason: str = "duplicate consecutive coordinates",
    status: int = 400,
    doc_id: str = "item-1",
) -> Dict[str, Any]:
    """Return a synthetic ES bulk response that contains one error."""
    return {
        "errors": True,
        "items": [{
            "index": {
                "_index": "test-items-cat1",
                "_id": doc_id,
                "status": status,
                "error": {"type": error_type, "reason": reason},
            },
        }],
    }


def _bulk_ok_response(doc_id: str = "item-1") -> Dict[str, Any]:
    return {
        "errors": False,
        "items": [{"index": {"_index": "test-items-cat1", "_id": doc_id, "status": 200}}],
    }


# ---------------------------------------------------------------------------
# Task 1 — shared classifier
# ---------------------------------------------------------------------------


class TestClassifyBulkResponse:
    """Unit tests for the shared :func:`classify_bulk_response` helper."""

    def test_passed_on_200(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = _bulk_ok_response("id-ok")
        passed, transient, poison = classify_bulk_response(resp, ["id-ok"])
        assert passed == ["id-ok"]
        assert transient == []
        assert poison == []

    def test_mapper_parsing_exception_is_poison(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = _bulk_error_response("mapper_parsing_exception", "bad shape", 400, "id-bad")
        passed, transient, poison = classify_bulk_response(resp, ["id-bad"])
        assert passed == []
        assert transient == []
        assert len(poison) == 1
        assert poison[0][0] == "id-bad"
        assert "mapper_parsing_exception" in poison[0][1]

    def test_invalid_shape_exception_is_poison(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = _bulk_error_response("invalid_shape_exception", "dup coords", 400, "id-shape")
        passed, transient, poison = classify_bulk_response(resp, ["id-shape"])
        assert len(poison) == 1
        assert "invalid_shape_exception" in poison[0][1]

    def test_429_is_transient(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = _bulk_error_response("es_rejected_execution_exception", "queue full", 429, "id-rl")
        passed, transient, poison = classify_bulk_response(resp, ["id-rl"])
        assert len(transient) == 1
        assert "429" in transient[0][1]
        assert poison == []

    def test_5xx_is_transient(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = _bulk_error_response("internal_error", "oops", 503, "id-5xx")
        passed, transient, poison = classify_bulk_response(resp, ["id-5xx"])
        assert len(transient) == 1
        assert poison == []

    def test_mixed_batch(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = {
            "errors": True,
            "items": [
                {"index": {"_id": "ok",      "status": 200}},
                {"index": {"_id": "rl",      "status": 429, "error": {"type": "x", "reason": "r"}}},
                {"index": {"_id": "mapping", "status": 400, "error": {
                    "type": "mapper_parsing_exception", "reason": "bad"
                }}},
            ],
        }
        passed, transient, poison = classify_bulk_response(resp, ["ok", "rl", "mapping"])
        assert passed == ["ok"]
        assert len(transient) == 1
        assert len(poison) == 1

    def test_no_errors_flag_skips_classification(self):
        """When ``errors=False`` the classifier should return all passed."""
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = {
            "errors": False,
            "items": [{"index": {"_id": "a", "status": 200}}],
        }
        passed, transient, poison = classify_bulk_response(resp, ["a"])
        assert passed == ["a"]
        assert transient == []
        assert poison == []

    def test_empty_response_returns_empty(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        passed, transient, poison = classify_bulk_response({}, [])
        assert passed == transient == poison == []

    def test_caused_by_chain_is_included_in_reason(self):
        """#2769: a geo_shape rejection's actual diagnosable cause typically
        lives one or more ``caused_by`` hops below the generic top-level
        reason — both must appear in the classified reason string."""
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = {
            "errors": True,
            "items": [{
                "index": {
                    "_id": "geo-1",
                    "status": 400,
                    "error": {
                        "type": "document_parsing_exception",
                        "reason": "failed to parse field [geometry] of type [geo_shape]",
                        "caused_by": {
                            "type": "invalid_shape_exception",
                            "reason": "Self-intersection at or near point [179.99, -85.0]",
                        },
                    },
                },
            }],
        }
        _passed, _transient, poison = classify_bulk_response(resp, ["geo-1"])
        assert len(poison) == 1
        reason = poison[0][1]
        assert "document_parsing_exception" in reason
        assert "failed to parse field [geometry]" in reason
        assert "invalid_shape_exception" in reason
        assert "Self-intersection" in reason

    def test_reason_is_capped_at_max_length(self):
        from dynastore.modules.elasticsearch.bulk_classify import (
            _MAX_REASON_LEN,
            classify_bulk_response,
        )

        huge_reason = "x" * (_MAX_REASON_LEN * 3)
        resp = {
            "errors": True,
            "items": [{
                "index": {
                    "_id": "geo-2",
                    "status": 400,
                    "error": {"type": "mapper_parsing_exception", "reason": huge_reason},
                },
            }],
        }
        _passed, _transient, poison = classify_bulk_response(resp, ["geo-2"])
        assert len(poison) == 1
        assert len(poison[0][1]) <= _MAX_REASON_LEN + len("...(truncated)") + 30


# ---------------------------------------------------------------------------
# #2799 — id/items length-mismatch (truncated bulk response)
# ---------------------------------------------------------------------------


class TestBulkIdLengthMismatch:
    """A truncated ``items`` array (fewer entries than ids submitted) must
    never silently drop the unaccounted tail — those ids must resurface as
    transient (retryable) rather than being assumed acknowledged (#2799)."""

    def test_classify_marks_missing_tail_as_transient(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        # Two ids submitted, ES only echoed one item.
        resp = {
            "errors": False,
            "items": [{"index": {"_id": "a", "status": 200}}],
        }
        passed, transient, poison = classify_bulk_response(resp, ["a", "b"])
        assert passed == ["a"]
        assert poison == []
        # The dropped tail id must be surfaced, not lost.
        assert [t[0] for t in transient] == ["b"]
        assert "truncated" in transient[0][1]

    def test_classify_all_missing_when_items_empty(self):
        from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

        resp = {"errors": False, "items": []}
        passed, transient, poison = classify_bulk_response(resp, ["a", "b"])
        assert passed == []
        assert poison == []
        assert [t[0] for t in transient] == ["a", "b"]

    def test_raise_on_bulk_errors_does_not_swallow_truncated_tail(self):
        """errors:false is NOT sufficient to fast-return list(ids) when the
        response is truncated — the tail must be raised as a failure."""
        from dynastore.modules.elasticsearch._mapping_errors import raise_on_bulk_errors
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = {
            "errors": False,
            "items": [{"index": {"_id": "a", "status": 200}}],
        }
        with pytest.raises(EsBulkWriteError) as exc_info:
            raise_on_bulk_errors(resp, "my-index", ["a", "b"])
        assert exc_info.value.acknowledged == ["a"]
        assert [f[0] for f in exc_info.value.failures] == ["b"]

    def test_clean_full_response_still_fast_returns(self):
        """The fast path must still trigger when counts match and no errors."""
        from dynastore.modules.elasticsearch._mapping_errors import raise_on_bulk_errors

        resp = {
            "errors": False,
            "items": [
                {"index": {"_id": "a", "status": 200}},
                {"index": {"_id": "b", "status": 201}},
            ],
        }
        assert raise_on_bulk_errors(resp, "my-index", ["a", "b"]) == ["a", "b"]

    @pytest.mark.asyncio
    async def test_ladder_variant_does_not_swallow_truncated_tail(self):
        from dynastore.modules.elasticsearch._mapping_errors import (
            raise_on_bulk_errors_with_ladder,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = {
            "errors": False,
            "items": [{"index": {"_id": "a", "status": 200}}],
        }
        with pytest.raises(EsBulkWriteError) as exc_info:
            await raise_on_bulk_errors_with_ladder(
                es=MagicMock(), bulk_resp=resp, index_name="my-index",
                ids=["a", "b"], doc_by_id={},
            )
        assert exc_info.value.acknowledged == ["a"]
        assert [f[0] for f in exc_info.value.failures] == ["b"]


# ---------------------------------------------------------------------------
# Task 2 helper — raise_on_bulk_errors
# ---------------------------------------------------------------------------


class TestRaiseOnBulkErrors:
    def test_raises_es_bulk_write_error_on_mapper_exception(self, caplog):
        from dynastore.modules.elasticsearch._mapping_errors import raise_on_bulk_errors
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = _bulk_error_response("mapper_parsing_exception", "dup coords", 400, "id-1")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(EsBulkWriteError) as exc_info:
                raise_on_bulk_errors(resp, "my-index", ["id-1"])

        assert exc_info.value.failures
        assert exc_info.value.failures[0][0] == "id-1"
        assert "mapper_parsing_exception" in exc_info.value.failures[0][1]
        # Must ERROR-log before raising.
        assert any("id-1" in r.message for r in caplog.records if r.levelno == logging.ERROR)

    def test_no_raise_when_errors_false(self):
        from dynastore.modules.elasticsearch._mapping_errors import raise_on_bulk_errors

        resp = _bulk_ok_response("id-ok")
        # Should not raise, and reports the id as acknowledged (#2799).
        assert raise_on_bulk_errors(resp, "my-index", ["id-ok"]) == ["id-ok"]

    def test_no_raise_on_none_response(self):
        from dynastore.modules.elasticsearch._mapping_errors import raise_on_bulk_errors

        raise_on_bulk_errors(None, "my-index", [])

    def test_mixed_batch_exception_carries_acknowledged_ids(self, caplog):
        """#2799: on a partial rejection, EsBulkWriteError.acknowledged must
        list exactly the ids ES actually accepted — not every non-failed id
        assumed from batch-size arithmetic."""
        from dynastore.modules.elasticsearch._mapping_errors import raise_on_bulk_errors
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = {
            "errors": True,
            "items": [
                {"index": {"_id": "ok", "status": 200}},
                {"index": {"_id": "bad", "status": 400, "error": {
                    "type": "mapper_parsing_exception", "reason": "bad shape",
                }}},
            ],
        }
        with caplog.at_level(logging.ERROR):
            with pytest.raises(EsBulkWriteError) as exc_info:
                raise_on_bulk_errors(resp, "my-index", ["ok", "bad"])

        assert exc_info.value.acknowledged == ["ok"]
        assert exc_info.value.failures == [("bad", "400 mapper_parsing_exception: bad shape")]

    def test_illegal_argument_raises_mapping_mismatch_not_es_bulk(self):
        """illegal_argument_exception must surface as IndexMappingMismatchError
        via maybe_raise_bulk_mapping_mismatch (called before raise_on_bulk_errors)
        so operators get the actionable 503 message."""
        from dynastore.modules.elasticsearch._mapping_errors import (
            maybe_raise_bulk_mapping_mismatch,
        )
        from dynastore.modules.storage.errors import IndexMappingMismatchError

        resp = _bulk_error_response("illegal_argument_exception", "unknown field", 400, "id-x")
        with pytest.raises(IndexMappingMismatchError):
            maybe_raise_bulk_mapping_mismatch(resp, "my-index")


# ---------------------------------------------------------------------------
# raise_on_bulk_errors_with_ladder (#2769)
# ---------------------------------------------------------------------------


class _LadderRecoveringEs:
    """Fake ES client whose ``index`` call always succeeds — every
    poison-classified doc with a geometry recovers on the first rung."""

    def __init__(self) -> None:
        self.index_calls: list = []

    async def index(self, *, index, id, body, params=None):
        self.index_calls.append((index, id, body, params))
        return {"result": "created"}


class _LadderFailingEs:
    """Fake ES client whose ``index`` call always raises — every rung is
    exhausted and the original rejection stands."""

    def __init__(self) -> None:
        self.index_calls: list = []

    async def index(self, *, index, id, body, params=None):
        self.index_calls.append((index, id, body, params))
        raise RuntimeError("still document_parsing_exception")


class TestRaiseOnBulkErrorsWithLadder:
    @pytest.mark.asyncio
    async def test_geo_shape_rejection_recovers_and_does_not_raise(self, caplog):
        from dynastore.modules.elasticsearch._mapping_errors import (
            raise_on_bulk_errors_with_ladder,
        )

        resp = _bulk_error_response(
            "document_parsing_exception",
            "failed to parse field [geometry] of type [geo_shape]",
            400, "geo-1",
        )
        doc = {
            "id": "geo-1",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        }
        es = _LadderRecoveringEs()
        with caplog.at_level(logging.WARNING):
            acknowledged = await raise_on_bulk_errors_with_ladder(
                es, resp, "my-index", ["geo-1"], {"geo-1": doc},
            )
        assert es.index_calls, "ladder must have attempted at least one rung"
        assert any(
            "recovered on degraded" in r.message for r in caplog.records
            if r.levelno == logging.WARNING
        )
        # #2799: a doc recovered on a degraded rung counts as acknowledged —
        # a caller crediting only the raw ES ``passed`` classification would
        # silently drop it from its written count.
        assert acknowledged == ["geo-1"]

    @pytest.mark.asyncio
    async def test_geo_shape_rejection_exhausted_still_raises(self, caplog):
        from dynastore.modules.elasticsearch._mapping_errors import (
            raise_on_bulk_errors_with_ladder,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = _bulk_error_response(
            "document_parsing_exception",
            "failed to parse field [geometry] of type [geo_shape]",
            400, "geo-2",
        )
        doc = {
            "id": "geo-2",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        }
        es = _LadderFailingEs()
        with caplog.at_level(logging.ERROR):
            with pytest.raises(EsBulkWriteError) as exc_info:
                await raise_on_bulk_errors_with_ladder(
                    es, resp, "my-index", ["geo-2"], {"geo-2": doc},
                )
        assert exc_info.value.failures[0][0] == "geo-2"
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    @pytest.mark.asyncio
    async def test_mixed_batch_acknowledged_includes_passed_and_recovered_not_exhausted(self):
        """#2799: a sub-chunk with one clean pass, one ladder-recovered doc,
        and one rung-exhausted rejection must report exactly the first two
        as acknowledged — never the exhausted one, and never assumed from
        ``len(ids) - len(failures)`` arithmetic."""
        from dynastore.modules.elasticsearch._mapping_errors import (
            raise_on_bulk_errors_with_ladder,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = {
            "errors": True,
            "items": [
                {"index": {"_id": "ok", "status": 200}},
                {"index": {"_id": "geo-recovers", "status": 400, "error": {
                    "type": "document_parsing_exception",
                    "reason": "failed to parse field [geometry] of type [geo_shape]",
                }}},
                {"index": {"_id": "geo-exhausted", "status": 400, "error": {
                    "type": "document_parsing_exception",
                    "reason": "failed to parse field [geometry] of type [geo_shape]",
                }}},
            ],
        }
        polygon = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
        }
        doc_by_id = {
            "geo-recovers": {"id": "geo-recovers", "geometry": polygon},
            "geo-exhausted": {"id": "geo-exhausted", "geometry": polygon},
        }

        class _SelectiveLadderEs:
            """Recovers ``geo-recovers`` on the first rung; every rung for
            ``geo-exhausted`` keeps failing."""

            async def index(self, *, index, id, body, params=None):
                if id == "geo-recovers":
                    return {"result": "created"}
                raise RuntimeError("still document_parsing_exception")

        with pytest.raises(EsBulkWriteError) as exc_info:
            await raise_on_bulk_errors_with_ladder(
                _SelectiveLadderEs(), resp, "my-index",
                ["ok", "geo-recovers", "geo-exhausted"], doc_by_id,
            )

        assert sorted(exc_info.value.acknowledged) == ["geo-recovers", "ok"]
        assert [f[0] for f in exc_info.value.failures] == ["geo-exhausted"]

    @pytest.mark.asyncio
    async def test_non_geometry_rejection_raises_without_ladder_attempt(self):
        """A doc with no geometry key is not a candidate for the ladder —
        it must be reported exactly as raise_on_bulk_errors would."""
        from dynastore.modules.elasticsearch._mapping_errors import (
            raise_on_bulk_errors_with_ladder,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        resp = _bulk_error_response(
            "mapper_parsing_exception", "unrelated field type mismatch", 400, "id-1",
        )
        doc = {"id": "id-1"}  # no geometry
        es = _LadderRecoveringEs()
        with pytest.raises(EsBulkWriteError):
            await raise_on_bulk_errors_with_ladder(
                es, resp, "my-index", ["id-1"], {"id-1": doc},
            )
        assert es.index_calls == []


# ---------------------------------------------------------------------------
# Task 2 — public ItemsElasticsearchDriver.write_entities
# ---------------------------------------------------------------------------

def _has_opensearchpy() -> bool:
    try:
        import opensearchpy  # noqa: F401
        return True
    except ImportError:
        return False


_skip_no_opensearch = pytest.mark.skipif(
    not _has_opensearchpy(),
    reason="opensearchpy not installed",
)


@_skip_no_opensearch
class TestItemsElasticsearchDriverWriteEntities:
    """write_entities must ERROR-log and raise EsBulkWriteError when ES
    rejects a document (errors=true in bulk response)."""

    @pytest.mark.asyncio
    async def test_raises_on_mapper_parsing_exception(self, caplog):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        driver = ItemsElasticsearchDriver()

        # Minimal stubs so write_entities reaches the es.bulk() call.
        mock_es = AsyncMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        mock_es.bulk = AsyncMock(return_value=_bulk_error_response(
            "mapper_parsing_exception", "bad geom", 400, "item-1",
        ))

        items = [{"id": "item-1", "type": "Feature", "geometry": None, "properties": {}}]

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=mock_es,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch.object(
                ItemsElasticsearchDriver, "get_driver_config",
                new=AsyncMock(return_value=MagicMock(
                    simplify_geometry=False, simplify_target_bytes=None,
                    snap_to_grid=False, snap_grid_size=1e-5,
                )),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_enforce_field_constraints",
                new=AsyncMock(),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_resolve_write_policy",
                new=AsyncMock(return_value=MagicMock(
                    external_id_path=lambda: None,
                    on_conflict=None,
                    on_batch_conflict=None,
                    validity=None,
                )),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_items_index_name",
                return_value="test-items-cat1",
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, 1.0, "none"),
            ),
            caplog.at_level(logging.ERROR),
        ):
            with pytest.raises(EsBulkWriteError) as exc_info:
                await driver.write_entities("cat1", "col1", items)

        assert exc_info.value.failures
        assert any(
            r.levelno == logging.ERROR for r in caplog.records
        ), "Expected at least one ERROR-level log before raising"

    @pytest.mark.asyncio
    async def test_no_raise_on_success(self):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        driver = ItemsElasticsearchDriver()

        mock_es = AsyncMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        mock_es.bulk = AsyncMock(return_value=_bulk_ok_response("item-1"))

        items = [{"id": "item-1", "type": "Feature", "geometry": None, "properties": {}}]

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=mock_es,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch.object(
                ItemsElasticsearchDriver, "get_driver_config",
                new=AsyncMock(return_value=MagicMock(
                    simplify_geometry=False, simplify_target_bytes=None,
                    snap_to_grid=False, snap_grid_size=1e-5,
                )),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_enforce_field_constraints",
                new=AsyncMock(),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_resolve_write_policy",
                new=AsyncMock(return_value=MagicMock(
                    external_id_path=lambda: None,
                    on_conflict=None,
                    on_batch_conflict=None,
                    validity=None,
                )),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_items_index_name",
                return_value="test-items-cat1",
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, 1.0, "none"),
            ),
        ):
            result = await driver.write_entities("cat1", "col1", items)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_partial_rejection_raises_with_only_acknowledged_ids(self, caplog):
        """#2799: when ES's ``_bulk`` response acknowledges FEWER docs than
        submitted-minus-failures — the silent-sibling-drop case audited on
        gaulb/gaul_l1 — the raised EsBulkWriteError.acknowledged must list
        only the id ES actually confirmed, never the silently-dropped one."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        driver = ItemsElasticsearchDriver()

        mock_es = AsyncMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        # Bulk response only accounts for 2 of the 3 submitted docs: one
        # explicit 200, one explicit rejection — the third ("item-3") has
        # no entry at all, simulating ES's response undercounting relative
        # to what was submitted.
        mock_es.bulk = AsyncMock(return_value={
            "errors": True,
            "items": [
                {"index": {"_id": "item-1", "status": 200}},
                {"index": {"_id": "item-2", "status": 400, "error": {
                    "type": "mapper_parsing_exception", "reason": "bad shape",
                }}},
            ],
        })

        items = [
            {"id": "item-1", "type": "Feature", "geometry": None, "properties": {}},
            {"id": "item-2", "type": "Feature", "geometry": None, "properties": {}},
            {"id": "item-3", "type": "Feature", "geometry": None, "properties": {}},
        ]

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=mock_es,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch.object(
                ItemsElasticsearchDriver, "get_driver_config",
                new=AsyncMock(return_value=MagicMock(
                    simplify_geometry=False, simplify_target_bytes=None,
                    snap_to_grid=False, snap_grid_size=1e-5,
                )),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_enforce_field_constraints",
                new=AsyncMock(),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_resolve_write_policy",
                new=AsyncMock(return_value=MagicMock(
                    external_id_path=lambda: None,
                    on_conflict=None,
                    on_batch_conflict=None,
                    validity=None,
                )),
            ),
            patch.object(
                ItemsElasticsearchDriver, "_items_index_name",
                return_value="test-items-cat1",
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, 1.0, "none"),
            ),
            caplog.at_level(logging.ERROR),
        ):
            with pytest.raises(EsBulkWriteError) as exc_info:
                await driver.write_entities("cat1", "col1", items)

        # item-1 confirmed; item-2 explicitly rejected; item-3 had NO entry in
        # the (truncated) response — it must resurface as a failure (transient),
        # never silently assumed acknowledged (#2799).
        assert exc_info.value.acknowledged == ["item-1"]
        failure_ids = {f[0] for f in exc_info.value.failures}
        assert failure_ids == {"item-2", "item-3"}
        item3_reason = next(r for i, r in exc_info.value.failures if i == "item-3")
        assert "truncated" in item3_reason


# ---------------------------------------------------------------------------
# Task 2 — private driver write_entities
# ---------------------------------------------------------------------------


class TestPrivateDriverWriteEntities:
    @pytest.mark.asyncio
    async def test_raises_on_mapper_parsing_exception(self, caplog):
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        driver = ItemsElasticsearchPrivateDriver()

        mock_es = AsyncMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        mock_es.bulk = AsyncMock(return_value=_bulk_error_response(
            "mapper_parsing_exception", "dup coords", 400, "priv-1",
        ))

        items = [{"id": "priv-1", "type": "Feature", "geometry": None, "properties": {}}]

        with (
            patch.object(driver, "_get_client", return_value=mock_es),
            patch.object(driver, "_items_index_name", return_value="priv-idx"),
            patch.object(driver, "_resolve_simplify_geometry", new=AsyncMock(return_value=False)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.mappings.resolve_catalog_private_known_fields",
                new=AsyncMock(return_value=[]),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.doc_builder.build_tenant_feature_doc",
                side_effect=lambda item, **kw: dict(item),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.mappings.project_private_doc",
                side_effect=lambda doc, _fields: doc,
            ),
            patch(
                "dynastore.tools.geometry_simplify.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, 1.0, "none"),
            ),
            caplog.at_level(logging.ERROR),
        ):
            with pytest.raises(EsBulkWriteError) as exc_info:
                await driver.write_entities("cat1", "col1", items)

        assert exc_info.value.failures
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    @pytest.mark.asyncio
    async def test_error_logs_items_with_no_id(self, caplog):
        """Items without an id must be ERROR-logged, not silently skipped."""
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        driver = ItemsElasticsearchPrivateDriver()

        mock_es = AsyncMock()
        mock_es.indices.exists = AsyncMock(return_value=True)

        items = [{"type": "Feature", "geometry": None, "properties": {}}]  # no id

        with (
            patch.object(driver, "_get_client", return_value=mock_es),
            patch.object(driver, "_items_index_name", return_value="priv-idx"),
            patch.object(driver, "_resolve_simplify_geometry", new=AsyncMock(return_value=False)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch_private.mappings.resolve_catalog_private_known_fields",
                new=AsyncMock(return_value=[]),
            ),
            caplog.at_level(logging.ERROR),
        ):
            await driver.write_entities("cat1", "col1", items)

        error_msgs = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("skipped" in m or "no id" in m for m in error_msgs), (
            f"Expected ERROR about missing id, got: {error_msgs}"
        )
        # bulk() must NOT have been called (nothing to send)
        mock_es.bulk.assert_not_called()


# ---------------------------------------------------------------------------
# Task 2 — envelope driver write_entities
# ---------------------------------------------------------------------------


class TestEnvelopeDriverWriteEntities:
    @pytest.mark.asyncio
    async def test_raises_on_mapper_parsing_exception(self, caplog):
        from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
            ItemsElasticsearchEnvelopeDriver,
        )
        from dynastore.modules.storage.errors import EsBulkWriteError

        driver = ItemsElasticsearchEnvelopeDriver()

        mock_es = AsyncMock()
        mock_es.indices.exists = AsyncMock(return_value=True)
        mock_es.bulk = AsyncMock(return_value=_bulk_error_response(
            "mapper_parsing_exception", "bad shape", 400, "env-1",
        ))

        items = [{"id": "env-1", "type": "Feature", "geometry": None, "properties": {}}]

        with (
            patch.object(driver, "_get_client", return_value=mock_es),
            patch.object(driver, "_items_index_name", return_value="env-idx"),
            patch.object(driver, "_resolve_simplify_geometry", new=AsyncMock(return_value=False)),
            patch.object(
                type(driver), "_ensure_index",
                new=AsyncMock(),
            ),
            patch.object(driver, "_build_doc", side_effect=lambda item, **kw: dict(item)),
            patch(
                "dynastore.tools.geometry_simplify.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, 1.0, "none"),
            ),
            caplog.at_level(logging.ERROR),
        ):
            with pytest.raises(EsBulkWriteError) as exc_info:
                await driver.write_entities("cat1", "col1", items)

        assert exc_info.value.failures
        assert any(r.levelno == logging.ERROR for r in caplog.records)


# ---------------------------------------------------------------------------
# Task 4 — circuit-breaker open → OUTBOX enqueue
# ---------------------------------------------------------------------------


class TestCircuitBreakerOutboxEnqueue:
    """When the breaker is open, _dispatch_bulk must call _handle_failure_bulk
    so on_failure=OUTBOX still enqueues the batch."""

    @pytest.mark.asyncio
    async def test_breaker_open_with_outbox_policy_calls_handle_failure_bulk(self):
        from dynastore.models.protocols.indexer import IndexContext, IndexOp
        from dynastore.modules.storage.circuit_breaker import CircuitBreaker
        from dynastore.modules.storage.index_dispatcher import IndexDispatcher
        from dynastore.modules.storage.routing_config import (
            FailurePolicy, Operation, OperationDriverEntry, WriteMode,
        )

        # Build a breaker that is already open for "es-driver".
        breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=9999)
        # Force it open by recording one failure (threshold=1).
        breaker.record_failure("es-driver")
        assert breaker.is_open("es-driver")

        enqueued: list = []

        class _FakeOutbox:
            """Minimal outbox stub that implements the legacy enqueue() surface
            (IndexOp shape) which _enqueue_or_warn calls for IndexOp batches.
            """
            async def enqueue(self, *, indexer_id, ctx, ops, last_error=None):
                enqueued.extend(ops)

        entry = OperationDriverEntry(
            driver_ref="es-driver",
            on_failure=FailurePolicy.OUTBOX,
            write_mode=WriteMode.SYNC,
            secondary_index=True,
            source="auto",
        )

        class _StubRouting:
            operations = {Operation.WRITE: [entry]}

        async def _routing(c, col):
            return _StubRouting()

        async def _registry(ref):
            return None  # driver not actually needed — breaker fires first

        dispatcher = IndexDispatcher(
            routing_resolver=_routing,
            indexer_registry=_registry,
            outbox=_FakeOutbox(),  # type: ignore[arg-type]
            breaker=breaker,
        )

        # ``pg_conn`` must be a live handle for ``_enqueue_or_warn`` to
        # actually enqueue (drop path (b), #2686): without one the durable
        # write can't be made transactional with the caller's TX, so the
        # dispatcher degrades to WARN instead of enqueuing.
        ctx = IndexContext(
            catalog="cat1", collection="col1", correlation_id="cid",
            pg_conn=object(),
        )
        ops = [
            IndexOp(op_type="upsert", entity_type="item", entity_id="i1"),
        ]

        await dispatcher.fan_out_bulk(ctx, ops)

        # The OUTBOX handler must have been called.
        assert enqueued, (
            "Expected at least one outbox row when breaker is open with "
            "on_failure=OUTBOX, but enqueued list is empty."
        )

    @pytest.mark.asyncio
    async def test_breaker_open_with_warn_policy_does_not_enqueue(self):
        from dynastore.models.protocols.indexer import IndexContext, IndexOp
        from dynastore.modules.storage.circuit_breaker import CircuitBreaker
        from dynastore.modules.storage.index_dispatcher import IndexDispatcher
        from dynastore.modules.storage.routing_config import (
            FailurePolicy, Operation, OperationDriverEntry, WriteMode,
        )

        breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=9999)
        breaker.record_failure("es-warn")

        enqueued: list = []

        class _FakeOutbox:
            async def enqueue(self, *, indexer_id, ctx, ops, last_error=None):
                enqueued.extend(ops)

        entry = OperationDriverEntry(
            driver_ref="es-warn",
            on_failure=FailurePolicy.WARN,
            write_mode=WriteMode.SYNC,
            secondary_index=True,
            source="auto",
        )

        class _StubRouting:
            operations = {Operation.WRITE: [entry]}

        async def _routing(c, col):
            return _StubRouting()

        async def _registry(ref):
            return None

        dispatcher = IndexDispatcher(
            routing_resolver=_routing,
            indexer_registry=_registry,
            outbox=_FakeOutbox(),  # type: ignore[arg-type]
            breaker=breaker,
        )

        ctx = IndexContext(catalog="cat1", collection="col1", correlation_id="cid")
        ops = [IndexOp(op_type="upsert", entity_type="item", entity_id="i1")]
        await dispatcher.fan_out_bulk(ctx, ops)
        # WARN policy — nothing enqueued.
        assert not enqueued
