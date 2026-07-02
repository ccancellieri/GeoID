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

import pytest
from pydantic import ValidationError

from dynastore.modules.joins.models import JoinRequest


def _minimal_request_dict():
    return {
        "secondary": {"driver": "registered", "ref": "my-bq-collection"},
        "join": {"primary_column": "uid", "secondary_column": "user_id"},
    }


def test_minimal_request_roundtrips():
    req = JoinRequest(**_minimal_request_dict())
    assert req.secondary.driver == "registered"
    assert req.secondary.ref == "my-bq-collection"
    assert req.join.enrichment is True
    assert req.projection.with_geometry is True
    assert req.output.format == "geojson"


def test_unknown_secondary_driver_rejected():
    bad = _minimal_request_dict()
    bad["secondary"]["driver"] = "bogus"
    with pytest.raises(ValidationError):
        JoinRequest(**bad)


def test_extra_field_rejected_at_top_level():
    bad = _minimal_request_dict()
    bad["bogus"] = 1
    with pytest.raises(ValidationError):
        JoinRequest(**bad)


def test_paging_optional_and_clamped():
    req = JoinRequest(**_minimal_request_dict(), paging={"limit": 500, "offset": 0})
    assert req.paging.limit == 500
    with pytest.raises(ValidationError):
        JoinRequest(**_minimal_request_dict(), paging={"limit": 0, "offset": 0})
    # An over-max ``limit`` is no longer rejected at the model layer (OGC API
    # - Features Part 1 Core /req/core/fc-limit-response-1: over-max SHALL
    # NOT error). ``joins_service._resolve_paging`` clamps it to
    # ``MAX_PAGE_LIMIT`` at request-handling time instead.
    req_over_max = JoinRequest(
        **_minimal_request_dict(), paging={"limit": 100000, "offset": 0}
    )
    assert req_over_max.paging.limit == 100000


def test_output_format_enum_validated():
    base = _minimal_request_dict()
    JoinRequest(**base, output={"format": "csv"})
    with pytest.raises(ValidationError):
        JoinRequest(**base, output={"format": "xml"})


def test_bigquery_secondary_round_trips():
    from dynastore.modules.joins.models import BigQuerySecondarySpec
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    spec = BigQuerySecondarySpec(
        target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
    )
    assert spec.driver == "bigquery"
    assert spec.target.fqn() == "p.d.t"


def test_join_request_accepts_bigquery_secondary():
    body = {
        "secondary": {
            "driver": "bigquery",
            "target": {"project_id": "p", "dataset_id": "d", "table_name": "t"},
        },
        "join": {"primary_column": "uid", "secondary_column": "user_id"},
    }
    req = JoinRequest(**body)
    assert req.secondary.driver == "bigquery"
    assert req.secondary.target.dataset_id == "d"


def test_bigquery_secondary_rejects_unfully_qualified_target_at_use_time():
    """The DTO accepts partial targets (consistent with Phase 4a's BigQueryTarget),
    but the executor's resolver will reject at request time. The DTO-level test
    just confirms partial targets are *accepted* by the discriminator."""
    from dynastore.modules.joins.models import BigQuerySecondarySpec
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    spec = BigQuerySecondarySpec(target=BigQueryTarget(project_id="p"))
    assert not spec.target.is_fully_qualified()


def test_primary_filter_optional_and_validated():
    from dynastore.modules.joins.models import PrimaryFilterSpec  # noqa: F401

    # Absent by default.
    req = JoinRequest(**_minimal_request_dict())
    assert req.primary_filter is None

    # Accepts CQL2-text (default).
    req = JoinRequest(**_minimal_request_dict(), primary_filter={"cql": "status='active'"})
    assert req.primary_filter is not None
    assert req.primary_filter.cql == "status='active'"
    assert req.primary_filter.cql_lang == "cql2-text"

    # Rejects empty string.
    with pytest.raises(ValidationError):
        JoinRequest(**_minimal_request_dict(), primary_filter={"cql": ""})

    # Rejects unknown cql_lang.
    with pytest.raises(ValidationError):
        JoinRequest(**_minimal_request_dict(), primary_filter={"cql": "x", "cql_lang": "ogc-filter"})


def test_primary_filter_rejects_extra_fields():
    with pytest.raises(ValidationError):
        JoinRequest(**_minimal_request_dict(), primary_filter={"cql": "x", "bogus": 1})
