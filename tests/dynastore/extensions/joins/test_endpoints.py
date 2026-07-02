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

from dynastore.extensions.joins.joins_service import JoinsService


def _build_service():
    return JoinsService()


def test_resolve_paging_clamps_over_max_instead_of_erroring():
    """OGC API - Features Part 1 Core /req/core/fc-limit-response-1: a
    ``?limit=`` above ``MAX_PAGE_LIMIT`` is clamped, not rejected. The
    ``Query(ge=1, le=MAX_PAGE_LIMIT)`` gate that used to 422 here was
    removed; ``_resolve_paging`` is now the sole enforcement point."""
    from dynastore.extensions.joins.joins_service import (
        DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, _resolve_paging,
    )
    from dynastore.modules.joins.models import JoinRequest

    body = JoinRequest(
        secondary={"driver": "registered", "ref": "my-bq-collection"},
        join={"primary_column": "uid", "secondary_column": "user_id"},
    )

    over_max = _resolve_paging(body, limit=MAX_PAGE_LIMIT * 2, offset=None)
    assert over_max.limit == MAX_PAGE_LIMIT

    omitted = _resolve_paging(body, limit=None, offset=None)
    assert omitted.limit == DEFAULT_PAGE_LIMIT

    in_range = _resolve_paging(body, limit=250, offset=None)
    assert in_range.limit == 250


@pytest.mark.asyncio
async def test_describe_lists_registered_driver():
    from unittest.mock import MagicMock
    from fastapi import Request

    svc = _build_service()
    req = MagicMock(spec=Request)
    req.url = "http://ex/join/catalogs/c/collections/l/join"
    payload = await svc.describe_join("c", "l", req)
    assert "registered" in payload["supported_secondary_drivers"]
    assert "bigquery" in payload["supported_secondary_drivers"]
    assert payload["primary"]["catalog"] == "c"




@pytest.mark.asyncio
async def test_execute_join_bigquery_materializes_secondary(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    # Patch the bq_secondary streamer so we don't hit real BigQuery.
    async def fake_stream(spec, *, secondary_column, **kwargs):
        from dynastore.models.ogc import Feature
        yield Feature(type="Feature", id="r1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_stream)

    # Provide a primary driver so the request runs through run_join; use a
    # non-matching primary so the join yields zero features but the
    # secondary-materialization counter is still reported.
    class _EmptyPrimary:
        async def read_entities(self, *args, **kwargs):
            if False:
                yield  # pragma: no cover — empty stream

    fake_resolved = type("R", (), {"driver": _EmptyPrimary()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert resp["type"] == "FeatureCollection"
    # Conformant response: no _join_meta foreign member; carries OGC API -
    # Features members instead. A non-matching primary yields an empty page.
    assert "_join_meta" not in resp
    assert resp["features"] == []
    assert resp["numberReturned"] == 0
    assert "timeStamp" in resp
    # A partial page (< limit) advertises no `next` link.
    assert {ln["rel"] for ln in resp["links"]} == {"self"}


@pytest.mark.asyncio
async def test_execute_join_bigquery_end_to_end(monkeypatch):
    """Primary stream + BQ secondary materialization + dict join → real FeatureCollection."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    # Fake BQ secondary stream.
    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})
        yield Feature(type="Feature", id="bq2", geometry=None,
                      properties={"user_id": "bob", "score": 7})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    # Fake primary driver returning two features matching one BQ row.
    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "carol", "name": "Carol"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert resp["type"] == "FeatureCollection"
    assert len(resp["features"]) == 1  # only Alice matches
    assert resp["features"][0]["properties"]["score"] == 42
    assert "_join_meta" not in resp
    assert resp["numberReturned"] == 1
    assert "timeStamp" in resp
    assert any(ln["rel"] == "self" for ln in resp["links"])


@pytest.mark.asyncio
async def test_execute_join_bigquery_returns_404_when_no_primary_driver(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import HTTPException, Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        if False:
            yield  # empty

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[]))

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    with pytest.raises(HTTPException) as exc:
        await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_execute_join_named_secondary_resolves_via_registry(monkeypatch):
    """Both primary and secondary resolved via resolve_drivers; join executes."""
    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        JoinRequest, JoinSpec, NamedSecondarySpec,
    )
    from dynastore.models.ogc import Feature

    class _PrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "color": "red"})

    class _SecondaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="s1", geometry=None,
                          properties={"user_id": "alice", "score": 99})

    primary_resolved = type("R", (), {"driver": _PrimaryDriver()})()
    secondary_resolved = type("R", (), {"driver": _SecondaryDriver()})()

    # resolve_drivers is called twice: once for the secondary (with secondary
    # collection_id) and once for the primary. Use a side_effect that returns
    # the right driver per call.
    calls = []
    async def fake_resolve(operation, catalog_id, collection_id=None, **kwargs):
        calls.append((catalog_id, collection_id))
        if collection_id == "the-other-collection":
            return [secondary_resolved]
        return [primary_resolved]

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "resolve_drivers", fake_resolve)

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=NamedSecondarySpec(ref="the-other-collection"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert resp["type"] == "FeatureCollection"
    assert len(resp["features"]) == 1
    assert resp["features"][0]["properties"]["score"] == 99
    assert "_join_meta" not in resp
    assert resp["numberReturned"] == 1


@pytest.mark.asyncio
async def test_execute_join_named_404_when_secondary_missing(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import HTTPException, Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        JoinRequest, JoinSpec, NamedSecondarySpec,
    )

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=NamedSecondarySpec(ref="missing"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    with pytest.raises(HTTPException) as exc:
        await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_primary_filter_forwarded_as_cql_filter_on_query_request(monkeypatch):
    """When primary_filter is set, driver.read_entities sees request.cql_filter."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PrimaryFilterSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    observed_requests = []

    class _RecordingPrimary:
        async def read_entities(self, catalog_id, collection_id, **kwargs):
            observed_requests.append(kwargs.get("request"))
            if False:
                yield  # empty stream — join returns no features

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        if False:
            yield

    import dynastore.modules.joins.bq_secondary as bq_mod
    monkeypatch.setattr(bq_mod, "stream_bigquery_secondary", fake_bq_stream)
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    primary_resolved = type("R", (), {"driver": _RecordingPrimary()})()
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[primary_resolved]))

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        primary_filter=PrimaryFilterSpec(cql="status='active'"),
    )
    await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert len(observed_requests) == 1
    qr = observed_requests[0]
    assert qr is not None
    assert qr.cql_filter == "status='active'"


@pytest.mark.asyncio
async def test_primary_filter_validation_error_maps_to_400(monkeypatch):
    """If the primary driver raises ValueError while parsing CQL, map to 400."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import HTTPException, Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PrimaryFilterSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    class _RaisingPrimary:
        async def read_entities(self, catalog_id, collection_id, **kwargs):
            raise ValueError("Unknown CQL2 property: bogus_field")
            yield  # pragma: no cover

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        if False:
            yield

    import dynastore.modules.joins.bq_secondary as bq_mod
    monkeypatch.setattr(bq_mod, "stream_bigquery_secondary", fake_bq_stream)
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    primary_resolved = type("R", (), {"driver": _RaisingPrimary()})()
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[primary_resolved]))

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        primary_filter=PrimaryFilterSpec(cql="bogus_field='x'"),
    )
    with pytest.raises(HTTPException) as exc:
        await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert exc.value.status_code == 400
    assert "Unknown CQL2 property" in str(exc.value.detail)


def test_joins_service_discovered_via_entry_point():
    """JoinsService must be wired through the dynastore.extensions entry-point group.

    Bundles with the dwh extra: ``dynastore[dwh]`` installs both, so
    deployments that ship the legacy tile-join surface also expose the
    new OGC /join/* endpoints.
    """
    from importlib.metadata import entry_points
    eps = [
        ep for ep in entry_points(group="dynastore.extensions")
        if ep.name == "joins"
    ]
    assert len(eps) == 1
    assert "JoinsService" in eps[0].value


@pytest.mark.asyncio
async def test_execute_join_left_join_yields_all_primary_features(monkeypatch):
    """LEFT JOIN yields all primary features, with null secondary props for non-matches."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    # Fake BQ secondary stream - only has alice and bob
    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})
        yield Feature(type="Feature", id="bq2", geometry=None,
                      properties={"user_id": "bob", "score": 7})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    # Fake primary driver with three features: alice matches, carol doesn't
    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "carol", "name": "Carol"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(
            primary_column="uid",
            secondary_column="user_id",
            join_type="LEFT",
        ),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert resp["type"] == "FeatureCollection"
    # LEFT JOIN should yield both features
    assert len(resp["features"]) == 2
    # Alice matches - has score
    assert resp["features"][0]["properties"]["name"] == "Alice"
    assert resp["features"][0]["properties"]["score"] == 42
    # Carol doesn't match - no score field
    assert resp["features"][1]["properties"]["name"] == "Carol"
    assert "score" not in resp["features"][1]["properties"]


@pytest.mark.asyncio
async def test_execute_join_inner_join_filters_non_matching(monkeypatch):
    """INNER JOIN (default) filters out primary features without matching secondary."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    # Fake BQ secondary stream - only has alice
    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    # Fake primary driver with two features: alice matches, carol doesn't
    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "carol", "name": "Carol"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(
            primary_column="uid",
            secondary_column="user_id",
            join_type="INNER",  # explicit INNER JOIN
        ),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert resp["type"] == "FeatureCollection"
    # INNER JOIN should only yield matching feature
    assert len(resp["features"]) == 1
    assert resp["features"][0]["properties"]["name"] == "Alice"
    assert resp["features"][0]["properties"]["score"] == 42


@pytest.mark.asyncio
async def test_execute_join_default_is_inner_join(monkeypatch):
    """Default join_type (when not specified) is INNER JOIN."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "carol", "name": "Carol"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    # Not specifying join_type - should default to INNER
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(
            primary_column="uid",
            secondary_column="user_id",
            # join_type not specified
        ),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())
    assert resp["type"] == "FeatureCollection"
    # Default INNER JOIN should only yield matching feature
    assert len(resp["features"]) == 1
    assert resp["features"][0]["properties"]["name"] == "Alice"


@pytest.mark.asyncio
async def test_execute_join_paginates_with_conformant_next_link(monkeypatch):
    """A full page emits a followable `next` link with offset advanced by limit.

    Two primary features match the secondary; ``?limit=1`` returns the first
    page (1 feature) and, because the page is full, a ``next`` link pointing at
    the same endpoint with ``offset=1&limit=1`` so the POST body can be replayed.
    """
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PagingSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 1})
        yield Feature(type="Feature", id="bq2", geometry=None,
                      properties={"user_id": "bob", "score": 2})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "bob", "name": "Bob"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    req.url = "http://h/api/maps/join/catalogs/c/collections/l/join?hints=geometry_exact"
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        paging=PagingSpec(limit=1, offset=0),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    assert resp["type"] == "FeatureCollection"
    assert "_join_meta" not in resp
    assert resp["numberReturned"] == 1
    assert len(resp["features"]) == 1
    assert "timeStamp" in resp
    links = {ln["rel"]: ln for ln in resp["links"]}
    assert links["self"]["type"] == "application/geo+json"
    # Full page → next link, offset advanced by limit, other params preserved.
    assert "next" in links
    nxt = links["next"]["href"]
    assert "offset=1" in nxt and "limit=1" in nxt
    assert "hints=geometry_exact" in nxt
    # First page (offset=0) must not advertise a `prev` link.
    assert "prev" not in links


@pytest.mark.asyncio
async def test_execute_join_offset_page_emits_prev_link(monkeypatch):
    """A page with offset>0 emits a `prev` link (mirrors `next`'s link builder),
    pointing back at offset - limit."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PagingSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 1})
        yield Feature(type="Feature", id="bq2", geometry=None,
                      properties={"user_id": "bob", "score": 2})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "bob", "name": "Bob"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    req.url = "http://h/join/catalogs/c/collections/l/join"
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        paging=PagingSpec(limit=1, offset=1),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    links = {ln["rel"]: ln for ln in resp["links"]}
    assert "prev" in links
    prev_href = links["prev"]["href"]
    assert "offset=0" in prev_href and "limit=1" in prev_href
    # Final page (no more matches beyond bob) → no `next`.
    assert "next" not in links
    # This is also a terminal page reached without a truncated scan, so the
    # exact total is known for free.
    assert resp["numberMatched"] == 2


@pytest.mark.asyncio
async def test_execute_join_honors_accept_application_json(monkeypatch):
    """`Accept: application/json` gets a plain JSONResponse, not geo+json."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi.responses import JSONResponse
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    req.url = "http://h/join/catalogs/c/collections/l/join"
    req.headers = {"Accept": "application/json"}
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    assert isinstance(resp, JSONResponse)
    assert resp.media_type == "application/json"
    import json
    payload = json.loads(resp.body)
    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 1


@pytest.mark.asyncio
async def test_execute_join_default_accept_stays_geojson_dict(monkeypatch):
    """Absent/`*/*` `Accept` keeps the existing dict return (served as
    geo+json by the route's `response_class`)."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    req.url = "http://h/join/catalogs/c/collections/l/join"
    req.headers = {"Accept": "*/*"}
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    assert isinstance(resp, dict)
    assert resp["type"] == "FeatureCollection"


# ---------------------------------------------------------------------------
# Sparse-join pagination integration tests (issue #2587)
#
# These tests exercise execute_join end-to-end with a mocked primary driver
# that yields many non-matching rows so the inner-join is sparse (<<1 match
# per primary row).  We verify that the match-bounded read strategy fills
# pages correctly and emits `next` links only when warranted.
# ---------------------------------------------------------------------------


def _make_sparse_bq_env(monkeypatch, *, match_ids, primary_rows):
    """Wire up monkeypatched BQ secondary + primary driver for sparse-join tests.

    ``match_ids`` is the set of uid values that appear in both primary and BQ
    secondary. Primary rows for non-matching uids are yielded but produce no
    join output (INNER join).

    Returns (svc_mod, JoinsService instance).
    """
    from unittest.mock import AsyncMock
    from dynastore.models.ogc import Feature
    import dynastore.extensions.joins.joins_service as svc_mod

    # BQ secondary: one row per match_id.
    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        for uid in match_ids:
            yield Feature(
                type="Feature", id=uid, geometry=None,
                properties={"user_id": uid, "score": list(match_ids).index(uid)},
            )

    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    # Primary driver: yields primary_rows features, only match_ids have a uid
    # that appears in the secondary.  Non-matching rows have unique uids.
    primary_features = []
    match_list = list(match_ids)
    match_idx = 0
    non_match_counter = 0
    for i in range(primary_rows):
        if match_idx < len(match_list) and i % (primary_rows // max(len(match_list), 1)) == 0:
            uid = match_list[match_idx]
            match_idx += 1
        else:
            uid = f"nomatch_{non_match_counter}"
            non_match_counter += 1
        primary_features.append(
            Feature(type="Feature", id=str(i), geometry=None, properties={"uid": uid})
        )

    class _SparsePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            for feat in primary_features:
                yield feat

    fake_resolved = type("R", (), {"driver": _SparsePrimaryDriver()})()
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]))

    return svc_mod, svc_mod.JoinsService()


@pytest.mark.asyncio
async def test_execute_join_sparse_page1_returns_full_limit_and_has_next(monkeypatch):
    """Page 1 of a sparse INNER join must return exactly `limit` matches and a
    `next` link, even when non-matching primary rows outnumber matching ones.

    This is the core regression: the old row-based read window (offset+limit rows)
    would stop the primary scan before finding `limit` matches and suppress `next`.
    """
    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PagingSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    # 4 matches in 20 primary rows (1-in-5 match rate).
    match_ids = ["m0", "m1", "m2", "m3"]
    svc_mod, svc = _make_sparse_bq_env(monkeypatch, match_ids=match_ids, primary_rows=20)

    req = MagicMock(spec=Request)
    req.url = "http://h/join/catalogs/c/collections/l/join"
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        paging=PagingSpec(limit=2, offset=0),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    assert resp["type"] == "FeatureCollection"
    # Page 1 must return exactly limit=2 matches (not 0 or 1).
    assert resp["numberReturned"] == 2, (
        f"sparse page 1 must return limit=2 matches, got {resp['numberReturned']}"
    )
    assert len(resp["features"]) == 2
    # More matches exist (m2, m3) so a `next` link must be present.
    link_rels = {ln["rel"] for ln in resp["links"]}
    assert "next" in link_rels, "next link missing on sparse page 1 that has more matches"
    next_href = next(ln["href"] for ln in resp["links"] if ln["rel"] == "next")
    assert "offset=2" in next_href and "limit=2" in next_href


@pytest.mark.asyncio
async def test_execute_join_sparse_page2_no_gaps_and_no_next(monkeypatch):
    """Page 2 of a sparse join returns the remaining matches with no gaps and
    no `next` link (the page is the final one).

    Page 1 consumed offset=0..1; page 2 must start at offset=2 and return
    matches 2 and 3 (m2, m3), with no overlap and no missing items.
    """
    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PagingSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    match_ids = ["m0", "m1", "m2", "m3"]
    svc_mod, svc = _make_sparse_bq_env(monkeypatch, match_ids=match_ids, primary_rows=20)

    req = MagicMock(spec=Request)
    req.url = "http://h/join/catalogs/c/collections/l/join"
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        paging=PagingSpec(limit=2, offset=2),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    assert resp["type"] == "FeatureCollection"
    assert resp["numberReturned"] == 2, (
        f"sparse page 2 must return the 2 remaining matches, got {resp['numberReturned']}"
    )
    # The last page: no more matches after m2, m3 → no `next` link.
    link_rels = {ln["rel"] for ln in resp["links"]}
    assert "next" not in link_rels, "next link must be absent on the final sparse page"


@pytest.mark.asyncio
async def test_execute_join_ceiling_hit_emits_next(monkeypatch):
    """When the primary-scan safety ceiling is hit before finding limit+1 matches,
    a `next` link is still emitted so the client does not stop prematurely.

    We monkeypatch MAX_PRIMARY_SCAN_ROWS to a small value (5) so it trips with
    only 10 primary rows, even though more matches exist beyond the ceiling.
    """
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.models.ogc import Feature
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec, PagingSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    import dynastore.extensions.joins.joins_service as svc_mod

    # All 10 primary rows match the secondary (dense join), but the ceiling is
    # lowered to 5 rows so the scan is truncated before finding limit+1=3 matches.
    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        for i in range(10):
            yield Feature(type="Feature", id=f"s{i}", geometry=None,
                          properties={"user_id": str(i), "val": i})

    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    class _DensePrimary:
        async def read_entities(self, *args, limit=None, **kwargs):
            cap = limit if limit is not None else 10
            for i in range(min(10, cap)):
                yield Feature(type="Feature", id=str(i), geometry=None,
                              properties={"uid": str(i)})

    fake_resolved = type("R", (), {"driver": _DensePrimary()})()
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]))

    # Ceiling of 5 rows: with limit=2 (peek=3), only 5 primary rows are scanned,
    # yielding 5 matches. 5 >= 3 (peek limit), so has_next = True from peek.
    # Actually with limit=2+1=3 peek limit and 5 rows scanned (all matching),
    # run_join yields 3 matches → len(joined)=3 > paging.limit=2 → has_next=True.
    # But if ceiling=2 and dense primary with limit=2 peek → only 2 rows scanned,
    # yielding 2 matches < peek 3 → ceiling_hit=True → has_next=True.
    monkeypatch.setattr(svc_mod, "MAX_PRIMARY_SCAN_ROWS", 2)

    svc = svc_mod.JoinsService()
    req = MagicMock(spec=Request)
    req.url = "http://h/join/catalogs/c/collections/l/join"
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
        paging=PagingSpec(limit=2, offset=0),
    )
    resp = await svc.execute_join("c", "l", req, body=body, request_hints=frozenset())

    assert resp["type"] == "FeatureCollection"
    # Ceiling (2 rows) < limit+1 (3), so `next` must be emitted regardless of
    # whether the page is full — the scan was truncated.
    link_rels = {ln["rel"] for ln in resp["links"]}
    assert "next" in link_rels, "ceiling hit must emit `next` so the client keeps paging"
