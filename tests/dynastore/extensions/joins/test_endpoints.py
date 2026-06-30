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
