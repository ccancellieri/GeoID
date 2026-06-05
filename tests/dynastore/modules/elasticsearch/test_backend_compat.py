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

"""Unit tests for the OpenSearch/Elasticsearch mapping dialect shim.

Covers distribution detection, the ``flattened`` → ``flat_object`` rewrite on
the real canonical mappings (no in-place mutation), and the idempotent
``indices.create`` / ``indices.put_mapping`` wrapper that auto-applies it.
"""

import json

import pytest

from dynastore.modules.elasticsearch._backend_compat import (
    install_opensearch_mapping_shim,
    is_opensearch_distribution,
    rewrite_es_only_types,
)


def _count_type(body, t):
    return json.dumps(body).count(f'"type": "{t}"')


# --------------------------------------------------------------------------
# distribution detection
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "info,expected",
    [
        ({"version": {"distribution": "opensearch", "number": "2.17.0"}}, True),
        ({"version": {"distribution": "OpenSearch"}}, True),  # case-insensitive
        ({"version": {"number": "8.13.0", "build_flavor": "default"}}, False),
        ({"version": {}}, False),
        ({}, False),
        (None, False),
        ("not-a-dict", False),
    ],
)
def test_is_opensearch_distribution(info, expected):
    assert is_opensearch_distribution(info) is expected


# --------------------------------------------------------------------------
# rewrite
# --------------------------------------------------------------------------


def test_rewrite_flattened_to_flat_object():
    body = {
        "mappings": {
            "properties": {
                "extras": {"type": "flattened"},
                "id": {"type": "keyword"},
            }
        }
    }
    out = rewrite_es_only_types(body)
    assert _count_type(out, "flattened") == 0
    assert _count_type(out, "flat_object") == 1
    # original not mutated
    assert body["mappings"]["properties"]["extras"]["type"] == "flattened"


def test_rewrite_is_noop_identity_when_no_match():
    body = {"mappings": {"properties": {"id": {"type": "keyword"}}}}
    # Returns the same object (no copy) so it is free to apply universally.
    assert rewrite_es_only_types(body) is body


def test_rewrite_walks_nested_and_lists():
    body = {
        "properties": {
            "a": {"properties": {"deep": {"type": "flattened"}}},
            "templates": [{"t": {"mapping": {"type": "flattened"}}}],
        }
    }
    out = rewrite_es_only_types(body)
    assert _count_type(out, "flattened") == 0
    assert _count_type(out, "flat_object") == 2


def test_rewrite_real_canonical_mappings():
    from dynastore.modules.elasticsearch.mappings import (
        CATALOG_MAPPING,
        COLLECTION_MAPPING,
        ITEM_MAPPING,
    )

    for mapping in (CATALOG_MAPPING, COLLECTION_MAPPING, ITEM_MAPPING):
        before = _count_type(mapping, "flattened")
        assert before >= 1
        out = rewrite_es_only_types(mapping)
        assert _count_type(mapping, "flattened") == before  # original untouched
        assert _count_type(out, "flattened") == 0
        assert _count_type(out, "flat_object") >= before


# --------------------------------------------------------------------------
# shim install
# --------------------------------------------------------------------------


class _FakeIndices:
    def __init__(self):
        self.created_bodies = []
        self.put_bodies = []

    async def create(self, *, index, body=None, **kw):
        self.created_bodies.append(body)
        return {"acknowledged": True, "index": index}

    async def put_mapping(self, *, index, body=None, **kw):
        self.put_bodies.append(body)
        return {"acknowledged": True}


class _FakeClient:
    def __init__(self):
        self.indices = _FakeIndices()


@pytest.mark.asyncio
async def test_shim_rewrites_create_body():
    client = _FakeClient()
    install_opensearch_mapping_shim(client)
    await client.indices.create(
        index="x", body={"mappings": {"properties": {"extras": {"type": "flattened"}}}}
    )
    sent = client.indices.created_bodies[-1]
    assert _count_type(sent, "flattened") == 0
    assert _count_type(sent, "flat_object") == 1


@pytest.mark.asyncio
async def test_shim_rewrites_put_mapping_body():
    client = _FakeClient()
    install_opensearch_mapping_shim(client)
    await client.indices.put_mapping(
        index="x", body={"properties": {"extras": {"type": "flattened"}}}
    )
    sent = client.indices.put_bodies[-1]
    assert _count_type(sent, "flattened") == 0
    assert _count_type(sent, "flat_object") == 1


@pytest.mark.asyncio
async def test_shim_is_idempotent():
    client = _FakeClient()
    install_opensearch_mapping_shim(client)
    first = client.indices.create
    install_opensearch_mapping_shim(client)
    # second install must not re-wrap
    assert client.indices.create is first
    await client.indices.create(
        index="x", body={"mappings": {"properties": {"e": {"type": "flattened"}}}}
    )
    assert _count_type(client.indices.created_bodies[-1], "flattened") == 0


@pytest.mark.asyncio
async def test_shim_passes_through_bodyless_calls():
    client = _FakeClient()
    install_opensearch_mapping_shim(client)
    await client.indices.create(index="x")  # no body
    assert client.indices.created_bodies[-1] is None
