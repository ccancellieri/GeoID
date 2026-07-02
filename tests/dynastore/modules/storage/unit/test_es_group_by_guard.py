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

"""#2829: a ``group_by`` request must fail loudly against non-PG item drivers
instead of silently degenerating to an ungrouped page scan.

With hint-derivation (``item_query._derive_hints_from_request``) in place,
dispatch never sends ``group_by`` to these drivers in practice — but a direct
``read_entities`` call must not silently lie about what it served.
"""

from __future__ import annotations

import pytest

from dynastore.models.query_builder import FieldSelection, QueryRequest
from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
from dynastore.modules.storage.drivers.elasticsearch_private import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
    ItemsElasticsearchEnvelopeDriver,
)


def _group_by_request() -> QueryRequest:
    return QueryRequest(select=[FieldSelection(field="region")], group_by=["region"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "driver",
    [
        ItemsElasticsearchDriver(),
        ItemsElasticsearchPrivateDriver(),
        ItemsElasticsearchEnvelopeDriver(),
    ],
    ids=["public", "private", "envelope"],
)
async def test_read_entities_rejects_group_by(driver):
    with pytest.raises(ValueError, match="group_by"):
        async for _ in driver.read_entities(
            "cat1", "col1", request=_group_by_request(),
        ):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "driver",
    [
        ItemsElasticsearchDriver(),
        ItemsElasticsearchPrivateDriver(),
        ItemsElasticsearchEnvelopeDriver(),
    ],
    ids=["public", "private", "envelope"],
)
async def test_read_entities_no_group_by_does_not_raise_the_guard(driver, monkeypatch):
    """Sanity check: the guard is specific to ``group_by`` — a plain request
    without it must not be rejected by ``_reject_unsupported_group_by``
    itself (it may still fail later for unrelated reasons, e.g. no ES client
    configured in this unit-test environment)."""
    request = QueryRequest(select=[FieldSelection(field="region")])
    try:
        async for _ in driver.read_entities("cat1", "col1", request=request):
            pass
    except ValueError as exc:
        assert "group_by" not in str(exc)
    except Exception:
        # No live ES client in this unit test — any other failure mode is
        # out of scope; only the group_by guard itself is under test here.
        pass
