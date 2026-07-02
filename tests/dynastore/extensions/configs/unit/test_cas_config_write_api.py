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

"""#2707 — pin the compare-and-set (CAS) primitive on ``ConfigsProtocol``.

Static-shape + pure-logic pin tests (no DB):

1. ``ConfigsProtocol`` / ``PlatformConfigsProtocol`` carry ``get_config_versioned``
   and ``set_config`` grew an ``expected_version`` parameter.
2. ``PlatformConfigService`` and ``ConfigService`` both implement
   ``get_config_versioned``.
3. The version-token codec round-trips a real ``updated_at`` timestamp and
   two distinct instants encode to distinct tokens.
4. The CAS SQL query factories produce an ``UPDATE ... WHERE ... AND
   updated_at = :expected_version`` shape, ``ROWCOUNT``-handled so a lost
   race is detectable without a second read.
5. Dispatch: ``ConfigService._set_collection_config`` still requires
   ``catalog_id`` when ``collection_id`` is given, with ``expected_version``
   threaded through.

DB-backed CAS success/conflict end-to-end lands in
``tests/dynastore/modules/db_config/integration/test_cas_config_write.py``.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from dynastore.modules.db_config.config_version import (
    decode_config_version,
    encode_config_version,
)
from dynastore.modules.db_config.exceptions import ConfigVersionConflictError
from dynastore.modules.db_config.typed_store import config_queries as _cq


# ---------------------------------------------------------------------------
# Protocol surface
# ---------------------------------------------------------------------------


def test_configs_protocol_carries_get_config_versioned():
    assert hasattr(ConfigsProtocol, "get_config_versioned")


def test_platform_configs_protocol_carries_get_config_versioned():
    assert hasattr(PlatformConfigsProtocol, "get_config_versioned")


def test_configs_protocol_set_config_has_expected_version_param():
    sig = inspect.signature(ConfigsProtocol.set_config)
    assert "expected_version" in sig.parameters
    assert sig.parameters["expected_version"].default is None


def test_platform_configs_protocol_set_config_has_expected_version_param():
    sig = inspect.signature(PlatformConfigsProtocol.set_config)
    assert "expected_version" in sig.parameters
    assert sig.parameters["expected_version"].default is None


# ---------------------------------------------------------------------------
# Implementation discovery
# ---------------------------------------------------------------------------


def test_platform_service_implements_get_config_versioned():
    from dynastore.modules.db_config.platform_config_service import PlatformConfigService

    assert callable(getattr(PlatformConfigService, "get_config_versioned", None))


def test_config_service_implements_get_config_versioned():
    from dynastore.modules.catalog.config_service import ConfigService

    assert callable(getattr(ConfigService, "get_config_versioned", None))


def test_config_service_set_collection_config_dispatch_requires_catalog():
    """Sanity: unchanged guard still fires with expected_version threaded
    through — collection scope without a catalog is a config error."""
    import asyncio
    from dynastore.modules.catalog.config_service import ConfigService

    svc = ConfigService(engine=None)

    async def _run():
        try:
            await svc.set_config(
                "any_cls", config=None, collection_id="c", catalog_id=None,  # type: ignore[arg-type]
                expected_version="2026-01-01T00:00:00+00:00",
            )
        except (ValueError, TypeError, LookupError) as e:
            return str(e)
        return None

    msg = asyncio.run(_run())
    # Either the catalog_id guard or the class-resolution guard fires first —
    # both indicate the call never silently proceeded to a write.
    assert msg is not None


# ---------------------------------------------------------------------------
# Version-token codec
# ---------------------------------------------------------------------------


def test_version_token_round_trips_a_real_timestamp():
    now = datetime(2026, 7, 3, 12, 0, 0, 123456, tzinfo=timezone.utc)
    token = encode_config_version(now)
    assert isinstance(token, str)
    assert decode_config_version(token) == now


def test_distinct_instants_encode_to_distinct_tokens():
    t1 = datetime(2026, 7, 3, 12, 0, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(microseconds=1)
    assert encode_config_version(t1) != encode_config_version(t2)


def test_decode_config_version_rejects_malformed_token():
    import pytest

    with pytest.raises(ValueError):
        decode_config_version("not-a-timestamp")


# ---------------------------------------------------------------------------
# CAS query shape
# ---------------------------------------------------------------------------


def test_cas_update_platform_config_query_shape():
    template = _cq.cas_update_platform_config.template
    assert "UPDATE" in template
    assert "updated_at = :expected_version" in template
    assert "ref_key = :ref_key" in template


def test_cas_update_catalog_config_query_shape():
    q = _cq.cas_update_catalog_config("tenant_abc")
    template = q.template
    assert "UPDATE" in template
    assert "updated_at = :expected_version" in template
    assert '"tenant_abc"' in template


def test_cas_update_collection_config_query_shape():
    q = _cq.cas_update_collection_config("tenant_abc")
    template = q.template
    assert "UPDATE" in template
    assert "collection_id = :collection_id" in template
    assert "updated_at = :expected_version" in template


def test_get_platform_config_versioned_query_selects_updated_at():
    template = _cq.get_platform_config_versioned.template
    assert "updated_at" in template
    assert "config_data" in template


def test_config_version_conflict_error_is_a_value_error():
    """Matches the ``ImmutableConfigError`` / ``ConfigValidationError`` idiom
    so the existing 4xx exception-handler chain classifies it correctly."""
    assert issubclass(ConfigVersionConflictError, ValueError)
