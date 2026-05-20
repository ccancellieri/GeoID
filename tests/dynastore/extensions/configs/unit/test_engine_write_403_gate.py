"""Cycle F.4b — pin the tenant-engines write 403 gate.

Engines are sysadmin-only platform-tier resources (decisions #15 / #18
in the F plan).  The existing ``configs_access`` policy already gates
the entire ``/configs/.*`` surface to SYSADMIN; F.4b adds defence-in-
depth at the routing layer so engine writes at catalog / collection
scope return a clean 403 with a clear message even if the policy is
misconfigured.

These tests pin the gate at:
- ``ConfigsService._reject_engine_write_at_tenant_scope`` (private
  helper used by per-plugin PUT/DELETE)
- ``ConfigsService._gate_engine_writes_in_patch_body`` (private
  helper used by RFC 7396 merge-patch)
- ``problem_details.engine_write_forbidden_at_tenant_scope``
  constructor
"""
from __future__ import annotations

import pytest

from dynastore.extensions.configs import problem_details
from dynastore.extensions.configs.problem_details import (
    ProblemException,
    engine_write_forbidden_at_tenant_scope,
)
from dynastore.extensions.configs.service import ConfigsService
from dynastore.modules.catalog.catalog_config import CollectionInfo
from dynastore.modules.db_config.engine_config import (
    DuckdbEngineConfig,
    ElasticsearchEngineConfig,
    EngineConfig,
    IcebergEngineConfig,
    PostgresqlEngineConfig,
)


# ---------------------------------------------------------------------------
# Problem-details constructor
# ---------------------------------------------------------------------------


def test_engine_write_forbidden_constructor_returns_403():
    exc = engine_write_forbidden_at_tenant_scope(
        "postgresql_engine_config", scope="catalog",
    )
    assert isinstance(exc, ProblemException)
    assert exc.problem.status == 403
    assert "postgresql_engine_config" in exc.problem.detail
    assert "catalog" in exc.problem.detail
    assert "sysadmin" in exc.problem.detail.lower()


def test_engine_write_forbidden_constructor_collection_scope():
    exc = engine_write_forbidden_at_tenant_scope(
        "elasticsearch_engine_config", scope="collection",
    )
    assert exc.problem.status == 403
    assert "elasticsearch_engine_config" in exc.problem.detail
    assert "collection" in exc.problem.detail


# ---------------------------------------------------------------------------
# _reject_engine_write_at_tenant_scope (per-plugin gate)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "engine_cls",
    [
        PostgresqlEngineConfig,
        ElasticsearchEngineConfig,
        DuckdbEngineConfig,
        IcebergEngineConfig,
    ],
)
def test_reject_engine_write_raises_403_for_each_engine_kind(engine_cls):
    """All four F.1 engine kinds must trigger the 403 gate at tenant
    scope."""
    plugin_id = engine_cls.class_key()
    with pytest.raises(ProblemException) as ei:
        ConfigsService._reject_engine_write_at_tenant_scope(
            engine_cls, plugin_id, scope="catalog",
        )
    assert ei.value.problem.status == 403
    assert plugin_id in ei.value.problem.detail


def test_reject_engine_write_passes_for_non_engine_class():
    """Non-engine classes (regular PluginConfig subclasses) must pass
    through the gate without raising."""
    # CollectionInfo is a non-engine PluginConfig.
    ConfigsService._reject_engine_write_at_tenant_scope(
        CollectionInfo, CollectionInfo.class_key(), scope="catalog",
    )  # No exception.


def test_reject_engine_write_passes_for_collection_scope_non_engine():
    # ``CollectionInfo`` is a collection-tier non-engine PluginConfig
    # (replaces the deleted CollectionPrivacy in this assertion shape).
    ConfigsService._reject_engine_write_at_tenant_scope(
        CollectionInfo, "collection_info", scope="collection",
    )  # No exception.


def test_reject_engine_write_with_abstract_engineconfig_base():
    """The abstract ``EngineConfig`` base is not directly registered as
    a concrete plugin, but the gate still rejects it via issubclass —
    consistent with the ``EngineConfig`` hierarchy contract."""
    with pytest.raises(ProblemException) as ei:
        ConfigsService._reject_engine_write_at_tenant_scope(
            EngineConfig, "engine_config", scope="catalog",
        )
    assert ei.value.problem.status == 403


# ---------------------------------------------------------------------------
# _gate_engine_writes_in_patch_body (bulk merge-patch gate)
# ---------------------------------------------------------------------------


def test_gate_patch_body_raises_for_engine_key():
    """A merge-patch body containing an engine class_key must raise
    403 BEFORE patch_config fires (atomic — no partial writes)."""
    body = {
        "postgresql_engine_config": {"pool_size": 20},
        "catalog_routing_templates": {"collection_defaults": {}},
    }
    with pytest.raises(ProblemException) as ei:
        ConfigsService._gate_engine_writes_in_patch_body(body, scope="catalog")
    assert ei.value.problem.status == 403
    assert "postgresql_engine_config" in ei.value.problem.detail


def test_gate_patch_body_passes_for_non_engine_only_body():
    """A merge-patch body with only non-engine plugins must pass the
    gate (no exception)."""
    body = {
        "catalog_routing_templates": {"collection_defaults": {}},
    }
    ConfigsService._gate_engine_writes_in_patch_body(body, scope="catalog")
    # No exception.


def test_gate_patch_body_skips_unknown_plugin_ids():
    """Unknown plugin_ids in the body are NOT rejected by the engine
    gate — patch_config will raise its own ValueError downstream.
    The gate only knows engines; it stays silent on other validation."""
    body = {"completely_unknown_plugin": {"foo": "bar"}}
    ConfigsService._gate_engine_writes_in_patch_body(body, scope="collection")
    # No exception from the engine gate; downstream patch_config raises ValueError.


def test_gate_patch_body_engine_in_collection_scope():
    body = {"duckdb_engine_config": {"pool_size": 8}}
    with pytest.raises(ProblemException) as ei:
        ConfigsService._gate_engine_writes_in_patch_body(body, scope="collection")
    assert ei.value.problem.status == 403
    assert "collection" in ei.value.problem.detail


def test_gate_patch_body_engine_with_null_value_still_rejected():
    """RFC 7396 ``null`` values delete the override.  Tenant-scope
    delete of an engine is also rejected by the gate (the plan calls
    for sysadmin-only writes — delete is a write)."""
    body = {"iceberg_engine_config": None}
    with pytest.raises(ProblemException) as ei:
        ConfigsService._gate_engine_writes_in_patch_body(body, scope="catalog")
    assert ei.value.problem.status == 403


# ---------------------------------------------------------------------------
# problem_details module export
# ---------------------------------------------------------------------------


def test_problem_details_module_exposes_constructor():
    """The constructor must be reachable via the module surface so
    other extensions can raise the same shape if needed."""
    assert hasattr(problem_details, "engine_write_forbidden_at_tenant_scope")
    assert callable(problem_details.engine_write_forbidden_at_tenant_scope)
