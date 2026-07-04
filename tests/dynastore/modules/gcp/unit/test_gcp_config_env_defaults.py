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

"""Regression tests for the env-derived defaults on ``GcpModuleConfig`` and
``GcpCatalogBucketConfig`` (geoid#2830 C6).

These fields previously used ``Field(default=os.getenv(...))``, which is
evaluated exactly once at class-definition/import time — silently freezing
whatever the environment happened to be at that instant and masking the env
dependency from anyone reading the config. They now use
``Field(default_factory=lambda: os.getenv(...))``, which re-reads the
environment on every bare construction instead. This file pins:

- a sane literal fallback when the env var is unset;
- that the factory actually re-reads the environment per-instantiation (the
  regression this guards against: reintroducing a bare ``default=`` would
  make the second assertion in each test fail only when the env changes
  *after* the module was first imported, which is exactly the silent-freeze
  bug being fixed).
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py — pure
    in-memory introspection."""
    return None


def _gcp_config_module():
    from dynastore.modules.gcp import gcp_config
    return gcp_config


# ---------------------------------------------------------------------------
# GcpModuleConfig.project_id / region
# ---------------------------------------------------------------------------


def test_project_id_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("PROJECT_ID", raising=False)
    gcp_config = _gcp_config_module()

    cfg = gcp_config.GcpModuleConfig()
    assert cfg.project_id == "local-project"


def test_project_id_default_reads_env_per_instantiation(monkeypatch):
    gcp_config = _gcp_config_module()

    monkeypatch.setenv("PROJECT_ID", "fao-geoid-prod")
    cfg = gcp_config.GcpModuleConfig()
    assert cfg.project_id == "fao-geoid-prod"

    monkeypatch.setenv("PROJECT_ID", "fao-geoid-other")
    cfg2 = gcp_config.GcpModuleConfig()
    assert cfg2.project_id == "fao-geoid-other"


def test_region_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("REGION", raising=False)
    gcp_config = _gcp_config_module()

    cfg = gcp_config.GcpModuleConfig()
    assert cfg.region == "europe-west1"


def test_region_default_reads_env_per_instantiation(monkeypatch):
    gcp_config = _gcp_config_module()

    monkeypatch.setenv("REGION", "us-central1")
    cfg = gcp_config.GcpModuleConfig()
    assert cfg.region == "us-central1"

    monkeypatch.setenv("REGION", "europe-west3")
    cfg2 = gcp_config.GcpModuleConfig()
    assert cfg2.region == "europe-west3"


# ---------------------------------------------------------------------------
# GcpCatalogBucketConfig.location
# ---------------------------------------------------------------------------


def test_location_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("REGION", raising=False)
    gcp_config = _gcp_config_module()

    cfg = gcp_config.GcpCatalogBucketConfig()
    assert cfg.location == gcp_config.GcpLocation.EUROPE_WEST1


def test_location_default_reads_env_per_instantiation(monkeypatch):
    gcp_config = _gcp_config_module()

    monkeypatch.setenv("REGION", "europe-west3")
    cfg = gcp_config.GcpCatalogBucketConfig()
    assert cfg.location == gcp_config.GcpLocation.EUROPE_WEST3

    monkeypatch.setenv("REGION", "us-central1")
    cfg2 = gcp_config.GcpCatalogBucketConfig()
    assert cfg2.location == gcp_config.GcpLocation.US_CENTRAL1
