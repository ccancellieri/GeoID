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

"""Regression test for geoid#3132: ``DBConfig.__repr__`` must never leak a
raw connection URI, including the LISTEN/NOTIFY bridge's
``listen_database_url`` — a second, direct-DSN field alongside
``database_url`` on the same class. The DSN below is a fake placeholder.
"""
from __future__ import annotations


import pytest


@pytest.fixture()
def fresh_db_config(monkeypatch):
    """Reload db_config with a controlled DB_LISTEN_DATABASE_URL so the
    fake credential below is actually present on the instance under test."""
    import sys

    fake_listen_url = "postgresql://listener:FAKEPASS@listen-host:5432/gis"
    monkeypatch.setenv("DB_LISTEN_DATABASE_URL", fake_listen_url)
    monkeypatch.setenv("DATABASE_URL", "postgresql://svc:unused@db:5432/gis")

    for mod in list(sys.modules.keys()):
        if mod.endswith("db_config.db_config") or mod.endswith("db_config.instance"):
            del sys.modules[mod]

    from dynastore.modules.db_config.db_config import DBConfig

    yield DBConfig, fake_listen_url

    for mod in list(sys.modules.keys()):
        if mod.endswith("db_config.db_config") or mod.endswith("db_config.instance"):
            del sys.modules[mod]


def test_repr_masks_listen_database_url(fresh_db_config):
    DBConfig, fake_listen_url = fresh_db_config
    rendered = repr(DBConfig())
    assert "FAKEPASS" not in rendered
    assert "listener" not in rendered
    assert "listen_database_url='***'" in rendered


def test_str_falls_back_to_masked_repr(fresh_db_config):
    DBConfig, fake_listen_url = fresh_db_config
    rendered = str(DBConfig())
    assert "FAKEPASS" not in rendered
