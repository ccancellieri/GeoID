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

"""Unit tests for ``modules/db_config/instance.py`` — per-instance config.

The dispatcher relies on this to learn its own ``service_name`` for
service-affinity routing. We need the loader to be tolerant: a missing or
malformed file must NOT crash the service — it returns ``{}`` and the
dispatcher falls back to legacy behaviour.

Tests patch ``INSTANCE_FILE`` directly rather than reloading the module —
``importlib.reload`` mutates module-level state and can leak across
xdist-parallel test workers.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from dynastore.modules.db_config import instance as inst


def test_missing_instance_file_returns_empty(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(inst, "INSTANCE_FILE", tmp_path / "no-such.json")
    caplog.set_level("WARNING")
    assert inst.load_instance() == {}
    assert any("instance config missing" in r.message for r in caplog.records)


def test_malformed_json_returns_empty(monkeypatch, tmp_path, caplog):
    bad = tmp_path / "instance.json"
    bad.write_text("{not json")
    monkeypatch.setattr(inst, "INSTANCE_FILE", bad)
    caplog.set_level("WARNING")
    assert inst.load_instance() == {}
    assert any("unreadable" in r.message for r in caplog.records)


def test_well_formed_returns_dict(monkeypatch, tmp_path):
    f = tmp_path / "instance.json"
    f.write_text(json.dumps({"service_name": "catalog", "extra": 42}))
    monkeypatch.setattr(inst, "INSTANCE_FILE", f)
    assert inst.load_instance() == {"service_name": "catalog", "extra": 42}
    assert inst.get_service_name() == "catalog"


def test_get_service_name_none_when_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(inst, "INSTANCE_FILE", tmp_path / "absent.json")
    assert inst.get_service_name() is None


def test_resolve_root_uses_explicit_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DYNASTORE_CONFIG_ROOT", str(tmp_path))
    assert inst._resolve_root() == pathlib.Path(str(tmp_path))


def test_resolve_root_default_uses_app_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("DYNASTORE_CONFIG_ROOT", raising=False)
    monkeypatch.setenv("APP_DIR", str(tmp_path))
    assert inst._resolve_root() == pathlib.Path(str(tmp_path)) / "config"


def test_instance_id_is_stable_within_process():
    assert inst.get_instance_id() == inst.get_instance_id()


def test_stamped_application_name_uses_service_name(monkeypatch, tmp_path):
    f = tmp_path / "instance.json"
    f.write_text(json.dumps({"service_name": "catalog-api"}))
    monkeypatch.setattr(inst, "INSTANCE_FILE", f)
    stamped = inst.get_stamped_application_name()
    service, _, instance_id = stamped.rpartition(":")
    assert service == "catalog-api"
    assert instance_id == inst.get_instance_id()


def test_stamped_application_name_falls_back_to_service_name_env(monkeypatch, tmp_path):
    monkeypatch.setattr(inst, "INSTANCE_FILE", tmp_path / "absent.json")
    monkeypatch.setenv("SERVICE_NAME", "env-service")
    stamped = inst.get_stamped_application_name()
    assert stamped.startswith("env-service:")


def test_stamped_application_name_falls_back_to_dynastore(monkeypatch, tmp_path):
    monkeypatch.setattr(inst, "INSTANCE_FILE", tmp_path / "absent.json")
    monkeypatch.delenv("SERVICE_NAME", raising=False)
    stamped = inst.get_stamped_application_name()
    assert stamped.startswith("dynastore:")


def test_stamped_application_name_truncates_long_service_name(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(inst, "_warned_application_name_truncated", False)
    long_service = "a" * 100
    f = tmp_path / "instance.json"
    f.write_text(json.dumps({"service_name": long_service}))
    monkeypatch.setattr(inst, "INSTANCE_FILE", f)

    caplog.set_level("WARNING")
    stamped = inst.get_stamped_application_name()

    assert len(stamped.encode("utf-8")) <= 63
    assert stamped.endswith(f":{inst.get_instance_id()}")
    assert any("exceeding" in r.message for r in caplog.records)
