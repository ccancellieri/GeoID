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

"""Unit tests for the db_config hardcoded-fallback gate.

Verifies:
- RuntimeError raised when DATABASE_URL is absent and no db_config.json
  provides it, without DYNASTORE_ALLOW_DEV_SECRET=1.
- Dev fallback is allowed when DYNASTORE_ALLOW_DEV_SECRET=1.
- An explicit DATABASE_URL env var always works without the flag.
- A db_config.json-provided DATABASE_URL always works without the flag.
"""
from __future__ import annotations

import os
import pytest


def _resolve(env: dict, file_values: dict) -> str:
    """Call _resolve_database_url with controlled environment and file_values."""
    # Patch os.getenv and _FILE_VALUES within db_config module
    import sys

    # Remove cached module to get fresh state
    for mod in list(sys.modules.keys()):
        if "db_config.db_config" in mod or mod.endswith(".db_config"):
            del sys.modules[mod]

    # Set env
    original = {}
    for k in ("DATABASE_URL", "DYNASTORE_ALLOW_DEV_SECRET"):
        original[k] = os.environ.get(k)
        if k in env:
            os.environ[k] = env[k]
        elif k in os.environ:
            del os.environ[k]

    try:
        # Patch instance.load_db_config to return controlled file_values
        import dynastore.modules.db_config.instance as inst_mod
        orig_load = inst_mod.load_db_config
        inst_mod.load_db_config = lambda: file_values

        from dynastore.modules.db_config.db_config import _resolve_database_url
        return _resolve_database_url()
    finally:
        inst_mod.load_db_config = orig_load  # type: ignore[assignment]
        for k, v in original.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        # Flush cached module again
        for mod in list(sys.modules.keys()):
            if "db_config.db_config" in mod:
                del sys.modules[mod]


def test_hardcoded_fallback_raises_without_flag():
    """No DATABASE_URL, no db_config.json, no allow flag → RuntimeError."""
    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        _resolve(env={}, file_values={})


def test_hardcoded_fallback_allowed_with_flag():
    """No DATABASE_URL, no db_config.json, but allow flag=1 → dev URL returned."""
    url = _resolve(
        env={"DYNASTORE_ALLOW_DEV_SECRET": "1"},
        file_values={},
    )
    assert "testuser" in url  # hardcoded dev URL contains testuser
    assert "gis_dev" in url


def test_env_database_url_works_without_flag():
    """Explicit DATABASE_URL env var is accepted without the allow flag."""
    url = _resolve(
        env={"DATABASE_URL": "postgresql://prod:secret@prod-db:5432/prod"},
        file_values={},
    )
    assert url == "postgresql://prod:secret@prod-db:5432/prod"


def test_file_database_url_works_without_flag():
    """DATABASE_URL from db_config.json is accepted without the allow flag."""
    url = _resolve(
        env={},
        file_values={"DATABASE_URL": "postgresql://file-user:pass@db:5432/app"},
    )
    assert url == "postgresql://file-user:pass@db:5432/app"


def test_env_wins_over_file():
    """Env var DATABASE_URL takes precedence over db_config.json value."""
    url = _resolve(
        env={"DATABASE_URL": "postgresql://env-wins:pw@host/db"},
        file_values={"DATABASE_URL": "postgresql://file-loses:pw@host/db"},
    )
    assert url == "postgresql://env-wins:pw@host/db"
