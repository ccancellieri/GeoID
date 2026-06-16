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

"""Unit tests for secrets.py dev-key fail-fast gate.

Verifies:
- RuntimeError raised when neither secret key env var is set and
  DYNASTORE_ALLOW_DEV_SECRET is not '1'.
- Dev fallback is allowed when DYNASTORE_ALLOW_DEV_SECRET=1.
- A real DYNASTORE_SECRET_KEY is always used without requiring the flag.
"""
from __future__ import annotations

import importlib
import sys

import pytest


def _reload_secrets_module() -> object:
    """Force-reload secrets so module-level state (_warned_about_dev_key,
    _fernet_instance) is reset between tests."""
    mod_name = "dynastore.tools.secrets"
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    return importlib.import_module(mod_name)


def test_derive_key_raises_without_flag(monkeypatch):
    """Without DYNASTORE_ALLOW_DEV_SECRET=1 and no secret key, must raise."""
    monkeypatch.delenv("DYNASTORE_SECRET_KEY", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("SESSION_SECRET_KEY", raising=False)
    monkeypatch.delenv("DYNASTORE_ALLOW_DEV_SECRET", raising=False)

    secrets = _reload_secrets_module()
    with pytest.raises(RuntimeError, match="DYNASTORE_SECRET_KEY"):
        secrets._derive_key()


def test_derive_key_allowed_with_flag(monkeypatch):
    """With DYNASTORE_ALLOW_DEV_SECRET=1 and no secret key, uses fallback key."""
    monkeypatch.delenv("DYNASTORE_SECRET_KEY", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.setenv("DYNASTORE_ALLOW_DEV_SECRET", "1")

    secrets = _reload_secrets_module()
    key = secrets._derive_key()
    # Must be a non-empty bytes value (base64-encoded 32-byte key)
    assert isinstance(key, bytes)
    assert len(key) == 44  # base64(32 bytes) = 44 chars


def test_derive_key_uses_real_secret_key(monkeypatch):
    """DYNASTORE_SECRET_KEY is used regardless of the allow flag."""
    monkeypatch.setenv("DYNASTORE_SECRET_KEY", "my-super-secret-key-for-testing")
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("DYNASTORE_ALLOW_DEV_SECRET", raising=False)

    secrets = _reload_secrets_module()
    key = secrets._derive_key()
    assert isinstance(key, bytes)
    assert len(key) == 44


def test_derive_key_uses_jwt_secret_fallback(monkeypatch):
    """JWT_SECRET is used when DYNASTORE_SECRET_KEY is absent."""
    monkeypatch.delenv("DYNASTORE_SECRET_KEY", raising=False)
    monkeypatch.setenv("JWT_SECRET", "jwt-secret-for-testing")
    monkeypatch.delenv("DYNASTORE_ALLOW_DEV_SECRET", raising=False)

    secrets = _reload_secrets_module()
    key = secrets._derive_key()
    assert isinstance(key, bytes)
    assert len(key) == 44


def test_derive_key_uses_session_secret_fallback(monkeypatch):
    """SESSION_SECRET_KEY is used when neither DYNASTORE_SECRET_KEY nor
    JWT_SECRET is set — it is the key source every deployment tier already
    provisions (it signs the Starlette session cookie), so a working install
    needs no separate secret to encrypt config credentials. Regression for
    #2210: dev/review provision only SESSION_SECRET_KEY, so without this the
    IdpConfig seed crashes while encrypting client_secret and no IdP registers.
    """
    monkeypatch.delenv("DYNASTORE_SECRET_KEY", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.setenv("SESSION_SECRET_KEY", "session-secret-for-testing")
    monkeypatch.delenv("DYNASTORE_ALLOW_DEV_SECRET", raising=False)

    secrets = _reload_secrets_module()
    key = secrets._derive_key()
    assert isinstance(key, bytes)
    assert len(key) == 44


def test_dedicated_key_precedence_over_session_secret(monkeypatch):
    """DYNASTORE_SECRET_KEY wins over SESSION_SECRET_KEY so the dedicated,
    independently-rotatable key always takes precedence over the shared one."""
    monkeypatch.setenv("DYNASTORE_SECRET_KEY", "dedicated-key")
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.setenv("SESSION_SECRET_KEY", "session-key")
    monkeypatch.delenv("DYNASTORE_ALLOW_DEV_SECRET", raising=False)

    import base64
    import hashlib

    secrets = _reload_secrets_module()
    expected = base64.urlsafe_b64encode(hashlib.sha256(b"dedicated-key").digest())
    assert secrets._derive_key() == expected


def test_flag_value_must_be_exactly_one(monkeypatch):
    """DYNASTORE_ALLOW_DEV_SECRET='true' is NOT sufficient — only '1'."""
    monkeypatch.delenv("DYNASTORE_SECRET_KEY", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("SESSION_SECRET_KEY", raising=False)
    monkeypatch.setenv("DYNASTORE_ALLOW_DEV_SECRET", "true")

    secrets = _reload_secrets_module()
    with pytest.raises(RuntimeError, match="DYNASTORE_SECRET_KEY"):
        secrets._derive_key()
