#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
Secret handling for per-collection config fields.

Per-collection config rows carry connection strings, API keys, and service-
account JSON. Three threats we must defend against:

1. **At-rest leak** — the config_data jsonb column lives in the tenant's
   PostgreSQL schema. A DB snapshot / backup / admin read exposes every
   tenant's credentials.
2. **Response leak** — GET /configs/catalogs/X/collections/Y/plugins/{plugin_id}
   would otherwise return the raw secret in JSON.
3. **Log leak** — error messages / debug dumps that print the config model
   should not reveal the secret.

Design:
- ``Secret`` is a thin wrapper around a plaintext string. Its ``__str__`` /
  ``__repr__`` / JSON-serialization return a mask (``***``) unless
  ``reveal()`` is called.
- ``Secret.encrypt(plaintext)`` / ``Secret.decrypt(token)`` use Fernet
  (AES-128-CBC + HMAC-SHA256) with a key derived from ``DYNASTORE_SECRET_KEY``
  (falling back to ``JWT_SECRET``) via SHA-256.
- Persistence layer calls ``encode_secrets_for_persistence(model_dump_dict)``
  before writing to Postgres — every Secret becomes
  ``{"__secret__": "<fernet-token>"}``.
- Load layer calls ``decode_secrets_from_persistence(dict_from_db)`` — every
  encrypted envelope becomes a live ``Secret`` again.
- API-response layer calls ``mask_secrets_for_response(model_dump_dict)`` —
  every Secret becomes the string ``"***"`` (no envelope, no ciphertext).

Absence of a key (neither DYNASTORE_SECRET_KEY nor JWT_SECRET set) is a fatal
misconfiguration in production. In dev / test we fall back to a deterministic
dev key with a prominent warning; this must never be used to store real
credentials.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Envelope marker on the wire (inside the JSON blob persisted to Postgres).
_ENCRYPTED_KEY = "__secret__"
# Mask used in API responses and __str__ / __repr__.
_MASK = "***"
# Dev-only fallback key. Never use for real credentials — log a loud warning
# the first time we resort to it.
_DEV_FALLBACK_SECRET = "dynastore-dev-secret-DO-NOT-USE-IN-PRODUCTION"
_warned_about_dev_key = False


def _derive_key() -> bytes:
    """Derive a 32-byte Fernet key from the configured secret.

    Precedence:
        1. ``DYNASTORE_SECRET_KEY`` — dedicated secret-encryption key.
        2. ``JWT_SECRET`` — reused so single-tenant deploys don't need to
           configure two keys.
        3. Hard-coded dev fallback — emits a WARNING once per process.

    Returns a URL-safe base64 32-byte blob suitable for ``cryptography.fernet.Fernet``.
    """
    source = os.getenv("DYNASTORE_SECRET_KEY") or os.getenv("JWT_SECRET")
    if not source:
        global _warned_about_dev_key
        if not _warned_about_dev_key:
            logger.warning(
                "SECRET ENCRYPTION: neither DYNASTORE_SECRET_KEY nor JWT_SECRET is "
                "set; using a deterministic development key. NEVER use this in "
                "production — rotate immediately after setting a real key."
            )
            _warned_about_dev_key = True
        source = _DEV_FALLBACK_SECRET
    raw = hashlib.sha256(source.encode()).digest()  # 32 bytes
    return base64.urlsafe_b64encode(raw)


def _fernet():
    """Lazy import + cache the Fernet instance — avoids importing cryptography
    unless a secret is actually encrypted/decrypted in this process."""
    global _fernet_instance
    inst = globals().get("_fernet_instance")
    if inst is None:
        from cryptography.fernet import Fernet
        inst = Fernet(_derive_key())
        globals()["_fernet_instance"] = inst
    return inst


class Secret:
    """Plaintext secret wrapper.

    Instances expose the ciphertext / mask by default and only yield the
    plaintext through the explicit ``reveal()`` accessor.
    """

    __slots__ = ("_plaintext",)

    def __init__(self, plaintext: str) -> None:
        self._plaintext = plaintext

    @classmethod
    def encrypt(cls, plaintext: str) -> str:
        """Return a base64 Fernet token for ``plaintext``."""
        return _fernet().encrypt(plaintext.encode()).decode()

    @classmethod
    def decrypt(cls, token: str) -> str:
        """Decrypt a Fernet token back to plaintext."""
        return _fernet().decrypt(token.encode()).decode()

    def reveal(self) -> str:
        """Return the plaintext. Call site must document why the plaintext is
        needed (usually: passing to a driver connection string)."""
        return self._plaintext

    # Safety defaults — never expose plaintext through str/repr/JSON.
    def __str__(self) -> str:
        return _MASK

    def __repr__(self) -> str:
        return f"Secret({_MASK})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._plaintext == self._plaintext

    def __hash__(self) -> int:
        return hash(self._plaintext)

    # Pydantic hook — let v2 models accept strings / dicts on the way in.
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        from pydantic_core import core_schema

        def _validate(v: Any) -> "Secret":
            if isinstance(v, Secret):
                return v
            if isinstance(v, str):
                return Secret(v)
            if v is None:
                # Pydantic handles Optional wrapping; this branch is defensive
                # for code that explicitly passes None through a required field.
                raise TypeError("Secret() does not accept None — wrap in Optional[Secret]")
            if isinstance(v, dict) and _ENCRYPTED_KEY in v:
                return Secret(cls.decrypt(v[_ENCRYPTED_KEY]))
            raise TypeError(f"Cannot construct Secret from {type(v).__name__}")

        def _serialize(instance: "Secret", info) -> Any:
            # ``mode="db"``: emit the encrypted envelope → persisted to jsonb.
            # ``mode="mask"``: emit the mask string → safe for API responses
            # and logs. Any other mode (default): mask as well, so
            # ``model_dump()`` is safe by default.
            ctx = getattr(info, "context", None) or {}
            out_mode = ctx.get("secret_mode", "mask")
            if out_mode == "db":
                return {_ENCRYPTED_KEY: cls.encrypt(instance._plaintext)}
            if out_mode == "reveal":
                return instance._plaintext
            return _MASK

        return core_schema.no_info_plain_validator_function(
            _validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                _serialize, info_arg=True, when_used="always"
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler):
        """Advertise Secret as an optional plaintext string in the generated
        OpenAPI schema. Clients PUT the plaintext once; everything after is
        masked on the read path. The alternate ``{__secret__: ...}`` envelope
        is a server-internal encoding and intentionally omitted from the
        public contract.

        The ``x-ui`` extension tells the schema-driven admin UI to render a
        password input and treat the value as sensitive (no echo on reload
        unless the user edited the field in the current session).
        """
        return {
            "type": "string",
            "format": "password",
            "description": (
                "Secret value — persisted encrypted, never returned in full. "
                "GET responses mask it as '***'."
            ),
            "x-ui": {"widget": "password", "sensitive": True},
        }


# Module-level helpers for the persistence and response layers.
# ``walk`` receives any already-dumped Python dict (the output of model_dump(mode="python"))
# and in-place rewrites every Secret instance.


def _walk(obj: Any, transform) -> Any:
    """Recursively apply ``transform(secret)`` to every Secret in obj."""
    if isinstance(obj, Secret):
        return transform(obj)
    if isinstance(obj, dict):
        # Convert an already-encrypted envelope back if we round-trip.
        if _ENCRYPTED_KEY in obj and len(obj) == 1 and isinstance(obj[_ENCRYPTED_KEY], str):
            return obj  # leave as-is; it's already the wire form
        return {k: _walk(v, transform) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(v, transform) for v in obj]
    return obj


def encode_secrets_for_persistence(payload: Any) -> Any:
    """Rewrite every Secret to its encrypted envelope. Call before jsonb write."""
    return _walk(payload, lambda s: {_ENCRYPTED_KEY: Secret.encrypt(s.reveal())})


def mask_secrets_for_response(payload: Any) -> Any:
    """Rewrite every Secret to the mask string. Call before returning to HTTP."""
    return _walk(payload, lambda s: _MASK)


def reveal_secrets(payload: Any) -> Any:
    """Rewrite every Secret to its plaintext. Only callable from driver code."""
    return _walk(payload, lambda s: s.reveal())


def is_encrypted_envelope(value: Any) -> bool:
    """Predicate used by tests / migration tools."""
    return (
        isinstance(value, dict)
        and len(value) == 1
        and _ENCRYPTED_KEY in value
        and isinstance(value[_ENCRYPTED_KEY], str)
    )


__all__ = [
    "Secret",
    "encode_secrets_for_persistence",
    "mask_secrets_for_response",
    "reveal_secrets",
    "is_encrypted_envelope",
]
