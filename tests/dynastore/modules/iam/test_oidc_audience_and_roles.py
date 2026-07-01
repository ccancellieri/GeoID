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

"""Unit tests for OIDC audience validation + configurable roles claim path.

These tests pin two operator-facing guarantees of
:class:`OidcIdentityProvider`:

1. ``aud`` is enforced by PyJWT against ``self.audience`` — both for the
   single-string and array forms — and a mismatch raises
   :class:`InvalidAudienceError`.
2. The ``IDP_ROLES_CLAIM_PATH`` configuration (stored as
   ``self.roles_claim_path``) drives :meth:`extract_roles` deterministically
   and never silently merges other paths.

Tests use HS256 with a per-test secret and stub the JWKS client so we don't
need a live IdP (and don't pull the session-level DB fixture in conftest).
"""

from typing import Any, Dict

import jwt
import pytest
from jwt.exceptions import InvalidAudienceError

from dynastore.modules.iam.identity_providers.oidc_identity import OidcIdentityProvider


_HS256_SECRET = "test-secret-do-not-use-in-prod-padded-to-32+bytes"
_ISSUER = "https://idp.example.test/realms/test"


class _StubSigningKey:
    """Minimal stand-in for ``jwt.PyJWK``. Only the ``key`` attribute is used."""

    def __init__(self, secret: str):
        self.key = secret


class _StubJwksClient:
    """Stub PyJWKClient that ignores the JWT header and returns the HS256 secret."""

    def __init__(self, secret: str):
        self._secret = secret

    def get_signing_key_from_jwt(self, _token: str) -> _StubSigningKey:
        return _StubSigningKey(self._secret)


def _build_provider(
    *,
    audience: str = "geoid-be",
    client_id: str = "geoid-fe",
    roles_claim_path: str | None = None,
) -> OidcIdentityProvider:
    """Build a provider with JWKS + discovery already short-circuited."""
    provider = OidcIdentityProvider(
        issuer_url=_ISSUER,
        client_id=client_id,
        audience=audience,
        roles_claim_path=roles_claim_path,
    )
    # Pretend OIDC discovery has run so ``_decode_token`` skips the network
    # call. Marking ``_meta`` non-None and setting a fresh fetch timestamp
    # bypasses ``_ensure_meta``'s refresh path.
    from datetime import datetime, timezone
    provider._meta = {"jwks_uri": "stub"}
    provider._meta_fetched_at = datetime.now(timezone.utc)
    provider._jwks_client = _StubJwksClient(_HS256_SECRET)  # type: ignore[assignment]
    return provider


def _encode(payload: Dict[str, Any]) -> str:
    """Encode an HS256 token. ``iss`` defaults to the test issuer."""
    payload = dict(payload)
    payload.setdefault("iss", _ISSUER)
    return jwt.encode(payload, _HS256_SECRET, algorithm="HS256")


def _patch_algorithms(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``_decode_token`` to accept HS256 (the production allow-list is RS/ES only)."""
    real_decode = jwt.decode

    def _decode_hs256(token, key=None, algorithms=None, **kwargs):  # type: ignore[no-untyped-def]
        return real_decode(token, key=key, algorithms=["HS256"], **kwargs)

    monkeypatch.setattr(
        "dynastore.modules.iam.identity_providers.oidc_identity.jwt.decode",
        _decode_hs256,
    )


# ---------------------------------------------------------------------------
# Audience validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aud_mismatch_raises_invalid_audience(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_algorithms(monkeypatch)
    provider = _build_provider(audience="geoid-be")
    token = _encode({"sub": "alice", "aud": "something-else"})

    with pytest.raises(InvalidAudienceError):
        await provider._decode_token(token)


@pytest.mark.asyncio
async def test_aud_match_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_algorithms(monkeypatch)
    provider = _build_provider(audience="geoid-be")
    token = _encode({"sub": "alice", "aud": "geoid-be"})

    claims = await provider._decode_token(token)
    assert claims["sub"] == "alice"
    assert claims["aud"] == "geoid-be"


@pytest.mark.asyncio
async def test_aud_array_with_match_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_algorithms(monkeypatch)
    provider = _build_provider(audience="geoid-be")
    token = _encode({"sub": "alice", "aud": ["geoid-fe", "geoid-be"]})

    claims = await provider._decode_token(token)
    assert claims["sub"] == "alice"
    assert "geoid-be" in claims["aud"]


# ---------------------------------------------------------------------------
# Roles claim path
# ---------------------------------------------------------------------------


def test_extract_roles_default_path() -> None:
    provider = _build_provider(audience="geoid-be")
    # Default path resolves to ``realm_access.roles`` (Keycloak's standard
    # realm-role location), matching IdpConfig.roles_claim_path's default.
    claims = {"realm_access": {"roles": ["sysadmin"]}}

    assert provider.extract_roles(claims) == ["sysadmin"]


def test_extract_roles_account_path() -> None:
    provider = _build_provider(
        audience="geoid-be",
        roles_claim_path="resource_access.account.roles",
    )
    claims = {"resource_access": {"account": {"roles": ["sysadmin"]}}}

    assert provider.extract_roles(claims) == ["sysadmin"]


def test_extract_roles_realm_path() -> None:
    provider = _build_provider(
        audience="geoid-be",
        roles_claim_path="realm_access.roles",
    )
    claims = {"realm_access": {"roles": ["sysadmin"]}}

    assert provider.extract_roles(claims) == ["sysadmin"]


def test_extract_roles_no_silent_merge() -> None:
    """Default path (``realm_access.roles``) must NOT pull resource_access
    roles when realm_access has its own list — no silent cross-claim merge."""
    provider = _build_provider(audience="geoid-be")
    claims = {
        "resource_access": {"geoid-be": {"roles": ["other"]}},
        "realm_access": {"roles": ["sysadmin"]},
    }

    roles = provider.extract_roles(claims)
    assert roles == ["sysadmin"]
    assert "other" not in roles


def test_extract_roles_missing_path_returns_empty() -> None:
    provider = _build_provider(audience="geoid-be")
    # Audience client absent from resource_access — must yield [].
    claims = {"resource_access": {"some-other-client": {"roles": ["x"]}}}

    assert provider.extract_roles(claims) == []


def test_extract_roles_template_substitution_at_construction() -> None:
    """``${audience}`` in the path is substituted at construction time."""
    provider = _build_provider(
        audience="geoid-be",
        roles_claim_path="resource_access.${audience}.roles",
    )
    assert provider.roles_claim_path == "resource_access.geoid-be.roles"
    claims = {"resource_access": {"geoid-be": {"roles": ["sysadmin"]}}}
    assert provider.extract_roles(claims) == ["sysadmin"]
