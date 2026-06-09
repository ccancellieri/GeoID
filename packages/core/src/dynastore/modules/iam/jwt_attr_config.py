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

"""Platform-scope config for enriching Principal.attributes from validated JWT claims.

:class:`JwtAttributeClaimsConfig` is a :class:`~dynastore.models.plugin_config.PluginConfig`
that maps Principal attribute keys to JWT claim names.  When populated, the
IAM service merges those claim values into ``Principal.attributes`` after
principal resolution, for every authenticated request.

Trust-boundary rationale (in-code contract):
  1. Opt-in per key — the operator must explicitly map every claim key;
     unmapped claims are silently ignored.
  2. DB-row attributes WIN on key collision — ``principals.attributes`` is the
     ground truth; JWT claims only fill keys that are absent there.
  3. Key validation — both sides of ``claim_map`` (attr key) must match
     ``[A-Za-z_][A-Za-z0-9_]*``, the same regex enforced by
     ``AttributePredicate.key``, to keep JSONB paths injection-free.
  4. Claim paths are limited to top-level JWT fields or ``$.``-prefixed
     shorthands; nested selectors and array indexing are not supported in
     this slice.
"""
from __future__ import annotations

import re
from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import Field, field_validator

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

__all__ = ["JwtAttributeClaimsConfig"]

# Reuse the same regex constraint as AttributePredicate.key — no duplication.
_KEY_PATTERN = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")


class JwtAttributeClaimsConfig(PluginConfig):
    """Platform-scope config for JWT-claim → Principal.attributes enrichment.

    ``claim_map`` is a ``{attr_key: claim_path}`` dict.  On every authenticated
    request the IAM service iterates the map, extracts the claim value from the
    validated token, and merges it into ``Principal.attributes`` for any key not
    already present (DB attributes win on collision).

    Trust-boundary guarantees (enforce in this order):
      1. Opt-in per key — only explicitly mapped claims are extracted.
      2. DB wins on collision — a claim never overrides a DB-managed attribute.
      3. Key format — ``attr_key`` must match ``[A-Za-z_][A-Za-z0-9_]*`` so
         it is safe as an unquoted JSONB path component.
      4. Claim-path scope — only top-level claim names and ``$.``-prefixed
         shorthands (e.g. ``$.dept``) are resolved; no nested or array paths.

    A misconfigured Keycloak realm could widen access by injecting unexpected
    values into ``_attrs``; mitigations 1–4 above bound the blast radius.

    Example::

        PUT /configs/plugins/jwt_attribute_claims
        {"claim_map": {"dept": "dept", "region": "$.region"}}
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "modules",
        "iam",
        "jwt_attribute_claims",
    )

    claim_map: Mutable[Dict[str, str]] = Field(
        default_factory=dict,
        description=(
            "Map of Principal attribute key → JWT claim name or ``$.``-prefixed "
            "path.  Only top-level claims are supported in this slice. "
            "Keys must match ``[A-Za-z_][A-Za-z0-9_]*``.  "
            "An empty dict (default) disables JWT attribute enrichment."
        ),
    )

    issuer_allowlist: Mutable[Optional[List[str]]] = Field(
        default=None,
        description=(
            "Restrict JWT-claim attribute enrichment to tokens whose ``iss`` "
            "claim is in this list.  ``None`` (default) accepts all verified "
            "issuers — safe for single-IdP deployments.  In multi-provider "
            "setups, set this to prevent a partner IdP's ``dept`` (or any other "
            "mapped claim) from silently widening or restricting ABAC access. "
            "Mirrors ``OidcRoleSyncConfig.issuer_whitelist``."
        ),
    )

    @field_validator("claim_map")
    @classmethod
    def _validate_claim_map_keys(cls, value: Dict[str, str]) -> Dict[str, str]:
        """Reject any attr_key that fails the safe-identifier pattern."""
        for key in value:
            if not _KEY_PATTERN.fullmatch(key):
                raise ValueError(
                    f"JwtAttributeClaimsConfig.claim_map key {key!r} must match "
                    "[A-Za-z_][A-Za-z0-9_]* (no quotes, dots, or whitespace)."
                )
        return value


def _resolve_claim_value(raw_claims: Dict[str, object], claim_path: str) -> Optional[str]:
    """Extract a top-level claim value from *raw_claims*.

    Supports:
      - bare name: ``"dept"`` → ``raw_claims["dept"]``
      - ``$.``-prefixed shorthand: ``"$.dept"`` → ``raw_claims["dept"]``

    Returns a non-empty string or ``None`` (missing / None / empty / non-scalar).
    Nested paths and array selectors are not supported; they return ``None``.
    """
    key = claim_path.lstrip("$.")  # strip leading ``$.`` or ``.``
    # Reject anything that looks like a nested path (contains a dot after strip).
    if "." in key or "[" in key:
        return None
    raw = raw_claims.get(key)
    if raw is None:
        return None
    if not isinstance(raw, (str, int, float, bool)):
        # Non-scalar claim (list, dict, …) — coercing to str would produce
        # an undebuggable "['finance']"-style string that no predicate matches.
        return None
    value = str(raw).strip()
    return value if value else None
