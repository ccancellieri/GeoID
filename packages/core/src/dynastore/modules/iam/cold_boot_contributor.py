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

"""IAM cold-boot contributor — authoritative boot sequence for IAM presets.

:class:`IamColdBootContributor` encapsulates ALL cold-boot logic that was
previously inlined in ``IamModule.lifespan``.  It runs at ``priority=100``
(the highest priority, ahead of web at 50 and auth at 40) so foundational
auth roles and IdP config are always seeded before any surface that needs
them can serve requests.

Execution order within ``run``:

1. Seed ``OidcRoleSyncConfig`` (idempotent one-shot; operator PATCHes preserved).
2. Apply ``auth_bootstrap`` preset (idempotent IdpConfig seed from IDP_* ENV).
3. Warn when JWT-claim attribute enrichment has no issuer allowlist.
4. Apply ``default_roles_baseline`` (force=True, self-healing).
5. Apply ``iam_baseline`` (force=True, self-healing).
6. Apply ``public_access_baseline`` LAST (force=True; gates /health probe).
7. Re-register the OIDC identity provider as a self-heal, in case step 2
   just seeded a fresh IdpConfig row this process had not seen yet.

Each step runs in its own ``try/except`` so a failure in one does NOT abort
the others — partial bootstrap beats a full abort.

Fleet scope (geoid#3199): this whole sequence — including step 7 — only
ever runs via ``run_cold_boot()``, which ``main.py``'s
``_ColdBootReconciliationService`` gates to a single lease-winning process
per service revision. Step 7 is therefore NOT the primary place identity
providers get registered: ``IamModule.lifespan`` calls
``_register_identity_provider`` unconditionally on every process first.
Step 7 exists only so the fleet-once leader picks up a row it just seeded
in step 2 without waiting for its own next per-process registration.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dynastore.modules.iam.module import IamModule


class IamColdBootContributor:
    """Cold-boot contributor that owns the IAM bootstrap sequence.

    Holds a reference to the owning :class:`IamModule` instance so it can
    call ``_register_identity_provider`` after IdpConfig has been seeded.
    That call is idempotent per process (see the method's docstring), so it
    is safe to also run unconditionally from ``IamModule.lifespan``.
    """

    name: str = "iam"
    priority: int = 100

    def __init__(self, module: "IamModule") -> None:
        self._module = module

    async def run(self, engine: Any) -> None:
        """Run the full IAM cold-boot sequence."""
        # ------------------------------------------------------------------
        # Step 1: Seed OidcRoleSyncConfig — idempotent one-shot; skipped
        # when a row already exists so operator PATCHes are preserved.
        # ------------------------------------------------------------------
        try:
            from dynastore.modules.iam.module import _seed_oidc_role_sync_config
            await _seed_oidc_role_sync_config(engine)
        except Exception as exc:
            logger.warning(
                "IamModule: OidcRoleSyncConfig seed failed (non-fatal): %s", exc
            )

        # ------------------------------------------------------------------
        # Step 2: Apply auth_bootstrap preset — idempotent seed of IdpConfig
        # from IDP_* ENV when no row exists.  Must run before
        # _register_identity_provider (step 7) so the provider has a config
        # row to read from on a fresh DB.
        # ------------------------------------------------------------------
        try:
            from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
            await bootstrap_preset_if_absent(engine, preset_name="auth_bootstrap", force=True)
        except Exception as exc:
            logger.warning(
                "IamModule: auth_bootstrap preset failed (non-fatal): %s", exc
            )

        # ------------------------------------------------------------------
        # Step 3: Warn when JWT-claim attribute enrichment is active without
        # an issuer_allowlist — a security gap in multi-provider deployments.
        # ------------------------------------------------------------------
        try:
            from dynastore.modules.iam.module import _warn_jwt_attr_no_issuer_allowlist
            await _warn_jwt_attr_no_issuer_allowlist()
        except Exception as exc:
            logger.debug("IamModule: JWT-attr allowlist warning check failed: %s", exc)

        # ------------------------------------------------------------------
        # Steps 4–5: Apply foundational IAM presets (force=True so they
        # self-heal every cold-boot — see module.py comment for the full
        # rationale).  Each preset runs in its OWN try/except so a failure
        # in one does NOT abort the others.
        # ------------------------------------------------------------------
        from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent as _bpa
        for _preset_name in (
            "default_roles_baseline",
            "iam_baseline",
        ):
            try:
                await _bpa(engine, preset_name=_preset_name, force=True)
            except Exception as exc:
                logger.error(
                    "IamModule: cold-boot bootstrap of preset %r failed; "
                    "authorization for surfaces gated by this preset may 403 "
                    "until it is applied manually: %s",
                    _preset_name,
                    exc,
                    exc_info=True,
                )

        # ------------------------------------------------------------------
        # Step 6: Re-assert public_access_baseline LAST (force=True).
        # Gates the anonymous Cloud Run /health startup probe.  Must run
        # AFTER steps 4–5 so a same-boot re-seed of those roles cannot
        # clobber it.
        # ------------------------------------------------------------------
        try:
            await _bpa(engine, preset_name="public_access_baseline", force=True)
        except Exception as exc:
            logger.error(
                "IamModule: cold-boot re-assert of 'public_access_baseline' "
                "failed; the anonymous /health probe may 403 and the service "
                "may fail its Cloud Run startup probe: %s",
                exc,
                exc_info=True,
            )

        # ------------------------------------------------------------------
        # Step 7: Register the OIDC identity provider from the IdpConfig row
        # that step 2 ensured exists.  Runs AFTER auth_bootstrap so a fresh
        # DB always has a config row to read.  Guarded like every other step
        # so a transient failure here does not abort the orchestrator's
        # remaining contributors.
        # ------------------------------------------------------------------
        try:
            await self._module._register_identity_provider()
        except Exception as exc:
            logger.error(
                "IamModule: OIDC identity provider registration failed; "
                "token-authenticated requests will receive 401 until it is "
                "registered (seed 'idp_config' and restart): %s",
                exc,
                exc_info=True,
            )
