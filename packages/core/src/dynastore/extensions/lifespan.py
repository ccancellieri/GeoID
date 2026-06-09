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

"""Extension lifespan and post-discovery application configuration.

Lives in a regular submodule (not ``extensions/__init__.py``) so the
``dynastore.extensions`` package is a PEP 420 namespace — required for
the per-extension wheel split where each extension wheel installs a
``dynastore/extensions/<name>/`` subtree.

Callers should import the public functions from this module directly:

    from dynastore.extensions.lifespan import lifespan, apply_app_configurations
"""

import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import List

from fastapi import APIRouter, Depends, FastAPI

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.registry import ExtensionConfig
from dynastore.extensions.documentation import (
    configure_swagger_ui,
    enrich_extension_metadata,
)
from dynastore.extensions.tools.exposure_matrix import ExposureMatrix
from dynastore.extensions.tools.exposure_mixin import (
    ExposableConfigMixin,
    find_dead_exposable_configs,
    _get_dynamic_sets,
)
from dynastore.extensions.tools.exposure_openapi import install_filtered_openapi
from dynastore.extensions.tools.exposure_route import make_exposure_dependency
from dynastore.models.plugin_config import list_registered_configs
from dynastore.models.protocols import ConfigsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def apply_app_configurations(app: FastAPI):
    """
    Applies global configurations and triggers extension specific hooks.
    """
    configs: List[ExtensionConfig] = getattr(app.state, "ordered_configs", [])
    logger.info("--- Applying pre-startup app configurations from extensions (e.g., middleware) ---")

    # 1. Apply global Swagger UI enhancements
    configure_swagger_ui(app)

    # 2. Apply extension-specific configurations
    for config in configs:
        if isinstance(config.instance, ExtensionProtocol):
            try:
                config.instance.configure_app(app)
                logger.info(f"Applied app configuration from '{config.cls.__name__}'.")
            except Exception:
                logger.error(f"Failed during configure_app of '{config.cls.__name__}'.", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    A FastAPI lifespan manager that orchestrates the startup and shutdown
    of all enabled extensions.
    """
    configs: List[ExtensionConfig] = getattr(app.state, "ordered_configs", [])

    if not configs:
        logger.warning("Lifespan: No extensions enabled/instantiated.")
        yield
        return

    logger.info(f"Activating {len(configs)} extensions...")

    async with AsyncExitStack() as stack:
        # --- PHASE 2: Lifespans ---
        logger.info("--- Phase 2: Executing all extension lifespans ---")
        for config in configs:
            if isinstance(config.instance, ExtensionProtocol):
                try:
                    await stack.enter_async_context(config.instance.lifespan(app))
                    logger.info(f"Lifespan for '{config.cls.__name__}' entered successfully.")
                except Exception:
                    logger.error(f"CRITICAL: Lifespan for '{config.cls.__name__}' failed on startup.", exc_info=True)

        # --- PHASE 3: Routers & Docs ---
        logger.info("--- Phase 3: Mounting all extension routers ---")

        # Surface mixin misuses early so the next dead-toggle regression
        # (#853 / #854) shows up at boot instead of as an operator report.
        for dead_cls, reason in find_dead_exposable_configs():
            logger.warning(
                "ExposableConfigMixin is on %s but ExposureMatrix cannot "
                "enforce its `enabled` field: %s",
                dead_cls.__name__,
                reason,
            )

        # Build the exposure matrix + custom route class.
        # ConfigsProtocol is provided by the catalog module; services whose
        # SCOPE excludes it (e.g. auth, tools) run without exposure gating.
        #
        # Derive always-on and known sets purely from the live registry —
        # extensions declare ``always_on = True`` on their class, and the
        # set of known extensions is whatever was discovered via entry-points
        # for the current SCOPE.  ``always_on`` selects from the discovered
        # set, never extends it; an extension whose wheel is not installed
        # is simply absent (#1003 / #1014: no hardcoded fallback).
        always_on_exts, known_exts = _get_dynamic_sets()
        togglable = known_exts - always_on_exts

        configs_svc = get_protocol(ConfigsProtocol)
        matrix: ExposureMatrix | None = None
        if configs_svc is not None:
            plugin_cls_by_ext: dict[str, type] = {}
            for cls in list_registered_configs().values():
                if not issubclass(cls, ExposableConfigMixin):
                    continue
                parts = cls.__module__.split(".")
                if len(parts) >= 3 and parts[1] in ("extensions", "modules"):
                    ext = parts[2]
                    if ext in togglable:
                        plugin_cls_by_ext.setdefault(ext, cls)

            matrix = ExposureMatrix(
                configs_service=configs_svc,
                togglable_extensions=togglable,
                plugin_class_by_extension=plugin_cls_by_ext,
                ttl_seconds=30.0,
            )
            app.state.exposure_matrix = matrix
            install_filtered_openapi(app, matrix)
            # Pre-warm the snapshot so the sync OpenAPI filter sees real state on
            # the very first /openapi.json request (before any config write has
            # triggered an invalidate).
            await matrix.get()
        else:
            logger.info(
                "ConfigsProtocol not registered (catalog module not in SCOPE); "
                "skipping exposure matrix — all extensions mount unconditionally."
            )

        # Map router prefix → extension name so the filtered OpenAPI can hide
        # disabled extensions without polluting each operation's tags (which
        # would cause Swagger UI to render every endpoint twice — once under
        # the router's display tag and once under the extension name).
        extension_prefixes: list[tuple[str, str]] = []

        for config in configs:
            if config.instance is None: continue

            router = getattr(config.instance, "router", None)

            if isinstance(router, APIRouter):
                # 1. Enrich Metadata (READMEs, Tags, Help Links)
                enrich_extension_metadata(app, config, router)

                # 2. Mount Router
                extension_name = config.cls._registered_name  # type: ignore[attr-defined]
                # Gated extensions get a dependency that 503s when the matrix
                # says so. Operation→extension mapping is tracked via prefixes
                # below rather than via an injected tag.
                include_kwargs: dict = {}
                if (
                    matrix is not None
                    and extension_name in togglable
                ):
                    include_kwargs["dependencies"] = [
                        Depends(make_exposure_dependency(matrix, extension_name))
                    ]
                app.include_router(router, **include_kwargs)
                if router.prefix:
                    extension_prefixes.append((router.prefix, extension_name))

                logger.info(f"Mounted router for '{config.cls.__name__}' at prefix '{router.prefix}'.")

        app.state.extension_prefixes = extension_prefixes

        # --- Force OpenAPI Schema Refresh ---
        app.openapi_schema = None

        yield

    logger.info("Application shutting down.")
