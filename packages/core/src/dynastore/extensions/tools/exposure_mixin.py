"""Shared mixin and registry constants for the service-exposure control panel."""

from pydantic import BaseModel, Field

from dynastore.models.mutability import Mutable


class ExposableConfigMixin(BaseModel):
    """Adds a per-scope `enabled` toggle to any PluginConfig."""

    enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When False, the extension is unavailable at this scope and returns "
            "503 Service Unavailable. At platform scope, the routes are also "
            "omitted from the OpenAPI schema."
        ),
    )


ALWAYS_ON_EXTENSIONS: frozenset[str] = frozenset({
    "iam", "auth", "configs", "web", "admin",
    "tools", "template", "httpx", "documentation",
})


KNOWN_EXTENSION_IDS: frozenset[str] = frozenset({
    *ALWAYS_ON_EXTENSIONS,
    "stac", "features", "wfs", "coverages", "edr", "records", "processes", "dggs",
    "tiles", "maps", "styles", "dimensions", "dwh", "joins", "search", "stats",
    "gcp", "logs", "notebooks", "crs", "gdal", "assets", "moving_features",
    "connected_systems", "volumes",
})


def find_dead_exposable_configs() -> list[tuple[type, str]]:
    """Return registered ``ExposableConfigMixin`` subclasses that
    ``ExposureMatrix`` cannot enforce.

    The matrix in ``extensions/lifespan.py`` only consults ``enabled`` for
    configs whose module path matches ``dynastore.{extensions,modules}.<ext>``
    AND ``<ext> in KNOWN_EXTENSION_IDS - ALWAYS_ON_EXTENSIONS``. Any other
    use of the mixin produces an ``enabled`` field that appears in
    ``GET /configs/...`` responses but flips without effect — the operator
    bug #853/#854 cleaned up.

    Also reports **fan-out shadowing** (#855 item 1): when multiple
    ``ExposableConfigMixin`` subclasses share the same ``parts[2]`` ext
    name, ``ExposureMatrix`` only consults the first-registered (via
    ``plugin_cls_by_ext.setdefault``); the siblings' toggles silently
    no-op. The fix is to demote sibling configs to plain ``PluginConfig``
    and (if they need their own boolean knob) declare it under a distinct
    field name — see ``GcpCatalogBucketConfig.provision_enabled`` and
    ``TilesCachingConfig.cache_enabled`` for canonical examples.
    """
    # Local import to avoid a registration-time cycle: this module is
    # imported by config files before the platform service is wired.
    from dynastore.modules.db_config.plugin_config import list_registered_configs

    togglable = KNOWN_EXTENSION_IDS - ALWAYS_ON_EXTENSIONS
    dead: list[tuple[type, str]] = []
    per_ext: dict[str, list[type]] = {}
    for cls in list_registered_configs().values():
        if not issubclass(cls, ExposableConfigMixin):
            continue
        parts = cls.__module__.split(".")
        if not (len(parts) >= 3 and parts[1] in ("extensions", "modules")):
            dead.append((
                cls,
                f"module path {cls.__module__!r} is not under "
                "dynastore.{extensions,modules}.<ext>",
            ))
            continue
        ext = parts[2]
        if ext in ALWAYS_ON_EXTENSIONS:
            dead.append((
                cls,
                f"extension {ext!r} is in ALWAYS_ON_EXTENSIONS — routes are "
                "mounted unconditionally, `enabled` is never consulted",
            ))
            continue
        if ext not in togglable:
            dead.append((
                cls,
                f"extension {ext!r} is not in KNOWN_EXTENSION_IDS — "
                "ExposureMatrix skips it; add it to the registry if the "
                "toggle should be live",
            ))
            continue
        per_ext.setdefault(ext, []).append(cls)

    for ext, classes in per_ext.items():
        if len(classes) < 2:
            continue
        siblings = ", ".join(sorted(c.__name__ for c in classes))
        for cls in classes:
            dead.append((
                cls,
                f"extension {ext!r} has {len(classes)} ExposableConfigMixin "
                f"subclasses ({siblings}); ExposureMatrix consults only the "
                "first-registered via setdefault — strip the mixin from the "
                "non-canonical siblings (and declare any local boolean knob "
                "under a distinct field name)",
            ))
    return dead
