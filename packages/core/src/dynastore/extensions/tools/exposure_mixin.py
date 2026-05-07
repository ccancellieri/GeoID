"""Shared mixin and registry constants for the service-exposure control panel."""

from pydantic import BaseModel, Field


class ExposableConfigMixin(BaseModel):
    """Adds a per-scope `enabled` toggle to any PluginConfig."""

    enabled: bool = Field(
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
})
