from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from typing import ClassVar, Optional, Tuple


class JoinsPluginConfig(ExposableConfigMixin, PluginConfig):
    """Service-exposure config for the OGC API - Joins extension."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "joins")

    # `enabled` inherited from ExposableConfigMixin — no further fields.
