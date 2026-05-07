#    Copyright 2025 FAO
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

from typing import Protocol, Optional, Type, Union, Dict, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.db_config.platform_config_service import PluginConfig
    from dynastore.models.driver_context import DriverContext


@runtime_checkable
class PlatformConfigsProtocol(Protocol):
    """
    Protocol for platform-level configuration management.
    Handles global system settings that act as the base tier of the configuration waterfall.
    """

    @property
    def is_platform_manager(self) -> bool:
        """Marker property to distinguish from ConfigsProtocol during discovery."""
        ...

    async def get_config(
        self,
        config_cls: Union[str, Type["PluginConfig"]],
        ctx: Optional["DriverContext"] = None,
    ) -> "PluginConfig":
        """
        Retrieves a platform-level configuration or its default.
        """
        ...

    async def set_config(
        self,
        config_cls: Union[str, Type["PluginConfig"]],
        config: "PluginConfig",
        check_immutability: bool = True,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """
        Writes a platform-level configuration.
        """
        ...

    async def list_configs(self) -> Dict[Type["PluginConfig"], "PluginConfig"]:
        """
        Lists all platform-level configurations.
        """
        ...

    async def delete_config(
        self,
        config_cls: Union[str, Type["PluginConfig"]],
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """
        Deletes a platform-level configuration (resets to defaults).
        """
        ...
