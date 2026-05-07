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

from typing import Protocol, Optional, Any, Tuple, runtime_checkable

@runtime_checkable
class EventingProtocol(Protocol):
    """
    Protocol for cloud-based eventing and notifications (e.g., Pub/Sub, SNS).
    """

    async def setup_catalog_eventing(self, catalog_id: str) -> Tuple[str, Any]:
        """
        Sets up eventing resources (e.g., topics, subscriptions) for a catalog.
        Returns a tuple of (resource_identifier, config_object).
        """
        ...

    async def teardown_catalog_eventing(self, catalog_id: str, config: Optional[Any] = None) -> None:
        """
        Tears down cloud-based eventing resources for a catalog.
        If config is None, it should attempt to teardown deterministic/default resources (Force Cleanup).
        """
        ...

    async def get_eventing_config(self, catalog_id: str) -> Optional[Any]:
        """Returns the eventing configuration for a catalog."""
        ...

    async def apply_eventing_config(self, catalog_id: str, config: Any, conn: Optional[Any] = None) -> None:
        """Applies eventing configuration changes to the live cloud resources."""
        ...

    async def teardown_managed_eventing_channel(
        self, catalog_id: str, managed_eventing: Any
    ) -> None:
        """Tears down a managed eventing channel (notification + subscription + topic)."""
        ...
