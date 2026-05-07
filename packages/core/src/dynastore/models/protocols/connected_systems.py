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

from typing import Any, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class ConnectedSystemsProtocol(Protocol):
    """Structural type contract for the OGC API - Connected Systems service."""

    async def list_systems(
        self, catalog_id: str, limit: int = 100, offset: int = 0
    ) -> List[Any]: ...

    async def get_system(
        self, system_id: str, catalog_id: str
    ) -> Optional[Any]: ...

    async def list_datastreams(
        self, catalog_id: str, limit: int = 100, offset: int = 0
    ) -> List[Any]: ...

    async def get_datastream(
        self, datastream_id: str, catalog_id: str
    ) -> Optional[Any]: ...
