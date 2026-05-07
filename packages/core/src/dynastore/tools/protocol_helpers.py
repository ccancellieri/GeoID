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

"""
Protocol Migration and Resolution Helpers

This module provides standardized, professional access to platform protocols.
It replaces legacy direct module access patterns and the deprecated get_any_engine().

Access Strategies:
1. resolve(ProtocolClass): For one-time resolution of a specific protocol.
2. get_engine(): Specialized accessor for the most frequent dependency.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Type, TypeVar, Optional, Any, Union, cast
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.database import DatabaseProtocol

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

T = TypeVar("T")

def resolve(protocol_type: Type[T]) -> T:
    """
    Standardized, fail-fast resolution of a protocol.
    
    Args:
        protocol_type: The protocol class to resolve
        
    Returns:
        The registered implementation of the protocol
        
    Raises:
        RuntimeError: If no implementation is found for the protocol
    """
    instance = get_protocol(protocol_type)
    if not instance:
        # Professionals prefer clear, actionable error messages
        raise RuntimeError(
            f"Required protocol '{protocol_type.__name__}' is not available. "
            "Ensure the responsible module is installed and has been correctly initialized."
        )
    return cast(T, instance)


def get_engine(app_state: Optional[Any] = None) -> Optional[Union[AsyncEngine, Engine]]:
    from dynastore.tools.discovery import get_protocols
    providers = get_protocols(DatabaseProtocol)
    for db_raw in providers:
        db = cast(DatabaseProtocol, db_raw)
        try:
            engine = db.engine
            if engine is not None:
                return engine
        except Exception:
            continue
    return None
