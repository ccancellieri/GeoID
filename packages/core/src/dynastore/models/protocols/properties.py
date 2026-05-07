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

"""
PropertiesProtocol: Protocol for managing shared system-wide properties.
"""

from typing import Protocol, Optional, Any, runtime_checkable

@runtime_checkable
class PropertiesProtocol(Protocol):
    """
    Protocol for managing shared key-value properties stored in the database.
    Used for cross-module configuration and shared state.
    """
    
    async def get_property(
        self, 
        key_name: str, 
        db_resource: Optional[Any] = None
    ) -> Optional[str]:
        """
        Retrieves a property value by its key.
        
        Args:
            key_name: The unique name of the property.
            db_resource: Optional database resource. If provided, the value is fetched
                        directly from the database (non-cached). If None, a cached
                        value may be returned.
        
        Returns:
            The property value as a string, or None if not found.
        """
        ...
        
    async def set_property(
        self, 
        key_name: str, 
        key_value: str, 
        owner_code: str, 
        db_resource: Optional[Any] = None
    ) -> int:
        """
        Sets or updates a property value.
        
        Args:
            key_name: The unique name of the property.
            key_value: The value to store.
            owner_code: Identifies the owner/setter of the property.
            db_resource: Optional database resource for transaction-aware operations.
            
        Returns:
            The number of rows affected (1 for success).
        """
        ...
        
    async def delete_property(
        self, 
        key_name: str, 
        db_resource: Optional[Any] = None
    ) -> int:
        """
        Deletes a property by its key.
        
        Args:
            key_name: The unique name of the property to delete.
            db_resource: Optional database resource for transaction-aware operations.
            
        Returns:
            The number of rows affected.
        """
        ...
