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
Cloud storage client protocol definitions for object storage access.
"""

from typing import Protocol, Any, runtime_checkable

@runtime_checkable
class CloudStorageClientProtocol(Protocol):
    """
    Protocol for cloud storage client access, enabling decoupled access
    to object storage operations without direct dependency on specific providers.
    
    This protocol is designed to be optional - implementations should handle
    unavailability gracefully. Can be implemented by GCS, S3, Azure Blob Storage,
    MinIO, or any other object storage provider.
    """
    
    def get_storage_client(self) -> Any:
        """
        Returns the shared cloud storage client instance.
        
        Returns:
            Cloud storage client instance (provider-specific)
            
        Raises:
            RuntimeError: If the storage client is not available
        """
        ...
    
    async def get_fresh_token(self) -> str:
        """
        Ensures credentials are valid and returns a fresh access token.
        
        Returns:
            Fresh access token string
            
        Raises:
            RuntimeError: If credentials are not available
        """
        ...

