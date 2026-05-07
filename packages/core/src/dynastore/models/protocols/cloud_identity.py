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

from typing import Protocol, Optional, Any, Dict, runtime_checkable

@runtime_checkable
class CloudIdentityProtocol(Protocol):
    """
    Protocol for accessing cloud identity and infrastructure context
    (e.g., Project ID, Service Account details).
    """

    def get_project_id(self) -> Optional[str]:
        """Returns the project ID or account identifier for the cloud provider."""
        ...

    def get_account_email(self) -> Optional[str]:
        """Returns the email or identifier of the active cloud identity."""
        ...

    def get_region(self) -> Optional[str]:
        """Returns the active cloud region."""
        ...

    def get_credentials_object(self) -> Any:
        """
        Returns the native credentials object (e.g., google.auth.credentials.Credentials).
        Mainly for use with native Python SDKs.
        """
        ...

    def get_identity_info(self) -> Optional[Dict[str, Any]]:
        """Returns a dictionary with full identity details."""
        ...

    async def get_fresh_token(self) -> str:
        """Returns a fresh access token for the active cloud identity."""
        ...
