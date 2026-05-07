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
#    See the License for the apecific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Localization-related protocol definitions.
"""

from typing import Protocol, Any, List, Dict, runtime_checkable

@runtime_checkable
class LocalizationProtocol(Protocol):
    """
    Protocol for localization utilities and metadata access.
    """
    
    def get_supported_languages(self) -> List[str]:
        """
        Returns a list of all supported language codes (e.g., ['en', 'fr', 'it']).
        """
        ...
        
    def get_language_details(self, lang: str) -> Dict[str, Any]:
        """
        Returns details for a specific language.
        """
        ...
        
    def is_multilanguage_input(self, value: Any) -> bool:
        """
        Detects if a value is a multilanguage dictionary.
        """
        ...
        
    def validate_language_consistency(self, data: Dict[str, Any], lang: str) -> None:
        """
        Validates that multilanguage input is consistent with the provided lang parameter.
        """
        ...

    def localize_model(self, model: Any, lang: str) -> Any:
        """
        Localizes a model object to the specified language.
        Returns the localized model (may be the same object if unchanged).
        """
        ...
