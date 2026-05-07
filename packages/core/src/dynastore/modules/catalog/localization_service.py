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
LocalizationService: Implementation of LocalizationProtocol.
"""

from typing import List, Dict, Any
from dynastore.models.protocols.localization import LocalizationProtocol
from dynastore.models.localization import (
    _LANGUAGE_METADATA,
    is_multilanguage_input,
    localize_dict,
    validate_language_consistency,
)

class LocalizationService(LocalizationProtocol):
    """
    Standard implementation of LocalizationProtocol.
    """
    
    def get_supported_languages(self) -> List[str]:
        return list(_LANGUAGE_METADATA.keys())
        
    def get_language_details(self, lang: str) -> Dict[str, Any]:
        return _LANGUAGE_METADATA.get(lang, {"name": lang, "dir": "ltr"})
        
    def is_multilanguage_input(self, value: Any) -> bool:
        return is_multilanguage_input(value)
        
    def validate_language_consistency(self, data: Dict[str, Any], lang: str) -> None:
        return validate_language_consistency(data, lang)

    def localize_model(self, model: Any, lang: str) -> Any:
        if hasattr(model, "model_dump"):
            data = model.model_dump()
            localized, _ = localize_dict(data, lang)
            return model.model_validate(localized)
        if isinstance(model, dict):
            localized, _ = localize_dict(model, lang)
            return localized
        return model
