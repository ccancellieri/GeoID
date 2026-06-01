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

from typing import Any, ClassVar, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field, field_validator
from dynastore.models.localization import LocalizedText
from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig


class WebPageConfig(BaseModel):

    id: str
    title: LocalizedText
    icon: str = "fa-circle"
    description: Optional[LocalizedText] = None
    priority: int = 0
    section: Optional[LocalizedText] = None
    is_embed: bool = False
    required_roles: Optional[List[str]] = None
    enabled: bool = True


    @field_validator("title", "description", "section", mode="before")
    @classmethod
    def convert_to_localized_text(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            return LocalizedText(en=v)
        if isinstance(v, dict):
            # Check if it's already compatible or needs conversion
            try:
                return LocalizedText(**v)
            except Exception:
                # If it's a dict but doesn't match keys, it might be a partial update
                # for a single language, but LocalizedText handles that in create_from_localized_input
                # For now let's assume it's a valid dict or should error.
                pass
        if isinstance(v, LocalizedText):
            return v
        return v


class WebPageSettingsConfig(PluginConfig):
    """Persistent configuration for web pages (overrides)."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "web")

    pages: Mutable[Dict[str, WebPageConfig]] = Field(default_factory=dict)


class StaticProviderConfig(BaseModel):
    prefix: str
    files: list[str] = Field(default_factory=list)


class DocItem(BaseModel):
    id: str
    title: str
    path: str
    category: str

