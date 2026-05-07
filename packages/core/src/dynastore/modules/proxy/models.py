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

# File: dynastore/modules/proxy/models.py

from pydantic import BaseModel, Field
import datetime
from typing import List, Optional, Dict, Union, Any
from ipaddress import IPv4Address, IPv6Address

class ShortURL(BaseModel):
    id: int
    short_key: str
    long_url: str
    collection_id: str = "_catalog_"
    created_at: datetime.datetime
    comment: Optional[str] = None

class URLAnalytics(BaseModel):
    id: int
    short_key_ref: str
    timestamp: datetime.datetime
    ip_address: Union[IPv4Address, IPv6Address, str, None] # Flexible IP handling
    user_agent: Optional[str] = None
    referrer: Optional[str] = None

class AnalyticsPage(BaseModel):
    """
    Response model for analytics queries.
    Includes the raw logs page and optional aggregated summaries.
    """
    data: List[URLAnalytics]
    long_url: Optional[str] = None
    next_cursor: Optional[str] = None
    
    # NEW: Support for the aggregated stats returned by the optimized driver
    aggregations: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Summary statistics (total_clicks, clicks_per_day, etc.) if requested."
    )