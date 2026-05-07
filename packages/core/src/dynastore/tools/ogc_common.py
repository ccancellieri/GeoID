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

import re
from typing import Dict, Any, Optional

def parse_subset_parameter(subset_str: Optional[str]) -> Dict[str, Any]:
    """
    Parses an OGC API 'subset' parameter string into a dictionary.
    Example: "asset_code(api-ogc-123),h3_lvl10(8a3d8d2d4d2dfff)"
    """
    if not subset_str:
        return {}
    
    params = {}
    # Regex to find key(value) pairs
    pattern = re.compile(r'(\w+)\(([^)]+)\)')
    matches = pattern.findall(subset_str)
    
    for key, value in matches:
        # Basic type inference (can be expanded)
        if value.isnumeric():
            params[key] = int(value)
        elif re.match(r'^-?\d+\.\d+$', value):
            params[key] = float(value)
        else:
            params[key] = value
            
    return params