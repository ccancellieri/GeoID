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

def __repr__(self, sensitive_attrs=None, private_attrs=None) -> str:
    """
    Provides a flexible string representation of the object.

    Iterates over the class's annotations to display attribute values. Public attributes
    (not starting with '_') are included by default. Specific private attributes
    can be included via the `private_attrs` list.
    
    Args:
        sensitive_attrs: A list of attribute names to mask in the output.
        private_attrs: A list of private attribute names (starting with '_') to
                       explicitly include in the output.
    """
    if sensitive_attrs is None:
        sensitive_attrs = []
    if private_attrs is None:
        private_attrs = []
    attrs_to_show = []
    # Iterate over all annotated attributes
    for attr_name in self.__class__.__annotations__.keys():
        is_public = not attr_name.startswith('_')
        is_explicitly_included = attr_name in private_attrs

        # Include public attributes OR those explicitly requested in private_attrs
        if is_public or is_explicitly_included:
            # First, get the value since we've decided to show this attribute
            attr_value = getattr(self, attr_name, None)

            # Next, decide how to format it (masked or not)
            if attr_name in sensitive_attrs:
                attrs_to_show.append(f"{attr_name}='***'")
            else:
                attrs_to_show.append(f"{attr_name}={repr(attr_value)}")

    return f"{self.__class__.__name__}({', '.join(attrs_to_show)})"