#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    you may obtain a copy of the License at
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
import os

def insert_before_extension(file_path: str, insert_string: str) -> str:
    """
    Inserts a custom string immediately before the file extension in a path.

    Args:
        file_path: The original path string (e.g., 'gs://bucket/dir/file.json').
        insert_string: The string to insert (e.g., '_chunk_1').

    Returns:
        The modified path string.
    """
    # 1. Split the path into the base (everything before the extension) and the extension.
    #    Example: 'gs://.../file.json' -> ('gs://.../file', '.json')
    base, ext = os.path.splitext(file_path)

    # 2. Combine the base, the insert string, and the extension.
    modified_path = f"{base}{insert_string}{ext}"

    return modified_path