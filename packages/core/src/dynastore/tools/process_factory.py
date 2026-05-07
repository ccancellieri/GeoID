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

from typing import Type, Dict, Any, List, Optional
from pydantic import BaseModel
from dynastore.modules.processes.models import (
    Process,
    ProcessInput,
    ProcessScope,
    JobControlOptions,
    TransmissionMode,
)

def create_process_definition(
    id: str,
    title: str,
    description: str,
    input_model: Type[BaseModel],
    scopes: List[ProcessScope],
    version: str = "1.0.0",
    job_control_options: Optional[List[JobControlOptions]] = None,
    output_transmission: Optional[List[TransmissionMode]] = None,
    additional_inputs: Optional[Dict[str, Any]] = None,
) -> Process:
    """
    Generates an OGC Process definition from a Pydantic model.

    Args:
        id: The unique identifier for the process.
        title: A human-readable title.
        description: A description of what the process does.
        input_model: The Pydantic model defining the process inputs.
        version: The version of the process.
        job_control_options: List of supported job control options. Defaults to [ASYNC_EXECUTE].
        output_transmission: List of supported output transmission modes. Defaults to [REFERENCE].
        additional_inputs: Optional dictionary of additional raw input schemas to merge.

    Returns:
        Process: A fully compliant OGC Process definition.
    """
    schema = input_model.model_json_schema()
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    inputs = {}
    # Map Pydantic properties to OGC ProcessInput
    defs = schema.get("$defs", {})
    
    for name, prop in properties.items():
        # Clean up Pydantic schema artifacts if necessary
        
        # Ensure that if the property uses definitions, they are available in the per-input schema.
        # This prevents 'PointerToNowhere' errors when validating individual inputs.
        full_prop_schema = prop.copy()
        if defs:
            full_prop_schema["$defs"] = defs

        # Note: ProcessInput model currently does not support minOccurs/maxOccurs.
        # We construct the dict corresponding to ProcessInput structure.
        # Using 'schema' key because alias="schema" allows populating schema_ via alias in input dict.
        input_desc = {
            "title": prop.get("title", name),
            "description": prop.get("description", ""),
            "schema": full_prop_schema, 
        }
        inputs[name] = input_desc

    if additional_inputs:
        inputs.update(additional_inputs)

    if not scopes:
        raise ValueError(
            f"Process '{id}' must declare at least one ProcessScope (got empty list)."
        )
    return Process(
        id=id,
        title=title,
        description=description,
        version=version,
        scopes=list(scopes),
        jobControlOptions=job_control_options or [JobControlOptions.ASYNC_EXECUTE],
        outputTransmission=output_transmission or [TransmissionMode.REFERENCE],
        inputs=inputs,
        outputs={} # Default to empty outputs for now as per ingestion/tiles usage
    )
