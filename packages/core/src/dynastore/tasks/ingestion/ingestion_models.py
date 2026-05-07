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

# dynastore/modules/ingestion/models.py

from pydantic import BaseModel, Field, model_validator
from typing import Optional, List, Literal, Dict, Any

# --- Configuration Models ---


IngestionReportingConfig = Dict[str, Dict[str, Any]]
"""A type alias for the reporting configuration dictionary, where keys are reporter class names and values are their configurations."""

class AttributeMappingItem(BaseModel):
    source: Optional[str] = None
    constant: Optional[str] = None
    map_to: str

    @model_validator(mode='after')
    def check_source_or_constant(self) -> 'AttributeMappingItem':
        if not self.source and not self.constant:
            raise ValueError("Either 'source' or 'constant' must be provided.")
        if self.source and self.constant:
            raise ValueError("Only one of 'source' or 'constant' can be provided, not both.")
        return self

class ColumnMappingConfig(BaseModel):
    """Defines how source file columns map to database fields."""
    external_id: Optional[str] = Field(None, description="The source column that serves as a unique identifier for each feature.")
    geometry_wkb: Optional[str] = Field(None, description="For Parquet files, the column containing geometry in WKB format.")
    csv_wkt_column: Optional[str] = Field(None, description="For CSV files, the column containing geometry in WKT format.")
    csv_lat_column: Optional[str] = Field(None, description="For CSV files, the column containing latitude.")
    csv_lon_column: Optional[str] = Field(None, description="For CSV files, the column containing longitude.")
    csv_elevation_column: Optional[str] = Field(None, description="For CSV files, the column containing elevation.")
    csv_elevation_unit: Optional[str] = Field(None, description="For CSV files, the unit of elevation.")
    # csv_time_validity_start_column: Optional[str] = Field(None, description="For CSV files, the column containing time validity start.")
    # csv_time_validity_end_column: Optional[str] = Field(None, description="For CSV files, the column containing time validity end.")
    attributes_source_type: Literal['explicit_list', 'all'] = Field('all', description="Determines how attributes are sourced.")
    attribute_mapping: Optional[List[AttributeMappingItem]] = Field(None, description="If attributes_source_type is 'explicit_list', defines source and map_to for attributes.")


class CsvGeometryConfig(BaseModel):
    """Configuration for CSV geometry columns."""
    """
    This model is no longer directly used in TaskIngestionRequest,
    its fields are now part of ColumnMappingConfig.
    Keeping it here for reference or if it's used elsewhere.
    """
    wkt_column: Optional[str] = Field(None, description="The column containing geometry in WKT format.")
    lat_column: Optional[str] = Field(None, description="The column containing latitude.")
    lon_column: Optional[str] = Field(None, description="The column containing longitude.")

class IngestionAsset(BaseModel):
    """
    Defines the source asset for an ingestion task.
    An asset can be identified by its existing asset_id or by a new URI.
    """
    asset_id: Optional[str] = Field(None, description="The asset_id of an existing asset to ingest from.")
    uri: Optional[str] = Field(None, description="The URI of a file to ingest. If the asset doesn't exist, it will be created.")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Metadata to be attached to the asset upon creation or update.")

    @model_validator(mode='before')
    @classmethod
    def check_code_or_uri(cls, data: Any) -> Any:
        if isinstance(data, dict):
            code_provided = data.get('asset_id')
            uri_provided = data.get('uri')

            if code_provided and uri_provided:
                # Relaxed validation: If both are provided, we accept it.
                # The logic in main_ingestion will handle priority (lookup by asset_id, if missing create from uri).
                pass 
            if not code_provided and not uri_provided:
                # Instead of raising an error immediately, we allow it to pass if it's going to be populated later
                pass
        return data

# ... (rest of the file)

class TaskIngestionRequest(BaseModel):
    """
    Defines the transient parameters for a single, specific ingestion task.
    This is the work order for an ingestion run.
    """

    # Asset is now Optional to allow for the legacy 'source_file_path' to be processed first.
    asset: Optional[IngestionAsset] = Field(None, description="The source asset for the ingestion task, specified by asset_id or URI.")
    encoding: str = Field(
        default="utf-8", 
        description="The character encoding of the source file (e.g., 'utf-8', 'latin-1', 'cp1252'). Defaults to 'utf-8'."
    )
    column_mapping: ColumnMappingConfig = Field(..., description="Defines how source file columns map to database fields.")
    source_srid: Optional[int] = Field(None, description="Default source srid")
    lang: str = Field(default="en", description="Target language for the ingested metadata. Use '*' for multi-language input.")


    time_validity_start_column: Optional[str] = None
    time_validity_end_column: Optional[str] = None
    
    reporting: Optional[IngestionReportingConfig] = Field(default=None, description="Configuration for generating detailed ingestion reports.")
    pre_operations: Optional[Dict[str, Dict[str, Any]]] = Field(default=None, description="Configuration for pre-ingestion operations.")
    post_operations: Optional[Dict[str, Dict[str, Any]]] = Field(default=None, description="Configuration for post-ingestion operations.")
    database_batch_size: Optional[int] = Field(default=None, description="Number of records to insert into the database in a single transaction. If not set, max_batch_memory_mb is used.")
    max_batch_memory_mb: int = Field(default=100, description="Maximum memory (in MB) to accumulate in a batch before flushing to database. Takes precedence over database_batch_size if both are set.")
    offset: int = Field(default=0, description="Number of records to skip from the beginning of the source file.")
    limit: Optional[int] = Field(default=None, description="Maximum number of records to process from the source file.")
    read_batch_size:  int = Field(default=1000, description="Number of records to read from the source file if supported by the source file format.")

    @model_validator(mode='after')
    def validate_asset_present(self) -> 'TaskIngestionRequest':
        """
        Final check to ensure we have a valid asset definition.
        """
        if not self.asset or (not self.asset.asset_id and not self.asset.uri):
             raise ValueError("A valid asset definition (asset_id or uri) is required.")
        return self

class IngestionProcessRequest(BaseModel):
    """
    Represents the input structure for the OGC Process 'ingestion'.

    The target ``catalog_id`` and ``collection_id`` are taken from the URL
    path (``/catalogs/{catalog_id}/collections/{collection_id}/processes/ingestion/execution``)
    — not from the inputs body. The endpoint rejects requests that try to
    target the process via a non-matching URL scope (see ``ProcessScope``).
    """
    ingestion_request: TaskIngestionRequest = Field(..., description="The detailed configuration for the ingestion task.")
