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

"""
GcsStorageEventTask — durable task that reacts to GCS Pub/Sub object notifications.

Decouples the GCP extension from the asset service: the HTTP push handler simply
enqueues this task and returns immediately.  The task executor calls AssetsProtocol
via protocol discovery — no direct import of catalog internals.

Supported GCS event types:
  OBJECT_FINALIZE → create_asset
  OBJECT_DELETE   → hard delete asset
  OBJECT_ARCHIVE  → soft delete asset
"""

import logging
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict

# Hard runtime dep — see tasks/gcp_provision/task.py for rationale.
import google.cloud.storage  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.models.tasks import TaskPayload
from dynastore.modules import get_protocol
from dynastore.models.protocols import AssetsProtocol

logger = logging.getLogger(__name__)


class GcsStorageEventInputs(BaseModel):
    model_config = ConfigDict(extra="ignore")

    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str          # OBJECT_FINALIZE | OBJECT_DELETE | OBJECT_ARCHIVE
    asset_id: str
    asset_type: str = "ASSET"
    uri: str                 # gs://<bucket>/<object>
    metadata: Dict[str, Any] = {}


class GcsStorageEventTask(TaskProtocol):
    """
    Durable task for reacting to GCS object lifecycle events.

    Idempotent: create_asset is a no-op if the asset already exists;
    delete calls are no-ops if the asset is already gone.
    """

    task_type = "gcs_storage_event"
    priority: int = 80

    async def run(self, payload: TaskPayload[GcsStorageEventInputs]) -> Dict[str, Any]:
        inputs = payload.inputs

        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id
        event_type = inputs.event_type
        asset_id = inputs.asset_id

        assets = get_protocol(AssetsProtocol)
        if not assets:
            raise RuntimeError("AssetsProtocol not available — catalog module not loaded")

        if event_type == "OBJECT_FINALIZE":
            logger.info(
                f"GcsStorageEventTask: Creating asset '{asset_id}' "
                f"in {catalog_id}:{collection_id or ''} from GCS OBJECT_FINALIZE."
            )
            from dynastore.modules.catalog.asset_service import AssetCreate, AssetTypeEnum
            import os as _os
            asset_type = AssetTypeEnum(inputs.asset_type)
            # Storage-event finalize: the bytes already exist in GCS, so we
            # land an ACTIVE physical row with the resolved URI in one shot.
            filename_val = _os.path.basename(inputs.uri.rstrip('/')) or asset_id
            await assets.create_asset(
                catalog_id=catalog_id,
                collection_id=collection_id,
                asset=AssetCreate(
                    asset_id=asset_id,
                    filename=filename_val,
                    uri=inputs.uri,
                    asset_type=asset_type,
                    metadata=inputs.metadata,
                    owned_by="gcs",
                ),
            )
            logger.info(
                f"GcsStorageEventTask: Asset '{asset_id}' created successfully."
            )

        elif event_type == "OBJECT_DELETE":
            logger.info(
                f"GcsStorageEventTask: Hard-deleting asset '{asset_id}' "
                f"in {catalog_id}:{collection_id or ''} from GCS OBJECT_DELETE."
            )
            await assets.delete_assets(
                catalog_id=catalog_id,
                asset_id=asset_id,
                collection_id=collection_id,
                hard=True,
                propagate=False,  # We are reacting TO storage deletion — do not re-delete
            )

        elif event_type == "OBJECT_ARCHIVE":
            logger.info(
                f"GcsStorageEventTask: Soft-deleting asset '{asset_id}' "
                f"in {catalog_id}:{collection_id or ''} from GCS OBJECT_ARCHIVE."
            )
            await assets.delete_assets(
                catalog_id=catalog_id,
                asset_id=asset_id,
                collection_id=collection_id,
                hard=False,
                propagate=False,
            )

        else:
            logger.warning(
                f"GcsStorageEventTask: Unrecognised event_type '{event_type}' "
                f"for asset '{asset_id}'. No action taken."
            )

        return {"asset_id": asset_id, "event_type": event_type, "status": "handled"}
