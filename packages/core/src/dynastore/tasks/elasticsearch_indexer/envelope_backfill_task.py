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

"""Opt-in backfill task: stamp ``_attrs`` onto pre-existing envelope-driver docs.

Documents written before #1441 shipped lack the ``_attrs`` field that the
read-filter compiler now uses for per-document ABAC restrictions.  This task
repairs existing docs without triggering a full reindex.

Triggering is always explicit — sysadmin via
``POST /search/catalogs/{cat}/collections/{coll}/backfill-envelope-attrs``.

The task:
  1. Reads the ``AttributeStampingPolicy`` for the collection via
     ``ConfigsProtocol.get_config``.  The policy is IAM-owned but the task
     reaches it through the protocol layer so the task itself carries no hard
     IAM import.
  2. Streams items from the SoR in pages of ``batch_size``.
  3. For each item calls the pure ``stamp_attrs_from_feature`` helper.
  4. Issues ES ``_bulk`` ``update`` actions targeting only the ``attrs``
     field (``doc_as_upsert: false`` — updates existing docs only;
     ``retry_on_conflict: 3``).
  5. ``dry_run=True`` counts items but skips the bulk write.
  6. Errors are collected per-batch and returned in the summary; partial
     backfill is acceptable (the operator re-runs to converge).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from pydantic import BaseModel, Field

import opensearchpy  # noqa: F401 — gates entry-point loading on services without ES

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input model
# ---------------------------------------------------------------------------

class EnvelopeAttrsBackfillInputs(BaseModel):
    catalog_id: str = Field(..., description="Catalog whose collection should be backfilled.")
    collection_id: str = Field(..., description="Collection to backfill.")
    dry_run: bool = Field(
        False,
        description="When True, count items but do not write to Elasticsearch.",
    )
    batch_size: int = Field(
        500,
        ge=1,
        le=5000,
        description="Number of items per SoR page and ES bulk batch.",
    )


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

class EnvelopeAttrsBackfillTask(TaskProtocol):
    """Stamp ``attrs`` onto existing envelope-driver docs for a single collection.

    Per-collection, opt-in only.  Requires an ``AttributeStampingPolicy``
    to be configured for the collection — absent policy short-circuits
    immediately.

    Triggered by
    ``POST /search/catalogs/{cat}/collections/{coll}/backfill-envelope-attrs``.
    """

    task_type = "envelope_attrs_backfill_collection"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:  # type: ignore[override]
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.elasticsearch.client import (
            get_index_prefix as _get_index_prefix,
        )
        from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
            get_envelope_index_name,
        )
        from dynastore.modules.iam.stamping_config import stamp_attrs_from_feature
        from dynastore.modules.elasticsearch.bulk_reindex import (
            get_es_client as _build_es_client,
        )
        from dynastore.tools.discovery import get_protocol

        inputs = EnvelopeAttrsBackfillInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id

        # --- 1. Resolve stamping policy (via ConfigsProtocol, no hard IAM dep) ---
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return {
                "status": "skipped",
                "reason": "ConfigsProtocol not available",
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }

        try:
            # Lazy import keeps the task free of direct IAM coupling at module
            # load time; the protocol lookup is the only runtime dep.
            from dynastore.modules.iam.stamping_config import AttributeStampingPolicy

            policy = await configs.get_config(
                AttributeStampingPolicy,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception as exc:
            logger.warning(
                "EnvelopeAttrsBackfillTask: policy lookup failed for %s/%s: %s",
                catalog_id,
                collection_id,
                exc,
            )
            return {
                "status": "skipped",
                "reason": f"policy lookup error: {exc}",
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }

        if policy is None:
            return {
                "status": "skipped",
                "reason": "no AttributeStampingPolicy configured",
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }

        attribute_paths: Dict[str, str] = getattr(policy, "attribute_paths", {}) or {}
        if not attribute_paths:
            return {
                "status": "skipped",
                "reason": "AttributeStampingPolicy.attribute_paths is empty",
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }

        # --- 2. Resolve envelope index name ---
        from dynastore.modules.storage.drivers.elasticsearch import (
            _resolve_catalog_physical_id,
        )
        catalog_physical_id = await _resolve_catalog_physical_id(catalog_id)
        index_name = get_envelope_index_name(_get_index_prefix(), catalog_physical_id)

        # --- 3. Resolve dependencies ---
        # get_protocol returns CatalogsProtocol which gains .search() at runtime
        # via the concrete implementation; the protocol base class does not declare
        # search() so we annotate as Any to keep pyright clean (same pattern as
        # bulk_reindex.py:get_protocol(CatalogsProtocol)).
        catalogs_proto: Any = get_protocol(CatalogsProtocol)
        if catalogs_proto is None:
            raise RuntimeError("CatalogsProtocol not available in this process.")

        es = _build_es_client()

        # --- 4. Stream + update ---
        total_updated = 0
        total_would_update = 0
        total_errors: List[Dict[str, Any]] = []
        offset = 0
        batch_size = inputs.batch_size

        while True:
            try:
                result = await catalogs_proto.search(
                    catalog_id,
                    collection_id,
                    limit=batch_size,
                    offset=offset,
                )
            except Exception as exc:
                logger.error(
                    "EnvelopeAttrsBackfillTask: SoR page fetch failed at offset %d "
                    "for %s/%s: %s",
                    offset, catalog_id, collection_id, exc,
                )
                total_errors.append({"offset": offset, "error": str(exc)})
                break

            features = result.get("features", [])
            if not features:
                break

            if inputs.dry_run:
                total_would_update += len(features)
            else:
                bulk_body = _build_bulk_update_body(
                    features, index_name, catalog_id, collection_id, attribute_paths,
                    stamp_attrs_from_feature,
                )
                if bulk_body:
                    try:
                        resp = await es.bulk(body=bulk_body)
                        batch_errors = _collect_bulk_errors(resp, offset)
                        total_errors.extend(batch_errors)
                        total_updated += (len(bulk_body) // 2) - len(batch_errors)
                    except Exception as exc:
                        logger.error(
                            "EnvelopeAttrsBackfillTask: bulk update failed at offset %d "
                            "for %s/%s: %s",
                            offset, catalog_id, collection_id, exc,
                        )
                        total_errors.append({"offset": offset, "error": str(exc)})
                        # Continue to next batch — partial backfill is acceptable.

            if len(features) < batch_size:
                break
            offset += batch_size

        if inputs.dry_run:
            return {
                "status": "dry_run",
                "would_update": total_would_update,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }

        return {
            "status": "done",
            "updated": total_updated,
            "errors": total_errors,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
        }


# ---------------------------------------------------------------------------
# Private helpers (pure, testable separately)
# ---------------------------------------------------------------------------

def _extract_geoid(feature: Any) -> str | None:
    """Extract the geoid (item id) from a Feature dict or pydantic model."""
    return getattr(feature, "id", None) or (
        feature.get("id") if isinstance(feature, dict) else None
    )


def _feature_to_dict(feature: Any) -> Dict[str, Any]:
    """Coerce a Feature (pydantic model or raw dict) to a plain dict."""
    if isinstance(feature, dict):
        return feature
    if hasattr(feature, "model_dump"):
        return feature.model_dump(by_alias=True, exclude_none=True, mode="json")
    return dict(feature) if hasattr(feature, "__iter__") else {}


def _build_bulk_update_body(
    features: List[Any],
    index_name: str,
    catalog_id: str,
    collection_id: str,
    attribute_paths: Dict[str, str],
    stamp_fn: Any,
) -> List[Any]:
    """Build the ES bulk request body for ``_update`` actions.

    Uses ``doc_as_upsert: false`` (update existing docs only — do not create
    new ones for items that were never indexed into the envelope index).
    ``retry_on_conflict: 3`` guards against concurrent writes.
    """
    body: List[Any] = []
    for feature in features:
        geoid = _extract_geoid(feature)
        if not geoid:
            continue
        doc_dict = _feature_to_dict(feature)
        new_attrs = stamp_fn(doc_dict, attribute_paths)
        body.append({
            "update": {
                "_index": index_name,
                "_id": geoid,
                "retry_on_conflict": 3,
            }
        })
        body.append({
            "doc": {"attrs": new_attrs},
            "doc_as_upsert": False,
        })
    return body


def _collect_bulk_errors(resp: Any, offset: int) -> List[Dict[str, Any]]:
    """Extract per-item errors from an ES bulk response."""
    errors: List[Dict[str, Any]] = []
    for item in (resp or {}).get("items", []):
        entry = (item.get("update") or {}) if isinstance(item, dict) else {}
        err = entry.get("error") if isinstance(entry, dict) else None
        if err:
            errors.append({
                "id": entry.get("_id"),
                "offset": offset,
                "reason": str(
                    err.get("reason", err) if isinstance(err, dict) else err
                ),
            })
    return errors
