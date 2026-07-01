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

"""Standard job-result *message* helpers for async tasks (OGC API - Processes).

Every async task surfaces a single human-facing ``message`` on its OGC job
status / results documents. That message flows through one funnel:

* the task runner persists ``run()``'s return value verbatim as
  ``task.outputs`` (a JSON object);
* :func:`dynastore.modules.processes.models.task_to_status_info` lifts
  ``outputs["message"]`` onto the ``StatusInfo`` for successful jobs, uses
  ``error_message`` for failures, and synthesises a default when neither is
  present — so a terminal job is **never** message-less.

This module is the *general way* a task sets that message. Pick the helper that
matches the task's output shape:

* :func:`completed` — build the success ``StatusInfo`` carrying a ``message``
  (for tasks whose result *is* the status).
* :func:`with_message` — inject ``message`` into a data-bearing ``dict`` output
  without discarding the payload (for tasks that return structured data, e.g.
  ``gdalinfo`` returning ``{"info": ...}``).

For the two families of "best message":

* **File-producing exports** (``dwh_join``, ``export_features``,
  ``tiles_export``) deliver the artifact *by reference*: write it to a
  server-owned, per-job key with :func:`server_output_uri`, then return a
  time-limited signed URL from :func:`signed_result_url`.
* **In-place resource tasks** (``gdalinfo``, ``ingestion``) point at the public
  resource they affected: :func:`asset_verify_url`, :func:`items_verify_url`,
  or :func:`collection_verify_url`.

Headless runners (Cloud Run Jobs, background workers) have no HTTP request to
derive an absolute URL from, so the public ingress root is operator
configuration (:class:`PublicUrlConfig`). When it is unset the verify-URL
helpers return root-relative paths rather than guessing a host.
"""

import logging
from datetime import timedelta
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Union
from urllib.parse import quote
from uuid import UUID

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.protocols import (
    CloudIdentityProtocol,
    CloudStorageClientProtocol,
    StorageProtocol,
)
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url
from dynastore.modules.processes.models import Link, LocalizedText, StatusInfo
from dynastore.modules.tasks.models import TaskStatusEnum
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# Signed result URLs are valid for one week: OGC results are fetched
# out-of-band, and a week comfortably covers download + retry windows.
_SIGNED_URL_TTL = timedelta(days=7)


class PublicUrlConfig(PluginConfig):
    """Public base URL of the deployed API, for headless task result links.

    A headless task runner has no HTTP ``Request`` to derive an absolute URL
    from, so the externally reachable API root is operator-supplied here. Set
    it to the public root that fronts the catalog service — scheme + host +
    any proxy path prefix, no trailing slash — e.g.
    ``https://data.example.org/geospatial/v2/api/catalog``. OGC extension
    routers mount beneath it (``/features/...``, ``/assets/...``). When unset,
    the verify-URL helpers emit root-relative paths.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "public_url")

    public_base_url: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Externally reachable API root that fronts the catalog service "
            "(scheme + host + any proxy path prefix, no trailing slash). Used "
            "by headless task runners to build absolute result/verify URLs in "
            "job messages. Unset -> helpers emit root-relative paths."
        ),
    )


# --------------------------------------------------------------------------- #
# Message constructors — the one way a task sets its result message.
# --------------------------------------------------------------------------- #


def completed(
    job_id: Union[str, UUID],
    message: str,
    *,
    links: Optional[List[Link]] = None,
) -> StatusInfo:
    """Build the success ``StatusInfo`` a task returns, carrying ``message``.

    The runner persists this verbatim as ``task.outputs``; ``task_to_status_info``
    then surfaces ``message`` on the job status document.
    """
    return StatusInfo(
        jobID=job_id if isinstance(job_id, UUID) else UUID(str(job_id)),
        status=TaskStatusEnum.COMPLETED,
        message=message,
        progress=100,
        links=links or [],
    )


def with_message(data: Dict[str, Any], message: str) -> Dict[str, Any]:
    """Attach the standard ``message`` to a structured ``dict`` output.

    For tasks whose result is data the caller consumes (e.g. ``gdalinfo``'s
    ``{"info": ...}``): keeps the payload and adds a top-level ``message`` so
    the job status document is never blank.
    """
    out = dict(data)
    out["message"] = message
    return out


def reference_result(
    output_id: str,
    href: str,
    *,
    media_type: str,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    """Task ``outputs`` for a single file artifact delivered *by reference*.

    Produces an OGC API - Processes results-document entry: a map keyed by the
    process's declared output id whose value is a ``Link``-shaped qualified
    value (``href`` + media ``type``), as required by Part 1 §7.13
    (``/req/core/job-results-success``, schema ``results.yaml``) for an output
    transmitted with ``transmissionMode: reference``.

    The standard human-facing ``message`` is carried alongside (defaulting to
    the artifact ``href``) so :func:`task_to_status_info` still populates the
    job *status* document.

    Note: ``message`` also lands in ``task.outputs`` alongside the declared
    output, and the results handler (``_handle_job_results`` in the processes
    extension) returns it verbatim by default. For the frozen ``dwh_join``
    customer contract this is intentional — the released integration reads
    ``message``, and the conformant ``result`` link was added additively beside
    it. A strict OGC API - Processes §7.13 results document would carry only
    declared output ids; the results handler projects ``message`` out for
    processes that opt in (new, non-frozen processes — see
    ``_STRICT_RESULTS_PROCESSES`` there) rather than changing this helper's
    output for everyone.
    """
    return {
        "message": message or href,
        output_id: {"href": href, "type": media_type},
    }


# --------------------------------------------------------------------------- #
# File-producing exports — server-owned storage + signed URL by reference.
# --------------------------------------------------------------------------- #


async def server_output_uri(
    catalog_id: str, process_id: str, job_id: str, filename: str
) -> str:
    """Server-derived GCS URI for a process artifact.

    Per OGC API - Processes the server owns result storage, so the output lands
    in the catalog's own bucket under a deterministic, per-job key:
    ``processes/outputs/{process_id}/{job_id}/{filename}``. The location is
    never client-addressed.
    """
    storage = get_protocol(StorageProtocol)
    if storage is None:
        raise RuntimeError(
            "StorageProtocol is not available; cannot resolve the output "
            f"bucket for process '{process_id}'."
        )
    bucket = await storage.get_storage_identifier(catalog_id)
    if not bucket:
        raise RuntimeError(
            f"No storage bucket is provisioned for catalog '{catalog_id}'; "
            f"cannot write the '{process_id}' output."
        )
    return f"gs://{bucket}/processes/outputs/{process_id}/{job_id}/{filename}"


async def signed_result_url(gs_uri: str) -> str:
    """Best-effort 7-day GET signed URL for ``gs_uri``.

    Reuses the shared ``generate_gcs_signed_url`` helper (V4, IAM signing on
    Cloud Run via ``CloudIdentityProtocol``). Falls back to the raw ``gs://``
    URI when signing is unavailable, so a successful export still reports a
    usable location rather than failing after the upload.

    The download URL is signed WITHOUT ``content_type``: pinning it would add
    ``content-type`` to ``X-Goog-SignedHeaders``, forcing every client (e.g. a
    browser GET) to echo a byte-identical ``Content-Type`` request header or be
    rejected with ``MalformedSecurityHeader``. The object's stored MIME — set at
    upload time — is what GCS returns as the response ``Content-Type`` anyway.
    """
    client_provider = get_protocol(CloudStorageClientProtocol)
    if client_provider is None:
        logger.warning(
            "CloudStorageClientProtocol unavailable; returning gs:// URI unsigned."
        )
        return gs_uri
    try:
        signed = await generate_gcs_signed_url(
            gs_uri,
            method="GET",
            expiration=_SIGNED_URL_TTL,
            client_provider=client_provider,
            identity_provider=get_protocol(CloudIdentityProtocol),
            check_exists=True,
        )
    except Exception as e:  # signing must never sink a successful export
        logger.warning(
            "Failed to sign output URI %s: %s; returning gs:// URI.", gs_uri, e
        )
        return gs_uri
    return signed or gs_uri


# --------------------------------------------------------------------------- #
# In-place resource tasks — a public URL to verify the affected resource.
# --------------------------------------------------------------------------- #


async def public_base_url() -> str:
    """Resolve the operator-configured public API root (no trailing slash).

    Returns an empty string when unset/unavailable, so callers build
    root-relative paths instead of guessing a host.
    """
    mgr = get_protocol(ConfigsProtocol)
    if mgr is None:
        return ""
    try:
        cfg = await mgr.get_config(PublicUrlConfig)
    except Exception as e:
        logger.warning("Could not load PublicUrlConfig: %s", e)
        return ""
    base = cfg.public_base_url if isinstance(cfg, PublicUrlConfig) else None
    return base.rstrip("/") if base else ""


async def collection_verify_url(catalog_id: str, collection_id: str) -> str:
    """Public OGC API - Features URL of a collection."""
    base = await public_base_url()
    return f"{base}/features/catalogs/{catalog_id}/collections/{collection_id}"


async def items_verify_url(
    catalog_id: str, collection_id: str, *, asset_id: Optional[str] = None
) -> str:
    """Public items-listing URL for a collection.

    When ``asset_id`` is given, append a best-effort CQL2-text filter on the
    source ``asset_id`` so the link opens the features ingested from that asset;
    without it, the link opens the full collection listing.
    """
    url = f"{await collection_verify_url(catalog_id, collection_id)}/items"
    if asset_id:
        url = f"{url}?filter=" + quote(f"asset_id='{asset_id}'")
    return url


async def asset_verify_url(
    catalog_id: str, collection_id: Optional[str], asset_id: str
) -> str:
    """Public Assets API URL of a single asset (collection- or catalog-scoped)."""
    base = await public_base_url()
    if collection_id:
        return (
            f"{base}/assets/catalogs/{catalog_id}"
            f"/collections/{collection_id}/assets/{asset_id}"
        )
    return f"{base}/assets/catalogs/{catalog_id}/assets/{asset_id}"


def results_link(href: str) -> Link:
    """An OGC ``results`` HATEOAS link for a delivered artifact URL."""
    return Link(
        rel="http://www.opengis.net/def/rel/ogc/1.0/results",
        type="application/json",
        title=LocalizedText(en="job results"),
        href=href,
    )
