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
"""Shared helper for generating GCS V4 signed URLs.

Consolidates the signing logic previously duplicated in ``tiles_storage.py``
and ``bucket_reporter.py`` behind a single async function, and exposes it as
the common primitive used by the asset download process and any other caller
that needs a pre-signed GCS URL (PUT or GET).

Two signing modes are supported:

* **Remote IAM signing** — when a ``CloudIdentityProtocol`` is supplied, the
  service-account email and a fresh access token are passed to
  ``blob.generate_signed_url`` so signing is performed via the IAM API.
  Required on Cloud Run / GCE where no static key file is mounted.
* **Local signing** — when ``identity_provider`` is ``None``, the storage
  client's default credentials must carry a private key.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional, Tuple

from dynastore.models.protocols import CloudStorageClientProtocol, CloudIdentityProtocol
from dynastore.modules.concurrency import run_in_thread

logger = logging.getLogger(__name__)


def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    """Split ``gs://bucket/path/to/blob`` into ``(bucket, blob_path)``.

    Raises ``ValueError`` for any URI that is not a well-formed GCS URI with
    both a bucket and an object path.
    """
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {gs_uri!r}")
    bucket, _, blob = gs_uri.removeprefix("gs://").partition("/")
    if not bucket or not blob:
        raise ValueError(f"Malformed GCS URI: {gs_uri!r}")
    return bucket, blob


async def generate_gcs_signed_url(
    gs_uri: str,
    *,
    method: str,
    expiration: timedelta,
    client_provider: CloudStorageClientProtocol,
    identity_provider: Optional[CloudIdentityProtocol] = None,
    content_type: Optional[str] = None,
    check_exists: bool = False,
) -> Optional[str]:
    """Return a V4 signed URL for ``gs_uri`` or ``None`` if absent.

    Args:
        gs_uri: Target blob as ``gs://bucket/path``.
        method: HTTP method the URL authorizes (``"GET"``, ``"PUT"`` …).
        expiration: How long the URL remains valid.
        client_provider: Supplies the GCS storage client.
        identity_provider: When provided, enables remote IAM signing.
        content_type: Pinned content-type (required by GCS for PUT when
            the client enforces it).
        check_exists: When true, returns ``None`` if the blob is absent
            rather than signing a URL to a non-existent object.
    """
    bucket_name, blob_path = parse_gs_uri(gs_uri)
    storage_client = client_provider.get_storage_client()
    blob = storage_client.bucket(bucket_name).blob(blob_path)

    if check_exists and not await run_in_thread(blob.exists):
        return None

    kwargs: dict = {
        "version": "v4",
        "expiration": expiration,
        "method": method,
    }
    if content_type is not None:
        kwargs["content_type"] = content_type
    if identity_provider is not None:
        kwargs["service_account_email"] = identity_provider.get_account_email()
        kwargs["access_token"] = await identity_provider.get_fresh_token()

    return await run_in_thread(blob.generate_signed_url, **kwargs)
