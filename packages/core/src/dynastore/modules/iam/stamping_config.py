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

"""Per-collection attribute-stamping policy config.

:class:`AttributeStampingPolicy` is a :class:`~dynastore.modules.db_config.plugin_config.PluginConfig`
scoped to ``(catalog_id, collection_id)``.  When present it declares which
Feature properties to lift onto the document's ``_attrs`` envelope at write
time.  The ``_attrs`` envelope is then queried by the read-filter compiler to
enforce per-document ABAC restrictions from :data:`iam.grants.attribute_predicates`.

Missing config (collection not enrolled in ABAC stamping) ⟹ no ``_attrs`` key
is written — byte-for-byte today's behaviour except that ``_grant_subjects``
is also no longer written (retired by #1441).

Example config:

    PUT /configs/catalogs/{cat}/collections/{col}/plugins/attribute_stamping_policy
    {
        "attribute_paths": {
            "dept":        "$.properties.department",
            "sensitivity": "$.properties.classification"
        }
    }

Each key in ``attribute_paths`` becomes ``_attrs.<key>`` on the stored
document; the value is a simple property path of the form
``$.properties.<field_name>``.  Only direct ``properties`` sub-keys are
supported in this first slice; nested paths and array selectors are deferred.
"""
from __future__ import annotations

from typing import ClassVar, Dict, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

__all__ = ["AttributeStampingPolicy"]


class AttributeStampingPolicy(PluginConfig):
    """Per-collection policy for write-time attribute stamping.

    When ``attribute_paths`` is non-empty, :meth:`ItemService._resolve_access_envelope`
    extracts each declared path from the Feature and populates ``_attrs`` on
    the index payload.  Drivers that do not understand ``_attrs`` ignore it.

    Activate via::

        PUT /configs/catalogs/{cat}/collections/{col}/plugins/attribute_stamping_policy
        {"attribute_paths": {"dept": "$.properties.department"}}
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "collection",
        "attribute_stamping_policy",
    )
    _freeze_at: ClassVar[Optional[str]] = "collection"

    attribute_paths: Mutable[Dict[str, str]] = Field(
        default_factory=dict,
        description=(
            "Map of envelope attribute key → JSONPath to the Feature property. "
            "Only ``$.properties.<field>`` paths are supported in this slice. "
            "Example: {\"dept\": \"$.properties.department\"}. "
            "Missing config or empty dict = no attribute stamping."
        ),
    )
