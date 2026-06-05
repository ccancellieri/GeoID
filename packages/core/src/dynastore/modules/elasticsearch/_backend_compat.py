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

"""Backend dialect compatibility for the shared OpenSearch/Elasticsearch client.

opensearch-py is wire-compatible with both servers, but a few *mapping* field
types are dialect-specific. The most load-bearing divergence: the unknown-tail
"extras" lane is mapped as the ``flattened`` type — an **Elasticsearch-only**
type. OpenSearch (2.7+) calls the equivalent ``flat_object``. Creating any index
whose mapping declares ``{"type": "flattened"}`` against an OpenSearch cluster
fails at the mapping boundary with::

    mapper_parsing_exception: No handler for type [flattened] declared on field [extras]

Every canonical mapping builder (items / collections / catalogs / private) emits
``flattened`` because the production backend is Elasticsearch. Rather than fork
each builder or patch each ``indices.create`` / ``indices.put_mapping`` call
site, this module installs a single shim on the client's ``indices`` namespace
when (and only when) the connected cluster reports itself as OpenSearch. The shim
rewrites ``flattened`` → ``flat_object`` in the request body, covering every
current and future index-create path from one place.

The two types are query-compatible for our usage (bare declaration, exact-match
keyword leaves), so the swap is transparent to read/write code.
"""

import copy
import logging
from typing import Any

logger = logging.getLogger(__name__)


def is_opensearch_distribution(info: Any) -> bool:
    """True when an ``await client.info()`` payload reports the OpenSearch
    distribution.

    OpenSearch sets ``version.distribution == "opensearch"``; Elasticsearch has
    no ``distribution`` member (it uses ``build_flavor``). Defensive against
    missing/oddly-shaped payloads — anything we can't positively identify as
    OpenSearch is treated as Elasticsearch (no rewrite).
    """
    if not isinstance(info, dict):
        return False
    version = info.get("version")
    if not isinstance(version, dict):
        return False
    return str(version.get("distribution", "")).lower() == "opensearch"


def rewrite_es_only_types(body: Any) -> Any:
    """Return a copy of an index ``create`` / ``put_mapping`` body with
    Elasticsearch-only mapping types rewritten to their OpenSearch equivalents.

    Currently: ``flattened`` → ``flat_object``. The walk is type-driven (any
    ``{"type": "flattened", ...}`` leaf, at any depth, inside ``mappings`` /
    ``properties`` / ``dynamic_templates`` / nested objects), so it needs no
    knowledge of the surrounding mapping shape.

    A no-op (returns the input unchanged) when nothing matches, so it is safe to
    apply to every body indiscriminately.
    """
    if not _contains_es_only_type(body):
        return body
    out = copy.deepcopy(body)
    _rewrite_in_place(out)
    return out


_ES_ONLY_TYPE_MAP = {"flattened": "flat_object"}


def _contains_es_only_type(node: Any) -> bool:
    if isinstance(node, dict):
        t = node.get("type")
        if isinstance(t, str) and t in _ES_ONLY_TYPE_MAP:
            return True
        return any(_contains_es_only_type(v) for v in node.values())
    if isinstance(node, (list, tuple)):
        return any(_contains_es_only_type(v) for v in node)
    return False


def _rewrite_in_place(node: Any) -> None:
    if isinstance(node, dict):
        t = node.get("type")
        if isinstance(t, str) and t in _ES_ONLY_TYPE_MAP:
            node["type"] = _ES_ONLY_TYPE_MAP[t]
        for v in node.values():
            _rewrite_in_place(v)
    elif isinstance(node, (list, tuple)):
        for v in node:
            _rewrite_in_place(v)


def install_opensearch_mapping_shim(client: Any) -> None:
    """Wrap ``client.indices.create`` / ``client.indices.put_mapping`` so every
    request body is passed through :func:`rewrite_es_only_types`.

    Idempotent — re-installing on an already-shimmed client is a no-op. Only the
    ``body`` kwarg is touched (all internal call sites pass mappings via
    ``body=``); positional/other kwargs are forwarded untouched.
    """
    indices = getattr(client, "indices", None)
    if indices is None:
        return
    if getattr(indices, "_dynastore_mapping_shim", False):
        return

    orig_create = indices.create
    orig_put_mapping = indices.put_mapping

    async def create(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("body"):
            kwargs["body"] = rewrite_es_only_types(kwargs["body"])
        return await orig_create(*args, **kwargs)

    async def put_mapping(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("body"):
            kwargs["body"] = rewrite_es_only_types(kwargs["body"])
        return await orig_put_mapping(*args, **kwargs)

    indices.create = create
    indices.put_mapping = put_mapping
    indices._dynastore_mapping_shim = True
    logger.info(
        "OpenSearch backend detected — installed flattened→flat_object "
        "mapping compatibility shim on index create/put_mapping."
    )
