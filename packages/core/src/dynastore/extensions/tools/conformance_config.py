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

"""PluginConfig governing anonymous access to OGC conformance endpoints.

OGC API Common Part 1 expects ``/conformance`` and per-service
``/{svc}/conformance`` to be readable without authentication — they declare
what the server implements, not any tenant data.  This config controls
whether the platform enforces that expectation (``public=True``, the
default) or reverts to the standard deny-by-default IAM posture
(``public=False``) for deployments that need it.

The flag is evaluated once at boot via ``bootstrap_preset_if_absent`` in
``extensions/web/web.py``; it is not checked per-request.  To change the
posture after boot an operator must update the config, revoke the
``ogc_conformance_public`` preset, and restart.
"""

from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class ConformanceExposureConfig(PluginConfig):
    """Controls anonymous access to OGC conformance and landing-page endpoints.

    When ``public`` is ``True`` (the default) the platform applies the
    ``ogc_conformance_public`` IAM preset at boot, which grants anonymous
    ``GET`` access to every ``/{svc}/conformance`` route (and the equivalent
    landing pages at ``/{svc}/``).  Setting ``public`` to ``False`` skips
    this grant so the deployment's deny-by-default policy applies instead.

    This is a PluginConfig, not an ExposableConfigMixin — it does not gate
    extension routing (no 503).  It governs a single IAM policy declaration.
    """

    _address: ClassVar[Tuple[Optional[str], ...]] = (
        "platform", "extensions", "ogc", "conformance"
    )

    public: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), anonymous principals may read OGC conformance "
            "endpoints (GET /{svc}/conformance, GET /{svc}/) without authentication. "
            "Set to False to restrict conformance to authenticated users."
        ),
    )
