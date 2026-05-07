#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
OGC conformance-class contribution protocol.

Each extension that implements part of an OGC API standard exposes the
conformance URIs it supports as a class attribute. The tools layer
iterates `get_protocols(ConformanceContributor)` to assemble the global
conformance document.
"""

from typing import List, Protocol, runtime_checkable


@runtime_checkable
class ConformanceContributor(Protocol):
    """Producer of OGC conformance URIs.

    Implementors declare a `conformance_uris` attribute (list of URIs).
    `OGCServiceMixin` already provides a compatible default; other
    extensions (Tiles, Maps, Processes, Dimensions, Web) set it directly
    on the class.
    """

    conformance_uris: List[str]
