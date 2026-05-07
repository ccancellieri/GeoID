"""Protocol implemented by plugins that ship example notebooks.

`NotebooksModule.lifespan()` enumerates implementers via
`dynastore.tools.discovery.get_protocols(NotebookContributorProtocol)`
and seeds the returned `NotebookContribution`s into
`notebooks.platform_notebooks` (ON CONFLICT DO NOTHING).
"""
from typing import Protocol, runtime_checkable

from dynastore.modules.notebooks.contribution import NotebookContribution


@runtime_checkable
class NotebookContributorProtocol(Protocol):
    def get_notebooks(self) -> list[NotebookContribution]: ...
