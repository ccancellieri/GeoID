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

"""Protocol for the shared task temp-directory root.

Task workers that need a writable scratch directory (zip extraction, tile
rendering, etc.) call ``TempDirProtocol.mkdtemp()`` rather than calling
``tempfile.mkdtemp()`` directly.  This allows the deployment to supply a
custom root — e.g. the path where a GCSFuse bucket or an NFS share is
mounted — via a registered implementation.  Test environments can redirect
all scratch I/O to a controlled ``tmp_path``.

The protocol owns both the directory root and the naming convention so that
the reaper can glob exactly the right entries without needing to know which
task type created them.

Default: the ``$TMPDIR``-honouring ``tempfile.gettempdir()`` with the shared
``TASK_DIR_PREFIX`` — no registration required for services that run on plain
local disk.
"""

from __future__ import annotations

import tempfile
from typing import Optional, Protocol, runtime_checkable

# Prefix stamped on every task scratch directory regardless of which task
# type created it.  The reaper globs ``<tmpdir>/<TASK_DIR_PREFIX>*`` so a
# single sweep covers zip extractions, tile preseed buffers, and any future
# task that adopts this convention.
TASK_DIR_PREFIX = "dynastore_task_"


@runtime_checkable
class TempDirProtocol(Protocol):
    """Provides the root directory and naming convention for task scratch space."""

    def get_tmpdir(self) -> str:
        """Return the absolute path to the temp-directory root.

        Implementations MUST return an existing, writable directory.
        """
        ...

    def mkdtemp(
        self,
        *,
        task_id: Optional[str] = None,
        task_schema: Optional[str] = None,
    ) -> str:
        """Create and return a new task scratch directory.

        The directory name is built from ``TASK_DIR_PREFIX`` + *task_id* so
        the reaper can attribute it without reading the sidecar first.
        The caller is responsible for writing the ``.owner`` sidecar and for
        cleanup (``shutil.rmtree``) on exit — see the ``finally`` block in
        ``_extract_archive_to_local``.

        Args:
            task_id:     Owning task UUID string (embedded in the dir name).
            task_schema: Physical catalog schema (stored in ``.owner`` sidecar).
        """
        ...


class DefaultTempDir:
    """Built-in implementation: honours ``$TMPDIR`` via ``tempfile.gettempdir()``."""

    def get_tmpdir(self) -> str:
        return tempfile.gettempdir()

    def mkdtemp(
        self,
        *,
        task_id: Optional[str] = None,
        task_schema: Optional[str] = None,
    ) -> str:
        task_suffix = f"{task_id}_" if task_id else ""
        prefix = f"{TASK_DIR_PREFIX}{task_suffix}"
        return tempfile.mkdtemp(prefix=prefix, dir=self.get_tmpdir())
