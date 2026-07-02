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

"""Shared CORE-metadata DDL fragment: the ``title`` / ``description``
column pair that every CORE-metadata table hand-copies (platform + tenant
notebooks, catalog/collection core).

One copy — ``{schema}.notebooks`` in
:mod:`dynastore.modules.notebooks.notebooks_db` — had drifted to a
nullable ``title`` even though every model that writes to it
(``NotebookBase.title``, inherited by both ``NotebookCreate`` and
``PlatformNotebookCreate``) declares ``title`` as required. The
catalog/collection CORE tables genuinely allow a missing title
(``LocalizedFieldsBase.title`` is ``Optional`` — STAC collections are not
required to have one), so their nullability was already correct.

Primary keys, foreign keys, and every other column stay inline in the
owning module; only the two lines that disagreed are centralized here.
Lives in ``db_config`` (not ``catalog`` or ``notebooks``) because
``catalog`` already imports from ``notebooks`` (platform notebook
registration) — placing this fragment in either module would create a
cycle for the other.
"""


def core_metadata_columns(*, title_required: bool) -> str:
    """Return the ``title``/``description`` column pair as raw DDL text.

    Splice the result into a ``CREATE TABLE`` body with ``%s`` string
    formatting (not an f-string/``.format()``) so any ``{schema}``-style
    template placeholder elsewhere in the surrounding DDL is left intact
    for ``DDLQuery`` to resolve at execution time.
    """
    title_clause = "title JSONB NOT NULL," if title_required else "title JSONB,"
    return f"{title_clause}\n    description JSONB,"
