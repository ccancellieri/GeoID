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

"""Engine registry — resolve ``engine_ref`` strings to their engine kind.

Cycle F.4a foundation for the F.4b-e ref-keyed-storage rewrite:

- ``list_registered_engines()`` enumerates every concrete
  :class:`EngineConfig` subclass currently registered in the
  :class:`TypedModelRegistry`.  Used by the ``/configs/registry``
  surface and admin tooling.

- ``resolve_engine_class(engine_ref)`` maps an operator-facing
  ``engine_ref`` to the engine's ``engine_class`` discriminator.  In
  Cycle F.1's single-instance-per-kind model, the ref equals the
  engine's snake_case class key (e.g. ``"postgresql_engine_config"``
  resolves to ``"postgresql_engine"``); F.4c will widen the resolver
  to consult the per-scope ref-map populated from stored configs so
  operator-chosen ref names like ``"pg_main"`` resolve correctly.

The registry is a lightweight read-only view over the
:class:`TypedModelRegistry`; no caching is needed because it walks
the existing registry once per call.  Test fixtures or admin
endpoints that need bulk lookups can build a dict from
``list_registered_engines()`` themselves.
"""

from __future__ import annotations

from typing import Dict, Optional, Type

from dynastore.modules.db_config.engine_config import EngineConfig
from dynastore.tools.typed_store.registry import TypedModelRegistry


def list_registered_engines() -> Dict[str, Type[EngineConfig]]:
    """Return ``{engine_class_key → concrete EngineConfig subclass}``.

    Walks the :class:`TypedModelRegistry` for every concrete
    :class:`EngineConfig` subclass (the abstract base + intermediate
    bases are filtered out via ``is_abstract_base``).  The returned
    dict is keyed by ``cls.class_key()`` (snake_case of the class
    name, e.g. ``"postgresql_engine_config"``); the engine kind
    discriminator is ``cls.engine_class`` (e.g. ``"postgresql_engine"``).

    Operators that want the ``engine_class`` keying instead can build
    ``{cls.engine_class: cls for cls in list_registered_engines().values()}``.
    """
    out: Dict[str, Type[EngineConfig]] = {}
    for cls in TypedModelRegistry.subclasses_of(EngineConfig):
        if cls.__dict__.get("is_abstract_base", False):
            continue
        if not cls.engine_class:
            # Defensive: F.1's __init_subclass__ enforcement should
            # already have raised TypeError, but a future refactor
            # (or test fixture) may sidestep the check.  Skip the
            # entry rather than silently key on an empty string.
            continue
        out[cls.class_key()] = cls
    return out


def resolve_engine_class(engine_ref: str) -> Optional[str]:
    """Map an ``engine_ref`` to its ``engine_class`` discriminator.

    Returns ``None`` when no registered engine matches the ref.
    Callers MUST treat ``None`` as the negative case (unknown ref) and
    surface a 422 / configuration error rather than fall back to a
    default engine — silently routing an unknown ref to a default
    engine could mask typos and cross-class compatibility violations.

    Resolution path (Cycle F.4a):

    1. Direct lookup against ``list_registered_engines()`` — the ref
       is treated as a ``class_key()``.  This handles the F.1
       single-instance-per-kind contract where every default
       deployment uses ref names equal to engine class keys
       (e.g. ``"postgresql_engine_config"``).

    2. Fallback lookup by ``engine_class`` discriminator — the ref is
       compared against ``cls.engine_class`` (e.g. ``"postgresql_engine"``).
       This covers operator usage that names refs after the engine
       kind directly (also the form F.2's driver-config validator
       uses for ``required_engine_class`` matching).

    F.4c will widen this to consult a per-scope ref-map populated
    from stored configs so operator-chosen ref names
    (e.g. ``"pg_main"``, ``"pg_secondary"``) resolve correctly to
    their declared ``engine_class``.
    """
    if not engine_ref:
        return None

    engines = list_registered_engines()

    # 1. Direct class_key lookup
    cls = engines.get(engine_ref)
    if cls is not None:
        return cls.engine_class

    # 2. engine_class discriminator lookup
    for cls in engines.values():
        if cls.engine_class == engine_ref:
            return cls.engine_class

    return None


__all__ = [
    "list_registered_engines",
    "resolve_engine_class",
]
