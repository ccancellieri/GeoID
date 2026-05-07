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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Decorator that auto-opens a managed_transaction when the caller
doesn't supply an existing connection/engine.

Usage::

    from dynastore.modules.db_config.transactional import transactional

    class MyService:
        engine: DbResource

        @transactional()
        async def get_items(self, catalog_id: str, *, db_resource=None):
            # db_resource is guaranteed non-None here
            return await some_query.execute(db_resource, catalog_id=catalog_id)

        @transactional(param="conn", engine_attr="_engine")
        async def do_work(self, *, conn=None):
            return await other_query.execute(conn)
"""

from functools import wraps
from typing import Optional

from .query_executor import managed_transaction


def transactional(
    param: str = "db_resource",
    engine_attr: str = "engine",
):
    """Decorator that wraps an async method in a ``managed_transaction``.

    If the keyword argument named *param* is ``None`` (or absent), the
    decorator opens ``managed_transaction(getattr(self, engine_attr))``
    and passes the resulting connection as that keyword argument.

    If the caller already supplied a non-None value, the method runs
    directly (re-entrant — ``managed_transaction`` handles savepoints).

    Parameters
    ----------
    param : str
        Name of the keyword argument that carries the DB resource
        (commonly ``"db_resource"`` or ``"conn"``).
    engine_attr : str
        Attribute name on ``self`` that holds the fallback engine/resource.
    """

    def decorator(fn):
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            resource = kwargs.get(param)
            if resource is None:
                engine = getattr(self, engine_attr)
                async with managed_transaction(engine) as conn:
                    kwargs[param] = conn
                    return await fn(self, *args, **kwargs)
            return await fn(self, *args, **kwargs)

        return wrapper

    return decorator
