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

"""Minimal ASGI fixture app for the gunicorn drain-budget test.

Not part of the application package — a standalone target module the
test spawns via ``gunicorn <this module>:app``. On lifespan shutdown it
writes a sentinel file (path from ``LIFESPAN_SENTINEL_FILE``) so the
test can assert the ASGI shutdown handler actually ran, the same
signal DB engine dispose / BackgroundSupervisor.stop() would need in
the real app (#2924).
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    sentinel = os.environ.get("LIFESPAN_SENTINEL_FILE")
    if sentinel:
        with open(sentinel, "w") as f:
            f.write("shutdown-ran\n")


app = FastAPI(lifespan=lifespan)


@app.get("/slow")
async def slow(seconds: float = 2.0):
    started = os.environ.get("REQUEST_STARTED_SENTINEL_FILE")
    if started:
        with open(started, "w") as f:
            f.write("started\n")
    await asyncio.sleep(seconds)
    return {"slept": seconds}
