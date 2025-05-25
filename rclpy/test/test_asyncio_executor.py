# Copyright 2025 Nadav Elkabets
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from rclpy.task import Future
import pytest
from rclpy.experimental.asyncio_executor import AsyncioExecutor


@pytest.fixture
def loop():
    l = asyncio.new_event_loop()
    asyncio.set_event_loop(l)

    yield l

    l.close()


def test_asyncio_does_not_crash_awaiting_rclpy_future(loop: asyncio.AbstractEventLoop) -> None:
    f = Future()

    async def coro() -> None:
        return await f

    task = loop.create_task(coro())
    loop.call_soon(f.set_result, True)
    loop.run_until_complete(asyncio.wait_for(task, 1.0))

    assert f.done() and task.done() and task.result()


def test_asyncio_executor_attaches_to_loop(loop):
    ex = AsyncioExecutor(loop)
    assert loop is ex.get_loop()
