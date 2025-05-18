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
