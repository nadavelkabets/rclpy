import pytest
import asyncio
from rclpy.task import Future
from rclpy.experimental.asyncio_executor import AsyncioExecutor

@pytest.fixture
def asyncio_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

@pytest.fixture
def asyncio_executor(asyncio_loop):
    ex = AsyncioExecutor(asyncio_loop)
    yield ex
    ex.shutdown()

def test_rclpy_future_crashes_asyncio_task():
    async def test_coro():
        await Future()

    with pytest.raises(RuntimeError):
        asyncio.run(test_coro())

def test_wrapped_future_is_done_when_future_is_done(asyncio_executor):
    ros_fut = Future()

    async def test_coro():    
        return await asyncio_executor.wrap_future(ros_fut)

    task = asyncio_executor.create_task(test_coro())
    asyncio_executor.call_soon(ros_fut.set_result, "finished")

    asyncio_executor.spin_until_future_complete(task)
    assert task.result() == "finished"
