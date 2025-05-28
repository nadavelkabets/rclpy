import pytest
import rclpy
import asyncio
from rclpy.task import Future
from rclpy.experimental.asyncio_executor import AsyncioExecutor
from rclpy.node import Node
from std_msgs.msg import String
from unittest.mock import Mock
import time
import math

@pytest.fixture
def rclpy_init():
    rclpy.init()
    yield
    rclpy.shutdown()

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

@pytest.fixture
def test_node(rclpy_init, asyncio_executor):
    node = Node("test_node")
    asyncio_executor.add_node(node)
    yield node
    asyncio_executor.remove_node(node)
    node.destroy_node()

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

def test_wrapped_future_is_cancelled_when_future_is_cancelled(asyncio_executor):
    ros_fut = Future()

    async def test_coro():    
        return await asyncio_executor.wrap_future(ros_fut)

    task = asyncio_executor.create_task(test_coro())
    asyncio_executor.call_soon(ros_fut.cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio_executor.spin_until_future_complete(task)

def test_spin_once_returns_after_callback(asyncio_executor, test_node):
    mock = Mock()
    msg = String(data="test")
    test_node.create_subscription(String, "/test", mock, 10)    
    pub = test_node.create_publisher(String, "/test", 10)
    pub.publish(msg)
    start_time = time.time()
    asyncio_executor.spin_once()
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    mock.assert_called_once_with(msg)

def test_spin_once_returns_after_timeout(asyncio_executor, test_node):
    start_time = time.time()
    asyncio_executor.spin_once(timeout=0.5)
    assert math.isclose(time.time() - start_time, 0.5, abs_tol=0.01)
