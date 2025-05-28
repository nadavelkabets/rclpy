import asyncio
import math
import time
from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from rclpy import Context
from rclpy.executors import ExecutorBase, ExternalShutdownException
from rclpy.experimental.asyncio_executor import AsyncioExecutor
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile
from rclpy.task import Future
from std_msgs.msg import String
from test_msgs.srv import BasicTypes

import rclpy


@contextmanager
def attach_to_executor(node: Node, executor: ExecutorBase):
    executor.add_node(node)
    yield
    executor.remove_node(node)


@pytest.fixture(autouse=True)
def rclpy_init():
    rclpy.init()
    yield
    rclpy.try_shutdown()


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
def test_node():
    node = Node("test_node")
    yield node
    node.destroy_node()


@pytest.fixture
def attached_test_node(test_node, asyncio_executor):
    with attach_to_executor(test_node, asyncio_executor):
        yield test_node


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


def test_spin_once_returns_after_callback(asyncio_executor, attached_test_node):
    mock = Mock()
    msg = String(data="test")
    attached_test_node.create_subscription(String, "/test", mock, 10)
    pub = attached_test_node.create_publisher(String, "/test", 10)
    pub.publish(msg)

    start_time = time.time()
    asyncio_executor.spin_once()
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    mock.assert_called_once_with(msg)


def test_spin_once_returns_after_timeout(asyncio_executor):
    start_time = time.time()
    asyncio_executor.spin_once(timeout=0.5)
    assert math.isclose(time.time() - start_time, 0.5, abs_tol=0.01)


def test_executor_executes_existing_callbacks_from_initialized_node(test_node, asyncio_executor):
    mock = Mock()
    msg = String(data="test")
    test_node.create_subscription(String, "/test", mock, 10)
    pub = test_node.create_publisher(String, "/test", 10)
    pub.publish(msg)
    with attach_to_executor(test_node, asyncio_executor):
        asyncio_executor.spin_once()

    mock.assert_called_once_with(msg)


def test_executor_discards_subscription_from_removed_node(test_node, asyncio_executor):
    mock = Mock()
    msg = String(data="test")
    test_node.create_subscription(String, "/test", mock, 10)
    pub = test_node.create_publisher(String, "/test", 10)

    with attach_to_executor(test_node, asyncio_executor):
        asyncio_executor.spin_once(timeout=0.01)

    pub.publish(msg)
    asyncio_executor.spin_once(timeout=0.01)

    mock.assert_not_called()


def test_executor_attaches_to_running_asyncio_loop(asyncio_loop):
    async def test_coro():
        return AsyncioExecutor()

    task = asyncio_loop.create_task(test_coro())
    asyncio_loop.run_until_complete(task)
    ex = task.result()
    assert asyncio_loop is ex.loop
    ex.shutdown()


def test_basic_service_call(asyncio_executor, attached_test_node):
    def cb(request, response):
        response.string_value = str(request.bool_value)
        return response

    attached_test_node.create_service(BasicTypes, '/test_srv', cb)
    client = attached_test_node.create_client(BasicTypes, '/test_srv')
    fut = client.call_async(BasicTypes.Request(bool_value=True))
    asyncio_executor.spin_until_future_complete(asyncio_executor.wrap_future(fut))
    assert fut.result().string_value == "True"


def test_transient_local_subscriber_receives_queued_messages(test_node, asyncio_executor):
    qos = QoSProfile(history=HistoryPolicy.KEEP_ALL, durability=DurabilityPolicy.TRANSIENT_LOCAL)
    pub = test_node.create_publisher(String, "/test", qos)

    for _ in range(5):
        pub.publish(String(data="msg"))

    received = []
    test_node.create_subscription(String, "/test", received.append, qos)

    with attach_to_executor(test_node, asyncio_executor):
        asyncio_executor.spin_once()

    assert len(received) == 5


def test_service_unavailable_after_node_removed(test_node, asyncio_executor):
    def service_cb(req, resp):
        resp.bool_value = True
        return resp

    test_node.create_service(BasicTypes, "/test_srv", service_cb)
    client = test_node.create_client(BasicTypes, "/test_srv")

    with attach_to_executor(test_node, asyncio_executor):
        req = BasicTypes.Request()
        fut = client.call_async(req)
        asyncio_executor.spin_until_future_complete(asyncio_executor.wrap_future(fut))
        assert fut.result().bool_value is True

    fut2 = client.call_async(BasicTypes.Request())
    asyncio_executor.spin_once(timeout=0.1)
    assert not fut2.done()

def test_spin_returns_if_context_is_not_ok():
    executor = AsyncioExecutor()
    executor.context.shutdown()
    mock = Mock()
    executor.call_soon(mock)
    executor.spin()
    mock.assert_not_called()

def test_executor_crashes_if_context_shuts_down_during_spin():
    executor = AsyncioExecutor()
    executor.call_soon(executor.context.shutdown)
    with pytest.raises(ExternalShutdownException):
        executor.spin()

def test_timer_jumps_when_expected(attached_test_node, asyncio_executor):
    future = asyncio_executor.create_future()
    timer = attached_test_node.create_timer(0.5, lambda: future.set_result(None))
    start_time = time.time()
    asyncio_executor.spin_until_future_complete(future, timeout=0.6)
    assert future.done() and math.isclose(time.time() - start_time, 0.5, abs_tol=0.1) 
