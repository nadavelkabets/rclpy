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
from typing import Generator

import rclpy

@contextmanager
def asyncio_executor(loop=None) -> Generator[AsyncioExecutor, None, None]:
    executor = AsyncioExecutor(loop)
    yield executor
    executor.shutdown()

@contextmanager
def attach_to_executor(node: Node, executor: ExecutorBase) -> Generator[None, None, None]:
    executor.add_node(node)
    yield
    executor.remove_node(node)


@pytest.fixture(autouse=True)
def rclpy_init() -> Generator[None, None, None]:
    rclpy.init()
    yield
    rclpy.try_shutdown()


@pytest.fixture
def asyncio_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    if not loop.is_closed():
        loop.close()


@pytest.fixture
def executor(asyncio_loop) -> Generator[AsyncioExecutor, None, None]:
    with asyncio_executor(asyncio_loop) as ex:
        yield ex


@pytest.fixture
def test_node() -> Generator[Node, None, None]:
    node = Node("test_node")
    yield node
    node.destroy_node()


@pytest.fixture
def attached_test_node(test_node, executor) -> Generator[Node, None, None]:
    with attach_to_executor(test_node, executor):
        yield test_node


def test_rclpy_future_crashes_asyncio_task():
    async def test_coro():
        await Future()

    with pytest.raises(RuntimeError):
        asyncio.run(test_coro())


def test_wrapped_future_is_done_when_future_is_done(executor):
    ros_fut = Future()

    async def test_coro():
        return await executor.create_future(from_future=ros_fut)

    task = executor.create_task(test_coro())
    executor.loop.call_soon(ros_fut.set_result, "finished")

    executor.spin_until_future_complete(task, timeout=0.3)
    assert task.result() == "finished"


def test_wrapped_future_is_cancelled_when_future_is_cancelled(executor):
    ros_fut = Future()

    async def test_coro():
        return await executor.create_future(from_future=ros_fut)

    task = executor.create_task(test_coro())
    executor.loop.call_soon(ros_fut.cancel)
    executor.spin_until_future_complete(task, timeout=0.3)
    assert task.cancelled()


def test_spin_once_returns_after_callback(executor, attached_test_node):
    mock = Mock()
    msg = String(data="test")
    attached_test_node.create_subscription(String, "/test", mock, 10)
    pub = attached_test_node.create_publisher(String, "/test", 10)
    pub.publish(msg)

    start_time = time.time()
    executor.spin_once()
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    mock.assert_called_once_with(msg)


def test_spin_once_returns_after_timeout(executor):
    start_time = time.time()
    executor.spin_once(timeout=0.5)
    assert math.isclose(time.time() - start_time, 0.5, abs_tol=0.01)


def test_executor_executes_existing_callbacks_from_initialized_node(test_node, executor):
    mock = Mock()
    msg = String(data="test")
    test_node.create_subscription(String, "/test", mock, 10)
    pub = test_node.create_publisher(String, "/test", 10)
    pub.publish(msg)
    with attach_to_executor(test_node, executor):
        executor.spin_once()

    mock.assert_called_once_with(msg)


def test_executor_discards_subscription_from_removed_node(test_node, executor):
    mock = Mock()
    msg = String(data="test")
    test_node.create_subscription(String, "/test", mock, 10)
    pub = test_node.create_publisher(String, "/test", 10)

    with attach_to_executor(test_node, executor):
        executor.spin_once(timeout=0.01)

    pub.publish(msg)
    executor.spin_once(timeout=0.01)

    mock.assert_not_called()


def test_executor_attaches_to_running_asyncio_loop(asyncio_loop):
    async def test_coro():
        return AsyncioExecutor()

    task = asyncio_loop.create_task(test_coro())
    asyncio_loop.run_until_complete(task)
    ex = task.result()
    assert asyncio_loop is ex.loop
    ex.shutdown()


def test_basic_service_call(executor, attached_test_node):
    def cb(request, response):
        response.string_value = str(request.bool_value)
        return response

    attached_test_node.create_service(BasicTypes, '/test_srv', cb)
    client = attached_test_node.create_client(BasicTypes, '/test_srv')
    fut = client.call_async(BasicTypes.Request(bool_value=True))
    executor.spin_until_future_complete(executor.create_future(from_future=fut), timeout=0.3)
    assert fut.result().string_value == "True"


def test_transient_local_subscriber_receives_queued_messages(test_node, executor):
    fut = executor.create_future()
    qos = QoSProfile(history=HistoryPolicy.KEEP_ALL, durability=DurabilityPolicy.TRANSIENT_LOCAL)
    pub = test_node.create_publisher(String, "/test", qos)

    for _ in range(5):
        pub.publish(String(data="msg"))

    received = []

    def callback(msg):
        received.append(msg)
        if len(received) == 5:
            fut.set_result(None)

    test_node.create_subscription(String, "/test", callback, qos)

    with attach_to_executor(test_node, executor):
        executor.spin_until_future_complete(fut, timeout=0.5)
    
    assert fut.done()


def test_service_unavailable_after_node_removed(test_node, executor):
    def service_cb(req, resp):
        resp.bool_value = True
        return resp

    test_node.create_service(BasicTypes, "/test_srv", service_cb)
    client = test_node.create_client(BasicTypes, "/test_srv")

    with attach_to_executor(test_node, executor):
        req = BasicTypes.Request()
        fut = client.call_async(req)
        executor.spin_until_future_complete(executor.create_future(from_future=fut), timeout=0.3)
        assert fut.result().bool_value is True

    fut2 = client.call_async(BasicTypes.Request())
    executor.spin_once(timeout=0.1)
    assert not fut2.done()

def test_spin_returns_if_context_is_not_ok():
    with asyncio_executor() as executor:
        mock = Mock()
        executor.loop.call_soon(mock)
        executor.context.shutdown()
        executor.spin()
        mock.assert_not_called()

def test_executor_crashes_if_context_shuts_down_during_spin():
    with asyncio_executor() as executor:
        executor.loop.call_soon(executor.context.shutdown)
        with pytest.raises(ExternalShutdownException):
            executor.spin()

def test_timer_jumps_when_expected(attached_test_node, executor):
    future = executor.create_future()
    def _cb():
        future.set_result(None)
    attached_test_node.create_timer(0.5, _cb)
    start_time = time.time()
    executor.spin_until_future_complete(future, timeout=0.7)
    assert future.done() and math.isclose(time.time() - start_time, 0.5, abs_tol=0.1) 

def test_timer_reset_rewinds_the_timer(attached_test_node, executor):
    future = executor.create_future()
    def _cb():
        future.set_result(None)

    timer = attached_test_node.create_timer(0.5, _cb)
    start_time = time.time()
    executor.spin_until_future_complete(future, timeout=0.3)
    assert not future.done()
    timer.reset()
    executor.spin_until_future_complete(future, timeout=0.3)
    assert not future.done()
    executor.spin_until_future_complete(future, timeout=0.5)
    assert future.done() and math.isclose(time.time() - start_time, 0.8, abs_tol=0.1)

def test_shutdown_context(attached_test_node, executor):
    rclpy.shutdown()
