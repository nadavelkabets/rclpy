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
from contextlib import contextmanager
import math
import time
from typing import Generator
from unittest.mock import Mock

import pytest
import rclpy
from rclpy.executors import AbstractExecutor, ExternalShutdownException
from rclpy.experimental.asyncio_executor import AsyncioExecutor, TaskHandler
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile
from rclpy.task import Future
from std_msgs.msg import String
from test_msgs.srv import BasicTypes


@contextmanager
def asyncio_executor(loop=None) -> Generator[AsyncioExecutor, None, None]:
    executor = AsyncioExecutor(loop)
    yield executor
    executor.shutdown()


@contextmanager
def attach_to_executor(node: Node, executor: AbstractExecutor) -> Generator[None, None, None]:
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
    node = Node('test_node')
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


@pytest.fixture
def task_handler(asyncio_loop) -> Generator[TaskHandler, None, None]:
    handler = TaskHandler(asyncio_loop)
    yield handler
    handler.cancel_all()
    handler.wait_for_pending_tasks_to_finish()


def test_task_handler_calls_exception_handler_with_captured_exception(asyncio_loop, task_handler):
    mock = Mock()
    exception = RuntimeError("err")
    
    async def test_coro():
        raise exception
    
    task = task_handler.create_task(test_coro, mock)
    asyncio_loop.run_until_complete(task)
    mock.assert_called_once_with(exception)


def test_task_handler_raises_cancelled_error_on_cancelled_task(asyncio_loop, task_handler):
    mock = Mock()
    
    async def test_coro():
        await asyncio_loop.create_future()
    
    task = task_handler.create_task(test_coro, mock)
    asyncio_loop.call_soon(task.cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio_loop.run_until_complete(task)
    mock.assert_not_called()


def test_task_handler_task_count_updates_after_immediate_completion(asyncio_loop, task_handler):
    async def immediate_coro():
        return "done"

    assert task_handler.task_count == 0
    task = task_handler.create_task(immediate_coro, exception_handler=lambda e: None)
    asyncio_loop.run_until_complete(task)
    assert task_handler.task_count == 0


def test_wait_for_pending_tasks_to_finish_returns_false_if_still_pending(asyncio_loop, task_handler):
    async def long_coro():
        await asyncio_loop.create_future()

    task = task_handler.create_task(long_coro, exception_handler=lambda e: None)
    assert task_handler.task_count == 1
    result = task_handler.wait_for_pending_tasks_to_finish(timeout_sec=0.01)
    assert result is False
    assert task_handler.task_count == 1


def test_wait_for_pending_tasks_to_finish_returns_true_after_task_finishes(task_handler):
    async def short_coro():
        await asyncio.sleep(0.01)

    task = task_handler.create_task(short_coro, exception_handler=lambda e: None)
    assert task_handler.task_count == 1
    result = task_handler.wait_for_pending_tasks_to_finish(timeout_sec=0.1)
    assert result is True
    assert task_handler.task_count == 0


def test_cancel_all_cancels_every_pending_task_and_decrements_count(asyncio_loop, task_handler):
    async def long_coro():
        await asyncio_loop.create_future()

    task1 = task_handler.create_task(long_coro, exception_handler=lambda e: None)
    task2 = task_handler.create_task(long_coro, exception_handler=lambda e: None)
    assert task_handler.task_count == 2
    task_handler.cancel_all()
    finished = task_handler.wait_for_pending_tasks_to_finish(timeout_sec=0.1)
    assert finished is True
    assert task_handler.task_count == 0


def test_cancelled_task_raises_cancelled_error_and_does_not_call_exception_handler(asyncio_loop, task_handler):
    mock = Mock()

    async def never_ending():
        await asyncio.Future()

    task = task_handler.create_task(never_ending, mock)
    asyncio_loop.call_soon(task.cancel)

    with pytest.raises(asyncio.CancelledError):
        asyncio_loop.run_until_complete(task)

    mock.assert_not_called()


def test_spin_once_returns_after_callback(executor, attached_test_node):
    mock = Mock()
    msg = String(data='test')
    attached_test_node.create_subscription(String, '/test', mock, 10)
    pub = attached_test_node.create_publisher(String, '/test', 10)
    pub.publish(msg)

    start_time = time.time()
    executor.spin_once()
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    mock.assert_called_once_with(msg)


def test_spin_once_returns_after_timeout(executor):
    start_time = time.time()
    executor.spin_once(timeout_sec=0.5)
    assert math.isclose(time.time() - start_time, 0.5, abs_tol=0.01)


def test_executor_executes_existing_callbacks_from_initialized_node(test_node, executor):
    mock = Mock()
    msg = String(data='test')
    test_node.create_subscription(String, '/test', mock, 10)
    pub = test_node.create_publisher(String, '/test', 10)
    pub.publish(msg)
    with attach_to_executor(test_node, executor):
        executor.spin_once()

    mock.assert_called_once_with(msg)


def test_executor_discards_subscription_from_removed_node(test_node, executor):
    mock = Mock()
    msg = String(data='test')
    test_node.create_subscription(String, '/test', mock, 10)
    pub = test_node.create_publisher(String, '/test', 10)

    with attach_to_executor(test_node, executor):
        executor.spin_once(timeout_sec=0.01)

    pub.publish(msg)
    executor.spin_once(timeout_sec=0.01)

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
    executor.spin_until_future_complete(fut, timeout_sec=0.3)
    assert fut.result().string_value == 'True'


def test_transient_local_subscriber_receives_queued_messages(test_node, executor):
    fut = executor.create_future()
    qos = QoSProfile(history=HistoryPolicy.KEEP_ALL, durability=DurabilityPolicy.TRANSIENT_LOCAL)
    pub = test_node.create_publisher(String, '/test', qos)

    for _ in range(5):
        pub.publish(String(data='msg'))

    received = []

    def callback(msg):
        received.append(msg)
        if len(received) == 5:
            fut.set_result(None)

    test_node.create_subscription(String, '/test', callback, qos)

    with attach_to_executor(test_node, executor):
        executor.spin_until_future_complete(fut, timeout_sec=0.5)

    assert fut.done()


def test_service_unavailable_after_node_removed(test_node, executor):
    def service_cb(req, resp):
        resp.bool_value = True
        return resp

    test_node.create_service(BasicTypes, '/test_srv', service_cb)
    client = test_node.create_client(BasicTypes, '/test_srv')

    with attach_to_executor(test_node, executor):
        req = BasicTypes.Request()
        fut = client.call_async(req)
        executor.spin_until_future_complete(fut, timeout_sec=0.3)
        assert fut.result().bool_value is True

    fut2 = client.call_async(BasicTypes.Request())
    executor.spin_once(timeout_sec=0.1)
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
    executor.spin_until_future_complete(future, timeout_sec=0.7)
    assert future.done() and math.isclose(time.time() - start_time, 0.5, abs_tol=0.1)


def test_timer_reset_rewinds_the_timer(attached_test_node, executor):
    future = executor.create_future()

    def _cb():
        future.set_result(None)

    timer = attached_test_node.create_timer(0.5, _cb)
    start_time = time.time()
    executor.spin_until_future_complete(future, timeout_sec=0.3)
    assert not future.done()
    timer.reset()
    executor.spin_until_future_complete(future, timeout_sec=0.3)
    assert not future.done()
    executor.spin_until_future_complete(future, timeout_sec=0.5)
    assert future.done() and math.isclose(time.time() - start_time, 0.8, abs_tol=0.1)


def test_shutdown_context(attached_test_node, executor):
    rclpy.shutdown()
