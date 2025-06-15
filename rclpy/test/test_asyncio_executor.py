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
from rclpy.constants import S_TO_NS
from rclpy.duration import Duration
from rclpy.executors import AbstractExecutor, ExternalShutdownException
from rclpy.experimental.asyncio_executor import AsyncioClock, AsyncioExecutor
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile
from rclpy.task import Future
from rclpy.time import Time
from test_msgs.msg import Strings
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
    node = Node('test_node', clock=AsyncioClock())
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


def test_spin_once_returns_after_callback(executor, attached_test_node):
    mock = Mock()
    msg = Strings(string_value='test')
    attached_test_node.create_subscription(Strings, '/test', mock, 10)
    pub = attached_test_node.create_publisher(Strings, '/test', 10)
    pub.publish(msg)

    start_time = time.time()
    executor.spin_once(timeout_sec=0.3)
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    mock.assert_called_once_with(msg)


def test_spin_once_returns_after_timeout(executor):
    start_time = time.time()
    executor.spin_once(timeout_sec=0.5)
    assert math.isclose(time.time() - start_time, 0.5, abs_tol=0.01)


def test_executor_executes_existing_callback_from_initialized_node(test_node, executor):
    mock = Mock()
    msg = Strings(string_value='test')
    test_node.create_subscription(Strings, '/test', mock, 10)
    pub = test_node.create_publisher(Strings, '/test', 10)
    pub.publish(msg)
    with attach_to_executor(test_node, executor):
        executor.spin_once(timeout_sec=0.3)

    mock.assert_called_once_with(msg)


def test_executor_discards_subscription_from_removed_node(test_node, executor):
    mock = Mock()
    msg = Strings(string_value='test')
    test_node.create_subscription(Strings, '/test', mock, 10)
    pub = test_node.create_publisher(Strings, '/test', 10)

    with attach_to_executor(test_node, executor):
        executor.spin_once(timeout_sec=0)

    pub.publish(msg)
    executor.spin_once(timeout_sec=0)

    mock.assert_not_called()


def test_executor_attaches_to_running_asyncio_loop(asyncio_loop):
    async def test_coro():
        return AsyncioExecutor()

    task = asyncio_loop.create_task(test_coro())
    ex = asyncio_loop.run_until_complete(task)
    assert asyncio_loop is ex.loop


def test_basic_service_call(executor, attached_test_node):
    def cb(request, response):
        response.string_value = str(request.bool_value)
        return response

    attached_test_node.create_service(BasicTypes, '/test_srv', cb)
    client = attached_test_node.create_client(BasicTypes, '/test_srv')
    fut = client.call_async(BasicTypes.Request(bool_value=True))
    start_time = time.time()
    executor.spin_until_future_complete(fut, timeout_sec=0.3)
    assert fut.done()
    assert fut.result().string_value == 'True'
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.1)


def test_transient_local_subscriber_receives_queued_messages(test_node, executor):
    fut = executor.create_future()
    qos = QoSProfile(history=HistoryPolicy.KEEP_ALL, durability=DurabilityPolicy.TRANSIENT_LOCAL)
    pub = test_node.create_publisher(Strings, '/test', qos)

    for _ in range(5):
        pub.publish(Strings(string_value='test'))

    received = []

    def callback(msg):
        received.append(msg)
        if len(received) == 5:
            fut.set_result(None)

    test_node.create_subscription(Strings, '/test', callback, qos)

    with attach_to_executor(test_node, executor):
        start_time = time.time()
        executor.spin_until_future_complete(fut, timeout_sec=0.5)

    assert fut.done()
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.1)


def test_service_unavailable_after_node_removed(test_node, executor):
    def service_cb(req, resp):
        resp.bool_value = True
        return resp

    test_node.create_service(BasicTypes, '/test_srv', service_cb)
    client = test_node.create_client(BasicTypes, '/test_srv')

    with attach_to_executor(test_node, executor):
        req = BasicTypes.Request()
        fut = client.call_async(req)
        start_time = time.time()
        executor.spin_until_future_complete(fut, timeout_sec=0.3)
        assert fut.result().bool_value is True
        assert math.isclose(time.time() - start_time, 0, abs_tol=0.1)

    fut2 = client.call_async(BasicTypes.Request())
    start_time = time.time()
    executor.spin_until_future_complete(fut2, timeout_sec=0.1)
    assert math.isclose(time.time() - start_time, 0.1, abs_tol=0.01)
    assert not fut2.done()


def test_spin_returns_if_context_is_not_ok():
    with asyncio_executor() as executor:
        mock = Mock()
        executor.create_task(mock)
        executor.context.shutdown()
        executor.spin()
        mock.assert_not_called()


def test_spin_once_raises_if_context_is_closed(executor):
    with pytest.raises(ExternalShutdownException):
        executor.context.handle.shutdown()
        executor.create_task(lambda: None)
        executor.spin_once(timeout_sec=0.3)


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


def test_timer_fires_with_system_time_by_default(attached_test_node, executor):
    future = executor.create_future()

    def cb():
        future.set_result(time.time())

    attached_test_node.create_timer(0.2, cb)
    start = time.time()
    executor.spin_until_future_complete(future, timeout_sec=0.4)
    assert future.done()
    elapsed = future.result() - start
    assert math.isclose(elapsed, 0.2, abs_tol=0.05)


def test_timer_fires_when_ros_time_is_active(attached_test_node, executor):
    attached_test_node.set_parameters([Parameter('use_sim_time', Parameter.Type.BOOL, True)])
    future = executor.create_future()

    def cb():
        future.set_result(None)

    attached_test_node.create_timer(0.3, cb)

    attached_test_node.get_clock().set_ros_time_override(Time(nanoseconds=int(0.25 * S_TO_NS)))
    executor.spin_once(timeout_sec=0.1)
    assert not future.done()

    attached_test_node.get_clock().set_ros_time_override(Time(nanoseconds=int(0.3 * S_TO_NS)))
    executor.spin_until_future_complete(future, timeout_sec=0.1)
    assert future.done()


def test_timer_cancel_prevents_callback(attached_test_node, executor):
    fut = executor.create_future()

    def cb():
        fut.set_result('fired')

    timer = attached_test_node.create_timer(0.1, cb)
    executor.loop.call_soon(timer.cancel)

    executor.spin_until_future_complete(fut, timeout_sec=0.3)
    assert not fut.done()


def test_timer_destroy_prevents_callback(attached_test_node, executor):
    fut = executor.create_future()

    def cb():
        fut.set_result('fired')

    timer = attached_test_node.create_timer(0.1, cb)
    executor.loop.call_soon(attached_test_node.destroy_timer, timer)

    executor.spin_until_future_complete(fut, timeout_sec=0.3)
    assert not fut.done()


def test_sleep_for_async_system_time(asyncio_loop):
    clock = AsyncioClock()

    async def coro():
        start = time.time()
        done = await clock.sleep_for_async(Duration(seconds=0.2))
        return done, time.time() - start

    done, elapsed = asyncio_loop.run_until_complete(coro())
    assert done is True
    assert math.isclose(elapsed, 0.2, abs_tol=0.01)


def test_sleep_until_async_system_time(asyncio_loop):
    clock = AsyncioClock()

    async def coro():
        target = clock.now() + Duration(seconds=0.25)
        start = time.time()
        done = await clock.sleep_until_async(target)
        return done, time.time() - start

    done, elapsed = asyncio_loop.run_until_complete(coro())
    assert done is True
    assert math.isclose(elapsed, 0.25, abs_tol=0.01)


def test_sleep_until_async_respects_ros_time(test_node, executor):
    test_node.set_parameters([Parameter('use_sim_time', Parameter.Type.BOOL, True)])
    clock = test_node.get_clock()
    clock.set_ros_time_override(Time(nanoseconds=0))

    async def sleep_coro():
        target = Time(nanoseconds=int(0.5 * S_TO_NS), clock_type=clock.clock_type)
        await clock.sleep_until_async(target)

    start_time = time.time()
    task = executor.create_task(sleep_coro())
    executor.create_task(
        clock.set_ros_time_override,
        Time(nanoseconds=int(0.5 * S_TO_NS), clock_type=clock.clock_type)
    )

    executor.spin_until_future_complete(task)
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    assert task.done()


def test_sleep_until_async_ros_time(test_node, asyncio_loop):
    test_node.set_parameters([Parameter('use_sim_time', Parameter.Type.BOOL, True)])
    clock = test_node.get_clock()
    clock.set_ros_time_override(Time(nanoseconds=0))

    async def coro():
        target = Time(nanoseconds=int(0.5 * S_TO_NS), clock_type=clock.clock_type)
        fut = asyncio_loop.create_task(clock.sleep_until_async(target))
        assert not fut.done()
        clock.set_ros_time_override(
            Time(nanoseconds=int(0.5 * S_TO_NS), clock_type=clock.clock_type)
        )
        return await fut

    start_time = time.time()
    done = asyncio_loop.run_until_complete(coro())
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    assert done is True


def test_sleep_for_async_ros_time(test_node, executor):
    test_node.set_parameters([Parameter('use_sim_time', Parameter.Type.BOOL, True)])
    clock: AsyncioClock = test_node.get_clock()
    clock.set_ros_time_override(Time(nanoseconds=0, clock_type=clock.clock_type))

    async def coro():
        target = Duration(nanoseconds=0.2 * S_TO_NS)
        fut = asyncio_loop.create_task(clock.sleep_for_async(target))
        assert not fut.done()
        clock.set_ros_time_override(Time(nanoseconds=0.2 * S_TO_NS, clock_type=clock.clock_type))
        await fut

    start_time = time.time()
    task = executor.create_task(coro())
    executor.spin_until_future_complete(task)
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)
    assert task.done()


def test_wrapped_future_is_done_when_future_is_done(executor):
    ros_fut = Future(executor=executor)

    async def test_coro():
        return await executor.wrap_future(ros_fut)

    task = executor.create_task(test_coro())
    executor.loop.call_soon(ros_fut.set_result, 'finished')

    executor.spin_until_future_complete(task, timeout_sec=0.3)
    assert task.result() == 'finished'


def test_wrapped_future_is_cancelled_when_future_is_cancelled(executor):
    ros_fut = Future(executor=executor)

    async def test_coro():
        return await executor.wrap_future(ros_fut)

    task = executor.create_task(test_coro())
    executor.loop.call_soon(ros_fut.cancel)
    executor.spin_until_future_complete(task, timeout_sec=0.3)
    assert task.cancelled()


def test_rclpy_task_can_await_asyncio_task(attached_test_node, executor):
    async def coro():
        await asyncio.sleep(0.01)
        return True

    task = executor.loop.create_task(coro())
    executor.spin_until_future_complete(task, timeout_sec=0.1)
    assert task.done()
    assert task.result()


def test_executor_immediate_shutdown(attached_test_node, executor):
    got_callback = False

    def timer_callback() -> None:
        nonlocal got_callback
        got_callback = True

    attached_test_node.create_timer(1, timer_callback)
    executor.create_task(executor.shutdown)
    start_time = time.time()
    executor.spin()
    assert not got_callback
    assert math.isclose(time.time() - start_time, 0, abs_tol=0.01)


def test_create_task_during_spin(executor):
    future = None

    def func():
        nonlocal future
        future = executor.create_task(lambda: 'Sentinel Result')

    executor.loop.call_later(0.2, func)
    executor.spin_once(timeout_sec=0.3)

    assert future is not None
    assert future.done()
    assert future.result() == 'Sentinel Result'


def test_add_node_wakes_executor(executor, test_node):
    mock = Mock()
    test_node.create_timer(0.2, mock)
    executor.loop.call_later(0.1, executor.add_node, test_node)
    executor.spin_once(timeout_sec=0.3)
    mock.assert_called_once()
