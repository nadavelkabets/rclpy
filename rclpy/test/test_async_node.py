# Copyright 2024 Open Source Robotics Foundation, Inc.
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
"""Tests for AsyncNode and async entity wrappers."""

import asyncio

import pytest

from rcl_interfaces.msg import Parameter as ParameterMsg
from rcl_interfaces.msg import ParameterType
from rcl_interfaces.msg import ParameterValue
from rcl_interfaces.srv import GetParameters
from rcl_interfaces.srv import SetParameters

import rclpy
from rclpy.exceptions import TimeSourceChangedError
from rclpy.experimental import AsyncNode
from rclpy.parameter import Parameter
from rclpy.qos import HistoryPolicy
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy

from std_msgs.msg import String

from std_srvs.srv import SetBool

TEST_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


@pytest.fixture(scope='session', autouse=True)
def rclpy_context():
    """Initialize and shut down rclpy for the test session."""
    with rclpy.init():
        yield


@pytest.mark.asyncio
async def test_lifecycle():
    """Node creates and destroys cleanly via async context manager."""
    async with AsyncNode('test_lifecycle_node') as node:
        await node.close()


@pytest.mark.asyncio
async def test_subscription_receives_message():
    """Subscription callback fires when a message is published."""
    received = asyncio.Event()
    received_data = []

    async def callback(msg):
        received_data.append(msg.data)
        received.set()

    async with AsyncNode('test_sub_node') as node:
        pub = node.create_publisher(String, '/test_sub_topic', TEST_QOS)
        node.create_subscription(String, '/test_sub_topic', callback, TEST_QOS)

        try:
            pub.publish(String(data='hello'))

            async with asyncio.timeout(5):
                await received.wait()

            assert received_data == ['hello']
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_client_calls_async_service():
    """Client can call a service hosted by another AsyncNode."""
    async def handler(request, response):
        response.success = not request.data
        response.message = 'inverted'
        return response

    async with (
        AsyncNode('test_full_srv_node') as srv_node,
        AsyncNode('test_full_client_node') as client_node,
    ):
        srv_node.create_service(SetBool, '/test_full_service', handler)
        client = client_node.create_client(SetBool, '/test_full_service')

        try:
            async with asyncio.timeout(5):
                await client.wait_for_service()
                response = await client.call(SetBool.Request(data=True))

            assert response.success is False
            assert response.message == 'inverted'
        finally:
            await client_node.close()
            await srv_node.close()


@pytest.mark.asyncio
async def test_sleep_wall_clock():
    """Sleep completes after the requested duration (wall clock)."""
    async with AsyncNode('test_sleep_node') as node:
        try:
            async with asyncio.timeout(5):
                await node.sleep(0.1)
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_sleep_cancelled_on_close():
    """Pending sleeps are cancelled when node.close() is called."""
    async with AsyncNode('test_sleep_cancel_node') as node:
        close_task = asyncio.create_task(node.close())
        try:
            with pytest.raises(asyncio.CancelledError):
                async with asyncio.timeout(5):
                    await node.sleep(999)
        finally:
            await close_task


@pytest.mark.asyncio
async def test_sleep_raises_on_clock_change():
    """Wall clock sleep raises TimeSourceChangedError when sim time activates."""
    async with AsyncNode('test_sleep_clock_change_node') as node:
        try:
            sleep_task = asyncio.ensure_future(node.sleep(999))
            await asyncio.sleep(0.05)  # let sleep start

            # Activate sim time — triggers ROS_TIME_ACTIVATED jump callback
            node.set_parameters([Parameter(
                'use_sim_time', Parameter.Type.BOOL, True)])

            async with asyncio.timeout(5):
                with pytest.raises(TimeSourceChangedError):
                    await sleep_task
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_subscription_callback_exception_sequential():
    """Exception in sequential callback propagates as ExceptionGroup."""
    async def bad_callback(msg):
        raise ValueError('boom')

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5), AsyncNode('test_seq_exc_node') as node:
            pub = node.create_publisher(String, '/test_seq_exc_topic', TEST_QOS)
            node.create_subscription(
                String, '/test_seq_exc_topic', bad_callback, TEST_QOS)

            pub.publish(String(data='trigger'))

    assert exc_info.value.subgroup(ValueError)


@pytest.mark.asyncio
async def test_subscription_callback_exception_concurrent():
    """Exception in concurrent callback propagates as ExceptionGroup."""
    async def bad_callback(msg):
        raise ValueError('concurrent boom')

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5), AsyncNode('test_conc_exc_node') as node:
            pub = node.create_publisher(
                String, '/test_conc_exc_topic', TEST_QOS)
            node.create_subscription(
                String, '/test_conc_exc_topic', bad_callback, TEST_QOS,
                concurrent=True)

            pub.publish(String(data='trigger'))

    assert exc_info.value.subgroup(ValueError)


@pytest.mark.asyncio
async def test_subscription_concurrent_dispatch():
    """Concurrent dispatch runs callbacks in parallel, not sequentially."""
    NUM_MESSAGES = 3
    barrier = asyncio.Barrier(NUM_MESSAGES)
    done = asyncio.Event()

    async def callback(msg):
        await barrier.wait()
        done.set()

    async with AsyncNode('test_conc_dispatch_node') as node:
        pub = node.create_publisher(
            String, '/test_conc_dispatch_topic', TEST_QOS)
        node.create_subscription(
            String, '/test_conc_dispatch_topic', callback, TEST_QOS,
            concurrent=True)

        try:
            for i in range(NUM_MESSAGES):
                pub.publish(String(data=f'msg_{i}'))

            async with asyncio.timeout(5):
                await done.wait()
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_direct_entity_close():
    """Calling entity.close() directly removes it from the node entity set."""
    async def callback(msg):
        pass

    async with AsyncNode('test_direct_close_node') as node:
        sub = node.create_subscription(
            String, '/test_direct_close_topic', callback, TEST_QOS)
        assert sub in node._subscriptions

        await sub.close()
        assert sub not in node._subscriptions

        await node.close()


@pytest.mark.asyncio
async def test_create_before_aenter_raises():
    """Creating entities before entering async context raises RuntimeError."""
    node = AsyncNode('test_no_ctx_node')

    async def noop(msg):
        pass

    with pytest.raises(RuntimeError, match='Node context manager not active'):
        node.create_subscription(String, '/unused', noop, TEST_QOS)

    with pytest.raises(RuntimeError, match='Node context manager not active'):
        node.create_publisher(String, '/unused', TEST_QOS)

    with pytest.raises(RuntimeError, match='Node context manager not active'):
        node.create_service(SetBool, '/unused', noop)

    with pytest.raises(RuntimeError, match='Node context manager not active'):
        node.create_client(SetBool, '/unused')

    node.handle.destroy_when_not_in_use()


@pytest.mark.asyncio
async def test_multiple_entities_two_nodes():
    """Two nodes each with a publisher and subscription communicate cross-node."""
    received_a = asyncio.Event()
    received_b = asyncio.Event()

    async def callback_a(msg):
        received_a.set()

    async def callback_b(msg):
        received_b.set()

    async with (
        AsyncNode('test_multi_node_a') as node_a,
        AsyncNode('test_multi_node_b') as node_b,
    ):
        pub_a = node_a.create_publisher(String, '/test_multi_a', TEST_QOS)
        node_b.create_subscription(String, '/test_multi_a', callback_a, TEST_QOS)

        pub_b = node_b.create_publisher(String, '/test_multi_b', TEST_QOS)
        node_a.create_subscription(String, '/test_multi_b', callback_b, TEST_QOS)

        try:
            pub_a.publish(String(data='a'))
            pub_b.publish(String(data='b'))

            async with asyncio.timeout(5):
                await received_a.wait()
                await received_b.wait()
        finally:
            await node_a.close()
            await node_b.close()


@pytest.mark.asyncio
async def test_wait_for_service_timeout():
    """Timeout is raised when no service server exists."""
    async with AsyncNode('test_wfs_timeout_node') as node:
        client = node.create_client(SetBool, '/nonexistent_service')
        try:
            with pytest.raises(TimeoutError):
                await client.wait_for_service(timeout_sec=0.5)
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_enable_logger_service():
    """Logger service creation path works with enable_logger_service=True."""
    async with AsyncNode(
        'test_logger_svc_node', enable_logger_service=True
    ) as node:
        try:
            # 6 ParameterService + 2 LoggingService = at least 8
            assert len(node._services) >= 8
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_parameter_service_over_dds():
    """Get and set parameters over DDS between two AsyncNodes."""
    async with (
        AsyncNode(
            'test_param_srv_node',
            allow_undeclared_parameters=True,
        ) as srv_node,
        AsyncNode('test_param_client_node') as client_node,
    ):
        srv_node.declare_parameter('my_param', 42)

        get_client = client_node.create_client(
            GetParameters, '/test_param_srv_node/get_parameters')
        set_client = client_node.create_client(
            SetParameters, '/test_param_srv_node/set_parameters')

        try:
            async with asyncio.timeout(5):
                await get_client.wait_for_service()
                await set_client.wait_for_service()

            get_req = GetParameters.Request(names=['my_param'])
            async with asyncio.timeout(5):
                get_resp = await get_client.call(get_req)
            assert len(get_resp.values) == 1
            assert get_resp.values[0].integer_value == 42
            assert get_resp.values[0].type == ParameterType.PARAMETER_INTEGER

            set_req = SetParameters.Request(parameters=[
                ParameterMsg(
                    name='my_param',
                    value=ParameterValue(
                        type=ParameterType.PARAMETER_INTEGER,
                        integer_value=99,
                    ),
                ),
            ])
            async with asyncio.timeout(5):
                set_resp = await set_client.call(set_req)
            assert len(set_resp.results) == 1
            assert set_resp.results[0].successful is True

            async with asyncio.timeout(5):
                get_resp2 = await get_client.call(get_req)
            assert get_resp2.values[0].integer_value == 99
        finally:
            await client_node.close()
            await srv_node.close()
