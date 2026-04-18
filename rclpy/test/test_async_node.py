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
from rcl_interfaces.srv import GetLoggerLevels
from rcl_interfaces.srv import GetParameters
from rcl_interfaces.srv import SetParameters

import rclpy
from rclpy.exceptions import TimeSourceChangedError
from rclpy.experimental import AsyncNode
from rclpy.experimental import AsyncTimer
from rclpy.parameter import Parameter
from rclpy.qos import HistoryPolicy
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy
from rclpy.timer import TimerInfo

from test_msgs.msg import BasicTypes
from test_msgs.msg import Strings
from test_msgs.srv import BasicTypes as BasicTypesSrv

from type_description_interfaces.srv import GetTypeDescription

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
    async with AsyncNode('test_lifecycle_node'):
        pass


@pytest.mark.asyncio
async def test_subscription_receives_message():
    """Subscription callback fires when a message is published."""
    received = asyncio.Event()
    received_data = []

    async def callback(msg):
        received_data.append(msg.string_value)
        received.set()

    async with AsyncNode('test_sub_node') as node:
        pub = node.create_publisher(Strings, '/test_sub_topic', TEST_QOS)
        node.create_subscription(Strings, '/test_sub_topic', callback, TEST_QOS)

        pub.publish(Strings(string_value='hello'))

        async with asyncio.timeout(5):
            await received.wait()

        assert received_data == ['hello']


@pytest.mark.asyncio
async def test_client_calls_async_service():
    """Client can call a service hosted by another AsyncNode."""
    async def handler(request, response):
        response.bool_value = not request.bool_value
        response.string_value = 'inverted'
        return response

    async with (
        AsyncNode('test_full_srv_node') as srv_node,
        AsyncNode('test_full_client_node') as client_node,
    ):
        srv_node.create_service(BasicTypesSrv, '/test_full_service', handler)
        client = client_node.create_client(BasicTypesSrv, '/test_full_service')

        async with asyncio.timeout(5):
            await client.wait_for_service()
            response = await client.call(BasicTypesSrv.Request(bool_value=True))

        assert response.bool_value is False
        assert response.string_value == 'inverted'


@pytest.mark.asyncio
async def test_sleep_wall_clock():
    """Sleep completes after the requested duration (wall clock)."""
    async with AsyncNode('test_sleep_node') as node:
        async with asyncio.timeout(5):
            await node.get_clock().sleep(0.1)


@pytest.mark.asyncio
async def test_sleep_cancelled_on_close():
    """Pending sleeps are cancelled when node.destroy_node() is called."""
    async with AsyncNode('test_sleep_cancel_node') as node:
        loop = asyncio.get_running_loop()
        loop.call_soon(node.destroy_node)
        with pytest.raises(asyncio.CancelledError):
            async with asyncio.timeout(5):
                await node.get_clock().sleep(999)


@pytest.mark.asyncio
async def test_sleep_raises_on_clock_change():
    """Wall clock sleep raises TimeSourceChangedError when sim time activates."""
    async with AsyncNode('test_sleep_clock_change_node') as node:
        # Activate sim time — triggers ROS_TIME_ACTIVATED jump callback
        loop = asyncio.get_running_loop()
        loop.call_soon(node.set_parameters, [Parameter(
            'use_sim_time', Parameter.Type.BOOL, True)])

        async with asyncio.timeout(5):
            with pytest.raises(TimeSourceChangedError):
                await node.get_clock().sleep(999)


@pytest.mark.asyncio
async def test_subscription_callback_exception_sequential():
    """Exception in sequential callback propagates as ExceptionGroup."""
    async def bad_callback(msg):
        raise ValueError('boom')

    node = AsyncNode('test_seq_exc_node')
    pub = node.create_publisher(Strings, '/test_seq_exc_topic', TEST_QOS)
    node.create_subscription(
        Strings, '/test_seq_exc_topic', bad_callback, TEST_QOS)
    pub.publish(Strings(string_value='trigger'))

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5):
            await node.run()

    assert exc_info.value.subgroup(ValueError)


@pytest.mark.asyncio
async def test_subscription_callback_exception_concurrent():
    """Exception in concurrent callback propagates as ExceptionGroup."""
    async def bad_callback(msg):
        raise ValueError('concurrent boom')

    node = AsyncNode('test_conc_exc_node')
    pub = node.create_publisher(
        Strings, '/test_conc_exc_topic', TEST_QOS)
    node.create_subscription(
        Strings, '/test_conc_exc_topic', bad_callback, TEST_QOS,
        concurrent=True)
    pub.publish(Strings(string_value='trigger'))

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5):
            await node.run()

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
            Strings, '/test_conc_dispatch_topic', TEST_QOS)
        node.create_subscription(
            Strings, '/test_conc_dispatch_topic', callback, TEST_QOS,
            concurrent=True)

        for i in range(NUM_MESSAGES):
            pub.publish(Strings(string_value=f'msg_{i}'))

        async with asyncio.timeout(5):
            await done.wait()


@pytest.mark.asyncio
async def test_direct_entity_destroy():
    """Destroying a subscription stops message delivery."""
    received = asyncio.Event()

    async def callback(msg):
        received.set()

    async with AsyncNode('test_direct_destroy_node') as node:
        pub = node.create_publisher(Strings, '/test_direct_destroy_topic', TEST_QOS)
        sub = node.create_subscription(
            Strings, '/test_direct_destroy_topic', callback, TEST_QOS)

        pub.publish(Strings(string_value='before'))
        async with asyncio.timeout(5):
            await received.wait()
        received.clear()

        sub.destroy()

        pub.publish(Strings(string_value='after'))
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await received.wait()


@pytest.mark.asyncio
async def test_create_before_aenter():
    """Entities created before entering async context dispatch after entry."""
    received = asyncio.Event()

    async def callback(msg):
        received.set()

    node = AsyncNode('test_create_before_aenter_node')
    pub = node.create_publisher(Strings, '/test_pre_aenter_topic', TEST_QOS)
    node.create_subscription(
        Strings, '/test_pre_aenter_topic', callback, TEST_QOS)

    async with node:
        pub.publish(Strings(string_value='hello'))
        async with asyncio.timeout(5):
            await received.wait()


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
        pub_a = node_a.create_publisher(Strings, '/test_multi_a', TEST_QOS)
        node_b.create_subscription(Strings, '/test_multi_a', callback_a, TEST_QOS)

        pub_b = node_b.create_publisher(Strings, '/test_multi_b', TEST_QOS)
        node_a.create_subscription(Strings, '/test_multi_b', callback_b, TEST_QOS)

        pub_a.publish(Strings(string_value='a'))
        pub_b.publish(Strings(string_value='b'))

        async with asyncio.timeout(5):
            await received_a.wait()
            await received_b.wait()


@pytest.mark.asyncio
async def test_wait_for_service_timeout():
    """Timeout is raised when no service server exists."""
    async with AsyncNode('test_wfs_timeout_node') as node:
        client = node.create_client(BasicTypesSrv, '/nonexistent_service')
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await client.wait_for_service()


@pytest.mark.asyncio
async def test_enable_logger_service():
    """Logger service is reachable over DDS when enabled."""
    async with (
        AsyncNode(
            'test_logger_svc_node', enable_logger_service=True
        ) as srv_node,
        AsyncNode('test_logger_client_node') as client_node,
    ):
        client = client_node.create_client(
            GetLoggerLevels,
            '/test_logger_svc_node/get_logger_levels')

        async with asyncio.timeout(5):
            await client.wait_for_service()

        logger_name = srv_node.get_logger().name
        req = GetLoggerLevels.Request(names=[logger_name])
        async with asyncio.timeout(5):
            resp = await client.call(req)
        assert len(resp.levels) == 1
        assert resp.levels[0].name == logger_name


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


@pytest.mark.asyncio
async def test_timer_fires():
    """Timer callback fires at least once within timeout."""
    count = 0
    fired = asyncio.Event()

    async def callback():
        nonlocal count
        count += 1
        fired.set()

    async with AsyncNode('test_timer_fires_node') as node:
        node.create_timer(0.05, callback)
        async with asyncio.timeout(5):
            await fired.wait()
        assert count >= 1


@pytest.mark.asyncio
async def test_timer_fires_multiple():
    """Timer fires multiple times over a short period."""
    count = 0
    enough = asyncio.Event()

    async def callback():
        nonlocal count
        count += 1
        if count >= 3:
            enough.set()

    async with AsyncNode('test_timer_multi_node') as node:
        node.create_timer(0.05, callback)
        async with asyncio.timeout(5):
            await enough.wait()
        assert count >= 3


@pytest.mark.asyncio
async def test_timer_cancel_and_reset():
    """Cancelled timer stops firing; reset resumes it."""
    count = 0
    fired = asyncio.Event()

    async def callback():
        nonlocal count
        count += 1
        fired.set()

    async with AsyncNode('test_timer_cancel_reset_node') as node:
        timer = node.create_timer(0.05, callback)

        # Wait for first fire
        async with asyncio.timeout(5):
            await fired.wait()
        assert count >= 1

        # Cancel and verify no more fires
        timer.cancel()
        count_at_cancel = count
        await asyncio.sleep(0.15)
        assert count == count_at_cancel

        # Reset and verify it fires again
        fired.clear()
        timer.reset()
        async with asyncio.timeout(5):
            await fired.wait()
        assert count > count_at_cancel


@pytest.mark.asyncio
async def test_timer_destroy():
    """Destroying a timer stops it."""
    count = 0
    fired = asyncio.Event()

    async def callback():
        nonlocal count
        count += 1
        fired.set()

    async with AsyncNode('test_timer_destroy_node') as node:
        timer = node.create_timer(0.05, callback)
        async with asyncio.timeout(5):
            await fired.wait()

        timer.destroy()
        count_at_destroy = count
        await asyncio.sleep(0.15)
        assert count == count_at_destroy


@pytest.mark.asyncio
async def test_timer_callback_exception():
    """Exception in timer callback propagates as ExceptionGroup."""
    async def bad_callback():
        raise ValueError('timer boom')

    node = AsyncNode('test_timer_exc_node')
    node.create_timer(0.05, bad_callback)

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5):
            await node.run()

    assert exc_info.value.subgroup(ValueError)


@pytest.mark.asyncio
async def test_timer_create_before_aenter():
    """Timer created before entering async context fires after entry."""
    fired = asyncio.Event()

    async def callback():
        fired.set()

    node = AsyncNode('test_timer_pre_aenter_node')
    node.create_timer(0.05, callback)

    async with node:
        async with asyncio.timeout(5):
            await fired.wait()


@pytest.mark.asyncio
async def test_timer_introspection():
    """Timer exposes period and cancel state via BaseTimer."""
    fired = asyncio.Event()

    async def callback():
        fired.set()

    async with AsyncNode('test_timer_introspect_node') as node:
        timer = node.create_timer(0.1, callback)
        assert isinstance(timer, AsyncTimer)
        assert timer.timer_period_ns == 0.1 * 1e9
        assert not timer.is_canceled()

        timer.cancel()
        assert timer.is_canceled()

        timer.reset()
        assert not timer.is_canceled()


@pytest.mark.asyncio
async def test_timer_callback_with_info():
    """Timer callback receives TimerInfo when it accepts a parameter."""
    received_info = []
    fired = asyncio.Event()

    async def callback(info: TimerInfo):
        received_info.append(info)
        fired.set()

    async with AsyncNode('test_timer_info_node') as node:
        node.create_timer(0.05, callback)
        async with asyncio.timeout(5):
            await fired.wait()
        assert len(received_info) >= 1
        assert isinstance(received_info[0], TimerInfo)
        assert received_info[0].expected_call_time is not None
        assert received_info[0].actual_call_time is not None


@pytest.mark.asyncio
async def test_run_basic():
    """run() blocks until destroy_node() is called."""
    node = AsyncNode('test_run_node')
    loop = asyncio.get_running_loop()
    loop.call_later(0.1, node.destroy_node)
    async with asyncio.timeout(5):
        await node.run()


@pytest.mark.asyncio
async def test_run_with_callback_shutdown():
    """run() returns when a callback calls destroy_node()."""
    received = asyncio.Event()

    node = AsyncNode('test_run_cb_shutdown_node')

    async def callback(msg):
        received.set()
        node.destroy_node()

    pub = node.create_publisher(Strings, '/test_run_cb_topic', TEST_QOS)
    node.create_subscription(Strings, '/test_run_cb_topic', callback, TEST_QOS)

    loop = asyncio.get_running_loop()
    loop.call_later(0.1, pub.publish, Strings(string_value='stop'))
    async with asyncio.timeout(5):
        await node.run()

    assert received.is_set()


@pytest.mark.asyncio
async def test_run_raises_if_already_running():
    """run() raises if node is already under async with."""
    async with AsyncNode('test_run_conflict_node') as node:
        with pytest.raises(RuntimeError):
            await node.run()


@pytest.mark.asyncio
async def test_type_description_service():
    """Verify type description service responds to requests on AsyncNode."""
    async with (
        AsyncNode('test_type_desc_srv_node') as srv_node,
        AsyncNode('test_type_desc_client_node') as client_node,
    ):
        topic = '/test_type_desc_basic_types'
        srv_node.create_publisher(BasicTypes, topic, 10)

        client = client_node.create_client(
            GetTypeDescription,
            '/test_type_desc_srv_node/get_type_description')

        async with asyncio.timeout(5):
            await client.wait_for_service()

            pub_infos = srv_node.get_publishers_info_by_topic(topic)
            assert len(pub_infos)
            type_hash = str(pub_infos[0].topic_type_hash)

            request = GetTypeDescription.Request(
                type_name='test_msgs/msg/BasicTypes',
                type_hash=type_hash,
                include_type_sources=True)
            response = await client.call(request)

        assert response.successful
        assert (response.type_description.type_description.type_name
                == 'test_msgs/msg/BasicTypes')
        assert len(response.type_sources)


@pytest.mark.asyncio
async def test_graph_discovery_methods():
    """Graph discovery methods are accessible on AsyncNode via BaseNode."""
    async with AsyncNode('test_graph_node', namespace='/test_ns') as node:
        topics = node.get_topic_names_and_types()
        assert isinstance(topics, list)

        services = node.get_service_names_and_types()
        assert isinstance(services, list)

        actions = node.get_action_names_and_types()
        assert isinstance(actions, list)

        names = node.get_node_names()
        assert 'test_graph_node' in names

        fq_names = node.get_fully_qualified_node_names()
        assert '/test_ns/test_graph_node' in fq_names

        names_ns = node.get_node_names_and_namespaces()
        assert any(n == 'test_graph_node' and ns == '/test_ns'
                   for n, ns in names_ns)

        names_ns_enc = node.get_node_names_and_namespaces_with_enclaves()
        assert isinstance(names_ns_enc, list)

        fq_name = node.get_fully_qualified_name()
        assert fq_name == '/test_ns/test_graph_node'


@pytest.mark.asyncio
async def test_count_methods():
    """Count methods work on AsyncNode."""
    async with AsyncNode('test_count_node') as node:
        topic = '/test_count_topic'
        node.create_publisher(Strings, topic, TEST_QOS)
        async def _noop(msg): pass
        node.create_subscription(Strings, topic, _noop, TEST_QOS)
        assert node.count_publishers(topic) == 1
        assert node.count_subscribers(topic) == 1


@pytest.mark.asyncio
async def test_endpoint_info_methods():
    """Endpoint info methods work on AsyncNode."""
    async with AsyncNode('test_endpoint_node') as node:
        node.create_publisher(BasicTypes, '/test_endpoint_topic', TEST_QOS)

        pub_info = node.get_publishers_info_by_topic('/test_endpoint_topic')
        assert len(pub_info) == 1
        assert pub_info[0].node_name == 'test_endpoint_node'

        async def _noop(msg): pass
        node.create_subscription(
            BasicTypes, '/test_endpoint_topic', _noop, TEST_QOS)
        sub_info = node.get_subscriptions_info_by_topic('/test_endpoint_topic')
        assert len(sub_info) == 1
        assert sub_info[0].node_name == 'test_endpoint_node'


@pytest.mark.asyncio
async def test_remote_node_introspection():
    """Remote node introspection methods work on AsyncNode."""
    async with AsyncNode('test_remote_node', namespace='/test_ns') as node:
        pubs = node.get_publisher_names_and_types_by_node(
            'test_remote_node', '/test_ns')
        assert isinstance(pubs, list)

        subs = node.get_subscriber_names_and_types_by_node(
            'test_remote_node', '/test_ns')
        assert isinstance(subs, list)

        svcs = node.get_service_names_and_types_by_node(
            'test_remote_node', '/test_ns')
        assert isinstance(svcs, list)

        clients = node.get_client_names_and_types_by_node(
            'test_remote_node', '/test_ns')
        assert isinstance(clients, list)


@pytest.mark.asyncio
async def test_wait_for_node_async():
    """Async wait_for_node finds an existing node."""
    async with (
        AsyncNode('test_wfn_target', namespace='/test_ns') as _,
        AsyncNode('test_wfn_watcher', namespace='/test_ns') as watcher,
    ):
        async with asyncio.timeout(3.0):
            await watcher.wait_for_node('/test_ns/test_wfn_target')


@pytest.mark.asyncio
async def test_wait_for_node_async_timeout():
    """Async wait_for_node raises TimeoutError for nonexistent node."""
    async with AsyncNode('test_wfn_timeout_node') as node:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.3):
                await node.wait_for_node('nonexistent_node')


@pytest.mark.asyncio
async def test_create_publisher_on_destroyed_node():
    """create_publisher raises RuntimeError on a destroyed node."""
    node = AsyncNode('test_destroyed_pub_node')
    node.destroy_node()
    with pytest.raises(RuntimeError):
        node.create_publisher(Strings, '/topic', TEST_QOS)


@pytest.mark.asyncio
async def test_create_subscription_on_destroyed_node():
    """create_subscription raises RuntimeError on a destroyed node."""
    node = AsyncNode('test_destroyed_sub_node')
    node.destroy_node()

    async def callback(msg):
        pass

    with pytest.raises(RuntimeError):
        node.create_subscription(Strings, '/topic', callback, TEST_QOS)


@pytest.mark.asyncio
async def test_create_client_on_destroyed_node():
    """create_client raises RuntimeError on a destroyed node."""
    node = AsyncNode('test_destroyed_client_node')
    node.destroy_node()
    with pytest.raises(RuntimeError):
        node.create_client(BasicTypesSrv, '/service')


@pytest.mark.asyncio
async def test_create_timer_on_destroyed_node():
    """create_timer raises RuntimeError on a destroyed node."""
    node = AsyncNode('test_destroyed_timer_node')
    node.destroy_node()

    async def callback():
        pass

    with pytest.raises(RuntimeError):
        node.create_timer(1.0, callback)


@pytest.mark.asyncio
async def test_destroy_node_idempotent():
    """Calling destroy_node() twice does not raise."""
    node = AsyncNode('test_double_destroy_node')
    node.destroy_node()
    node.destroy_node()


@pytest.mark.asyncio
async def test_entity_destroy_idempotent():
    """Calling destroy() twice on an entity does not raise."""
    async with AsyncNode('test_entity_double_destroy_node') as node:
        pub = node.create_publisher(Strings, '/topic', TEST_QOS)

        pub.destroy()
        pub.destroy()

        async def callback(msg):
            pass

        sub = node.create_subscription(Strings, '/topic', callback, TEST_QOS)

        sub.destroy()
        sub.destroy()


@pytest.mark.asyncio
async def test_client_call_on_destroyed_client():
    """Calling a destroyed client raises RuntimeError."""
    async with AsyncNode('test_destroyed_call_node') as node:
        client = node.create_client(BasicTypesSrv, '/some_service')
        client.destroy()
        with pytest.raises(RuntimeError):
            await client.call(BasicTypesSrv.Request())


@pytest.mark.asyncio
async def test_client_concurrent_calls():
    """Multiple concurrent calls are correctly demuxed by sequence number."""
    NUM_CALLS = 3
    barrier = asyncio.Barrier(NUM_CALLS)

    async def handler(request, response):
        await barrier.wait()
        return response

    async with (
        AsyncNode('test_conc_call_srv_node') as srv_node,
        AsyncNode('test_conc_call_client_node') as client_node,
    ):
        srv_node.create_service(
            BasicTypesSrv, '/test_conc_call_svc', handler, concurrent=True)
        client = client_node.create_client(
            BasicTypesSrv, '/test_conc_call_svc')

        async with asyncio.timeout(5):
            await client.wait_for_service()
            async with asyncio.TaskGroup() as tg:
                for _ in range(NUM_CALLS):
                    tg.create_task(client.call(BasicTypesSrv.Request()))


@pytest.mark.asyncio
async def test_publish_on_destroyed_publisher():
    """Publishing on a destroyed publisher raises RuntimeError."""
    async with AsyncNode('test_pub_destroyed_node') as node:
        pub = node.create_publisher(Strings, '/topic', TEST_QOS)
        pub.destroy()
        with pytest.raises(RuntimeError):
            pub.publish(Strings(string_value='nope'))


@pytest.mark.asyncio
async def test_subscription_callback_with_message_info():
    """Subscription callback receives MessageInfo when it accepts two params."""
    received = asyncio.Event()
    received_info = []

    async def callback(msg, info):
        received_info.append(info)
        received.set()

    async with AsyncNode('test_sub_info_node') as node:
        pub = node.create_publisher(Strings, '/test_sub_info_topic', TEST_QOS)
        node.create_subscription(
            Strings, '/test_sub_info_topic', callback, TEST_QOS)

        pub.publish(Strings(string_value='hello'))

        async with asyncio.timeout(5):
            await received.wait()

        assert len(received_info) == 1
        assert 'source_timestamp' in received_info[0]
        assert 'received_timestamp' in received_info[0]


@pytest.mark.asyncio
async def test_service_concurrent_dispatch():
    """Concurrent service dispatch handles requests in parallel."""
    NUM_REQUESTS = 3
    barrier = asyncio.Barrier(NUM_REQUESTS)

    async def handler(request, response):
        await barrier.wait()
        response.bool_value = True
        return response

    async with (
        AsyncNode('test_conc_svc_srv_node') as srv_node,
        AsyncNode('test_conc_svc_client_node') as client_node,
    ):
        srv_node.create_service(
            BasicTypesSrv, '/test_conc_svc', handler, concurrent=True)
        client = client_node.create_client(BasicTypesSrv, '/test_conc_svc')

        async with asyncio.timeout(5):
            await client.wait_for_service()
            async with asyncio.TaskGroup() as tg:
                for _ in range(NUM_REQUESTS):
                    tg.create_task(client.call(BasicTypesSrv.Request()))


@pytest.mark.asyncio
async def test_service_callback_exception():
    """Exception in service callback propagates as ExceptionGroup."""
    async def bad_handler(request, response):
        raise ValueError('service boom')

    node = AsyncNode('test_svc_exc_node')
    node.create_service(BasicTypesSrv, '/test_svc_exc', bad_handler)
    client_node = AsyncNode('test_svc_exc_client_node')
    client = client_node.create_client(BasicTypesSrv, '/test_svc_exc')

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5):
            async with node:
                async with client_node:
                    await client.wait_for_service()
                    await client.call(BasicTypesSrv.Request())

    assert exc_info.value.subgroup(ValueError)


@pytest.mark.asyncio
async def test_timer_zero_period():
    """Timer with zero period fires immediately."""
    fired = asyncio.Event()

    async def callback():
        fired.set()

    async with AsyncNode('test_timer_zero_node') as node:
        node.create_timer(0.0, callback)
        async with asyncio.timeout(5):
            await fired.wait()


@pytest.mark.asyncio
async def test_timer_callback_signature_rejected():
    """Timer rejects callbacks with invalid signatures (2+ params)."""
    node = AsyncNode('test_timer_bad_sig_node')

    async def bad_callback(a, b):
        pass

    with pytest.raises(RuntimeError):
        node.create_timer(1.0, bad_callback)

    node.destroy_node()


@pytest.mark.asyncio
async def test_clock_sleep_zero_duration():
    """Sleeping for zero duration returns immediately."""
    async with AsyncNode('test_clock_zero_node') as node:
        async with asyncio.timeout(1):
            await node.get_clock().sleep(0)
            await node.get_clock().sleep(-1.0)


@pytest.mark.asyncio
async def test_clock_sleep_on_destroyed_clock():
    """Sleeping on a destroyed clock raises RuntimeError."""
    node = AsyncNode('test_clock_destroyed_node')
    clock = node.get_clock()
    node.destroy_node()
    with pytest.raises(RuntimeError):
        await clock.sleep(1.0)


@pytest.mark.asyncio
async def test_aexit_destroys_on_exception():
    """Node is properly destroyed even when async-with body raises."""
    node = AsyncNode('test_aexit_exc_node')
    pub = node.create_publisher(Strings, '/topic', TEST_QOS)

    with pytest.raises(ExceptionGroup) as exc_info:
        async with node:
            raise RuntimeError('user error')

    assert exc_info.value.subgroup(RuntimeError)

    # Node should be destroyed — creating entities must fail
    with pytest.raises(RuntimeError):
        node.create_publisher(Strings, '/topic2', TEST_QOS)

    # Publisher should be destroyed — publishing must fail
    with pytest.raises(RuntimeError):
        pub.publish(Strings(string_value='nope'))
