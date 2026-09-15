# Copyright 2026 Open Source Robotics Foundation, Inc.
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
import os
import socket

import pytest

import rclpy
from rclpy.experimental import AsyncNode
from rclpy.experimental._wakeup_socket import WakeupSocket
from rclpy.qos import HistoryPolicy
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy

from test_msgs.msg import Strings

TEST_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


@pytest.fixture(autouse=True)
def rclpy_context():
    """Initialize and shut down rclpy for each test."""
    with rclpy.init():
        yield


@pytest.mark.asyncio
async def test_subscription_receives_message():
    """Subscription callback fires when a message is published (sync callback)."""
    received = asyncio.Event()
    received_data = []

    def callback(msg):
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
async def test_subscription_burst_all_received():
    """All messages arrive even when the wakeup socket buffer fills up."""
    num_messages = 1000
    qos = QoSProfile(
        reliability=ReliabilityPolicy.RELIABLE,
        history=HistoryPolicy.KEEP_LAST,
        depth=num_messages,
    )
    done = asyncio.Event()
    received = []

    def callback(msg):
        received.append(msg.string_value)
        if len(received) == num_messages:
            done.set()

    async with AsyncNode('test_sub_burst_node') as node:
        pub = node.create_publisher(Strings, '/test_sub_burst_topic', qos)
        node.create_subscription(Strings, '/test_sub_burst_topic', callback, qos)
        await asyncio.sleep(0.1)  # let the reader attach its wakeup socket

        # Publishing from the loop thread keeps the reader from running, so wakeup bytes pile
        # up past the socket buffer and most writes hit EAGAIN.
        for i in range(num_messages):
            pub.publish(Strings(string_value=f'msg_{i}'))

        async with asyncio.timeout(10):
            await done.wait()

    assert received == [f'msg_{i}' for i in range(num_messages)]


def _open_socket_count() -> int:
    count = 0
    for fd in os.listdir('/proc/self/fd'):
        try:
            if os.readlink(f'/proc/self/fd/{fd}').startswith('socket:'):
                count += 1
        except OSError:
            pass
    return count


@pytest.mark.skipif(not os.path.isdir('/proc/self/fd'), reason='needs /proc')
@pytest.mark.asyncio
async def test_subscription_destroy_releases_sockets():
    """Creating and destroying subscriptions leaves no wakeup sockets behind."""
    async with AsyncNode('test_sub_sockets_node') as node:
        # Warm up so sockets the middleware opens lazily are part of the baseline.
        sub = node.create_subscription(
            Strings, '/test_sub_sockets_warmup', lambda msg: None, TEST_QOS)
        await asyncio.sleep(0.1)
        sub.destroy()
        await asyncio.sleep(0.1)
        baseline = _open_socket_count()

        for i in range(100):
            sub = node.create_subscription(
                Strings, f'/test_sub_sockets_{i}', lambda msg: None, TEST_QOS)
            await asyncio.sleep(0.01)  # let the reader open and attach its wakeup socket
            sub.destroy()
        await asyncio.sleep(0.1)

        # A leak would add at least 100 sockets. Allow a little middleware noise.
        assert _open_socket_count() <= baseline + 10


@pytest.mark.asyncio
async def test_wakeup_socket_close_from_other_thread():
    """A byte on the write end sets the event, and close() works from another thread."""
    event = asyncio.Event()
    wakeup = await WakeupSocket.create(event)
    writer = socket.socket(fileno=wakeup.detach_write_end())
    writer.send(b'\x01')

    async with asyncio.timeout(5):
        await event.wait()

    writer.close()
    await asyncio.to_thread(wakeup.close)
    await asyncio.sleep(0.05)  # the transport closes on the loop
