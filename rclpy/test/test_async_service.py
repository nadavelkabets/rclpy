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

import pytest

import rclpy
from rclpy.experimental import AsyncNode
from rclpy.qos import HistoryPolicy
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy

from test_msgs.srv import BasicTypes as BasicTypesSrv


@pytest.fixture(autouse=True)
def rclpy_context():
    """Initialize and shut down rclpy for each test."""
    with rclpy.init():
        yield


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
@pytest.mark.parametrize('concurrent', [False, True])
async def test_service_callback_exception(concurrent):
    """Exception in service callback propagates as ExceptionGroup."""
    async def bad_handler(request, response):
        raise ValueError('service boom')

    node = AsyncNode('test_svc_exc_node')
    node.create_service(
        BasicTypesSrv, '/test_svc_exc', bad_handler, concurrent=concurrent)
    client_node = AsyncNode('test_svc_exc_client_node')
    client = client_node.create_client(BasicTypesSrv, '/test_svc_exc')

    with pytest.raises(ExceptionGroup) as exc_info:
        async with asyncio.timeout(5):
            async with node, client_node:
                await client.wait_for_service()
                await client.call(BasicTypesSrv.Request())

    assert exc_info.value.subgroup(ValueError)


@pytest.mark.asyncio
@pytest.mark.parametrize('concurrent', [False, True])
async def test_service_destroy_cancels_in_flight_handler(concurrent):
    """destroy_node() during an in-flight handler cancels it without crashing the node."""
    handler_started = asyncio.Event()

    async def slow_handler(request, response):
        handler_started.set()
        await asyncio.Event().wait()
        return response

    async with (
        AsyncNode('test_srv_cancel_srv_node') as srv_node,
        AsyncNode('test_srv_cancel_client_node') as client_node,
    ):
        srv_node.create_service(
            BasicTypesSrv, '/test_srv_cancel_svc',
            slow_handler, concurrent=concurrent)
        client = client_node.create_client(
            BasicTypesSrv, '/test_srv_cancel_svc')

        async with asyncio.timeout(5):
            await client.wait_for_service()
            call_task = asyncio.create_task(
                client.call(BasicTypesSrv.Request()))
            await handler_started.wait()

        srv_node.destroy_node()

        # Service was destroyed mid-handler — no response arrives
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await call_task


def _open_socket_count() -> int:
    count = 0
    for fd in os.listdir('/proc/self/fd'):
        try:
            if os.readlink(f'/proc/self/fd/{fd}').startswith('socket:'):
                count += 1
        except OSError:
            pass
    return count


@pytest.mark.asyncio
async def test_service_burst_all_answered():
    """Every request in a burst is answered and every response reaches its caller."""
    num_calls = 500
    qos = QoSProfile(
        reliability=ReliabilityPolicy.RELIABLE,
        history=HistoryPolicy.KEEP_LAST,
        depth=num_calls,
    )

    def handler(request, response):
        response.int32_value = request.int32_value
        return response

    async with (
        AsyncNode('test_srv_burst_srv_node') as srv_node,
        AsyncNode('test_srv_burst_client_node') as client_node,
    ):
        srv_node.create_service(
            BasicTypesSrv, '/test_srv_burst_svc', handler, qos_profile=qos)
        client = client_node.create_client(
            BasicTypesSrv, '/test_srv_burst_svc', qos_profile=qos)

        async with asyncio.timeout(10):
            await client.wait_for_service()
            # The calls send their requests before the service's reader runs, so request and
            # response wakeup bytes pile up past the socket buffer.
            responses = await asyncio.gather(*(
                client.call(BasicTypesSrv.Request(int32_value=i)) for i in range(num_calls)))

    assert [response.int32_value for response in responses] == list(range(num_calls))


@pytest.mark.skipif(not os.path.isdir('/proc/self/fd'), reason='needs /proc')
@pytest.mark.asyncio
async def test_service_destroy_releases_sockets():
    """Creating and destroying services leaves no wakeup sockets behind."""
    def handler(request, response):
        return response

    async with AsyncNode('test_srv_sockets_node') as node:
        # Warm up so sockets the middleware opens lazily are part of the baseline.
        srv = node.create_service(BasicTypesSrv, '/test_srv_sockets_warmup', handler)
        await asyncio.sleep(0.1)
        srv.destroy()
        await asyncio.sleep(0.1)
        baseline = _open_socket_count()

        for i in range(100):
            srv = node.create_service(BasicTypesSrv, f'/test_srv_sockets_{i}', handler)
            await asyncio.sleep(0.01)  # let the reader open and attach its wakeup socket
            srv.destroy()
        await asyncio.sleep(0.1)

        # A leak would add at least 100 sockets. Allow a little middleware noise.
        assert _open_socket_count() <= baseline + 10
