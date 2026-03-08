import asyncio
import threading

import pytest
import rclpy
import rclpy.executors
from rclpy.node import Node as SyncNode
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String
from std_srvs.srv import SetBool

from rclpy.experimental import AsyncNode
from rclpy.exceptions import TimeSourceChangedError

TEST_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)

@pytest.fixture(scope="session", autouse=True)
def rclpy_context():
    rclpy.init()
    yield
    rclpy.shutdown()

@pytest.mark.asyncio
async def test_lifecycle():
    """Node creates and destroys cleanly via async context manager."""
    async with AsyncNode("test_lifecycle_node") as node:
        await node.close()


@pytest.mark.asyncio
async def test_subscription_receives_message():
    """Subscription callback fires when a message is published."""
    received = asyncio.Event()
    received_data = []

    async def callback(msg):
        received_data.append(msg.data)
        received.set()

    async with AsyncNode("test_sub_node") as node:
        node.create_subscription(String, "/test_sub_topic", callback, TEST_QOS)

        helper = SyncNode("test_pub_helper")
        pub = helper.create_publisher(String, "/test_sub_topic", TEST_QOS)

        try:
            await asyncio.sleep(0.5)  # DDS discovery
            pub.publish(String(data="hello"))

            async with asyncio.timeout(5):
                await received.wait()

            assert received_data == ["hello"]
        finally:
            helper.destroy_node()
            await node.close()


@pytest.mark.asyncio
async def test_client_calls_service():
    """AsyncNode client.call() sends a request to a sync service node."""
    helper = SyncNode("test_srv_helper")
    executor = rclpy.executors.SingleThreadedExecutor()
    executor.add_node(helper)

    def sync_handler(request, response):
        response.success = request.data
        response.message = "from_sync"
        return response

    helper.create_service(SetBool, "/test_client_service", sync_handler)

    spin_thread = threading.Thread(target=executor.spin, daemon=True)
    spin_thread.start()

    try:
        async with AsyncNode("test_client_node") as node:
            client = node.create_client(SetBool, "/test_client_service")

            try:
                await asyncio.sleep(0.5)  # DDS discovery

                await client.wait_for_service(timeout_sec=5.0)

                async with asyncio.timeout(5):
                    response = await client.call(SetBool.Request(data=True))

                assert response.success is True
                assert response.message == "from_sync"
            finally:
                await node.close()
    finally:
        executor.shutdown()
        spin_thread.join(timeout=5)
        helper.destroy_node()


@pytest.mark.asyncio
async def test_client_calls_async_service():
    """Client can call a service hosted by another AsyncNode."""
    async def handler(request, response):
        response.success = not request.data
        response.message = "inverted"
        return response

    async with AsyncNode("test_full_srv_node") as srv_node:
        srv_node.create_service(SetBool, "/test_full_service", handler)

        async with AsyncNode("test_full_client_node") as client_node:
            client = client_node.create_client(SetBool, "/test_full_service")

            try:
                await asyncio.sleep(0.5)  # DDS discovery

                await client.wait_for_service(timeout_sec=5.0)

                async with asyncio.timeout(5):
                    response = await client.call(SetBool.Request(data=True))

                assert response.success is False
                assert response.message == "inverted"
            finally:
                await client_node.close()
        await srv_node.close()


@pytest.mark.asyncio
async def test_sleep_wall_clock():
    """node.sleep() completes after the requested duration (wall clock)."""
    async with AsyncNode("test_sleep_node") as node:
        try:
            async with asyncio.timeout(5):
                await node.sleep(0.1)
        finally:
            await node.close()


@pytest.mark.asyncio
async def test_sleep_cancelled_on_close():
    """Pending sleeps are cancelled when node.close() is called."""
    async with AsyncNode("test_sleep_cancel_node") as node:
        sleep_task = asyncio.ensure_future(node.sleep(999))
        await asyncio.sleep(0.05)  # let sleep start
        await node.close()
        with pytest.raises(asyncio.CancelledError):
            await sleep_task

@pytest.mark.asyncio
async def test_sleep_raises_on_clock_change():
    """A wall clock sleep raises TimeSourceChangedError when sim time is activated."""
    async with AsyncNode("test_sleep_clock_change_node") as node:
        try:
            sleep_task = asyncio.ensure_future(node.sleep(999))
            await asyncio.sleep(0.05)  # let sleep start

            # Activate sim time — triggers ROS_TIME_ACTIVATED jump callback
            node.set_parameters([Parameter(
                "use_sim_time", Parameter.Type.BOOL, True)])

            async with asyncio.timeout(5):
                with pytest.raises(TimeSourceChangedError):
                    await sleep_task
        finally:
            await node.close()
