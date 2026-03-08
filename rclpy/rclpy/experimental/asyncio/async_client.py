import asyncio
from typing import Dict, Optional, Type

from rclpy.client import BaseClient
from rclpy.qos import QoSProfile
from rclpy.type_support import Srv, SrvRequestT, SrvResponseT


class AsyncClient(BaseClient[SrvRequestT, SrvResponseT]):
    """Async client that owns its DDS bridge response loop."""

    def __init__(
        self,
        client_impl: object,
        srv_type: Type[Srv[SrvRequestT, SrvResponseT]],
        srv_name: str,
        qos_profile: QoSProfile,
    ) -> None:
        super().__init__(client_impl, srv_type, srv_name, qos_profile)
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._closing = False
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._read_event = asyncio.Event()

    def _on_new_response(self, _num_waiting: int) -> None:
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._read_event.set)

    async def wait_for_service(self, timeout_sec: Optional[float] = None) -> None:
        """
        Wait for a service server to become ready.

        :param timeout_sec: Seconds to wait. If ``None``, then wait forever.
        :raises asyncio.TimeoutError: If the timeout expires before the service is ready.
        """
        async with asyncio.timeout(timeout_sec):
            while not self.service_is_ready():
                await asyncio.sleep(0.1)

    async def call(self, request: SrvRequestT) -> SrvResponseT:
        """Send a service request and await the response."""
        if self._loop is None:
            raise RuntimeError("Client is not running")
        future: asyncio.Future[SrvResponseT] = self._loop.create_future()
        sequence_number = self.handle.send_request(request)
        self._pending_requests[sequence_number] = future
        try:
            return await future
        finally:
            self._pending_requests.pop(sequence_number, None)

    async def _responses(self):
        """Async generator yielding (header, response) from DDS."""
        with self.handle:
            self.handle.set_on_new_response_callback(self._on_new_response)
            try:
                while not self._closing:
                    header_and_response = self.handle.take_response(
                        self.srv_type.Response)
                    if header_and_response != (None, None):
                        yield header_and_response
                    else:
                        self._read_event.clear()
                        await self._read_event.wait()
            finally:
                self.handle.clear_on_new_response_callback()
                for future in self._pending_requests.values():
                    future.cancel()
                self._pending_requests.clear()
                self.handle.destroy_when_not_in_use()

    async def _run(self) -> None:
        """DDS bridge response loop for clients."""
        self._loop = asyncio.get_running_loop()

        try:
            async for header, response in self._responses():
                future = self._pending_requests.get(
                    header.request_id.sequence_number)
                if future is not None:
                    future.set_result(response)
        finally:
            self._task = None

    async def close(self) -> None:
        """Signal the response loop to stop and cancel pending requests."""
        if self._task is None:
            raise RuntimeError("Entity is not running")
        self._closing = True
        self._read_event.set()
        await self._task
