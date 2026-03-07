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
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._read_event: Optional[asyncio.Event] = None

    def _on_new_response(self, _num_waiting: int) -> None:
        assert self._loop is not None
        assert self._read_event is not None
        self._loop.call_soon_threadsafe(self._read_event.set)

    def wait_for_service(self, timeout_sec: Optional[float] = None) -> bool:
        """
        Wait for a service server to become ready (blocking).

        :param timeout_sec: Seconds to wait. If ``None``, then wait forever.
        :return: ``True`` if server became ready, ``False`` on timeout.
        """
        import time
        sleep_time = 0.25
        if timeout_sec is None:
            timeout_sec = float('inf')
        while not self.service_is_ready() and timeout_sec > 0.0:
            time.sleep(sleep_time)
            timeout_sec -= sleep_time
        return self.service_is_ready()

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

    async def _run(self) -> None:
        """DDS bridge response loop for clients."""
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        self._read_event = asyncio.Event()

        with self.handle:
            self.handle.set_on_new_response_callback(self._on_new_response)
            try:
                while True:
                    header_and_response = self.handle.take_response(
                        self.srv_type.Response)
                    if header_and_response != (None, None):
                        header, response = header_and_response
                        future = self._pending_requests.get(
                            header.request_id.sequence_number)
                        if future is not None:
                            future.set_result(response)
                    else:
                        try:
                            self._read_event.clear()
                            await self._read_event.wait()
                        except asyncio.CancelledError:
                            break
            finally:
                self.handle.clear_on_new_response_callback()
                for future in self._pending_requests.values():
                    future.cancel()
                self._pending_requests.clear()
                self.handle.destroy_when_not_in_use()

    async def close(self) -> None:
        """Cancel the response loop and cancel pending requests."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
