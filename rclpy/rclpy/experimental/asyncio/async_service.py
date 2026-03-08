import asyncio
import inspect
from typing import Any, Callable, Coroutine, Optional, Type

from rclpy.qos import QoSProfile
from rclpy.service import BaseService
from rclpy.type_support import Srv, SrvRequestT, SrvResponseT


class AsyncService(BaseService[SrvRequestT, SrvResponseT]):
    """Async service that owns its DDS bridge read loop."""

    def __init__(
        self,
        service_impl: object,
        srv_type: Type[Srv[SrvRequestT, SrvResponseT]],
        srv_name: str,
        callback: Callable[..., Coroutine],
        qos_profile: QoSProfile,
        concurrent: bool = False,
    ) -> None:
        if not inspect.iscoroutinefunction(callback):
            raise TypeError('AsyncService callback must be an async function')
        super().__init__(service_impl, srv_type, srv_name, qos_profile)
        self.callback = callback
        self._concurrent = concurrent
        self._closing = False
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._read_event = asyncio.Event()

    def _on_new_request(self, _num_waiting: int) -> None:
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._read_event.set)

    async def _handle_request(
        self,
        request: SrvRequestT,
        header: Any,
    ) -> None:
        response = await self.callback(request, self.srv_type.Response())
        self._send_response(response, header)

    async def _requests(self):
        """Async generator yielding (request, header) from DDS."""
        with self.handle:
            self.handle.set_on_new_request_callback(self._on_new_request)
            try:
                while not self._closing:
                    request_and_header = self.handle.service_take_request(
                        self.srv_type.Request)
                    if request_and_header != (None, None):
                        yield request_and_header
                    else:
                        self._read_event.clear()
                        await self._read_event.wait()
            finally:
                self.handle.clear_on_new_request_callback()
                self.handle.destroy_when_not_in_use()

    async def _run(self) -> None:
        """DDS bridge read loop for services."""
        self._loop = asyncio.get_running_loop()

        try:
            if self._concurrent:
                async with asyncio.TaskGroup() as tg:
                    async for request, header in self._requests():
                        tg.create_task(self._handle_request(request, header))
            else:
                async for request, header in self._requests():
                    await self._handle_request(request, header)
        finally:
            self._task = None

    # TODO: calling this from within a service callback will deadlock
    async def close(self) -> None:
        """Signal the read loop to stop and wait for in-flight callbacks."""
        if self._task is None:
            return
        self._closing = True
        self._read_event.set()
        await self._task
