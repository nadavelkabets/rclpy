import asyncio
import inspect
from typing import Awaitable, Optional, Type

from rclpy.qos import QoSProfile
from rclpy.subscription import AsyncGenericSubscriptionCallback, BaseSubscription
from rclpy.type_support import MsgT


class AsyncSubscription(BaseSubscription[MsgT]):
    """Async subscription that owns its DDS bridge read loop."""

    def __init__(
        self,
        subscription_impl: object,
        msg_type: Type[MsgT],
        topic: str,
        callback: AsyncGenericSubscriptionCallback[MsgT],
        qos_profile: QoSProfile,
        raw: bool = False,
        concurrent: bool = False,
    ) -> None:
        if not inspect.iscoroutinefunction(callback):
            raise TypeError('AsyncSubscription callback must be an async function')
        super().__init__(subscription_impl, msg_type, topic, qos_profile, raw)
        self._callback = callback
        self._callback_type = self._detect_callback_type(callback)
        self._concurrent = concurrent
        self._destroyed = False
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._read_event = asyncio.Event()

    def _on_new_message(self, _num_waiting: int) -> None:
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._read_event.set)

    @property
    def callback(self) -> AsyncGenericSubscriptionCallback[MsgT]:
        return self._callback

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        if self._task is not None:
            self._task.cancel()
        self.handle.clear_on_new_message_callback()
        super().destroy()

    async def _messages(self):
        """Async generator yielding (msg, msg_info) from DDS."""
        self.handle.set_on_new_message_callback(self._on_new_message)
        while True:
            msg_and_info = self.handle.take_message(
                self.msg_type, self.raw)
            if msg_and_info is not None:
                yield msg_and_info
            else:
                self._read_event.clear()
                await self._read_event.wait()

    def _make_callback(self, msg_and_info: tuple) -> Awaitable[None]:
        """Create a callback coroutine from a (msg, msg_info) tuple."""
        if self._callback_type is BaseSubscription.CallbackType.MessageOnly:
            return self._callback(msg_and_info[0])
        return self._callback(*msg_and_info)

    async def _run(self) -> None:
        """DDS bridge read loop for subscriptions."""
        self._loop = asyncio.get_running_loop()
        try:
            if self._concurrent:
                async with asyncio.TaskGroup() as tg:
                    async for msg_and_info in self._messages():
                        tg.create_task(self._make_callback(msg_and_info))
            else:
                async for msg_and_info in self._messages():
                    await self._make_callback(msg_and_info)
        finally:
            self._task = None
            self.destroy()
