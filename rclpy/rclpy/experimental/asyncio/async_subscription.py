import asyncio
import inspect
from typing import Callable, Coroutine, Optional, Type

from rclpy.qos import QoSProfile
from rclpy.subscription import BaseSubscription
from rclpy.type_support import MsgT


class AsyncSubscription(BaseSubscription[MsgT]):
    """Async subscription that owns its DDS bridge read loop."""

    def __init__(
        self,
        subscription_impl: object,
        msg_type: Type[MsgT],
        topic: str,
        callback: Callable[..., Coroutine],
        qos_profile: QoSProfile,
        raw: bool = False,
    ) -> None:
        if not inspect.iscoroutinefunction(callback):
            raise TypeError('AsyncSubscription callback must be an async function')
        super().__init__(subscription_impl, msg_type, topic, qos_profile, raw)
        self._callback = callback
        self._callback_type = self._detect_callback_type(callback)
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._read_event: Optional[asyncio.Event] = None

    def _on_new_message(self, _num_waiting: int) -> None:
        assert self._loop is not None
        assert self._read_event is not None
        self._loop.call_soon_threadsafe(self._read_event.set)

    @property
    def callback(self) -> Callable[..., Coroutine]:
        return self._callback

    async def _run(self) -> None:
        """DDS bridge read loop for subscriptions."""
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        self._read_event = asyncio.Event()

        with self.handle:
            self.handle.set_on_new_message_callback(self._on_new_message)
            try:
                async with asyncio.TaskGroup() as tg:
                    while True:
                        msg_and_info = self.handle.take_message(
                            self.msg_type, self.raw)
                        if msg_and_info is not None:
                            if self._callback_type is BaseSubscription.CallbackType.MessageOnly:
                                msg_tuple = (msg_and_info[0],)
                            else:
                                msg_tuple = msg_and_info
                            tg.create_task(self._callback(*msg_tuple))
                        else:
                            try:
                                self._read_event.clear()
                                await self._read_event.wait()
                            except asyncio.CancelledError:
                                break  # let in-flight callbacks complete
            finally:
                self.handle.clear_on_new_message_callback()
                self.handle.destroy_when_not_in_use()

    async def close(self) -> None:
        """Cancel the read loop and wait for in-flight callbacks to complete."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
