import asyncio
from typing import Optional, Type

from rclpy.publisher import BasePublisher
from rclpy.qos import QoSProfile
from rclpy.type_support import MsgT


class AsyncPublisher(BasePublisher[MsgT]):
    """Async publisher that integrates with AsyncNode's structured concurrency."""

    def __init__(
        self,
        publisher_impl: object,
        msg_type: Type[MsgT],
        topic: str,
        qos_profile: QoSProfile,
    ) -> None:
        super().__init__(publisher_impl, msg_type, topic, qos_profile)
        self._task: Optional[asyncio.Task] = None
        self._closing = False
        self._close_event = asyncio.Event()

    async def _run(self) -> None:
        """Wait for close signal, then destroy the handle."""
        with self.handle:
            try:
                await self._close_event.wait()
            finally:
                self._task = None
                self.handle.destroy_when_not_in_use()

    async def close(self) -> None:
        """Signal the publisher to shut down."""
        if self._task is None:
            raise RuntimeError("Entity is not running")
        self._closing = True
        self._close_event.set()
        await self._task
