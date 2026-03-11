# Copyright 2025 Open Source Robotics Foundation, Inc.
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
from typing import Optional, Set

from rclpy.clock import BaseClock, ClockChange, JumpThreshold, TimeJump
from rclpy.clock_type import ClockType
from rclpy.duration import Duration
from rclpy.exceptions import TimeSourceChangedError


class AsyncClock(BaseClock):
    """Clock with asyncio-compatible sleep support."""

    def __init__(self, *, clock_type: ClockType = ClockType.SYSTEM_TIME) -> None:
        super().__init__(clock_type=clock_type)
        self._pending_sleeps: Set[asyncio.Future] = set()
        self._destroyed = False

    def _destroy(self) -> None:
        """Cancel all pending sleeps. Called by AsyncNode.destroy_node()."""
        self._destroyed = True
        for future in list(self._pending_sleeps):
            future.cancel()

    async def sleep(self, duration_sec: float) -> None:
        """
        Sleep for a duration respecting sim time.

        Cancelled on clock destruction. Raises TimeSourceChangedError if ROS
        time is activated or deactivated during the sleep.
        """
        if self._destroyed:
            raise RuntimeError('Cannot sleep on a destroyed clock')
        if duration_sec <= 0:
            await asyncio.sleep(0)
            return

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        timer_handle: Optional[asyncio.TimerHandle] = None
        target = None

        def _resolve() -> None:
            if not future.done():
                future.set_result(None)

        def _reject() -> None:
            if not future.done():
                future.set_exception(TimeSourceChangedError())

        if self.ros_time_is_active:
            target = self.now() + Duration(nanoseconds=int(duration_sec * 1e9))
        else:
            timer_handle = loop.call_later(duration_sec, _resolve)

        def _on_jump(time_jump: TimeJump) -> None:
            if time_jump.clock_change in (
                ClockChange.ROS_TIME_ACTIVATED,
                ClockChange.ROS_TIME_DEACTIVATED,
            ):
                _reject()
            elif target is not None and self.now() >= target:
                _resolve()

        threshold = JumpThreshold(
            min_forward=Duration(nanoseconds=1),
            min_backward=None,
            on_clock_change=True,
        )
        with self.create_jump_callback(threshold, post_callback=_on_jump):
            self._pending_sleeps.add(future)
            try:
                await future
            finally:
                self._pending_sleeps.discard(future)
                if timer_handle is not None:
                    timer_handle.cancel()
