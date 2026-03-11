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
from typing import Optional, Type, Union

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
        self._destroyed = False
        self._task: Optional[asyncio.Task] = None

    def publish(self, msg: Union[MsgT, bytes]) -> None:
        if self._destroyed:
            raise RuntimeError('Publishing on a destroyed publisher is forbidden')
        super().publish(msg)

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        if self._task is not None:
            self._task.cancel()
        super().destroy()

    async def _run(self) -> None:
        try:
            await asyncio.Event().wait()  # wait forever, cancelled on shutdown
        finally:
            self._task = None
            self.destroy()
