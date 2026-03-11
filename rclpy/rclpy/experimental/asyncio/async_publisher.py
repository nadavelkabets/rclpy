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

from typing import Callable, Optional, Type, Union

from rclpy.publisher import BasePublisher
from rclpy.qos import QoSProfile
from rclpy.type_support import MsgT


class AsyncPublisher(BasePublisher[MsgT]):
    """Async publisher that integrates with AsyncNode lifecycle tracking."""

    def __init__(
        self,
        publisher_impl: object,
        msg_type: Type[MsgT],
        topic: str,
        qos_profile: QoSProfile,
        on_destroy: Callable[['AsyncPublisher'], None],
    ) -> None:
        super().__init__(publisher_impl, msg_type, topic, qos_profile)
        self._on_destroy: Optional[Callable[['AsyncPublisher'], None]] = on_destroy
        self._destroyed = False

    def publish(self, msg: Union[MsgT, bytes]) -> None:
        if self._destroyed:
            raise RuntimeError('Publishing on a destroyed publisher is forbidden')
        super().publish(msg)

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        if self._on_destroy is not None:
            self._on_destroy(self)
            self._on_destroy = None
        super().destroy()
