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
import socket


class _WakeProtocol(asyncio.Protocol):

    def __init__(self, event: asyncio.Event) -> None:
        self._event = event

    def data_received(self, data: bytes) -> None:
        self._event.set()


class WakeupSocket:
    """
    Sets an asyncio.Event from rmw threads without taking the GIL.

    The rmw callback writes one byte to the write end, whose handle is fileno(), and the event
    loop reads the other end. Clear the rmw callback before calling close().
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, transport: asyncio.BaseTransport,
                 rsock: socket.socket, wsock: socket.socket) -> None:
        self._loop = loop
        self._transport = transport
        self._rsock = rsock
        self._wsock = wsock

    @classmethod
    async def create(cls, event: asyncio.Event) -> 'WakeupSocket':
        """Create the socket pair and start reading it on the running loop."""
        loop = asyncio.get_running_loop()
        rsock, wsock = socket.socketpair()
        try:
            wsock.setblocking(False)
            if wsock.family in (socket.AF_INET, socket.AF_INET6):
                # Windows has no native socketpair, and CPython's fallback is loopback TCP.
                wsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            transport, _ = await loop.create_connection(
                lambda: _WakeProtocol(event), sock=rsock)
        except BaseException:
            rsock.close()
            wsock.close()
            raise
        return cls(loop, transport, rsock, wsock)

    def fileno(self) -> int:
        """Return the handle of the write end, for the rmw callback."""
        return self._wsock.fileno()

    def close(self) -> None:
        """Close both ends, from any thread, after the rmw callback has been cleared."""
        self._wsock.close()
        try:
            on_loop = asyncio.get_running_loop() is self._loop
        except RuntimeError:
            on_loop = False
        if on_loop:
            self._transport.close()
            return
        try:
            self._loop.call_soon_threadsafe(self._transport.close)
        except RuntimeError:  # the loop is already closed
            self._rsock.close()
