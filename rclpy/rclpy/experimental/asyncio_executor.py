# Copyright 2025 Nadav Elkabets
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
from functools import partial
from sys import stderr
import time
import traceback
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Type
from typing import TypeVar
from typing import Union

from attr import dataclass

from rclpy.client import Client
from rclpy.clock import ClockChange, JumpHandle, JumpThreshold, ROSClock, TimeJump
from rclpy.constants import S_TO_NS
from rclpy.context import Context
from rclpy.duration import Duration
from rclpy.executors import await_or_execute
from rclpy.executors import BaseExecutor
from rclpy.executors import ExternalShutdownException
from rclpy.executors import TracebackType
from rclpy.logging import get_logger
from rclpy.node import Node
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.task import Future
from rclpy.task import Task
from rclpy.time import Time
from rclpy.timer import Timer
from rclpy.utilities import get_default_context

EntityT = TypeVar('EntityT', bound=Union[Subscription, Service, Client])


@dataclass
class NodeEntities:
    tasks: Set[Task] = set()
    subscriptions: Set[Subscription] = set()


class AsyncioExecutor(BaseExecutor):
    def __init__(
        self, loop: Optional[asyncio.AbstractEventLoop] = None,
        *,
        context: Optional[Context] = None
    ) -> None:
        self._loop = loop or self._get_loop()
        self._context = context or get_default_context()
        # self._context.on_shutdown(self.shutdown)
        self._node_to_entities: Dict[Node, NodeEntities] = {}

    def get_nodes(self) -> List['Node']:
        """Return nodes that have been added to this executor."""
        return list(self._node_to_entities.keys())

    @property
    def context(self) -> Context:
        """Get the context associated with the executor."""
        return self._context

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def __enter__(self) -> 'AsyncioExecutor':
        return self

    # def __exit__(
    #     self,
    #     exc_type: Optional[Type[BaseException]],
    #     exc_val: Optional[BaseException],
    #     exc_tb: Optional[TracebackType],
    # ) -> None:
    #     self.shutdown()

    # def shutdown(self, timeout_sec: Optional[float] = None) -> bool:
    #     """Clear all nodes and close the event loop."""
    #     if not self._shutdown_fut.done():
    #         self._shutdown_fut.set_result(None)

    #     self._nodes.clear()
    #     self._update_entities_from_nodes()

    #     while not self._ready_tasks.empty():
    #         self._ready_tasks.get_nowait()

    #     for _ in range(len(self._tasks)):
    #         task = self._tasks.pop()
    #         task.cancel()

    #     return True

    # def __del__(self):
    #     self.shutdown()
    #     if not self._loop.is_closed():
    #         self._loop.close()

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def wake(self) -> None:
        self._update_entities_from_nodes()

    def add_node(self, node: Node) -> bool:
        if node in self._node_to_entities:
            return False

        self._node_to_entities[node] = NodeEntities()
        node.executor = self
        self._update_entities_from_nodes()
        return True

    async def remove_node(self, node: Node) -> None:
        if node not in self._node_to_entities:
            return

        self._update_entities_from_nodes()
        node_tasks = self._node_to_entities[node].tasks
        for task in node_tasks:
            task.cancel()
        del self._node_to_entities[node]
        await asyncio.gather(node_tasks)

    def _update_entities_from_nodes(self) -> None:
        for node, node_entities in self._node_to_entities.items():
            subscriptions = set()
            subscriptions.update(node.subscriptions)

            self._update_entity_set(
                node,
                node_entities.subscriptions,
                subscriptions,
                self._handle_added_subscription,
                self._handle_removed_subscription
            )
    
    def _handle_added_subscription(self, sub: Subscription, node: Node):
        sub.handle.set_on_new_message_callback(partial(self._handle_ready_subscription, sub, node))

    def _handle_removed_subscription(self, sub: Subscription):
        sub.handle.clear_on_new_message_callback()

    def _update_entity_set(
        self,
        node: Node,
        current_set: set[EntityT],
        new_set: set[EntityT],
        new_entity_cb: Callable[[EntityT], None],
        removed_entity_cb: Callable[[EntityT], None],
    ) -> bool:
        added_entities = new_set - current_set
        for entity in added_entities:
            current_set.add(entity)
            entity.handle.__enter__()
            new_entity_cb(entity, node)

        removed_entities = current_set - new_set
        for entity in removed_entities:
            removed_entity_cb(entity)
            entity.handle.__exit__(None, None, None)
            current_set.remove(entity)

        return added_entities or removed_entities

    def _handle_ready_subscription(
            self,
            subscription: Subscription,
            node: Node,
            number_of_events: int
    ) -> None:
        self._loop.call_soon_threadsafe(
            self._handle_ready_entity,
            self._take_subscription,
            subscription,
            node,
            number_of_events
        )
    
    def _handle_ready_entity(
        self,
        take_entity_callback: Callable[[EntityT], Optional[Coroutine]],
        entity: EntityT,
        node: Node,
        number_of_events: int,
    ) -> None:
        if node not in self._node_to_entities:
            return
        
        tasks = self._node_to_entities[node].tasks
        for _ in range(number_of_events):
            callback = take_entity_callback(entity)
            if not callback:
                return
            
            task = self._loop.create_task(callback)
            task.add_done_callback(partial(self._done_callback, entity, node))
            tasks.add(task)

    def _done_callback(
        self,
        entity: EntityT,
        node_tasks: Set[Task],
        task: Task,
    ):
        if task.cancelled():
            return
        
        exc = task.exception()
        if exc:
            logger = get_logger(entity.get_logger_name())
            logger.error(''.join(traceback.format_exception(exc)))
        
        node_tasks.remove(task)
