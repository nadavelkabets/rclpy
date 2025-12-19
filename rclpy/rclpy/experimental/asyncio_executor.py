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
import traceback
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import TypeVar
from typing import Union

from rclpy.client import Client
from rclpy.context import Context
from rclpy.executors import BaseExecutor
from rclpy.node import Node
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.utilities import get_default_context

EntityT = TypeVar('EntityT', bound=Union[Subscription, Service, Client])


class AsyncioExecutor(BaseExecutor):
    def __init__(
        self, loop: Optional[asyncio.AbstractEventLoop] = None,
        *,
        context: Optional[Context] = None
    ) -> None:
        self._loop = loop or self._get_loop()
        self._context = context or get_default_context()
        # self._context.on_shutdown(self.shutdown)
        self._nodes: Set['Node'] = set()
        self._subscription_to_node: Dict[Subscription, Node] = {}
        self._node_to_tasks: Dict[Node, Set[asyncio.Task]] = {}

    def get_nodes(self) -> List['Node']:
        """Return nodes that have been added to this executor."""
        return list(self._nodes)

    @property
    def context(self) -> Context:
        """Get the context associated with the executor."""
        return self._context

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    async def __enter__(self) -> 'AsyncioExecutor':
        return self

    # async def __exit__(
    #     self,
    #     exc_type: Optional[Type[BaseException]],
    #     exc_val: Optional[BaseException],
    #     exc_tb: Optional[TracebackType],
    # ) -> None:
    #     await self.shutdown()

    # async def shutdown(self, timeout_sec: Optional[float] = None) -> bool:
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
        if node in self._nodes:
            return False

        self._nodes.add(node)
        self._node_to_tasks[node] = set()
        node.executor = self
        self._update_entities_from_nodes()
        return True

    async def remove_node(self, node: Node) -> None:
        if node not in self._nodes:
            return

        self._nodes.remove(node)
        self._update_entities_from_nodes()

        node_tasks = self._node_to_tasks.pop(node)
        for task in node_tasks:
            task.cancel()
        if node_tasks:
            await asyncio.gather(*node_tasks, return_exceptions=True)

    def _update_entities_from_nodes(self) -> None:
        new_subscriptions: Dict[Subscription, Node] = {}
        for node in self._nodes:
            new_subscriptions.update({sub: node for sub in node.subscriptions})

        self._update_entity_set(
            self._subscription_to_node,
            new_subscriptions,
            self._handle_added_subscription,
            self._handle_removed_subscription
        )
    
    def _handle_added_subscription(self, sub: Subscription, node: Node):
        sub.handle.set_on_new_message_callback(
            partial(
                self._loop.call_soon_threadsafe,
                self._handle_ready_entity,
                self._take_subscription,
                sub,
                node,
            )
        )

    def _handle_removed_subscription(self, sub: Subscription):
        sub.handle.clear_on_new_message_callback()

    def _update_entity_set(
        self,
        current_entity_to_node: Dict[EntityT, Node],
        new_entity_to_node: Dict[EntityT, Node],
        on_added_entity: Callable[[EntityT, Node], None],
        on_removed_entity: Callable[[EntityT], None],
    ) -> bool:
        current_entities = set(current_entity_to_node.keys())
        new_entities = set(new_entity_to_node.keys())

        added_entities = new_entities - current_entities
        for entity in added_entities:
            node = new_entity_to_node[entity]
            current_entity_to_node[entity] = node
            entity.handle.__enter__()
            on_added_entity(entity, node)

        removed_entities = current_entities - new_entities
        for entity in removed_entities:
            on_removed_entity(entity)
            entity.handle.__exit__(None, None, None)
            del current_entity_to_node[entity]

        return bool(added_entities or removed_entities)
    
    def _handle_ready_entity(
        self,
        take_entity_callback: Callable[[EntityT], Optional[Coroutine]],
        entity: EntityT,
        node: Node,
        number_of_events: int,
    ) -> None:
        if node not in self._nodes:
            return

        tasks = self._node_to_tasks[node]
        for _ in range(number_of_events):
            callback = take_entity_callback(entity)
            if not callback:
                return

            task = self._loop.create_task(callback)
            task.add_done_callback(partial(self._done_callback, node))
            tasks.add(task)

    def _done_callback(
        self,
        node: Node,
        task: asyncio.Task,
    ) -> None:
        if task.cancelled():
            return

        self._node_to_tasks[node].remove(task)

        exc = task.exception()
        if exc:
            node.get_logger().error(''.join(traceback.format_exception(exc)))
