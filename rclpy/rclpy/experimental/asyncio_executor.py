import asyncio
from functools import partial
from typing import Any, Callable, Coroutine, Optional, Set, Tuple, Union

from rclpy.executors import InvalidHandle, Msg, await_or_execute
from rclpy.node import MessageInfo, Node
from rclpy.subscription import Subscription

import rclpy


class AsyncioExecutor:

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._tasks: Set[asyncio.Task] = set()
        self._nodes: Set[Node] = set()
        self._subscriptions: Set[Subscription] = set()
        self._loop = loop

    def _take_subscription(
        self, sub: Subscription[Any]
    ) -> Optional[Callable[[], Coroutine[None, None, None]]]:
        try:
            with sub.handle:
                msg_info = sub.handle.take_message(sub.msg_type, sub.raw)
                if msg_info is None:
                    return None

                if sub._callback_type is Subscription.CallbackType.MessageOnly:
                    msg_tuple: Union[Tuple[Msg], Tuple[Msg, MessageInfo]] = (msg_info[0],)
                else:
                    msg_tuple = msg_info

                async def _execute() -> None:
                    await await_or_execute(sub.callback, *msg_tuple)

                return _execute
        except InvalidHandle:
            # Subscription is a Destroyable, which means that on __enter__ it can throw an
            # InvalidHandle exception if the entity has already been destroyed.  Handle that here
            # by just returning an empty argument, which means we will skip doing any real work
            # in _execute_subscription below
            pass

        return None

    def _execute_entity(self, coro: Coroutine) -> asyncio.Task:
        task = self._loop.create_task(coro)
        task.add_done_callback(self._exception_handler)
        self._tasks.add(task)

    def _exception_handler(self, fut):
        ex = fut.exception()
        if ex:
            raise ex

        self._tasks.remove(fut)

    def wake(self) -> None:
        self._update_entities_from_nodes()

    def add_node(self, node: Node) -> bool:
        if node in self._nodes:
            return False

        self._nodes.add(node)
        node.executor = self
        self._update_entities_from_nodes()
        return True

    def remove_node(self, node: Node) -> None:
        if node not in self._nodes:
            return

        self._nodes.remove(node)
        self._update_entities_from_nodes()

    def _update_entities_from_nodes(self) -> None:
        subscriptions = set()
        for node in self._nodes:
            subscriptions.update(node.subscriptions)

        # Sync each entity category
        self._update_entity_set(
            self._subscriptions,
            subscriptions,
            lambda s: s.set_on_new_message_callback(
                partial(
                    self._loop.call_soon_threadsafe,
                    partial(self._handle_ready_subscription, s)
                )
            ),
            lambda s: s.clear_on_new_message_callback(),
        )

    def _handle_ready_subscription(self, subscription: Subscription, number_of_events: int):
        for _ in range(number_of_events):
            coro = self._take_subscription(subscription)
            if not coro:
                break

            self._execute_entity(coro())

    def _update_entity_set(
        self,
        current_set: set,
        new_set: set,
        added_cb: Callable[[Any], None],
        removed_cb: Callable[[Any], None],
    ) -> None:
        # Handle additions
        for h in new_set - current_set:
            current_set.add(h)
            added_cb(h)

        # Handle removals
        for h in current_set - new_set:
            current_set.remove(h)
            removed_cb(h)
